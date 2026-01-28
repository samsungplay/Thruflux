package app

import (
	"bufio"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/sheerbytes/sheerbytes/internal/bench"
	"github.com/sheerbytes/sheerbytes/internal/clienthttp"
	"github.com/sheerbytes/sheerbytes/internal/ice"
	"github.com/sheerbytes/sheerbytes/internal/progress"
	"github.com/sheerbytes/sheerbytes/internal/quictransport"
	"github.com/sheerbytes/sheerbytes/internal/transfer"
	"github.com/sheerbytes/sheerbytes/internal/transferquic"
	"github.com/sheerbytes/sheerbytes/internal/transport"
	"github.com/sheerbytes/sheerbytes/internal/wsclient"
	"github.com/sheerbytes/sheerbytes/pkg/manifest"
	"github.com/sheerbytes/sheerbytes/pkg/protocol"
)

const (
	ReceiverStatusJoined       = "JOINED"
	ReceiverStatusAccepted     = "ACCEPTED"
	ReceiverStatusQueued       = "QUEUED"
	ReceiverStatusTransferring = "TRANSFERRING"
	ReceiverStatusDone         = "DONE"
	ReceiverStatusFailed       = "FAILED"
)

func copyJoinCodeToClipboard(joinCode string) bool {
	switch runtime.GOOS {
	case "darwin":
		return runClipboardCmd(joinCode, "pbcopy")
	case "windows":
		return runClipboardCmd(joinCode, "cmd", "/c", "clip")
	default:
		if runClipboardCmd(joinCode, "wl-copy") {
			return true
		}
		if runClipboardCmd(joinCode, "xclip", "-selection", "clipboard") {
			return true
		}
		return runClipboardCmd(joinCode, "xsel", "--clipboard", "--input")
	}
}

func runClipboardCmd(payload string, name string, args ...string) bool {
	cmd := exec.Command(name, args...)
	cmd.Stdin = strings.NewReader(payload)
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	return cmd.Run() == nil
}

// ReceiverState tracks the state of a receiver peer.
type ReceiverState struct {
	PeerID              string
	JoinedAt            time.Time
	LastSeen            time.Time
	Status              string
	ParallelConnections int
	ParallelStreams     int
}

// SnapshotSenderConfig configures the snapshot sender.
type SnapshotSenderConfig struct {
	ServerURL              string
	Paths                  []string
	MaxReceivers           int
	ReceiverTTL            time.Duration
	TransferOpts           transfer.Options
	Benchmark              bool
	Dumb                   bool
	DumbSizeBytes          int64
	DumbName               string
	DumbTCP                bool
	DumbConnections        int
	ParallelConnections    int
	UDPReadBufferBytes     int
	UDPWriteBufferBytes    int
	QuicConnWindowBytes    int
	QuicStreamWindowBytes  int
	QuicMaxIncomingStreams int
	StunServers            []string
	TurnServers            []string
	TurnOnly               bool
	Verbose                bool
}

// SnapshotSender orchestrates snapshot transfer scheduling.
type SnapshotSender struct {
	logger        *slog.Logger
	serverURL     string
	paths         []string
	maxRecv       int
	receiverTTL   time.Duration
	transferOpts  transfer.Options
	transferFn    func(context.Context, string) error
	dumb          bool
	dumbPath      string
	dumbSizeBytes int64
	dumbName      string
	dumbTCP       bool
	dumbConns     int
	parallelConns int

	peerID     string
	sessionID  string
	joinCode   string
	manifest   manifest.Manifest
	manifestID string
	summary    protocol.ManifestSummary

	conn                   *wsclient.Conn
	receivers              map[string]*ReceiverState
	queue                  []string
	active                 map[string]*transferSlot
	signalCh               map[string]chan protocol.Envelope
	mu                     sync.Mutex
	progressMu             sync.Mutex
	progress               map[string]*senderProgress
	snapshotLine           string
	tuneLine               string
	tuneTTY                bool
	paramsMu               sync.RWMutex
	params                 perfParams
	benchmark              bool
	benchSummaryLogged     bool
	benchAggregate         bench.AggregateSnapshot
	udpReadBufferBytes     int
	udpWriteBufferBytes    int
	quicConnWindowBytes    int
	quicStreamWindowBytes  int
	quicMaxIncomingStreams int
	stunServers            []string
	turnServers            []string
	turnOnly               bool
	turnMu                 sync.RWMutex
	transportSummary       string
	transportLines         []string
	transportLogged        bool
	verbose                bool
	now                    func() time.Time
	onChange               func()
	exitFn                 func(int)
	closeConn              func()
}

type perfParams struct {
	ChunkSize           int
	ParallelStreams     int
	ParallelConnections int
}

type transferSlot struct {
	peerID  string
	cancel  context.CancelFunc
	closeFn func()
}

func startScanStatus() func(error) {
	if !progress.IsTTY(os.Stdout) && !progress.IsTTY(os.Stderr) {
		fmt.Fprintln(os.Stderr, "Scanning files...")
		return func(err error) {
			if err != nil {
				fmt.Fprintln(os.Stderr, "Scan failed.")
				return
			}
			fmt.Fprintln(os.Stderr, "Scan complete.")
		}
	}

	done := make(chan struct{})
	start := time.Now()
	go func() {
		spinner := []rune{'|', '/', '-', '\\'}
		idx := 0
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				elapsed := time.Since(start).Truncate(100 * time.Millisecond)
				fmt.Fprintf(os.Stderr, "\rScanning files... %c %s", spinner[idx%len(spinner)], elapsed)
				idx++
			}
		}
	}()

	return func(err error) {
		close(done)
		elapsed := time.Since(start).Truncate(100 * time.Millisecond)
		if err != nil {
			fmt.Fprintf(os.Stderr, "\rScan failed after %s.\n", elapsed)
			return
		}
		fmt.Fprintf(os.Stderr, "\rScan complete in %s.\n", elapsed)
	}
}

// RunSnapshotSender runs the snapshot sender flow.
func RunSnapshotSender(ctx context.Context, logger *slog.Logger, cfg SnapshotSenderConfig) error {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(os.Stderr, nil))
	}
	if len(cfg.Paths) == 0 && !(cfg.Dumb && cfg.DumbSizeBytes > 0) {
		return fmt.Errorf("no input paths provided")
	}
	if cfg.MaxReceivers <= 0 {
		cfg.MaxReceivers = 4
	}
	if cfg.ReceiverTTL <= 0 {
		cfg.ReceiverTTL = 10 * time.Minute
	}
	if cfg.DumbConnections < 1 {
		cfg.DumbConnections = 1
	}
	if cfg.ParallelConnections < 1 {
		cfg.ParallelConnections = 4
	}
	if cfg.UDPReadBufferBytes <= 0 {
		cfg.UDPReadBufferBytes = 8 * 1024 * 1024
	}
	if cfg.UDPWriteBufferBytes <= 0 {
		cfg.UDPWriteBufferBytes = 8 * 1024 * 1024
	}
	if cfg.QuicConnWindowBytes <= 0 {
		cfg.QuicConnWindowBytes = 512 * 1024 * 1024
	}
	if cfg.QuicStreamWindowBytes <= 0 {
		cfg.QuicStreamWindowBytes = 64 * 1024 * 1024
	}
	if cfg.QuicMaxIncomingStreams <= 0 {
		cfg.QuicMaxIncomingStreams = 100
	}

	var (
		m        manifest.Manifest
		resolver func(string) string
		err      error
	)
	if cfg.Dumb && cfg.DumbSizeBytes > 0 {
		name := cfg.DumbName
		if name == "" {
			name = "mem.bin"
		}
		m = manifest.Manifest{
			Root:       "dumb",
			Items:      []manifest.FileItem{{RelPath: name, Size: cfg.DumbSizeBytes, ModTime: time.Now().Unix(), IsDir: false}},
			TotalBytes: cfg.DumbSizeBytes,
			FileCount:  1,
		}
	} else {
		stopScan := startScanStatus()
		m, err = manifest.ScanPaths(cfg.Paths)
		stopScan(err)
		if err != nil {
			return fmt.Errorf("failed to scan paths: %w", err)
		}
		if cfg.Dumb {
			if len(cfg.Paths) != 1 {
				return fmt.Errorf("dumb mode requires exactly one file path")
			}
			info, err := os.Stat(cfg.Paths[0])
			if err != nil {
				return fmt.Errorf("failed to stat path: %w", err)
			}
			if info.IsDir() {
				return fmt.Errorf("dumb mode requires a file, not a directory")
			}
		}
		resolver, err = buildPathResolver(cfg.Paths)
		if err != nil {
			return err
		}
	}
	manifestID, err := hashManifestJSON(m)
	if err != nil {
		return err
	}
	summary := protocol.ManifestSummary{
		ManifestID:  manifestID,
		TotalBytes:  m.TotalBytes,
		FileCount:   m.FileCount,
		FolderCount: m.FolderCount,
		RootName:    m.Root,
	}

	peerID := randomPeerID()
	sessionID, joinCode, _, err := clienthttp.CreateSession(ctx, cfg.ServerURL, cfg.MaxReceivers)
	if err != nil {
		return fmt.Errorf("failed to create session: %w", err)
	}

	fmt.Printf("\n=== Join Code: %s ===\n", joinCode)
	if copyJoinCodeToClipboard(joinCode) {
		fmt.Fprintln(os.Stdout, "Join code copied to clipboard.")
	}
	fmt.Printf("Receivers should run: thru join %s --out <dir>\n\n", joinCode)
	if len(cfg.Paths) > 0 {
		fmt.Fprintln(os.Stdout, "Sharing the following paths:")
		for _, path := range cfg.Paths {
			fmt.Fprintf(os.Stdout, "  - %s\n", path)
		}
		fmt.Fprintln(os.Stdout)
	}

	wsURL, err := buildWebSocketURL(cfg.ServerURL, joinCode, peerID, "sender", cfg.MaxReceivers)
	if err != nil {
		return err
	}
	conn, err := wsclient.Dial(ctx, wsURL, logger)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}

	s := &SnapshotSender{
		logger:       logger,
		serverURL:    cfg.ServerURL,
		paths:        cfg.Paths,
		maxRecv:      cfg.MaxReceivers,
		receiverTTL:  cfg.ReceiverTTL,
		transferOpts: cfg.TransferOpts,
		benchmark:    cfg.Benchmark,
		dumb:         cfg.Dumb,
		dumbPath: func() string {
			if len(cfg.Paths) > 0 {
				return cfg.Paths[0]
			}
			return ""
		}(),
		dumbSizeBytes:          cfg.DumbSizeBytes,
		dumbName:               cfg.DumbName,
		dumbTCP:                cfg.DumbTCP,
		dumbConns:              cfg.DumbConnections,
		parallelConns:          cfg.ParallelConnections,
		verbose:                cfg.Verbose,
		peerID:                 peerID,
		sessionID:              sessionID,
		joinCode:               joinCode,
		manifest:               m,
		manifestID:             manifestID,
		summary:                summary,
		udpReadBufferBytes:     cfg.UDPReadBufferBytes,
		udpWriteBufferBytes:    cfg.UDPWriteBufferBytes,
		quicConnWindowBytes:    cfg.QuicConnWindowBytes,
		quicStreamWindowBytes:  cfg.QuicStreamWindowBytes,
		quicMaxIncomingStreams: cfg.QuicMaxIncomingStreams,
		stunServers:            cfg.StunServers,
		turnServers:            cfg.TurnServers,
		turnOnly:               cfg.TurnOnly,
		conn:                   conn,
		receivers:              make(map[string]*ReceiverState),
		active:                 make(map[string]*transferSlot),
		signalCh:               make(map[string]chan protocol.Envelope),
		progress:               make(map[string]*senderProgress),
		now:                    time.Now,
		exitFn:                 os.Exit,
	}
	s.transferFn = s.runICEQUICTransfer
	if resolver != nil {
		s.transferOpts.ResolveFilePath = resolver
	}
	s.closeConn = func() { conn.Close() }
	s.onChange = s.logSnapshotState
	s.logSnapshotState()
	s.tuneTTY = progress.IsTTY(os.Stdout)
	s.initParams()
	s.initTransport()

	if s.benchmark {
		s.startBenchmarkLoop(ctx)
	}

	uiStop := progress.RenderSender(ctx, os.Stdout, s.senderView, s.verbose)
	defer uiStop()

	go s.cleanupLoop(ctx)
	go s.watchHardQuit()

	err = conn.ReadLoop(ctx, func(env protocol.Envelope) {
		s.handleEnvelope(ctx, env)
	})
	if err != nil && err != context.Canceled {
		return err
	}
	return nil
}

func (s *SnapshotSender) handleEnvelope(ctx context.Context, env protocol.Envelope) {
	if err := env.ValidateBasic(); err != nil {
		s.logger.Error("invalid envelope", "error", err)
		return
	}

	switch env.Type {
	case protocol.TypeTurnCredentials:
		var creds protocol.TurnCredentials
		if err := env.DecodePayload(&creds); err != nil {
			s.logger.Error("failed to decode turn_credentials", "error", err)
			return
		}
		_ = s.setTurnServersIfEmpty(creds.Servers)

	case protocol.TypePeerJoined:
		var peerJoined protocol.PeerJoined
		if err := env.DecodePayload(&peerJoined); err != nil {
			s.logger.Error("failed to decode peer_joined", "error", err)
			return
		}
		if peerJoined.Peer.Role != "receiver" {
			return
		}
		s.handlePeerJoined(peerJoined.Peer.PeerID)
		s.sendManifestOffer(peerJoined.Peer.PeerID)

	case protocol.TypeManifestAccept:
		var accept protocol.ManifestAccept
		if err := env.DecodePayload(&accept); err != nil {
			s.logger.Error("failed to decode manifest_accept", "error", err)
			return
		}
		s.handleManifestAccept(env.From, accept)
		s.maybeStartTransfers(ctx)

	case protocol.TypePeerLeft:
		var peerLeft protocol.PeerLeft
		if err := env.DecodePayload(&peerLeft); err != nil {
			s.logger.Error("failed to decode peer_left", "error", err)
			return
		}
		s.logger.Error("receiver left session", "peer_id", peerLeft.PeerID, "session_id", s.sessionID)
		s.handlePeerLeft(peerLeft.PeerID)

	case protocol.TypeIceCredentials, protocol.TypeIceCandidates, protocol.TypeIceCandidate, protocol.TypeDumbQUICDone:
		s.forwardSignal(env)
	}
}

func (s *SnapshotSender) handlePeerJoined(peerID string) {
	now := s.now()
	s.mu.Lock()
	state, exists := s.receivers[peerID]
	if !exists {
		state = &ReceiverState{
			PeerID:   peerID,
			JoinedAt: now,
		}
		s.receivers[peerID] = state
	}
	state.LastSeen = now
	state.Status = ReceiverStatusJoined
	s.mu.Unlock()
	s.emitChange()
}

func (s *SnapshotSender) handleManifestAccept(peerID string, accept protocol.ManifestAccept) {
	now := s.now()
	var queuedMsgs []queuedUpdate
	s.mu.Lock()
	state, exists := s.receivers[peerID]
	if !exists {
		state = &ReceiverState{
			PeerID:   peerID,
			JoinedAt: now,
		}
		s.receivers[peerID] = state
	}
	state.LastSeen = now
	if accept.ParallelConnections > 0 {
		state.ParallelConnections = accept.ParallelConnections
	}
	if accept.ParallelStreams > 0 {
		state.ParallelStreams = accept.ParallelStreams
	}
	if state.Status == ReceiverStatusTransferring {
		s.mu.Unlock()
		return
	}
	state.Status = ReceiverStatusQueued
	s.enqueueLocked(peerID)
	queuedMsgs = s.collectQueuedUpdatesLocked()
	s.mu.Unlock()
	s.emitChange()
	s.sendQueuedUpdates(queuedMsgs)
}

func (s *SnapshotSender) handlePeerLeft(peerID string) {
	var queuedMsgs []queuedUpdate
	s.mu.Lock()
	state := s.receivers[peerID]
	if state != nil && state.Status != ReceiverStatusDone {
		state.Status = ReceiverStatusFailed
		state.LastSeen = s.now()
	}
	if slot := s.active[peerID]; slot != nil {
		if slot.closeFn != nil {
			slot.closeFn()
		}
		if slot.cancel != nil {
			slot.cancel()
		}
		delete(s.active, peerID)
	}
	delete(s.signalCh, peerID)
	filtered := s.queue[:0]
	for _, queued := range s.queue {
		if queued != peerID {
			filtered = append(filtered, queued)
		}
	}
	s.queue = filtered
	queuedMsgs = s.collectQueuedUpdatesLocked()
	s.mu.Unlock()

	s.emitChange()
	s.sendQueuedUpdates(queuedMsgs)
	s.maybeStartTransfers(context.Background())
}

func (s *SnapshotSender) receiverParallelCaps(peerID string) (int, int) {
	if peerID == "" {
		return 0, 0
	}
	s.mu.Lock()
	state := s.receivers[peerID]
	s.mu.Unlock()
	if state == nil {
		return 0, 0
	}
	return state.ParallelConnections, state.ParallelStreams
}

func (s *SnapshotSender) effectiveParallelConnections(peerID string) int {
	conns := s.parallelConns
	if conns < 1 {
		conns = 1
	}
	if recvConns, _ := s.receiverParallelCaps(peerID); recvConns > 0 && recvConns < conns {
		conns = recvConns
	}
	if conns < 1 {
		conns = 1
	}
	return conns
}

func (s *SnapshotSender) setTurnServersIfEmpty(servers []string) bool {
	if len(servers) == 0 {
		return false
	}
	s.turnMu.Lock()
	defer s.turnMu.Unlock()
	if len(s.turnServers) > 0 {
		return false
	}
	s.turnServers = append([]string{}, servers...)
	return true
}

func (s *SnapshotSender) currentTurnServers() []string {
	s.turnMu.RLock()
	defer s.turnMu.RUnlock()
	return append([]string{}, s.turnServers...)
}

func (s *SnapshotSender) enqueueLocked(peerID string) {
	for _, existing := range s.queue {
		if existing == peerID {
			return
		}
	}
	s.queue = append(s.queue, peerID)
}

func (s *SnapshotSender) maybeStartTransfers(ctx context.Context) {
	for {
		var queuedMsgs []queuedUpdate
		s.mu.Lock()
		if len(s.active) >= s.maxRecv || len(s.queue) == 0 {
			s.mu.Unlock()
			return
		}
		peerID := s.queue[0]
		s.queue = s.queue[1:]
		state := s.receivers[peerID]
		if state == nil {
			s.mu.Unlock()
			continue
		}
		if state.Status == ReceiverStatusTransferring {
			s.mu.Unlock()
			continue
		}
		state.Status = ReceiverStatusTransferring
		state.LastSeen = s.now()
		ctxTransfer, cancel := context.WithCancel(ctx)
		slot := &transferSlot{peerID: peerID, cancel: cancel}
		s.active[peerID] = slot
		s.signalCh[peerID] = make(chan protocol.Envelope, 64)
		queuedMsgs = s.collectQueuedUpdatesLocked()
		s.mu.Unlock()
		s.emitChange()
		s.sendQueuedUpdates(queuedMsgs)

		s.sendTransferStart(peerID)

		go s.runTransfer(ctxTransfer, peerID)
	}
}

func (s *SnapshotSender) runTransfer(ctx context.Context, peerID string) {
	transferFn := s.transferFn
	if transferFn == nil {
		transferFn = s.runICEQUICTransfer
	}
	err := transferFn(ctx, peerID)
	now := s.now()
	var queuedMsgs []queuedUpdate

	s.mu.Lock()
	state := s.receivers[peerID]
	if state != nil {
		state.LastSeen = now
		if err == nil {
			state.Status = ReceiverStatusDone
		} else {
			state.Status = ReceiverStatusFailed
			// Mark stage as failed if it hasn't reached connect_ok
			s.mu.Unlock() // avoid deadlock as setSenderStage locks s.progressMu then state.mu
			s.setSenderStage(peerID, fmt.Sprintf("FAILED: %v", err))
			s.mu.Lock()
		}
	}
	delete(s.active, peerID)
	delete(s.signalCh, peerID)
	queuedMsgs = s.collectQueuedUpdatesLocked()
	s.mu.Unlock()
	s.emitChange()
	s.sendQueuedUpdates(queuedMsgs)

	if err != nil {
		fmt.Fprintf(os.Stderr, "transfer failed: %v\n", err)
		s.logger.Error("transfer failed", "peer_id", peerID, "error", err)
	} else {
		s.ForceComplete(peerID)
	}

	if s.benchmark {
		s.freezeBench(peerID)
		s.maybePrintBenchSummary()
	}

	s.maybeStartTransfers(ctx)
}

func (s *SnapshotSender) runICEQUICTransfer(ctx context.Context, peerID string) error {
	if s.dumbTCP {
		return s.runDumbTCPTransfer(ctx, peerID)
	}
	signalCh := s.getSignalCh(peerID)
	if signalCh == nil {
		return fmt.Errorf("no signal channel for %s", peerID)
	}
	progressState := s.initSenderProgress(peerID, s.manifest.TotalBytes)

	iceLog := func(stage string) {
		s.setSenderStage(peerID, stage)
	}

	sendSignal := func(msgType string, payload any) error {
		env, err := protocol.NewEnvelope(msgType, protocol.NewMsgID(), payload)
		if err != nil {
			return err
		}
		env.SessionID = s.sessionID
		env.From = s.peerID
		env.To = peerID
		return s.conn.Send(env)
	}

	// Initialize Prober
	proberCfg := ice.ProberConfig{
		StunServers: s.stunServers,
		TurnServers: s.currentTurnServers(),
		TurnOnly:    s.turnOnly,
	}
	prober, err := ice.NewProber(proberCfg, s.logger)
	if err != nil {
		return fmt.Errorf("failed to create prober: %w", err)
	}
	defer prober.Close()

	// Get local candidates
	localCandidates := prober.GetProbingAddresses()
	iceLog("gather_complete")

	// Send candidates
	if err := sendSignal(protocol.TypeIceCandidates, protocol.IceCandidates{Candidates: localCandidates}); err != nil {
		return fmt.Errorf("failed to send candidates: %w", err)
	}
	s.setSenderStage(peerID, fmt.Sprintf("local_candidates_sent count=%d", len(localCandidates)))

	// Receiver Remote Candidates
	remoteCandsCh := make(chan []string, 1)
	readCtx, readCancel := context.WithCancel(ctx)
	defer readCancel()

	readErr := make(chan error, 1)

	go func() {
		for {
			select {
			case <-readCtx.Done():
				return
			case env := <-signalCh:
				switch env.Type {
				case protocol.TypeIceCandidates:
					var cands protocol.IceCandidates
					if err := env.DecodePayload(&cands); err != nil {
						readErr <- err
						return
					}
					select {
					case remoteCandsCh <- cands.Candidates:
					default:
					}
				case protocol.TypeIceCandidate:
					// Supporting incremental just in case, though we prefer bulk
					var cand protocol.IceCandidate
					if err := env.DecodePayload(&cand); err != nil {
						readErr <- err
						return
					}
					select {
					case remoteCandsCh <- []string{cand.Candidate}:
					default:
					}
				}
			}
		}
	}()

	var remoteCands []string
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-readErr:
		return err
	case cands := <-remoteCandsCh:
		remoteCands = cands
		s.setSenderStage(peerID, fmt.Sprintf("remote_candidates_received count=%d", len(cands)))
		prober.PrimeTurnPermissions(remoteCands)
		for _, cand := range remoteCands {
			if ice.IsTurnCandidate(cand) {
				s.setSenderProbeStatus(peerID, cand, ice.ProbeStateReadyFallback)
			}
		}
	case <-time.After(10 * time.Second):
		return fmt.Errorf("timeout waiting for remote candidates")
	}

	iceLog("probing_start")

	// Prepare QUIC Config
	udpTune := transport.ApplyUDPBeyondBestEffort(nil, s.udpReadBufferBytes, s.udpWriteBufferBytes)
	// Apply to our socket
	if conn, ok := prober.ListenPacket().(*net.UDPConn); ok {
		udpTune = transport.ApplyUDPBeyondBestEffort(conn, s.udpReadBufferBytes, s.udpWriteBufferBytes)
	}

	quicCfg, quicTune := transport.BuildQuicConfig(
		quictransport.DefaultClientQUICConfig(),
		s.quicConnWindowBytes,
		s.quicStreamWindowBytes,
		s.quicMaxIncomingStreams,
	)
	s.setTransportLines(
		formatTransportSummary(udpTune, quicTune),
		[]string{formatUDPTuneLine(udpTune), formatQuicTuneLine(quicTune)},
	)

	// Probe and Dial
	// We need a dummy TLS config for quic-go if not using a custom one (quictransport usually provides one)
	// But `ProbeAndDial` expects us to pass it.
	// `quictransport.DialWithConfig` generates one internally if we don't pass it?
	// Actually `DialWithConfig` in `internal/quictransport` calls `quic.Dial`.
	// Let's grab the TLS config from `transferquic` or `quictransport`.
	// `quictransport.DefaultClientQUICConfig` only returns *quic.Config.
	// We need a TLS config. `quictransport` helper might have one.
	// `quictransport.DialWithConfig` creates a TLS config. We should replicate that or expose it.
	// Since I can't easily change `quictransport` right now without reading it, I'll assume I can construct a skip-verify one.

	tlsConf := quictransport.ClientConfig()

	quicConn, err := prober.ProbeAndDial(ctx, remoteCands, tlsConf, quicCfg, func(upd ice.ProbeUpdate) {
		s.setSenderProbeStatus(peerID, upd.Addr, upd.State)
	})
	if err != nil {
		return fmt.Errorf("probing failed: %w", err)
	}
	defer quicConn.CloseWithError(0, "")

	iceLog("connect_ok")
	if s.verbose {
		fmt.Fprintf(os.Stderr, "QUIC connection established via probing (peer=%s session=%s)\n", peerID, s.sessionID)
	}
	// Log route
	s.setSenderRoute(peerID, fmt.Sprintf("local=%s remote=%s", quicConn.LocalAddr(), quicConn.RemoteAddr()))

	s.setTransferCloser(peerID, func() {
		_ = quicConn.CloseWithError(0, "")
	})

	quicTransport := transferquic.NewDialer(quicConn, s.logger)
	defer quicTransport.Close()

	transferConn, err := quicTransport.Dial(ctx, peerID)
	if err != nil {
		return fmt.Errorf("failed to dial transfer connection: %w", err)
	}
	defer transferConn.Close()
	if s.verbose {
		fmt.Fprintf(os.Stderr, "transfer connection ready (peer=%s session=%s)\n", peerID, s.sessionID)
	}

	authCtx, authCancel := context.WithTimeout(ctx, 10*time.Second)
	if err := authenticateTransport(authCtx, transferConn, s.joinCode, authRoleSender); err != nil {
		authCancel()
		return fmt.Errorf("transport auth failed: %w", err)
	}
	authCancel()

	if s.dumb {
		name := s.dumbName
		if name == "" {
			name = filepath.Base(s.dumbPath)
		}
		size := s.dumbSizeBytes
		if size == 0 {
			info, err := os.Stat(s.dumbPath)
			if err != nil {
				return fmt.Errorf("failed to stat path: %w", err)
			}
			size = info.Size()
		}
		if s.dumbConns <= 1 {
			s.setSenderConnCount(peerID, 1)
			return sendDumbData(ctx, transferConn, name, size)
		}
		if err := sendSignal(protocol.TypeDumbQUICMulti, protocol.DumbQUICMulti{Connections: s.dumbConns}); err != nil {
			return fmt.Errorf("failed to send dumb quic multi: %w", err)
		}
		remoteAddr, err := resolveUDPAddr(quicConn.RemoteAddr())
		if err != nil {
			return err
		}
		extra, err := s.dialExtraConns(ctx, peerID, remoteAddr, tlsConf, quicCfg, s.dumbConns-1)
		if err != nil {
			return err
		}
		conns := make([]transfer.Conn, 0, 1+len(extra))
		conns = append(conns, transferConn)
		for _, c := range extra {
			conns = append(conns, c.conn)
		}
		s.setSenderConnCount(peerID, len(conns))
		if err := sendDumbDataMulti(ctx, conns, name, size); err != nil {
			return err
		}
		if err := s.waitForDumbQUICDone(ctx, peerID, 10*time.Second); err != nil {
			return err
		}
		return nil
	}

	targetConns := s.effectiveParallelConnections(peerID)
	conns := []transfer.Conn{transferConn}
	var extra []dumbExtraConn
	if targetConns > 1 {
		remoteAddr, err := resolveUDPAddr(quicConn.RemoteAddr())
		if err != nil {
			return err
		}
		dialCtx, dialCancel := context.WithTimeout(ctx, 10*time.Second)
		extra, err = s.dialExtraConns(dialCtx, peerID, remoteAddr, tlsConf, quicCfg, targetConns-1)
		dialCancel()
		if err != nil {
			s.logger.Warn("failed to establish extra connections", "error", err)
		}
		for _, c := range extra {
			conns = append(conns, c.conn)
		}
	}
	if len(conns) == 0 {
		return fmt.Errorf("no transfer connections available")
	}
	s.setSenderConnCount(peerID, len(conns))
	defer func() {
		for _, c := range extra {
			c.close()
		}
	}()

	requestedStreams := s.transferOpts.ParallelFiles
	if _, recvStreams := s.receiverParallelCaps(peerID); recvStreams > 0 && (requestedStreams == 0 || recvStreams < requestedStreams) {
		requestedStreams = recvStreams
	}
	totalStreams, _ := computeParallelBudget(s.manifest.FileCount, requestedStreams, len(conns), len(conns) > 1)
	s.setParams(perfParams{
		ChunkSize:           int(s.transferOpts.ChunkSize),
		ParallelStreams:     totalStreams,
		ParallelConnections: len(conns),
	})
	s.setTuneLine(fmt.Sprintf("Params: %s", formatTuneParams(s.getParams())))

	var lastStats int64
	transferCtx, transferCancel := context.WithCancel(ctx)
	defer transferCancel()

	progressCollector := newProgressCollector()
	progressStop := startProgressTicker(transferCtx, progressCollector, func(relpath string, bytesSent int64, total int64) {
		s.updateSenderProgress(progressState, relpath, bytesSent, total)
	})
	defer progressStop()

	opts := s.transferOptions()
	opts.ParallelFiles = totalStreams
	opts.StripeMax = len(conns)
	opts.ParamSource = func() transfer.RuntimeParams {
		return transfer.RuntimeParams{
			ChunkSize:     s.transferOpts.ChunkSize,
			ParallelFiles: totalStreams,
		}
	}
	opts.ResumeStatsFn = func(relpath string, skippedChunks, totalChunks uint32, verifiedChunk uint32, totalBytes int64, chunkSize uint32) {
		if skippedChunks == 0 || totalChunks == 0 {
			return
		}
		skippedBytes := int64(skippedChunks) * int64(chunkSize)
		if totalBytes > 0 && skippedBytes > totalBytes {
			skippedBytes = totalBytes
		}
		verifyBytes := computeVerifyBytes(totalBytes, totalChunks, chunkSize)
		s.addSenderSkipped(progressState, relpath, skippedBytes, verifyBytes)
	}
	opts.ProgressFn = func(relpath string, bytesSent int64, total int64) {
		progressCollector.Update(relpath, bytesSent, total)
	}
	opts.TransferStatsFn = func(activeFiles, completedFiles int, remainingBytes int64) {
		if !shouldUpdateProgress(&lastStats) {
			return
		}
		s.updateSenderStats(progressState, completedFiles)
	}
	opts.FileDoneFn = func(relpath string, ok bool) {
		s.markSenderVerified(progressState, relpath, ok)
		if total := progressCollector.Total(relpath); total > 0 {
			progressState.mu.Lock()
			offset := progressState.skipOffset[relpath]
			progressState.mu.Unlock()
			bytes := total - offset
			if bytes < 0 {
				bytes = 0
			}
			progressCollector.Update(relpath, bytes, total)
			s.updateSenderProgress(progressState, relpath, bytes, total)
		}
	}

	var multiTransferConn transfer.Conn = conns[0]
	if len(conns) > 1 {
		multi, err := transfer.NewMultiConn(conns)
		if err != nil {
			return err
		}
		defer multi.Close()
		multiTransferConn = multi
	}

	if err := transfer.SendManifestMultiStream(transferCtx, multiTransferConn, ".", s.manifest, opts); err != nil {
		return err
	}
	return nil
}

func (s *SnapshotSender) waitForDumbQUICDone(ctx context.Context, peerID string, timeout time.Duration) error {
	signalCh := s.getSignalCh(peerID)
	if signalCh == nil {
		return fmt.Errorf("no signal channel for %s", peerID)
	}
	waitCtx := ctx
	if timeout > 0 {
		var cancel context.CancelFunc
		waitCtx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	for {
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("timeout waiting for dumb completion")
		case env := <-signalCh:
			if env.Type != protocol.TypeDumbQUICDone {
				continue
			}
			var msg protocol.DumbQUICDone
			if err := env.DecodePayload(&msg); err != nil {
				return err
			}
			if msg.Parts <= 0 {
				return nil
			}
			return nil
		}
	}
}

type dumbExtraConn struct {
	conn  transfer.Conn
	close func()
}

func resolveUDPAddr(addr net.Addr) (*net.UDPAddr, error) {
	if addr == nil {
		return nil, fmt.Errorf("remote address is nil")
	}
	if udp, ok := addr.(*net.UDPAddr); ok {
		return udp, nil
	}
	udp, err := net.ResolveUDPAddr("udp", addr.String())
	if err != nil {
		return nil, fmt.Errorf("failed to resolve remote address: %w", err)
	}
	return udp, nil
}

func listenUDPForRemote(remote *net.UDPAddr) (*net.UDPConn, error) {
	if remote == nil {
		return nil, fmt.Errorf("remote address is nil")
	}
	if remote.IP != nil && remote.IP.To4() != nil {
		return net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4zero, Port: 0})
	}
	return net.ListenUDP("udp6", &net.UDPAddr{IP: net.IPv6zero, Port: 0, Zone: remote.Zone})
}

func (s *SnapshotSender) dialExtraConns(ctx context.Context, peerID string, remote *net.UDPAddr, tlsConf *tls.Config, quicCfg *quic.Config, extra int) ([]dumbExtraConn, error) {
	if extra <= 0 {
		return nil, nil
	}
	if remote == nil {
		return nil, fmt.Errorf("remote address is nil")
	}

	conns := make([]dumbExtraConn, 0, extra)
	var lastErr error
	for i := 0; i < extra; i++ {
		udpConn, err := listenUDPForRemote(remote)
		if err != nil {
			lastErr = err
			continue
		}
		_ = transport.ApplyUDPBeyondBestEffort(udpConn, s.udpReadBufferBytes, s.udpWriteBufferBytes)
		tr := &quic.Transport{Conn: udpConn}
		quicConn, err := tr.Dial(ctx, remote, tlsConf, quicCfg)
		if err != nil {
			udpConn.Close()
			lastErr = err
			continue
		}
		tconn, err := transferquic.NewDialer(quicConn, s.logger).Dial(ctx, peerID)
		if err != nil {
			quicConn.CloseWithError(0, "dial_failed")
			udpConn.Close()
			lastErr = err
			continue
		}
		authCtx, authCancel := context.WithTimeout(ctx, 10*time.Second)
		if err := authenticateTransport(authCtx, tconn, s.joinCode, authRoleSender); err != nil {
			authCancel()
			tconn.Close()
			quicConn.CloseWithError(0, "auth_failed")
			udpConn.Close()
			lastErr = err
			continue
		}
		authCancel()
		conn := tconn
		qc := quicConn
		uc := udpConn
		conns = append(conns, dumbExtraConn{
			conn: conn,
			close: func() {
				conn.Close()
				qc.CloseWithError(0, "")
				uc.Close()
			},
		})
	}
	if len(conns) == 0 && lastErr != nil {
		return conns, lastErr
	}
	return conns, lastErr
}

func (s *SnapshotSender) runDumbTCPTransfer(ctx context.Context, peerID string) error {
	sendSignal := func(msgType string, payload any) error {
		env, err := protocol.NewEnvelope(msgType, protocol.NewMsgID(), payload)
		if err != nil {
			return err
		}
		env.SessionID = s.sessionID
		env.From = s.peerID
		env.To = peerID
		return s.conn.Send(env)
	}

	listener, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		return fmt.Errorf("failed to listen tcp: %w", err)
	}
	defer listener.Close()

	port := listener.Addr().(*net.TCPAddr).Port
	addrs := dumbTCPListenAddrs(port)
	if err := sendSignal(protocol.TypeDumbTCPListen, protocol.DumbTCPListen{Addrs: addrs}); err != nil {
		return fmt.Errorf("failed to send tcp listen addrs: %w", err)
	}

	conn, err := acceptWithContext(ctx, listener)
	if err != nil {
		return fmt.Errorf("failed to accept tcp: %w", err)
	}
	defer conn.Close()

	name := s.dumbName
	if name == "" {
		name = filepath.Base(s.dumbPath)
	}
	size := s.dumbSizeBytes
	if size == 0 {
		info, err := os.Stat(s.dumbPath)
		if err != nil {
			return fmt.Errorf("failed to stat path: %w", err)
		}
		size = info.Size()
	}
	return sendDumbDataWriter(conn, []byte(name), size)
}

func (s *SnapshotSender) sendManifestOffer(peerID string) {
	if s.conn == nil {
		return
	}
	offer := protocol.ManifestOffer{Summary: s.summary}
	env, err := protocol.NewEnvelope(protocol.TypeManifestOffer, protocol.NewMsgID(), offer)
	if err != nil {
		s.logger.Error("failed to create manifest offer", "error", err)
		return
	}
	env.SessionID = s.sessionID
	env.From = s.peerID
	env.To = peerID
	if err := s.conn.Send(env); err != nil {
		s.logger.Error("failed to send manifest offer", "error", err, "peer_id", peerID)
	}
}

func (s *SnapshotSender) sendTransferStart(peerID string) {
	if s.conn == nil {
		return
	}
	start := protocol.TransferStart{
		ManifestID:          s.manifestID,
		SenderPeerID:        s.peerID,
		ReceiverPeerID:      peerID,
		TransferID:          randomTransferID(),
		ParallelConnections: s.effectiveParallelConnections(peerID),
	}
	env, err := protocol.NewEnvelope(protocol.TypeTransferStart, protocol.NewMsgID(), start)
	if err != nil {
		s.logger.Error("failed to create transfer start", "error", err)
		return
	}
	env.SessionID = s.sessionID
	env.From = s.peerID
	env.To = peerID
	if err := s.conn.Send(env); err != nil {
		s.logger.Error("failed to send transfer start", "error", err, "peer_id", peerID)
	}
}

func (s *SnapshotSender) sendTransferQueued(peerID string, queued protocol.TransferQueued) {
	if s.conn == nil {
		return
	}
	env, err := protocol.NewEnvelope(protocol.TypeTransferQueued, protocol.NewMsgID(), queued)
	if err != nil {
		s.logger.Error("failed to create transfer queued", "error", err)
		return
	}
	env.SessionID = s.sessionID
	env.From = s.peerID
	env.To = peerID
	if err := s.conn.Send(env); err != nil {
		s.logger.Error("failed to send transfer queued", "error", err, "peer_id", peerID)
	}
}

type queuedUpdate struct {
	peerID string
	msg    protocol.TransferQueued
}

func (s *SnapshotSender) collectQueuedUpdatesLocked() []queuedUpdate {
	if len(s.queue) == 0 {
		return nil
	}
	updates := make([]queuedUpdate, 0, len(s.queue))
	active := len(s.active)
	for idx, peerID := range s.queue {
		updates = append(updates, queuedUpdate{
			peerID: peerID,
			msg: protocol.TransferQueued{
				ManifestID:     s.manifestID,
				ReceiverPeerID: peerID,
				Position:       idx + 1,
				Active:         active,
				Max:            s.maxRecv,
			},
		})
	}
	return updates
}

func (s *SnapshotSender) sendQueuedUpdates(updates []queuedUpdate) {
	for _, update := range updates {
		s.sendTransferQueued(update.peerID, update.msg)
	}
}

func (s *SnapshotSender) forwardSignal(env protocol.Envelope) {
	peerID := env.From
	if peerID == "" {
		return
	}
	s.mu.Lock()
	ch := s.signalCh[peerID]
	s.mu.Unlock()
	if ch == nil {
		return
	}
	switch env.Type {
	case protocol.TypeIceCredentials, protocol.TypeIceCandidates, protocol.TypeIceCandidate, protocol.TypeDumbQUICDone:
		select {
		case ch <- env:
		default:
		}
	}
}

func (s *SnapshotSender) getSignalCh(peerID string) chan protocol.Envelope {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.signalCh[peerID]
}

func (s *SnapshotSender) setTransferCloser(peerID string, fn func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if slot := s.active[peerID]; slot != nil {
		slot.closeFn = fn
	}
}

func (s *SnapshotSender) cleanupLoop(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.cleanup()
		}
	}
}

func (s *SnapshotSender) cleanup() {
	now := s.now()
	changed := false

	s.mu.Lock()
	for peerID, state := range s.receivers {
		if state.Status == ReceiverStatusTransferring {
			continue
		}
		if now.Sub(state.LastSeen) > s.receiverTTL {
			delete(s.receivers, peerID)
			changed = true
		}
	}
	if changed {
		filtered := make([]string, 0, len(s.queue))
		for _, peerID := range s.queue {
			if _, ok := s.receivers[peerID]; ok {
				filtered = append(filtered, peerID)
			}
		}
		s.queue = filtered
	}
	s.mu.Unlock()

	if changed {
		s.emitChange()
	}
}

func (s *SnapshotSender) watchHardQuit() {
	reader := bufio.NewReader(os.Stdin)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		if strings.TrimSpace(line) == "q" {
			s.hardStop()
			return
		}
	}
}

func (s *SnapshotSender) hardStop() {
	s.mu.Lock()
	slots := make([]*transferSlot, 0, len(s.active))
	for _, slot := range s.active {
		slots = append(slots, slot)
	}
	s.mu.Unlock()

	for _, slot := range slots {
		if slot.closeFn != nil {
			slot.closeFn()
		}
		if slot.cancel != nil {
			slot.cancel()
		}
	}
	if s.closeConn != nil {
		s.closeConn()
	}
	if s.exitFn != nil {
		s.exitFn(0)
	}
}

func (s *SnapshotSender) emitChange() {
	if s.onChange != nil {
		s.onChange()
	}
}

func (s *SnapshotSender) logSnapshotState() {
	total := 0
	queued := 0
	active := 0
	done := 0
	failed := 0

	s.mu.Lock()
	for _, state := range s.receivers {
		total++
		switch state.Status {
		case ReceiverStatusQueued:
			queued++
		case ReceiverStatusTransferring:
			active++
		case ReceiverStatusDone:
			done++
		case ReceiverStatusFailed:
			failed++
		}
	}
	s.mu.Unlock()

	s.snapshotLine = fmt.Sprintf("Snapshot %s | total=%d queued=%d active=%d done=%d failed=%d", s.manifestID, total, queued, active, done, failed)
}

func (s *SnapshotSender) transferOptions() transfer.Options {
	opts := s.transferOpts
	opts.Resume = true
	opts.ParamSource = s.runtimeParams
	return opts
}

func (s *SnapshotSender) initParams() {
	heuristic := transfer.HeuristicParams(s.manifest.FileCount)
	if s.transferOpts.ChunkSize == 0 {
		s.transferOpts.ChunkSize = heuristic.ChunkSize
	}
	if s.transferOpts.ParallelFiles == 0 {
		s.transferOpts.ParallelFiles = heuristic.ParallelFiles
	}
	runtime := transfer.NormalizeParams(transfer.RuntimeParams{
		ChunkSize:     s.transferOpts.ChunkSize,
		ParallelFiles: s.transferOpts.ParallelFiles,
	}, s.transferOpts)
	s.paramsMu.Lock()
	s.params = perfParams{
		ChunkSize:           int(runtime.ChunkSize),
		ParallelStreams:     runtime.ParallelFiles,
		ParallelConnections: s.parallelConns,
	}
	s.paramsMu.Unlock()
	s.setTuneLine(fmt.Sprintf("Params: %s", formatTuneParams(s.getParams())))
}

func (s *SnapshotSender) initTransport() {
	var udpTune transport.UdpTuneResult
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4zero, Port: 0})
	if err == nil {
		udpTune = transport.ApplyUDPBeyondBestEffort(conn, s.udpReadBufferBytes, s.udpWriteBufferBytes)
		conn.Close()
	} else {
		udpTune = transport.ApplyUDPBeyondBestEffort(nil, s.udpReadBufferBytes, s.udpWriteBufferBytes)
	}

	_, quicTune := transport.BuildQuicConfig(
		quictransport.DefaultClientQUICConfig(),
		s.quicConnWindowBytes,
		s.quicStreamWindowBytes,
		s.quicMaxIncomingStreams,
	)

	s.setTransportLines(
		formatTransportSummary(udpTune, quicTune),
		[]string{formatUDPTuneLine(udpTune), formatQuicTuneLine(quicTune)},
	)
}

func (s *SnapshotSender) runtimeParams() transfer.RuntimeParams {
	s.paramsMu.RLock()
	params := s.params
	s.paramsMu.RUnlock()
	return transfer.RuntimeParams{
		ChunkSize:     uint32(params.ChunkSize),
		ParallelFiles: params.ParallelStreams,
	}
}

func (s *SnapshotSender) setParams(params perfParams) {
	s.paramsMu.Lock()
	s.params = params
	s.paramsMu.Unlock()
}

func (s *SnapshotSender) getParams() perfParams {
	s.paramsMu.RLock()
	params := s.params
	s.paramsMu.RUnlock()
	return params
}

func (s *SnapshotSender) setTuneLine(line string) {
	s.paramsMu.Lock()
	s.tuneLine = line
	s.paramsMu.Unlock()
}

func (s *SnapshotSender) tuneHeaderLine() string {
	s.paramsMu.RLock()
	line := s.tuneLine
	s.paramsMu.RUnlock()
	return line
}

func (s *SnapshotSender) startBenchmarkLoop(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				s.tickBenchmarks(now)
			}
		}
	}()
}

func (s *SnapshotSender) tickBenchmarks(now time.Time) {
	snaps := make(map[string]bench.Snapshot)
	s.progressMu.Lock()
	for peerID, state := range s.progress {
		state.mu.Lock()
		if state.bench != nil && !state.benchFrozen {
			if s.isPeerTransferring(peerID) {
				state.benchSnap = state.bench.Tick(now, state.sentBytes, s.manifest.TotalBytes)
			}
		}
		snaps[peerID] = state.benchSnap
		state.mu.Unlock()
	}
	s.progressMu.Unlock()
	s.paramsMu.Lock()
	s.benchAggregate = bench.Aggregate(snaps)
	s.paramsMu.Unlock()
}

func (s *SnapshotSender) isPeerTransferring(peerID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	state := s.receivers[peerID]
	return state != nil && state.Status == ReceiverStatusTransferring
}

func (s *SnapshotSender) setTransportLines(summary string, lines []string) {
	s.paramsMu.Lock()
	s.transportSummary = summary
	s.transportLines = append([]string(nil), lines...)
	if s.verbose && !s.tuneTTY && !s.transportLogged && summary != "" {
		fmt.Fprintln(os.Stdout, summary)
		for _, line := range lines {
			fmt.Fprintln(os.Stdout, line)
		}
		s.transportLogged = true
	}
	s.paramsMu.Unlock()
}

func (s *SnapshotSender) transportHeaderLines() []string {
	s.paramsMu.RLock()
	defer s.paramsMu.RUnlock()
	if s.transportSummary == "" {
		return nil
	}
	lines := make([]string, 0, 1+len(s.transportLines))
	lines = append(lines, s.transportSummary)
	lines = append(lines, s.transportLines...)
	return lines
}

type senderProgress struct {
	meter         *progress.Meter
	perFile       map[string]int64
	totals        map[string]int64
	route         string
	stage         string
	probes        map[string]ice.ProbeState
	appliedSkip   map[string]bool
	verifySeen    map[string]bool
	verified      map[string]bool
	skipOffset    map[string]int64
	verifyTotal   int
	verifyDone    int
	pendingVerify map[string]int64
	resumedFiles  int
	sentBytes     int64
	fileDone      int
	fileTotal     int
	connCount     int
	bench         *bench.Bench
	benchSnap     bench.Snapshot
	benchFrozen   bool
	mu            sync.Mutex
}

func (s *SnapshotSender) initSenderProgress(peerID string, totalBytes int64) *senderProgress {
	s.progressMu.Lock()
	state := s.progress[peerID]
	if state == nil {
		state = &senderProgress{
			meter:   progress.NewMeter(),
			perFile: make(map[string]int64),
		}
		s.progress[peerID] = state
	}
	s.progressMu.Unlock()

	state.mu.Lock()
	state.perFile = make(map[string]int64)
	state.totals = make(map[string]int64)
	state.route = ""
	state.stage = ""
	state.probes = make(map[string]ice.ProbeState)
	state.appliedSkip = make(map[string]bool)
	state.verifySeen = make(map[string]bool)
	state.verified = make(map[string]bool)
	state.skipOffset = make(map[string]int64)
	state.verifyTotal = 0
	state.verifyDone = 0
	state.pendingVerify = make(map[string]int64)
	state.resumedFiles = 0
	state.sentBytes = 0
	state.fileDone = 0
	state.fileTotal = s.manifest.FileCount
	state.connCount = 0
	if s.benchmark {
		state.bench = bench.NewBench()
		state.benchSnap = bench.Snapshot{}
		state.benchFrozen = false
	}
	state.mu.Unlock()
	state.meter.Start(totalBytes)

	return state
}

func (s *SnapshotSender) updateSenderProgress(state *senderProgress, relpath string, bytes int64, total int64) {
	if state == nil || relpath == "" {
		return
	}
	state.mu.Lock()
	offset := state.skipOffset[relpath]
	effective := bytes + offset
	if total > 0 {
		state.totals[relpath] = total
		if effective > total {
			effective = total
		}
	}
	prev := state.perFile[relpath]
	if effective > prev {
		delta := effective - prev
		state.meter.Add(int(delta))
		state.sentBytes += delta
	}
	if effective > prev {
		state.perFile[relpath] = effective
	} else {
		state.perFile[relpath] = prev
	}
	state.mu.Unlock()
}

func (s *SnapshotSender) updateSenderStats(state *senderProgress, completed int) {
	if state == nil {
		return
	}
	state.mu.Lock()
	state.fileDone = completed
	if state.fileTotal == 0 {
		state.fileTotal = s.manifest.FileCount
	}
	state.mu.Unlock()
}

func (s *SnapshotSender) addSenderSkipped(state *senderProgress, relpath string, skippedBytes int64, verifyBytes int64) {
	if state == nil || relpath == "" || skippedBytes <= 0 {
		return
	}
	state.mu.Lock()
	if state.appliedSkip[relpath] {
		state.mu.Unlock()
		return
	}
	state.appliedSkip[relpath] = true
	if skippedBytes > state.skipOffset[relpath] {
		state.skipOffset[relpath] = skippedBytes
	}
	state.resumedFiles++
	state.meter.Advance(int(skippedBytes))
	if state.perFile[relpath] < skippedBytes {
		state.perFile[relpath] = skippedBytes
	}
	if !state.verifySeen[relpath] {
		state.verifySeen[relpath] = true
		state.verifyTotal++
	}
	if verifyBytes > 0 {
		state.pendingVerify[relpath] = verifyBytes
		state.meter.AddTotal(verifyBytes)
	}
	state.mu.Unlock()
}

func (s *SnapshotSender) markSenderVerified(state *senderProgress, relpath string, ok bool) {
	if state == nil || relpath == "" {
		return
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if !ok {
		return
	}
	if pending, ok := state.pendingVerify[relpath]; ok && pending > 0 {
		state.meter.Advance(int(pending))
		delete(state.pendingVerify, relpath)
	}
	if !state.verified[relpath] && state.verifySeen[relpath] {
		state.verified[relpath] = true
		state.verifyDone++
	}
	if state.verifyTotal > 0 &&
		state.verifyDone >= state.verifyTotal &&
		len(state.pendingVerify) == 0 {
		// no-op; verification state not shown in UI
	}
}

func computeVerifyBytes(totalBytes int64, totalChunks uint32, chunkSize uint32) int64 {
	if totalBytes <= 0 || totalChunks == 0 || chunkSize == 0 {
		return 0
	}
	fullChunks := int64(totalChunks - 1)
	lastSize := totalBytes - fullChunks*int64(chunkSize)
	if lastSize <= 0 || lastSize > int64(chunkSize) {
		lastSize = int64(chunkSize)
		if totalBytes < lastSize {
			lastSize = totalBytes
		}
	}
	return lastSize
}

func isErrorEvent(args ...any) bool {
	for i := 0; i+1 < len(args); i += 2 {
		key, ok := args[i].(string)
		if !ok || key != "level" {
			continue
		}
		val, ok := args[i+1].(string)
		return ok && val == "error"
	}
	return false
}

func (s *SnapshotSender) setSenderRoute(peerID string, route string) {
	s.progressMu.Lock()
	state := s.progress[peerID]
	s.progressMu.Unlock()
	if state == nil {
		return
	}
	state.mu.Lock()
	state.route = route
	state.mu.Unlock()
}

func (s *SnapshotSender) setSenderStage(peerID string, stage string) {
	s.progressMu.Lock()
	state := s.progress[peerID]
	s.progressMu.Unlock()
	if state == nil {
		return
	}
	state.mu.Lock()
	state.stage = stage
	state.mu.Unlock()
}

func (s *SnapshotSender) setSenderConnCount(peerID string, count int) {
	s.progressMu.Lock()
	state := s.progress[peerID]
	s.progressMu.Unlock()
	if state == nil {
		return
	}
	state.mu.Lock()
	state.connCount = count
	state.mu.Unlock()
}

func (s *SnapshotSender) setSenderProbeStatus(peerID string, addr string, state ice.ProbeState) {
	s.progressMu.Lock()
	progressState := s.progress[peerID]
	s.progressMu.Unlock()
	if progressState == nil {
		return
	}
	progressState.mu.Lock()
	if progressState.probes[addr] == ice.ProbeStateWon {
		progressState.mu.Unlock()
		return
	}
	progressState.probes[addr] = state
	progressState.mu.Unlock()
}

func (s *SnapshotSender) senderSentBytes(peerID string) int64 {
	s.progressMu.Lock()
	state := s.progress[peerID]
	s.progressMu.Unlock()
	if state == nil {
		return 0
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	return state.sentBytes
}

// Auto-tuning removed; parameters are heuristic-driven.

func formatTuneParams(p perfParams) string {
	perConn := 0
	if p.ParallelConnections > 0 {
		perConn = (p.ParallelStreams + p.ParallelConnections - 1) / p.ParallelConnections
	}
	return fmt.Sprintf("Performance: chunk_size=%s total_streams=%d (per_conn=%d) total_connections=%d",
		formatMiB(p.ChunkSize),
		p.ParallelStreams,
		perConn,
		p.ParallelConnections,
	)
}

func formatMiB(bytes int) string {
	if bytes <= 0 {
		return "0B"
	}
	const mib = 1024 * 1024
	if bytes%mib == 0 {
		return fmt.Sprintf("%dMiB", bytes/mib)
	}
	return fmt.Sprintf("%dB", bytes)
}

func (s *SnapshotSender) senderView() progress.SenderView {
	s.mu.Lock()
	peerIDs := make([]string, 0, len(s.receivers))
	statuses := make(map[string]string, len(s.receivers))
	for peerID, state := range s.receivers {
		peerIDs = append(peerIDs, peerID)
		statuses[peerID] = state.Status
	}
	s.mu.Unlock()

	sort.Strings(peerIDs)
	rows := make([]progress.SenderRow, 0, len(peerIDs))
	for _, peerID := range peerIDs {
		var stats progress.Stats
		var route string
		var stage string
		var probes map[string]string
		var benchSnap bench.Snapshot
		var fileDone int
		var fileTotal int
		var resumedFiles int
		var connCount int
		status := statuses[peerID]
		s.progressMu.Lock()
		state := s.progress[peerID]
		s.progressMu.Unlock()
		if state != nil {
			stats = state.meter.Snapshot()
			state.mu.Lock()
			route = state.route
			stage = state.stage
			if len(state.probes) > 0 {
				probes = make(map[string]string, len(state.probes))
				for k, v := range state.probes {
					probes[k] = v.String()
				}
			}
			benchSnap = state.benchSnap
			fileDone = state.fileDone
			fileTotal = state.fileTotal
			resumedFiles = state.resumedFiles
			connCount = state.connCount
			state.mu.Unlock()
		} else {
			stats = progress.Stats{Total: s.manifest.TotalBytes}
		}
		rows = append(rows, progress.SenderRow{
			Peer:      shortPeerID(peerID),
			Status:    status,
			Stats:     stats,
			Bench:     benchSnap,
			Route:     route,
			Stage:     stage,
			FileDone:  fileDone,
			FileTotal: fileTotal,
			Resumed:   resumedFiles,
			Probes:    probes,
			ConnCount: connCount,
		})
	}
	s.mu.Lock()
	header := ""
	if s.verbose {
		header = s.snapshotLine
	}
	s.mu.Unlock()

	if s.verbose {
		if tune := s.tuneHeaderLine(); tune != "" {
			if header != "" {
				header += "\n"
			}
			header += tune
		}
	}
	if s.verbose && s.tuneTTY {
		if lines := s.transportHeaderLines(); len(lines) > 0 {
			if header != "" {
				header = header + "\n"
			}
			header = header + strings.Join(lines, "\n")
		}
	}
	return progress.SenderView{
		Header:    header,
		Rows:      rows,
		Benchmark: s.benchmark,
	}
}

func (s *SnapshotSender) freezeBench(peerID string) {
	s.progressMu.Lock()
	state := s.progress[peerID]
	s.progressMu.Unlock()
	if state == nil {
		return
	}
	state.mu.Lock()
	if state.bench != nil && !state.benchFrozen {
		state.benchSnap = state.bench.Tick(time.Now(), state.sentBytes, s.manifest.TotalBytes)
		state.benchFrozen = true
	}
	state.mu.Unlock()
}

func (s *SnapshotSender) maybePrintBenchSummary() {
	if !s.benchmark || s.benchSummaryLogged {
		return
	}

	s.mu.Lock()
	active := len(s.active)
	queued := len(s.queue)
	s.mu.Unlock()
	if active > 0 || queued > 0 {
		return
	}

	now := time.Now()
	var (
		receiverSummaries []string
	)

	s.progressMu.Lock()
	for peerID, state := range s.progress {
		state.mu.Lock()
		if state.bench == nil {
			state.mu.Unlock()
			continue
		}
		summary := state.bench.Final(now, state.sentBytes, s.manifest.TotalBytes)
		route := routeNetwork(state.route)
		tuned := formatTuneParams(s.getParams())
		label := shortPeerID(peerID)
		receiverSummaries = append(receiverSummaries, benchSummaryLine(label, summary, route, tuned))
		state.mu.Unlock()
	}
	s.progressMu.Unlock()

	if len(receiverSummaries) == 0 {
		return
	}

	for _, line := range receiverSummaries {
		fmt.Fprintln(os.Stdout, line)
	}
	s.benchSummaryLogged = true
}

func (s *SnapshotSender) ForceComplete(peerID string) {
	s.progressMu.Lock()
	state := s.progress[peerID]
	s.progressMu.Unlock()
	if state == nil {
		return
	}
	stats := state.meter.Snapshot()
	remaining := s.manifest.TotalBytes - stats.BytesDone
	if remaining <= 0 {
		return
	}
	state.meter.Advance(int(remaining))
}

func shortPeerID(peerID string) string {
	if len(peerID) <= 8 {
		return peerID
	}
	return peerID[:8]
}

func hashManifestJSON(m manifest.Manifest) (string, error) {
	data, err := json.Marshal(m)
	if err != nil {
		return "", fmt.Errorf("marshal manifest: %w", err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

type pathTarget struct {
	abs   string
	isDir bool
}

func buildPathResolver(paths []string) (func(relPath string) string, error) {
	if len(paths) == 0 {
		return func(string) string { return "" }, nil
	}

	baseNameCount := make(map[string]int)

	absPaths := make([]string, 0, len(paths))
	for _, path := range paths {
		absPath, err := filepath.Abs(path)
		if err != nil {
			return nil, fmt.Errorf("cannot get absolute path for %s: %w", path, err)
		}
		absPaths = append(absPaths, absPath)

		baseName := filepath.Base(absPath)
		if baseName == "." || baseName == "/" {
			if baseName == "." {
				baseName = "current"
			} else {
				baseName = "root"
			}
		}
		baseNameCount[baseName]++
	}

	targets := make(map[string]pathTarget, len(absPaths))
	for i, absPath := range absPaths {
		info, err := os.Stat(absPath)
		if err != nil {
			return nil, fmt.Errorf("cannot access path %s: %w", absPath, err)
		}

		baseName := filepath.Base(absPath)
		if baseName == "." || baseName == "/" {
			if baseName == "." {
				baseName = "current"
			} else {
				baseName = "root"
			}
		}

		prefix := ""
		if baseNameCount[baseName] > 1 {
			ordinal := 0
			for j := 0; j < i; j++ {
				otherBase := filepath.Base(absPaths[j])
				if otherBase == "." || otherBase == "/" {
					if otherBase == "." {
						otherBase = "current"
					} else {
						otherBase = "root"
					}
				}
				if otherBase == baseName {
					ordinal++
				}
			}
			prefix = fmt.Sprintf("%d_", ordinal+1)
		}

		key := prefix + baseName
		targets[key] = pathTarget{abs: absPath, isDir: info.IsDir()}
	}

	return func(relPath string) string {
		parts := strings.SplitN(relPath, "/", 2)
		key := parts[0]
		target, ok := targets[key]
		if !ok {
			return ""
		}
		if len(parts) == 1 || parts[1] == "" {
			return target.abs
		}
		if !target.isDir {
			return ""
		}
		return filepath.Join(target.abs, filepath.FromSlash(parts[1]))
	}, nil
}
