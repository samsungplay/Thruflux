package app

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sheerbytes/sheerbytes/internal/clienthttp"
	"github.com/sheerbytes/sheerbytes/internal/ice"
	"github.com/sheerbytes/sheerbytes/internal/perf"
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

// ReceiverState tracks the state of a receiver peer.
type ReceiverState struct {
	PeerID   string
	JoinedAt time.Time
	LastSeen time.Time
	Status   string
}

// FixedTuneParams specifies which tuning knobs are fixed by user flags.
type FixedTuneParams = perf.FixedParams

// SnapshotSenderConfig configures the snapshot sender.
type SnapshotSenderConfig struct {
	ServerURL              string
	Paths                  []string
	MaxReceivers           int
	ReceiverTTL            time.Duration
	TransferOpts           transfer.Options
	AutoTune               bool
	FixedTune              FixedTuneParams
	UDPReadBufferBytes     int
	UDPWriteBufferBytes    int
	QuicConnWindowBytes    int
	QuicStreamWindowBytes  int
	QuicMaxIncomingStreams int
}

// SnapshotSender orchestrates snapshot transfer scheduling.
type SnapshotSender struct {
	logger       *slog.Logger
	serverURL    string
	paths        []string
	maxRecv      int
	receiverTTL  time.Duration
	transferOpts transfer.Options
	transferFn   func(context.Context, string) error

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
	params                 perf.Params
	autoTune               bool
	autoTuneOnce           sync.Once
	autoTuneDone           bool
	fixedTune              perf.FixedParams
	udpReadBufferBytes     int
	udpWriteBufferBytes    int
	quicConnWindowBytes    int
	quicStreamWindowBytes  int
	quicMaxIncomingStreams int
	transportSummary       string
	transportLines         []string
	transportLogged        bool
	now                    func() time.Time
	onChange               func()
	exitFn                 func(int)
	closeConn              func()
}

type transferSlot struct {
	peerID  string
	cancel  context.CancelFunc
	closeFn func()
}

// RunSnapshotSender runs the snapshot sender flow.
func RunSnapshotSender(ctx context.Context, logger *slog.Logger, cfg SnapshotSenderConfig) error {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(os.Stderr, nil))
	}
	if len(cfg.Paths) == 0 {
		return fmt.Errorf("no input paths provided")
	}
	if cfg.MaxReceivers <= 0 {
		cfg.MaxReceivers = 4
	}
	if cfg.ReceiverTTL <= 0 {
		cfg.ReceiverTTL = 10 * time.Minute
	}
	if cfg.UDPReadBufferBytes <= 0 {
		cfg.UDPReadBufferBytes = 8 * 1024 * 1024
	}
	if cfg.UDPWriteBufferBytes <= 0 {
		cfg.UDPWriteBufferBytes = 8 * 1024 * 1024
	}
	if cfg.QuicConnWindowBytes <= 0 {
		cfg.QuicConnWindowBytes = 64 * 1024 * 1024
	}
	if cfg.QuicStreamWindowBytes <= 0 {
		cfg.QuicStreamWindowBytes = 32 * 1024 * 1024
	}
	if cfg.QuicMaxIncomingStreams <= 0 {
		cfg.QuicMaxIncomingStreams = 256
	}

	m, err := manifest.ScanPaths(cfg.Paths)
	if err != nil {
		return fmt.Errorf("failed to scan paths: %w", err)
	}
	resolver, err := buildPathResolver(cfg.Paths)
	if err != nil {
		return err
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
	sessionID, joinCode, _, err := clienthttp.CreateSession(ctx, cfg.ServerURL)
	if err != nil {
		return fmt.Errorf("failed to create session: %w", err)
	}

	fmt.Printf("\n=== Join Code: %s ===\n\n", joinCode)

	wsURL, err := buildWebSocketURL(cfg.ServerURL, joinCode, peerID, "sender")
	if err != nil {
		return err
	}
	conn, err := wsclient.Dial(ctx, wsURL, logger)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}

	s := &SnapshotSender{
		logger:                 logger,
		serverURL:              cfg.ServerURL,
		paths:                  cfg.Paths,
		maxRecv:                cfg.MaxReceivers,
		receiverTTL:            cfg.ReceiverTTL,
		transferOpts:           cfg.TransferOpts,
		autoTune:               cfg.AutoTune,
		fixedTune:              cfg.FixedTune,
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
		conn:                   conn,
		receivers:              make(map[string]*ReceiverState),
		active:                 make(map[string]*transferSlot),
		signalCh:               make(map[string]chan protocol.Envelope),
		progress:               make(map[string]*senderProgress),
		now:                    time.Now,
		exitFn:                 os.Exit,
	}
	s.transferFn = s.runICEQUICTransfer
	s.transferOpts.ResolveFilePath = resolver
	s.closeConn = func() { conn.Close() }
	s.onChange = s.logSnapshotState
	s.logSnapshotState()
	s.tuneTTY = progress.IsTTY(os.Stdout)
	s.initParams()

	uiStop := progress.RenderSender(ctx, os.Stdout, s.senderView)
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
		s.handleManifestAccept(env.From)
		s.maybeStartTransfers(ctx)

	case protocol.TypePeerLeft:
		var peerLeft protocol.PeerLeft
		if err := env.DecodePayload(&peerLeft); err != nil {
			s.logger.Error("failed to decode peer_left", "error", err)
			return
		}
		s.handlePeerLeft(peerLeft.PeerID)

	case protocol.TypeIceCredentials, protocol.TypeIceCandidates, protocol.TypeIceCandidate:
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

func (s *SnapshotSender) handleManifestAccept(peerID string) {
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
		}
	}
	delete(s.active, peerID)
	delete(s.signalCh, peerID)
	queuedMsgs = s.collectQueuedUpdatesLocked()
	s.mu.Unlock()
	s.emitChange()
	s.sendQueuedUpdates(queuedMsgs)

	if err != nil {
		s.logger.Error("transfer failed", "peer_id", peerID, "error", err)
	} else {
		s.ForceComplete(peerID)
	}

	s.maybeStartTransfers(ctx)
}

func (s *SnapshotSender) runICEQUICTransfer(ctx context.Context, peerID string) error {
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

	iceCfg := ice.ICEConfig{
		StunServers: []string{"stun:stun.l.google.com:19302"},
		Lite:        false,
	}
	icePeer, err := ice.NewICEPeer(iceCfg, s.logger)
	if err != nil {
		return fmt.Errorf("failed to create ICE peer: %w", err)
	}
	defer icePeer.Close()

	attemptCtx, attemptCancel := context.WithCancel(ctx)
	defer attemptCancel()

	var localCandidates []string
	icePeer.OnLocalCandidate(func(c string) {
		localCandidates = append(localCandidates, c)
	})

	iceLog("gather_start")
	if err := icePeer.StartGathering(attemptCtx); err != nil {
		return fmt.Errorf("failed to start gathering: %w", err)
	}
	select {
	case <-icePeer.GatheringDone():
		iceLog("gather_complete")
	case <-time.After(10 * time.Second):
		return fmt.Errorf("timeout waiting for candidate gathering")
	}

	ufrag, pwd := icePeer.LocalCredentials()
	if err := sendSignal(protocol.TypeIceCredentials, protocol.IceCredentials{Ufrag: ufrag, Pwd: pwd}); err != nil {
		return fmt.Errorf("failed to send credentials: %w", err)
	}
	iceLog("local_creds_sent")
	if err := sendSignal(protocol.TypeIceCandidates, protocol.IceCandidates{Candidates: localCandidates}); err != nil {
		return fmt.Errorf("failed to send candidates: %w", err)
	}
	s.setSenderStage(peerID, fmt.Sprintf("local_candidates_sent count=%d", len(localCandidates)))

	remoteCredsCh := make(chan protocol.IceCredentials, 1)
	remoteCandsCh := make(chan []string, 1)

	readCtx, readCancel := context.WithCancel(attemptCtx)
	readErr := make(chan error, 1)
	go func() {
		for {
			select {
			case <-readCtx.Done():
				return
			case env := <-signalCh:
				switch env.Type {
				case protocol.TypeIceCredentials:
					var creds protocol.IceCredentials
					if err := env.DecodePayload(&creds); err != nil {
						readErr <- err
						return
					}
					select {
					case remoteCredsCh <- creds:
					default:
					}
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

	var remoteCreds *protocol.IceCredentials
	var remoteCands []string
	waitDeadline := time.After(10 * time.Second)
	for remoteCreds == nil || remoteCands == nil {
		select {
		case <-attemptCtx.Done():
			readCancel()
			return attemptCtx.Err()
		case err := <-readErr:
			readCancel()
			return err
		case creds := <-remoteCredsCh:
			remoteCreds = &creds
			iceLog("remote_creds_received")
		case cands := <-remoteCandsCh:
			remoteCands = cands
			s.setSenderStage(peerID, fmt.Sprintf("remote_candidates_received count=%d", len(cands)))
		case <-waitDeadline:
			readCancel()
			return fmt.Errorf("timeout waiting for remote ICE data")
		}
	}

	if err := icePeer.AddRemoteCredentials(remoteCreds.Ufrag, remoteCreds.Pwd); err != nil {
		readCancel()
		return err
	}
	for _, cand := range remoteCands {
		_ = icePeer.AddRemoteCandidate(cand)
	}

	iceLog("connect_start")
	connectCtx, connectCancel := context.WithTimeout(attemptCtx, 10*time.Second)
	iceConn, err := icePeer.Connect(connectCtx)
	connectCancel()
	readCancel()
	if err != nil {
		return fmt.Errorf("failed to establish ICE connection: %w", err)
	}
	iceLog("connect_ok")
	if route := iceRouteString("sender", peerID, icePeer); route != "" {
		s.setSenderRoute(peerID, route)
	}

	_, remoteAddr, err := icePeer.PacketConnInfo()
	if err != nil {
		iceConn.Close()
		return fmt.Errorf("failed to get PacketConn info: %w", err)
	}
	iceConn.Close()

	udpConn, err := icePeer.CreatePacketConn()
	if err != nil {
		return fmt.Errorf("failed to create PacketConn: %w", err)
	}
	defer udpConn.Close()

	udpTune := transport.ApplyUDPBeyondBestEffort(nil, s.udpReadBufferBytes, s.udpWriteBufferBytes)
	if udpTyped, ok := udpConn.(*net.UDPConn); ok {
		udpTune = transport.ApplyUDPBeyondBestEffort(udpTyped, s.udpReadBufferBytes, s.udpWriteBufferBytes)
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

	quicConn, err := quictransport.DialWithConfig(ctx, udpConn, remoteAddr, s.logger, quicCfg)
	if err != nil {
		return fmt.Errorf("failed to dial QUIC connection: %w", err)
	}
	defer quicConn.CloseWithError(0, "")

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

	opts := s.transferOptions()
	var stallCount int64
	stallReset := func() {
		atomic.StoreInt64(&stallCount, 0)
	}
	stallCheck := func() bool {
		return atomic.LoadInt64(&stallCount) > 0
	}
	opts.ReadStallFn = func(time.Duration) {
		atomic.AddInt64(&stallCount, 1)
	}
	opts.ResumeStatsFn = func(relpath string, skippedChunks, totalChunks uint32, verifiedChunk uint32, totalBytes int64, chunkSize uint32) {
		if skippedChunks == 0 || totalChunks == 0 {
			return
		}
		skippedBytes := int64(skippedChunks) * int64(chunkSize)
		if totalBytes > 0 && skippedBytes > totalBytes {
			skippedBytes = totalBytes
		}
		s.addSenderSkipped(progressState, relpath, skippedBytes)
	}
	opts.ProgressFn = func(relpath string, bytesSent int64, total int64) {
		s.updateSenderProgress(progressState, relpath, bytesSent)
	}
	tuneCtx, tuneCancel := context.WithCancel(ctx)
	defer tuneCancel()
	s.maybeStartAutoTune(tuneCtx, peerID, stallReset, stallCheck)
	if err := transfer.SendManifestMultiStream(ctx, transferConn, ".", s.manifest, opts); err != nil {
		return err
	}

	return nil
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
		ManifestID:     s.manifestID,
		SenderPeerID:   s.peerID,
		ReceiverPeerID: peerID,
		TransferID:     randomTransferID(),
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
	case protocol.TypeIceCredentials, protocol.TypeIceCandidates, protocol.TypeIceCandidate:
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
	runtime := transfer.NormalizeParams(transfer.RuntimeParams{
		ChunkSize:     s.transferOpts.ChunkSize,
		WindowSize:    s.transferOpts.WindowSize,
		ReadAhead:     s.transferOpts.ReadAhead,
		ParallelFiles: s.transferOpts.ParallelFiles,
	}, s.transferOpts)
	s.paramsMu.Lock()
	s.params = perf.Params{
		ChunkSize:     int(runtime.ChunkSize),
		Window:        int(runtime.WindowSize),
		ReadAhead:     int(runtime.ReadAhead),
		ParallelFiles: runtime.ParallelFiles,
	}
	s.paramsMu.Unlock()
}

func (s *SnapshotSender) runtimeParams() transfer.RuntimeParams {
	s.paramsMu.RLock()
	params := s.params
	s.paramsMu.RUnlock()
	return transfer.RuntimeParams{
		ChunkSize:     uint32(params.ChunkSize),
		WindowSize:    uint32(params.Window),
		ReadAhead:     uint32(params.ReadAhead),
		ParallelFiles: params.ParallelFiles,
	}
}

func (s *SnapshotSender) setParams(params perf.Params) {
	s.paramsMu.Lock()
	s.params = params
	s.paramsMu.Unlock()
}

func (s *SnapshotSender) getParams() perf.Params {
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

func (s *SnapshotSender) setTransportLines(summary string, lines []string) {
	s.paramsMu.Lock()
	s.transportSummary = summary
	s.transportLines = append([]string(nil), lines...)
	if !s.tuneTTY && !s.transportLogged && summary != "" {
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
	meter       *progress.Meter
	perFile     map[string]int64
	route       string
	stage       string
	appliedSkip map[string]bool
	sentBytes   int64
	mu          sync.Mutex
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
	state.route = ""
	state.stage = ""
	state.appliedSkip = make(map[string]bool)
	state.sentBytes = 0
	state.mu.Unlock()
	state.meter.Start(totalBytes)

	return state
}

func (s *SnapshotSender) updateSenderProgress(state *senderProgress, relpath string, bytes int64) {
	if state == nil || relpath == "" {
		return
	}
	state.mu.Lock()
	prev := state.perFile[relpath]
	if bytes > prev {
		delta := bytes - prev
		state.meter.Add(int(delta))
		state.sentBytes += delta
	}
	state.perFile[relpath] = bytes
	state.mu.Unlock()
}

func (s *SnapshotSender) addSenderSkipped(state *senderProgress, relpath string, skippedBytes int64) {
	if state == nil || relpath == "" || skippedBytes <= 0 {
		return
	}
	state.mu.Lock()
	if state.appliedSkip[relpath] {
		state.mu.Unlock()
		return
	}
	state.appliedSkip[relpath] = true
	state.mu.Unlock()

	maxInt := int64(^uint(0) >> 1)
	for skippedBytes > 0 {
		step := skippedBytes
		if step > maxInt {
			step = maxInt
		}
		state.meter.Add(int(step))
		skippedBytes -= step
	}
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

func (s *SnapshotSender) maybeStartAutoTune(ctx context.Context, peerID string, stallReset func(), stallCheck func() bool) {
	if !s.autoTune {
		return
	}
	s.autoTuneOnce.Do(func() {
		go s.runAutoTune(ctx, peerID, stallReset, stallCheck)
	})
}

func (s *SnapshotSender) runAutoTune(ctx context.Context, peerID string, stallReset func(), stallCheck func() bool) {
	initial := s.getParams()
	measurer := func() int64 {
		return s.senderSentBytes(peerID)
	}
	apply := func(p perf.Params) error {
		s.setParams(p)
		return nil
	}
	stopFn := func() bool {
		s.mu.Lock()
		queued := len(s.queue) > 0
		s.mu.Unlock()
		return queued
	}

	cfg := perf.AutoTuneConfig{
		Enabled:          true,
		MaxTime:          10 * time.Second,
		ImproveThreshold: 0.05,
		ProbeDuration:    1 * time.Second,
		Warmup:           200 * time.Millisecond,
		Alpha:            0.2,
		MaxInflightBytes: 256 * 1024 * 1024,
		StopFn:           stopFn,
		StallReset:       stallReset,
		StallCheck:       stallCheck,
		OnState:          s.updateTuneState,
		Fixed:            s.fixedTune,
	}

	_, _ = perf.RunAutoTune(ctx, cfg, measurer, apply, initial, perf.WorkloadInfo{
		NumFiles:   s.manifest.FileCount,
		TotalBytes: s.manifest.TotalBytes,
	})
}

func (s *SnapshotSender) updateTuneState(state perf.AutoTuneState) {
	if state.Done {
		line := fmt.Sprintf("Tuned: %s", formatTuneParams(state.BestParams))
		s.setTuneLine(line)
		s.paramsMu.Lock()
		s.autoTuneDone = true
		s.paramsMu.Unlock()
		if !s.tuneTTY {
			fmt.Fprintln(os.Stdout, "Auto-tuned:", formatTuneParams(state.BestParams))
		}
		return
	}

	line := fmt.Sprintf(
		"Tuning: %s trying %s score=%.1fMB/s best=%.1fMB/s",
		state.Phase,
		formatTuneParams(state.Trying),
		state.ScoreMbps,
		state.BestMbps,
	)
	s.setTuneLine(line)
	if !s.tuneTTY {
		fmt.Fprintf(os.Stdout, "[tune] phase=%s trying=%s score=%.1fMB/s best=%.1fMB/s\n",
			state.Phase,
			formatTuneParams(state.Trying),
			state.ScoreMbps,
			state.BestMbps,
		)
	}
}

func formatTuneParams(p perf.Params) string {
	return fmt.Sprintf("chunk=%s window=%d readahead=%d parallel=%d",
		formatMiB(p.ChunkSize),
		p.Window,
		p.ReadAhead,
		p.ParallelFiles,
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
		s.progressMu.Lock()
		state := s.progress[peerID]
		s.progressMu.Unlock()
		if state != nil {
			stats = state.meter.Snapshot()
			state.mu.Lock()
			route = state.route
			stage = state.stage
			state.mu.Unlock()
		} else {
			stats = progress.Stats{Total: s.manifest.TotalBytes}
		}
		rows = append(rows, progress.SenderRow{
			Peer:   shortPeerID(peerID),
			Status: statuses[peerID],
			Stats:  stats,
			Route:  route,
			Stage:  stage,
		})
	}
	header := s.snapshotLine
	if tune := s.tuneHeaderLine(); tune != "" {
		header = header + "\n" + tune
	}
	if s.tuneTTY {
		if lines := s.transportHeaderLines(); len(lines) > 0 {
			header = header + "\n" + strings.Join(lines, "\n")
		}
	}
	return progress.SenderView{
		Header: header,
		Rows:   rows,
	}
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
	state.meter.Add(int(remaining))
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
