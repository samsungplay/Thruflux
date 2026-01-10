package app

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/sheerbytes/sheerbytes/internal/clienthttp"
	"github.com/sheerbytes/sheerbytes/internal/ice"
	"github.com/sheerbytes/sheerbytes/internal/quictransport"
	"github.com/sheerbytes/sheerbytes/internal/transfer"
	"github.com/sheerbytes/sheerbytes/internal/transferquic"
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

// SnapshotSenderConfig configures the snapshot sender.
type SnapshotSenderConfig struct {
	ServerURL    string
	Paths        []string
	MaxReceivers int
	ReceiverTTL  time.Duration
	TransferOpts transfer.Options
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

	conn      *wsclient.Conn
	receivers map[string]*ReceiverState
	queue     []string
	active    map[string]*transferSlot
	signalCh  map[string]chan protocol.Envelope
	mu        sync.Mutex
	now       func() time.Time
	onChange  func()
	exitFn    func(int)
	closeConn func()
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
		logger:       logger,
		serverURL:    cfg.ServerURL,
		paths:        cfg.Paths,
		maxRecv:      cfg.MaxReceivers,
		receiverTTL:  cfg.ReceiverTTL,
		transferOpts: cfg.TransferOpts,
		peerID:       peerID,
		sessionID:    sessionID,
		joinCode:     joinCode,
		manifest:     m,
		manifestID:   manifestID,
		summary:      summary,
		conn:         conn,
		receivers:    make(map[string]*ReceiverState),
		active:       make(map[string]*transferSlot),
		signalCh:     make(map[string]chan protocol.Envelope),
		now:          time.Now,
		exitFn:       os.Exit,
	}
	s.transferFn = s.runICEQUICTransfer
	s.transferOpts.ResolveFilePath = resolver
	s.closeConn = func() { conn.Close() }
	s.onChange = s.logSnapshotState

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
		fmt.Printf("transfer complete peer=%s\n", peerID)
	}

	s.maybeStartTransfers(ctx)
}

func (s *SnapshotSender) runICEQUICTransfer(ctx context.Context, peerID string) error {
	signalCh := s.getSignalCh(peerID)
	if signalCh == nil {
		return fmt.Errorf("no signal channel for %s", peerID)
	}

	iceLog := func(stage string) {
		fmt.Printf("ice sender peer=%s stage=%s\n", peerID, stage)
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
	fmt.Printf("ice sender peer=%s stage=local_candidates_sent count=%d\n", peerID, len(localCandidates))

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
			fmt.Printf("ice sender peer=%s stage=remote_candidates_received count=%d\n", peerID, len(cands))
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
	logICEPair("sender", peerID, icePeer)

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

	quicConn, err := quictransport.Dial(ctx, udpConn, remoteAddr, s.logger)
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
	progress := newProgressTicker("sender", peerID, s.manifest.TotalBytes)
	defer progress.Stop()
	opts.ProgressFn = func(relpath string, bytesSent int64, total int64) {
		progress.Update(relpath, bytesSent)
	}
	opts.TransferStatsFn = func(activeFiles, completedFiles int, remainingBytes int64) {
		progress.UpdateStats(activeFiles, completedFiles)
	}
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

	fmt.Printf("Snapshot %s | total=%d queued=%d active=%d done=%d failed=%d\n", s.manifestID, total, queued, active, done, failed)
}

func (s *SnapshotSender) transferOptions() transfer.Options {
	opts := s.transferOpts
	opts.Resume = true
	return opts
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
