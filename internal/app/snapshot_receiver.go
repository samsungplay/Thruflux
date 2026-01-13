package app

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/sheerbytes/sheerbytes/internal/bench"
	"github.com/sheerbytes/sheerbytes/internal/ice"
	"github.com/sheerbytes/sheerbytes/internal/progress"
	"github.com/sheerbytes/sheerbytes/internal/quictransport"
	"github.com/sheerbytes/sheerbytes/internal/transfer"
	"github.com/sheerbytes/sheerbytes/internal/transferquic"
	"github.com/sheerbytes/sheerbytes/internal/transport"
	"github.com/sheerbytes/sheerbytes/internal/wsclient"
	"github.com/sheerbytes/sheerbytes/pkg/protocol"
)

// SnapshotReceiverConfig configures the snapshot receiver.
type SnapshotReceiverConfig struct {
	ServerURL              string
	JoinCode               string
	OutDir                 string
	Benchmark              bool
	UDPReadBufferBytes     int
	UDPWriteBufferBytes    int
	QuicConnWindowBytes    int
	QuicStreamWindowBytes  int
	QuicMaxIncomingStreams int
	StunServers            []string
	TurnServers            []string
}

// RunSnapshotReceiver runs the snapshot receiver flow.
func RunSnapshotReceiver(ctx context.Context, logger *slog.Logger, cfg SnapshotReceiverConfig) error {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(os.Stderr, nil))
	}
	if cfg.JoinCode == "" {
		return fmt.Errorf("join code required")
	}
	if cfg.OutDir == "" {
		cfg.OutDir = "."
	}
	if cfg.UDPReadBufferBytes <= 0 {
		cfg.UDPReadBufferBytes = 8 * 1024 * 1024
	}
	if cfg.UDPWriteBufferBytes <= 0 {
		cfg.UDPWriteBufferBytes = 8 * 1024 * 1024
	}
	if cfg.QuicConnWindowBytes <= 0 {
		cfg.QuicConnWindowBytes = 1024 * 1024 * 1024
	}
	if cfg.QuicStreamWindowBytes <= 0 {
		cfg.QuicStreamWindowBytes = 32 * 1024 * 1024
	}
	if cfg.QuicMaxIncomingStreams <= 0 {
		cfg.QuicMaxIncomingStreams = 100
	}
	absOut, err := filepath.Abs(cfg.OutDir)
	if err != nil {
		return fmt.Errorf("failed to resolve output dir: %w", err)
	}
	cfg.OutDir = absOut
	if err := os.MkdirAll(cfg.OutDir, 0755); err != nil {
		return fmt.Errorf("failed to create output dir: %w", err)
	}

	peerID := randomPeerID()
	wsURL, err := buildWebSocketURL(cfg.ServerURL, cfg.JoinCode, peerID, "receiver", 0)
	if err != nil {
		return err
	}

	conn, err := wsclient.Dial(ctx, wsURL, logger)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	defer conn.Close()

	s := &snapshotReceiver{
		logger:                 logger,
		conn:                   conn,
		peerID:                 peerID,
		outDir:                 cfg.OutDir,
		benchmark:              cfg.Benchmark,
		udpReadBufferBytes:     cfg.UDPReadBufferBytes,
		udpWriteBufferBytes:    cfg.UDPWriteBufferBytes,
		quicConnWindowBytes:    cfg.QuicConnWindowBytes,
		quicStreamWindowBytes:  cfg.QuicStreamWindowBytes,
		quicMaxIncomingStreams: cfg.QuicMaxIncomingStreams,
		stunServers:            cfg.StunServers,
		turnServers:            cfg.TurnServers,
		signalCh:               make(chan protocol.Envelope, 64),
		transfer:               make(chan protocol.TransferStart, 1),
		sessionID:              "",
	}

	go s.watchInterrupt()

	readErr := conn.ReadLoop(ctx, func(env protocol.Envelope) {
		s.handleEnvelope(env)
	})

	// If the transfer has started, wait for it to complete (it will exit the process).
	// This prevents the program from exiting silently if the WebSocket disconnects
	// while the transfer (running over ICE/QUIC) is still in progress.
	s.wg.Wait()

	if readErr != nil && readErr != context.Canceled {
		return readErr
	}
	return nil
}

type snapshotReceiver struct {
	logger                 *slog.Logger
	conn                   *wsclient.Conn
	peerID                 string
	outDir                 string
	benchmark              bool
	udpReadBufferBytes     int
	udpWriteBufferBytes    int
	quicConnWindowBytes    int
	quicStreamWindowBytes  int
	quicMaxIncomingStreams int
	stunServers            []string
	turnServers            []string
	senderID               string
	manifest               string
	sessionID              string
	totalBytes             int64
	fileTotal              int
	signalCh               chan protocol.Envelope
	transfer               chan protocol.TransferStart
	cleanupMu              sync.Mutex
	uiCleanup              func()
	wg                     sync.WaitGroup
}

func (r *snapshotReceiver) handleEnvelope(env protocol.Envelope) {
	if err := env.ValidateBasic(); err != nil {
		r.logger.Error("invalid envelope", "error", err)
		return
	}
	if r.sessionID == "" && env.SessionID != "" {
		r.sessionID = env.SessionID
	}

	switch env.Type {
	case protocol.TypeManifestOffer:
		var offer protocol.ManifestOffer
		if err := env.DecodePayload(&offer); err != nil {
			r.logger.Error("failed to decode manifest_offer", "error", err)
			return
		}
		r.manifest = offer.Summary.ManifestID
		r.totalBytes = offer.Summary.TotalBytes
		r.fileTotal = offer.Summary.FileCount
		r.senderID = env.From
		r.sendAccept(offer.Summary.ManifestID)
	case protocol.TypePeerLeft:
		var peerLeft protocol.PeerLeft
		if err := env.DecodePayload(&peerLeft); err != nil {
			r.logger.Error("failed to decode peer_left", "error", err)
			return
		}
		if r.senderID != "" && peerLeft.PeerID == r.senderID {
			r.cleanupMu.Lock()
			cleanup := r.uiCleanup
			r.cleanupMu.Unlock()
			if cleanup != nil {
				cleanup()
			}
			fmt.Fprintf(os.Stderr, "\nSender disconnected (peer left).\n")
			os.Exit(1)
		}
	case protocol.TypeTransferStart:
		var start protocol.TransferStart
		if err := env.DecodePayload(&start); err != nil {
			r.logger.Error("failed to decode transfer_start", "error", err)
			return
		}
		r.senderID = start.SenderPeerID
		r.manifest = start.ManifestID
		select {
		case r.transfer <- start:
		default:
		}
		r.wg.Add(1)
		go r.runTransfer(start)
	case protocol.TypeTransferQueued:
		var queued protocol.TransferQueued
		if err := env.DecodePayload(&queued); err != nil {
			r.logger.Error("failed to decode transfer_queued", "error", err)
			return
		}
		if queued.Position > 0 {
			fmt.Printf("Queued for transfer (position %d, active=%d/%d)\n", queued.Position, queued.Active, queued.Max)
		} else {
			fmt.Printf("Queued for transfer (active=%d/%d)\n", queued.Active, queued.Max)
		}
	case protocol.TypeIceCredentials, protocol.TypeIceCandidates, protocol.TypeIceCandidate:
		select {
		case r.signalCh <- env:
		default:
		}
	}
}

func (r *snapshotReceiver) sendAccept(manifestID string) {
	accept := protocol.ManifestAccept{
		ManifestID:    manifestID,
		Mode:          "all",
		SelectedPaths: nil,
	}
	env, err := protocol.NewEnvelope(protocol.TypeManifestAccept, protocol.NewMsgID(), accept)
	if err != nil {
		r.logger.Error("failed to create manifest accept", "error", err)
		return
	}
	env.SessionID = r.sessionID
	env.From = r.peerID
	env.To = r.senderID
	if err := r.conn.Send(env); err != nil {
		r.logger.Error("failed to send manifest accept", "error", err)
		return
	}
}

func (r *snapshotReceiver) runTransfer(start protocol.TransferStart) {
	baseCtx := context.Background()
	progressState := newReceiverProgress(r.totalBytes, r.fileTotal, start.ManifestID, r.outDir, r.benchmark)
	var benchCancel context.CancelFunc
	if r.benchmark {
		var benchCtx context.Context
		benchCtx, benchCancel = context.WithCancel(baseCtx)
		r.startReceiverBenchLoop(benchCtx, progressState)
	}
	stopUI := progress.RenderReceiver(baseCtx, os.Stdout, progressState.View)
	var stopUIOnce sync.Once
	stopUIFn := func() {
		stopUIOnce.Do(stopUI)
	}
	r.cleanupMu.Lock()
	r.uiCleanup = stopUIFn
	r.cleanupMu.Unlock()
	defer stopUIFn()
	exitWith := func(code int) {
		stopUIFn()
		if code != 0 {
			view := progressState.View()
			stats := view.Stats
			r.logger.Error("receiver exiting", "code", code, "ice_stage", view.IceStage, "route", view.Route, "current_file", view.CurrentFile, "bytes_done", stats.BytesDone, "bytes_total", stats.Total, "session_id", r.sessionID, "sender_id", r.senderID)
			fmt.Fprintf(os.Stderr, "receiver exit=%d ice=%s route=%s file=%s bytes=%d/%d session=%s sender=%s\n", code, view.IceStage, view.Route, view.CurrentFile, stats.BytesDone, stats.Total, r.sessionID, r.senderID)
		}
		os.Exit(code)
	}

	iceLog := func(stage string) {
		progressState.SetIceStage(stage)
	}

	sendSignal := func(msgType string, payload any) error {
		env, err := protocol.NewEnvelope(msgType, protocol.NewMsgID(), payload)
		if err != nil {
			return err
		}
		env.SessionID = r.sessionID
		env.From = r.peerID
		env.To = r.senderID
		return r.conn.Send(env)
	}

	iceCfg := ice.ICEConfig{
		StunServers: r.stunServers,
		TurnServers: r.turnServers,
		Lite:        false,
	}
	var (
		icePeer *ice.ICEPeer
		err     error
	)
	drain := func() {
		for {
			select {
			case <-r.signalCh:
			default:
				return
			}
		}
	}
	for attempt := 1; ; attempt++ {
		if attempt > 1 {
			drain()
		}
		attemptCtx, attemptCancel := context.WithCancel(baseCtx)

		icePeer, err = ice.NewICEPeer(iceCfg, r.logger)
		if err != nil {
			attemptCancel()
			r.logger.Error("failed to create ICE peer", "error", err)
			exitWith(1)
		}

		var localCandidates []string
		icePeer.OnLocalCandidate(func(c string) {
			localCandidates = append(localCandidates, c)
		})

		iceLog("gather_start")
		if err := icePeer.StartGathering(attemptCtx); err != nil {
			attemptCancel()
			icePeer.Close()
			r.logger.Error("failed to start gathering", "error", err)
			exitWith(1)
		}
		select {
		case <-icePeer.GatheringDone():
			iceLog("gather_complete")
		case <-time.After(10 * time.Second):
			attemptCancel()
			icePeer.Close()
			progressState.SetIceStage(fmt.Sprintf("restart attempt=%d", attempt+1))
			time.Sleep(200 * time.Millisecond)
			continue
		}

		ufrag, pwd := icePeer.LocalCredentials()
		if err := sendSignal(protocol.TypeIceCredentials, protocol.IceCredentials{Ufrag: ufrag, Pwd: pwd}); err != nil {
			attemptCancel()
			icePeer.Close()
			r.logger.Error("failed to send credentials", "error", err)
			exitWith(1)
		}
		iceLog("local_creds_sent")
		if err := sendSignal(protocol.TypeIceCandidates, protocol.IceCandidates{Candidates: localCandidates}); err != nil {
			attemptCancel()
			icePeer.Close()
			r.logger.Error("failed to send candidates", "error", err)
			exitWith(1)
		}
		progressState.SetIceStage(fmt.Sprintf("local_candidates_sent count=%d", len(localCandidates)))

		remoteCredsCh := make(chan protocol.IceCredentials, 1)
		remoteCandsCh := make(chan []string, 1)

		readCtx, readCancel := context.WithCancel(attemptCtx)
		readErr := make(chan error, 1)
		go func() {
			for {
				select {
				case <-readCtx.Done():
					return
				case env := <-r.signalCh:
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
					}
				}
			}
		}()

		var remoteCreds *protocol.IceCredentials
		var remoteCands []string
		restart := false
		waitDeadline := time.After(10 * time.Second)
		for remoteCreds == nil || remoteCands == nil {
			select {
			case <-attemptCtx.Done():
				readCancel()
				icePeer.Close()
				r.logger.Error("transfer canceled")
				exitWith(1)
			case err := <-readErr:
				readCancel()
				attemptCancel()
				icePeer.Close()
				r.logger.Error("signal error", "error", err)
				exitWith(1)
			case creds := <-remoteCredsCh:
				remoteCreds = &creds
				iceLog("remote_creds_received")
			case cands := <-remoteCandsCh:
				remoteCands = cands
				progressState.SetIceStage(fmt.Sprintf("remote_candidates_received count=%d", len(cands)))
			case <-waitDeadline:
				readCancel()
				attemptCancel()
				icePeer.Close()
				progressState.SetIceStage(fmt.Sprintf("restart attempt=%d", attempt+1))
				time.Sleep(200 * time.Millisecond)
				restart = true
				break
			}
			if restart {
				break
			}
		}
		if restart {
			continue
		}

		if err := icePeer.AddRemoteCredentials(remoteCreds.Ufrag, remoteCreds.Pwd); err != nil {
			readCancel()
			attemptCancel()
			icePeer.Close()
			r.logger.Error("failed to set remote credentials", "error", err)
			exitWith(1)
		}
		for _, cand := range remoteCands {
			_ = icePeer.AddRemoteCandidate(cand)
		}

		iceLog("connect_start")
		acceptCtx, acceptCancel := context.WithTimeout(attemptCtx, 10*time.Second)
		// We don't need the returned conn as icePeer stores it
		_, err = icePeer.Accept(acceptCtx)
		acceptCancel()
		readCancel()
		attemptCancel()
		if err == nil {
			break
		}
		icePeer.Close()
		progressState.SetIceStage(fmt.Sprintf("restart attempt=%d", attempt+1))
		time.Sleep(200 * time.Millisecond)
		continue
	}
	iceLog("connect_ok")
	r.logger.Info("ice connected", "session_id", r.sessionID, "peer_id", r.peerID, "sender_id", r.senderID)
	if route := iceRouteString("receiver", r.peerID, icePeer); route != "" {
		progressState.SetRoute(route)
	}

	_, _, err = icePeer.PacketConnInfo()
	if err != nil {
		r.logger.Error("failed to get PacketConn info", "error", err)
		exitWith(1)
	}
	// Do NOT close iceConn here. We use it for QUIC.
	// iceConn.Close()

	udpConn, err := icePeer.CreatePacketConn()
	if err != nil {
		r.logger.Error("failed to create PacketConn", "error", err)
		exitWith(1)
	}
	defer udpConn.Close()

	udpTune := transport.ApplyUDPBeyondBestEffort(nil, r.udpReadBufferBytes, r.udpWriteBufferBytes)
	if udpTyped, ok := udpConn.(*net.UDPConn); ok {
		udpTune = transport.ApplyUDPBeyondBestEffort(udpTyped, r.udpReadBufferBytes, r.udpWriteBufferBytes)
	}
	quicCfg, quicTune := transport.BuildQuicConfig(
		quictransport.DefaultServerQUICConfig(),
		r.quicConnWindowBytes,
		r.quicStreamWindowBytes,
		r.quicMaxIncomingStreams,
	)
	transportSummary := formatTransportSummary(udpTune, quicTune)
	transportLines := []string{formatUDPTuneLine(udpTune), formatQuicTuneLine(quicTune)}
	progressState.SetTransportLines(append([]string{transportSummary}, transportLines...))
	if !progress.IsTTY(os.Stdout) {
		fmt.Fprintln(os.Stdout, transportSummary)
		for _, line := range transportLines {
			fmt.Fprintln(os.Stdout, line)
		}
	}

	quicListener, err := quictransport.ListenWithConfig(baseCtx, udpConn, r.logger, quicCfg)
	if err != nil {
		r.logger.Error("failed to listen for QUIC", "error", err)
		exitWith(1)
	}
	defer quicListener.Close()
	fmt.Fprintf(os.Stderr, "waiting for QUIC transfer connection (session=%s sender=%s)\n", r.sessionID, r.senderID)

	quicTransport := transferquic.NewListener(quicListener, r.logger)
	defer quicTransport.Close()

	transferConn, err := quicTransport.Accept(baseCtx)
	if err != nil {
		r.logger.Error("failed to accept transfer connection", "error", err)
		exitWith(1)
	}
	defer transferConn.Close()
	fmt.Fprintf(os.Stderr, "accepted QUIC transfer connection (session=%s sender=%s)\n", r.sessionID, r.senderID)

	opts := transfer.Options{
		Resume:    true,
		NoRootDir: true,
		HashAlg:   "crc32c",
		ProgressFn: func(relpath string, bytesReceived int64, total int64) {
			progressState.Update(relpath, bytesReceived, total)
		},
		TransferStatsFn: func(activeFiles, completedFiles int, remainingBytes int64) {
			progressState.UpdateStats(activeFiles, completedFiles)
		},
		ResumeStatsFn: func(relpath string, skippedChunks, totalChunks uint32, verifiedChunk uint32, totalBytes int64, chunkSize uint32) {
			progressState.RecordResume(relpath, skippedChunks, totalChunks, totalBytes, chunkSize)
		},
		FileDoneFn: func(relpath string, ok bool) {
			progressState.MarkVerified(relpath, ok)
		},
	}
	_, err = transfer.RecvManifestMultiStream(baseCtx, transferConn, r.outDir, opts)
	if err != nil {
		stopUIFn()
		fmt.Fprintf(os.Stderr, "transfer failed: %v\n", err)
		fmt.Fprintf(os.Stdout, "transfer failed: %v\n", err)
		r.logger.Error("transfer failed", "error", err)
		if benchCancel != nil {
			benchCancel()
		}
		exitWith(1)
	}

	if r.benchmark {
		if benchCancel != nil {
			benchCancel()
		}
		progressState.FreezeBench(time.Now())
		r.printReceiverBenchSummary(progressState)
	}

	progressState.ForceComplete()
	exitWith(0)
}

type receiverProgress struct {
	meter            *progress.Meter
	perFile          map[string]int64
	totals           map[string]int64
	appliedSkip      map[string]bool
	pendingSkip      map[string]resumeSkip
	verifySeen       map[string]bool
	verified         map[string]bool
	verifyTotal      int
	verifyDone       int
	currentFile      string
	fileDone         int
	fileTotal        int
	route            string
	iceStage         string
	transportLines   []string
	benchmark        bool
	bench            *bench.Bench
	benchSnap        bench.Snapshot
	benchBytes       int64
	verifying        map[string]bool
	pendingSkipBytes map[string]int64
	pendingVerify    map[string]int64
	verifyingActive  bool
	resumedFiles     int
	totalBytes       int64
	snapshotID       string
	outDir           string
	mu               sync.Mutex
}

type resumeSkip struct {
	skippedChunks uint32
	totalChunks   uint32
	chunkSize     uint32
}

func newReceiverProgress(totalBytes int64, fileTotal int, snapshotID string, outDir string, benchmark bool) *receiverProgress {
	meter := progress.NewMeter()
	meter.Start(totalBytes)
	state := &receiverProgress{
		meter:            meter,
		perFile:          make(map[string]int64),
		totals:           make(map[string]int64),
		appliedSkip:      make(map[string]bool),
		pendingSkip:      make(map[string]resumeSkip),
		verifying:        make(map[string]bool),
		verifySeen:       make(map[string]bool),
		verified:         make(map[string]bool),
		pendingSkipBytes: make(map[string]int64),
		pendingVerify:    make(map[string]int64),
		verifyingActive:  false,
		resumedFiles:     0,
		fileTotal:        fileTotal,
		totalBytes:       totalBytes,
		snapshotID:       snapshotID,
		outDir:           outDir,
	}
	if benchmark {
		state.benchmark = true
		state.bench = bench.NewBench()
	}
	return state
}

func (p *receiverProgress) Update(relpath string, bytes int64, total int64) {
	if relpath == "" {
		return
	}
	p.mu.Lock()
	prev := p.perFile[relpath]
	if bytes > prev {
		delta := bytes - prev
		p.addBytesLocked(delta)
		p.benchBytes += delta
	}
	p.perFile[relpath] = bytes
	p.currentFile = relpath
	if total > 0 {
		p.totals[relpath] = total
	}
	if skip, ok := p.pendingSkip[relpath]; ok && !p.appliedSkip[relpath] {
		p.applySkipLocked(relpath, p.totals[relpath], skip)
		delete(p.pendingSkip, relpath)
	}
	p.mu.Unlock()
}

func (p *receiverProgress) UpdateStats(active, completed int) {
	p.mu.Lock()
	p.fileDone = completed
	p.mu.Unlock()
}

func (p *receiverProgress) ForceComplete() {
	p.mu.Lock()
	p.fileDone = p.fileTotal
	p.mu.Unlock()
}

func (p *receiverProgress) SetRoute(route string) {
	p.mu.Lock()
	p.route = route
	p.mu.Unlock()
}

func (p *receiverProgress) SetIceStage(stage string) {
	p.mu.Lock()
	p.iceStage = stage
	p.mu.Unlock()
}

func (p *receiverProgress) SetTransportLines(lines []string) {
	p.mu.Lock()
	p.transportLines = append([]string(nil), lines...)
	p.mu.Unlock()
}

func (p *receiverProgress) TickBench(now time.Time) {
	if !p.benchmark || p.bench == nil {
		return
	}
	p.mu.Lock()
	p.benchSnap = p.bench.Tick(now, p.benchBytes, p.totalBytes)
	p.mu.Unlock()
}

func (p *receiverProgress) FreezeBench(now time.Time) {
	if !p.benchmark || p.bench == nil {
		return
	}
	p.mu.Lock()
	p.benchSnap = p.bench.Tick(now, p.benchBytes, p.totalBytes)
	p.mu.Unlock()
}

func (p *receiverProgress) RecordResume(relpath string, skippedChunks, totalChunks uint32, totalBytes int64, chunkSize uint32) {
	if relpath == "" {
		return
	}
	p.mu.Lock()
	p.currentFile = relpath
	if p.appliedSkip[relpath] {
		p.mu.Unlock()
		return
	}
	if skippedChunks == 0 || totalChunks == 0 {
		p.mu.Unlock()
		return
	}
	p.resumedFiles++
	if totalBytes > 0 {
		p.totals[relpath] = totalBytes
		p.applySkipLocked(relpath, totalBytes, resumeSkip{skippedChunks: skippedChunks, totalChunks: totalChunks, chunkSize: chunkSize})
	} else {
		p.pendingSkip[relpath] = resumeSkip{skippedChunks: skippedChunks, totalChunks: totalChunks, chunkSize: chunkSize}
	}
	p.mu.Unlock()
}

func (p *receiverProgress) applySkipLocked(relpath string, totalBytes int64, skip resumeSkip) {
	if skip.totalChunks == 0 {
		p.pendingSkip[relpath] = skip
		return
	}
	skippedBytes := computeSkippedBytes(totalBytes, skip)
	verifyBytes := computeVerifyBytes(totalBytes, skip.totalChunks, skip.chunkSize)
	if skippedBytes > 0 {
		p.addSkippedLocked(skippedBytes)
	}
	if verifyBytes > 0 {
		p.pendingVerify[relpath] = verifyBytes
		p.meter.AddTotal(verifyBytes)
	}
	if skippedBytes > 0 || verifyBytes > 0 {
		p.appliedSkip[relpath] = true
		if !p.verifySeen[relpath] {
			p.verifySeen[relpath] = true
			p.verifyTotal++
		}
		p.verifyingActive = true
	}
}

func (p *receiverProgress) MarkVerified(relpath string, ok bool) {
	if relpath == "" {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if !ok {
		return
	}
	if pending, ok := p.pendingSkipBytes[relpath]; ok && pending > 0 {
		p.addSkippedLocked(pending)
		delete(p.pendingSkipBytes, relpath)
	}
	if pending, ok := p.pendingVerify[relpath]; ok && pending > 0 {
		p.addSkippedLocked(pending)
		delete(p.pendingVerify, relpath)
	}
	if !p.verified[relpath] && p.verifySeen[relpath] {
		p.verified[relpath] = true
		p.verifyDone++
	}
	if p.verifyTotal > 0 &&
		p.verifyDone >= p.verifyTotal &&
		len(p.pendingSkip) == 0 &&
		len(p.pendingSkipBytes) == 0 &&
		len(p.pendingVerify) == 0 {
		p.verifyingActive = false
	}
}

func (p *receiverProgress) addBytesLocked(n int64) {
	if n <= 0 {
		return
	}
	maxInt := int64(^uint(0) >> 1)
	for n > 0 {
		step := n
		if step > maxInt {
			step = maxInt
		}
		p.meter.Add(int(step))
		n -= step
	}
}

func (p *receiverProgress) addSkippedLocked(n int64) {
	if n <= 0 {
		return
	}
	maxInt := int64(^uint(0) >> 1)
	for n > 0 {
		step := n
		if step > maxInt {
			step = maxInt
		}
		p.meter.Advance(int(step))
		n -= step
	}
}

func (p *receiverProgress) View() progress.ReceiverView {
	p.mu.Lock()
	current := p.currentFile
	done := p.fileDone
	total := p.fileTotal
	route := p.route
	stage := p.iceStage
	transportLines := append([]string(nil), p.transportLines...)
	benchSnap := p.benchSnap
	benchmark := p.benchmark
	snapshotID := p.snapshotID
	outDir := p.outDir
	resumedFiles := p.resumedFiles
	p.mu.Unlock()
	return progress.ReceiverView{
		SnapshotID:     snapshotID,
		OutDir:         outDir,
		IceStage:       stage,
		TransportLines: transportLines,
		Stats:          p.meter.Snapshot(),
		Bench:          benchSnap,
		Benchmark:      benchmark,
		Resumed:        resumedFiles,
		CurrentFile:    current,
		FileDone:       done,
		FileTotal:      total,
		Route:          route,
	}
}

func computeSkippedBytes(totalBytes int64, skip resumeSkip) int64 {
	if totalBytes <= 0 || skip.totalChunks == 0 || skip.skippedChunks == 0 {
		return 0
	}
	chunkSize := int64(skip.chunkSize)
	if chunkSize == 0 {
		chunkSize = (totalBytes + int64(skip.totalChunks) - 1) / int64(skip.totalChunks)
	}
	if skip.skippedChunks == skip.totalChunks {
		return totalBytes
	}
	skippedBytes := int64(skip.skippedChunks) * chunkSize
	if skippedBytes > totalBytes {
		skippedBytes = totalBytes
	}
	return skippedBytes
}

func isErrorEventReceiver(args ...any) bool {
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

func (r *snapshotReceiver) watchInterrupt() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	sig := <-sigChan
	r.logger.Error("received signal, exiting", "signal", sig.String())
	os.Exit(1)
}

func (r *snapshotReceiver) startReceiverBenchLoop(ctx context.Context, progressState *receiverProgress) {
	ticker := time.NewTicker(1 * time.Second)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				progressState.TickBench(now)
			}
		}
	}()
}

func (r *snapshotReceiver) printReceiverBenchSummary(progressState *receiverProgress) {
	now := time.Now()
	progressState.mu.Lock()
	benchInst := progressState.bench
	benchBytes := progressState.benchBytes
	totalBytes := progressState.totalBytes
	route := routeNetwork(progressState.route)
	progressState.mu.Unlock()
	if benchInst == nil {
		return
	}
	summary := benchInst.Final(now, benchBytes, totalBytes)
	tuned := "unknown"
	fmt.Fprintln(os.Stdout, benchSummaryLineReceiver(summary, route, tuned))
}
