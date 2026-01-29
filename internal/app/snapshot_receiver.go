package app

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/quic-go/quic-go"
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
	Dumb                   bool
	DumbTCP                bool
	ParallelConnections    int
	ParallelStreams        int
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
		cfg.QuicConnWindowBytes = 512 * 1024 * 1024
	}
	if cfg.QuicStreamWindowBytes <= 0 {
		cfg.QuicStreamWindowBytes = 64 * 1024 * 1024
	}
	if cfg.QuicMaxIncomingStreams <= 0 {
		cfg.QuicMaxIncomingStreams = 100
	}
	if cfg.ParallelConnections < 1 {
		cfg.ParallelConnections = 4
	}
	absOut, err := filepath.Abs(cfg.OutDir)
	if err != nil {
		return fmt.Errorf("failed to resolve output dir: %w", err)
	}
	cfg.OutDir = absOut
	if !cfg.Dumb {
		if err := os.MkdirAll(cfg.OutDir, 0755); err != nil {
			return fmt.Errorf("failed to create output dir: %w", err)
		}
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
		joinCode:               cfg.JoinCode,
		outDir:                 cfg.OutDir,
		benchmark:              cfg.Benchmark,
		verbose:                cfg.Verbose,
		dumb:                   cfg.Dumb,
		dumbTCP:                cfg.DumbTCP,
		parallelConnections:    cfg.ParallelConnections,
		parallelStreams:        cfg.ParallelStreams,
		udpReadBufferBytes:     cfg.UDPReadBufferBytes,
		udpWriteBufferBytes:    cfg.UDPWriteBufferBytes,
		quicConnWindowBytes:    cfg.QuicConnWindowBytes,
		quicStreamWindowBytes:  cfg.QuicStreamWindowBytes,
		quicMaxIncomingStreams: cfg.QuicMaxIncomingStreams,
		stunServers:            cfg.StunServers,
		turnServers:            cfg.TurnServers,
		turnOnly:               cfg.TurnOnly,
		resumeEnabled:          true,
		signalCh:               make(chan protocol.Envelope, 64),
		transfer:               make(chan protocol.TransferStart, 1),
		sessionID:              "",
		dumbQuicConnections:    1,
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
	joinCode               string
	outDir                 string
	benchmark              bool
	dumb                   bool
	dumbTCP                bool
	dumbQuicMu             sync.Mutex
	dumbQuicConnections    int
	parallelConnections    int
	parallelStreams        int
	udpReadBufferBytes     int
	udpWriteBufferBytes    int
	quicConnWindowBytes    int
	quicStreamWindowBytes  int
	quicMaxIncomingStreams int
	stunServers            []string
	turnServers            []string
	turnOnly               bool
	turnMu                 sync.RWMutex
	verbose                bool
	resumeEnabled          bool
	manifestPrompted       bool
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
	case protocol.TypeTurnCredentials:
		var creds protocol.TurnCredentials
		if err := env.DecodePayload(&creds); err != nil {
			r.logger.Error("failed to decode turn_credentials", "error", err)
			return
		}
		_ = r.setTurnServersIfEmpty(creds.Servers)
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
		if !r.manifestPrompted {
			r.manifestPrompted = true
			printIncomingSummary(offer.Summary)
			reader := bufio.NewReader(os.Stdin)
			accepted, err := promptAccept(reader)
			if err != nil {
				r.logger.Error("failed to read acceptance", "error", err)
				os.Exit(1)
			}
			if !accepted {
				fmt.Fprintln(os.Stderr, "Transfer declined.")
				os.Exit(1)
			}
			if hasResumeData(r.outDir, offer.Summary.RootName) {
				resume, err := promptResumeOrOverwrite(reader)
				if err != nil {
					r.logger.Error("failed to read resume choice", "error", err)
					os.Exit(1)
				}
				if !resume {
					r.resumeEnabled = false
					if err := clearResumeData(r.outDir, offer.Summary.RootName); err != nil && r.verbose {
						fmt.Fprintf(os.Stderr, "Failed to clear resume data: %v\n", err)
					}
				}
			}
		}
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
		if r.verbose {
			if queued.Position > 0 {
				fmt.Printf("Queued for transfer (position %d, active=%d/%d)\n", queued.Position, queued.Active, queued.Max)
			} else {
				fmt.Printf("Queued for transfer (active=%d/%d)\n", queued.Active, queued.Max)
			}
		}
	case protocol.TypeDumbTCPListen:
		select {
		case r.signalCh <- env:
		default:
		}
	case protocol.TypeDumbQUICMulti:
		var msg protocol.DumbQUICMulti
		if err := env.DecodePayload(&msg); err != nil {
			r.logger.Error("failed to decode dumb_quic_multi", "error", err)
			return
		}
		r.setDumbQuicConnections(msg.Connections)
		select {
		case r.signalCh <- env:
		default:
		}
	case protocol.TypeIceCredentials, protocol.TypeIceCandidates, protocol.TypeIceCandidate:
		select {
		case r.signalCh <- env:
		default:
		}
	}
}

func (r *snapshotReceiver) setDumbQuicConnections(n int) {
	if n < 1 {
		n = 1
	}
	r.dumbQuicMu.Lock()
	r.dumbQuicConnections = n
	r.dumbQuicMu.Unlock()
}

func (r *snapshotReceiver) getDumbQuicConnections() int {
	r.dumbQuicMu.Lock()
	n := r.dumbQuicConnections
	r.dumbQuicMu.Unlock()
	if n < 1 {
		return 1
	}
	return n
}

func (r *snapshotReceiver) setTurnServersIfEmpty(servers []string) bool {
	if len(servers) == 0 {
		return false
	}
	r.turnMu.Lock()
	defer r.turnMu.Unlock()
	if len(r.turnServers) > 0 {
		return false
	}
	r.turnServers = append([]string{}, servers...)
	return true
}

func (r *snapshotReceiver) currentTurnServers() []string {
	r.turnMu.RLock()
	defer r.turnMu.RUnlock()
	return append([]string{}, r.turnServers...)
}

func (r *snapshotReceiver) sendAccept(manifestID string) {
	accept := protocol.ManifestAccept{
		ManifestID:          manifestID,
		Mode:                "all",
		SelectedPaths:       nil,
		ParallelConnections: r.parallelConnections,
		ParallelStreams:     r.parallelStreams,
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

func (r *snapshotReceiver) sendDumbQUICDone(parts int) error {
	env, err := protocol.NewEnvelope(protocol.TypeDumbQUICDone, protocol.NewMsgID(), protocol.DumbQUICDone{Parts: parts})
	if err != nil {
		return err
	}
	env.SessionID = r.sessionID
	env.From = r.peerID
	env.To = r.senderID
	return r.conn.Send(env)
}

func (r *snapshotReceiver) runTransfer(start protocol.TransferStart) {
	baseCtx := context.Background()
	progressState := newReceiverProgress(r.totalBytes, r.fileTotal, start.ManifestID, r.outDir, r.benchmark)
	if r.benchmark {
		benchCtx, benchCancel := context.WithCancel(baseCtx)
		r.startReceiverBenchLoop(benchCtx, progressState)
		defer benchCancel()
	}
	stopUI := progress.RenderReceiver(baseCtx, os.Stderr, progressState.View, r.verbose)
	var stopUIOnce sync.Once
	stopUIFn := func() {
		stopUIOnce.Do(stopUI)
	}
	r.cleanupMu.Lock()
	r.uiCleanup = stopUIFn
	r.cleanupMu.Unlock()
	defer stopUIFn()
	exitWith := func(code int) {
		r.cleanupMu.Lock()
		cleanup := r.uiCleanup
		r.cleanupMu.Unlock()
		if cleanup != nil {
			cleanup()
		}
		if code != 0 {
			view := progressState.View()
			stats := view.Stats
			// Set stage to FAILED for UI/logs
			progressState.SetIceStage(fmt.Sprintf("FAILED (code=%d)", code))

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

	var lastProgress int64
	if r.dumbTCP {
		if err := r.runDumbTCPTransfer(baseCtx, progressState, &lastProgress); err != nil {
			r.cleanupMu.Lock()
			cleanup := r.uiCleanup
			r.cleanupMu.Unlock()
			if cleanup != nil {
				cleanup()
			}
			if r.verbose {
				fmt.Fprintf(os.Stderr, "dumb tcp transfer failed: %v\n", err)
			}
			r.logger.Error("dumb tcp transfer failed", "error", err)
			exitWith(1)
		}
		progressState.ForceComplete()
		exitWith(0)
	}

	var (
		prober *ice.Prober
		err    error
	)

	// Initialize Prober
	proberCfg := ice.ProberConfig{
		StunServers: r.stunServers,
		TurnServers: r.currentTurnServers(),
		TurnOnly:    r.turnOnly,
	}
	prober, err = ice.NewProber(proberCfg, r.logger)
	if err != nil {
		r.logger.Error("failed to create prober", "error", err)
		exitWith(1)
	}
	defer prober.Close()

	// Get local candidates (IP:Port)
	localCandidates := prober.GetProbingAddresses()
	iceLog("gather_complete")

	// Send candidates to sender
	// We reuse IceCandidates message type for simplicity
	if err := sendSignal(protocol.TypeIceCandidates, protocol.IceCandidates{Candidates: localCandidates}); err != nil {
		r.logger.Error("failed to send candidates", "error", err)
		exitWith(1)
	}
	progressState.SetIceStage(fmt.Sprintf("local_candidates_sent count=%d", len(localCandidates)))

	// Start listening for QUIC immediately
	udpConn := prober.ListenPacket()

	// Tweak buffers
	// udpTune := transport.ApplyUDPBeyondBestEffort(udpConn.(*net.UDPConn), r.udpReadBufferBytes, r.udpWriteBufferBytes)
	// TypAssertion might fail if it's not *net.UDPConn (it is in our case)
	var udpTune transport.UdpTuneResult
	if conn, ok := udpConn.(*net.UDPConn); ok {
		udpTune = transport.ApplyUDPBeyondBestEffort(conn, r.udpReadBufferBytes, r.udpWriteBufferBytes)
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
		if r.verbose {
			fmt.Fprintln(os.Stdout, transportSummary)
			for _, line := range transportLines {
				fmt.Fprintln(os.Stdout, line)
			}
		}
	}

	// Share the prober transport to avoid multiple QUIC readers on the same UDP socket.
	quicListener, err := quictransport.ListenWithTransport(baseCtx, prober.Transport(), r.logger, quicCfg)
	if err != nil {
		r.logger.Error("failed to listen for QUIC", "error", err)
		exitWith(1)
	}
	defer quicListener.Close()

	quicTransport := transferquic.NewListener(quicListener, r.logger)
	defer quicTransport.Close()

	var turnListener *quic.Listener
	var turnTransport *transferquic.QUICTransport
	if tr := prober.TurnTransport(); tr != nil {
		turnListener, err = quictransport.ListenWithTransport(baseCtx, tr, r.logger, quicCfg)
		if err != nil {
			r.logger.Warn("failed to listen on TURN relay", "error", err)
		} else {
			turnTransport = transferquic.NewListener(turnListener, r.logger)
			defer turnListener.Close()
			defer turnTransport.Close()
		}
	}

	iceLog("waiting_for_connection")

	// Bidirectional Probing: Dial sender candidates while accepting
	dialResCh := make(chan transfer.Conn, 1)
	probeCtx, probeCancel := context.WithCancel(baseCtx)
	defer probeCancel()

	go func() {
		for {
			select {
			case <-probeCtx.Done():
				return
			case env := <-r.signalCh:
				if env.Type == protocol.TypeIceCandidates {
					var cands protocol.IceCandidates
					if err := env.DecodePayload(&cands); err != nil {
						r.logger.Error("failed to decode sender candidates", "error", err)
						continue
					}
					r.logger.Info("received sender candidates", "count", len(cands.Candidates))
					prober.PrimeTurnPermissions(cands.Candidates)
					for _, cand := range cands.Candidates {
						if ice.IsTurnCandidate(cand) {
							progressState.SetProbeStatus(cand, ice.ProbeStateReadyFallback)
						}
					}

					// Delayed Dial: Wait 500ms to let the Sender (initiator) win the race if possible.
					// This avoids "Split Brain" where both sides dial simultaneously and deadlock on different connections.
					select {
					case <-probeCtx.Done():
						return
					case <-time.After(500 * time.Millisecond):
					}

					// We use ProbeAndDial (which expects ClientConfig)
					quicConn, err := prober.ProbeAndDial(probeCtx, cands.Candidates, quictransport.ClientConfig(), quicCfg, func(upd ice.ProbeUpdate) {
						progressState.SetProbeStatus(upd.Addr, upd.State)
					})
					if err == nil {
						tconn, derr := transferquic.NewDialer(quicConn, r.logger).Dial(probeCtx, r.senderID)
						if derr == nil {
							select {
							case dialResCh <- tconn:
								r.logger.Info("outgoing dial won the race", "addr", quicConn.RemoteAddr())
							default:
								tconn.Close()
							}
						} else {
							quicConn.CloseWithError(0, "dial_failed")
						}
					}
				}
			}
		}
	}()

	acceptResCh := make(chan transfer.Conn, 1)
	acceptOnce := func(t *transferquic.QUICTransport, turn bool) {
		conn, err := t.Accept(probeCtx)
		if err == nil {
			select {
			case acceptResCh <- conn:
				r.logger.Info("incoming accept won the race", "addr", conn.RemoteAddr())
				if turn {
					progressState.SetProbeStatus("turn:"+conn.RemoteAddr().String(), ice.ProbeStateWon)
				} else {
					progressState.SetProbeStatus(conn.RemoteAddr().String(), ice.ProbeStateWon)
				}
			default:
				conn.Close()
			}
		}
	}
	go acceptOnce(quicTransport, false)
	if turnTransport != nil {
		go acceptOnce(turnTransport, true)
	}

	var transferConn transfer.Conn
	select {
	case <-baseCtx.Done():
		exitWith(1)
	case tc := <-dialResCh:
		transferConn = tc
		iceLog("connect_ok (dial)")
	case tc := <-acceptResCh:
		transferConn = tc
		iceLog("connect_ok (accept)")
	}
	probeCancel() // Stop other attempts

	// Log route
	if transferConn != nil {
		progressState.SetRoute(fmt.Sprintf("route %s", transferConn.RemoteAddr()))
	}

	defer transferConn.Close()
	if r.verbose {
		fmt.Fprintf(os.Stderr, "QUIC transfer connection established (session=%s sender=%s route=%s)\n", r.sessionID, r.senderID, transferConn.RemoteAddr())
	}

	authCtx, authCancel := context.WithTimeout(baseCtx, 10*time.Second)
	if err := authenticateTransport(authCtx, transferConn, r.joinCode, authRoleReceive); err != nil {
		authCancel()
		r.cleanupMu.Lock()
		cleanup := r.uiCleanup
		r.cleanupMu.Unlock()
		if cleanup != nil {
			cleanup()
		}
		fmt.Fprintf(os.Stderr, "transport auth failed: %v\n", err)
		fmt.Fprintf(os.Stdout, "transport auth failed: %v\n", err)
		r.logger.Error("transport auth failed", "error", err)
		exitWith(1)
	}
	authCancel()

	if r.dumb {
		expectedConns := r.getDumbQuicConnections()
		if expectedConns <= 1 {
			expectedConns = r.waitForDumbQuicConnections(baseCtx, r.signalCh, 2*time.Second)
		}
		if expectedConns < 1 {
			expectedConns = 1
		}

		var dumbConns []transfer.Conn
		dumbConns = append(dumbConns, transferConn)
		if expectedConns > 1 {
			extraConns, err := r.acceptExtraConns(baseCtx, quicTransport, expectedConns-1)
			if err != nil {
				r.cleanupMu.Lock()
				cleanup := r.uiCleanup
				r.cleanupMu.Unlock()
				if cleanup != nil {
					cleanup()
				}
				if r.verbose {
					fmt.Fprintf(os.Stderr, "dumb transfer failed: %v\n", err)
				}
				r.logger.Error("dumb transfer failed", "error", err)
				exitWith(1)
			}
			dumbConns = append(dumbConns, extraConns...)
		}
		progressState.SetConnCount(len(dumbConns))

		dumbCollector := newProgressCollector()
		dumbStop := startProgressTicker(baseCtx, dumbCollector, func(relpath string, bytesReceived int64, total int64) {
			progressState.Update(relpath, bytesReceived, total)
		})
		if err := recvDumbDiscardMulti(baseCtx, dumbConns, func(relpath string, bytesReceived int64, total int64) {
			dumbCollector.Update(relpath, bytesReceived, total)
		}); err != nil {
			dumbStop()
			r.cleanupMu.Lock()
			cleanup := r.uiCleanup
			r.cleanupMu.Unlock()
			if cleanup != nil {
				cleanup()
			}
			if r.verbose {
				fmt.Fprintf(os.Stderr, "dumb transfer failed: %v\n", err)
			}
			r.logger.Error("dumb transfer failed", "error", err)
			exitWith(1)
		}
		dumbStop()
		_ = r.sendDumbQUICDone(len(dumbConns))
		progressState.ForceComplete()
		exitWith(0)
	}

	effectiveConnections := r.parallelConnections
	if effectiveConnections < 1 {
		effectiveConnections = 1
	}
	if start.ParallelConnections > 0 && start.ParallelConnections < effectiveConnections {
		effectiveConnections = start.ParallelConnections
	}

	conns := []transfer.Conn{transferConn}
	var extra []transfer.Conn
	if effectiveConnections > 1 {
		extraConns, err := r.acceptExtraConns(baseCtx, quicTransport, effectiveConnections-1)
		if err != nil {
			r.logger.Warn("failed to accept extra connections", "error", err)
		}
		extra = extraConns
		conns = append(conns, extraConns...)
	}
	if len(conns) == 0 {
		r.cleanupMu.Lock()
		cleanup := r.uiCleanup
		r.cleanupMu.Unlock()
		if cleanup != nil {
			cleanup()
		}
		fmt.Fprintf(os.Stderr, "transfer failed: no connections available\n")
		exitWith(1)
	}
	progressState.SetConnCount(len(conns))
	defer func() {
		for _, c := range extra {
			c.Close()
		}
	}()

	totalStreams, _ := computeParallelBudget(r.fileTotal, r.parallelStreams, len(conns), len(conns) > 1)

	var lastStats int64
	recvCtx, recvCancel := context.WithCancel(baseCtx)
	defer recvCancel()

	progressCollector := newProgressCollector()
	progressStop := startProgressTicker(recvCtx, progressCollector, func(relpath string, bytesReceived int64, total int64) {
		progressState.Update(relpath, bytesReceived, total)
	})
	defer progressStop()

	opts := transfer.Options{
		Resume:        r.resumeEnabled,
		NoRootDir:     true,
		HashAlg:       "crc32c",
		ParallelFiles: totalStreams,
		ProgressFn: func(relpath string, bytesReceived int64, total int64) {
			progressCollector.Update(relpath, bytesReceived, total)
		},
		ProgressDeltaFn: func(relpath string, delta int64) {
			progressCollector.Add(relpath, delta)
		},
		TransferStatsFn: func(activeFiles, completedFiles int, remainingBytes int64) {
			if !shouldUpdateProgress(&lastStats) {
				return
			}
			progressState.UpdateStats(activeFiles, completedFiles)
		},
		ResumeStatsFn: func(relpath string, skippedChunks, totalChunks uint32, verifiedChunk uint32, totalBytes int64, chunkSize uint32) {
			progressState.RecordResume(relpath, skippedChunks, totalChunks, verifiedChunk, totalBytes, chunkSize)
		},
		FileDoneFn: func(relpath string, ok bool) {
			progressState.MarkVerified(relpath, ok)
			if total := progressCollector.Total(relpath); total > 0 {
				progressState.mu.Lock()
				offset := progressState.skipOffset[relpath]
				progressState.mu.Unlock()
				bytes := total - offset
				if bytes < 0 {
					bytes = 0
				}
				progressCollector.Update(relpath, bytes, total)
				progressState.Update(relpath, bytes, total)
			}
		},
	}

	var recvConn transfer.Conn = conns[0]
	if len(conns) > 1 {
		multi, err := transfer.NewMultiConn(conns)
		if err != nil {
			r.cleanupMu.Lock()
			cleanup := r.uiCleanup
			r.cleanupMu.Unlock()
			if cleanup != nil {
				cleanup()
			}
			fmt.Fprintf(os.Stderr, "transfer failed: %v\n", err)
			fmt.Fprintf(os.Stdout, "transfer failed: %v\n", err)
			r.logger.Error("transfer failed", "error", err)
			exitWith(1)
		}
		defer multi.Close()
		recvConn = multi
	}

	if _, err := transfer.RecvManifestMultiStream(recvCtx, recvConn, r.outDir, opts); err != nil {
		r.cleanupMu.Lock()
		cleanup := r.uiCleanup
		r.cleanupMu.Unlock()
		if cleanup != nil {
			cleanup()
		}
		fmt.Fprintf(os.Stderr, "transfer failed: %v\n", err)
		fmt.Fprintf(os.Stdout, "transfer failed: %v\n", err)
		r.logger.Error("transfer failed", "error", err)
		exitWith(1)
	}

	if r.benchmark {
		progressState.FreezeBench(time.Now())
		r.printReceiverBenchSummary(progressState)
	}

	progressState.ForceComplete()
	exitWith(0)
}

func printIncomingSummary(summary protocol.ManifestSummary) {
	root := summary.RootName
	if strings.TrimSpace(root) == "" {
		root = "(root)"
	}
	fmt.Printf("Incoming transfer: %s (%d files, %d folders, %s)\n", root, summary.FileCount, summary.FolderCount, formatBytes(summary.TotalBytes))
}

func promptAccept(reader *bufio.Reader) (bool, error) {
	for {
		fmt.Print("Accept transfer? [y/N]: ")
		line, err := readLine(reader)
		if err != nil {
			return false, err
		}
		switch strings.ToLower(line) {
		case "":
			return false, nil
		case "y", "yes":
			return true, nil
		case "n", "no":
			return false, nil
		}
	}
}

func promptResumeOrOverwrite(reader *bufio.Reader) (bool, error) {
	for {
		fmt.Print("Resume existing data? [Y]es / [O]verwrite: ")
		line, err := readLine(reader)
		if err != nil {
			return true, err
		}
		switch strings.ToLower(line) {
		case "", "y", "yes", "r", "resume":
			return true, nil
		case "n", "no", "o", "overwrite":
			return false, nil
		}
	}
}

func readLine(reader *bufio.Reader) (string, error) {
	line, err := reader.ReadString('\n')
	if err != nil && len(line) == 0 {
		return "", err
	}
	return strings.TrimSpace(line), nil
}

func hasResumeData(outDir, root string) bool {
	for _, sidecarDir := range resumeSidecarDirs(outDir, root) {
		entries, err := os.ReadDir(sidecarDir)
		if err != nil {
			continue
		}
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			if strings.HasSuffix(entry.Name(), ".sbxmap") {
				return true
			}
		}
	}
	return false
}

func clearResumeData(outDir, root string) error {
	var firstErr error
	for _, sidecarDir := range resumeSidecarDirs(outDir, root) {
		if err := os.RemoveAll(sidecarDir); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func resumeSidecarDirs(outDir, root string) []string {
	seen := make(map[string]struct{}, 2)
	add := func(r string) {
		dir := filepath.Dir(transfer.SidecarPath(outDir, r, "probe"))
		seen[dir] = struct{}{}
	}
	add("")
	if strings.TrimSpace(root) != "" {
		add(root)
	}
	dirs := make([]string, 0, len(seen))
	for dir := range seen {
		dirs = append(dirs, dir)
	}
	return dirs
}

func formatBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	value := float64(n)
	exp := 0
	for value >= unit && exp < 5 {
		value /= unit
		exp++
	}
	suffixes := []string{"KiB", "MiB", "GiB", "TiB", "PiB"}
	return fmt.Sprintf("%.2f %s", value, suffixes[exp-1])
}

func (r *snapshotReceiver) runDumbTCPTransfer(ctx context.Context, progressState *receiverProgress, lastProgress *int64) error {
	var addrs []string
	timeout := time.NewTimer(15 * time.Second)
	defer timeout.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timeout.C:
			return fmt.Errorf("timeout waiting for dumb tcp listen")
		case env := <-r.signalCh:
			if env.Type != protocol.TypeDumbTCPListen {
				continue
			}
			var msg protocol.DumbTCPListen
			if err := env.DecodePayload(&msg); err != nil {
				return err
			}
			addrs = msg.Addrs
			goto dial
		}
	}

dial:
	if len(addrs) == 0 {
		return fmt.Errorf("no tcp addresses received")
	}
	conn, err := dialAddrs(ctx, addrs)
	if err != nil {
		return err
	}
	defer conn.Close()

	_, err = recvDumbDiscardReader(conn, func(relpath string, bytesReceived int64, total int64) {
		if !shouldUpdateProgress(lastProgress) {
			return
		}
		progressState.Update(relpath, bytesReceived, total)
	})
	return err
}

func (r *snapshotReceiver) waitForDumbQuicConnections(ctx context.Context, signalCh <-chan protocol.Envelope, timeout time.Duration) int {
	current := r.getDumbQuicConnections()
	if current > 1 || timeout <= 0 {
		return current
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return r.getDumbQuicConnections()
		case <-timer.C:
			return r.getDumbQuicConnections()
		case env := <-signalCh:
			if env.Type == protocol.TypeDumbQUICMulti {
				var msg protocol.DumbQUICMulti
				if err := env.DecodePayload(&msg); err == nil {
					r.setDumbQuicConnections(msg.Connections)
				}
				return r.getDumbQuicConnections()
			}
		}
	}
}

func (r *snapshotReceiver) acceptExtraConns(ctx context.Context, transport *transferquic.QUICTransport, extra int) ([]transfer.Conn, error) {
	if extra <= 0 {
		return nil, nil
	}
	if transport == nil {
		return nil, fmt.Errorf("quic transport unavailable")
	}

	acceptCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	conns := make([]transfer.Conn, 0, extra)
	var lastErr error
	for i := 0; i < extra; i++ {
		conn, err := transport.Accept(acceptCtx)
		if err != nil {
			lastErr = err
			break
		}
		authCtx, authCancel := context.WithTimeout(acceptCtx, 10*time.Second)
		if err := authenticateTransport(authCtx, conn, r.joinCode, authRoleReceive); err != nil {
			authCancel()
			conn.Close()
			lastErr = err
			break
		}
		authCancel()
		conns = append(conns, conn)
	}
	if len(conns) == 0 && lastErr != nil {
		return conns, lastErr
	}
	return conns, lastErr
}

func recvDumbDiscardMulti(ctx context.Context, conns []transfer.Conn, progressFn func(string, int64, int64)) error {
	if len(conns) == 0 {
		return fmt.Errorf("no connections available")
	}
	if len(conns) == 1 {
		_, err := recvDumbDiscard(ctx, conns[0], progressFn)
		return err
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, len(conns))
	var wg sync.WaitGroup
	for _, conn := range conns {
		wg.Add(1)
		go func(c transfer.Conn) {
			defer wg.Done()
			defer c.Close()
			if _, err := recvDumbDiscard(ctx, c, progressFn); err != nil {
				errCh <- err
				cancel()
			}
		}(conn)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return err
		}
	}
	return nil
}

type receiverProgress struct {
	meter            *progress.Meter
	perFile          map[string]int64
	totals           map[string]int64
	appliedSkip      map[string]bool
	pendingSkip      map[string]resumeSkip
	verifySeen       map[string]bool
	verified         map[string]bool
	skipOffset       map[string]int64
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
	connCount        int
	totalBytes       int64
	snapshotID       string
	outDir           string
	probes           map[string]ice.ProbeState
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
		skipOffset:       make(map[string]int64),
		pendingSkipBytes: make(map[string]int64),
		pendingVerify:    make(map[string]int64),
		verifyingActive:  false,
		resumedFiles:     0,
		fileTotal:        fileTotal,
		totalBytes:       totalBytes,
		snapshotID:       snapshotID,
		outDir:           outDir,
		probes:           make(map[string]ice.ProbeState),
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
	offset := p.skipOffset[relpath]
	effective := bytes + offset
	if total > 0 {
		if effective > total {
			effective = total
		}
	}
	prev := p.perFile[relpath]
	if effective > prev {
		delta := effective - prev
		p.addBytesLocked(delta)
		p.benchBytes += delta
	}
	if effective > prev {
		p.perFile[relpath] = effective
	} else {
		p.perFile[relpath] = prev
	}
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

func (p *receiverProgress) SetConnCount(count int) {
	p.mu.Lock()
	p.connCount = count
	p.mu.Unlock()
}

func (p *receiverProgress) SetProbeStatus(addr string, state ice.ProbeState) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.probes[addr] == ice.ProbeStateWon {
		return
	}
	p.probes[addr] = state
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

func (p *receiverProgress) RecordResume(relpath string, skippedChunks, totalChunks uint32, verifiedChunk uint32, totalBytes int64, chunkSize uint32) {
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
		if skippedBytes > p.skipOffset[relpath] {
			p.skipOffset[relpath] = skippedBytes
		}
		if p.perFile[relpath] < skippedBytes {
			p.perFile[relpath] = skippedBytes
		}
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
	connCount := p.connCount
	probes := make(map[string]string)
	for k, v := range p.probes {
		probes[k] = v.String()
	}
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
		Probes:         probes,
		ConnCount:      connCount,
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
