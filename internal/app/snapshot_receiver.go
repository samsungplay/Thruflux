package app

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/sheerbytes/sheerbytes/internal/ice"
	"github.com/sheerbytes/sheerbytes/internal/quictransport"
	"github.com/sheerbytes/sheerbytes/internal/transfer"
	"github.com/sheerbytes/sheerbytes/internal/transferquic"
	"github.com/sheerbytes/sheerbytes/internal/wsclient"
	"github.com/sheerbytes/sheerbytes/pkg/protocol"
)

// SnapshotReceiverConfig configures the snapshot receiver.
type SnapshotReceiverConfig struct {
	ServerURL string
	JoinCode  string
	OutDir    string
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
	absOut, err := filepath.Abs(cfg.OutDir)
	if err != nil {
		return fmt.Errorf("failed to resolve output dir: %w", err)
	}
	cfg.OutDir = absOut
	if err := os.MkdirAll(cfg.OutDir, 0755); err != nil {
		return fmt.Errorf("failed to create output dir: %w", err)
	}

	peerID := randomPeerID()
	wsURL, err := buildWebSocketURL(cfg.ServerURL, cfg.JoinCode, peerID, "receiver")
	if err != nil {
		return err
	}

	conn, err := wsclient.Dial(ctx, wsURL, logger)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	defer conn.Close()

	s := &snapshotReceiver{
		logger:    logger,
		conn:      conn,
		peerID:    peerID,
		outDir:    cfg.OutDir,
		signalCh:  make(chan protocol.Envelope, 64),
		transfer:  make(chan protocol.TransferStart, 1),
		sessionID: "",
	}

	go s.watchInterrupt()

	readErr := conn.ReadLoop(ctx, func(env protocol.Envelope) {
		s.handleEnvelope(env)
	})
	if readErr != nil && readErr != context.Canceled {
		return readErr
	}
	return nil
}

type snapshotReceiver struct {
	logger     *slog.Logger
	conn       *wsclient.Conn
	peerID     string
	outDir     string
	senderID   string
	manifest   string
	sessionID  string
	totalBytes int64
	signalCh   chan protocol.Envelope
	transfer   chan protocol.TransferStart
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
		r.senderID = env.From
		r.sendAccept(offer.Summary.ManifestID)
	case protocol.TypePeerLeft:
		var peerLeft protocol.PeerLeft
		if err := env.DecodePayload(&peerLeft); err != nil {
			r.logger.Error("failed to decode peer_left", "error", err)
			return
		}
		if r.senderID != "" && peerLeft.PeerID == r.senderID {
			fmt.Println("Sender disconnected. Exiting.")
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
	fmt.Printf("Receiving snapshot %s\n", start.ManifestID)
	fmt.Printf("Saving to %s\n", r.outDir)

	baseCtx := context.Background()
	iceLog := func(stage string) {
		fmt.Printf("ice receiver stage=%s\n", stage)
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
		StunServers: []string{"stun:stun.l.google.com:19302"},
		Lite:        false,
	}
	var (
		icePeer *ice.ICEPeer
		iceConn net.Conn
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
			os.Exit(1)
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
			os.Exit(1)
		}
		select {
		case <-icePeer.GatheringDone():
			iceLog("gather_complete")
		case <-time.After(10 * time.Second):
			attemptCancel()
			icePeer.Close()
			fmt.Printf("ice receiver stage=restart attempt=%d\n", attempt+1)
			time.Sleep(200 * time.Millisecond)
			continue
		}

		ufrag, pwd := icePeer.LocalCredentials()
		if err := sendSignal(protocol.TypeIceCredentials, protocol.IceCredentials{Ufrag: ufrag, Pwd: pwd}); err != nil {
			attemptCancel()
			icePeer.Close()
			r.logger.Error("failed to send credentials", "error", err)
			os.Exit(1)
		}
		iceLog("local_creds_sent")
		if err := sendSignal(protocol.TypeIceCandidates, protocol.IceCandidates{Candidates: localCandidates}); err != nil {
			attemptCancel()
			icePeer.Close()
			r.logger.Error("failed to send candidates", "error", err)
			os.Exit(1)
		}
		fmt.Printf("ice receiver stage=local_candidates_sent count=%d\n", len(localCandidates))

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
				os.Exit(1)
			case err := <-readErr:
				readCancel()
				attemptCancel()
				icePeer.Close()
				r.logger.Error("signal error", "error", err)
				os.Exit(1)
			case creds := <-remoteCredsCh:
				remoteCreds = &creds
				iceLog("remote_creds_received")
			case cands := <-remoteCandsCh:
				remoteCands = cands
				fmt.Printf("ice receiver stage=remote_candidates_received count=%d\n", len(cands))
			case <-waitDeadline:
				readCancel()
				attemptCancel()
				icePeer.Close()
				fmt.Printf("ice receiver stage=restart attempt=%d\n", attempt+1)
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
			os.Exit(1)
		}
		for _, cand := range remoteCands {
			_ = icePeer.AddRemoteCandidate(cand)
		}

		iceLog("connect_start")
		acceptCtx, acceptCancel := context.WithTimeout(attemptCtx, 10*time.Second)
		iceConn, err = icePeer.Accept(acceptCtx)
		acceptCancel()
		readCancel()
		attemptCancel()
		if err == nil {
			break
		}
		icePeer.Close()
		fmt.Printf("ice receiver stage=restart attempt=%d\n", attempt+1)
		time.Sleep(200 * time.Millisecond)
		continue
	}
	iceLog("connect_ok")
	logICEPair("receiver", r.peerID, icePeer)

	_, _, err = icePeer.PacketConnInfo()
	if err != nil {
		iceConn.Close()
		r.logger.Error("failed to get PacketConn info", "error", err)
		os.Exit(1)
	}
	iceConn.Close()

	udpConn, err := icePeer.CreatePacketConn()
	if err != nil {
		r.logger.Error("failed to create PacketConn", "error", err)
		os.Exit(1)
	}
	defer udpConn.Close()

	quicListener, err := quictransport.Listen(baseCtx, udpConn, r.logger)
	if err != nil {
		r.logger.Error("failed to listen for QUIC", "error", err)
		os.Exit(1)
	}
	defer quicListener.Close()

	quicTransport := transferquic.NewListener(quicListener, r.logger)
	defer quicTransport.Close()

	transferConn, err := quicTransport.Accept(baseCtx)
	if err != nil {
		r.logger.Error("failed to accept transfer connection", "error", err)
		os.Exit(1)
	}
	defer transferConn.Close()

	progress := newProgressTicker("receiver", r.senderID, r.totalBytes)
	defer progress.Stop()
	opts := transfer.Options{
		Resume:    true,
		NoRootDir: true,
		HashAlg:   "crc32c",
		ProgressFn: func(relpath string, bytesReceived int64, total int64) {
			progress.Update(relpath, bytesReceived)
		},
		TransferStatsFn: func(activeFiles, completedFiles int, remainingBytes int64) {
			progress.UpdateStats(activeFiles, completedFiles)
		},
	}
	_, err = transfer.RecvManifestMultiStream(baseCtx, transferConn, r.outDir, opts)
	if err != nil {
		r.logger.Error("transfer failed", "error", err)
		os.Exit(1)
	}

	fmt.Println("Transfer complete. Exiting.")
	os.Exit(0)
}

func (r *snapshotReceiver) watchInterrupt() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan
	fmt.Println("Interrupted. You may resume by re-running.")
	os.Exit(1)
}
