package sender

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sheerbytes/sheerbytes/internal/app"
	"github.com/sheerbytes/sheerbytes/internal/appstate"
	"github.com/sheerbytes/sheerbytes/internal/ice"
	"github.com/sheerbytes/sheerbytes/internal/logging"
	"github.com/sheerbytes/sheerbytes/internal/quictransport"
	"github.com/sheerbytes/sheerbytes/internal/transfer"
	"github.com/sheerbytes/sheerbytes/internal/transferquic"
	"github.com/sheerbytes/sheerbytes/internal/wsclient"
	"github.com/sheerbytes/sheerbytes/pkg/manifest"
	"github.com/sheerbytes/sheerbytes/pkg/protocol"
)

const senderDefaultServerURL = "https://bytepipe.app"

var senderDefaultStunServers = []string{
	"stun:stun.l.google.com:19302",
	"stun:stun.cloudflare.com:3478",
	"stun:stun.bytepipe.app:3478",
}

func Run(args []string) {
	if len(args) == 0 {
		printSenderUsage()
		os.Exit(2)
	}
	if hasHelpFlag(args) {
		printSenderUsage()
		return
	}

	paths := make([]string, 0)
	serverURL := ""
	maxReceivers := 4
	benchmark := false
	udpReadBufferBytes := 8 * 1024 * 1024
	udpWriteBufferBytes := 8 * 1024 * 1024
	quicConnWindowBytes := 512 * 1024 * 1024
	quicStreamWindowBytes := 64 * 1024 * 1024
	quicMaxIncomingStreams := 100
	stunServers := make([]string, 0)
	turnServers := make([]string, 0)
	var chunkSize uint64
	parallelFiles := 0
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--max-receivers" && i+1 < len(args) {
			i++
			value := args[i]
			parsed, err := strconv.Atoi(value)
			if err != nil || parsed < 1 {
				fmt.Fprintln(os.Stderr, "invalid --max-receivers value")
				os.Exit(2)
			}
			maxReceivers = parsed
			continue
		}
		if arg == "--benchmark" {
			benchmark = true
			continue
		}
		if arg == "--server-url" && i+1 < len(args) {
			i++
			serverURL = args[i]
			continue
		}
		if arg == "--stun-server" && i+1 < len(args) {
			i++
			stunServers = append(stunServers, splitServers(args[i])...)
			continue
		}
		if arg == "--turn-server" && i+1 < len(args) {
			i++
			turnServers = append(turnServers, splitServers(args[i])...)
			continue
		}
		if arg == "--udp-read-buffer-bytes" && i+1 < len(args) {
			i++
			value := args[i]
			parsed, err := strconv.Atoi(value)
			if err != nil || parsed <= 0 {
				fmt.Fprintln(os.Stderr, "invalid --udp-read-buffer-bytes value")
				os.Exit(2)
			}
			udpReadBufferBytes = parsed
			continue
		}
		if arg == "--udp-write-buffer-bytes" && i+1 < len(args) {
			i++
			value := args[i]
			parsed, err := strconv.Atoi(value)
			if err != nil || parsed <= 0 {
				fmt.Fprintln(os.Stderr, "invalid --udp-write-buffer-bytes value")
				os.Exit(2)
			}
			udpWriteBufferBytes = parsed
			continue
		}
		if arg == "--quic-conn-window-bytes" && i+1 < len(args) {
			i++
			value := args[i]
			parsed, err := strconv.Atoi(value)
			if err != nil || parsed <= 0 {
				fmt.Fprintln(os.Stderr, "invalid --quic-conn-window-bytes value")
				os.Exit(2)
			}
			quicConnWindowBytes = parsed
			continue
		}
		if arg == "--quic-stream-window-bytes" && i+1 < len(args) {
			i++
			value := args[i]
			parsed, err := strconv.Atoi(value)
			if err != nil || parsed <= 0 {
				fmt.Fprintln(os.Stderr, "invalid --quic-stream-window-bytes value")
				os.Exit(2)
			}
			quicStreamWindowBytes = parsed
			continue
		}
		if arg == "--quic-max-incoming-streams" && i+1 < len(args) {
			i++
			value := args[i]
			parsed, err := strconv.Atoi(value)
			if err != nil || parsed <= 0 {
				fmt.Fprintln(os.Stderr, "invalid --quic-max-incoming-streams value")
				os.Exit(2)
			}
			quicMaxIncomingStreams = parsed
			continue
		}
		if (arg == "--chunk-size" || arg == "--parallel-files") && i+1 < len(args) {
			i++
			value := args[i]
			switch arg {
			case "--chunk-size":
				parsed, err := strconv.ParseUint(value, 10, 64)
				if err != nil || parsed == 0 {
					fmt.Fprintln(os.Stderr, "invalid --chunk-size value")
					os.Exit(2)
				}
				chunkSize = parsed
			case "--parallel-files":
				parsed, err := strconv.Atoi(value)
				if err != nil || parsed < 1 || parsed > 8 {
					fmt.Fprintln(os.Stderr, "invalid --parallel-files value")
					os.Exit(2)
				}
				parallelFiles = parsed
			}
			continue
		}
		if strings.HasPrefix(arg, "--") {
			fmt.Fprintf(os.Stderr, "unknown flag: %s\n", arg)
			printSenderUsage()
			os.Exit(2)
		}
		paths = append(paths, arg)
	}
	if len(paths) == 0 {
		printSenderUsage()
		os.Exit(2)
	}

	if strings.TrimSpace(serverURL) == "" {
		serverURL = senderDefaultServerURL
	}
	if len(stunServers) == 0 {
		stunServers = append([]string{}, senderDefaultStunServers...)
	}

	logLevel := "error"
	logger := logging.New("thruflux-sender", logLevel)
	if err := app.RunSnapshotSender(context.Background(), logger, app.SnapshotSenderConfig{
		ServerURL:              serverURL,
		Paths:                  paths,
		MaxReceivers:           maxReceivers,
		ReceiverTTL:            10 * time.Minute,
		Benchmark:              benchmark,
		UDPReadBufferBytes:     udpReadBufferBytes,
		UDPWriteBufferBytes:    udpWriteBufferBytes,
		QuicConnWindowBytes:    quicConnWindowBytes,
		QuicStreamWindowBytes:  quicStreamWindowBytes,
		QuicMaxIncomingStreams: quicMaxIncomingStreams,
		StunServers:            stunServers,
		TurnServers:            turnServers,
		TransferOpts: transfer.Options{
			ChunkSize:     uint32(chunkSize),
			ParallelFiles: parallelFiles,
		},
	}); err != nil {
		logger.Error("snapshot sender failed", "error", err)
		os.Exit(1)
	}
}

func printSenderUsage() {
	fmt.Fprintln(os.Stderr, "usage: thru host <paths...> [--max-receivers N] [--server-url URL] [--benchmark]")
	fmt.Fprintln(os.Stderr, "  --max-receivers N           max concurrent receivers (default 4)")
	fmt.Fprintln(os.Stderr, "  --server-url URL            signaling server URL (default https://bytepipe.app)")
	fmt.Fprintf(os.Stderr, "  --stun-server URLS          STUN server URLs (comma-separated, default %s)\n", strings.Join(ice.DefaultStunServers, ","))
	fmt.Fprintln(os.Stderr, "                              example: --stun-server stun:stun.l.google.com:19302")
	fmt.Fprintln(os.Stderr, "  --turn-server URLS          TURN server URLs (comma-separated, default none)")
	fmt.Fprintln(os.Stderr, "                              example: --turn-server turn:username:password@turn.example.com:3478?transport=udp")
	fmt.Fprintln(os.Stderr, "  --benchmark                 enable benchmark stats")
	fmt.Fprintln(os.Stderr, "  --udp-read-buffer-bytes N   UDP read buffer size (default 8388608)")
	fmt.Fprintln(os.Stderr, "  --udp-write-buffer-bytes N  UDP write buffer size (default 8388608)")
	fmt.Fprintln(os.Stderr, "  --quic-conn-window-bytes N  QUIC connection window (default 536870912)")
	fmt.Fprintln(os.Stderr, "  --quic-stream-window-bytes N QUIC stream window (default 67108864)")
	fmt.Fprintln(os.Stderr, "  --quic-max-incoming-streams N max QUIC incoming streams (default 100)")
	fmt.Fprintln(os.Stderr, "  --chunk-size N              chunk size in bytes (default 0=auto)")
	fmt.Fprintln(os.Stderr, "  --parallel-files N          max concurrent file transfers (1..8)")
}

func hasHelpFlag(args []string) bool {
	for _, arg := range args {
		if arg == "--help" || arg == "-h" {
			return true
		}
	}
	return false
}

func splitServers(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		out = append(out, part)
	}
	return out
}

func buildWebSocketURL(serverURL, joinCode, peerID, role string) (string, error) {
	// Parse server URL
	u, err := url.Parse(serverURL)
	if err != nil {
		return "", err
	}

	// Convert http(s):// to ws(s)://
	scheme := strings.Replace(u.Scheme, "http", "ws", 1)
	if scheme == "ws" && u.Scheme == "https" {
		scheme = "wss"
	}

	// Build WebSocket URL
	wsURL := url.URL{
		Scheme:   scheme,
		Host:     u.Host,
		Path:     "/ws",
		RawQuery: fmt.Sprintf("join_code=%s&peer_id=%s&role=%s", url.QueryEscape(joinCode), url.QueryEscape(peerID), url.QueryEscape(role)),
	}

	return wsURL.String(), nil
}

func handleEnvelope(env protocol.Envelope, logger *slog.Logger, state *appstate.SenderState, printReadiness func(), targetPeer string, uiLogf func(string, ...any), targetOfferSent *bool) {
	// Validate envelope
	if err := env.ValidateBasic(); err != nil {
		logger.Warn("invalid envelope", "error", err)
		return
	}

	// Handle by type
	switch env.Type {
	case protocol.TypePeerList:
		var peerList protocol.PeerList
		if err := env.DecodePayload(&peerList); err != nil {
			logger.Warn("failed to decode peer_list", "error", err)
			return
		}
		peerIDs := make([]string, len(peerList.Peers))
		roleMap := make(map[string]string)
		receiverIDs := make([]string, 0)
		for i, p := range peerList.Peers {
			peerIDs[i] = fmt.Sprintf("%s (%s)", p.PeerID, p.Role)
			if p.Role == "receiver" {
				receiverIDs = append(receiverIDs, p.PeerID)
			}
			if targetPeer == "" {
				roleMap[p.PeerID] = p.Role
				continue
			}
			if p.PeerID == targetPeer {
				roleMap[p.PeerID] = p.Role
			}
		}
		logger.Info("peer list received", "count", len(peerList.Peers), "peers", strings.Join(peerIDs, ", "))
		if uiLogf != nil {
			sort.Strings(receiverIDs)
			uiLogf("peer_list receivers=%d peers=%s", len(receiverIDs), strings.Join(receiverIDs, ","))
		}

		// Update state from peer list
		state.UpdatePeerList(roleMap)
		printReadiness()

		if targetPeer != "" && targetOfferSent != nil && !*targetOfferSent {
			for _, p := range peerList.Peers {
				if p.PeerID == targetPeer && p.Role == "receiver" {
					if err := state.HandlePeerJoined(p.PeerID, p.Role); err != nil {
						logger.Error("failed to handle target peer from list", "error", err)
					} else {
						*targetOfferSent = true
					}
					break
				}
			}
		}

	case protocol.TypePeerJoined:
		var peerJoined protocol.PeerJoined
		if err := env.DecodePayload(&peerJoined); err != nil {
			logger.Warn("failed to decode peer_joined", "error", err)
			return
		}
		logger.Info("peer joined", "peer_id", peerJoined.Peer.PeerID, "role", peerJoined.Peer.Role)
		if uiLogf != nil {
			uiLogf("peer_joined peer_id=%s role=%s", peerJoined.Peer.PeerID, peerJoined.Peer.Role)
		}

		// Handle peer joined (will send direct offer if manifest exists)
		if targetPeer == "" || peerJoined.Peer.PeerID == targetPeer {
			if err := state.HandlePeerJoined(peerJoined.Peer.PeerID, peerJoined.Peer.Role); err != nil {
				logger.Error("failed to handle peer joined", "error", err)
			} else if targetOfferSent != nil {
				*targetOfferSent = true
			}
		}
		printReadiness()

	case protocol.TypePeerLeft:
		var peerLeft protocol.PeerLeft
		if err := env.DecodePayload(&peerLeft); err != nil {
			logger.Warn("failed to decode peer_left", "error", err)
			return
		}
		logger.Info("peer left", "peer_id", peerLeft.PeerID)
		if uiLogf != nil {
			uiLogf("peer_left peer_id=%s", peerLeft.PeerID)
		}

		// Remove peer from state
		if targetPeer == "" || peerLeft.PeerID == targetPeer {
			state.HandlePeerLeft(peerLeft.PeerID)
			if targetOfferSent != nil && peerLeft.PeerID == targetPeer {
				*targetOfferSent = false
			}
		}
		printReadiness()

	case protocol.TypeManifestAccept:
		var accept protocol.ManifestAccept
		if err := env.DecodePayload(&accept); err != nil {
			logger.Warn("failed to decode manifest_accept", "error", err)
			return
		}

		// Record acceptance
		state.HandleAccept(env.From, accept.ManifestID)
		ready := state.ReadyReceivers()
		total := state.TotalReceivers()
		logger.Info("manifest accepted", "peer_id", env.From, "manifest_id", accept.ManifestID, "mode", accept.Mode, "ready_count", len(ready), "total_receivers", total)

		// Print readiness change
		printReadiness()

	case protocol.TypeError:
		var errMsg protocol.Error
		if err := env.DecodePayload(&errMsg); err != nil {
			logger.Warn("failed to decode error", "error", err)
			return
		}
		logger.Warn("server error", "code", errMsg.Code, "message", errMsg.Message)

	default:
		logger.Debug("unknown message type", "type", env.Type)
	}
}

func runICETest(ctx context.Context, logger *slog.Logger, conn *wsclient.Conn, peerID, sessionID string) error {
	logger.Info("starting ICE connectivity test")

	// Wait for peer list to find receiver
	var receiverPeerID string
	peerListDone := make(chan struct{})
	var peerListOnce sync.Once
	markPeerList := func() {
		peerListOnce.Do(func() {
			close(peerListDone)
		})
	}

	// Track ICE state
	var icePeer *ice.ICEPeer
	var localCredsSent bool
	var remoteCredsReceived bool
	credsDone := make(chan struct{})
	var credsOnce sync.Once
	markCredsExchanged := func() {
		if localCredsSent && remoteCredsReceived {
			credsOnce.Do(func() {
				close(credsDone)
			})
		}
	}

	// Create ICE peer
	iceCfg := ice.ICEConfig{
		StunServers: []string{"stun:stun.l.google.com:19302"},
		Lite:        false,
	}
	var err error
	icePeer, err = ice.NewICEPeer(iceCfg, logger)
	if err != nil {
		return fmt.Errorf("failed to create ICE peer: %w", err)
	}
	defer icePeer.Close()

	// Queue for candidates gathered before we know the receiver
	var queuedCandidates []string
	var candidateCallback func(string)
	var candidateMu sync.Mutex

	icePeer.OnLocalCandidate(func(c string) {
		candidateMu.Lock()
		defer candidateMu.Unlock()
		if candidateCallback != nil {
			candidateCallback(c)
		} else {
			// Queue until we know the receiver
			queuedCandidates = append(queuedCandidates, c)
			logger.Debug("queued candidate (receiver not found yet)", "candidate", c)
		}
	})

	// Start gathering candidates
	if err := icePeer.StartGathering(ctx); err != nil {
		return fmt.Errorf("failed to start gathering: %w", err)
	}

	// Read loop for ICE test
	readCtx, readCancel := context.WithCancel(ctx)
	defer readCancel()

	readErrChan := make(chan error, 1)
	go func() {
		err := conn.ReadLoop(readCtx, func(env protocol.Envelope) {
			if err := env.ValidateBasic(); err != nil {
				logger.Warn("invalid envelope", "error", err)
				return
			}

			// Helper to set up receiver connection
			setupReceiver := func(recvPeerID string) {
				if receiverPeerID != "" {
					return // Already have receiver
				}

				receiverPeerID = recvPeerID
				logger.Info("receiver found", "peer_id", receiverPeerID)

				// Define the actual send function
				sendCandidate := func(c string) {
					candidateMsg := protocol.IceCandidate{Candidate: c}
					env, err := protocol.NewEnvelope(protocol.TypeIceCandidate, protocol.NewMsgID(), candidateMsg)
					if err != nil {
						logger.Error("failed to create candidate envelope", "error", err)
						return
					}
					env.SessionID = sessionID
					env.From = peerID
					env.To = receiverPeerID
					if err := conn.Send(env); err != nil {
						logger.Error("failed to send candidate", "error", err)
					} else {
						logger.Debug("sent local candidate", "candidate", c)
					}
				}

				// Set up candidate callback and flush queued candidates
				candidateMu.Lock()
				candidateCallback = sendCandidate
				queued := queuedCandidates
				queuedCandidates = nil
				candidateMu.Unlock()

				// Flush queued candidates
				for _, c := range queued {
					logger.Debug("flushing queued candidate", "candidate", c)
					sendCandidate(c)
				}

				// Send local credentials
				ufrag, pwd := icePeer.LocalCredentials()
				credsMsg := protocol.IceCredentials{Ufrag: ufrag, Pwd: pwd}
				credsEnv, err := protocol.NewEnvelope(protocol.TypeIceCredentials, protocol.NewMsgID(), credsMsg)
				if err != nil {
					logger.Error("failed to create credentials envelope", "error", err)
					return
				}
				credsEnv.SessionID = sessionID
				credsEnv.From = peerID
				credsEnv.To = receiverPeerID
				if err := conn.Send(credsEnv); err != nil {
					logger.Error("failed to send credentials", "error", err)
					return
				}
				localCredsSent = true
				logger.Info("sent local ICE credentials", "ufrag", ufrag)
				markCredsExchanged()

				markPeerList() // Signal once when we found receiver and sent credentials
			}

			switch env.Type {
			case protocol.TypePeerList:
				var peerList protocol.PeerList
				if err := env.DecodePayload(&peerList); err != nil {
					logger.Warn("failed to decode peer_list", "error", err)
					return
				}

				// Find receiver
				for _, p := range peerList.Peers {
					if p.Role == "receiver" {
						setupReceiver(p.PeerID)
						return
					}
				}

				logger.Warn("no receivers found, waiting...")

			case protocol.TypePeerJoined:
				var peerJoined protocol.PeerJoined
				if err := env.DecodePayload(&peerJoined); err != nil {
					logger.Warn("failed to decode peer_joined", "error", err)
					return
				}

				if peerJoined.Peer.Role == "receiver" {
					setupReceiver(peerJoined.Peer.PeerID)
				}

			case protocol.TypeIceCredentials:
				var creds protocol.IceCredentials
				if err := env.DecodePayload(&creds); err != nil {
					logger.Warn("failed to decode ice_credentials", "error", err)
					return
				}

				if remoteCredsReceived {
					logger.Debug("ignoring duplicate credentials")
					return
				}

				// If we haven't found receiver yet, use the sender of this message
				if receiverPeerID == "" && env.From != "" {
					setupReceiver(env.From)
				}

				if err := icePeer.AddRemoteCredentials(creds.Ufrag, creds.Pwd); err != nil {
					logger.Error("failed to add remote credentials", "error", err)
					return
				}
				remoteCredsReceived = true
				logger.Info("received remote ICE credentials", "ufrag", creds.Ufrag)

				// If we've exchanged credentials, signal ready to connect
				markCredsExchanged()

			case protocol.TypeIceCandidate:
				var candidate protocol.IceCandidate
				if err := env.DecodePayload(&candidate); err != nil {
					logger.Warn("failed to decode ice_candidate", "error", err)
					return
				}

				if err := icePeer.AddRemoteCandidate(candidate.Candidate); err != nil {
					logger.Warn("failed to add remote candidate", "error", err, "candidate", candidate.Candidate)
					return
				}
				logger.Debug("received remote candidate", "candidate", candidate.Candidate)
			}
		})
		if err != nil && err != context.Canceled {
			readErrChan <- err
		}
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-peerListDone:
		// Got peer list
	case err := <-readErrChan:
		return err
	case <-time.After(30 * time.Second):
		return fmt.Errorf("timeout waiting for peer list")
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-credsDone:
		// Credentials exchanged
	case err := <-readErrChan:
		return err
	case <-time.After(30 * time.Second):
		return fmt.Errorf("timeout waiting for credentials exchange")
	}

	// Give some time for candidates to be exchanged
	// Wait a bit longer to ensure candidates are processed
	logger.Info("waiting for candidates to be exchanged...")
	time.Sleep(5 * time.Second)

	// Verify we have both credentials before connecting
	if !localCredsSent {
		return fmt.Errorf("local credentials not sent")
	}
	if !remoteCredsReceived {
		return fmt.Errorf("remote credentials not received")
	}

	// Connect
	logger.Info("attempting ICE connection...")
	connectCtx, connectCancel := context.WithTimeout(ctx, 30*time.Second)
	defer connectCancel()
	iceConn, err := icePeer.Connect(connectCtx)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	defer iceConn.Close()

	logger.Info("ICE connection established, sending ping...")

	// Send ping
	pingMsg := []byte("ping")
	if _, err := iceConn.Write(pingMsg); err != nil {
		return fmt.Errorf("failed to send ping: %w", err)
	}

	// Read pong
	buf := make([]byte, 1024)
	iceConn.SetReadDeadline(time.Now().Add(10 * time.Second))
	n, err := iceConn.Read(buf)
	if err != nil {
		return fmt.Errorf("failed to read pong: %w", err)
	}

	response := string(buf[:n])
	if response != "pong" {
		return fmt.Errorf("unexpected response: %s", response)
	}

	logger.Info("ICE test successful: ping/pong completed")
	return nil
}

func runQUICTest(ctx context.Context, logger *slog.Logger, conn *wsclient.Conn, peerID, sessionID string) error {
	logger.Info("starting QUIC connectivity test")

	// First, establish ICE connection (similar to runICETest)
	// Wait for peer list to find receiver
	var receiverPeerID string
	peerListDone := make(chan struct{})
	var peerListOnce sync.Once
	markPeerList := func() {
		peerListOnce.Do(func() {
			close(peerListDone)
		})
	}

	// Track ICE state
	var icePeer *ice.ICEPeer
	var localCredsSent bool
	var remoteCredsReceived bool
	credsDone := make(chan struct{})
	var credsOnce sync.Once
	markCredsExchanged := func() {
		if localCredsSent && remoteCredsReceived {
			credsOnce.Do(func() {
				close(credsDone)
			})
		}
	}

	// Create ICE peer
	iceCfg := ice.ICEConfig{
		StunServers: []string{"stun:stun.l.google.com:19302"},
		Lite:        false,
	}
	var err error
	icePeer, err = ice.NewICEPeer(iceCfg, logger)
	if err != nil {
		return fmt.Errorf("failed to create ICE peer: %w", err)
	}
	defer icePeer.Close()

	// Queue for candidates gathered before we know the receiver
	var queuedCandidates []string
	var candidateCallback func(string)
	var candidateMu sync.Mutex

	icePeer.OnLocalCandidate(func(c string) {
		candidateMu.Lock()
		defer candidateMu.Unlock()
		if candidateCallback != nil {
			candidateCallback(c)
		} else {
			// Queue until we know the receiver
			queuedCandidates = append(queuedCandidates, c)
			logger.Debug("queued candidate (receiver not found yet)", "candidate", c)
		}
	})

	// Start gathering candidates
	if err := icePeer.StartGathering(ctx); err != nil {
		return fmt.Errorf("failed to start gathering: %w", err)
	}

	// Read loop for ICE test
	readCtx, readCancel := context.WithCancel(ctx)
	defer readCancel()

	readErrChan := make(chan error, 1)
	go func() {
		err := conn.ReadLoop(readCtx, func(env protocol.Envelope) {
			if err := env.ValidateBasic(); err != nil {
				logger.Warn("invalid envelope", "error", err)
				return
			}

			// Helper to set up receiver connection
			setupReceiver := func(recvPeerID string) {
				if receiverPeerID != "" {
					return // Already have receiver
				}

				receiverPeerID = recvPeerID
				logger.Info("receiver found", "peer_id", receiverPeerID)

				// Define the actual send function
				sendCandidate := func(c string) {
					candidateMsg := protocol.IceCandidate{Candidate: c}
					env, err := protocol.NewEnvelope(protocol.TypeIceCandidate, protocol.NewMsgID(), candidateMsg)
					if err != nil {
						logger.Error("failed to create candidate envelope", "error", err)
						return
					}
					env.SessionID = sessionID
					env.From = peerID
					env.To = receiverPeerID
					if err := conn.Send(env); err != nil {
						logger.Error("failed to send candidate", "error", err)
					} else {
						logger.Debug("sent local candidate", "candidate", c)
					}
				}

				// Set up candidate callback and flush queued candidates
				candidateMu.Lock()
				candidateCallback = sendCandidate
				queued := queuedCandidates
				queuedCandidates = nil
				candidateMu.Unlock()

				// Flush queued candidates
				for _, c := range queued {
					logger.Debug("flushing queued candidate", "candidate", c)
					sendCandidate(c)
				}

				// Send local credentials
				ufrag, pwd := icePeer.LocalCredentials()
				credsMsg := protocol.IceCredentials{Ufrag: ufrag, Pwd: pwd}
				credsEnv, err := protocol.NewEnvelope(protocol.TypeIceCredentials, protocol.NewMsgID(), credsMsg)
				if err != nil {
					logger.Error("failed to create credentials envelope", "error", err)
					return
				}
				credsEnv.SessionID = sessionID
				credsEnv.From = peerID
				credsEnv.To = receiverPeerID
				if err := conn.Send(credsEnv); err != nil {
					logger.Error("failed to send credentials", "error", err)
					return
				}
				localCredsSent = true
				logger.Info("sent local ICE credentials", "ufrag", ufrag)
				markCredsExchanged()

				markPeerList() // Signal once when we found receiver and sent credentials
			}

			switch env.Type {
			case protocol.TypePeerList:
				var peerList protocol.PeerList
				if err := env.DecodePayload(&peerList); err != nil {
					logger.Warn("failed to decode peer_list", "error", err)
					return
				}

				// Find receiver
				for _, p := range peerList.Peers {
					if p.Role == "receiver" {
						setupReceiver(p.PeerID)
						return
					}
				}

				logger.Warn("no receivers found, waiting...")

			case protocol.TypePeerJoined:
				var peerJoined protocol.PeerJoined
				if err := env.DecodePayload(&peerJoined); err != nil {
					logger.Warn("failed to decode peer_joined", "error", err)
					return
				}

				if peerJoined.Peer.Role == "receiver" {
					setupReceiver(peerJoined.Peer.PeerID)
				}

			case protocol.TypeIceCredentials:
				var creds protocol.IceCredentials
				if err := env.DecodePayload(&creds); err != nil {
					logger.Warn("failed to decode ice_credentials", "error", err)
					return
				}

				if remoteCredsReceived {
					logger.Debug("ignoring duplicate credentials")
					return
				}

				// If we haven't found receiver yet, use the sender of this message
				if receiverPeerID == "" && env.From != "" {
					setupReceiver(env.From)
				}

				if err := icePeer.AddRemoteCredentials(creds.Ufrag, creds.Pwd); err != nil {
					logger.Error("failed to add remote credentials", "error", err)
					return
				}
				remoteCredsReceived = true
				logger.Info("received remote ICE credentials", "ufrag", creds.Ufrag)

				// If we've exchanged credentials, signal ready to connect
				markCredsExchanged()

			case protocol.TypeIceCandidate:
				var candidate protocol.IceCandidate
				if err := env.DecodePayload(&candidate); err != nil {
					logger.Warn("failed to decode ice_candidate", "error", err)
					return
				}

				if err := icePeer.AddRemoteCandidate(candidate.Candidate); err != nil {
					logger.Warn("failed to add remote candidate", "error", err, "candidate", candidate.Candidate)
					return
				}
				logger.Debug("received remote candidate", "candidate", candidate.Candidate)
			}
		})
		if err != nil && err != context.Canceled {
			readErrChan <- err
		}
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-peerListDone:
		// Got peer list
	case err := <-readErrChan:
		return err
	case <-time.After(30 * time.Second):
		return fmt.Errorf("timeout waiting for peer list")
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-credsDone:
		// Credentials exchanged
	case err := <-readErrChan:
		return err
	case <-time.After(30 * time.Second):
		return fmt.Errorf("timeout waiting for credentials exchange")
	}

	// Give some time for candidates to be exchanged
	logger.Info("waiting for candidates to be exchanged...")
	time.Sleep(5 * time.Second)

	// Verify we have both credentials before connecting
	if !localCredsSent {
		return fmt.Errorf("local credentials not sent")
	}
	if !remoteCredsReceived {
		return fmt.Errorf("remote credentials not received")
	}

	// Establish ICE connection (sender uses Connect/Dial)
	logger.Info("establishing ICE connection...")
	connectCtx, connectCancel := context.WithTimeout(ctx, 30*time.Second)
	defer connectCancel()
	_, err = icePeer.Connect(connectCtx)
	if err != nil {
		return fmt.Errorf("failed to establish ICE connection: %w", err)
	}

	logger.Info("ICE connection established, setting up QUIC client...")

	// Get PacketConn info from ICE before closing
	localAddr, remoteAddr, err := icePeer.PacketConnInfo()
	if err != nil {
		return fmt.Errorf("failed to get PacketConn info: %w", err)
	}

	// Do NOT close iceConn here. We use the underlying socket for QUIC.
	// iceConn.Close()

	// Create PacketConn for QUIC (will bind to same address as ICE)
	udpConn, err := icePeer.CreatePacketConn()
	if err != nil {
		return fmt.Errorf("failed to create PacketConn: %w", err)
	}
	defer udpConn.Close()

	logger.Info("QUIC client dialing", "local_addr", localAddr, "remote_addr", remoteAddr)

	// Dial QUIC connection
	quicConn, err := quictransport.Dial(ctx, udpConn, remoteAddr, logger)
	if err != nil {
		return fmt.Errorf("failed to dial QUIC connection: %w", err)
	}
	defer quicConn.CloseWithError(0, "")

	logger.Info("QUIC connection established, opening stream...")

	// Open a bidirectional stream
	stream, err := quicConn.OpenStreamSync(ctx)
	if err != nil {
		return fmt.Errorf("failed to open stream: %w", err)
	}
	defer stream.Close()

	logger.Info("stream opened, sending ping...")

	// Send "ping"
	if _, err := stream.Write([]byte("ping")); err != nil {
		return fmt.Errorf("failed to send ping: %w", err)
	}

	logger.Info("ping sent, waiting for pong...")

	// Read "pong"
	buf := make([]byte, 4)
	n, err := stream.Read(buf)
	if err != nil {
		return fmt.Errorf("failed to read pong: %w", err)
	}

	response := string(buf[:n])
	if response != "pong" {
		return fmt.Errorf("unexpected response: %s", response)
	}

	logger.Info("QUIC test successful: ping/pong completed")
	return nil
}

func runQUICTransferTest(ctx context.Context, logger *slog.Logger, conn *wsclient.Conn, peerID, sessionID string, paths []string, chunkSize uint32, multiStream bool, parallelFiles int, smallThreshold int64, mediumThreshold int64, smallSlotFrac float64, agingAfter time.Duration, resume bool, resumeTimeout time.Duration, resumeVerify string, hashAlg string, resumeVerifyTail uint32, verbose bool) error {
	logger.Info("starting QUIC transfer test", "paths", paths)
	iceLog := func(format string, args ...any) {
		if !verbose {
			fmt.Printf(format, args...)
		}
	}

	// Scan paths and create manifest
	var m manifest.Manifest
	var err error
	if len(paths) > 1 {
		m, err = manifest.ScanPaths(paths)
	} else if len(paths) == 1 {
		m, err = manifest.Scan(paths[0])
	} else {
		m, err = manifest.Scan(".")
	}
	if err != nil {
		return fmt.Errorf("failed to scan paths: %w", err)
	}

	logger.Info("manifest created",
		"root", m.Root,
		"file_count", len(m.Items),
		"total_bytes", m.TotalBytes)

	// Establish ICE+QUIC connection (reuse logic from runQUICTest)
	var receiverPeerID string
	peerListDone := make(chan struct{})
	var peerListOnce sync.Once
	markPeerList := func() {
		peerListOnce.Do(func() {
			close(peerListDone)
		})
	}

	var icePeer *ice.ICEPeer
	var localCredsSent bool
	var remoteCredsReceived bool
	credsDone := make(chan struct{})
	var credsOnce sync.Once
	markCredsExchanged := func() {
		if localCredsSent && remoteCredsReceived {
			credsOnce.Do(func() {
				close(credsDone)
			})
		}
	}

	iceCfg := ice.ICEConfig{
		StunServers: []string{"stun:stun.l.google.com:19302"},
		Lite:        false,
	}
	icePeer, err = ice.NewICEPeer(iceCfg, logger)
	if err != nil {
		return fmt.Errorf("failed to create ICE peer: %w", err)
	}
	defer icePeer.Close()

	var queuedCandidates []string
	var candidateCallback func(string)
	var candidateMu sync.Mutex

	icePeer.OnLocalCandidate(func(c string) {
		candidateMu.Lock()
		defer candidateMu.Unlock()
		if candidateCallback != nil {
			candidateCallback(c)
		} else {
			queuedCandidates = append(queuedCandidates, c)
			logger.Debug("queued candidate (receiver not found yet)", "candidate", c)
		}
	})

	if err := icePeer.StartGathering(ctx); err != nil {
		return fmt.Errorf("failed to start gathering: %w", err)
	}

	readCtx, readCancel := context.WithCancel(ctx)
	defer readCancel()

	readErrChan := make(chan error, 1)
	go func() {
		err := conn.ReadLoop(readCtx, func(env protocol.Envelope) {
			if err := env.ValidateBasic(); err != nil {
				logger.Warn("invalid envelope", "error", err)
				return
			}

			setupReceiver := func(recvPeerID string) {
				if receiverPeerID != "" {
					return
				}

				receiverPeerID = recvPeerID
				logger.Info("receiver found", "peer_id", receiverPeerID)
				iceLog("ice stage=peer_found peer_id=%s\n", receiverPeerID)

				sendCandidate := func(c string) {
					candidateMsg := protocol.IceCandidate{Candidate: c}
					env, err := protocol.NewEnvelope(protocol.TypeIceCandidate, protocol.NewMsgID(), candidateMsg)
					if err != nil {
						logger.Error("failed to create candidate envelope", "error", err)
						return
					}
					env.SessionID = sessionID
					env.From = peerID
					env.To = receiverPeerID
					if err := conn.Send(env); err != nil {
						logger.Error("failed to send candidate", "error", err)
					} else {
						logger.Debug("sent local candidate", "candidate", c)
					}
				}

				candidateMu.Lock()
				candidateCallback = sendCandidate
				queued := queuedCandidates
				queuedCandidates = nil
				candidateMu.Unlock()

				for _, c := range queued {
					logger.Debug("flushing queued candidate", "candidate", c)
					sendCandidate(c)
				}

				ufrag, pwd := icePeer.LocalCredentials()
				credsMsg := protocol.IceCredentials{Ufrag: ufrag, Pwd: pwd}
				credsEnv, err := protocol.NewEnvelope(protocol.TypeIceCredentials, protocol.NewMsgID(), credsMsg)
				if err != nil {
					logger.Error("failed to create credentials envelope", "error", err)
					return
				}
				credsEnv.SessionID = sessionID
				credsEnv.From = peerID
				credsEnv.To = receiverPeerID
				if err := conn.Send(credsEnv); err != nil {
					logger.Error("failed to send credentials", "error", err)
					return
				}
				localCredsSent = true
				logger.Info("sent local ICE credentials", "ufrag", ufrag)
				markCredsExchanged()
				iceLog("ice stage=local_creds_sent ufrag=%s\n", ufrag)

				markPeerList()
			}

			switch env.Type {
			case protocol.TypePeerList:
				var peerList protocol.PeerList
				if err := env.DecodePayload(&peerList); err != nil {
					logger.Warn("failed to decode peer_list", "error", err)
					return
				}

				for _, p := range peerList.Peers {
					if p.Role == "receiver" {
						setupReceiver(p.PeerID)
						return
					}
				}

				logger.Warn("no receivers found, waiting...")

			case protocol.TypePeerJoined:
				var peerJoined protocol.PeerJoined
				if err := env.DecodePayload(&peerJoined); err != nil {
					logger.Warn("failed to decode peer_joined", "error", err)
					return
				}

				if peerJoined.Peer.Role == "receiver" {
					setupReceiver(peerJoined.Peer.PeerID)
				}

			case protocol.TypeIceCredentials:
				var creds protocol.IceCredentials
				if err := env.DecodePayload(&creds); err != nil {
					logger.Warn("failed to decode ice_credentials", "error", err)
					return
				}

				if remoteCredsReceived {
					logger.Debug("ignoring duplicate credentials")
					return
				}

				if receiverPeerID == "" && env.From != "" {
					setupReceiver(env.From)
				}

				if err := icePeer.AddRemoteCredentials(creds.Ufrag, creds.Pwd); err != nil {
					logger.Error("failed to add remote credentials", "error", err)
					return
				}
				remoteCredsReceived = true
				logger.Info("received remote ICE credentials", "ufrag", creds.Ufrag)
				iceLog("ice stage=remote_creds_received ufrag=%s\n", creds.Ufrag)

				markCredsExchanged()

			case protocol.TypeIceCandidate:
				var candidate protocol.IceCandidate
				if err := env.DecodePayload(&candidate); err != nil {
					logger.Warn("failed to decode ice_candidate", "error", err)
					return
				}

				if err := icePeer.AddRemoteCandidate(candidate.Candidate); err != nil {
					logger.Warn("failed to add remote candidate", "error", err, "candidate", candidate.Candidate)
					return
				}
				logger.Debug("received remote candidate", "candidate", candidate.Candidate)
			}
		})
		if err != nil && err != context.Canceled {
			readErrChan <- err
		}
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-peerListDone:
	case err := <-readErrChan:
		return err
	case <-time.After(30 * time.Second):
		return fmt.Errorf("timeout waiting for peer list")
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-credsDone:
	case err := <-readErrChan:
		return err
	case <-time.After(30 * time.Second):
		return fmt.Errorf("timeout waiting for credentials exchange")
	}

	logger.Info("waiting for candidates to be exchanged...")
	time.Sleep(5 * time.Second)

	if !localCredsSent {
		return fmt.Errorf("local credentials not sent")
	}
	if !remoteCredsReceived {
		return fmt.Errorf("remote credentials not received")
	}

	logger.Info("establishing ICE connection...")
	iceLog("ice stage=connect_start role=sender\n")
	connectCtx, connectCancel := context.WithTimeout(ctx, 30*time.Second)
	defer connectCancel()
	_, err = icePeer.Connect(connectCtx)
	if err != nil {
		return fmt.Errorf("failed to establish ICE connection: %w", err)
	}
	iceLog("ice stage=connect_ok role=sender\n")

	logger.Info("ICE connection established, setting up QUIC client...")

	localAddr, remoteAddr, err := icePeer.PacketConnInfo()
	if err != nil {
		return fmt.Errorf("failed to get PacketConn info: %w", err)
	}

	// Do NOT close iceConn here. We use the underlying socket for QUIC.
	// iceConn.Close()

	udpConn, err := icePeer.CreatePacketConn()
	if err != nil {
		return fmt.Errorf("failed to create PacketConn: %w", err)
	}
	defer udpConn.Close()

	logger.Info("QUIC client dialing", "local_addr", localAddr, "remote_addr", remoteAddr)

	quicConn, err := quictransport.Dial(ctx, udpConn, remoteAddr, logger)
	if err != nil {
		return fmt.Errorf("failed to dial QUIC connection: %w", err)
	}
	defer quicConn.CloseWithError(0, "")

	logger.Info("QUIC connection established, setting up transfer...")

	// Create QUIC transport as dialer
	quicTransport := transferquic.NewDialer(quicConn, logger)
	defer quicTransport.Close()

	// Dial to get a Conn
	transferConn, err := quicTransport.Dial(ctx, receiverPeerID)
	if err != nil {
		return fmt.Errorf("failed to dial transfer connection: %w", err)
	}
	defer transferConn.Close()

	useMultiStream := multiStream
	if useMultiStream {
		logger.Info("transfer connection ready, starting multi-stream transfer...")
	} else {
		logger.Info("transfer connection ready, opening stream...")
	}

	// Determine root path for SendManifest
	// For SendManifest, we need the actual root path where files are located
	// If multiple paths, manifest.ScanPaths already handled the structure
	// We just need to pass "." as rootPath since ScanPaths creates the structure
	rootPath := "."
	if len(paths) == 1 {
		// For single path, use that path as root
		rootPath = paths[0]
	}
	// For multiple paths, rootPath stays "." since ScanPaths creates the structure

	// Set up progress logging with throughput stats
	var totalBytesSent int64
	var currentFile string
	startTime := time.Now()
	var progressMu sync.Mutex

	// Start throughput logging goroutine
	progressCtx, progressCancel := context.WithCancel(ctx)
	defer progressCancel()

	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-progressCtx.Done():
				return
			case <-ticker.C:
				progressMu.Lock()
				sent := totalBytesSent
				file := currentFile
				progressMu.Unlock()

				elapsed := time.Since(startTime).Seconds()
				if elapsed > 0 {
					mbps := float64(sent) / (1024 * 1024) / elapsed
					if verbose {
						logger.Info("transfer progress",
							"sent_bytes", sent,
							"mbps", fmt.Sprintf("%.2f", mbps),
							"current_file", file,
							"pool_buf_size", chunkSize)
					} else {
						fmt.Printf("progress sent=%d bytes mbps=%s file=%s\n", sent, fmt.Sprintf("%.2f", mbps), file)
					}
				}
			}
		}
	}()

	// Track total bytes sent across all files
	progressFn := func(relpath string, bytesSent int64, total int64) {
		progressMu.Lock()
		totalBytesSent = bytesSent // This is cumulative per file, but we'll track it per file
		currentFile = relpath
		progressMu.Unlock()
	}

	logger.Info("transfer configuration",
		"chunk_size", chunkSize,
		"pool_buf_size", chunkSize)

	if useMultiStream {
		transferStatsFn := func(activeFiles, completedFiles int, remainingBytes int64) {
			if verbose {
				logger.Info("transfer status",
					"active_files", activeFiles,
					"completed_files", completedFiles,
					"remaining_bytes", remainingBytes)
			}
		}
		resumeStatsFn := func(relpath string, skippedChunks, totalChunks uint32, verifiedChunk uint32, totalBytes int64, chunkSize uint32) {
			if verbose {
				logger.Info("resume stats", "relpath", relpath, "skipped_chunks", skippedChunks, "total_chunks", totalChunks, "verified_chunk", verifiedChunk)
				return
			}
			status := "fresh"
			if totalChunks > 0 && skippedChunks == totalChunks {
				status = "skipped_complete"
			} else if skippedChunks > 0 {
				status = "resumed_partial"
			}
			fmt.Printf("resume file=%s status=%s skipped=%d/%d verified=%d\n", relpath, status, skippedChunks, totalChunks, verifiedChunk)
		}
		if err := transfer.SendManifestMultiStream(ctx, transferConn, rootPath, m, transfer.Options{
			ChunkSize:        chunkSize,
			ParallelFiles:    parallelFiles,
			SmallThreshold:   smallThreshold,
			MediumThreshold:  mediumThreshold,
			SmallSlotFrac:    smallSlotFrac,
			AgingAfter:       agingAfter,
			Resume:           resume,
			ResumeTimeout:    resumeTimeout,
			ResumeVerify:     resumeVerify,
			HashAlg:          hashAlg,
			ResumeVerifyTail: resumeVerifyTail,
			ProgressFn:       progressFn,
			TransferStatsFn:  transferStatsFn,
			ResumeStatsFn:    resumeStatsFn,
		}); err != nil {
			return fmt.Errorf("failed to send manifest: %w", err)
		}
	} else {
		stream, err := transferConn.OpenStream(ctx)
		if err != nil {
			return fmt.Errorf("failed to open stream: %w", err)
		}
		defer stream.Close()

		logger.Info("stream opened, sending manifest...")

		if err := transfer.SendManifest(ctx, stream, rootPath, m, chunkSize, progressFn); err != nil {
			return fmt.Errorf("failed to send manifest: %w", err)
		}
	}

	progressCancel() // Stop progress logging

	logger.Info("transfer complete",
		"root", m.Root,
		"file_count", len(m.Items),
		"total_bytes", m.TotalBytes)

	// The protocol has an END marker (0xFF), so receiver knows when done.
	// Wait briefly to ensure receiver processes the END marker before we close.
	time.Sleep(500 * time.Millisecond)

	fmt.Printf("\n=== Transfer Complete ===\n")
	fmt.Printf("Selected paths: %v\n", paths)
	fmt.Printf("Manifest root: %s\n", m.Root)
	fmt.Printf("Files sent: %d\n", len(m.Items))
	fmt.Printf("Total bytes: %d\n", m.TotalBytes)
	fmt.Printf("========================\n\n")

	return nil
}
