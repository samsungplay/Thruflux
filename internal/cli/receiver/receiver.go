package receiver

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sheerbytes/sheerbytes/internal/app"
	"github.com/sheerbytes/sheerbytes/internal/ice"
	"github.com/sheerbytes/sheerbytes/internal/logging"
	"github.com/sheerbytes/sheerbytes/internal/quictransport"
	"github.com/sheerbytes/sheerbytes/internal/transfer"
	"github.com/sheerbytes/sheerbytes/internal/transferquic"
	"github.com/sheerbytes/sheerbytes/internal/wsclient"
	"github.com/sheerbytes/sheerbytes/pkg/manifest"
	"github.com/sheerbytes/sheerbytes/pkg/protocol"
)

const receiverDefaultServerURL = "https://bytepipe.app"

var receiverDefaultStunServers = []string{
	"stun:stun.l.google.com:19302",
	"stun:stun.cloudflare.com:3478",
	"stun:stun.bytepipe.app:3478",
}

func Run(args []string) {
	if len(args) == 0 {
		printReceiverUsage()
		os.Exit(2)
	}
	if hasHelpFlag(args) {
		printReceiverUsage()
		return
	}

	joinCode := ""
	outDir := ""
	serverURL := ""
	benchmark := false
	udpReadBufferBytes := 8 * 1024 * 1024
	udpWriteBufferBytes := 8 * 1024 * 1024
	quicConnWindowBytes := 64 * 1024 * 1024
	quicStreamWindowBytes := 16 * 1024 * 1024
	quicMaxIncomingStreams := 100
	stunServers := make([]string, 0)
	turnServers := make([]string, 0)
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--out" && i+1 < len(args) {
			i++
			outDir = args[i]
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
		if strings.HasPrefix(arg, "--") {
			fmt.Fprintf(os.Stderr, "unknown flag: %s\n", arg)
			printReceiverUsage()
			os.Exit(2)
		}
		if joinCode == "" {
			joinCode = arg
			continue
		}
		fmt.Fprintln(os.Stderr, "unexpected extra argument")
		printReceiverUsage()
		os.Exit(2)
	}
	if joinCode == "" {
		printReceiverUsage()
		os.Exit(2)
	}
	if outDir == "" {
		outDir = "."
	}

	if strings.TrimSpace(serverURL) == "" {
		serverURL = receiverDefaultServerURL
	}
	if len(stunServers) == 0 {
		stunServers = append([]string{}, receiverDefaultStunServers...)
	}

	logLevel := "error"
	logger := logging.New("thruflux-receiver", logLevel)
	if err := app.RunSnapshotReceiver(context.Background(), logger, app.SnapshotReceiverConfig{
		ServerURL:              serverURL,
		JoinCode:               joinCode,
		OutDir:                 outDir,
		Benchmark:              benchmark,
		UDPReadBufferBytes:     udpReadBufferBytes,
		UDPWriteBufferBytes:    udpWriteBufferBytes,
		QuicConnWindowBytes:    quicConnWindowBytes,
		QuicStreamWindowBytes:  quicStreamWindowBytes,
		QuicMaxIncomingStreams: quicMaxIncomingStreams,
		StunServers:            stunServers,
		TurnServers:            turnServers,
	}); err != nil {
		logger.Error("snapshot receiver failed", "error", err)
		os.Exit(1)
	}
}

func printReceiverUsage() {
	fmt.Fprintln(os.Stderr, "usage: thru join <join-code> [--out DIR] [--server-url URL] [--benchmark]")
	fmt.Fprintln(os.Stderr, "  --out DIR                   output directory (default .)")
	fmt.Fprintln(os.Stderr, "  --server-url URL            signaling server URL (default https://bytepipe.app)")
	fmt.Fprintf(os.Stderr, "  --stun-server URLS          STUN server URLs (comma-separated, default %s)\n", strings.Join(ice.DefaultStunServers, ","))
	fmt.Fprintln(os.Stderr, "                              example: --stun-server stun:stun.l.google.com:19302")
	fmt.Fprintln(os.Stderr, "  --turn-server URLS          TURN server URLs (comma-separated, default none)")
	fmt.Fprintln(os.Stderr, "                              example: --turn-server turn:username:password@turn.example.com:3478?transport=udp")
	fmt.Fprintln(os.Stderr, "  --benchmark                 enable benchmark stats")
	fmt.Fprintln(os.Stderr, "  --udp-read-buffer-bytes N   UDP read buffer size (default 8388608)")
	fmt.Fprintln(os.Stderr, "  --udp-write-buffer-bytes N  UDP write buffer size (default 8388608)")
	fmt.Fprintln(os.Stderr, "  --quic-conn-window-bytes N  QUIC connection window (default 67108864)")
	fmt.Fprintln(os.Stderr, "  --quic-stream-window-bytes N QUIC stream window (default 16777216)")
	fmt.Fprintln(os.Stderr, "  --quic-max-incoming-streams N max QUIC incoming streams (default 100)")
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

func handleEnvelope(env protocol.Envelope, logger *slog.Logger, conn *wsclient.Conn, mu *sync.Mutex, senderPeerID *string, pendingAccept **protocol.ManifestAccept, receiverPeerID, sessionID string) {
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
		senderFound := false
		for i, p := range peerList.Peers {
			peerIDs[i] = fmt.Sprintf("%s (%s)", p.PeerID, p.Role)
			// Cache sender peer_id (track updates reliably)
			if p.Role == "sender" {
				senderFound = true
				mu.Lock()
				*senderPeerID = p.PeerID
				// Send pending accept if we have one
				if *pendingAccept != nil {
					accept := *pendingAccept
					*pendingAccept = nil
					mu.Unlock()
					sendAccept(conn, logger, *senderPeerID, accept.ManifestID, env.SessionID, receiverPeerID)
				} else {
					mu.Unlock()
				}
			}
		}
		logger.Info("peer list received", "count", len(peerList.Peers), "peers", strings.Join(peerIDs, ", "))
		// Warn if sender not found in peer list (but don't clear existing sender)
		if !senderFound && *senderPeerID == "" {
			logger.Warn("no sender found in peer list")
		}

	case protocol.TypePeerJoined:
		var peerJoined protocol.PeerJoined
		if err := env.DecodePayload(&peerJoined); err != nil {
			logger.Warn("failed to decode peer_joined", "error", err)
			return
		}
		logger.Info("peer joined", "peer_id", peerJoined.Peer.PeerID, "role", peerJoined.Peer.Role)
		// Cache sender peer_id if this is the sender
		if peerJoined.Peer.Role == "sender" {
			mu.Lock()
			*senderPeerID = peerJoined.Peer.PeerID
			// Send pending accept if we have one
			if *pendingAccept != nil {
				accept := *pendingAccept
				*pendingAccept = nil
				mu.Unlock()
				sendAccept(conn, logger, *senderPeerID, accept.ManifestID, env.SessionID, receiverPeerID)
			} else {
				mu.Unlock()
			}
		}

	case protocol.TypePeerLeft:
		var peerLeft protocol.PeerLeft
		if err := env.DecodePayload(&peerLeft); err != nil {
			logger.Warn("failed to decode peer_left", "error", err)
			return
		}
		logger.Info("peer left", "peer_id", peerLeft.PeerID)
		// Clear sender if it left and warn
		if peerLeft.PeerID == *senderPeerID {
			mu.Lock()
			*senderPeerID = ""
			mu.Unlock()
			logger.Warn("sender disappeared", "peer_id", peerLeft.PeerID)
		}

	case protocol.TypeManifestOffer:
		var offer protocol.ManifestOffer
		if err := env.DecodePayload(&offer); err != nil {
			logger.Warn("failed to decode manifest_offer", "error", err)
			return
		}
		summary := offer.Summary
		logger.Info("manifest offer received",
			"manifest_id", summary.ManifestID,
			"root", summary.RootName,
			"total_bytes", summary.TotalBytes,
			"file_count", summary.FileCount,
			"folder_count", summary.FolderCount)

		// Create accept response
		accept := protocol.ManifestAccept{
			ManifestID:    summary.ManifestID,
			Mode:          "all",
			SelectedPaths: nil,
		}

		// Use env.From as sender peer_id (offer always comes from sender)
		// Fall back to cached sender if env.From is empty (shouldn't happen, but be defensive)
		sender := env.From
		if sender == "" {
			mu.Lock()
			sender = *senderPeerID
			mu.Unlock()
		} else {
			// Update cached sender peer_id from offer
			mu.Lock()
			*senderPeerID = sender
			mu.Unlock()
		}

		if sender != "" {
			sendAccept(conn, logger, sender, accept.ManifestID, env.SessionID, receiverPeerID)
		} else {
			// Store pending accept (shouldn't happen, but be defensive)
			mu.Lock()
			*pendingAccept = &accept
			mu.Unlock()
			logger.Warn("sender not known yet, accept pending")
		}

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

func sendAccept(conn *wsclient.Conn, logger *slog.Logger, senderPeerID, manifestID, sessionID, receiverPeerID string) {
	accept := protocol.ManifestAccept{
		ManifestID:    manifestID,
		Mode:          "all",
		SelectedPaths: nil,
	}
	acceptEnv, err := protocol.NewEnvelope(protocol.TypeManifestAccept, protocol.NewMsgID(), accept)
	if err != nil {
		logger.Error("failed to create manifest accept", "error", err)
		return
	}
	acceptEnv.SessionID = sessionID
	acceptEnv.From = receiverPeerID
	acceptEnv.To = senderPeerID

	if err := conn.Send(acceptEnv); err != nil {
		logger.Error("failed to send manifest accept", "error", err)
		return
	}

	logger.Info("manifest accept sent", "manifest_id", manifestID, "to", senderPeerID)
}

func runICETest(ctx context.Context, logger *slog.Logger, conn *wsclient.Conn, peerID, initialSessionID string) error {
	logger.Info("starting ICE connectivity test")

	// Wait for peer list to find sender
	var senderPeerID string
	peerListDone := make(chan struct{})
	var peerListOnce sync.Once
	markPeerList := func() {
		peerListOnce.Do(func() {
			close(peerListDone)
		})
	}

	// Track sessionID (may be empty initially, will be set from first envelope)
	sessionID := initialSessionID

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

	// Queue for candidates gathered before we know the sender
	var queuedCandidates []string
	var candidateCallback func(string)
	var candidateMu sync.Mutex

	icePeer.OnLocalCandidate(func(c string) {
		candidateMu.Lock()
		defer candidateMu.Unlock()
		if candidateCallback != nil {
			candidateCallback(c)
		} else {
			// Queue until we know the sender
			queuedCandidates = append(queuedCandidates, c)
			logger.Debug("queued candidate (sender not found yet)", "candidate", c)
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

			// Get sessionID from first envelope if not set
			if sessionID == "" && env.SessionID != "" {
				sessionID = env.SessionID
			}

			switch env.Type {
			case protocol.TypePeerList:
				var peerList protocol.PeerList
				if err := env.DecodePayload(&peerList); err != nil {
					logger.Warn("failed to decode peer_list", "error", err)
					return
				}

				// Find sender
				for _, p := range peerList.Peers {
					if p.Role == "sender" {
						senderPeerID = p.PeerID
						logger.Info("sender found", "peer_id", senderPeerID)
						break
					}
				}

				if senderPeerID == "" {
					logger.Warn("no sender found, waiting...")
					return // Keep waiting for more peer lists
				}

				// Only proceed if we haven't already sent credentials
				if !localCredsSent {
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
						env.To = senderPeerID
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
					credsEnv.To = senderPeerID
					if err := conn.Send(credsEnv); err != nil {
						logger.Error("failed to send credentials", "error", err)
						return
					}
					localCredsSent = true
					logger.Info("sent local ICE credentials", "ufrag", ufrag)
					markCredsExchanged()

					markPeerList() // Signal that we found sender and sent credentials
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

				// If we haven't found sender yet, use the sender of this message
				if senderPeerID == "" && env.From != "" {
					senderPeerID = env.From
					logger.Info("sender found via credentials message", "peer_id", senderPeerID)

					// Set up candidate callback if not already set
					if !localCredsSent {
						sendCandidate := func(c string) {
							candidateMsg := protocol.IceCandidate{Candidate: c}
							env, err := protocol.NewEnvelope(protocol.TypeIceCandidate, protocol.NewMsgID(), candidateMsg)
							if err != nil {
								logger.Error("failed to create candidate envelope", "error", err)
								return
							}
							env.SessionID = sessionID
							env.From = peerID
							env.To = senderPeerID
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

						// Send local credentials in response
						ufrag, pwd := icePeer.LocalCredentials()
						credsMsg := protocol.IceCredentials{Ufrag: ufrag, Pwd: pwd}
						credsEnv, err := protocol.NewEnvelope(protocol.TypeIceCredentials, protocol.NewMsgID(), credsMsg)
						if err != nil {
							logger.Error("failed to create credentials envelope", "error", err)
							return
						}
						credsEnv.SessionID = sessionID
						credsEnv.From = peerID
						credsEnv.To = senderPeerID
						if err := conn.Send(credsEnv); err != nil {
							logger.Error("failed to send credentials", "error", err)
							return
						}
						localCredsSent = true
						logger.Info("sent local ICE credentials", "ufrag", ufrag)
						markCredsExchanged()

						markPeerList() // Signal that we found sender and sent credentials
					}
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

	// Wait for peer list
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
	iceConn, err := icePeer.Accept(connectCtx)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	defer iceConn.Close()

	logger.Info("ICE connection established, waiting for ping...")

	// Read ping
	buf := make([]byte, 1024)
	iceConn.SetReadDeadline(time.Now().Add(10 * time.Second))
	n, err := iceConn.Read(buf)
	if err != nil {
		return fmt.Errorf("failed to read ping: %w", err)
	}

	request := string(buf[:n])
	if request != "ping" {
		return fmt.Errorf("unexpected request: %s", request)
	}

	logger.Info("received ping, sending pong...")

	// Send pong
	pongMsg := []byte("pong")
	if _, err := iceConn.Write(pongMsg); err != nil {
		return fmt.Errorf("failed to send pong: %w", err)
	}

	logger.Info("ICE test successful: ping/pong completed")
	return nil
}

func runQUICTest(ctx context.Context, logger *slog.Logger, conn *wsclient.Conn, peerID, initialSessionID string) error {
	logger.Info("starting QUIC connectivity test")

	// First, establish ICE connection (similar to runICETest)
	// Wait for peer list to find sender
	var senderPeerID string
	peerListDone := make(chan struct{})
	var peerListOnce sync.Once
	markPeerList := func() {
		peerListOnce.Do(func() {
			close(peerListDone)
		})
	}

	// Track sessionID (may be empty initially, will be set from first envelope)
	sessionID := initialSessionID

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

	// Queue for candidates gathered before we know the sender
	var queuedCandidates []string
	var candidateCallback func(string)
	var candidateMu sync.Mutex

	icePeer.OnLocalCandidate(func(c string) {
		candidateMu.Lock()
		defer candidateMu.Unlock()
		if candidateCallback != nil {
			candidateCallback(c)
		} else {
			// Queue until we know the sender
			queuedCandidates = append(queuedCandidates, c)
			logger.Debug("queued candidate (sender not found yet)", "candidate", c)
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

			// Get sessionID from first envelope if not set
			if sessionID == "" && env.SessionID != "" {
				sessionID = env.SessionID
			}

			switch env.Type {
			case protocol.TypePeerList:
				var peerList protocol.PeerList
				if err := env.DecodePayload(&peerList); err != nil {
					logger.Warn("failed to decode peer_list", "error", err)
					return
				}

				// Find sender
				for _, p := range peerList.Peers {
					if p.Role == "sender" {
						senderPeerID = p.PeerID
						logger.Info("sender found", "peer_id", senderPeerID)
						break
					}
				}

				if senderPeerID == "" {
					logger.Warn("no sender found, waiting...")
					return // Keep waiting for more peer lists
				}

				// Only proceed if we haven't already sent credentials
				if !localCredsSent {
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
						env.To = senderPeerID
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
					credsEnv.To = senderPeerID
					if err := conn.Send(credsEnv); err != nil {
						logger.Error("failed to send credentials", "error", err)
						return
					}
					localCredsSent = true
					logger.Info("sent local ICE credentials", "ufrag", ufrag)
					markCredsExchanged()

					markPeerList() // Signal that we found sender and sent credentials
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

				// If we haven't found sender yet, use the sender of this message
				if senderPeerID == "" && env.From != "" {
					senderPeerID = env.From
					logger.Info("sender found via credentials message", "peer_id", senderPeerID)

					// Set up candidate callback if not already set
					if !localCredsSent {
						sendCandidate := func(c string) {
							candidateMsg := protocol.IceCandidate{Candidate: c}
							env, err := protocol.NewEnvelope(protocol.TypeIceCandidate, protocol.NewMsgID(), candidateMsg)
							if err != nil {
								logger.Error("failed to create candidate envelope", "error", err)
								return
							}
							env.SessionID = sessionID
							env.From = peerID
							env.To = senderPeerID
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

						// Send local credentials in response
						ufrag, pwd := icePeer.LocalCredentials()
						credsMsg := protocol.IceCredentials{Ufrag: ufrag, Pwd: pwd}
						credsEnv, err := protocol.NewEnvelope(protocol.TypeIceCredentials, protocol.NewMsgID(), credsMsg)
						if err != nil {
							logger.Error("failed to create credentials envelope", "error", err)
							return
						}
						credsEnv.SessionID = sessionID
						credsEnv.From = peerID
						credsEnv.To = senderPeerID
						if err := conn.Send(credsEnv); err != nil {
							logger.Error("failed to send credentials", "error", err)
							return
						}
						localCredsSent = true
						logger.Info("sent local ICE credentials", "ufrag", ufrag)
						markCredsExchanged()

						markPeerList() // Signal that we found sender and sent credentials
					}
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

	// Establish ICE connection (receiver uses Accept)
	logger.Info("establishing ICE connection...")
	connectCtx, connectCancel := context.WithTimeout(ctx, 30*time.Second)
	defer connectCancel()
	// We don't need the returned conn as icePeer stores it
	_, err = icePeer.Accept(connectCtx)
	if err != nil {
		return fmt.Errorf("failed to establish ICE connection: %w", err)
	}

	logger.Info("ICE connection established, setting up QUIC server...")

	// Get PacketConn info from ICE before closing
	localAddr, remoteAddr, err := icePeer.PacketConnInfo()
	if err != nil {
		return fmt.Errorf("failed to get PacketConn info: %w", err)
	}

	// Do NOT close ICE connection to free up the address for QUIC
	// iceConn.Close()

	// Create PacketConn for QUIC (will bind to same address as ICE)
	udpConn, err := icePeer.CreatePacketConn()
	if err != nil {
		return fmt.Errorf("failed to create PacketConn: %w", err)
	}
	defer udpConn.Close()

	logger.Info("QUIC server listening", "local_addr", localAddr, "remote_addr", remoteAddr)

	// Listen for QUIC connections
	quicListener, err := quictransport.Listen(ctx, udpConn, logger)
	if err != nil {
		return fmt.Errorf("failed to create QUIC listener: %w", err)
	}
	defer quicListener.Close()

	// Accept QUIC connection
	logger.Info("waiting for QUIC connection...")
	quicConn, err := quicListener.Accept(ctx)
	if err != nil {
		return fmt.Errorf("failed to accept QUIC connection: %w", err)
	}
	defer quicConn.CloseWithError(0, "")

	logger.Info("QUIC connection accepted, waiting for stream...")

	// Accept a bidirectional stream
	stream, err := quicConn.AcceptStream(ctx)
	if err != nil {
		return fmt.Errorf("failed to accept stream: %w", err)
	}
	defer stream.Close()

	logger.Info("stream accepted, waiting for ping...")

	// Read "ping"
	buf := make([]byte, 4)
	n, err := stream.Read(buf)
	if err != nil {
		return fmt.Errorf("failed to read ping: %w", err)
	}

	if string(buf[:n]) != "ping" {
		return fmt.Errorf("unexpected message: %q", string(buf[:n]))
	}

	logger.Info("received ping, sending pong...")

	// Send "pong"
	if _, err := stream.Write([]byte("pong")); err != nil {
		return fmt.Errorf("failed to send pong: %w", err)
	}

	logger.Info("QUIC test successful: ping/pong completed")

	// Wait a bit to ensure the pong is sent and received before closing
	// This gives the sender time to read the response
	time.Sleep(500 * time.Millisecond)

	return nil
}

func runQUICTransferTest(ctx context.Context, logger *slog.Logger, conn *wsclient.Conn, peerID, initialSessionID string, multiStream bool, parallelFiles int, resume bool, resumeTimeout time.Duration, resumeVerify string, destination string, verbose bool) error {
	logger.Info("starting QUIC transfer test")
	iceLog := func(format string, args ...any) {
		if !verbose {
			fmt.Printf(format, args...)
		}
	}

	// Reuse ICE+QUIC setup from runQUICTest (up to QUIC connection)
	// First, establish ICE connection
	var senderPeerID string
	peerListDone := make(chan struct{})
	var peerListOnce sync.Once
	markPeerList := func() {
		peerListOnce.Do(func() {
			close(peerListDone)
		})
	}

	sessionID := initialSessionID

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
	var err error
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
			logger.Debug("queued candidate (sender not found yet)", "candidate", c)
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

			if sessionID == "" && env.SessionID != "" {
				sessionID = env.SessionID
			}

			switch env.Type {
			case protocol.TypePeerList:
				var peerList protocol.PeerList
				if err := env.DecodePayload(&peerList); err != nil {
					logger.Warn("failed to decode peer_list", "error", err)
					return
				}

				for _, p := range peerList.Peers {
					if p.Role == "sender" {
						senderPeerID = p.PeerID
						logger.Info("sender found", "peer_id", senderPeerID)
						break
					}
				}

				if senderPeerID == "" {
					logger.Warn("no sender found, waiting...")
					return
				}

				if !localCredsSent {
					sendCandidate := func(c string) {
						candidateMsg := protocol.IceCandidate{Candidate: c}
						env, err := protocol.NewEnvelope(protocol.TypeIceCandidate, protocol.NewMsgID(), candidateMsg)
						if err != nil {
							logger.Error("failed to create candidate envelope", "error", err)
							return
						}
						env.SessionID = sessionID
						env.From = peerID
						env.To = senderPeerID
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
					credsEnv.To = senderPeerID
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

				// If we haven't found sender yet, extract from credentials message
				if senderPeerID == "" && env.From != "" {
					senderPeerID = env.From
					logger.Info("sender found via credentials message", "peer_id", senderPeerID)
					iceLog("ice stage=peer_found peer_id=%s\n", senderPeerID)

					// Set up candidate callback if not already set
					if !localCredsSent {
						sendCandidate := func(c string) {
							candidateMsg := protocol.IceCandidate{Candidate: c}
							env, err := protocol.NewEnvelope(protocol.TypeIceCandidate, protocol.NewMsgID(), candidateMsg)
							if err != nil {
								logger.Error("failed to create candidate envelope", "error", err)
								return
							}
							env.SessionID = sessionID
							env.From = peerID
							env.To = senderPeerID
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

						// Send our credentials back
						ufrag, pwd := icePeer.LocalCredentials()
						credsMsg := protocol.IceCredentials{Ufrag: ufrag, Pwd: pwd}
						credsEnv, err := protocol.NewEnvelope(protocol.TypeIceCredentials, protocol.NewMsgID(), credsMsg)
						if err != nil {
							logger.Error("failed to create credentials envelope", "error", err)
							return
						}
						credsEnv.SessionID = sessionID
						credsEnv.From = peerID
						credsEnv.To = senderPeerID
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
	iceLog("ice stage=connect_start role=receiver\n")
	connectCtx, connectCancel := context.WithTimeout(ctx, 30*time.Second)
	defer connectCancel()
	iceConn, err := icePeer.Accept(connectCtx)
	if err != nil {
		return fmt.Errorf("failed to establish ICE connection: %w", err)
	}
	iceLog("ice stage=connect_ok role=receiver\n")

	logger.Info("ICE connection established, setting up QUIC server...")

	localAddr, remoteAddr, err := icePeer.PacketConnInfo()
	if err != nil {
		return fmt.Errorf("failed to get PacketConn info: %w", err)
	}

	iceConn.Close()

	udpConn, err := icePeer.CreatePacketConn()
	if err != nil {
		return fmt.Errorf("failed to create PacketConn: %w", err)
	}
	defer udpConn.Close()

	logger.Info("QUIC server listening", "local_addr", localAddr, "remote_addr", remoteAddr)

	quicListener, err := quictransport.Listen(ctx, udpConn, logger)
	if err != nil {
		return fmt.Errorf("failed to create QUIC listener: %w", err)
	}
	defer quicListener.Close()

	logger.Info("waiting for QUIC connection...")

	// Create QUIC transport as listener
	quicTransport := transferquic.NewListener(quicListener, logger)
	defer quicTransport.Close()

	// Accept a connection through the transport
	transferConn, err := quicTransport.Accept(ctx)
	if err != nil {
		return fmt.Errorf("failed to accept transfer connection: %w", err)
	}
	defer transferConn.Close()

	useMultiStream := multiStream
	if useMultiStream {
		logger.Info("transfer connection accepted, starting multi-stream receive...")
	} else {
		logger.Info("transfer connection accepted, waiting for stream...")
	}

	// Create temp directory for received files unless destination is specified
	tempDir := destination
	if tempDir == "" {
		var err error
		tempDir, err = os.MkdirTemp("", "sheerbytes_receive_*")
		if err != nil {
			return fmt.Errorf("failed to create temp directory: %w", err)
		}
		defer os.RemoveAll(tempDir)
	} else {
		if err := os.MkdirAll(tempDir, 0755); err != nil {
			return fmt.Errorf("failed to create destination directory: %w", err)
		}
	}

	// Set up progress logging with throughput stats
	var totalBytesReceived int64
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
				received := totalBytesReceived
				file := currentFile
				progressMu.Unlock()

				elapsed := time.Since(startTime).Seconds()
				if elapsed > 0 {
					mbps := float64(received) / (1024 * 1024) / elapsed
					if verbose {
						logger.Info("transfer progress",
							"received_bytes", received,
							"mbps", fmt.Sprintf("%.2f", mbps),
							"current_file", file)
					} else {
						fmt.Printf("progress received=%d bytes mbps=%s file=%s\n", received, fmt.Sprintf("%.2f", mbps), file)
					}
				}
			}
		}
	}()

	// Track total bytes received
	progressFn := func(relpath string, bytesReceived int64, total int64) {
		progressMu.Lock()
		totalBytesReceived = bytesReceived // Cumulative per file
		currentFile = relpath
		progressMu.Unlock()
	}

	// Receive manifest
	var manifest manifest.Manifest
	if useMultiStream {
		var transferStatsFn transfer.TransferStatsFn
		if verbose {
			transferStatsFn = func(activeFiles, completedFiles int, remainingBytes int64) {
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
		manifest, err = transfer.RecvManifestMultiStream(ctx, transferConn, tempDir, transfer.Options{
			ParallelFiles:   parallelFiles,
			Resume:          resume,
			ResumeTimeout:   resumeTimeout,
			ResumeVerify:    resumeVerify,
			ProgressFn:      progressFn,
			TransferStatsFn: transferStatsFn,
			ResumeStatsFn:   resumeStatsFn,
		})
		if err != nil {
			return fmt.Errorf("failed to receive manifest: %w", err)
		}
	} else {
		stream, err := transferConn.AcceptStream(ctx)
		if err != nil {
			return fmt.Errorf("failed to accept stream: %w", err)
		}
		defer stream.Close()

		logger.Info("stream accepted, receiving manifest...")

		manifest, err = transfer.RecvManifest(ctx, stream, tempDir, progressFn)
		if err != nil {
			return fmt.Errorf("failed to receive manifest: %w", err)
		}
	}

	logger.Info("transfer complete",
		"output_dir", tempDir,
		"root", manifest.Root,
		"file_count", len(manifest.Items),
		"total_bytes", manifest.TotalBytes)

	fmt.Printf("\n=== Transfer Complete ===\n")
	fmt.Printf("Files saved to: %s\n", tempDir)
	fmt.Printf("Manifest root: %s\n", manifest.Root)
	fmt.Printf("Files received: %d\n", len(manifest.Items))
	fmt.Printf("Total bytes: %d\n", manifest.TotalBytes)
	fmt.Printf("========================\n\n")

	return nil
}
