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

	"github.com/sheerbytes/sheerbytes/internal/app"
	"github.com/sheerbytes/sheerbytes/internal/logging"
	"github.com/sheerbytes/sheerbytes/internal/wsclient"
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
	dumb := false
	dumbTCP := false
	udpReadBufferBytes := 8 * 1024 * 1024
	udpWriteBufferBytes := 8 * 1024 * 1024
	quicConnWindowBytes := 512 * 1024 * 1024
	quicStreamWindowBytes := 64 * 1024 * 1024
	quicMaxIncomingStreams := 100
	stunServers := make([]string, 0)
	turnServers := make([]string, 0)
	turnOnly := false
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
		if arg == "--dumb" {
			dumb = true
			continue
		}
		if arg == "--dumb-tcp" {
			dumb = true
			dumbTCP = true
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
		if arg == "--test-turn" {
			turnOnly = true
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
		Dumb:                   dumb,
		DumbTCP:                dumbTCP,
		UDPReadBufferBytes:     udpReadBufferBytes,
		UDPWriteBufferBytes:    udpWriteBufferBytes,
		QuicConnWindowBytes:    quicConnWindowBytes,
		QuicStreamWindowBytes:  quicStreamWindowBytes,
		QuicMaxIncomingStreams: quicMaxIncomingStreams,
		StunServers:            stunServers,
		TurnServers:            turnServers,
		TurnOnly:               turnOnly,
	}); err != nil {
		logger.Error("snapshot receiver failed", "error", err)
		os.Exit(1)
	}
}

func printReceiverUsage() {
	fmt.Fprintln(os.Stderr, "usage: thru join <join-code> [--out DIR] [--server-url URL] [--benchmark]")
	fmt.Fprintln(os.Stderr, "  --out DIR                   output directory (default .)")
	fmt.Fprintln(os.Stderr, "  --server-url URL            signaling server URL (default https://bytepipe.app)")
	fmt.Fprintf(os.Stderr, "  --stun-server URLS          STUN server URLs (comma-separated, default %s)\n", strings.Join(receiverDefaultStunServers, ","))
	fmt.Fprintln(os.Stderr, "                              example: --stun-server stun:stun.l.google.com:19302")
	fmt.Fprintln(os.Stderr, "  --turn-server URLS          TURN server URLs (comma-separated, default none; server may provide)")
	fmt.Fprintln(os.Stderr, "                              example: --turn-server turn:username:password@turn.example.com:3478")
	fmt.Fprintln(os.Stderr, "                                       --turn-server turns:username:password@turn.example.com:5349")
	fmt.Fprintln(os.Stderr, "                                       --turn-server turns:username:password@turn.example.com:5349?servername=turn.example.com")
	fmt.Fprintln(os.Stderr, "                                       --turn-server turns:username:password@turn.example.com:5349?insecure=1  (debug only)")
	fmt.Fprintln(os.Stderr, "  --test-turn                 only use TURN relay candidates (no direct probing)")
	fmt.Fprintln(os.Stderr, "  --benchmark                 enable benchmark stats")
	fmt.Fprintln(os.Stderr, "  --dumb                      raw memory stream (discarded on receive)")
	fmt.Fprintln(os.Stderr, "  --dumb-tcp                  raw memory stream over TCP (discarded on receive)")
	fmt.Fprintln(os.Stderr, "  --udp-read-buffer-bytes N   UDP read buffer size (default 8388608)")
	fmt.Fprintln(os.Stderr, "  --udp-write-buffer-bytes N  UDP write buffer size (default 8388608)")
	fmt.Fprintln(os.Stderr, "  --quic-conn-window-bytes N  QUIC connection window (default 536870912)")
	fmt.Fprintln(os.Stderr, "  --quic-stream-window-bytes N QUIC stream window (default 67108864)")
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
