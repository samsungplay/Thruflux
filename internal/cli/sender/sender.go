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
	"time"

	"github.com/sheerbytes/sheerbytes/internal/app"
	"github.com/sheerbytes/sheerbytes/internal/appstate"
	"github.com/sheerbytes/sheerbytes/internal/ice"
	"github.com/sheerbytes/sheerbytes/internal/logging"
	"github.com/sheerbytes/sheerbytes/internal/transfer"
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
	turnOnly := false
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
		TurnOnly:               turnOnly,
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
	fmt.Fprintf(os.Stderr, "  --stun-server URLS          STUN server URLs (comma-separated, default %s)\n", strings.Join(senderDefaultStunServers, ","))
	fmt.Fprintln(os.Stderr, "                              example: --stun-server stun:stun.l.google.com:19302")
	fmt.Fprintln(os.Stderr, "  --turn-server URLS          TURN server URLs (comma-separated, default none; server may provide)")
	fmt.Fprintln(os.Stderr, "                              example: --turn-server turn:username:password@turn.example.com:3478")
	fmt.Fprintln(os.Stderr, "                                       --turn-server turns:username:password@turn.example.com:5349")
	fmt.Fprintln(os.Stderr, "                                       --turn-server turns:username:password@turn.example.com:5349?servername=turn.example.com")
	fmt.Fprintln(os.Stderr, "                                       --turn-server turns:username:password@turn.example.com:5349?insecure=1  (debug only)")
	fmt.Fprintln(os.Stderr, "  --test-turn                 only use TURN relay candidates (no direct probing)")
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
