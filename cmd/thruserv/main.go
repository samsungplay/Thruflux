package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/sheerbytes/sheerbytes/internal/config"
	"github.com/sheerbytes/sheerbytes/internal/logging"
	"github.com/sheerbytes/sheerbytes/internal/peers"
	"github.com/sheerbytes/sheerbytes/internal/session"
	"github.com/sheerbytes/sheerbytes/pkg/protocol"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow all origins for now
	},
}

const serverVersion = "v0.1.0"

func main() {
	if hasHelpFlag(os.Args[1:]) {
		printServerUsage()
		return
	}
	if hasVersionFlag(os.Args[1:]) {
		fmt.Println(serverVersion)
		return
	}
	cfg := config.ParseServerConfig()
	logger := logging.New("thruserv", "error")

	addr := fmt.Sprintf(":%d", cfg.Port)
	fmt.Printf("starting server addr=%s\n", addr)

	// Create session store with configured TTL
	store := session.NewStore(cfg.SessionTimeout)

	// Create peer hub
	hub := peers.NewHub()
	limits := newServerLimits(cfg)
	if limits.sessionCreateRatePerSec > 0 {
		sessionIPLimiter.SetLimits(limits.sessionCreateRatePerSec, limits.sessionCreateBurst)
	}

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]bool{"ok": true})
	})

	http.HandleFunc("/session", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		maxReceiversRaw := r.URL.Query().Get("max_receivers")
		if maxReceiversRaw != "" {
			reqMax, err := strconv.Atoi(maxReceiversRaw)
			if err != nil || reqMax < 1 {
				sendError(w, http.StatusBadRequest, "invalid max_receivers")
				return
			}
			if limits.maxReceiversPerSender > 0 && reqMax > limits.maxReceiversPerSender {
				sendError(w, http.StatusTooManyRequests, "max receivers exceeds server limit")
				return
			}
		}
		if limits.sessionCreateRatePerSec > 0 {
			ip := clientIP(r)
			if ip != "" && !sessionIPLimiter.Allow(ip) {
				sendError(w, http.StatusTooManyRequests, "rate limit exceeded")
				return
			}
		}
		if limits.maxSessions > 0 && store.Count() >= limits.maxSessions {
			sendError(w, http.StatusTooManyRequests, "session limit reached")
			return
		}

		// Create new session
		sess := store.Create()

		if !sess.ExpiresAt.IsZero() {
			ttl := time.Until(sess.ExpiresAt)
			if ttl > 0 {
				time.AfterFunc(ttl, func() {
					hub.CloseSession(sess.ID)
					store.Delete(sess.ID)
					fmt.Printf("session expired session_id=%s join_code=%s\n", sess.ID, sess.JoinCode)
				})
			}
		}

		// Prepare response
		response := map[string]interface{}{
			"session_id": sess.ID,
			"join_code":  sess.JoinCode,
		}
		if !sess.ExpiresAt.IsZero() {
			response["expires_at"] = sess.ExpiresAt.Format(time.RFC3339)
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		if err := json.NewEncoder(w).Encode(response); err != nil {
			logger.Error("failed to encode response", "error", err)
		}

		fmt.Printf("session created session_id=%s join_code=%s\n", sess.ID, sess.JoinCode)
	})

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		handleWebSocket(w, r, store, hub, logger, limits)
	})

	if err := http.ListenAndServe(addr, nil); err != nil {
		logger.Error("server failed", "error", err)
		os.Exit(1)
	}
}

type tokenBucket struct {
	mu     sync.Mutex
	tokens float64
	last   time.Time
	rate   float64
	burst  float64
}

func newTokenBucket(ratePerSec float64, burst int) *tokenBucket {
	if ratePerSec < 0 {
		ratePerSec = 0
	}
	if burst < 1 {
		burst = 1
	}
	return &tokenBucket{
		tokens: float64(burst),
		last:   time.Now(),
		rate:   ratePerSec,
		burst:  float64(burst),
	}
}

func (b *tokenBucket) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	elapsed := now.Sub(b.last).Seconds()
	b.last = now
	b.tokens += elapsed * b.rate
	if b.tokens > b.burst {
		b.tokens = b.burst
	}
	if b.tokens < 1 {
		return false
	}
	b.tokens -= 1
	return true
}

type serverLimits struct {
	maxSessions             int
	maxReceiversPerSender   int
	maxMessageBytes         int
	connectRatePerSec       float64
	connectBurst            int
	msgRatePerSec           float64
	msgBurst                int
	sessionCreateRatePerSec float64
	sessionCreateBurst      int
	maxWSConnections        int
	wsIdleTimeout           time.Duration
}

func newServerLimits(cfg config.ServerConfig) serverLimits {
	connectRate := float64(cfg.WSConnectsPerMin) / 60.0
	if cfg.WSConnectsPerMin <= 0 {
		connectRate = 0
	}
	msgRate := float64(cfg.WSMsgsPerSec)
	if cfg.WSMsgsPerSec <= 0 {
		msgRate = 0
	}
	sessionRate := float64(cfg.SessionCreatesPerMin) / 60.0
	if cfg.SessionCreatesPerMin <= 0 {
		sessionRate = 0
	}
	return serverLimits{
		maxSessions:             cfg.MaxSessions,
		maxReceiversPerSender:   cfg.MaxReceiversPerSender,
		maxMessageBytes:         cfg.MaxMessageBytes,
		connectRatePerSec:       connectRate,
		connectBurst:            cfg.WSConnectsBurst,
		msgRatePerSec:           msgRate,
		msgBurst:                cfg.WSMsgsBurst,
		sessionCreateRatePerSec: sessionRate,
		sessionCreateBurst:      cfg.SessionCreatesBurst,
		maxWSConnections:        cfg.MaxWSConnections,
		wsIdleTimeout:           cfg.WSIdleTimeout,
	}
}

type ipLimiter struct {
	mu      sync.Mutex
	buckets map[string]*tokenBucket
	rate    float64
	burst   int
}

func newIPLimiter(ratePerSec float64, burst int) *ipLimiter {
	return &ipLimiter{
		buckets: make(map[string]*tokenBucket),
		rate:    ratePerSec,
		burst:   burst,
	}
}

func (l *ipLimiter) SetLimits(ratePerSec float64, burst int) {
	l.mu.Lock()
	l.rate = ratePerSec
	l.burst = burst
	l.mu.Unlock()
}

func (l *ipLimiter) Allow(ip string) bool {
	l.mu.Lock()
	rate := l.rate
	burst := l.burst
	l.mu.Unlock()
	if rate <= 0 {
		return true
	}
	l.mu.Lock()
	bucket, ok := l.buckets[ip]
	if !ok {
		bucket = newTokenBucket(rate, burst)
		l.buckets[ip] = bucket
	}
	l.mu.Unlock()
	return bucket.Allow()
}

var wsIPLimiter = newIPLimiter(0, 1)
var sessionIPLimiter = newIPLimiter(0, 1)
var wsConnLimiter = newConnLimiter(0)

func handleWebSocket(w http.ResponseWriter, r *http.Request, store *session.Store, hub *peers.Hub, logger *slog.Logger, limits serverLimits) {
	// Parse query parameters
	joinCode := r.URL.Query().Get("join_code")
	peerID := r.URL.Query().Get("peer_id")
	role := r.URL.Query().Get("role")
	maxReceiversRaw := r.URL.Query().Get("max_receivers")

	// Validate join_code
	if joinCode == "" {
		sendError(w, http.StatusBadRequest, "missing join_code")
		return
	}

	// Validate and get session
	sess, found := store.GetByJoinCode(joinCode)
	if !found {
		sendError(w, http.StatusNotFound, "invalid or expired join_code")
		return
	}

	// Validate peer_id
	if peerID == "" {
		sendError(w, http.StatusBadRequest, "missing peer_id")
		return
	}

	// Validate role
	if role != "sender" && role != "receiver" {
		sendError(w, http.StatusBadRequest, "role must be 'sender' or 'receiver'")
		return
	}

	if role == "sender" && maxReceiversRaw != "" {
		reqMax, err := strconv.Atoi(maxReceiversRaw)
		if err != nil || reqMax < 1 {
			sendError(w, http.StatusBadRequest, "invalid max_receivers")
			return
		}
		if limits.maxReceiversPerSender > 0 && reqMax > limits.maxReceiversPerSender {
			sendError(w, http.StatusTooManyRequests, "max receivers exceeds server limit")
			return
		}
	}

	if limits.connectRatePerSec > 0 {
		wsIPLimiter.SetLimits(limits.connectRatePerSec, limits.connectBurst)
		ip := clientIP(r)
		if ip != "" && !wsIPLimiter.Allow(ip) {
			sendError(w, http.StatusTooManyRequests, "rate limit exceeded")
			return
		}
	}

	if limits.maxWSConnections > 0 {
		wsConnLimiter.SetLimit(limits.maxWSConnections)
		if !wsConnLimiter.Acquire() {
			sendError(w, http.StatusTooManyRequests, "connection limit reached")
			return
		}
		defer wsConnLimiter.Release()
	}

	if limits.maxReceiversPerSender > 0 && role == "receiver" {
		receivers := 0
		for _, p := range hub.List(sess.ID) {
			if p.Role == "receiver" {
				receivers++
			}
		}
		if receivers >= limits.maxReceiversPerSender {
			sendError(w, http.StatusTooManyRequests, "receiver limit reached")
			return
		}
	}

	// Upgrade to WebSocket
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		logger.Error("websocket upgrade failed", "error", err)
		return
	}
	defer conn.Close()
	if limits.maxMessageBytes > 0 {
		conn.SetReadLimit(int64(limits.maxMessageBytes))
	}
	var writeMu sync.Mutex
	if limits.wsIdleTimeout > 0 {
		conn.SetReadDeadline(time.Now().Add(limits.wsIdleTimeout))
		conn.SetPongHandler(func(string) error {
			conn.SetReadDeadline(time.Now().Add(limits.wsIdleTimeout))
			return nil
		})
		conn.SetPingHandler(func(appData string) error {
			conn.SetReadDeadline(time.Now().Add(limits.wsIdleTimeout))
			writeMu.Lock()
			err := conn.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(10*time.Second))
			writeMu.Unlock()
			return err
		})
	}

	// Generate unique connection ID
	connID := protocol.NewMsgID()

	// Create peer
	peer := peers.Peer{
		PeerID: peerID,
		Role:   role,
		ConnID: connID,
	}

	// Send function for this connection
	sendFunc := func(env protocol.Envelope) error {
		writeMu.Lock()
		err := conn.WriteJSON(env)
		writeMu.Unlock()
		return err
	}

	// Add peer to hub
	removePeer := hub.Add(sess.ID, peer, sendFunc, func() { _ = conn.Close() })
	defer removePeer()

	if limits.wsIdleTimeout > 0 {
		stopPing := make(chan struct{})
		defer close(stopPing)
		go func() {
			ticker := time.NewTicker(30 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-stopPing:
					return
				case <-ticker.C:
					writeMu.Lock()
					_ = conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(10*time.Second))
					writeMu.Unlock()
				}
			}
		}()
	}

	fmt.Printf("peer connected session_id=%s peer_id=%s role=%s conn_id=%s\n", sess.ID, peerID, role, connID)

	// Send current peer list to the newly joined peer
	currentPeers := hub.List(sess.ID)
	peerList := protocol.PeerList{Peers: currentPeers}
	peerListEnv, err := protocol.NewEnvelope(protocol.TypePeerList, protocol.NewMsgID(), peerList)
	if err != nil {
		logger.Error("failed to create peer list envelope", "error", err)
		return
	}
	peerListEnv.SessionID = sess.ID
	peerListEnv.From = "server"

	if err := sendFunc(peerListEnv); err != nil {
		logger.Error("failed to send peer list", "error", err)
		return
	}

	// Broadcast PeerJoined to all peers in session
	peerJoined := protocol.PeerJoined{
		Peer: protocol.PeerInfo{
			PeerID: peerID,
			Role:   role,
		},
	}
	peerJoinedEnv, err := protocol.NewEnvelope(protocol.TypePeerJoined, protocol.NewMsgID(), peerJoined)
	if err != nil {
		logger.Error("failed to create peer joined envelope", "error", err)
		return
	}
	peerJoinedEnv.SessionID = sess.ID
	peerJoinedEnv.From = "server"

	hub.Broadcast(sess.ID, peerJoinedEnv)

	// On disconnect, broadcast PeerLeft
	defer func() {
		peerLeft := protocol.PeerLeft{PeerID: peerID}
		peerLeftEnv, err := protocol.NewEnvelope(protocol.TypePeerLeft, protocol.NewMsgID(), peerLeft)
		if err != nil {
			logger.Error("failed to create peer left envelope", "error", err)
			return
		}
		peerLeftEnv.SessionID = sess.ID
		peerLeftEnv.From = "server"

		hub.Broadcast(sess.ID, peerLeftEnv)
		fmt.Printf("peer disconnected session_id=%s peer_id=%s\n", sess.ID, peerID)
		if role == "sender" {
			fmt.Printf("session deleted session_id=%s join_code=%s\n", sess.ID, sess.JoinCode)
			store.Delete(sess.ID)
		}
	}()

	// Read loop: process incoming messages
	maxMessageSize := limits.maxMessageBytes
	if maxMessageSize <= 0 {
		maxMessageSize = 64 * 1024
	}
	msgLimiter := newTokenBucket(limits.msgRatePerSec, limits.msgBurst)
	for {
		messageType, message, err := conn.ReadMessage()
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				logger.Info("websocket idle timeout", "peer_id", peerID)
			}
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				logger.Error("websocket read error", "error", err)
			}
			break
		}
		if limits.wsIdleTimeout > 0 {
			conn.SetReadDeadline(time.Now().Add(limits.wsIdleTimeout))
		}

		// Only process text messages
		if messageType != websocket.TextMessage {
			continue
		}

		if limits.msgRatePerSec > 0 && !msgLimiter.Allow() {
			logger.Warn("websocket message rate limit exceeded", "peer_id", peerID)
			conn.Close()
			break
		}

		// Enforce max message size
		if len(message) > maxMessageSize {
			logger.Warn("message too large", "size", len(message), "max", maxMessageSize, "peer_id", peerID)
			conn.Close()
			break
		}

		// Parse envelope
		var env protocol.Envelope
		if err := json.Unmarshal(message, &env); err != nil {
			logger.Warn("invalid JSON envelope", "error", err, "peer_id", peerID)
			continue
		}

		// Validate basic envelope structure
		if err := env.ValidateBasic(); err != nil {
			logger.Warn("invalid envelope", "error", err, "peer_id", peerID)
			continue
		}

		// Enforce that From matches the query peer_id (override to ensure correctness)
		env.From = peerID

		// Set SessionID if missing
		if env.SessionID == "" {
			env.SessionID = sess.ID
		}

		// Route the message
		if env.To != "" {
			// Targeted send
			sent := hub.SendTo(sess.ID, env.To, env)
			if !sent {
				// Target not found, send error back to sender
				errorMsg := protocol.Error{
					Code:    "peer_not_found",
					Message: "target peer not found: " + env.To,
				}
				errorEnv, err := protocol.NewEnvelope(protocol.TypeError, protocol.NewMsgID(), errorMsg)
				if err == nil {
					errorEnv.SessionID = sess.ID
					errorEnv.From = "server"
					errorEnv.To = peerID
					sendFunc(errorEnv)
				}
				logger.Warn("peer not found for targeted send", "from", peerID, "to", env.To)
			}
		} else {
			// Broadcast to all except sender
			hub.BroadcastExcept(sess.ID, peerID, env)
		}
	}
}

func sendError(w http.ResponseWriter, code int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": message})
}

func printServerUsage() {
	fmt.Fprintln(os.Stderr, "usage: thruserv [--port N] [--max-sessions N] [--max-receivers-per-sender N]")
	fmt.Fprintln(os.Stderr, "  --port N                     server port (default 8080)")
	fmt.Fprintln(os.Stderr, "  --max-sessions N             max concurrent sessions (default 1000)")
	fmt.Fprintln(os.Stderr, "  --max-receivers-per-sender N max receivers per sender (default 10)")
	fmt.Fprintln(os.Stderr, "  --max-message-bytes N        max websocket message size (default 65536)")
	fmt.Fprintln(os.Stderr, "  --ws-connects-per-min N      max websocket connects per minute per IP (default 30)")
	fmt.Fprintln(os.Stderr, "  --ws-connects-burst N        burst websocket connects per IP (default 10)")
	fmt.Fprintln(os.Stderr, "  --ws-msgs-per-sec N          max websocket messages per second per connection (default 50)")
	fmt.Fprintln(os.Stderr, "  --ws-msgs-burst N            burst websocket messages per connection (default 100)")
	fmt.Fprintln(os.Stderr, "  --session-creates-per-min N  max session creates per minute per IP (default 10)")
	fmt.Fprintln(os.Stderr, "  --session-creates-burst N    burst session creates per IP (default 5)")
	fmt.Fprintln(os.Stderr, "  --max-ws-connections N       max concurrent websocket connections (default 2000)")
	fmt.Fprintln(os.Stderr, "  --ws-idle-timeout DURATION   websocket idle timeout (default 10m)")
	fmt.Fprintln(os.Stderr, "  --session-timeout DURATION   max session lifetime (default 24h, 0 disables)")
}

func hasHelpFlag(args []string) bool {
	for _, arg := range args {
		if arg == "--help" || arg == "-h" {
			return true
		}
	}
	return false
}

func hasVersionFlag(args []string) bool {
	for _, arg := range args {
		if arg == "--version" || arg == "-v" {
			return true
		}
	}
	return false
}

func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

type connLimiter struct {
	mu    sync.Mutex
	limit int
	inUse int
}

func newConnLimiter(limit int) *connLimiter {
	return &connLimiter{limit: limit}
}

func (l *connLimiter) SetLimit(limit int) {
	l.mu.Lock()
	l.limit = limit
	l.mu.Unlock()
}

func (l *connLimiter) Acquire() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.limit > 0 && l.inUse >= l.limit {
		return false
	}
	l.inUse++
	return true
}

func (l *connLimiter) Release() {
	l.mu.Lock()
	if l.inUse > 0 {
		l.inUse--
	}
	l.mu.Unlock()
}
