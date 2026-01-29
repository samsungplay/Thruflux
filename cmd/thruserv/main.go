package main

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/sheerbytes/sheerbytes/internal/config"
	"github.com/sheerbytes/sheerbytes/internal/logging"
	"github.com/sheerbytes/sheerbytes/internal/peers"
	"github.com/sheerbytes/sheerbytes/internal/session"
	"github.com/sheerbytes/sheerbytes/internal/termio"
	"github.com/sheerbytes/sheerbytes/pkg/protocol"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow all origins for now
	},
}

const serverVersion = "v0.1.1"

func main() {
	if hasHelpFlag(os.Args[1:]) {
		printServerUsage()
		return
	}
	if hasVersionFlag(os.Args[1:]) {
		fmt.Fprintln(termio.Stdout(), serverVersion)
		return
	}
	cfg := config.ParseServerConfig()
	logger := logging.New("thruserv", "error")

	addr := fmt.Sprintf(":%d", cfg.Port)
	fmt.Fprintf(termio.Stdout(), "starting server addr=%s\n", addr)

	// Create session store with configured TTL
	store := session.NewStore(cfg.SessionTimeout)
	expiry := newSessionExpiryManager()

	// Create peer hub
	hub := peers.NewHub()
	limits := newServerLimits(cfg)
	if limits.sessionCreateRatePerSec > 0 {
		sessionIPLimiter.SetLimits(limits.sessionCreateRatePerSec, limits.sessionCreateBurst)
	}
	turnIssuer := newTurnIssuer(cfg, logger)

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
				expiry.schedule(sess.ID, ttl, func() {
					hub.CloseSession(sess.ID)
					store.Delete(sess.ID)
					fmt.Fprintf(termio.Stdout(), "session expired session_id=%s join_code=%s\n", sess.ID, sess.JoinCode)
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

		fmt.Fprintf(termio.Stdout(), "session created session_id=%s join_code=%s\n", sess.ID, sess.JoinCode)
	})

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		handleWebSocket(w, r, store, hub, expiry, logger, limits, turnIssuer)
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

type sessionExpiryManager struct {
	mu     sync.Mutex
	timers map[string]*time.Timer
}

func newSessionExpiryManager() *sessionExpiryManager {
	return &sessionExpiryManager{
		timers: make(map[string]*time.Timer),
	}
}

func (m *sessionExpiryManager) schedule(sessionID string, ttl time.Duration, fn func()) {
	if ttl <= 0 {
		return
	}
	m.mu.Lock()
	if existing := m.timers[sessionID]; existing != nil {
		existing.Stop()
	}
	timer := time.AfterFunc(ttl, func() {
		fn()
		m.mu.Lock()
		delete(m.timers, sessionID)
		m.mu.Unlock()
	})
	m.timers[sessionID] = timer
	m.mu.Unlock()
}

func (m *sessionExpiryManager) cancel(sessionID string) {
	m.mu.Lock()
	if timer := m.timers[sessionID]; timer != nil {
		timer.Stop()
		delete(m.timers, sessionID)
	}
	m.mu.Unlock()
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

func handleWebSocket(w http.ResponseWriter, r *http.Request, store *session.Store, hub *peers.Hub, expiry *sessionExpiryManager, logger *slog.Logger, limits serverLimits, turnIssuer *turnIssuer) {
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

	fmt.Fprintf(termio.Stdout(), "peer connected session_id=%s peer_id=%s role=%s conn_id=%s\n", sess.ID, peerID, role, connID)

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

	if turnIssuer != nil {
		turnCreds, err := turnIssuer.Issue(peerID)
		if err != nil {
			logger.Error("failed to issue turn credentials", "error", err, "peer_id", peerID)
		} else if len(turnCreds.Servers) > 0 {
			turnEnv, err := protocol.NewEnvelope(protocol.TypeTurnCredentials, protocol.NewMsgID(), turnCreds)
			if err != nil {
				logger.Error("failed to create turn credentials envelope", "error", err)
			} else {
				turnEnv.SessionID = sess.ID
				turnEnv.From = "server"
				turnEnv.To = peerID
				if err := sendFunc(turnEnv); err != nil {
					logger.Error("failed to send turn credentials", "error", err)
				}
			}
		}
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
		fmt.Fprintf(termio.Stdout(), "peer disconnected session_id=%s peer_id=%s\n", sess.ID, peerID)
		if role == "sender" {
			fmt.Fprintf(termio.Stdout(), "session deleted session_id=%s join_code=%s\n", sess.ID, sess.JoinCode)
			expiry.cancel(sess.ID)
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
	fmt.Fprintln(termio.Stderr(), "usage: thruserv [--port N] [--max-sessions N] [--max-receivers-per-sender N]")
	fmt.Fprintln(termio.Stderr(), "  --port N                     server port (default 8080)")
	fmt.Fprintln(termio.Stderr(), "  --max-sessions N             max concurrent sessions (default 1000)")
	fmt.Fprintln(termio.Stderr(), "  --max-receivers-per-sender N max receivers per sender (default 10)")
	fmt.Fprintln(termio.Stderr(), "  --max-message-bytes N        max websocket message size (default 65536)")
	fmt.Fprintln(termio.Stderr(), "  --ws-connects-per-min N      max websocket connects per minute per IP (default 30)")
	fmt.Fprintln(termio.Stderr(), "  --ws-connects-burst N        burst websocket connects per IP (default 10)")
	fmt.Fprintln(termio.Stderr(), "  --ws-msgs-per-sec N          max websocket messages per second per connection (default 50)")
	fmt.Fprintln(termio.Stderr(), "  --ws-msgs-burst N            burst websocket messages per connection (default 100)")
	fmt.Fprintln(termio.Stderr(), "  --session-creates-per-min N  max session creates per minute per IP (default 10)")
	fmt.Fprintln(termio.Stderr(), "  --session-creates-burst N    burst session creates per minute per IP (default 5)")
	fmt.Fprintln(termio.Stderr(), "  --max-ws-connections N       max concurrent websocket connections (default 2000)")
	fmt.Fprintln(termio.Stderr(), "  --ws-idle-timeout DURATION   websocket idle timeout (default 10m)")
	fmt.Fprintln(termio.Stderr(), "  --session-timeout DURATION   max session lifetime (default 24h, 0 disables)")
	fmt.Fprintln(termio.Stderr(), "  --turn-server URLS           TURN server URLs (repeatable, comma-separated)")
	fmt.Fprintln(termio.Stderr(), "                              example: --turn-server turns:stun.bytepipe.app:5349?servername=stun.bytepipe.app")
	fmt.Fprintln(termio.Stderr(), "  --turn-static-auth-secret S  TURN REST static auth secret (coturn use-auth-secret)")
	fmt.Fprintln(termio.Stderr(), "  --turn-cred-ttl DURATION      TURN credential TTL (default 1h)")
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

type turnIssuer struct {
	servers []string
	secret  []byte
	ttl     time.Duration
}

func newTurnIssuer(cfg config.ServerConfig, logger *slog.Logger) *turnIssuer {
	if len(cfg.TurnServers) == 0 || cfg.TurnStaticAuthSecret == "" {
		if len(cfg.TurnServers) == 0 && cfg.TurnStaticAuthSecret != "" {
			logger.Warn("TURN static auth secret set but no TURN servers configured")
		}
		if len(cfg.TurnServers) > 0 && cfg.TurnStaticAuthSecret == "" {
			logger.Warn("TURN servers configured but no static auth secret set")
		}
		return nil
	}
	ttl := cfg.TurnCredentialTTL
	if ttl <= 0 {
		ttl = 1 * time.Hour
	}
	issuer := &turnIssuer{
		servers: cfg.TurnServers,
		secret:  []byte(cfg.TurnStaticAuthSecret),
		ttl:     ttl,
	}
	logger.Info("TURN credential issuer enabled", "servers", len(cfg.TurnServers), "ttl", ttl)
	return issuer
}

func (t *turnIssuer) Issue(peerID string) (protocol.TurnCredentials, error) {
	if t == nil || len(t.servers) == 0 || len(t.secret) == 0 {
		return protocol.TurnCredentials{}, fmt.Errorf("turn issuer not configured")
	}
	expiry := time.Now().Add(t.ttl).UTC()
	username := fmt.Sprintf("%d:%s", expiry.Unix(), peerID)
	password := buildTurnPassword(t.secret, username)
	servers := make([]string, 0, len(t.servers))
	for _, raw := range t.servers {
		credURL, err := injectTurnCredentials(raw, username, password)
		if err != nil {
			return protocol.TurnCredentials{}, err
		}
		servers = append(servers, credURL)
	}
	return protocol.TurnCredentials{
		Servers:   servers,
		ExpiresAt: expiry.Format(time.RFC3339),
	}, nil
}

func buildTurnPassword(secret []byte, username string) string {
	mac := hmac.New(sha1.New, secret)
	_, _ = mac.Write([]byte(username))
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

func injectTurnCredentials(raw, username, password string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("empty TURN server")
	}
	switch {
	case strings.HasPrefix(raw, "turns://"):
	case strings.HasPrefix(raw, "turns:"):
		raw = "turns://" + strings.TrimPrefix(raw, "turns:")
	case strings.HasPrefix(raw, "turn://"):
	case strings.HasPrefix(raw, "turn:"):
		raw = "turn://" + strings.TrimPrefix(raw, "turn:")
	default:
		raw = "turn://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("parse TURN server: %w", err)
	}
	if u.Scheme != "turn" && u.Scheme != "turns" {
		return "", fmt.Errorf("unsupported TURN scheme %q", u.Scheme)
	}
	if u.Host == "" && u.Path != "" {
		u.Host = u.Path
		u.Path = ""
	}
	if u.Host == "" {
		return "", fmt.Errorf("missing TURN host")
	}
	u.User = url.UserPassword(username, password)
	return u.String(), nil
}
