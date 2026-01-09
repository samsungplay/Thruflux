package session

import (
	"crypto/rand"
	"encoding/hex"
	"strings"
	"sync"
	"time"
)

// Session represents a signaling session.
type Session struct {
	ID        string    `json:"session_id"`
	JoinCode  string    `json:"join_code"`
	CreatedAt time.Time `json:"created_at"`
	ExpiresAt time.Time `json:"expires_at"`
}

// Store is a thread-safe in-memory store for sessions.
type Store struct {
	mu        sync.RWMutex
	sessions  map[string]Session      // keyed by session ID
	byCode    map[string]string       // join code -> session ID
	ttl       time.Duration
}

// NewStore creates a new session store with the specified TTL.
func NewStore(ttl time.Duration) *Store {
	return &Store{
		sessions: make(map[string]Session),
		byCode:   make(map[string]string),
		ttl:      ttl,
	}
}

// Create creates a new session with a unique ID and join code.
func (s *Store) Create() Session {
	now := time.Now()
	session := Session{
		ID:        generateSessionID(),
		JoinCode:  generateJoinCode(),
		CreatedAt: now,
		ExpiresAt: now.Add(s.ttl),
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Ensure join code is unique (retry if collision)
	for _, exists := s.byCode[session.JoinCode]; exists; {
		session.JoinCode = generateJoinCode()
		_, exists = s.byCode[session.JoinCode]
	}

	s.sessions[session.ID] = session
	s.byCode[session.JoinCode] = session.ID

	return session
}

// GetByJoinCode retrieves a session by its join code.
// Returns the session and true if found, or zero value and false if not found.
func (s *Store) GetByJoinCode(code string) (Session, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	sessionID, exists := s.byCode[code]
	if !exists {
		return Session{}, false
	}

	session, exists := s.sessions[sessionID]
	return session, exists
}

// CleanupExpired removes all expired sessions from the store.
// Returns the number of sessions removed.
func (s *Store) CleanupExpired(now time.Time) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	var toRemove []string
	for id, session := range s.sessions {
		if now.After(session.ExpiresAt) {
			toRemove = append(toRemove, id)
		}
	}

	for _, id := range toRemove {
		session := s.sessions[id]
		delete(s.sessions, id)
		delete(s.byCode, session.JoinCode)
	}

	return len(toRemove)
}

// generateSessionID generates a random 32-character hex string for session identification.
func generateSessionID() string {
	b := make([]byte, 16) // 16 bytes = 32 hex characters
	if _, err := rand.Read(b); err != nil {
		// Fallback if rand fails (should be extremely rare)
		return strings.Repeat("0", 32)
	}
	return hex.EncodeToString(b)
}

// generateJoinCode generates a random 8-character join code.
// Uses uppercase A-Z and 0-9, excluding ambiguous characters: O, 0, I, 1.
func generateJoinCode() string {
	// Characters: A-Z excluding I and O, and 2-9 excluding 0 and 1
	chars := "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
	code := make([]byte, 8)
	b := make([]byte, 8)

	if _, err := rand.Read(b); err != nil {
		// Fallback if rand fails
		return "ABCDEFGH"
	}

	for i := 0; i < 8; i++ {
		code[i] = chars[b[i]%byte(len(chars))]
	}

	return string(code)
}

