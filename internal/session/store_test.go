package session

import (
	"testing"
	"time"
)

func TestStore_Create(t *testing.T) {
	store := NewStore(30 * time.Minute)

	// Create multiple sessions
	sessions := make([]Session, 100)
	ids := make(map[string]bool)
	codes := make(map[string]bool)

	for i := 0; i < 100; i++ {
		sessions[i] = store.Create()
		id := sessions[i].ID
		code := sessions[i].JoinCode

		// Verify ID is unique
		if ids[id] {
			t.Errorf("Duplicate session ID: %s", id)
		}
		ids[id] = true

		// Verify join code is unique
		if codes[code] {
			t.Errorf("Duplicate join code: %s", code)
		}
		codes[code] = true

		// Verify ID format (32 hex chars)
		if len(id) != 32 {
			t.Errorf("Session ID length = %d, want 32", len(id))
		}

		// Verify join code format (8 chars, uppercase A-Z0-9, no ambiguous)
		if len(code) != 8 {
			t.Errorf("Join code length = %d, want 8", len(code))
		}

		for _, char := range code {
			if char == 'O' || char == '0' || char == 'I' || char == '1' {
				t.Errorf("Join code contains ambiguous character: %c in %s", char, code)
			}
			if !((char >= 'A' && char <= 'Z') || (char >= '2' && char <= '9')) {
				t.Errorf("Join code contains invalid character: %c in %s", char, code)
			}
		}

		// Verify timestamps
		if sessions[i].CreatedAt.IsZero() {
			t.Error("CreatedAt should not be zero")
		}
		if sessions[i].ExpiresAt.IsZero() {
			t.Error("ExpiresAt should not be zero")
		}
		if !sessions[i].ExpiresAt.After(sessions[i].CreatedAt) {
			t.Error("ExpiresAt should be after CreatedAt")
		}
	}
}

func TestStore_GetByJoinCode(t *testing.T) {
	store := NewStore(30 * time.Minute)

	// Create a session
	session := store.Create()

	// Retrieve by join code
	retrieved, found := store.GetByJoinCode(session.JoinCode)
	if !found {
		t.Fatal("Session should be found by join code")
	}

	if retrieved.ID != session.ID {
		t.Errorf("Retrieved ID = %s, want %s", retrieved.ID, session.ID)
	}
	if retrieved.JoinCode != session.JoinCode {
		t.Errorf("Retrieved JoinCode = %s, want %s", retrieved.JoinCode, session.JoinCode)
	}

	// Try non-existent code
	_, found = store.GetByJoinCode("INVALID")
	if found {
		t.Error("Should not find session for invalid join code")
	}
}

func TestStore_CleanupExpired(t *testing.T) {
	store := NewStore(30 * time.Minute)

	// Create sessions with different expiration times
	now := time.Now()

	// This will create a session with normal expiration
	session1 := store.Create()

	// Create expired session by manipulating the store directly (for testing)
	expiredSession := Session{
		ID:        "expired123",
		JoinCode:  "EXPIRED1",
		CreatedAt: now.Add(-1 * time.Hour),
		ExpiresAt: now.Add(-1 * time.Minute), // Expired
	}

	store.mu.Lock()
	store.sessions[expiredSession.ID] = expiredSession
	store.byCode[expiredSession.JoinCode] = expiredSession.ID
	store.mu.Unlock()

	// Verify both sessions exist
	_, found := store.GetByJoinCode(session1.JoinCode)
	if !found {
		t.Fatal("session1 should exist")
	}
	_, found = store.GetByJoinCode(expiredSession.JoinCode)
	if !found {
		t.Fatal("expiredSession should exist before cleanup")
	}

	// Cleanup expired sessions
	removed := store.CleanupExpired(now)
	if removed != 1 {
		t.Errorf("CleanupExpired() removed = %d, want 1", removed)
	}

	// Verify expired session is gone
	_, found = store.GetByJoinCode(expiredSession.JoinCode)
	if found {
		t.Error("expiredSession should be removed")
	}

	// Verify non-expired session still exists
	_, found = store.GetByJoinCode(session1.JoinCode)
	if !found {
		t.Error("session1 should still exist")
	}

	// Cleanup again should remove nothing
	removed = store.CleanupExpired(now)
	if removed != 0 {
		t.Errorf("CleanupExpired() removed = %d, want 0", removed)
	}
}

func TestStore_CleanupExpired_Multiple(t *testing.T) {
	store := NewStore(30 * time.Minute)
	now := time.Now()

	// Create multiple expired sessions
	expiredSessions := []Session{
		{ID: "exp1", JoinCode: "EXPIRED1", ExpiresAt: now.Add(-1 * time.Minute)},
		{ID: "exp2", JoinCode: "EXPIRED2", ExpiresAt: now.Add(-2 * time.Minute)},
		{ID: "exp3", JoinCode: "EXPIRED3", ExpiresAt: now.Add(-3 * time.Minute)},
	}

	store.mu.Lock()
	for _, session := range expiredSessions {
		session.CreatedAt = now.Add(-1 * time.Hour)
		store.sessions[session.ID] = session
		store.byCode[session.JoinCode] = session.ID
	}
	store.mu.Unlock()

	removed := store.CleanupExpired(now)
	if removed != 3 {
		t.Errorf("CleanupExpired() removed = %d, want 3", removed)
	}

	// Verify all expired sessions are gone
	for _, session := range expiredSessions {
		_, found := store.GetByJoinCode(session.JoinCode)
		if found {
			t.Errorf("Session %s should be removed", session.JoinCode)
		}
	}
}

func TestStore_ConcurrentAccess(t *testing.T) {
	store := NewStore(30 * time.Minute)
	done := make(chan bool)

	// Concurrent creates
	go func() {
		for i := 0; i < 50; i++ {
			store.Create()
		}
		done <- true
	}()

	go func() {
		for i := 0; i < 50; i++ {
			store.Create()
		}
		done <- true
	}()

	// Concurrent lookups
	go func() {
		for i := 0; i < 50; i++ {
			store.GetByJoinCode("TESTCODE")
		}
		done <- true
	}()

	go func() {
		for i := 0; i < 50; i++ {
			store.CleanupExpired(time.Now())
		}
		done <- true
	}()

	// Wait for all goroutines
	for i := 0; i < 4; i++ {
		<-done
	}

	// Should not have panicked or deadlocked
}

