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

func TestStore_Delete(t *testing.T) {
	store := NewStore(30 * time.Minute)
	session := store.Create()

	_, found := store.GetByJoinCode(session.JoinCode)
	if !found {
		t.Fatal("session should exist")
	}

	store.Delete(session.ID)

	_, found = store.GetByJoinCode(session.JoinCode)
	if found {
		t.Fatal("session should be deleted")
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
			store.Delete("missing")
		}
		done <- true
	}()

	// Wait for all goroutines
	for i := 0; i < 4; i++ {
		<-done
	}

	// Should not have panicked or deadlocked
}
