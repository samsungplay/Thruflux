package clienthttp

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestCreateSession_Success(t *testing.T) {
	// Create test server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if r.URL.Path != "/session" {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		response := map[string]interface{}{
			"session_id": "test-session-id-123",
			"join_code":  "ABCDEFGH",
			"expires_at": time.Now().Add(30 * time.Minute).Format(time.RFC3339),
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	// Call CreateSession
	ctx := context.Background()
	sessionID, joinCode, expiresAt, err := CreateSession(ctx, server.URL, 0)

	if err != nil {
		t.Fatalf("CreateSession() error = %v", err)
	}

	if sessionID != "test-session-id-123" {
		t.Errorf("sessionID = %s, want test-session-id-123", sessionID)
	}
	if joinCode != "ABCDEFGH" {
		t.Errorf("joinCode = %s, want ABCDEFGH", joinCode)
	}
	if expiresAt.IsZero() {
		t.Error("expiresAt should not be zero")
	}
}

func TestCreateSession_Non2xx(t *testing.T) {
	// Create test server that returns error
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"error":"invalid request"}`))
	}))
	defer server.Close()

	// Call CreateSession
	ctx := context.Background()
	_, _, _, err := CreateSession(ctx, server.URL, 0)

	if err == nil {
		t.Fatal("CreateSession() expected error, got nil")
	}

	expected := "server returned 400"
	if err.Error()[:len(expected)] != expected {
		t.Errorf("error = %v, want error containing %s", err, expected)
	}
}

func TestCreateSession_InvalidJSON(t *testing.T) {
	// Create test server that returns invalid JSON
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte(`invalid json`))
	}))
	defer server.Close()

	// Call CreateSession
	ctx := context.Background()
	_, _, _, err := CreateSession(ctx, server.URL, 0)

	if err == nil {
		t.Fatal("CreateSession() expected error, got nil")
	}

	expected := "parse response"
	if err.Error()[:len(expected)] != expected {
		t.Errorf("error = %v, want error containing %s", err, expected)
	}
}

func TestCreateSession_ContextCancellation(t *testing.T) {
	// Create test server that hangs
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(10 * time.Second)
	}))
	defer server.Close()

	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// Call CreateSession
	_, _, _, err := CreateSession(ctx, server.URL, 0)

	if err == nil {
		t.Fatal("CreateSession() expected error, got nil")
	}
}
