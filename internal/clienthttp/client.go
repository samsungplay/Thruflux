package clienthttp

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// SessionResponse represents the response from POST /session.
type SessionResponse struct {
	SessionID string    `json:"session_id"`
	JoinCode  string    `json:"join_code"`
	ExpiresAt time.Time `json:"expires_at"`
}

// CreateSession creates a new session by calling POST /session on the server.
// Returns sessionID, joinCode, expiresAt, and any error.
// Uses a 5 second timeout for the HTTP request.
func CreateSession(ctx context.Context, serverURL string, maxReceivers int) (sessionID, joinCode string, expiresAt time.Time, err error) {
	// Build the URL
	url := serverURL + "/session"
	if url[:4] != "http" {
		url = "http://" + url
	}
	if maxReceivers > 0 {
		url = fmt.Sprintf("%s?max_receivers=%d", url, maxReceivers)
	}

	// Create HTTP client with timeout
	client := &http.Client{
		Timeout: 5 * time.Second,
	}

	// Create request
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
	if err != nil {
		return "", "", time.Time{}, fmt.Errorf("create request: %w", err)
	}

	// Send request
	resp, err := client.Do(req)
	if err != nil {
		return "", "", time.Time{}, fmt.Errorf("send request: %w", err)
	}
	defer resp.Body.Close()

	// Read response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", "", time.Time{}, fmt.Errorf("read response: %w", err)
	}

	// Check status code
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", "", time.Time{}, fmt.Errorf("server returned %d: %s", resp.StatusCode, string(body))
	}

	// Parse JSON response
	var sessionResp struct {
		SessionID string `json:"session_id"`
		JoinCode  string `json:"join_code"`
		ExpiresAt string `json:"expires_at"` // RFC3339 string
	}
	if err := json.Unmarshal(body, &sessionResp); err != nil {
		return "", "", time.Time{}, fmt.Errorf("parse response: %w", err)
	}

	// Parse expires_at as RFC3339
	expiresAt, parseErr := time.Parse(time.RFC3339, sessionResp.ExpiresAt)
	if parseErr != nil {
		return "", "", time.Time{}, fmt.Errorf("parse expires_at: %w", parseErr)
	}

	return sessionResp.SessionID, sessionResp.JoinCode, expiresAt, nil
}
