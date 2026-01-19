package wsclient

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/sheerbytes/sheerbytes/pkg/protocol"
)

// Conn represents a WebSocket connection to the server.
type Conn struct {
	conn     *websocket.Conn
	logger   *slog.Logger
	sendChan chan protocol.Envelope
	done     chan struct{}
	writeMu  sync.Mutex
}

var dialer = websocket.Dialer{
	HandshakeTimeout: 5 * time.Second,
}

// Dial establishes a WebSocket connection to the server.
// wsURL should be the full WebSocket URL including path and query parameters.
func Dial(ctx context.Context, wsURL string, logger *slog.Logger) (*Conn, error) {
	// Parse URL
	u, err := url.Parse(wsURL)
	if err != nil {
		return nil, err
	}

	// Create request headers
	headers := http.Header{}

	// Dial with context
	conn, resp, err := dialer.DialContext(ctx, u.String(), headers)
	if err != nil {
		if resp != nil {
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if len(body) > 0 {
				return nil, fmt.Errorf("websocket upgrade failed (%d): %s", resp.StatusCode, string(body))
			}
			return nil, fmt.Errorf("websocket upgrade failed (%d)", resp.StatusCode)
		}
		return nil, err
	}

	c := &Conn{
		conn:     conn,
		logger:   logger,
		sendChan: make(chan protocol.Envelope, 256), // Buffered channel for sends
		done:     make(chan struct{}),
	}

	// Start writer goroutine for serialized writes
	go c.writeLoop()

	return c, nil
}

// ReadLoop reads messages from the WebSocket connection and calls onEnv for each envelope.
// Returns when the connection is closed or context is cancelled.
func (c *Conn) ReadLoop(ctx context.Context, onEnv func(env protocol.Envelope)) error {
	// Set read deadline
	c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	// Start pinger
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				c.writeMu.Lock()
				c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
				err := c.conn.WriteMessage(websocket.PingMessage, nil)
				c.writeMu.Unlock()
				if err != nil {
					return
				}
			}
		}
	}()

	go func() {
		<-ctx.Done()
		// Closing the connection forces ReadMessage() to unblock instantly
		c.conn.Close()
	}()

	for {
		// Check context
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Read message
		messageType, message, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				c.logger.Error("websocket read error", "error", err)
			}
			return err
		}

		// Only process text messages
		if messageType != websocket.TextMessage {
			continue
		}

		// Parse envelope
		var env protocol.Envelope
		if err := json.Unmarshal(message, &env); err != nil {
			c.logger.Warn("invalid JSON envelope", "error", err)
			continue
		}

		// Call callback
		onEnv(env)
	}
}

// Send sends an envelope over the WebSocket connection.
// Uses a buffered channel to serialize writes and avoid concurrent write issues.
func (c *Conn) Send(env protocol.Envelope) error {
	select {
	case c.sendChan <- env:
		return nil
	case <-c.done:
		return fmt.Errorf("connection closed")
	}
}

// writeLoop handles serialized writes to the WebSocket connection.
func (c *Conn) writeLoop() {
	defer close(c.done)
	for {
		select {
		case env, ok := <-c.sendChan:
			if !ok {
				return
			}
			c.writeMu.Lock()
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			err := c.conn.WriteJSON(env)
			c.writeMu.Unlock()
			if err != nil {
				c.logger.Error("websocket write error", "error", err)
				return
			}
		}
	}
}

// Close closes the WebSocket connection.
func (c *Conn) Close() error {
	close(c.sendChan)
	<-c.done // Wait for write loop to finish
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.conn.Close()
}
