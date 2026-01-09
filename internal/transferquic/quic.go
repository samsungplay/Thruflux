package transferquic

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"

	"github.com/quic-go/quic-go"
	"github.com/sheerbytes/sheerbytes/internal/transfer"
)

var (
	_ transfer.Transport = (*QUICTransport)(nil)
	_ transfer.Conn      = (*QUICConn)(nil)
	_ transfer.Stream    = (*QUICStream)(nil)
)

// QUICTransport is a Transport implementation backed by QUIC.
// It can act as either a dialer (client) or listener (server).
type QUICTransport struct {
	mu       sync.Mutex
	role     string         // "dialer" or "listener"
	conn     *quic.Conn     // For dialer: the established connection
	listener *quic.Listener // For listener: the listener
	logger   *slog.Logger
	closed   bool
}

// NewDialer creates a QUICTransport that acts as a dialer (client).
// It uses the provided QUIC connection for dialing.
func NewDialer(conn *quic.Conn, logger *slog.Logger) *QUICTransport {
	return &QUICTransport{
		role:   "dialer",
		conn:   conn,
		logger: logger,
	}
}

// NewListener creates a QUICTransport that acts as a listener (server).
// It uses the provided QUIC listener for accepting connections.
func NewListener(listener *quic.Listener, logger *slog.Logger) *QUICTransport {
	return &QUICTransport{
		role:     "listener",
		listener: listener,
		logger:   logger,
	}
}

// Dial establishes a connection to the specified peer.
// For QUIC dialer, this returns the existing connection wrapped in QUICConn.
// peerID is ignored for QUIC (connection is already established).
func (t *QUICTransport) Dial(ctx context.Context, peerID string) (transfer.Conn, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return nil, io.ErrClosedPipe
	}

	if t.role != "dialer" {
		return nil, fmt.Errorf("Dial called on listener transport")
	}

	if t.conn == nil {
		return nil, fmt.Errorf("QUIC connection not available")
	}

	return &QUICConn{
		conn:   t.conn,
		logger: t.logger,
	}, nil
}

// Accept waits for and accepts an incoming connection from another peer.
// For QUIC listener, this accepts a new QUIC connection.
func (t *QUICTransport) Accept(ctx context.Context) (transfer.Conn, error) {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil, io.ErrClosedPipe
	}
	if t.role != "listener" {
		t.mu.Unlock()
		return nil, fmt.Errorf("Accept called on dialer transport")
	}
	listener := t.listener
	t.mu.Unlock()

	if listener == nil {
		return nil, fmt.Errorf("QUIC listener not available")
	}

	conn, err := listener.Accept(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to accept QUIC connection: %w", err)
	}

	t.logger.Info("QUIC connection accepted", "remote_addr", conn.RemoteAddr())

	return &QUICConn{
		conn:   conn,
		logger: t.logger,
	}, nil
}

// Close closes the transport and all associated connections.
func (t *QUICTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return nil
	}
	t.closed = true

	if t.role == "listener" && t.listener != nil {
		if err := t.listener.Close(); err != nil {
			return fmt.Errorf("failed to close QUIC listener: %w", err)
		}
	}

	// Note: We don't close the connection here if we're a dialer,
	// as it might be managed elsewhere. The caller should close it.

	return nil
}

// QUICConn wraps a quic.Conn and implements transfer.Conn.
type QUICConn struct {
	mu     sync.Mutex
	conn   *quic.Conn
	logger *slog.Logger
	closed bool
}

// OpenStream opens a new bidirectional stream to the remote peer.
func (c *QUICConn) OpenStream(ctx context.Context) (transfer.Stream, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, io.ErrClosedPipe
	}
	conn := c.conn
	c.mu.Unlock()

	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to open QUIC stream: %w", err)
	}

	c.logger.Debug("QUIC stream opened", "stream_id", stream.StreamID())

	return &QUICStream{
		stream: stream,
		logger: c.logger,
	}, nil
}

// AcceptStream waits for and accepts an incoming stream from the remote peer.
func (c *QUICConn) AcceptStream(ctx context.Context) (transfer.Stream, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, io.ErrClosedPipe
	}
	conn := c.conn
	c.mu.Unlock()

	stream, err := conn.AcceptStream(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to accept QUIC stream: %w", err)
	}

	c.logger.Debug("QUIC stream accepted", "stream_id", stream.StreamID())

	return &QUICStream{
		stream: stream,
		logger: c.logger,
	}, nil
}

// Close closes the connection and all associated streams.
func (c *QUICConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}
	c.closed = true

	if c.conn != nil {
		if err := c.conn.CloseWithError(0, ""); err != nil {
			return fmt.Errorf("failed to close QUIC connection: %w", err)
		}
	}

	return nil
}

// QUICStream wraps a quic.Stream and implements transfer.Stream.
type QUICStream struct {
	mu          sync.Mutex
	stream      *quic.Stream
	logger      *slog.Logger
	closed      bool
	writeClosed bool
}

// Read reads data from the stream.
func (s *QUICStream) Read(p []byte) (n int, err error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return 0, io.ErrClosedPipe
	}
	stream := s.stream
	s.mu.Unlock()

	return stream.Read(p)
}

// Write writes data to the stream.
func (s *QUICStream) Write(p []byte) (n int, err error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return 0, io.ErrClosedPipe
	}
	stream := s.stream
	s.mu.Unlock()

	return stream.Write(p)
}

// StreamID returns the QUIC stream ID.
func (s *QUICStream) StreamID() uint64 {
	s.mu.Lock()
	stream := s.stream
	s.mu.Unlock()

	if stream == nil {
		return 0
	}

	return uint64(stream.StreamID())
}

// CloseWrite closes the write side of the stream, signaling EOF to the receiver.
// This allows the receiver to finish reading while the stream remains open.
func (s *QUICStream) CloseWrite() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.writeClosed {
		return nil
	}
	s.writeClosed = true

	(*s.stream).CancelWrite(0)
	return nil
}

// Close closes the stream.
func (s *QUICStream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}
	s.closed = true

	if err := (*s.stream).Close(); err != nil {
		return fmt.Errorf("failed to close QUIC stream: %w", err)
	}

	return nil
}
