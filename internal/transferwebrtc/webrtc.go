package transferwebrtc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/datachannel"
	"github.com/pion/webrtc/v4"
	"github.com/sheerbytes/sheerbytes/internal/transfer"
)

var (
	_ transfer.Transport = (*WebRTCTransport)(nil)
	_ transfer.Conn      = (*WebRTCConn)(nil)
	_ transfer.Stream    = (*WebRTCStream)(nil)
)

// Config holds WebRTC transport configuration.
type Config struct {
	// Ordered specifies if data channels should guarantee ordering.
	// Set to false for parallel streams to avoid head-of-line blocking.
	// Default: false (unordered for better parallel performance)
	Ordered bool

	// MaxChannels is the maximum number of concurrent data channels.
	// Default: 100
	MaxChannels int

	// Logger for debug output.
	Logger *slog.Logger
}

// DefaultConfig returns the default WebRTC transport configuration.
func DefaultConfig() Config {
	return Config{
		Ordered:     false, // Unordered by default for parallel streams
		MaxChannels: 100,
	}
}

// WebRTCTransport wraps a PeerConnection and implements transfer.Transport.
type WebRTCTransport struct {
	pc     *webrtc.PeerConnection
	config Config
	logger *slog.Logger

	mu     sync.Mutex
	conn   *WebRTCConn
	closed bool
}

// NewTransport creates a new WebRTC transport from an established PeerConnection.
func NewTransport(pc *webrtc.PeerConnection, config Config) *WebRTCTransport {
	if config.MaxChannels <= 0 {
		config.MaxChannels = 100
	}
	logger := config.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &WebRTCTransport{
		pc:     pc,
		config: config,
		logger: logger,
	}
}

// Dial returns a connection for opening streams (sender side).
// peerID is ignored as the PeerConnection is already established.
func (t *WebRTCTransport) Dial(ctx context.Context, peerID string) (transfer.Conn, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return nil, io.ErrClosedPipe
	}

	if t.conn == nil {
		t.conn = newWebRTCConn(t.pc, t.config, t.logger, true)
	}
	return t.conn, nil
}

// Accept returns a connection for accepting streams (receiver side).
func (t *WebRTCTransport) Accept(ctx context.Context) (transfer.Conn, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return nil, io.ErrClosedPipe
	}

	if t.conn == nil {
		t.conn = newWebRTCConn(t.pc, t.config, t.logger, false)
	}
	return t.conn, nil
}

// Close closes the transport.
func (t *WebRTCTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return nil
	}
	t.closed = true

	if t.conn != nil {
		t.conn.Close()
	}
	return t.pc.Close()
}

// WebRTCConn wraps a PeerConnection and implements transfer.Conn.
type WebRTCConn struct {
	pc     *webrtc.PeerConnection
	config Config
	logger *slog.Logger
	dialer bool // true if this side opens channels (sender)

	mu             sync.Mutex
	closed         bool
	nextChannelID  uint64
	incomingCh     chan *WebRTCStream
	incomingClosed bool
}

func newWebRTCConn(pc *webrtc.PeerConnection, config Config, logger *slog.Logger, dialer bool) *WebRTCConn {
	c := &WebRTCConn{
		pc:         pc,
		config:     config,
		logger:     logger,
		dialer:     dialer,
		incomingCh: make(chan *WebRTCStream, config.MaxChannels),
	}

	// Set up handler for incoming data channels
	pc.OnDataChannel(func(dc *webrtc.DataChannel) {
		stream, err := newWebRTCStreamFromDC(dc, logger)
		if err != nil {
			logger.Warn("failed to create stream from incoming channel", "error", err)
			dc.Close()
			return
		}
		c.mu.Lock()
		closed := c.incomingClosed
		c.mu.Unlock()
		if closed {
			stream.Close()
			return
		}
		select {
		case c.incomingCh <- stream:
		default:
			logger.Warn("incoming data channel buffer full, dropping", "label", dc.Label())
			stream.Close()
		}
	})

	return c
}

// OpenStream opens a new data channel (for sender).
func (c *WebRTCConn) OpenStream(ctx context.Context) (transfer.Stream, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, io.ErrClosedPipe
	}
	channelID := atomic.AddUint64(&c.nextChannelID, 1)
	c.mu.Unlock()

	label := fmt.Sprintf("stream-%d", channelID)
	ordered := c.config.Ordered

	dc, err := c.pc.CreateDataChannel(label, &webrtc.DataChannelInit{
		Ordered: &ordered,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create data channel: %w", err)
	}

	stream, err := newWebRTCStreamFromDC(dc, c.logger)
	if err != nil {
		dc.Close()
		return nil, err
	}

	// Wait for the data channel to open
	select {
	case <-ctx.Done():
		stream.Close()
		return nil, ctx.Err()
	case <-stream.openCh:
		return stream, nil
	case <-time.After(30 * time.Second):
		stream.Close()
		return nil, errors.New("timeout waiting for data channel to open")
	}
}

// AcceptStream waits for an incoming data channel (for receiver).
func (c *WebRTCConn) AcceptStream(ctx context.Context) (transfer.Stream, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, io.ErrClosedPipe
	}
	c.mu.Unlock()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case stream, ok := <-c.incomingCh:
		if !ok {
			return nil, io.ErrClosedPipe
		}
		// Wait for the data channel to be open
		select {
		case <-ctx.Done():
			stream.Close()
			return nil, ctx.Err()
		case <-stream.openCh:
			return stream, nil
		case <-time.After(30 * time.Second):
			stream.Close()
			return nil, errors.New("timeout waiting for data channel to open")
		}
	}
}

// Close closes the connection.
func (c *WebRTCConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}
	c.closed = true
	c.incomingClosed = true
	close(c.incomingCh)

	return nil
}

// WebRTCStream wraps a detached DataChannel and implements transfer.Stream.
// Uses the detached API for high throughput.
type WebRTCStream struct {
	dc       *webrtc.DataChannel
	detached datachannel.ReadWriteCloser
	logger   *slog.Logger
	dcID     uint16

	mu       sync.Mutex
	closed   bool
	openCh   chan struct{}
	openOnce sync.Once
}

func newWebRTCStreamFromDC(dc *webrtc.DataChannel, logger *slog.Logger) (*WebRTCStream, error) {
	s := &WebRTCStream{
		dc:     dc,
		logger: logger,
		openCh: make(chan struct{}),
	}

	// Handle data channel open - detach when ready
	dc.OnOpen(func() {
		// Store ID before detach
		if id := dc.ID(); id != nil {
			s.dcID = *id
		}

		// Detach for raw io.ReadWriteCloser access (high performance)
		detached, err := dc.Detach()
		if err != nil {
			logger.Error("failed to detach data channel", "error", err)
			return
		}
		s.mu.Lock()
		s.detached = detached
		s.mu.Unlock()

		s.openOnce.Do(func() {
			close(s.openCh)
		})
	})

	// Check if already open
	if dc.ReadyState() == webrtc.DataChannelStateOpen {
		if id := dc.ID(); id != nil {
			s.dcID = *id
		}
		detached, err := dc.Detach()
		if err != nil {
			return nil, fmt.Errorf("failed to detach open data channel: %w", err)
		}
		s.detached = detached
		s.openOnce.Do(func() {
			close(s.openCh)
		})
	}

	return s, nil
}

// StreamID returns the data channel ID for multi-stream protocols.
func (s *WebRTCStream) StreamID() uint64 {
	return uint64(s.dcID)
}

// Read reads data from the stream using the detached io.Reader.
func (s *WebRTCStream) Read(p []byte) (n int, err error) {
	s.mu.Lock()
	detached := s.detached
	s.mu.Unlock()

	if detached == nil {
		// Wait for open
		<-s.openCh
		s.mu.Lock()
		detached = s.detached
		s.mu.Unlock()
	}

	if detached == nil {
		return 0, io.ErrClosedPipe
	}

	return detached.Read(p)
}

// Write writes data to the stream using the detached io.Writer.
func (s *WebRTCStream) Write(p []byte) (n int, err error) {
	s.mu.Lock()
	detached := s.detached
	closed := s.closed
	s.mu.Unlock()

	if closed {
		return 0, io.ErrClosedPipe
	}

	if detached == nil {
		// Wait for open
		<-s.openCh
		s.mu.Lock()
		detached = s.detached
		s.mu.Unlock()
	}

	if detached == nil {
		return 0, io.ErrClosedPipe
	}

	// Write directly using the detached channel's writer
	// The SCTP layer handles chunking internally
	return detached.Write(p)
}

// Close closes the stream.
func (s *WebRTCStream) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	detached := s.detached
	s.mu.Unlock()

	// Signal open in case someone is waiting
	s.openOnce.Do(func() {
		close(s.openCh)
	})

	if detached != nil {
		return detached.Close()
	}
	return s.dc.Close()
}
