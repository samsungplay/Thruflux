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
	// Default: true (reliable ordered)
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
		Ordered:     true,
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
		stream := newWebRTCStream(dc, logger)
		c.mu.Lock()
		closed := c.incomingClosed
		c.mu.Unlock()
		if closed {
			dc.Close()
			return
		}
		select {
		case c.incomingCh <- stream:
		default:
			logger.Warn("incoming data channel buffer full, dropping", "label", dc.Label())
			dc.Close()
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

	stream := newWebRTCStream(dc, c.logger)

	// Wait for the data channel to open
	openCh := make(chan struct{})
	dc.OnOpen(func() {
		close(openCh)
	})

	select {
	case <-ctx.Done():
		dc.Close()
		return nil, ctx.Err()
	case <-openCh:
		return stream, nil
	case <-time.After(30 * time.Second):
		dc.Close()
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
		if err := stream.waitOpen(ctx); err != nil {
			return nil, err
		}
		return stream, nil
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

// WebRTCStream wraps a DataChannel and implements transfer.Stream.
type WebRTCStream struct {
	dc     *webrtc.DataChannel
	logger *slog.Logger

	mu       sync.Mutex
	closed   bool
	openCh   chan struct{}
	openOnce sync.Once

	// Read buffer
	readBuf  []byte
	readCond *sync.Cond
	readErr  error
}

func newWebRTCStream(dc *webrtc.DataChannel, logger *slog.Logger) *WebRTCStream {
	s := &WebRTCStream{
		dc:     dc,
		logger: logger,
		openCh: make(chan struct{}),
	}
	s.readCond = sync.NewCond(&s.mu)

	// Handle data channel open
	dc.OnOpen(func() {
		s.openOnce.Do(func() {
			close(s.openCh)
		})
	})

	// Handle incoming messages
	dc.OnMessage(func(msg webrtc.DataChannelMessage) {
		s.mu.Lock()
		s.readBuf = append(s.readBuf, msg.Data...)
		s.mu.Unlock()
		s.readCond.Signal()
	})

	// Handle errors
	dc.OnError(func(err error) {
		s.mu.Lock()
		if s.readErr == nil {
			s.readErr = err
		}
		s.mu.Unlock()
		s.readCond.Signal()
	})

	// Handle close
	dc.OnClose(func() {
		s.mu.Lock()
		if s.readErr == nil {
			s.readErr = io.EOF
		}
		s.mu.Unlock()
		s.readCond.Signal()
	})

	return s
}

// waitOpen blocks until the data channel is open.
func (s *WebRTCStream) waitOpen(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.openCh:
		return nil
	case <-time.After(30 * time.Second):
		return errors.New("timeout waiting for data channel to open")
	}
}

// StreamID returns the data channel ID for multi-stream protocols.
func (s *WebRTCStream) StreamID() uint64 {
	if s.dc.ID() == nil {
		return 0
	}
	return uint64(*s.dc.ID())
}

// Read reads data from the stream.
func (s *WebRTCStream) Read(p []byte) (n int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Wait for data or error
	for len(s.readBuf) == 0 && s.readErr == nil {
		s.readCond.Wait()
	}

	if len(s.readBuf) > 0 {
		n = copy(p, s.readBuf)
		s.readBuf = s.readBuf[n:]
		return n, nil
	}

	return 0, s.readErr
}

// Write writes data to the stream.
func (s *WebRTCStream) Write(p []byte) (n int, err error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return 0, io.ErrClosedPipe
	}
	s.mu.Unlock()

	// Send data
	if err := s.dc.Send(p); err != nil {
		return 0, fmt.Errorf("failed to send data: %w", err)
	}
	return len(p), nil
}

// Close closes the stream.
func (s *WebRTCStream) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	if s.readErr == nil {
		s.readErr = io.ErrClosedPipe
	}
	s.mu.Unlock()
	s.readCond.Signal()

	return s.dc.Close()
}
