package transfer

import (
	"context"
	"io"
	"sync"
)

// MockTransport is an in-memory transport implementation for testing.
// It allows two MockTransport instances to connect to each other.
type MockTransport struct {
	mu            sync.Mutex
	peerID        string
	acceptChan    chan *mockConn
	connections   map[*mockConn]bool
	peerAcceptChan chan *mockConn // Channel to send connections to peer
	closed        bool
}

// NewMockPair creates a pair of MockTransport instances that can connect to each other.
// The transports are pre-configured to accept connections from each other.
func NewMockPair() (*MockTransport, *MockTransport) {
	t1Accept := make(chan *mockConn, 1)
	t2Accept := make(chan *mockConn, 1)

	t1 := &MockTransport{
		peerID:        "peer1",
		acceptChan:    t1Accept,
		connections:   make(map[*mockConn]bool),
		peerAcceptChan: t2Accept,
	}
	t2 := &MockTransport{
		peerID:        "peer2",
		acceptChan:    t2Accept,
		connections:   make(map[*mockConn]bool),
		peerAcceptChan: t1Accept,
	}

	return t1, t2
}

// mockConn represents a connection between two MockTransport instances.
type mockConn struct {
	mu            sync.Mutex
	transport     *MockTransport
	other         *mockConn
	streamChan    chan *mockStream
	pendingStreams []*mockStream
	closed        bool
}

// mockStream represents a bidirectional stream backed by io.Pipe.
type mockStream struct {
	mu      sync.Mutex
	reader  *io.PipeReader
	writer  *io.PipeWriter
	closed  bool
}

var _ Transport = (*MockTransport)(nil)
var _ Conn = (*mockConn)(nil)
var _ Stream = (*mockStream)(nil)

// Dial establishes a connection to the specified peer.
func (t *MockTransport) Dial(ctx context.Context, peerID string) (Conn, error) {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil, io.ErrClosedPipe
	}
	t.mu.Unlock()

	// Create a connection pair
	localConn := &mockConn{
		transport:      t,
		streamChan:     make(chan *mockStream, 10), // Buffered to avoid blocking
		pendingStreams: make([]*mockStream, 0),
	}
	remoteConn := &mockConn{
		transport:      nil, // Will be set when accepted by peer
		streamChan:     make(chan *mockStream, 10), // Buffered to avoid blocking
		pendingStreams: make([]*mockStream, 0),
	}

	// Link the connections
	localConn.other = remoteConn
	remoteConn.other = localConn

	// Add to connections
	t.mu.Lock()
	t.connections[localConn] = true
	t.mu.Unlock()

	// Send remote connection to peer's accept channel (non-blocking with buffered channel)
	select {
	case t.peerAcceptChan <- remoteConn:
		// Success
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		// Channel full - this shouldn't happen with buffered channel, but handle it
		return nil, io.ErrClosedPipe
	}

	return localConn, nil
}

// Accept waits for and accepts an incoming connection.
func (t *MockTransport) Accept(ctx context.Context) (Conn, error) {
	select {
	case conn := <-t.acceptChan:
		if conn == nil {
			return nil, io.ErrClosedPipe
		}
		// Set the transport for the accepted connection
		conn.transport = t
		t.mu.Lock()
		t.connections[conn] = true
		t.mu.Unlock()
		return conn, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Close closes the transport and all connections.
func (t *MockTransport) Close() error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	
	// Collect connections to close (avoid holding lock while closing)
	conns := make([]*mockConn, 0, len(t.connections))
	for conn := range t.connections {
		conns = append(conns, conn)
	}
	t.connections = nil
	t.mu.Unlock()

	// Close all connections (without holding transport lock)
	for _, conn := range conns {
		conn.Close()
	}

	return nil
}

// OpenStream opens a new bidirectional stream.
func (c *mockConn) OpenStream(ctx context.Context) (Stream, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, io.ErrClosedPipe
	}
	c.mu.Unlock()

	// Create two pipes for bidirectional communication
	// localStream writes -> remoteStream reads
	localToRemoteReader, localToRemoteWriter := io.Pipe()
	// remoteStream writes -> localStream reads
	remoteToLocalReader, remoteToLocalWriter := io.Pipe()

	localStream := &mockStream{
		reader: remoteToLocalReader,
		writer: localToRemoteWriter,
	}
	remoteStream := &mockStream{
		reader: localToRemoteReader,
		writer: remoteToLocalWriter,
	}

	// Send remote stream to the other side
	select {
	case c.other.streamChan <- remoteStream:
	case <-ctx.Done():
		localStream.Close()
		remoteStream.Close()
		return nil, ctx.Err()
	}

	return localStream, nil
}

// AcceptStream waits for and accepts an incoming stream.
func (c *mockConn) AcceptStream(ctx context.Context) (Stream, error) {
	select {
	case stream := <-c.streamChan:
		return stream, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Close closes the connection and all streams.
func (c *mockConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}
	c.closed = true

	// Close all pending streams
	for _, stream := range c.pendingStreams {
		stream.Close()
	}
	c.pendingStreams = nil

	// Remove from transport's connections
	c.transport.mu.Lock()
	delete(c.transport.connections, c)
	c.transport.mu.Unlock()

	return nil
}

// Read reads data from the stream.
func (s *mockStream) Read(p []byte) (n int, err error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return 0, io.ErrClosedPipe
	}
	reader := s.reader
	s.mu.Unlock()

	return reader.Read(p)
}

// Write writes data to the stream.
func (s *mockStream) Write(p []byte) (n int, err error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return 0, io.ErrClosedPipe
	}
	writer := s.writer
	s.mu.Unlock()

	return writer.Write(p)
}

// Close closes the stream.
func (s *mockStream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}
	s.closed = true

	if s.reader != nil {
		s.reader.Close()
	}
	if s.writer != nil {
		s.writer.Close()
	}

	return nil
}
