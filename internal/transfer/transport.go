package transfer

import (
	"context"
	"io"
	"net"
)

// Transport represents an established peer-to-peer path.
// It provides the ability to establish connections with other peers.
// A Transport is typically created once per peer and can handle
// multiple concurrent connections.
type Transport interface {
	// Dial establishes a connection to the specified peer.
	// Returns a Conn that can be used to open streams.
	Dial(ctx context.Context, peerID string) (Conn, error)

	// Accept waits for and accepts an incoming connection from another peer.
	// Returns a Conn that can be used to accept streams.
	Accept(ctx context.Context) (Conn, error)

	// Close closes the transport and all associated connections.
	Close() error
}

// Conn represents a connection between two peers.
// It provides the ability to open and accept bidirectional streams.
// A Conn can handle multiple concurrent streams.
type Conn interface {
	// OpenStream opens a new bidirectional stream to the remote peer.
	// Returns a Stream that can be used for reading and writing.
	OpenStream(ctx context.Context) (Stream, error)

	// AcceptStream waits for and accepts an incoming stream from the remote peer.
	// Returns a Stream that can be used for reading and writing.
	AcceptStream(ctx context.Context) (Stream, error)

	// RemoteAddr returns the remote network address.
	RemoteAddr() net.Addr

	// Close closes the connection and all associated streams.
	Close() error
}

// Stream is a bidirectional byte stream between two peers.
// It implements io.Reader and io.Writer for reading from and writing to the stream.
// Streams are independent and can be used concurrently.
type Stream interface {
	io.Reader
	io.Writer
	// Close closes the stream. After Close is called, Read and Write operations
	// will return errors.
	Close() error
}

// StreamIDer exposes a transport-specific stream ID when available.
// It is optional and used by multi-stream protocols.
type StreamIDer interface {
	StreamID() uint64
}
