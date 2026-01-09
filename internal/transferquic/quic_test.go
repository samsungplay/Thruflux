package transferquic

import (
	"io"
	"testing"

	"github.com/sheerbytes/sheerbytes/internal/transfer"
)

// TestQUICStreamInterfaceCompliance ensures QUICStream implements all required interfaces.
func TestQUICStreamInterfaceCompliance(t *testing.T) {
	// Compile-time assertions
	var _ transfer.Stream = (*QUICStream)(nil)
	var _ io.Reader = (*QUICStream)(nil)
	var _ io.Writer = (*QUICStream)(nil)
}

// TestQUICConnInterfaceCompliance ensures QUICConn implements the Conn interface.
func TestQUICConnInterfaceCompliance(t *testing.T) {
	var _ transfer.Conn = (*QUICConn)(nil)
}

// TestQUICTransportInterfaceCompliance ensures QUICTransport implements the Transport interface.
func TestQUICTransportInterfaceCompliance(t *testing.T) {
	var _ transfer.Transport = (*QUICTransport)(nil)
}

