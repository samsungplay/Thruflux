package transferwebrtc

import (
	"io"
	"testing"

	"github.com/sheerbytes/sheerbytes/internal/transfer"
)

// TestWebRTCStreamInterfaceCompliance ensures WebRTCStream implements all required interfaces.
func TestWebRTCStreamInterfaceCompliance(t *testing.T) {
	// Compile-time assertions
	var _ transfer.Stream = (*WebRTCStream)(nil)
	var _ io.Reader = (*WebRTCStream)(nil)
	var _ io.Writer = (*WebRTCStream)(nil)
	var _ transfer.StreamIDer = (*WebRTCStream)(nil)
}

// TestWebRTCConnInterfaceCompliance ensures WebRTCConn implements the Conn interface.
func TestWebRTCConnInterfaceCompliance(t *testing.T) {
	var _ transfer.Conn = (*WebRTCConn)(nil)
}

// TestWebRTCTransportInterfaceCompliance ensures WebRTCTransport implements the Transport interface.
func TestWebRTCTransportInterfaceCompliance(t *testing.T) {
	var _ transfer.Transport = (*WebRTCTransport)(nil)
}

// TestDefaultConfig ensures default config has sensible values.
func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.Ordered {
		t.Error("expected Ordered to be false by default for parallel performance")
	}
	if cfg.MaxChannels != 100 {
		t.Errorf("expected MaxChannels=100, got %d", cfg.MaxChannels)
	}
}
