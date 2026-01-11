package transport

import (
	"testing"
	"time"

	"github.com/quic-go/quic-go"
)

func TestBuildQuicConfigClampsAndCopies(t *testing.T) {
	base := &quic.Config{
		KeepAlivePeriod: 30 * time.Second,
	}
	cfg, res := BuildQuicConfig(base, maxQuicConnWindow+1, maxQuicStreamWindow+1, maxQuicMaxStreams+1)
	if res.ConnWin != maxQuicConnWindow {
		t.Fatalf("expected conn window clamp, got %d", res.ConnWin)
	}
	if res.StreamWin != maxQuicStreamWindow {
		t.Fatalf("expected stream window clamp, got %d", res.StreamWin)
	}
	if res.MaxStreams != maxQuicMaxStreams {
		t.Fatalf("expected max streams clamp, got %d", res.MaxStreams)
	}
	if cfg.InitialConnectionReceiveWindow != uint64(maxQuicConnWindow) {
		t.Fatalf("unexpected conn window in config")
	}
	if cfg.MaxStreamReceiveWindow != uint64(maxQuicStreamWindow) {
		t.Fatalf("unexpected stream window in config")
	}
	if cfg.MaxIncomingStreams != int64(maxQuicMaxStreams) {
		t.Fatalf("unexpected max streams in config")
	}
	if cfg.KeepAlivePeriod != base.KeepAlivePeriod {
		t.Fatalf("expected keepalive preserved from base")
	}
	if base.InitialConnectionReceiveWindow != 0 {
		t.Fatalf("expected base config untouched")
	}
}
