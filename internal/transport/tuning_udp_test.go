package transport

import "testing"

func TestClampUDPBuffer(t *testing.T) {
	if got := clampUDPBuffer(-1); got != minUDPBuffer {
		t.Fatalf("expected clamp to min, got %d", got)
	}
	if got := clampUDPBuffer(minUDPBuffer); got != minUDPBuffer {
		t.Fatalf("expected min to stay, got %d", got)
	}
	if got := clampUDPBuffer(maxUDPBuffer + 1); got != maxUDPBuffer {
		t.Fatalf("expected clamp to max, got %d", got)
	}
}

func TestApplyUDPUnavailable(t *testing.T) {
	result := ApplyUDPBeyondBestEffort(nil, 0, 0)
	if result.Status != StatusNA {
		t.Fatalf("expected NA status, got %s", result.Status)
	}
	if result.RequestedR != minUDPBuffer || result.RequestedW != minUDPBuffer {
		t.Fatalf("expected clamped requested buffers")
	}
}
