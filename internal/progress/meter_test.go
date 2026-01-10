package progress

import (
	"testing"
	"time"
)

func TestMeterRateAndETA(t *testing.T) {
	now := time.Date(2026, 1, 10, 12, 0, 0, 0, time.UTC)
	m := NewMeterWithNow(func() time.Time { return now })
	m.Start(2000)

	now = now.Add(1 * time.Second)
	m.Add(1000)

	stats := m.Snapshot()
	if stats.BytesDone != 1000 {
		t.Fatalf("expected bytes done 1000, got %d", stats.BytesDone)
	}
	if stats.RateBps < 900 || stats.RateBps > 1100 {
		t.Fatalf("expected rate around 1000 B/s, got %.2f", stats.RateBps)
	}
	if stats.ETA < 900*time.Millisecond || stats.ETA > 1100*time.Millisecond {
		t.Fatalf("expected ETA around 1s, got %s", stats.ETA)
	}
}

func TestMeterEWMASmoothing(t *testing.T) {
	now := time.Date(2026, 1, 10, 12, 0, 0, 0, time.UTC)
	m := NewMeterWithNow(func() time.Time { return now })
	m.Start(10000)

	now = now.Add(1 * time.Second)
	m.Add(1000)

	now = now.Add(1 * time.Second)
	m.Add(3000)

	stats := m.Snapshot()
	if stats.RateBps < 1300 || stats.RateBps > 1500 {
		t.Fatalf("expected smoothed rate around 1400 B/s, got %.2f", stats.RateBps)
	}
}

func TestMeterNoRateNoETA(t *testing.T) {
	now := time.Date(2026, 1, 10, 12, 0, 0, 0, time.UTC)
	m := NewMeterWithNow(func() time.Time { return now })
	m.Start(1000)

	stats := m.Snapshot()
	if stats.RateBps != 0 {
		t.Fatalf("expected rate 0, got %.2f", stats.RateBps)
	}
	if stats.ETA != 0 {
		t.Fatalf("expected ETA 0, got %s", stats.ETA)
	}
}
