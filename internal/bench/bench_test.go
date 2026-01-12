package bench

import (
	"testing"
	"time"
)

func TestBenchEWMAAndPeak(t *testing.T) {
	b := NewBench()
	start := time.Unix(0, 0)

	snap := b.Tick(start, 0, 100)
	if snap.InstMBps != 0 {
		t.Fatalf("expected zero inst, got %.2f", snap.InstMBps)
	}

	snap = b.Tick(start.Add(1*time.Second), 100*1024*1024, 200*1024*1024)
	if snap.InstMBps < 99 || snap.InstMBps > 101 {
		t.Fatalf("unexpected inst %.2f", snap.InstMBps)
	}
	if snap.PeakMBps < snap.InstMBps {
		t.Fatalf("peak should be >= inst")
	}

	prevEWMA := snap.EwmaMBps
	snap = b.Tick(start.Add(2*time.Second), 200*1024*1024, 200*1024*1024)
	if snap.EwmaMBps <= 0 || snap.EwmaMBps > prevEWMA {
		t.Fatalf("unexpected ewma %.2f", snap.EwmaMBps)
	}
}

func TestBenchTTFBFreeze(t *testing.T) {
	b := NewBench()
	start := time.Unix(0, 0)
	_ = b.Tick(start, 0, 100)
	snap := b.Tick(start.Add(1500*time.Millisecond), 1, 100)
	if !snap.GotTTFB || snap.TTFBMs != 1500 {
		t.Fatalf("expected ttfb 1500, got %d", snap.TTFBMs)
	}
	snap = b.Tick(start.Add(2500*time.Millisecond), 2, 100)
	if snap.TTFBMs != 1500 {
		t.Fatalf("ttfb should freeze, got %d", snap.TTFBMs)
	}
}

func TestBenchETA(t *testing.T) {
	b := NewBench()
	start := time.Unix(0, 0)
	_ = b.Tick(start, 0, 100*1024*1024)
	snap := b.Tick(start.Add(time.Second), 50*1024*1024, 100*1024*1024)
	if snap.ETA <= 0 {
		t.Fatalf("expected ETA > 0, got %s", snap.ETA)
	}
}
