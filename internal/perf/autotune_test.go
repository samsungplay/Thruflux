package perf

import (
	"context"
	"sync"
	"testing"
	"time"
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Unix(0, 0)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Sleep(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

type rateSource struct {
	mu    sync.Mutex
	clock *fakeClock
	last  time.Time
	bytes float64
	rate  float64
}

func newRateSource(clock *fakeClock, rate float64) *rateSource {
	return &rateSource{clock: clock, last: clock.Now(), rate: rate}
}

func (r *rateSource) Bytes() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.clock.Now()
	delta := now.Sub(r.last).Seconds()
	if delta > 0 {
		r.bytes += r.rate * delta
		r.last = now
	}
	return int64(r.bytes)
}

func (r *rateSource) SetRate(rate float64) {
	r.mu.Lock()
	now := r.clock.Now()
	delta := now.Sub(r.last).Seconds()
	if delta > 0 {
		r.bytes += r.rate * delta
		r.last = now
	}
	r.rate = rate
	r.mu.Unlock()
}

func TestProbeRateEWMA(t *testing.T) {
	clock := newFakeClock()
	source := newRateSource(clock, 10*1024*1024) // 10 MiB/s
	cfg := AutoTuneConfig{
		Enabled:       true,
		ProbeDuration: 1 * time.Second,
		Warmup:        0,
		Alpha:         0.2,
		Now:           clock.Now,
		Sleep:         clock.Sleep,
	}
	score, _ := probe(context.Background(), withDefaults(cfg), source.Bytes)
	if score < 9.5*1024*1024 || score > 10.5*1024*1024 {
		t.Fatalf("expected ~10MiB/s, got %.2f", score)
	}
}

func TestChunkSweepImprovementThreshold(t *testing.T) {
	clock := newFakeClock()
	rates := map[int]float64{
		1 << 20: 90,
		2 << 20: 100,
		4 << 20: 104,
		8 << 20: 104,
	}
	source := newRateSource(clock, 100)
	apply := func(p Params) error {
		if rate, ok := rates[p.ChunkSize]; ok {
			source.SetRate(rate)
		}
		return nil
	}
	cfg := AutoTuneConfig{
		Enabled:          true,
		ProbeDuration:    200 * time.Millisecond,
		Warmup:           20 * time.Millisecond,
		Alpha:            0.2,
		ImproveThreshold: 0.05,
		Now:              clock.Now,
		Sleep:            clock.Sleep,
		MaxTime:          5 * time.Second,
	}
	initial := Params{ChunkSize: 2 << 20, Window: 16, ReadAhead: 8, ParallelFiles: 1}
	final, _ := RunAutoTune(context.Background(), cfg, source.Bytes, apply, initial, WorkloadInfo{NumFiles: 1})
	if final.ChunkSize != initial.ChunkSize {
		t.Fatalf("expected chunk size to stay at %d, got %d", initial.ChunkSize, final.ChunkSize)
	}
}

func TestClampMaxInflight(t *testing.T) {
	params := Params{ChunkSize: 8 << 20, Window: 64}
	clamped := clampWindow(params, 256*1024*1024)
	if clamped.Window != 32 {
		t.Fatalf("expected window 32 after clamp, got %d", clamped.Window)
	}
}

func TestParallelDoubling(t *testing.T) {
	clock := newFakeClock()
	source := newRateSource(clock, 100)
	apply := func(p Params) error {
		rate := 100.0
		switch p.ParallelFiles {
		case 1:
			rate = 100
		case 2:
			rate = 120
		case 4:
			rate = 130
		case 8:
			rate = 125
		}
		source.SetRate(rate)
		return nil
	}
	cfg := AutoTuneConfig{
		Enabled:          true,
		ProbeDuration:    200 * time.Millisecond,
		Warmup:           20 * time.Millisecond,
		Alpha:            0.2,
		ImproveThreshold: 0.05,
		Now:              clock.Now,
		Sleep:            clock.Sleep,
		MaxTime:          5 * time.Second,
	}
	initial := Params{ChunkSize: 4 << 20, Window: 16, ReadAhead: 8, ParallelFiles: 1}
	final, trace := RunAutoTune(context.Background(), cfg, source.Bytes, apply, initial, WorkloadInfo{NumFiles: 10})
	if final.ParallelFiles != 4 {
		t.Fatalf("expected parallel files 4, got %d (trace=%v)", final.ParallelFiles, trace)
	}
}

func TestEarlyStopAfterNoImprove(t *testing.T) {
	clock := newFakeClock()
	source := newRateSource(clock, 100)
	apply := func(p Params) error {
		source.SetRate(100)
		return nil
	}
	cfg := AutoTuneConfig{
		Enabled:          true,
		ProbeDuration:    100 * time.Millisecond,
		Warmup:           20 * time.Millisecond,
		Alpha:            0.2,
		ImproveThreshold: 0.05,
		Now:              clock.Now,
		Sleep:            clock.Sleep,
		MaxTime:          3 * time.Second,
	}
	initial := Params{ChunkSize: 4 << 20, Window: 16, ReadAhead: 8, ParallelFiles: 1}
	_, trace := RunAutoTune(context.Background(), cfg, source.Bytes, apply, initial, WorkloadInfo{NumFiles: 1})
	if len(trace) > 8 {
		t.Fatalf("expected early stop, trace entries=%d trace=%v", len(trace), trace)
	}
}

func TestStopWhenReceiverReady(t *testing.T) {
	clock := newFakeClock()
	source := newRateSource(clock, 100)
	apply := func(p Params) error {
		source.SetRate(100)
		return nil
	}
	cfg := AutoTuneConfig{
		Enabled:          true,
		ProbeDuration:    200 * time.Millisecond,
		Warmup:           20 * time.Millisecond,
		Alpha:            0.2,
		ImproveThreshold: 0.05,
		Now:              clock.Now,
		Sleep:            clock.Sleep,
		MaxTime:          2 * time.Second,
		StopFn:           func() bool { return true },
	}
	initial := Params{ChunkSize: 4 << 20, Window: 16, ReadAhead: 8, ParallelFiles: 1}
	final, trace := RunAutoTune(context.Background(), cfg, source.Bytes, apply, initial, WorkloadInfo{NumFiles: 1})
	if final.ChunkSize != initial.ChunkSize || len(trace) != 1 {
		t.Fatalf("expected immediate stop with baseline only, trace=%d", len(trace))
	}
}
