package bench

import "time"

type Bench struct {
	start     time.Time
	last      time.Time
	lastBytes int64
	baseBytes int64
	ewma      float64
	peak      float64
	ttfbMs    int64
	gotTTFB   bool
}

type Snapshot struct {
	Bytes    int64
	Total    int64
	Elapsed  time.Duration
	InstMBps float64
	EwmaMBps float64
	AvgMBps  float64
	PeakMBps float64
	ETA      time.Duration
	TTFBMs   int64
	GotTTFB  bool
}

type Summary struct {
	Bytes    int64
	Total    int64
	Elapsed  time.Duration
	AvgMBps  float64
	PeakMBps float64
	TTFBMs   int64
	GotTTFB  bool
}

func NewBench() *Bench {
	return &Bench{}
}

func (b *Bench) Tick(now time.Time, bytesNow, totalBytes int64) Snapshot {
	if b.start.IsZero() {
		b.start = now
		b.last = now
		b.lastBytes = bytesNow
		b.baseBytes = bytesNow
		return Snapshot{
			Bytes:    bytesNow,
			Total:    totalBytes,
			Elapsed:  0,
			InstMBps: 0,
			EwmaMBps: 0,
			AvgMBps:  0,
			PeakMBps: 0,
			ETA:      0,
			TTFBMs:   0,
			GotTTFB:  false,
		}
	}
	elapsed := now.Sub(b.start)
	if elapsed <= 0 {
		elapsed = time.Millisecond
	}
	delta := bytesNow - b.lastBytes
	if delta < 0 {
		delta = 0
	}
	dt := now.Sub(b.last)
	if dt <= 0 {
		dt = time.Second
	}
	inst := float64(delta) / dt.Seconds() / (1024 * 1024)
	if b.ewma == 0 {
		b.ewma = inst
	} else {
		b.ewma = 0.2*inst + 0.8*b.ewma
	}
	if inst > b.peak {
		b.peak = inst
	}
	if !b.gotTTFB && bytesNow > 0 {
		b.gotTTFB = true
		b.ttfbMs = now.Sub(b.start).Milliseconds()
	}
	base := bytesNow - b.baseBytes
	if base < 0 {
		base = 0
	}
	avg := float64(base) / elapsed.Seconds() / (1024 * 1024)
	eta := time.Duration(0)
	if totalBytes > 0 && b.ewma > 0 {
		remaining := totalBytes - bytesNow
		if remaining < 0 {
			remaining = 0
		}
		etaSeconds := float64(remaining) / (b.ewma * 1024 * 1024)
		eta = time.Duration(etaSeconds * float64(time.Second))
	}

	b.last = now
	b.lastBytes = bytesNow

	return Snapshot{
		Bytes:    bytesNow,
		Total:    totalBytes,
		Elapsed:  elapsed,
		InstMBps: inst,
		EwmaMBps: b.ewma,
		AvgMBps:  avg,
		PeakMBps: b.peak,
		ETA:      eta,
		TTFBMs:   b.ttfbMs,
		GotTTFB:  b.gotTTFB,
	}
}

func (b *Bench) Final(now time.Time, bytesNow, totalBytes int64) Summary {
	snap := b.Tick(now, bytesNow, totalBytes)
	return Summary{
		Bytes:    snap.Bytes,
		Total:    snap.Total,
		Elapsed:  snap.Elapsed,
		AvgMBps:  snap.AvgMBps,
		PeakMBps: snap.PeakMBps,
		TTFBMs:   snap.TTFBMs,
		GotTTFB:  snap.GotTTFB,
	}
}
