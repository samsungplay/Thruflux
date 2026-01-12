package progress

import (
	"sync"
	"time"
)

// Stats represents a point-in-time snapshot of progress.
type Stats struct {
	BytesDone int64
	Total     int64
	RateBps   float64
	ETA       time.Duration
	Percent   float64
	StartedAt time.Time
}

// Meter tracks byte progress and computes a smoothed rate.
type Meter struct {
	mu        sync.Mutex
	total     int64
	done      int64
	startedAt time.Time
	lastAt    time.Time
	lastDone  int64
	rateBps   float64
	alpha     float64
	now       func() time.Time
}

// NewMeter returns a meter with a default smoothing factor.
func NewMeter() *Meter {
	return NewMeterWithNow(time.Now)
}

// NewMeterWithNow returns a meter with a custom time source (for tests).
func NewMeterWithNow(now func() time.Time) *Meter {
	if now == nil {
		now = time.Now
	}
	return &Meter{alpha: 0.2, now: now}
}

// Start initializes the meter with a total size.
func (m *Meter) Start(totalBytes int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.now == nil {
		m.now = time.Now
	}
	m.total = totalBytes
	m.done = 0
	m.startedAt = m.now()
	m.lastAt = m.startedAt
	m.lastDone = 0
	m.rateBps = 0
}

// Add increments the completed byte count.
func (m *Meter) Add(n int) {
	if n <= 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.now == nil {
		m.now = time.Now
	}
	now := m.now()
	m.done += int64(n)
	deltaBytes := m.done - m.lastDone
	deltaTime := now.Sub(m.lastAt).Seconds()
	if deltaTime > 0 {
		inst := float64(deltaBytes) / deltaTime
		if m.rateBps == 0 {
			m.rateBps = inst
		} else {
			m.rateBps = m.alpha*inst + (1-m.alpha)*m.rateBps
		}
		m.lastAt = now
		m.lastDone = m.done
	}
}

// Advance increments the completed byte count without affecting rate.
func (m *Meter) Advance(n int) {
	if n <= 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.done += int64(n)
	m.lastDone += int64(n)
}

// SetTotal updates the total bytes.
func (m *Meter) SetTotal(totalBytes int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.total = totalBytes
}

// AddTotal increments the total byte count without affecting rate.
func (m *Meter) AddTotal(n int64) {
	if n <= 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.total += n
}

// Snapshot returns a current snapshot of progress stats.
func (m *Meter) Snapshot() Stats {
	m.mu.Lock()
	defer m.mu.Unlock()
	stats := Stats{
		BytesDone: m.done,
		Total:     m.total,
		RateBps:   m.rateBps,
		StartedAt: m.startedAt,
	}
	if m.total > 0 {
		stats.Percent = float64(m.done) / float64(m.total) * 100
	}
	if m.rateBps > 0 && m.total > m.done {
		remaining := float64(m.total - m.done)
		stats.ETA = time.Duration(remaining/m.rateBps) * time.Second
	}
	return stats
}
