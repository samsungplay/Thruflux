package app

import (
	"fmt"
	"sync"
	"time"
)

type progressTicker struct {
	role       string
	peerID     string
	totalBytes int64
	perFile    map[string]int64
	mu         sync.Mutex
	ticker     *time.Ticker
	done       chan struct{}
	once       sync.Once
	lastBytes  int64
	lastTime   time.Time
	active     int
	completed  int
}

func newProgressTicker(role, peerID string, totalBytes int64) *progressTicker {
	pt := &progressTicker{
		role:       role,
		peerID:     peerID,
		totalBytes: totalBytes,
		perFile:    make(map[string]int64),
		ticker:     time.NewTicker(1 * time.Second),
		done:       make(chan struct{}),
		lastTime:   time.Now(),
	}
	go pt.run()
	return pt
}

func (p *progressTicker) run() {
	defer p.ticker.Stop()
	for {
		select {
		case <-p.ticker.C:
			p.mu.Lock()
			var total int64
			for _, v := range p.perFile {
				total += v
			}
			active := p.active
			completed := p.completed
			p.mu.Unlock()

			now := time.Now()
			elapsed := now.Sub(p.lastTime).Seconds()
			mbps := 0.0
			if elapsed > 0 {
				mbps = float64(total-p.lastBytes) / (1024 * 1024) / elapsed
			}
			p.lastBytes = total
			p.lastTime = now

			remaining := p.totalBytes - total
			if remaining < 0 {
				remaining = 0
			}
			pct := 0.0
			if p.totalBytes > 0 {
				pct = float64(total) / float64(p.totalBytes) * 100
			}
			fmt.Printf("progress %s peer=%s active=%d done=%d remaining_bytes=%d percent=%.2f mbps=%.2f\n", p.role, p.peerID, active, completed, remaining, pct, mbps)
		case <-p.done:
			return
		}
	}
}

func (p *progressTicker) Update(relpath string, bytes int64) {
	p.mu.Lock()
	p.perFile[relpath] = bytes
	p.mu.Unlock()
}

func (p *progressTicker) UpdateStats(active, completed int) {
	p.mu.Lock()
	p.active = active
	p.completed = completed
	p.mu.Unlock()
}

func (p *progressTicker) Stop() {
	p.once.Do(func() {
		close(p.done)
	})
}
