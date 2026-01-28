package app

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

const progressUpdateInterval = 250 * time.Millisecond

func shouldUpdateProgress(last *int64) bool {
	now := time.Now().UnixNano()
	prev := atomic.LoadInt64(last)
	if now-prev < int64(progressUpdateInterval) {
		return false
	}
	return atomic.CompareAndSwapInt64(last, prev, now)
}

type progressCollector struct {
	mu     sync.Mutex
	latest map[string]int64
	totals map[string]int64
	dirty  map[string]struct{}
}

func newProgressCollector() *progressCollector {
	return &progressCollector{
		latest: make(map[string]int64),
		totals: make(map[string]int64),
		dirty:  make(map[string]struct{}),
	}
}

func (c *progressCollector) Update(relpath string, bytes int64, total int64) {
	if relpath == "" {
		return
	}
	c.mu.Lock()
	if prev, ok := c.latest[relpath]; !ok || bytes > prev {
		c.latest[relpath] = bytes
	}
	if total > 0 {
		c.totals[relpath] = total
	}
	c.dirty[relpath] = struct{}{}
	c.mu.Unlock()
}

func (c *progressCollector) Flush(apply func(relpath string, bytes int64, total int64)) {
	c.mu.Lock()
	if len(c.dirty) == 0 {
		c.mu.Unlock()
		return
	}
	keys := make([]string, 0, len(c.dirty))
	for k := range c.dirty {
		keys = append(keys, k)
	}
	c.dirty = make(map[string]struct{})
	bytesByKey := make([]int64, len(keys))
	totalByKey := make([]int64, len(keys))
	for i, k := range keys {
		bytesByKey[i] = c.latest[k]
		totalByKey[i] = c.totals[k]
	}
	c.mu.Unlock()

	for i, k := range keys {
		apply(k, bytesByKey[i], totalByKey[i])
	}
}

func (c *progressCollector) Force(relpath string) (int64, int64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	total := c.totals[relpath]
	if total <= 0 {
		return 0, 0, false
	}
	c.latest[relpath] = total
	c.dirty[relpath] = struct{}{}
	return total, total, true
}

func startProgressTicker(ctx context.Context, collector *progressCollector, apply func(relpath string, bytes int64, total int64)) func() {
	if collector == nil {
		return func() {}
	}
	ticker := time.NewTicker(progressUpdateInterval)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-ctx.Done():
				collector.Flush(apply)
				return
			case <-ticker.C:
				collector.Flush(apply)
			}
		}
	}()
	return func() {
		ticker.Stop()
		<-done
	}
}
