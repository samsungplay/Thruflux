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
	slots sync.Map
}

type progressSlot struct {
	bytes atomic.Int64
	total atomic.Int64
	dirty atomic.Bool
}

func newProgressCollector() *progressCollector {
	return &progressCollector{}
}

func (c *progressCollector) Update(relpath string, bytes int64, total int64) {
	if relpath == "" {
		return
	}
	slot := c.getSlot(relpath)
	if slot == nil {
		return
	}
	if total > 0 {
		slot.total.Store(total)
	}
	for {
		prev := slot.bytes.Load()
		if bytes <= prev {
			break
		}
		if slot.bytes.CompareAndSwap(prev, bytes) {
			break
		}
	}
	slot.dirty.Store(true)
}

func (c *progressCollector) Add(relpath string, delta int64) {
	if relpath == "" || delta == 0 {
		return
	}
	slot := c.getSlot(relpath)
	if slot == nil {
		return
	}
	slot.bytes.Add(delta)
	slot.dirty.Store(true)
}

func (c *progressCollector) Flush(apply func(relpath string, bytes int64, total int64)) {
	c.slots.Range(func(key, value any) bool {
		relpath, ok := key.(string)
		if !ok {
			return true
		}
		slot, ok := value.(*progressSlot)
		if !ok {
			return true
		}
		if !slot.dirty.Swap(false) {
			return true
		}
		apply(relpath, slot.bytes.Load(), slot.total.Load())
		return true
	})
}

func (c *progressCollector) Total(relpath string) int64 {
	if relpath == "" {
		return 0
	}
	value, ok := c.slots.Load(relpath)
	if !ok {
		return 0
	}
	slot, ok := value.(*progressSlot)
	if !ok {
		return 0
	}
	return slot.total.Load()
}

func (c *progressCollector) getSlot(relpath string) *progressSlot {
	if relpath == "" {
		return nil
	}
	if value, ok := c.slots.Load(relpath); ok {
		if slot, ok := value.(*progressSlot); ok {
			return slot
		}
	}
	slot := &progressSlot{}
	actual, _ := c.slots.LoadOrStore(relpath, slot)
	if stored, ok := actual.(*progressSlot); ok {
		return stored
	}
	return slot
}

func startProgressTicker(ctx context.Context, collector *progressCollector, apply func(relpath string, bytes int64, total int64)) func() {
	return startProgressTickerWithInterval(ctx, collector, progressUpdateInterval, apply)
}

func startProgressTickerWithInterval(ctx context.Context, collector *progressCollector, interval time.Duration, apply func(relpath string, bytes int64, total int64)) func() {
	if collector == nil {
		return func() {}
	}
	if interval <= 0 {
		interval = progressUpdateInterval
	}
	ticker := time.NewTicker(interval)
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
