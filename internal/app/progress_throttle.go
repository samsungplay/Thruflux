package app

import (
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
