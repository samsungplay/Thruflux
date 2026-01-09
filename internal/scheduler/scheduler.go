package scheduler

import "time"

// FileKey identifies a file by stream and path.
type FileKey struct {
	StreamID uint64
	RelPath  string
}

// FileMeta tracks scheduling metadata for a file.
type FileMeta struct {
	RelPath         string
	Size            int64
	Remaining       int64
	AddedAt         time.Time
	StartedAt       time.Time
	LastScheduledAt time.Time
	Class           string // "small" | "medium" | "large"
}

// PolicyConfig configures the hybrid scheduler policy.
type PolicyConfig struct {
	ParallelFiles   int
	SmallThreshold  int64
	MediumThreshold int64
	SmallSlotFrac   float64
	AgingAfter      time.Duration
}

// Scheduler selects files to schedule next.
type Scheduler interface {
	Add(file FileKey, meta FileMeta)
	UpdateRemaining(file FileKey, remaining int64)
	Remove(file FileKey)
	Next(now time.Time) (FileKey, bool)
	Snapshot() any
}
