package scheduler

import (
	"testing"
	"time"
)

func TestSmallFilesPreferred(t *testing.T) {
	s := NewHybridScheduler(PolicyConfig{ParallelFiles: 4})
	now := time.Now()

	s.Add(FileKey{RelPath: "big.bin"}, FileMeta{RelPath: "big.bin", Size: 10 << 20, Remaining: 10 << 20, AddedAt: now})
	s.Add(FileKey{RelPath: "small.txt"}, FileMeta{RelPath: "small.txt", Size: 1 << 20, Remaining: 1 << 20, AddedAt: now})

	key, ok := s.Next(now)
	if !ok {
		t.Fatalf("expected candidate")
	}
	if key.RelPath != "small.txt" {
		t.Fatalf("expected small file first, got %s", key.RelPath)
	}
}

func TestAgingPreventsStarvation(t *testing.T) {
	s := NewHybridScheduler(PolicyConfig{ParallelFiles: 2, AgingAfter: 2 * time.Second})
	now := time.Now()

	mediumOld := FileMeta{RelPath: "mid-old", Size: 10 << 20, Remaining: 10 << 20, LastScheduledAt: now.Add(-3 * time.Second)}
	mediumNew := FileMeta{RelPath: "mid-new", Size: 10 << 20, Remaining: 10 << 20, LastScheduledAt: now}

	s.Add(FileKey{RelPath: "mid-old"}, mediumOld)
	s.Add(FileKey{RelPath: "mid-new"}, mediumNew)

	key, ok := s.Next(now)
	if !ok {
		t.Fatalf("expected candidate")
	}
	if key.RelPath != "mid-old" {
		t.Fatalf("expected aged file first, got %s", key.RelPath)
	}
}

func TestDeterministicTieBreak(t *testing.T) {
	s := NewHybridScheduler(PolicyConfig{ParallelFiles: 1})
	now := time.Now()

	s.Add(FileKey{RelPath: "b.txt"}, FileMeta{RelPath: "b.txt", Size: 1 << 20, Remaining: 1 << 20, AddedAt: now})
	s.Add(FileKey{RelPath: "a.txt"}, FileMeta{RelPath: "a.txt", Size: 1 << 20, Remaining: 1 << 20, AddedAt: now})

	key, ok := s.Next(now)
	if !ok {
		t.Fatalf("expected candidate")
	}
	if key.RelPath != "a.txt" {
		t.Fatalf("expected lexicographic tie-break, got %s", key.RelPath)
	}
}

func TestWeightedFairPreference(t *testing.T) {
	s := NewHybridScheduler(PolicyConfig{ParallelFiles: 2})
	now := time.Now()

	smallish := FileMeta{RelPath: "smallish", Size: 1 << 20, Remaining: 1 << 20, AddedAt: now}
	large := FileMeta{RelPath: "large", Size: 1 << 40, Remaining: 1 << 40, AddedAt: now}

	s.Add(FileKey{RelPath: "smallish"}, smallish)
	s.Add(FileKey{RelPath: "large"}, large)

	counts := map[string]int{}
	for i := 0; i < 10; i++ {
		key, ok := s.Next(now.Add(time.Duration(i) * time.Millisecond))
		if !ok {
			t.Fatalf("expected candidate")
		}
		counts[key.RelPath]++
		meta := s.files[key]
		meta.StartedAt = time.Time{}
		meta.LastScheduledAt = now
		s.files[key] = meta
	}

	if counts["smallish"] <= counts["large"] {
		t.Fatalf("expected smaller remaining to be favored, counts=%v", counts)
	}
}

func TestZeroSizeFileScheduled(t *testing.T) {
	s := NewHybridScheduler(PolicyConfig{ParallelFiles: 1})
	now := time.Now()

	s.Add(FileKey{RelPath: "zero"}, FileMeta{RelPath: "zero", Size: 0, Remaining: 0, AddedAt: now})

	key, ok := s.Next(now)
	if !ok {
		t.Fatalf("expected zero-size file to be schedulable")
	}
	if key.RelPath != "zero" {
		t.Fatalf("expected zero-size file, got %s", key.RelPath)
	}
}
