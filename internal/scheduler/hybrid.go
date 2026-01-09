package scheduler

import (
	"math"
	"sort"
	"sync"
	"time"
)

const (
	classSmall  = "small"
	classMedium = "medium"
	classLarge  = "large"
)

type HybridScheduler struct {
	mu      sync.Mutex
	cfg     PolicyConfig
	files   map[FileKey]FileMeta
	credits map[FileKey]float64
}

func NewHybridScheduler(cfg PolicyConfig) *HybridScheduler {
	if cfg.ParallelFiles < 1 {
		cfg.ParallelFiles = 1
	}
	if cfg.SmallThreshold <= 0 {
		cfg.SmallThreshold = 4 * 1024 * 1024
	}
	if cfg.MediumThreshold <= 0 {
		cfg.MediumThreshold = 64 * 1024 * 1024
	}
	if cfg.SmallSlotFrac <= 0 {
		cfg.SmallSlotFrac = 0.25
	}
	if cfg.AgingAfter <= 0 {
		cfg.AgingAfter = 5 * time.Second
	}
	return &HybridScheduler{
		cfg:     cfg,
		files:   make(map[FileKey]FileMeta),
		credits: make(map[FileKey]float64),
	}
}

func (s *HybridScheduler) Add(file FileKey, meta FileMeta) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if meta.RelPath == "" {
		meta.RelPath = file.RelPath
	}
	if meta.Remaining < 0 {
		meta.Remaining = meta.Size
	}
	meta.Class = s.classForRemaining(s.remainingForMeta(meta))
	s.files[file] = meta
	if _, ok := s.credits[file]; !ok {
		s.credits[file] = 0
	}
}

func (s *HybridScheduler) UpdateRemaining(file FileKey, remaining int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	meta, ok := s.files[file]
	if !ok {
		return
	}
	meta.Remaining = remaining
	meta.Class = s.classForRemaining(s.remainingForMeta(meta))
	s.files[file] = meta
}

func (s *HybridScheduler) Remove(file FileKey) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.files, file)
	delete(s.credits, file)
}

func (s *HybridScheduler) Next(now time.Time) (FileKey, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	smallSlots := int(math.Floor(float64(s.cfg.ParallelFiles) * s.cfg.SmallSlotFrac))
	if smallSlots < 1 {
		smallSlots = 1
	}
	if smallSlots > s.cfg.ParallelFiles {
		smallSlots = s.cfg.ParallelFiles
	}

	activeSmall, activeMedium, activeLarge := s.activeCounts()

	pendingSmall := s.pendingByClass(now, classSmall)
	if activeSmall < smallSlots && len(pendingSmall) > 0 {
		key := pickSmallestRemaining(s.files, pendingSmall)
		meta := s.files[key]
		meta.LastScheduledAt = now
		if meta.StartedAt.IsZero() {
			meta.StartedAt = now
		}
		s.files[key] = meta
		return key, true
	}

	_ = activeMedium
	_ = activeLarge

	pendingWeighted := s.pendingWeighted(now)
	if len(pendingWeighted) == 0 {
		return FileKey{}, false
	}

	for _, key := range pendingWeighted {
		weight := s.weightFor(s.remainingForMeta(s.files[key]))
		s.credits[key] += weight
	}

	bestKey := pendingWeighted[0]
	bestCredit := s.credits[bestKey]
	for _, key := range pendingWeighted[1:] {
		credit := s.credits[key]
		if credit > bestCredit || (credit == bestCredit && key.RelPath < bestKey.RelPath) {
			bestKey = key
			bestCredit = credit
		}
	}

	meta := s.files[bestKey]
	meta.LastScheduledAt = now
	if meta.StartedAt.IsZero() {
		meta.StartedAt = now
	}
	s.files[bestKey] = meta
	s.credits[bestKey] = bestCredit - 1
	return bestKey, true
}

func (s *HybridScheduler) Snapshot() any {
	s.mu.Lock()
	defer s.mu.Unlock()

	smallCount, mediumCount, largeCount := 0, 0, 0
	activeSmall, activeMedium, activeLarge := 0, 0, 0

	for _, meta := range s.files {
		switch meta.Class {
		case classSmall:
			smallCount++
			if !meta.StartedAt.IsZero() {
				activeSmall++
			}
		case classMedium:
			mediumCount++
			if !meta.StartedAt.IsZero() {
				activeMedium++
			}
		case classLarge:
			largeCount++
			if !meta.StartedAt.IsZero() {
				activeLarge++
			}
		}
	}

	candidates := make([]FileKey, 0, len(s.files))
	for key, meta := range s.files {
		if !meta.StartedAt.IsZero() {
			continue
		}
		candidates = append(candidates, key)
	}

	sort.Slice(candidates, func(i, j int) bool {
		mi := s.files[candidates[i]]
		mj := s.files[candidates[j]]
		ci := s.effectiveClass(mi, time.Now())
		cj := s.effectiveClass(mj, time.Now())
		if ci != cj {
			return classRank(ci) < classRank(cj)
		}
		ri := s.remainingForMeta(mi)
		rj := s.remainingForMeta(mj)
		if ri != rj {
			return ri < rj
		}
		return candidates[i].RelPath < candidates[j].RelPath
	})

	top := make([]string, 0, 5)
	for i := 0; i < len(candidates) && i < 5; i++ {
		meta := s.files[candidates[i]]
		top = append(top, candidates[i].RelPath+":"+meta.Class)
	}

	return map[string]any{
		"queued_small":   smallCount,
		"queued_medium":  mediumCount,
		"queued_large":   largeCount,
		"active_small":   activeSmall,
		"active_medium":  activeMedium,
		"active_large":   activeLarge,
		"top_candidates": top,
	}
}

func (s *HybridScheduler) classForRemaining(remaining int64) string {
	if remaining <= s.cfg.SmallThreshold {
		return classSmall
	}
	if remaining <= s.cfg.MediumThreshold {
		return classMedium
	}
	return classLarge
}

func (s *HybridScheduler) effectiveClass(meta FileMeta, now time.Time) string {
	class := s.classForRemaining(s.remainingForMeta(meta))
	if !meta.LastScheduledAt.IsZero() && now.Sub(meta.LastScheduledAt) > s.cfg.AgingAfter {
		switch class {
		case classLarge:
			return classMedium
		case classMedium:
			return classSmall
		}
	}
	return class
}

func (s *HybridScheduler) pendingByClass(now time.Time, class string) []FileKey {
	keys := make([]FileKey, 0)
	for key, meta := range s.files {
		if !meta.StartedAt.IsZero() {
			continue
		}
		if s.effectiveClass(meta, now) == class {
			keys = append(keys, key)
		}
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].RelPath < keys[j].RelPath })
	return keys
}

func (s *HybridScheduler) pendingWeighted(now time.Time) []FileKey {
	keys := make([]FileKey, 0)
	for key, meta := range s.files {
		if !meta.StartedAt.IsZero() {
			continue
		}
		eff := s.effectiveClass(meta, now)
		if eff == classMedium || eff == classLarge {
			keys = append(keys, key)
		}
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].RelPath < keys[j].RelPath })
	return keys
}

func (s *HybridScheduler) activeCounts() (int, int, int) {
	activeSmall, activeMedium, activeLarge := 0, 0, 0
	for _, meta := range s.files {
		if meta.StartedAt.IsZero() {
			continue
		}
		switch meta.Class {
		case classSmall:
			activeSmall++
		case classMedium:
			activeMedium++
		case classLarge:
			activeLarge++
		}
	}
	return activeSmall, activeMedium, activeLarge
}

func (s *HybridScheduler) weightFor(remaining int64) float64 {
	if remaining < 1 {
		remaining = 1
	}
	return 1.0 / math.Sqrt(float64(remaining))
}

func pickSmallestRemaining(files map[FileKey]FileMeta, keys []FileKey) FileKey {
	best := keys[0]
	bestRemaining := remainingForFile(files[best])
	for _, key := range keys[1:] {
		remaining := remainingForFile(files[key])
		if remaining < bestRemaining || (remaining == bestRemaining && key.RelPath < best.RelPath) {
			best = key
			bestRemaining = remaining
		}
	}
	return best
}

func remainingForFile(meta FileMeta) int64 {
	if meta.Remaining > 0 {
		return meta.Remaining
	}
	if meta.Size > 0 {
		return meta.Size
	}
	return 1
}

func (s *HybridScheduler) remainingForMeta(meta FileMeta) int64 {
	return remainingForFile(meta)
}

func classRank(class string) int {
	switch class {
	case classSmall:
		return 0
	case classMedium:
		return 1
	case classLarge:
		return 2
	default:
		return 3
	}
}
