package perf

import (
	"context"
	"fmt"
	"math"
	"time"
)

type Params struct {
	ChunkSize     int
	Window        int
	ReadAhead     int
	ParallelFiles int
}

type WorkloadInfo struct {
	NumFiles   int
	TotalBytes int64
}

type AutoTuneState struct {
	Phase      string
	Trying     Params
	BestParams Params
	BestMbps   float64
	ScoreMbps  float64
	StartedAt  time.Time
	Done       bool
}

type AutoTuneConfig struct {
	Enabled          bool
	MaxTime          time.Duration
	ImproveThreshold float64
	ProbeDuration    time.Duration
	Warmup           time.Duration
	Alpha            float64
	MaxInflightBytes int64
	StopFn           func() bool
	StallReset       func()
	StallCheck       func() bool
	OnState          func(AutoTuneState)
	Now              func() time.Time
	Sleep            func(time.Duration)
	Fixed            FixedParams
}

type FixedParams struct {
	ChunkSize     bool
	Window        bool
	ReadAhead     bool
	ParallelFiles bool
}

type TraceEntry struct {
	Phase     string
	Params    Params
	ScoreMbps float64
	BestMbps  float64
}

func RunAutoTune(ctx context.Context, cfg AutoTuneConfig, measurer func() int64, apply func(Params) error, initial Params, info WorkloadInfo) (Params, []TraceEntry) {
	cfg = withDefaults(cfg)
	if !cfg.Enabled {
		return initial, nil
	}

	ctx, cancel := context.WithTimeout(ctx, cfg.MaxTime)
	defer cancel()

	trace := make([]TraceEntry, 0, 16)
	startedAt := cfg.Now()

	bestParams := clampWindow(applyFixed(initial, cfg.Fixed, initial), cfg.MaxInflightBytes)
	if err := apply(bestParams); err != nil {
		return bestParams, trace
	}

	baselineScore, _ := probe(ctx, cfg, measurer)
	bestScore := baselineScore
	peakScore := baselineScore
	emitState(cfg, AutoTuneState{
		Phase:      "baseline",
		Trying:     bestParams,
		BestParams: bestParams,
		BestMbps:   toMBps(bestScore),
		ScoreMbps:  toMBps(baselineScore),
		StartedAt:  startedAt,
	})
	trace = append(trace, TraceEntry{Phase: "baseline", Params: bestParams, ScoreMbps: toMBps(baselineScore), BestMbps: toMBps(bestScore)})

	if shouldStop(cfg) {
		emitDone(cfg, bestParams, bestScore, startedAt)
		return bestParams, trace
	}

	bestChunkParams := bestParams
	bestChunkScore := baselineScore
	if !cfg.Fixed.ChunkSize {
		chunkCandidates := []int{1 << 20, 2 << 20, 4 << 20, 8 << 20}
		for _, size := range chunkCandidates {
			if ctx.Err() != nil {
				break
			}
			candidate := bestParams
			candidate.ChunkSize = size
			candidate = clampWindow(applyFixed(candidate, cfg.Fixed, initial), cfg.MaxInflightBytes)
			if err := apply(candidate); err != nil {
				break
			}
			score, _ := probe(ctx, cfg, measurer)
			if score > peakScore {
				peakScore = score
			}
			if score > bestChunkScore {
				bestChunkScore = score
				bestChunkParams = candidate
			}
			emitState(cfg, AutoTuneState{
				Phase:      "chunk",
				Trying:     candidate,
				BestParams: bestChunkParams,
				BestMbps:   toMBps(bestChunkScore),
				ScoreMbps:  toMBps(score),
				StartedAt:  startedAt,
			})
			trace = append(trace, TraceEntry{Phase: "chunk", Params: candidate, ScoreMbps: toMBps(score), BestMbps: toMBps(bestChunkScore)})
			if shouldStop(cfg) {
				break
			}
		}
		if bestChunkScore >= baselineScore*(1+cfg.ImproveThreshold) {
			bestParams = bestChunkParams
			bestScore = bestChunkScore
			if err := apply(bestParams); err != nil {
				emitDone(cfg, bestParams, bestScore, startedAt)
				return bestParams, trace
			}
		} else {
			if err := apply(bestParams); err != nil {
				emitDone(cfg, bestParams, bestScore, startedAt)
				return bestParams, trace
			}
		}
	}

	noImprove := 0
	for iter := 0; iter < 6 && noImprove < 2; iter++ {
		if ctx.Err() != nil || shouldStop(cfg) {
			break
		}
		improved := false
		stallDetected := false

		// Step A: window increase
		if !cfg.Fixed.Window {
			windowCandidate := bestParams
			windowCandidate.Window += 16
			windowCandidate = clampWindow(applyFixed(windowCandidate, cfg.Fixed, initial), cfg.MaxInflightBytes)
			if err := apply(windowCandidate); err == nil {
				score, stall := probe(ctx, cfg, measurer)
				stallDetected = stallDetected || stall
				if score > peakScore {
					peakScore = score
				}
				if score >= bestScore*(1+cfg.ImproveThreshold) {
					bestScore = score
					bestParams = windowCandidate
					improved = true
				} else {
					_ = apply(bestParams)
				}
				emitState(cfg, AutoTuneState{
					Phase:      "window",
					Trying:     windowCandidate,
					BestParams: bestParams,
					BestMbps:   toMBps(bestScore),
					ScoreMbps:  toMBps(score),
					StartedAt:  startedAt,
				})
				trace = append(trace, TraceEntry{Phase: "window", Params: windowCandidate, ScoreMbps: toMBps(score), BestMbps: toMBps(bestScore)})
			}
		}

		if ctx.Err() != nil || shouldStop(cfg) {
			break
		}

		// Step B: parallel-files increase
		if !cfg.Fixed.ParallelFiles && shouldTryParallel(info, bestScore, peakScore) {
			parallelCandidate := bestParams
			next := parallelCandidate.ParallelFiles * 2
			if next < 1 {
				next = 1
			}
			if next > 8 {
				next = 8
			}
			if info.NumFiles > 0 && next > info.NumFiles {
				next = info.NumFiles
			}
			if next > parallelCandidate.ParallelFiles {
				parallelCandidate.ParallelFiles = next
				parallelCandidate = applyFixed(parallelCandidate, cfg.Fixed, initial)
				if err := apply(parallelCandidate); err == nil {
					score, stall := probe(ctx, cfg, measurer)
					stallDetected = stallDetected || stall
					if score > peakScore {
						peakScore = score
					}
					if score >= bestScore*(1+cfg.ImproveThreshold) {
						bestScore = score
						bestParams = parallelCandidate
						improved = true
					} else {
						_ = apply(bestParams)
					}
					emitState(cfg, AutoTuneState{
						Phase:      "parallel",
						Trying:     parallelCandidate,
						BestParams: bestParams,
						BestMbps:   toMBps(bestScore),
						ScoreMbps:  toMBps(score),
						StartedAt:  startedAt,
					})
					trace = append(trace, TraceEntry{Phase: "parallel", Params: parallelCandidate, ScoreMbps: toMBps(score), BestMbps: toMBps(bestScore)})
				}
			}
		}

		if ctx.Err() != nil || shouldStop(cfg) {
			break
		}

		// Step C: readahead adjust if stalls detected
		if stallDetected && !cfg.Fixed.ReadAhead {
			raA := minInt(32, maxInt(4, bestParams.Window/2))
			raB := minInt(32, bestParams.Window)
			bestRA := bestParams.ReadAhead
			bestRAScore := bestScore

			raCandidate := bestParams
			raCandidate.ReadAhead = raA
			if err := apply(raCandidate); err == nil {
				score, _ := probe(ctx, cfg, measurer)
				if score > peakScore {
					peakScore = score
				}
				if score > bestRAScore {
					bestRAScore = score
					bestRA = raA
				}
				emitState(cfg, AutoTuneState{
					Phase:      "readahead",
					Trying:     raCandidate,
					BestParams: bestParams,
					BestMbps:   toMBps(bestScore),
					ScoreMbps:  toMBps(score),
					StartedAt:  startedAt,
				})
				trace = append(trace, TraceEntry{Phase: "readahead", Params: raCandidate, ScoreMbps: toMBps(score), BestMbps: toMBps(bestScore)})
			}

			if ctx.Err() != nil || shouldStop(cfg) {
				break
			}

			raCandidate = bestParams
			raCandidate.ReadAhead = raB
			if err := apply(raCandidate); err == nil {
				score, _ := probe(ctx, cfg, measurer)
				if score > peakScore {
					peakScore = score
				}
				if score > bestRAScore {
					bestRAScore = score
					bestRA = raB
				}
				emitState(cfg, AutoTuneState{
					Phase:      "readahead",
					Trying:     raCandidate,
					BestParams: bestParams,
					BestMbps:   toMBps(bestScore),
					ScoreMbps:  toMBps(score),
					StartedAt:  startedAt,
				})
				trace = append(trace, TraceEntry{Phase: "readahead", Params: raCandidate, ScoreMbps: toMBps(score), BestMbps: toMBps(bestScore)})
			}

			if bestRAScore >= bestScore*(1+cfg.ImproveThreshold) {
				bestParams.ReadAhead = bestRA
				bestScore = bestRAScore
				improved = true
				_ = apply(bestParams)
			} else {
				_ = apply(bestParams)
			}
		}

		if !improved {
			noImprove++
		} else {
			noImprove = 0
		}
	}

	emitDone(cfg, bestParams, bestScore, startedAt)
	return bestParams, trace
}

func withDefaults(cfg AutoTuneConfig) AutoTuneConfig {
	if cfg.MaxTime == 0 {
		cfg.MaxTime = 10 * time.Second
	}
	if cfg.ImproveThreshold == 0 {
		cfg.ImproveThreshold = 0.05
	}
	if cfg.ProbeDuration == 0 {
		cfg.ProbeDuration = 1 * time.Second
	}
	if cfg.Warmup == 0 {
		cfg.Warmup = 200 * time.Millisecond
	}
	if cfg.Alpha == 0 {
		cfg.Alpha = 0.2
	}
	if cfg.MaxInflightBytes == 0 {
		cfg.MaxInflightBytes = 256 * 1024 * 1024
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Sleep == nil {
		cfg.Sleep = time.Sleep
	}
	return cfg
}

func probe(ctx context.Context, cfg AutoTuneConfig, measurer func() int64) (float64, bool) {
	if cfg.StallReset != nil {
		cfg.StallReset()
	}
	start := cfg.Now()
	end := start.Add(cfg.ProbeDuration)
	warmupEnd := start.Add(cfg.Warmup)
	lastAt := start
	lastBytes := measurer()
	var ewma float64
	var sum float64
	samples := 0
	sampleEvery := time.Duration(math.Min(float64(50*time.Millisecond), float64(cfg.ProbeDuration/5)))
	if sampleEvery < 10*time.Millisecond {
		sampleEvery = 10 * time.Millisecond
	}

	for cfg.Now().Before(end) {
		if ctx.Err() != nil {
			break
		}
		cfg.Sleep(sampleEvery)
		now := cfg.Now()
		bytes := measurer()
		deltaBytes := bytes - lastBytes
		deltaTime := now.Sub(lastAt).Seconds()
		if deltaTime > 0 {
			inst := float64(deltaBytes) / deltaTime
			if ewma == 0 {
				ewma = inst
			} else {
				ewma = cfg.Alpha*inst + (1-cfg.Alpha)*ewma
			}
			if now.After(warmupEnd) {
				sum += ewma
				samples++
			}
		}
		lastAt = now
		lastBytes = bytes
	}

	score := 0.0
	if samples > 0 {
		score = sum / float64(samples)
	}
	stall := false
	if cfg.StallCheck != nil {
		stall = cfg.StallCheck()
	}
	return score, stall
}

func shouldStop(cfg AutoTuneConfig) bool {
	if cfg.StopFn == nil {
		return false
	}
	return cfg.StopFn()
}

func shouldTryParallel(info WorkloadInfo, current, peak float64) bool {
	if info.NumFiles >= 2 {
		return true
	}
	if peak <= 0 {
		return false
	}
	return current < peak*0.7
}

func clampWindow(p Params, maxInflight int64) Params {
	if maxInflight <= 0 || p.ChunkSize <= 0 || p.Window <= 0 {
		return p
	}
	maxWindow := int(maxInflight / int64(p.ChunkSize))
	if maxWindow < 1 {
		maxWindow = 1
	}
	if p.Window > maxWindow {
		p.Window = maxWindow
	}
	return p
}

func emitState(cfg AutoTuneConfig, state AutoTuneState) {
	if cfg.OnState != nil {
		cfg.OnState(state)
	}
}

func emitDone(cfg AutoTuneConfig, best Params, score float64, startedAt time.Time) {
	emitState(cfg, AutoTuneState{
		Phase:      "done",
		Trying:     best,
		BestParams: best,
		BestMbps:   toMBps(score),
		ScoreMbps:  toMBps(score),
		StartedAt:  startedAt,
		Done:       true,
	})
}

func toMBps(rateBps float64) float64 {
	return rateBps / (1024 * 1024)
}

func applyFixed(candidate Params, fixed FixedParams, base Params) Params {
	if fixed.ChunkSize {
		candidate.ChunkSize = base.ChunkSize
	}
	if fixed.Window {
		candidate.Window = base.Window
	}
	if fixed.ReadAhead {
		candidate.ReadAhead = base.ReadAhead
	}
	if fixed.ParallelFiles {
		candidate.ParallelFiles = base.ParallelFiles
	}
	return candidate
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func (p Params) String() string {
	return fmt.Sprintf("chunk=%d window=%d readahead=%d parallel=%d", p.ChunkSize, p.Window, p.ReadAhead, p.ParallelFiles)
}
