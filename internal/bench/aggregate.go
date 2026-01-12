package bench

type AggregateSnapshot struct {
	InstMBps float64
	AvgMBps  float64
	PeakMBps float64
}

func Aggregate(snaps map[string]Snapshot) AggregateSnapshot {
	var agg AggregateSnapshot
	for _, snap := range snaps {
		agg.InstMBps += snap.InstMBps
		agg.AvgMBps += snap.AvgMBps
		agg.PeakMBps += snap.PeakMBps
	}
	return agg
}
