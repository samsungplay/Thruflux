package bench

import "testing"

func TestAggregate(t *testing.T) {
	snaps := map[string]Snapshot{
		"a": {InstMBps: 10, AvgMBps: 11, PeakMBps: 12},
		"b": {InstMBps: 5, AvgMBps: 6, PeakMBps: 7},
	}
	agg := Aggregate(snaps)
	if agg.InstMBps != 15 || agg.AvgMBps != 17 || agg.PeakMBps != 19 {
		t.Fatalf("unexpected aggregate %+v", agg)
	}
}
