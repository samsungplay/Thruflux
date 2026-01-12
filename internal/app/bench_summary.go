package app

import (
	"fmt"
	"strings"
	"time"

	"github.com/sheerbytes/sheerbytes/internal/bench"
)

func routeNetwork(route string) string {
	route = strings.ToLower(route)
	switch {
	case strings.Contains(route, "udp4"):
		return "udp4"
	case strings.Contains(route, "udp6"):
		return "udp6"
	case strings.Contains(route, "tcp"):
		return "tcp"
	default:
		return "unknown"
	}
}

func benchSummaryLine(label string, summary bench.Summary, route string, tuned string) string {
	ttfb := "--"
	if summary.GotTTFB {
		ttfb = fmt.Sprintf("%dms", summary.TTFBMs)
	}
	return fmt.Sprintf("BENCH %s: avg=%.0fMB/s peak1s=%.0fMB/s ttfb=%s route=%s tuned=%s",
		label,
		summary.AvgMBps,
		summary.PeakMBps,
		ttfb,
		route,
		tuned,
	)
}

func benchSummaryLineReceiver(summary bench.Summary, route string, tuned string) string {
	return fmt.Sprintf("BENCH RECV: avg=%.0fMB/s peak1s=%.0fMB/s route=%s tuned=%s",
		summary.AvgMBps,
		summary.PeakMBps,
		route,
		tuned,
	)
}

func benchTotalLine(receivers int, bytes int64, duration time.Duration, avg float64, peak float64, route string, tuned string) string {
	return fmt.Sprintf(
		"BENCH TOTAL:\n  receivers=%d\n  bytes=%.2fGiB\n  dur=%.1fs\n  avg=%.0fMB/s\n  peak1s=%.0fMB/s\n  route=%s\n  tuned=%s",
		receivers,
		float64(bytes)/(1024*1024*1024),
		duration.Seconds(),
		avg,
		peak,
		route,
		tuned,
	)
}
