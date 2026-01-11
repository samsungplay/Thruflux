package progress

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"
)

type ReceiverView struct {
	SnapshotID     string
	OutDir         string
	IceStage       string
	TransportLines []string
	Stats          Stats
	CurrentFile    string
	FileDone       int
	FileTotal      int
	FileBytes      int64
	FileTotalBytes int64
	Route          string
}

type SenderRow struct {
	Peer   string
	Status string
	Stats  Stats
	Route  string
	Stage  string
}

type SenderView struct {
	Header string
	Rows   []SenderRow
}

func IsTTY(w io.Writer) bool {
	f, ok := w.(*os.File)
	if !ok {
		return false
	}
	info, err := f.Stat()
	if err != nil {
		return false
	}
	return (info.Mode() & os.ModeCharDevice) != 0
}

func RenderReceiver(ctx context.Context, w io.Writer, view func() ReceiverView) func() {
	ticker := time.NewTicker(100 * time.Millisecond)
	stop := make(chan struct{})
	isTTY := IsTTY(w)
	lastLines := 0
	var renderMu sync.Mutex
	if !isTTY {
		ticker.Stop()
		ticker = time.NewTicker(1 * time.Second)
	} else {
		fmt.Fprint(w, "\033[?25l")
	}

	renderOnce := func() {
		renderMu.Lock()
		defer renderMu.Unlock()
		v := view()
		if isTTY {
			if lastLines > 0 {
				fmt.Fprintf(w, "\033[%dA", lastLines)
				fmt.Fprint(w, "\033[J")
			}
			lines := 0
			if v.SnapshotID != "" {
				fmt.Fprintf(w, "snapshot %s\n", v.SnapshotID)
				lines++
			}
			if v.OutDir != "" {
				fmt.Fprintf(w, "saving to %s\n", v.OutDir)
				lines++
			}
			if v.IceStage != "" {
				fmt.Fprintf(w, "ice %s\n", v.IceStage)
				lines++
			}
			if len(v.TransportLines) > 0 {
				for _, line := range v.TransportLines {
					fmt.Fprintln(w, line)
					lines++
				}
			}
			if v.Route != "" {
				fmt.Fprintf(w, "%s\n", v.Route)
				lines++
			}
			fmt.Fprintf(w, "%s\n", formatReceiverLine(v))
			lines++
			if v.CurrentFile != "" {
				fmt.Fprintf(w, "file: %s (%d/%d)\n", v.CurrentFile, v.FileDone, v.FileTotal)
				lines++
			}
			lastLines = lines
		} else {
			fmt.Fprintf(w, "snapshot=%s %s file=%s (%d/%d)\n", v.SnapshotID, formatReceiverLine(v), v.CurrentFile, v.FileDone, v.FileTotal)
		}
	}

	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-stop:
				return
			case <-ticker.C:
				renderOnce()
			}
		}
	}()

	return func() {
		close(stop)
		renderOnce()
		if isTTY {
			fmt.Fprint(w, "\033[?25h")
		}
	}
}

func RenderSender(ctx context.Context, w io.Writer, view func() SenderView) func() {
	ticker := time.NewTicker(250 * time.Millisecond)
	stop := make(chan struct{})
	isTTY := IsTTY(w)
	lastLines := 0
	var renderMu sync.Mutex
	if !isTTY {
		ticker.Stop()
		ticker = time.NewTicker(1 * time.Second)
	} else {
		fmt.Fprint(w, "\033[?25l")
	}

	renderOnce := func() {
		renderMu.Lock()
		defer renderMu.Unlock()
		v := view()
		if isTTY {
			if lastLines > 0 {
				fmt.Fprintf(w, "\033[%dA", lastLines)
				fmt.Fprint(w, "\033[J")
			}
			lines := 0
			lines += writeHeader(w, v.Header)
			fmt.Fprintln(w, "peer       status   %    rate     ETA")
			lines++
			for _, row := range v.Rows {
				fmt.Fprintf(w, "%-10s %-7s %5.1f %7s %9s\n",
					row.Peer, row.Status, row.Stats.Percent, formatRate(row.Stats.RateBps), formatETA(row.Stats.ETA))
				lines++
				if row.Stage != "" {
					fmt.Fprintf(w, "  ice %s\n", row.Stage)
					lines++
				}
				if row.Route != "" {
					fmt.Fprintf(w, "  %s\n", row.Route)
					lines++
				}
			}
			lastLines = lines
		} else {
			writeHeader(w, v.Header)
			for _, row := range v.Rows {
				fmt.Fprintf(w, "peer=%s status=%s %.1f%% %s ETA %s\n",
					row.Peer, row.Status, row.Stats.Percent, formatRate(row.Stats.RateBps), formatETA(row.Stats.ETA))
			}
		}
	}

	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-stop:
				return
			case <-ticker.C:
				renderOnce()
			}
		}
	}()

	return func() {
		close(stop)
		renderOnce()
		if isTTY {
			fmt.Fprint(w, "\033[?25h")
		}
	}
}

func writeHeader(w io.Writer, header string) int {
	header = strings.TrimSuffix(header, "\n")
	if header == "" {
		return 0
	}
	lines := strings.Split(header, "\n")
	for _, line := range lines {
		fmt.Fprintln(w, line)
	}
	return len(lines)
}

func formatReceiverLine(v ReceiverView) string {
	bar := renderBar(v.Stats.Percent, 20)
	return fmt.Sprintf("%s %5.1f%%  %s  ETA %s  (recv %s/%s)",
		bar,
		v.Stats.Percent,
		formatRate(v.Stats.RateBps),
		formatETA(v.Stats.ETA),
		formatGiB(v.Stats.BytesDone),
		formatGiB(v.Stats.Total),
	)
}

func renderBar(percent float64, width int) string {
	if percent < 0 {
		percent = 0
	}
	if percent > 100 {
		percent = 100
	}
	filled := int((percent / 100) * float64(width))
	if filled > width {
		filled = width
	}
	return "[" + strings.Repeat("#", filled) + strings.Repeat(".", width-filled) + "]"
}

func formatRate(bps float64) string {
	const (
		k = 1024
		m = 1024 * k
		g = 1024 * m
	)
	if bps >= g {
		return fmt.Sprintf("%.2f GB/s", bps/float64(g))
	}
	if bps >= m {
		return fmt.Sprintf("%.1f MB/s", bps/float64(m))
	}
	if bps >= k {
		return fmt.Sprintf("%.0f KB/s", bps/float64(k))
	}
	return fmt.Sprintf("%.0f B/s", bps)
}

func formatGiB(n int64) string {
	const g = 1024 * 1024 * 1024
	if n <= 0 {
		return "0.00 GiB"
	}
	return fmt.Sprintf("%.2f GiB", float64(n)/float64(g))
}

func formatETA(d time.Duration) string {
	if d <= 0 {
		return "--:--:--"
	}
	secs := int(d.Seconds())
	h := secs / 3600
	m := (secs % 3600) / 60
	s := secs % 60
	return fmt.Sprintf("%02d:%02d:%02d", h, m, s)
}
