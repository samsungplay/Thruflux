package progress

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/sheerbytes/sheerbytes/internal/bench"
)

type ReceiverView struct {
	SnapshotID     string
	OutDir         string
	IceStage       string
	TransportLines []string
	Stats          Stats
	Resumed        int
	Bench          bench.Snapshot
	Benchmark      bool
	CurrentFile    string
	FileDone       int
	FileTotal      int
	FileBytes      int64
	FileTotalBytes int64
	Route          string
	Probes         map[string]string
}

type SenderRow struct {
	Peer      string
	Status    string
	Stats     Stats
	Bench     bench.Snapshot
	Route     string
	Stage     string
	FileDone  int
	FileTotal int
	Resumed   int
	Probes    map[string]string
}

type SenderView struct {
	Header    string
	Rows      []SenderRow
	Benchmark bool
}

const (
	colorReset  = "\033[0m"
	colorRed    = "\033[31m"
	colorGreen  = "\033[32m"
	colorCyan   = "\033[36m"
)

func colorize(s string, color string, enabled bool) string {
	if !enabled || color == "" {
		return s
	}
	return color + s + colorReset
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
	var lastBench time.Time
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
		if !isTTY && v.Benchmark {
			if time.Since(lastBench) < 5*time.Second {
				return
			}
			lastBench = time.Now()
		}
		if isTTY {
			if lastLines > 0 {
				fmt.Fprintf(w, "\033[%dA", lastLines)
				fmt.Fprint(w, "\033[J")
			}
			lines := 0
			if v.OutDir != "" {
				fmt.Fprintf(w, "saving to %s\n", v.OutDir)
				lines++
			}
			if len(v.TransportLines) > 0 {
				for _, line := range v.TransportLines {
					fmt.Fprintln(w, colorize(line, colorCyan, isTTY))
					lines++
				}
			}
			lines += renderConnSection(w, "receiver", v.IceStage, v.Route, v.Probes, isTTY)
			fmt.Fprintf(w, "%s\n", colorize(formatReceiverLine(v), colorGreen, isTTY))
			lines++
			currentFile := v.CurrentFile
			if currentFile == "" {
				currentFile = "-"
			}
			fmt.Fprintf(w, "%s\n", colorize(fmt.Sprintf("file: %s (%d/%d)", currentFile, v.FileDone, v.FileTotal), colorCyan, isTTY))
			lines++
			if v.Benchmark {
				fmt.Fprintf(w, "%s\n", colorize(formatBenchLine(v.Bench), colorCyan, isTTY))
				lines++
			}
			lastLines = lines
		} else {
			if v.Benchmark {
				fmt.Fprintf(w, "BENCH inst=%s ewma=%s avg=%s peak=%s elapsed=%s eta=%s\n",
					formatBenchRate(v.Bench.InstMBps),
					formatBenchRate(v.Bench.EwmaMBps),
					formatBenchRate(v.Bench.AvgMBps),
					formatBenchRate(v.Bench.PeakMBps),
					formatElapsed(v.Bench.Elapsed),
					formatETA(v.Bench.ETA))
			} else {
				currentFile := v.CurrentFile
				if currentFile == "" {
					currentFile = "-"
				}
				fmt.Fprintf(w, "%s file=%s (%d/%d)\n", formatReceiverLine(v), currentFile, v.FileDone, v.FileTotal)
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

func RenderSender(ctx context.Context, w io.Writer, view func() SenderView) func() {
	ticker := time.NewTicker(250 * time.Millisecond)
	stop := make(chan struct{})
	isTTY := IsTTY(w)
	lastLines := 0
	var lastBench time.Time
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
		if !isTTY && v.Benchmark {
			if time.Since(lastBench) < 5*time.Second {
				return
			}
			lastBench = time.Now()
		}
		if isTTY {
			if lastLines > 0 {
				fmt.Fprintf(w, "\033[%dA", lastLines)
				fmt.Fprint(w, "\033[J")
			}
			lines := 0
			lines += writeHeader(w, v.Header, isTTY)
			if v.Benchmark {
				headers := []string{"peer", "status", "files", "resumed", "%", "inst", "ewma", "avg", "peak", "elapsed", "ETA"}
				widths := []int{10, 12, 9, 7, 5, 12, 12, 12, 12, 9, 9}
				rows := make([][]string, 0, len(v.Rows))
				for _, row := range v.Rows {
					rows = append(rows, []string{
						row.Peer,
						row.Status,
						formatFileCount(row.FileDone, row.FileTotal),
						formatCount(int64(row.Resumed)),
						fmt.Sprintf("%.1f", row.Stats.Percent),
						formatBenchRate2(row.Bench.InstMBps),
						formatBenchRate2(row.Bench.EwmaMBps),
						formatBenchRate2(row.Bench.AvgMBps),
						formatBenchRate2(row.Bench.PeakMBps),
						formatElapsed(row.Bench.Elapsed),
						formatETA(row.Bench.ETA),
					})
				}
				lines += renderTable(w, headers, rows, widths)
				for _, row := range v.Rows {
					lines += renderConnSection(w, row.Peer, row.Stage, row.Route, row.Probes, isTTY)
				}
			} else {
				headers := []string{"peer", "status", "files", "resumed", "%", "rate", "ETA"}
				widths := []int{10, 12, 9, 7, 5, 9, 9}
				rows := make([][]string, 0, len(v.Rows))
				for _, row := range v.Rows {
					rows = append(rows, []string{
						row.Peer,
						row.Status,
						formatFileCount(row.FileDone, row.FileTotal),
						formatCount(int64(row.Resumed)),
						fmt.Sprintf("%.1f", row.Stats.Percent),
						formatRate(row.Stats.RateBps),
						formatETA(row.Stats.ETA),
					})
				}
				lines += renderTable(w, headers, rows, widths)
				for _, row := range v.Rows {
					lines += renderConnSection(w, row.Peer, row.Stage, row.Route, row.Probes, isTTY)
				}
			}
			lastLines = lines
		} else {
			writeHeader(w, v.Header, false)
			if v.Benchmark {
				for _, row := range v.Rows {
					fmt.Fprintf(w, "BENCH %s status=%s resumed=%s inst=%s ewma=%s avg=%s peak=%s elapsed=%s eta=%s\n",
						row.Peer,
						row.Status,
						formatCount(int64(row.Resumed)),
						formatBenchRate2(row.Bench.InstMBps),
						formatBenchRate2(row.Bench.EwmaMBps),
						formatBenchRate2(row.Bench.AvgMBps),
						formatBenchRate2(row.Bench.PeakMBps),
						formatElapsed(row.Bench.Elapsed),
						formatETA(row.Bench.ETA),
					)
				}
			} else {
				for _, row := range v.Rows {
					fmt.Fprintf(w, "peer=%s status=%s resumed=%s %.1f%% %s ETA %s\n",
						row.Peer,
						row.Status,
						formatCount(int64(row.Resumed)),
						row.Stats.Percent,
						formatRate(row.Stats.RateBps),
						formatETA(row.Stats.ETA),
					)
				}
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

func writeHeader(w io.Writer, header string, isTTY bool) int {
	header = strings.TrimSuffix(header, "\n")
	if header == "" {
		return 0
	}
	lines := strings.Split(header, "\n")
	for _, line := range lines {
		fmt.Fprintln(w, colorize(line, colorCyan, isTTY))
	}
	return len(lines)
}

func formatReceiverLine(v ReceiverView) string {
	bar := renderBar(v.Stats.Percent, 20)
	return fmt.Sprintf("%s %5.1f%%  %s  resumed=%d  ETA %s  (recv %s/%s)",
		bar,
		v.Stats.Percent,
		formatRate(v.Stats.RateBps),
		v.Resumed,
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
	return "[" + strings.Repeat("█", filled) + strings.Repeat("░", width-filled) + "]"
}

func renderConnSection(w io.Writer, peerLabel string, stage string, route string, probes map[string]string, isTTY bool) int {
	lines := 0
	if stage != "" && stage != "connect_ok" {
		stageColor := colorCyan
		if strings.Contains(strings.ToUpper(stage), "FAILED") {
			stageColor = colorRed
		}
		fmt.Fprintf(w, "  [%s] conn %s\n", peerLabel, colorize(stage, stageColor, isTTY))
		lines++
	}
	if route != "" {
		fmt.Fprintf(w, "  [%s] %s\n", peerLabel, colorize(route, colorCyan, isTTY))
		lines++
	}
	lines += renderProbes(w, peerLabel, probes, isTTY)
	return lines
}

func renderTable(w io.Writer, headers []string, rows [][]string, widths []int) int {
	lines := 0
	border := buildBorder(widths)
	fmt.Fprintln(w, border)
	lines++
	fmt.Fprintln(w, buildRow(headers, widths))
	lines++
	fmt.Fprintln(w, border)
	lines++
	for _, row := range rows {
		fmt.Fprintln(w, buildRow(row, widths))
		lines++
	}
	fmt.Fprintln(w, border)
	lines++
	return lines
}

func buildBorder(widths []int) string {
	var b strings.Builder
	b.WriteString("+")
	for _, width := range widths {
		b.WriteString(strings.Repeat("-", width+2))
		b.WriteString("+")
	}
	return b.String()
}

func buildRow(values []string, widths []int) string {
	var b strings.Builder
	b.WriteString("|")
	for i, width := range widths {
		cell := ""
		if i < len(values) {
			cell = values[i]
		}
		b.WriteString(" ")
		b.WriteString(padRight(cell, width))
		b.WriteString(" |")
	}
	return b.String()
}

func padRight(s string, width int) string {
	if width <= 0 {
		return ""
	}
	if len(s) >= width {
		return s
	}
	return s + strings.Repeat(" ", width-len(s))
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

func formatBenchLine(snap bench.Snapshot) string {
	return fmt.Sprintf("Bench: inst=%.0fMB/s ewma=%.0fMB/s avg=%.0fMB/s peak1s=%.0fMB/s elapsed=%s ETA=%s",
		snap.InstMBps,
		snap.EwmaMBps,
		snap.AvgMBps,
		snap.PeakMBps,
		formatElapsed(snap.Elapsed),
		formatETA(snap.ETA),
	)
}

func formatElapsed(d time.Duration) string {
	if d <= 0 {
		return "00:00:00"
	}
	secs := int(d.Seconds())
	h := secs / 3600
	m := (secs % 3600) / 60
	s := secs % 60
	return fmt.Sprintf("%02d:%02d:%02d", h, m, s)
}

func formatBenchRate(mbps float64) string {
	return fmt.Sprintf("%.0fMB/s", mbps)
}

func formatBenchRate2(mbps float64) string {
	return fmt.Sprintf("%.2fMB/s", mbps)
}

func formatFileCount(done, total int) string {
	if total <= 0 {
		return "-"
	}
	return fmt.Sprintf("%d/%d", done, total)
}

func formatCount(n int64) string {
	if n < 0 {
		n = 0
	}
	const (
		k = 1000
		m = 1000 * k
	)
	switch {
	case n >= m:
		return fmt.Sprintf("%.1fM", float64(n)/float64(m))
	case n >= k:
		return fmt.Sprintf("%.1fk", float64(n)/float64(k))
	default:
		return fmt.Sprintf("%d", n)
	}
}

func formatAge(d time.Duration) string {
	if d <= 0 {
		return "-"
	}
	if d < time.Second {
		return "0s"
	}
	if d < time.Minute {
		secs := d.Seconds()
		if secs >= 10 {
			return fmt.Sprintf("%.0fs", secs)
		}
		return fmt.Sprintf("%.1fs", secs)
	}
	if d < time.Hour {
		mins := d.Minutes()
		if mins >= 10 {
			return fmt.Sprintf("%.0fm", mins)
		}
		return fmt.Sprintf("%.1fm", mins)
	}
	hours := d.Hours()
	if hours >= 10 {
		return fmt.Sprintf("%.0fh", hours)
	}
	return fmt.Sprintf("%.1fh", hours)
}

func renderProbes(w io.Writer, peerID string, probes map[string]string, isTTY bool) int {
	if len(probes) == 0 {
		return 0
	}
	lines := 0
	// Sort addresses for consistent UI
	addrs := make([]string, 0, len(probes))
	for addr := range probes {
		addrs = append(addrs, addr)
	}
	// Simple string sort
	for i := 0; i < len(addrs); i++ {
		for j := i + 1; j < len(addrs); j++ {
			if addrs[i] > addrs[j] {
				addrs[i], addrs[j] = addrs[j], addrs[i]
			}
		}
	}

	for _, addr := range addrs {
		status := probes[addr]
		color := ""
		switch status {
		case "probing":
			color = ""
		case "failed":
			color = colorRed
		case "won":
			color = colorGreen
		}
		statusText := status
		if color != "" {
			statusText = colorize(status, color, isTTY)
		}
		fmt.Fprintf(w, "    [%s] probe %-8s  %s\n", peerID, statusText, addr)
		lines++
	}
	return lines
}
