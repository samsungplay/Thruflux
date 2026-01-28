package progress

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
)

type tickMsg struct{}
type stopMsg struct{}

type receiverTeaModel struct {
	viewFn  func() ReceiverView
	verbose bool
	view    ReceiverView
}

func (m receiverTeaModel) Init() tea.Cmd {
	return nil
}

func (m receiverTeaModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg.(type) {
	case tea.KeyMsg:
		key := msg.(tea.KeyMsg)
		if key.Type == tea.KeyCtrlC {
			os.Exit(130)
		}
	case tickMsg:
		m.view = m.viewFn()
		return m, nil
	case stopMsg:
		return m, tea.Quit
	}
	return m, nil
}

func (m receiverTeaModel) View() string {
	return renderReceiverTTY(m.view, m.verbose)
}

type senderTeaModel struct {
	viewFn  func() SenderView
	verbose bool
	view    SenderView
}

func (m senderTeaModel) Init() tea.Cmd {
	return nil
}

func (m senderTeaModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg.(type) {
	case tea.KeyMsg:
		key := msg.(tea.KeyMsg)
		if key.Type == tea.KeyCtrlC {
			os.Exit(130)
		}
	case tickMsg:
		m.view = m.viewFn()
		return m, nil
	case stopMsg:
		return m, tea.Quit
	}
	return m, nil
}

func (m senderTeaModel) View() string {
	return renderSenderTTY(m.view, m.verbose)
}

func renderReceiverTea(ctx context.Context, w io.Writer, view func() ReceiverView, verbose bool) func() {
	model := receiverTeaModel{viewFn: view, verbose: verbose, view: view()}
	program := tea.NewProgram(model, tea.WithOutput(w), tea.WithAltScreen())
	go func() {
		_, _ = program.Run()
	}()
	ticker := time.NewTicker(250 * time.Millisecond)
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-ctx.Done():
				program.Send(stopMsg{})
				return
			case <-stop:
				program.Send(stopMsg{})
				return
			case <-ticker.C:
				program.Send(tickMsg{})
			}
		}
	}()
	return func() {
		close(stop)
		ticker.Stop()
		program.Send(tickMsg{})
		program.Send(stopMsg{})
	}
}

func renderSenderTea(ctx context.Context, w io.Writer, view func() SenderView, verbose bool) func() {
	model := senderTeaModel{viewFn: view, verbose: verbose, view: view()}
	program := tea.NewProgram(model, tea.WithOutput(w), tea.WithAltScreen())
	go func() {
		_, _ = program.Run()
	}()
	ticker := time.NewTicker(250 * time.Millisecond)
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-ctx.Done():
				program.Send(stopMsg{})
				return
			case <-stop:
				program.Send(stopMsg{})
				return
			case <-ticker.C:
				program.Send(tickMsg{})
			}
		}
	}()
	return func() {
		close(stop)
		ticker.Stop()
		program.Send(tickMsg{})
		program.Send(stopMsg{})
	}
}

func renderReceiverTTY(v ReceiverView, verbose bool) string {
	var b strings.Builder
	if v.OutDir != "" {
		fmt.Fprintf(&b, "saving to %s\n", v.OutDir)
	}
	if verbose {
		if len(v.TransportLines) > 0 {
			for _, line := range v.TransportLines {
				fmt.Fprintln(&b, colorize(line, colorCyan, true))
			}
		}
		_ = renderConnSection(&b, "receiver", v.IceStage, v.Route, v.Probes, v.ConnCount, true)
	}
	fmt.Fprintf(&b, "%s\n", colorize(formatReceiverLine(v), colorGreen, true))
	currentFile := v.CurrentFile
	if currentFile == "" {
		currentFile = "-"
	}
	fmt.Fprintf(&b, "%s\n", colorize(fmt.Sprintf("file: %s (%d/%d)", currentFile, v.FileDone, v.FileTotal), colorCyan, true))
	if v.Benchmark {
		fmt.Fprintf(&b, "%s\n", colorize(formatBenchLine(v.Bench), colorCyan, true))
	}
	return strings.TrimSuffix(b.String(), "\n")
}

func renderSenderTTY(v SenderView, verbose bool) string {
	var b strings.Builder
	writeHeader(&b, v.Header, true)
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
		_ = renderTable(&b, headers, rows, widths)
		if verbose {
			for _, row := range v.Rows {
				_ = renderConnSection(&b, row.Peer, row.Stage, row.Route, row.Probes, row.ConnCount, true)
			}
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
		_ = renderTable(&b, headers, rows, widths)
		if verbose {
			for _, row := range v.Rows {
				_ = renderConnSection(&b, row.Peer, row.Stage, row.Route, row.Probes, row.ConnCount, true)
			}
		}
	}
	return strings.TrimSuffix(b.String(), "\n")
}
