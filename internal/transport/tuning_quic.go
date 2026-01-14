package transport

import "github.com/quic-go/quic-go"

const (
	defaultInitialConnWindow = 2 * 1024 * 1024
	minQuicConnWindow        = 1 * 1024 * 1024
	maxQuicConnWindow        = 1024 * 1024 * 1024
	minQuicStreamWindow      = 1 * 1024 * 1024
	maxQuicStreamWindow      = 256 * 1024 * 1024
	minQuicMaxStreams        = 1
	maxQuicMaxStreams        = 2048
)

type QuicTuneResult struct {
	ConnWin    int
	StreamWin  int
	MaxStreams int
	Status     string
	Err        string
}

func BuildQuicConfig(base *quic.Config, connWin, streamWin, maxStreams int) (*quic.Config, QuicTuneResult) {
	cfg := &quic.Config{}
	if base != nil {
		copyCfg := *base
		cfg = &copyCfg
	}

	conn := clampQuicConnWindow(connWin)
	stream := clampQuicStreamWindow(streamWin)
	maxStr := clampQuicMaxStreams(maxStreams)
	initialConn := defaultInitialConnWindow
	if initialConn > conn {
		initialConn = conn
	}
	cfg.InitialConnectionReceiveWindow = uint64(initialConn)
	cfg.MaxConnectionReceiveWindow = uint64(conn)
	cfg.InitialStreamReceiveWindow = uint64(stream)
	cfg.MaxStreamReceiveWindow = uint64(stream)
	cfg.MaxIncomingStreams = int64(maxStr)

	return cfg, QuicTuneResult{
		ConnWin:    conn,
		StreamWin:  stream,
		MaxStreams: maxStr,
		Status:     StatusOK,
	}
}

func clampQuicConnWindow(n int) int {
	if n < minQuicConnWindow {
		return minQuicConnWindow
	}
	if n > maxQuicConnWindow {
		return maxQuicConnWindow
	}
	return n
}

func clampQuicStreamWindow(n int) int {
	if n < minQuicStreamWindow {
		return minQuicStreamWindow
	}
	if n > maxQuicStreamWindow {
		return maxQuicStreamWindow
	}
	return n
}

func clampQuicMaxStreams(n int) int {
	if n < minQuicMaxStreams {
		return minQuicMaxStreams
	}
	if n > maxQuicMaxStreams {
		return maxQuicMaxStreams
	}
	return n
}
