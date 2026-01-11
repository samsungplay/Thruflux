package app

import (
	"fmt"

	"github.com/sheerbytes/sheerbytes/internal/transport"
)

func formatTransportSummary(udp transport.UdpTuneResult, quic transport.QuicTuneResult) string {
	return fmt.Sprintf("Transport: udpbuf=%s quicwin=%s", normalizeStatus(udp.Status), normalizeStatus(quic.Status))
}

func formatUDPTuneLine(result transport.UdpTuneResult) string {
	appliedR := formatAppliedBytes(result.AppliedR)
	appliedW := formatAppliedBytes(result.AppliedW)
	line := fmt.Sprintf(
		"UDP buffers: requested r=%s w=%s -> applied r=%s w=%s status=%s",
		transport.FormatBytesMiB(result.RequestedR),
		transport.FormatBytesMiB(result.RequestedW),
		appliedR,
		appliedW,
		normalizeStatus(result.Status),
	)
	if result.Err != "" {
		line += " err=" + result.Err
	}
	return line
}

func formatQuicTuneLine(result transport.QuicTuneResult) string {
	line := fmt.Sprintf(
		"QUIC tuning: conn_window=%s stream_window=%s max_streams=%d status=%s",
		transport.FormatBytesMiB(result.ConnWin),
		transport.FormatBytesMiB(result.StreamWin),
		result.MaxStreams,
		normalizeStatus(result.Status),
	)
	if result.Err != "" {
		line += " err=" + result.Err
	}
	return line
}

func normalizeStatus(status string) string {
	if status == "" {
		return transport.StatusNA
	}
	return status
}

func formatAppliedBytes(n int) string {
	if n < 0 {
		return "unknown"
	}
	return transport.FormatBytesMiB(n)
}
