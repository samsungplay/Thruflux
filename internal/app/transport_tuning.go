package app

import (
	"fmt"
	"strings"

	"github.com/sheerbytes/sheerbytes/internal/transport"
)

func formatTransportSummary(udp transport.UdpTuneResult, quic transport.QuicTuneResult) string {
	return fmt.Sprintf("Transport: dpbuf=%s quicwin=%s", normalizeStatus(udp.Status), normalizeStatus(quic.Status))
}

func formatUDPTuneLine(result transport.UdpTuneResult) string {
	appliedR := formatAppliedBytes(result.AppliedR)
	appliedW := formatAppliedBytes(result.AppliedW)
	line := fmt.Sprintf(
		"DP buffers: requested r=%s w=%s -> applied r=%s w=%s status=%s",
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

func mergeUDPTuneResults(results []transport.UdpTuneResult) transport.UdpTuneResult {
	if len(results) == 0 {
		return transport.UdpTuneResult{
			Status:   transport.StatusNA,
			Err:      "no udp sockets",
			AppliedR: -1,
			AppliedW: -1,
		}
	}

	merged := results[0]
	merged.Status = transport.StatusOK
	merged.Err = ""
	merged.AppliedR = results[0].AppliedR
	merged.AppliedW = results[0].AppliedW

	var errs []string
	okCount := 0
	for _, res := range results {
		if res.RequestedR > 0 {
			merged.RequestedR = res.RequestedR
		}
		if res.RequestedW > 0 {
			merged.RequestedW = res.RequestedW
		}
		if res.AppliedR >= 0 && (merged.AppliedR < 0 || res.AppliedR < merged.AppliedR) {
			merged.AppliedR = res.AppliedR
		}
		if res.AppliedW >= 0 && (merged.AppliedW < 0 || res.AppliedW < merged.AppliedW) {
			merged.AppliedW = res.AppliedW
		}
		if res.Status == transport.StatusDenied {
			merged.Status = transport.StatusDenied
		} else if res.Status == transport.StatusOK {
			okCount++
		}
		if res.Err != "" {
			errs = append(errs, res.Err)
		}
	}

	if okCount == 0 && merged.Status != transport.StatusDenied {
		merged.Status = transport.StatusNA
	}
	if len(errs) > 0 {
		merged.Err = strings.Join(errs, "; ")
	}

	return merged
}
