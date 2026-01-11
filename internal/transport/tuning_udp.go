package transport

import (
	"net"
	"strings"
)

const (
	minUDPBuffer = 256 * 1024
	maxUDPBuffer = 64 * 1024 * 1024
)

type UdpTuneResult struct {
	RequestedR int
	RequestedW int
	AppliedR   int
	AppliedW   int
	Status     string
	Err        string
}

func ApplyUDPBeyondBestEffort(conn *net.UDPConn, r, w int) UdpTuneResult {
	reqR := clampUDPBuffer(r)
	reqW := clampUDPBuffer(w)
	result := UdpTuneResult{
		RequestedR: reqR,
		RequestedW: reqW,
		AppliedR:   -1,
		AppliedW:   -1,
		Status:     StatusOK,
	}
	if conn == nil {
		result.Status = StatusNA
		result.Err = "no access to underlying UDPConn"
		return result
	}

	var errs []string
	if err := conn.SetReadBuffer(reqR); err != nil {
		errs = append(errs, "read: "+err.Error())
	}
	if err := conn.SetWriteBuffer(reqW); err != nil {
		errs = append(errs, "write: "+err.Error())
	}
	if len(errs) > 0 {
		result.Status = StatusDenied
		result.Err = strings.Join(errs, "; ")
	}
	return result
}

func clampUDPBuffer(n int) int {
	if n < minUDPBuffer {
		return minUDPBuffer
	}
	if n > maxUDPBuffer {
		return maxUDPBuffer
	}
	return n
}
