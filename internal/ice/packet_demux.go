package ice

import (
	"errors"
	"net"
	"os"
	"sync"
	"time"
)

// packetDemux splits a UDP connection into two: one for ICE (STUN/TURN) and one for Application (QUIC).
// It discriminates based on the first byte of the packet.
type packetDemux struct {
	conn       *net.UDPConn
	stunConn   *virtualPacketConn
	appConn    *virtualPacketConn
	closeCh    chan struct{}
	readLoopWg sync.WaitGroup
	closed     bool
	mu         sync.Mutex
}

func newPacketDemux(conn *net.UDPConn) *packetDemux {
	p := &packetDemux{
		conn:    conn,
		closeCh: make(chan struct{}),
	}
	p.stunConn = newVirtualPacketConn(p)
	p.appConn = newVirtualPacketConn(p)

	p.readLoopWg.Add(1)
	go p.readLoop()

	return p
}

func (p *packetDemux) STUNConn() net.PacketConn {
	return p.stunConn
}

func (p *packetDemux) AppConn() net.PacketConn {
	return p.appConn
}

// Stop stops the demuxer. For the new keep-alive behavior, this might be a no-op
// if we want to continue demultiplexing.
func (p *packetDemux) Stop() {
	// No-op: we keep running to mux/demux packets.
}

// Conn returns the underlying connection.
// Note: Direct access should be careful if read loop is running.
func (p *packetDemux) Conn() *net.UDPConn {
	return p.conn
}

func (p *packetDemux) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil
	}
	p.closed = true
	close(p.closeCh)

	// Close the underlying connection
	err := p.conn.Close()

	p.stunConn.close(errors.New("closed"))
	p.appConn.close(errors.New("closed"))

	return err
}

func (p *packetDemux) readLoop() {
	defer p.readLoopWg.Done()
	buf := make([]byte, 2048) // MTU + headroom

	for {
		// We rely on p.conn.Close() to unblock ReadFrom
		n, addr, err := p.conn.ReadFrom(buf)
		if err != nil {
			if !p.isClosed() {
				// If not explicitly closed by us, valid read error
				// Propagate to virtual connections?
				// Or just log and continue?
				// Usually ReadFrom error on UDP meant socket closed.
				p.stunConn.close(err)
				p.appConn.close(err)
				p.Close()
			}
			return
		}

		if n == 0 {
			continue
		}

		// Copy data because buf is reused
		pkt := make([]byte, n)
		copy(pkt, buf[:n])

		// Filter: 0x00-0x03 are STUN/ICE
		// (RFC 7983: 0-3 are STUN, 20-63 are DTLS, 128-191 are RTP/RTCP)
		// QUIC short header: 0xxxxxxx (0x00-0x7F) but usually has Spin bit etc.
		// QUIC long header: 1xxxxxxx (0x80-0xFF)
		// Wait, user said "0x00-0x03 go to ICE, everything else goes to QUIC".
		// This relies on QUIC packets not starting with 0x00-0x03.
		// QUIC Short Header packets start with 0, but the fixed bit (0x40) is usually 1 in v1.
		// So 0x40 | ... is > 0x03.
		// If 0-RTT etc, might be different. But user asked for this specific filter.
		if pkt[0] <= 3 {
			p.stunConn.push(pkt, addr)
		} else {
			p.appConn.push(pkt, addr)
		}
	}
}

func (p *packetDemux) isClosed() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.closed
}

type virtualPacketConn struct {
	p            *packetDemux
	readCh       chan packetData
	closeCh      chan struct{}
	closed       bool
	closedErr    error
	mu           sync.Mutex
	localAddr    net.Addr
	readDeadline time.Time
}

type packetData struct {
	data []byte
	addr net.Addr
}

func newVirtualPacketConn(p *packetDemux) *virtualPacketConn {
	return &virtualPacketConn{
		p:         p,
		readCh:    make(chan packetData, 128), // Buffer incoming packets
		closeCh:   make(chan struct{}),
		localAddr: p.conn.LocalAddr(),
	}
}

func (v *virtualPacketConn) ReadFrom(p []byte) (n int, addr net.Addr, err error) {
	var timer *time.Timer
	var timeoutCh <-chan time.Time

	v.mu.Lock()
	deadline := v.readDeadline
	v.mu.Unlock()

	if !deadline.IsZero() {
		dur := time.Until(deadline)
		if dur <= 0 {
			return 0, nil, os.ErrDeadlineExceeded
		}
		timer = time.NewTimer(dur)
		timeoutCh = timer.C
		defer timer.Stop()
	}

	select {
	case <-v.closeCh:
		return 0, nil, v.closedErr
	case pkt := <-v.readCh:
		n = copy(p, pkt.data)
		addr = pkt.addr
		return n, addr, nil
	case <-timeoutCh:
		return 0, nil, os.ErrDeadlineExceeded
	}
}

func (v *virtualPacketConn) WriteTo(p []byte, addr net.Addr) (n int, err error) {
	if v.isClosed() {
		return 0, errors.New("conn closed")
	}
	// Write directly to underlying connection
	// We ignore write deadlines because they are hard to virtualize on a shared socket
	// without blocking others. The underlying socket remains non-blocking or blocking default.
	return v.p.conn.WriteTo(p, addr)
}

func (v *virtualPacketConn) Close() error {
	return v.close(nil)
}

func (v *virtualPacketConn) LocalAddr() net.Addr {
	return v.localAddr
}

func (v *virtualPacketConn) SetDeadline(t time.Time) error {
	return v.SetReadDeadline(t)
}

func (v *virtualPacketConn) SetReadDeadline(t time.Time) error {
	v.mu.Lock()
	v.readDeadline = t
	v.mu.Unlock()
	return nil
}

func (v *virtualPacketConn) SetWriteDeadline(t time.Time) error {
	// No-op for now as we share the write socket
	return nil
}

func (v *virtualPacketConn) push(data []byte, addr net.Addr) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed {
		return
	}
	select {
	case v.readCh <- packetData{data, addr}:
	default:
		// Drop if buffer full
	}
}

func (v *virtualPacketConn) close(cause error) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed {
		return nil
	}
	v.closed = true
	if cause == nil {
		cause = errors.New("closed")
	}
	v.closedErr = cause
	close(v.closeCh)
	return nil
}

func (v *virtualPacketConn) isClosed() bool {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.closed
}
