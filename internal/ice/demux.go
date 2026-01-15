package ice

import (
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"github.com/pion/stun"
)

const (
	maxUDPPacketSize = 64 * 1024
	stunQueueDepth   = 64
	dataQueueDepth   = 2048
)

type demuxPacket struct {
	buf     []byte
	addr    net.Addr
	release func()
}

type packetDemux struct {
	conn      *net.UDPConn
	stunCh    chan demuxPacket
	dataCh    chan demuxPacket
	closed    chan struct{}
	closeOnce sync.Once

	mu       sync.Mutex
	stunConn net.PacketConn
	dataConn net.PacketConn
	bufPool  sync.Pool
}

func newPacketDemux(conn *net.UDPConn) *packetDemux {
	d := &packetDemux{
		conn:   conn,
		stunCh: make(chan demuxPacket, stunQueueDepth),
		dataCh: make(chan demuxPacket, dataQueueDepth),
		closed: make(chan struct{}),
	}
	d.bufPool.New = func() interface{} {
		return make([]byte, maxUDPPacketSize)
	}
	d.stunConn = newDemuxPacketConn(d, d.stunCh)
	d.dataConn = newDemuxPacketConn(d, d.dataCh)
	go d.readLoop()
	return d
}

func (d *packetDemux) readLoop() {
	for {
		buf := d.getBuffer()
		n, addr, err := d.conn.ReadFrom(buf)
		if err != nil {
			d.putBuffer(buf)
			if errors.Is(err, net.ErrClosed) {
				return
			}
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			return
		}
		if n == 0 {
			d.putBuffer(buf)
			continue
		}
		pktBuf := buf
		pkt := demuxPacket{
			buf:  pktBuf[:n],
			addr: addr,
			release: func() {
				d.putBuffer(pktBuf)
			},
		}
		if stun.IsMessage(pkt.buf) {
			select {
			case d.stunCh <- pkt:
			case <-d.closed:
				pkt.release()
				return
			}
			continue
		}
		select {
		case d.dataCh <- pkt:
		default:
			pkt.release()
		}
	}
}

func (d *packetDemux) Close() error {
	var err error
	d.closeOnce.Do(func() {
		close(d.closed)
		err = d.conn.Close()
	})
	return err
}

func (d *packetDemux) STUNConn() net.PacketConn {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.stunConn
}

func (d *packetDemux) DataConn() net.PacketConn {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.dataConn
}

func (d *packetDemux) getBuffer() []byte {
	buf := d.bufPool.Get().([]byte)
	if cap(buf) < maxUDPPacketSize {
		return make([]byte, maxUDPPacketSize)
	}
	return buf[:maxUDPPacketSize]
}

func (d *packetDemux) putBuffer(buf []byte) {
	if cap(buf) < maxUDPPacketSize {
		return
	}
	d.bufPool.Put(buf[:maxUDPPacketSize])
}

type demuxPacketConn struct {
	demux  *packetDemux
	ch     <-chan demuxPacket
	closed chan struct{}

	mu           sync.Mutex
	readDeadline time.Time
	closeOnce    sync.Once
}

func newDemuxPacketConn(demux *packetDemux, ch <-chan demuxPacket) *demuxPacketConn {
	return &demuxPacketConn{
		demux:  demux,
		ch:     ch,
		closed: make(chan struct{}),
	}
}

func (c *demuxPacketConn) ReadFrom(p []byte) (n int, addr net.Addr, err error) {
	deadline := c.readDeadlineAt()
	if !deadline.IsZero() {
		now := time.Now()
		if now.After(deadline) {
			return 0, nil, timeoutError{}
		}
		timer := time.NewTimer(deadline.Sub(now))
		defer timer.Stop()
		select {
		case pkt, ok := <-c.ch:
			if !ok {
				return 0, nil, io.ErrClosedPipe
			}
			n = copy(p, pkt.buf)
			if pkt.release != nil {
				pkt.release()
			}
			if n < len(pkt.buf) {
				return n, pkt.addr, io.ErrShortBuffer
			}
			return n, pkt.addr, nil
		case <-timer.C:
			return 0, nil, timeoutError{}
		case <-c.closed:
			return 0, nil, io.ErrClosedPipe
		case <-c.demux.closed:
			return 0, nil, io.ErrClosedPipe
		}
	}

	select {
	case pkt, ok := <-c.ch:
		if !ok {
			return 0, nil, io.ErrClosedPipe
		}
		n = copy(p, pkt.buf)
		if pkt.release != nil {
			pkt.release()
		}
		if n < len(pkt.buf) {
			return n, pkt.addr, io.ErrShortBuffer
		}
		return n, pkt.addr, nil
	case <-c.closed:
		return 0, nil, io.ErrClosedPipe
	case <-c.demux.closed:
		return 0, nil, io.ErrClosedPipe
	}
}

func (c *demuxPacketConn) WriteTo(p []byte, addr net.Addr) (n int, err error) {
	return c.demux.conn.WriteTo(p, addr)
}

func (c *demuxPacketConn) Close() error {
	c.closeOnce.Do(func() {
		close(c.closed)
	})
	return nil
}

func (c *demuxPacketConn) LocalAddr() net.Addr {
	return c.demux.conn.LocalAddr()
}

func (c *demuxPacketConn) SetDeadline(t time.Time) error {
	c.SetReadDeadline(t)
	return c.demux.conn.SetWriteDeadline(t)
}

func (c *demuxPacketConn) SetReadDeadline(t time.Time) error {
	c.mu.Lock()
	c.readDeadline = t
	c.mu.Unlock()
	return nil
}

func (c *demuxPacketConn) SetWriteDeadline(t time.Time) error {
	return c.demux.conn.SetWriteDeadline(t)
}

func (c *demuxPacketConn) readDeadlineAt() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.readDeadline
}

type timeoutError struct{}

func (timeoutError) Error() string   { return "i/o timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }
