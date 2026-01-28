package transfer

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const (
	virtualStreamIDShift = 56
	virtualStreamIDMask  = (uint64(1) << virtualStreamIDShift) - 1
)

type multiConn struct {
	conns     []Conn
	nextIdx   uint32
	acceptCh  chan acceptResult
	closeOnce sync.Once
	closeCh   chan struct{}
	startOnce sync.Once
	control   uint32
}

type acceptResult struct {
	stream    Stream
	connIndex int
	err       error
}

// NewMultiConn wraps multiple connections into a single Conn that
// distributes outgoing streams and accepts incoming streams from all conns.
func NewMultiConn(conns []Conn) (Conn, error) {
	if len(conns) == 0 {
		return nil, fmt.Errorf("no connections provided")
	}
	if len(conns) > 255 {
		return nil, fmt.Errorf("too many connections: %d", len(conns))
	}
	mc := &multiConn{
		conns:    append([]Conn(nil), conns...),
		acceptCh: make(chan acceptResult, 64),
		closeCh:  make(chan struct{}),
	}
	return mc, nil
}

func (m *multiConn) acceptLoop(idx int, conn Conn) {
	for {
		select {
		case <-m.closeCh:
			return
		default:
		}
		stream, err := conn.AcceptStream(context.Background())
		select {
		case m.acceptCh <- acceptResult{stream: stream, connIndex: idx, err: err}:
		case <-m.closeCh:
			if stream != nil {
				_ = stream.Close()
			}
			return
		}
		if err != nil {
			return
		}
	}
}

func (m *multiConn) startAcceptLoops() {
	m.startOnce.Do(func() {
		for idx, conn := range m.conns {
			go m.acceptLoop(idx, conn)
		}
	})
}

func (m *multiConn) OpenStream(ctx context.Context) (Stream, error) {
	idx := int(atomic.AddUint32(&m.nextIdx, 1)-1) % len(m.conns)
	stream, err := m.conns[idx].OpenStream(ctx)
	if err != nil {
		return nil, err
	}
	streamID, err := streamIDFromStream(stream)
	if err != nil {
		_ = stream.Close()
		return nil, err
	}
	return &multiStream{
		Stream:    stream,
		virtualID: makeVirtualStreamID(idx, streamID),
	}, nil
}

func (m *multiConn) AcceptStream(ctx context.Context) (Stream, error) {
	if atomic.CompareAndSwapUint32(&m.control, 0, 1) {
		stream, err := m.conns[0].AcceptStream(ctx)
		if err != nil {
			return nil, err
		}
		streamID, err := streamIDFromStream(stream)
		if err != nil {
			_ = stream.Close()
			return nil, err
		}
		m.startAcceptLoops()
		return &multiStream{
			Stream:    stream,
			virtualID: makeVirtualStreamID(0, streamID),
		}, nil
	}
	m.startAcceptLoops()
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case res := <-m.acceptCh:
			if res.err != nil {
				return nil, res.err
			}
			streamID, err := streamIDFromStream(res.stream)
			if err != nil {
				_ = res.stream.Close()
				return nil, err
			}
			return &multiStream{
				Stream:    res.stream,
				virtualID: makeVirtualStreamID(res.connIndex, streamID),
			}, nil
		}
	}
}

func (m *multiConn) RemoteAddr() net.Addr {
	if len(m.conns) == 0 {
		return nil
	}
	return m.conns[0].RemoteAddr()
}

func (m *multiConn) Close() error {
	var err error
	m.closeOnce.Do(func() {
		close(m.closeCh)
		for _, conn := range m.conns {
			if cerr := conn.Close(); cerr != nil && err == nil {
				err = cerr
			}
		}
	})
	return err
}

type multiStream struct {
	Stream
	virtualID uint64
}

func (s *multiStream) StreamID() uint64 {
	return s.virtualID
}

func (s *multiStream) SetReadDeadline(t time.Time) error {
	if ds, ok := s.Stream.(streamDeadlineSetter); ok {
		return ds.SetReadDeadline(t)
	}
	return nil
}

func (s *multiStream) SetWriteDeadline(t time.Time) error {
	if ds, ok := s.Stream.(streamDeadlineSetter); ok {
		return ds.SetWriteDeadline(t)
	}
	return nil
}

func (s *multiStream) SetDeadline(t time.Time) error {
	if ds, ok := s.Stream.(streamDeadlineSetter); ok {
		return ds.SetDeadline(t)
	}
	return nil
}

func makeVirtualStreamID(connIndex int, streamID uint64) uint64 {
	return (uint64(connIndex) << virtualStreamIDShift) | (streamID & virtualStreamIDMask)
}
