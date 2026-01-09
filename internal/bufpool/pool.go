package bufpool

import (
	"sync"
)

// Pool provides a pool of byte buffers of a fixed size.
// Buffers are reused to reduce allocations and GC pressure.
type Pool struct {
	pool    sync.Pool
	bufSize int
}

// New creates a new buffer pool that returns buffers of exactly bufSize bytes.
func New(bufSize int) *Pool {
	if bufSize <= 0 {
		panic("bufSize must be positive")
	}
	return &Pool{
		bufSize: bufSize,
		pool: sync.Pool{
			New: func() interface{} {
				return make([]byte, bufSize)
			},
		},
	}
}

// Get returns a buffer from the pool, or allocates a new one if the pool is empty.
// The returned buffer is always exactly bufSize bytes.
func (p *Pool) Get() []byte {
	buf := p.pool.Get().([]byte)
	// Ensure buffer is exactly the right size
	if cap(buf) < p.bufSize {
		// Buffer too small, allocate new one
		return make([]byte, p.bufSize)
	}
	// Reslice to exact size (may be larger if reused)
	return buf[:p.bufSize]
}

// Put returns a buffer to the pool for reuse.
// The buffer should have been obtained from Get().
// If the buffer's capacity is less than bufSize, it will be discarded.
func (p *Pool) Put(buf []byte) {
	if cap(buf) < p.bufSize {
		// Buffer too small, discard it
		return
	}
	// Reset length to capacity for reuse
	buf = buf[:cap(buf)]
	p.pool.Put(buf)
}

// BufSize returns the size of buffers in this pool.
func (p *Pool) BufSize() int {
	return p.bufSize
}

