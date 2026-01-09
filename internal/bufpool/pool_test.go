package bufpool

import (
	"testing"
)

func TestPool_GetPut(t *testing.T) {
	bufSize := 4096
	pool := New(bufSize)

	// Get a buffer
	buf1 := pool.Get()
	if len(buf1) != bufSize {
		t.Errorf("expected buffer length %d, got %d", bufSize, len(buf1))
	}
	if cap(buf1) < bufSize {
		t.Errorf("expected buffer capacity >= %d, got %d", bufSize, cap(buf1))
	}

	// Put it back
	pool.Put(buf1)

	// Get another buffer - should reuse the same one
	buf2 := pool.Get()
	if len(buf2) != bufSize {
		t.Errorf("expected buffer length %d, got %d", bufSize, len(buf2))
	}

	// Verify BufSize
	if pool.BufSize() != bufSize {
		t.Errorf("expected BufSize %d, got %d", bufSize, pool.BufSize())
	}
}

func TestPool_SizeCorrectness(t *testing.T) {
	bufSize := 8192
	pool := New(bufSize)

	// Get multiple buffers and verify they're all the correct size
	buffers := make([][]byte, 10)
	for i := range buffers {
		buffers[i] = pool.Get()
		if len(buffers[i]) != bufSize {
			t.Errorf("buffer %d: expected length %d, got %d", i, bufSize, len(buffers[i]))
		}
	}

	// Put them all back
	for _, buf := range buffers {
		pool.Put(buf)
	}

	// Get them again and verify reuse
	for i := range buffers {
		buf := pool.Get()
		if len(buf) != bufSize {
			t.Errorf("reused buffer %d: expected length %d, got %d", i, bufSize, len(buf))
		}
		pool.Put(buf)
	}
}

func TestPool_Reuse(t *testing.T) {
	bufSize := 1024
	pool := New(bufSize)

	// Get a buffer and modify it
	buf1 := pool.Get()
	for i := range buf1 {
		buf1[i] = byte(i % 256)
	}
	pool.Put(buf1)

	// Get another buffer - should be reused (but contents may be cleared or not)
	buf2 := pool.Get()
	if len(buf2) != bufSize {
		t.Errorf("expected buffer length %d, got %d", bufSize, len(buf2))
	}
	// Note: sync.Pool doesn't guarantee zeroing, so we don't check contents
}

func TestPool_TooSmallBuffer(t *testing.T) {
	bufSize := 4096
	pool := New(bufSize)

	// Put a buffer that's too small - should be discarded
	smallBuf := make([]byte, 1024)
	pool.Put(smallBuf)

	// Get a buffer - should be a new one of correct size
	buf := pool.Get()
	if len(buf) != bufSize {
		t.Errorf("expected buffer length %d, got %d", bufSize, len(buf))
	}
}

func TestPool_PanicOnZeroSize(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for zero bufSize")
		}
	}()
	New(0)
}

func TestPool_PanicOnNegativeSize(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for negative bufSize")
		}
	}()
	New(-1)
}

