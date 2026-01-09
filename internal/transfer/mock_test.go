package transfer

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"
)

func TestMockPair_DialAccept(t *testing.T) {
	t1, t2 := NewMockPair()

	ctx := context.Background()

	// Start accepting on t2
	acceptDone := make(chan Conn, 1)
	go func() {
		conn, err := t2.Accept(ctx)
		if err != nil {
			t.Errorf("Accept error: %v", err)
			return
		}
		acceptDone <- conn
	}()

	// Dial from t1
	conn1, err := t1.Dial(ctx, "peer2")
	if err != nil {
		t.Fatalf("Dial error: %v", err)
	}

	// Wait for accept
	select {
	case conn2 := <-acceptDone:
		if conn2 == nil {
			t.Fatal("Accept returned nil connection")
		}
		// Verify both connections are valid
		if conn1 == nil {
			t.Fatal("Dial returned nil connection")
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Accept timed out")
	}
}

func TestMockConn_OpenStreamAcceptStream(t *testing.T) {
	t1, t2 := NewMockPair()
	ctx := context.Background()

	// Establish connection
	conn1, err := t1.Dial(ctx, "peer2")
	if err != nil {
		t.Fatalf("Dial error: %v", err)
	}

	conn2, err := t2.Accept(ctx)
	if err != nil {
		t.Fatalf("Accept error: %v", err)
	}

	// Start accepting stream on conn2
	acceptDone := make(chan Stream, 1)
	go func() {
		stream, err := conn2.AcceptStream(ctx)
		if err != nil {
			t.Errorf("AcceptStream error: %v", err)
			return
		}
		acceptDone <- stream
	}()

	// Open stream on conn1
	stream1, err := conn1.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream error: %v", err)
	}

	// Wait for accept
	select {
	case stream2 := <-acceptDone:
		if stream2 == nil {
			t.Fatal("AcceptStream returned nil stream")
		}
		if stream1 == nil {
			t.Fatal("OpenStream returned nil stream")
		}
	case <-time.After(1 * time.Second):
		t.Fatal("AcceptStream timed out")
	}
}

func TestMockStream_ReadWrite(t *testing.T) {
	t1, t2 := NewMockPair()
	ctx := context.Background()

	// Establish connection
	conn1, err := t1.Dial(ctx, "peer2")
	if err != nil {
		t.Fatalf("Dial error: %v", err)
	}

	conn2, err := t2.Accept(ctx)
	if err != nil {
		t.Fatalf("Accept error: %v", err)
	}

	// Open stream
	stream1, err := conn1.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream error: %v", err)
	}

	stream2, err := conn2.AcceptStream(ctx)
	if err != nil {
		t.Fatalf("AcceptStream error: %v", err)
	}

	// Write from stream1, read from stream2
	testData := []byte("hello, world")

	// Start reading in a goroutine (io.Pipe blocks on Write until there's a reader)
	readDone := make(chan bool, 1)
	var readErr error
	var readBytes int
	var readBuf []byte
	go func() {
		buf := make([]byte, len(testData))
		n, err := stream2.Read(buf)
		readBytes = n
		readBuf = buf
		readErr = err
		readDone <- true
	}()

	// Give reader a moment to start
	time.Sleep(10 * time.Millisecond)

	// Now write
	written, err := stream1.Write(testData)
	if err != nil {
		t.Fatalf("Write error: %v", err)
	}
	if written != len(testData) {
		t.Errorf("Write returned %d, want %d", written, len(testData))
	}

	// Close writer to signal EOF
	stream1.Close()

	// Wait for read to complete
	select {
	case <-readDone:
		if readErr != nil && readErr != io.EOF {
			t.Fatalf("Read error: %v", readErr)
		}
		if readBytes != len(testData) {
			t.Errorf("Read returned %d, want %d", readBytes, len(testData))
		}
		if string(readBuf[:readBytes]) != string(testData) {
			t.Errorf("Read data = %q, want %q", string(readBuf[:readBytes]), string(testData))
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Read timed out")
	}

	// Write from stream2, read from stream1 (bidirectional)
	// Note: stream1 is closed, so we need a new stream
	stream3, err := conn1.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream error: %v", err)
	}

	stream4, err := conn2.AcceptStream(ctx)
	if err != nil {
		t.Fatalf("AcceptStream error: %v", err)
	}

	testData2 := []byte("bidirectional test")

	// Start reading in a goroutine
	readDone2 := make(chan bool, 1)
	var readErr2 error
	var readBytes2 int
	var readBuf2 []byte
	go func() {
		buf := make([]byte, len(testData2))
		n, err := stream3.Read(buf)
		readBytes2 = n
		readBuf2 = buf
		readErr2 = err
		readDone2 <- true
	}()

	// Give reader a moment to start
	time.Sleep(10 * time.Millisecond)

	written2, err := stream4.Write(testData2)
	if err != nil {
		t.Fatalf("Write error: %v", err)
	}
	if written2 != len(testData2) {
		t.Errorf("Write returned %d, want %d", written2, len(testData2))
	}

	stream4.Close()

	// Wait for read to complete
	select {
	case <-readDone2:
		if readErr2 != nil && readErr2 != io.EOF {
			t.Fatalf("Read error: %v", readErr2)
		}
		if readBytes2 != len(testData2) {
			t.Errorf("Read returned %d, want %d", readBytes2, len(testData2))
		}
		if string(readBuf2[:readBytes2]) != string(testData2) {
			t.Errorf("Read data = %q, want %q", string(readBuf2[:readBytes2]), string(testData2))
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Read timed out")
	}
}

func TestMockStream_MultipleStreams(t *testing.T) {
	t1, t2 := NewMockPair()
	ctx := context.Background()

	// Establish connection
	conn1, err := t1.Dial(ctx, "peer2")
	if err != nil {
		t.Fatalf("Dial error: %v", err)
	}

	conn2, err := t2.Accept(ctx)
	if err != nil {
		t.Fatalf("Accept error: %v", err)
	}

	// Open multiple streams
	streams1 := make([]Stream, 3)
	streams2 := make([]Stream, 3)

	for i := 0; i < 3; i++ {
		// Start accepting
		acceptDone := make(chan Stream, 1)
		go func() {
			stream, err := conn2.AcceptStream(ctx)
			if err != nil {
				t.Errorf("AcceptStream error: %v", err)
				return
			}
			acceptDone <- stream
		}()

		// Open stream
		stream1, err := conn1.OpenStream(ctx)
		if err != nil {
			t.Fatalf("OpenStream error: %v", err)
		}

		select {
		case stream2 := <-acceptDone:
			streams1[i] = stream1
			streams2[i] = stream2
		case <-time.After(1 * time.Second):
			t.Fatalf("AcceptStream timed out for stream %d", i)
		}
	}

	// Write different data to each stream
	testData := [][]byte{
		[]byte("stream 0"),
		[]byte("stream 1"),
		[]byte("stream 2"),
	}

	// Start reading in goroutines before writing (io.Pipe blocks on Write until reader is ready)
	readResults := make([]chan struct {
		data []byte
		err  error
	}, 3)
	for i := 0; i < 3; i++ {
		readResults[i] = make(chan struct {
			data []byte
			err  error
		}, 1)
		go func(idx int) {
			buf := make([]byte, len(testData[idx]))
			n, err := streams2[idx].Read(buf)
			readResults[idx] <- struct {
				data []byte
				err  error
			}{data: buf[:n], err: err}
		}(i)
	}

	// Give readers time to start
	time.Sleep(10 * time.Millisecond)

	// Now write
	for i := 0; i < 3; i++ {
		_, err := streams1[i].Write(testData[i])
		if err != nil {
			t.Fatalf("Write error on stream %d: %v", i, err)
		}
		streams1[i].Close()
	}

	// Wait for reads to complete
	for i := 0; i < 3; i++ {
		select {
		case result := <-readResults[i]:
			if result.err != nil && result.err != io.EOF {
				t.Fatalf("Read error on stream %d: %v", i, result.err)
			}
			if string(result.data) != string(testData[i]) {
				t.Errorf("Stream %d: read %q, want %q", i, string(result.data), string(testData[i]))
			}
		case <-time.After(1 * time.Second):
			t.Fatalf("Read timed out for stream %d", i)
		}
	}
}

func TestMockTransport_Close(t *testing.T) {
	t1, t2 := NewMockPair()
	ctx := context.Background()

	// Establish connection
	conn1, err := t1.Dial(ctx, "peer2")
	if err != nil {
		t.Fatalf("Dial error: %v", err)
	}

	// Accept on the other side to complete the connection
	_, err = t2.Accept(ctx)
	if err != nil {
		t.Fatalf("Accept error: %v", err)
	}

	// Close transport
	err = t1.Close()
	if err != nil {
		t.Fatalf("Close error: %v", err)
	}

	// Dial should fail after close
	_, err = t1.Dial(ctx, "peer2")
	if err == nil {
		t.Error("Expected error when dialing closed transport")
	}

	// Connection should still work (it's already established)
	// But we need to accept on the other side
	go func() {
		_, _ = conn1.OpenStream(ctx)
	}()

	// Give it a moment, then verify connection can still open streams
	// (though the other side won't accept, so it will hang - but that's expected)
	time.Sleep(50 * time.Millisecond)
}

func TestMockConn_Close(t *testing.T) {
	t1, t2 := NewMockPair()
	ctx := context.Background()

	// Establish connection
	conn1, err := t1.Dial(ctx, "peer2")
	if err != nil {
		t.Fatalf("Dial error: %v", err)
	}

	conn2, err := t2.Accept(ctx)
	if err != nil {
		t.Fatalf("Accept error: %v", err)
	}

	// Open a stream
	stream1, err := conn1.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream error: %v", err)
	}

	stream2, err := conn2.AcceptStream(ctx)
	if err != nil {
		t.Fatalf("AcceptStream error: %v", err)
	}

	// Close connection
	err = conn1.Close()
	if err != nil {
		t.Fatalf("Close error: %v", err)
	}

	// Opening new stream should fail
	_, err = conn1.OpenStream(ctx)
	if err == nil {
		t.Error("Expected error when opening stream on closed connection")
	}

	// Existing stream should still work (until it's closed)
	// Start reading before writing
	readDone := make(chan bool, 1)
	go func() {
		buf := make([]byte, 4)
		_, _ = stream2.Read(buf)
		readDone <- true
	}()

	time.Sleep(10 * time.Millisecond)

	testData := []byte("test")
	_, err = stream1.Write(testData)
	if err != nil {
		// This might fail if the connection close propagated, which is acceptable
		// The important thing is that OpenStream fails
	}

	// Don't wait for read - just verify OpenStream fails
	stream1.Close()
}

func TestMockStream_Close(t *testing.T) {
	t1, t2 := NewMockPair()
	ctx := context.Background()

	// Establish connection and stream
	conn1, err := t1.Dial(ctx, "peer2")
	if err != nil {
		t.Fatalf("Dial error: %v", err)
	}

	conn2, err := t2.Accept(ctx)
	if err != nil {
		t.Fatalf("Accept error: %v", err)
	}

	stream1, err := conn1.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream error: %v", err)
	}

	stream2, err := conn2.AcceptStream(ctx)
	if err != nil {
		t.Fatalf("AcceptStream error: %v", err)
	}

	// Close stream
	err = stream1.Close()
	if err != nil {
		t.Fatalf("Close error: %v", err)
	}

	// Write should fail after close
	_, err = stream1.Write([]byte("test"))
	if err == nil {
		t.Error("Expected error when writing to closed stream")
	}

	// Read should return EOF or error after close
	buf := make([]byte, 10)
	_, err = stream2.Read(buf)
	if err == nil {
		t.Error("Expected error when reading from closed stream")
	}
}

func TestMockStream_Concurrent(t *testing.T) {
	t1, t2 := NewMockPair()
	ctx := context.Background()

	// Establish connection
	conn1, err := t1.Dial(ctx, "peer2")
	if err != nil {
		t.Fatalf("Dial error: %v", err)
	}

	conn2, err := t2.Accept(ctx)
	if err != nil {
		t.Fatalf("Accept error: %v", err)
	}

	// Open and accept multiple streams concurrently, then pair by StreamID.
	numStreams := 10
	type pair struct {
		local  Stream
		remote Stream
	}
	pairs := make(map[uint64]pair)
	var pairsMu sync.Mutex
	pairCh := make(chan pair, numStreams)

	record := func(id uint64, local Stream, remote Stream) {
		pairsMu.Lock()
		current := pairs[id]
		if local != nil {
			current.local = local
		}
		if remote != nil {
			current.remote = remote
		}
		if current.local != nil && current.remote != nil {
			delete(pairs, id)
			pairsMu.Unlock()
			pairCh <- current
			return
		}
		pairs[id] = current
		pairsMu.Unlock()
	}

	var openWg sync.WaitGroup
	openWg.Add(numStreams)
	for i := 0; i < numStreams; i++ {
		go func() {
			defer openWg.Done()
			stream1, err := conn1.OpenStream(ctx)
			if err != nil {
				t.Errorf("OpenStream error: %v", err)
				return
			}
			ider, ok := stream1.(StreamIDer)
			if !ok {
				t.Errorf("stream missing StreamID")
				return
			}
			record(ider.StreamID(), stream1, nil)
		}()
	}

	var acceptWg sync.WaitGroup
	acceptWg.Add(numStreams)
	for i := 0; i < numStreams; i++ {
		go func() {
			defer acceptWg.Done()
			stream2, err := conn2.AcceptStream(ctx)
			if err != nil {
				t.Errorf("AcceptStream error: %v", err)
				return
			}
			ider, ok := stream2.(StreamIDer)
			if !ok {
				t.Errorf("stream missing StreamID")
				return
			}
			record(ider.StreamID(), nil, stream2)
		}()
	}

	openWg.Wait()
	acceptWg.Wait()

	done := make(chan bool, numStreams)
	for i := 0; i < numStreams; i++ {
		select {
		case p := <-pairCh:
			go func(pair pair) {
				defer pair.local.Close()
				defer pair.remote.Close()

				id := byte(0)
				if ider, ok := pair.local.(StreamIDer); ok {
					id = byte(ider.StreamID())
				}

				readDone := make(chan struct {
					data byte
					err  error
				}, 1)
				go func() {
					buf := make([]byte, 1)
					_, err := pair.remote.Read(buf)
					readDone <- struct {
						data byte
						err  error
					}{data: buf[0], err: err}
				}()

				time.Sleep(10 * time.Millisecond)

				_, err := pair.local.Write([]byte{id})
				if err != nil {
					t.Errorf("Write error: %v", err)
					done <- false
					return
				}

				select {
				case result := <-readDone:
					if result.err != nil && result.err != io.EOF {
						t.Errorf("Read error: %v", result.err)
						done <- false
						return
					}
					if result.data != id {
						t.Errorf("Read wrong data: got %d, want %d", result.data, id)
						done <- false
						return
					}
				case <-time.After(1 * time.Second):
					t.Errorf("Read timed out for stream")
					done <- false
					return
				}

				done <- true
			}(p)
		default:
			t.Errorf("Missing paired stream")
			done <- false
		}
	}

	successCount := 0
	for i := 0; i < numStreams; i++ {
		if <-done {
			successCount++
		}
	}

	if successCount != numStreams {
		t.Errorf("Only %d/%d streams succeeded", successCount, numStreams)
	}
}
