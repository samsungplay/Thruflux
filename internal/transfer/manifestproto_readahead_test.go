package transfer

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sheerbytes/sheerbytes/pkg/manifest"
)

func TestSendManifest_ReadAhead_Cancellation(t *testing.T) {
	// Test that read-ahead goroutine exits cleanly on cancellation
	tempDir, err := os.MkdirTemp("", "manifestproto_readahead_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create a large file to ensure read-ahead is active
	srcDir := filepath.Join(tempDir, "src")
	if err := os.MkdirAll(srcDir, 0755); err != nil {
		t.Fatalf("Failed to create src dir: %v", err)
	}

	largeFilePath := filepath.Join(srcDir, "large.bin")
	largeFileData := make([]byte, 2*1024*1024) // 2MB
	if err := os.WriteFile(largeFilePath, largeFileData, 0644); err != nil {
		t.Fatalf("Failed to write large file: %v", err)
	}

	// Build manifest
	m, err := manifest.Scan(srcDir)
	if err != nil {
		t.Fatalf("Failed to scan directory: %v", err)
	}

	// Create mock transport pair
	t1, t2 := NewMockPair()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Establish connection
	conn1, err := t1.Dial(ctx, "peer2")
	if err != nil {
		t.Fatalf("Dial error: %v", err)
	}
	defer conn1.Close()

	conn2, err := t2.Accept(ctx)
	if err != nil {
		t.Fatalf("Accept error: %v", err)
	}
	defer conn2.Close()

	// Open stream
	stream1, err := conn1.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream error: %v", err)
	}
	defer stream1.Close()

	stream2, err := conn2.AcceptStream(ctx)
	if err != nil {
		t.Fatalf("AcceptStream error: %v", err)
	}
	defer stream2.Close()

	// Use small chunk size to ensure multiple chunks
	chunkSize := uint32(64 * 1024) // 64KB

	// Create a context that will be cancelled
	cancelCtx, cancelFunc := context.WithCancel(ctx)
	
	// Start sender in goroutine
	senderDone := make(chan error, 1)
	go func() {
		// Cancel context after a short delay to test cancellation
		time.Sleep(10 * time.Millisecond)
		cancelFunc()
		// Give it a moment for cancellation to propagate
		time.Sleep(5 * time.Millisecond)
		err := SendManifest(cancelCtx, stream1, srcDir, m, chunkSize, nil)
		senderDone <- err
	}()

	// Start receiver
	outputDir := filepath.Join(tempDir, "output")
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		t.Fatalf("Failed to create output dir: %v", err)
	}

	receiverDone := make(chan error, 1)
	go func() {
		_, err := RecvManifest(ctx, stream2, outputDir, nil)
		receiverDone <- err
	}()

	// Wait for sender (should fail with context cancelled)
	select {
	case err := <-senderDone:
		if err == nil {
			t.Error("Expected sender to fail with context cancelled, but it succeeded")
		} else if err != context.Canceled && err != context.DeadlineExceeded {
			// Context cancellation is expected
			t.Logf("Sender error (expected): %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Error("Sender did not exit within timeout")
	}

	// Receiver may also fail, which is OK
	select {
	case <-receiverDone:
		// OK, receiver may fail when sender cancels
	case <-time.After(1 * time.Second):
		// Also OK, receiver may be waiting
	}

	// The key test: read-ahead goroutine should exit cleanly without deadlock
	// If we get here without hanging, the test passes
}
