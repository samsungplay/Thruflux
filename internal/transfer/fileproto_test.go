package transfer

import (
	"context"
	"crypto/rand"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSendFile_RecvFile_EndToEnd(t *testing.T) {
	// Create temporary directory for test files
	tempDir, err := os.MkdirTemp("", "fileproto_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create input file with random bytes (2MB)
	inputFilename := "test_file.dat"
	inputPath := filepath.Join(tempDir, inputFilename)
	inputSize := 2 * 1024 * 1024 // 2MB

	// Generate random data
	inputData := make([]byte, inputSize)
	if _, err := rand.Read(inputData); err != nil {
		t.Fatalf("Failed to generate random data: %v", err)
	}

	// Write input file
	if err := os.WriteFile(inputPath, inputData, 0644); err != nil {
		t.Fatalf("Failed to write input file: %v", err)
	}

	// Create output directory
	outputDir := filepath.Join(tempDir, "output")
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		t.Fatalf("Failed to create output dir: %v", err)
	}

	// Create mock transport pair
	t1, t2 := NewMockPair()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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

	// Run sender and receiver in parallel
	senderDone := make(chan error, 1)
	receiverDone := make(chan struct {
		path string
		err  error
	}, 1)

	// Start sender
	go func() {
		err := SendFile(ctx, stream1, inputPath)
		senderDone <- err
	}()

	// Start receiver
	go func() {
		path, err := RecvFile(ctx, stream2, outputDir)
		receiverDone <- struct {
			path string
			err  error
		}{path: path, err: err}
	}()

	// Wait for both to complete
	var senderErr error
	var receiverResult struct {
		path string
		err  error
	}

	select {
	case senderErr = <-senderDone:
	case <-ctx.Done():
		t.Fatalf("Sender timed out: %v", ctx.Err())
	}

	select {
	case receiverResult = <-receiverDone:
	case <-ctx.Done():
		t.Fatalf("Receiver timed out: %v", ctx.Err())
	}

	// Check for errors
	if senderErr != nil {
		t.Fatalf("SendFile error: %v", senderErr)
	}
	if receiverResult.err != nil {
		t.Fatalf("RecvFile error: %v", receiverResult.err)
	}

	outputPath := receiverResult.path

	// Verify output file exists
	if _, err := os.Stat(outputPath); err != nil {
		t.Fatalf("Output file does not exist: %v", err)
	}

	// Verify file size
	outputInfo, err := os.Stat(outputPath)
	if err != nil {
		t.Fatalf("Failed to stat output file: %v", err)
	}
	if outputInfo.Size() != int64(inputSize) {
		t.Errorf("Output file size = %d, want %d", outputInfo.Size(), inputSize)
	}

	// Verify byte-for-byte equality
	outputData, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("Failed to read output file: %v", err)
	}

	if len(outputData) != len(inputData) {
		t.Fatalf("Output data length = %d, want %d", len(outputData), len(inputData))
	}

	for i := 0; i < len(inputData); i++ {
		if outputData[i] != inputData[i] {
			t.Errorf("Byte mismatch at offset %d: got %d, want %d", i, outputData[i], inputData[i])
			break
		}
	}

	// Verify filename is correct (should be base name only)
	expectedOutputPath := filepath.Join(outputDir, inputFilename)
	if outputPath != expectedOutputPath {
		t.Errorf("Output path = %q, want %q", outputPath, expectedOutputPath)
	}
}

func TestSendFile_RecvFile_SmallFile(t *testing.T) {
	// Test with a small file (1 byte)
	tempDir, err := os.MkdirTemp("", "fileproto_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	inputPath := filepath.Join(tempDir, "small.txt")
	inputData := []byte("A")
	if err := os.WriteFile(inputPath, inputData, 0644); err != nil {
		t.Fatalf("Failed to write input file: %v", err)
	}

	outputDir := filepath.Join(tempDir, "output")
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		t.Fatalf("Failed to create output dir: %v", err)
	}

	t1, t2 := NewMockPair()
	ctx := context.Background()

	conn1, _ := t1.Dial(ctx, "peer2")
	conn2, _ := t2.Accept(ctx)
	defer conn1.Close()
	defer conn2.Close()

	stream1, _ := conn1.OpenStream(ctx)
	stream2, _ := conn2.AcceptStream(ctx)
	defer stream1.Close()
	defer stream2.Close()

	senderDone := make(chan error, 1)
	receiverDone := make(chan struct {
		path string
		err  error
	}, 1)

	go func() {
		senderDone <- SendFile(ctx, stream1, inputPath)
	}()

	go func() {
		path, err := RecvFile(ctx, stream2, outputDir)
		receiverDone <- struct {
			path string
			err  error
		}{path: path, err: err}
	}()

	if err := <-senderDone; err != nil {
		t.Fatalf("SendFile error: %v", err)
	}

	result := <-receiverDone
	if result.err != nil {
		t.Fatalf("RecvFile error: %v", result.err)
	}

	outputData, err := os.ReadFile(result.path)
	if err != nil {
		t.Fatalf("Failed to read output file: %v", err)
	}

	if string(outputData) != string(inputData) {
		t.Errorf("Output data = %q, want %q", string(outputData), string(inputData))
	}
}

func TestSendFile_RecvFile_EmptyFile(t *testing.T) {
	// Test with an empty file
	tempDir, err := os.MkdirTemp("", "fileproto_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	inputPath := filepath.Join(tempDir, "empty.txt")
	if err := os.WriteFile(inputPath, []byte{}, 0644); err != nil {
		t.Fatalf("Failed to write input file: %v", err)
	}

	outputDir := filepath.Join(tempDir, "output")
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		t.Fatalf("Failed to create output dir: %v", err)
	}

	t1, t2 := NewMockPair()
	ctx := context.Background()

	conn1, _ := t1.Dial(ctx, "peer2")
	conn2, _ := t2.Accept(ctx)
	defer conn1.Close()
	defer conn2.Close()

	stream1, _ := conn1.OpenStream(ctx)
	stream2, _ := conn2.AcceptStream(ctx)
	defer stream1.Close()
	defer stream2.Close()

	senderDone := make(chan error, 1)
	receiverDone := make(chan struct {
		path string
		err  error
	}, 1)

	go func() {
		senderDone <- SendFile(ctx, stream1, inputPath)
	}()

	go func() {
		path, err := RecvFile(ctx, stream2, outputDir)
		receiverDone <- struct {
			path string
			err  error
		}{path: path, err: err}
	}()

	if err := <-senderDone; err != nil {
		t.Fatalf("SendFile error: %v", err)
	}

	result := <-receiverDone
	if result.err != nil {
		t.Fatalf("RecvFile error: %v", result.err)
	}

	outputInfo, err := os.Stat(result.path)
	if err != nil {
		t.Fatalf("Failed to stat output file: %v", err)
	}
	if outputInfo.Size() != 0 {
		t.Errorf("Output file size = %d, want 0", outputInfo.Size())
	}
}

func TestRecvFile_InvalidMagic(t *testing.T) {
	t1, t2 := NewMockPair()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn1, _ := t1.Dial(ctx, "peer2")
	conn2, _ := t2.Accept(ctx)
	defer conn1.Close()
	defer conn2.Close()

	stream1, _ := conn1.OpenStream(ctx)
	stream2, _ := conn2.AcceptStream(ctx)
	defer stream1.Close()
	defer stream2.Close()

	tempDir, err := os.MkdirTemp("", "fileproto_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Start RecvFile in goroutine (it will block waiting for data)
	recvDone := make(chan error, 1)
	go func() {
		_, err := RecvFile(ctx, stream2, tempDir)
		recvDone <- err
	}()

	// Give RecvFile a moment to start reading
	time.Sleep(10 * time.Millisecond)

	// Write invalid magic (exactly 4 bytes, same as valid magic length)
	// This will unblock the reader, which will detect invalid magic and return
	stream1.Write([]byte("INVA"))
	stream1.Close() // Close to signal EOF after the invalid magic

	// Wait for RecvFile to complete
	select {
	case err := <-recvDone:
		if err != ErrInvalidMagic {
			t.Errorf("Expected ErrInvalidMagic, got: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("Test timed out: %v", ctx.Err())
	}
}

func TestRecvFile_InvalidFilename(t *testing.T) {
	t1, t2 := NewMockPair()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn1, _ := t1.Dial(ctx, "peer2")
	conn2, _ := t2.Accept(ctx)
	defer conn1.Close()
	defer conn2.Close()

	stream1, _ := conn1.OpenStream(ctx)
	stream2, _ := conn2.AcceptStream(ctx)
	defer stream1.Close()
	defer stream2.Close()

	tempDir, err := os.MkdirTemp("", "fileproto_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Start RecvFile in goroutine (it will block waiting for data)
	recvDone := make(chan error, 1)
	go func() {
		_, err := RecvFile(ctx, stream2, tempDir)
		recvDone <- err
	}()

	// Give RecvFile a moment to start reading
	time.Sleep(10 * time.Millisecond)

	// Write valid magic
	stream1.Write([]byte(magicBytes))

	// Write filename length with path traversal attempt
	filenameLen := uint16(5) // "../.."
	writeUint16(stream1, filenameLen)
	stream1.Write([]byte("../.."))

	// Wait for RecvFile to complete
	select {
	case err := <-recvDone:
		if err != ErrInvalidFilename {
			t.Errorf("Expected ErrInvalidFilename, got: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("Test timed out: %v", ctx.Err())
	}
}

// Helper function to write uint16 in big endian
func writeUint16(w io.Writer, v uint16) error {
	var buf [2]byte
	buf[0] = byte(v >> 8)
	buf[1] = byte(v)
	_, err := w.Write(buf[:])
	return err
}

