package transfer

import (
	"context"
	"crypto/rand"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/sheerbytes/sheerbytes/pkg/manifest"
)

func TestSendManifest_RecvManifest_EndToEnd(t *testing.T) {
	// Create temporary directory for test
	tempDir, err := os.MkdirTemp("", "manifestproto_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create source tree:
	// src/
	//   a.txt (random bytes)
	//   d1/
	//     b.bin (random bytes)
	//     emptydir/
	srcDir := filepath.Join(tempDir, "src")
	if err := os.MkdirAll(srcDir, 0755); err != nil {
		t.Fatalf("Failed to create src dir: %v", err)
	}

	// Create a.txt with random bytes (larger file to test chunking with small chunk size)
	aPath := filepath.Join(srcDir, "a.txt")
	aData := make([]byte, 256*1024) // 256KB to test chunking with 64KB chunks
	if _, err := rand.Read(aData); err != nil {
		t.Fatalf("Failed to generate random data: %v", err)
	}
	if err := os.WriteFile(aPath, aData, 0644); err != nil {
		t.Fatalf("Failed to write a.txt: %v", err)
	}

	// Create d1 directory
	d1Dir := filepath.Join(srcDir, "d1")
	if err := os.MkdirAll(d1Dir, 0755); err != nil {
		t.Fatalf("Failed to create d1 dir: %v", err)
	}

	// Create b.bin with random bytes
	bPath := filepath.Join(d1Dir, "b.bin")
	bData := make([]byte, 2048) // 2KB
	if _, err := rand.Read(bData); err != nil {
		t.Fatalf("Failed to generate random data: %v", err)
	}
	if err := os.WriteFile(bPath, bData, 0644); err != nil {
		t.Fatalf("Failed to write b.bin: %v", err)
	}

	// Create emptydir
	emptyDir := filepath.Join(d1Dir, "emptydir")
	if err := os.MkdirAll(emptyDir, 0755); err != nil {
		t.Fatalf("Failed to create emptydir: %v", err)
	}

	// Build manifest using Scan
	m, err := manifest.Scan(srcDir)
	if err != nil {
		t.Fatalf("Failed to scan directory: %v", err)
	}

	// Verify manifest has expected structure
	if m.FileCount != 2 {
		t.Errorf("Expected 2 files, got %d", m.FileCount)
	}
	if m.FolderCount < 2 { // d1 and emptydir
		t.Errorf("Expected at least 2 folders, got %d", m.FolderCount)
	}

	// Create output directory
	outputDir := filepath.Join(tempDir, "output")
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		t.Fatalf("Failed to create output dir: %v", err)
	}

	// Create mock transport pair
	t1, t2 := NewMockPair()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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
		manifest manifest.Manifest
		err      error
	}, 1)

	// Start sender - use small chunk size (64KB) and window 8 to stress the pipeline
	go func() {
		chunkSize := uint32(64 * 1024)
		windowSize := uint32(8)
		err := SendManifest(ctx, stream1, srcDir, m, chunkSize, windowSize, 0, nil, nil)
		senderDone <- err
	}()

	// Start receiver
	go func() {
		receivedManifest, err := RecvManifest(ctx, stream2, outputDir, nil)
		receiverDone <- struct {
			manifest manifest.Manifest
			err      error
		}{manifest: receivedManifest, err: err}
	}()

	// Wait for both to complete
	var senderErr error
	var receiverResult struct {
		manifest manifest.Manifest
		err      error
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
		t.Fatalf("SendManifest error: %v", senderErr)
	}
	if receiverResult.err != nil {
		t.Fatalf("RecvManifest error: %v", receiverResult.err)
	}

	receivedManifest := receiverResult.manifest

	// Verify returned manifest matches basic summary
	if receivedManifest.Root != m.Root {
		t.Errorf("Manifest Root = %s, want %s", receivedManifest.Root, m.Root)
	}
	if receivedManifest.FileCount != m.FileCount {
		t.Errorf("Manifest FileCount = %d, want %d", receivedManifest.FileCount, m.FileCount)
	}
	if receivedManifest.FolderCount != m.FolderCount {
		t.Errorf("Manifest FolderCount = %d, want %d", receivedManifest.FolderCount, m.FolderCount)
	}
	if receivedManifest.TotalBytes != m.TotalBytes {
		t.Errorf("Manifest TotalBytes = %d, want %d", receivedManifest.TotalBytes, m.TotalBytes)
	}

	// Verify output tree exists under outDir/<m.Root>/...
	outputRoot := filepath.Join(outputDir, m.Root)
	if _, err := os.Stat(outputRoot); err != nil {
		t.Fatalf("Output root directory does not exist: %v", err)
	}

	// Verify a.txt exists and matches byte-for-byte
	outputAPath := filepath.Join(outputRoot, "a.txt")
	if _, err := os.Stat(outputAPath); err != nil {
		t.Fatalf("Output a.txt does not exist: %v", err)
	}
	outputAData, err := os.ReadFile(outputAPath)
	if err != nil {
		t.Fatalf("Failed to read output a.txt: %v", err)
	}
	if !reflect.DeepEqual(outputAData, aData) {
		t.Error("Output a.txt does not match input byte-for-byte")
	}

	// Verify b.bin exists and matches byte-for-byte
	outputBPath := filepath.Join(outputRoot, "d1", "b.bin")
	if _, err := os.Stat(outputBPath); err != nil {
		t.Fatalf("Output b.bin does not exist: %v", err)
	}
	outputBData, err := os.ReadFile(outputBPath)
	if err != nil {
		t.Fatalf("Failed to read output b.bin: %v", err)
	}
	if !reflect.DeepEqual(outputBData, bData) {
		t.Error("Output b.bin does not match input byte-for-byte")
	}

	// Verify emptydir exists
	outputEmptyDir := filepath.Join(outputRoot, "d1", "emptydir")
	info, err := os.Stat(outputEmptyDir)
	if err != nil {
		t.Fatalf("Output emptydir does not exist: %v", err)
	}
	if !info.IsDir() {
		t.Error("Output emptydir is not a directory")
	}

	// Verify it's actually empty
	entries, err := os.ReadDir(outputEmptyDir)
	if err != nil {
		t.Fatalf("Failed to read emptydir: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("Expected emptydir to be empty, got %d entries", len(entries))
	}
}

func TestSendManifest_RecvManifest_ScanPaths_MultiSelection(t *testing.T) {
	// Test with ScanPaths to handle multiple files with same paths via ordinal distinction
	// Create two separate temp directories with the same base name to trigger ordinal prefixes
	tempDir1, err := os.MkdirTemp("", "manifestproto_test1_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir1)

	tempDir2, err := os.MkdirTemp("", "manifestproto_test2_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir2)

	// Create directories with the same base name in different locations
	// This will trigger ordinal prefixes in ScanPaths
	baseName := "samedir"
	src1Dir := filepath.Join(tempDir1, baseName)
	src2Dir := filepath.Join(tempDir2, baseName)
	
	if err := os.MkdirAll(src1Dir, 0755); err != nil {
		t.Fatalf("Failed to create src1 dir: %v", err)
	}
	if err := os.MkdirAll(src2Dir, 0755); err != nil {
		t.Fatalf("Failed to create src2 dir: %v", err)
	}

	// Create files with same names in both directories
	file1Data := []byte("content from src1")
	file2Data := []byte("content from src2")

	if err := os.WriteFile(filepath.Join(src1Dir, "samefile.txt"), file1Data, 0644); err != nil {
		t.Fatalf("Failed to write file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(src2Dir, "samefile.txt"), file2Data, 0644); err != nil {
		t.Fatalf("Failed to write file: %v", err)
	}

	// Also create a subdirectory with same name
	sub1Dir := filepath.Join(src1Dir, "subdir")
	sub2Dir := filepath.Join(src2Dir, "subdir")
	if err := os.MkdirAll(sub1Dir, 0755); err != nil {
		t.Fatalf("Failed to create subdir: %v", err)
	}
	if err := os.MkdirAll(sub2Dir, 0755); err != nil {
		t.Fatalf("Failed to create subdir: %v", err)
	}

	// Create files in subdirectories with same names
	sub1FileData := []byte("subdir content from src1")
	sub2FileData := []byte("subdir content from src2")

	if err := os.WriteFile(filepath.Join(sub1Dir, "subfile.txt"), sub1FileData, 0644); err != nil {
		t.Fatalf("Failed to write file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(sub2Dir, "subfile.txt"), sub2FileData, 0644); err != nil {
		t.Fatalf("Failed to write file: %v", err)
	}

	// Build manifest using ScanPaths - this should add ordinal prefixes
	m, err := manifest.ScanPaths([]string{src1Dir, src2Dir})
	if err != nil {
		t.Fatalf("Failed to scan paths: %v", err)
	}

	// Verify manifest has expected structure with ordinal prefixes
	if m.Root != "selection" {
		t.Errorf("Expected Root = 'selection', got %s", m.Root)
	}
	if m.FileCount != 4 { // 2 samefile.txt + 2 subfile.txt
		t.Errorf("Expected 4 files, got %d", m.FileCount)
	}

	// Verify ordinal prefixes are present (since both dirs have same base name)
	found1Prefix := false
	found2Prefix := false
	for _, item := range m.Items {
		if strings.HasPrefix(item.RelPath, "1_") {
			found1Prefix = true
		}
		if strings.HasPrefix(item.RelPath, "2_") {
			found2Prefix = true
		}
	}
	if !found1Prefix || !found2Prefix {
		t.Error("Expected ordinal prefixes (1_, 2_) in manifest items")
	}

	// Create output directory
	outputDir, err := os.MkdirTemp("", "manifestproto_output_*")
	if err != nil {
		t.Fatalf("Failed to create output dir: %v", err)
	}
	defer os.RemoveAll(outputDir)

	// Create mock transport pair
	t1, t2 := NewMockPair()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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

	// ScanPaths creates RelPaths like "1_samedir/samefile.txt" and "2_samedir/samefile.txt"
	// The actual files are at src1Dir/samefile.txt and src2Dir/samefile.txt
	// We need to create a structure that matches the RelPaths for SendManifest
	// Create a combined root with ordinal-prefixed directories
	combinedRoot, err := os.MkdirTemp("", "manifestproto_combined_*")
	if err != nil {
		t.Fatalf("Failed to create combined root: %v", err)
	}
	defer os.RemoveAll(combinedRoot)

	// Create "1_samedir" and "2_samedir" directories to match manifest RelPaths
	dir1Prefix := filepath.Join(combinedRoot, "1_"+baseName)
	dir2Prefix := filepath.Join(combinedRoot, "2_"+baseName)
	
	if err := os.MkdirAll(dir1Prefix, 0755); err != nil {
		t.Fatalf("Failed to create dir: %v", err)
	}
	if err := os.MkdirAll(dir2Prefix, 0755); err != nil {
		t.Fatalf("Failed to create dir: %v", err)
	}

	// Copy files to match the manifest structure
	if err := os.WriteFile(filepath.Join(dir1Prefix, "samefile.txt"), file1Data, 0644); err != nil {
		t.Fatalf("Failed to write file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir2Prefix, "samefile.txt"), file2Data, 0644); err != nil {
		t.Fatalf("Failed to write file: %v", err)
	}
	
	sub1Prefix := filepath.Join(dir1Prefix, "subdir")
	sub2Prefix := filepath.Join(dir2Prefix, "subdir")
	if err := os.MkdirAll(sub1Prefix, 0755); err != nil {
		t.Fatalf("Failed to create subdir: %v", err)
	}
	if err := os.MkdirAll(sub2Prefix, 0755); err != nil {
		t.Fatalf("Failed to create subdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(sub1Prefix, "subfile.txt"), sub1FileData, 0644); err != nil {
		t.Fatalf("Failed to write file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(sub2Prefix, "subfile.txt"), sub2FileData, 0644); err != nil {
		t.Fatalf("Failed to write file: %v", err)
	}

	// Run sender and receiver
	senderDone := make(chan error, 1)
	receiverDone := make(chan struct {
		manifest manifest.Manifest
		err      error
	}, 1)

	// Start sender - use combinedRoot which matches the manifest RelPaths
	go func() {
		err := SendManifest(ctx, stream1, combinedRoot, m, 0, 0, 0, nil, nil)
		senderDone <- err
	}()

	// Start receiver
	go func() {
		receivedManifest, err := RecvManifest(ctx, stream2, outputDir, nil)
		receiverDone <- struct {
			manifest manifest.Manifest
			err      error
		}{manifest: receivedManifest, err: err}
	}()

	// Wait for both to complete
	select {
	case senderErr := <-senderDone:
		if senderErr != nil {
			t.Fatalf("SendManifest error: %v", senderErr)
		}
	case <-ctx.Done():
		t.Fatalf("Sender timed out: %v", ctx.Err())
	}

	select {
	case receiverResult := <-receiverDone:
		if receiverResult.err != nil {
			t.Fatalf("RecvManifest error: %v", receiverResult.err)
		}

		receivedManifest := receiverResult.manifest

		// Verify manifest matches
		if receivedManifest.FileCount != m.FileCount {
			t.Errorf("Manifest FileCount = %d, want %d", receivedManifest.FileCount, m.FileCount)
		}

		// Verify all files exist and match
		outputRoot := filepath.Join(outputDir, "selection")
		for _, item := range m.Items {
			if !item.IsDir {
				outputPath := filepath.Join(outputRoot, filepath.FromSlash(item.RelPath))
				if _, err := os.Stat(outputPath); err != nil {
					t.Errorf("Output file does not exist: %s", outputPath)
					continue
				}

				// Determine which source file this should match
				var expectedData []byte
				if strings.Contains(item.RelPath, "1_") {
					if strings.Contains(item.RelPath, "samefile.txt") {
						expectedData = file1Data
					} else if strings.Contains(item.RelPath, "subfile.txt") {
						expectedData = sub1FileData
					}
				} else if strings.Contains(item.RelPath, "2_") {
					if strings.Contains(item.RelPath, "samefile.txt") {
						expectedData = file2Data
					} else if strings.Contains(item.RelPath, "subfile.txt") {
						expectedData = sub2FileData
					}
				}

				if expectedData != nil {
					outputData, err := os.ReadFile(outputPath)
					if err != nil {
						t.Errorf("Failed to read output file %s: %v", outputPath, err)
						continue
					}
					if !reflect.DeepEqual(outputData, expectedData) {
						t.Errorf("Output file %s does not match expected content", outputPath)
					}
				}
			}
		}
	case <-ctx.Done():
		t.Fatalf("Receiver timed out: %v", ctx.Err())
	}
}

func TestSendManifest_RecvManifest_NonDefaultChunkSize(t *testing.T) {
	// Test with a non-default chunk size (64KB)
	tempDir, err := os.MkdirTemp("", "manifestproto_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create source tree with a file larger than chunk size
	srcDir := filepath.Join(tempDir, "src")
	if err := os.MkdirAll(srcDir, 0755); err != nil {
		t.Fatalf("Failed to create src dir: %v", err)
	}

	// Create a file larger than 64KB to test chunking
	largeFilePath := filepath.Join(srcDir, "large.bin")
	largeFileData := make([]byte, 100*1024) // 100KB
	if _, err := rand.Read(largeFileData); err != nil {
		t.Fatalf("Failed to generate random data: %v", err)
	}
	if err := os.WriteFile(largeFilePath, largeFileData, 0644); err != nil {
		t.Fatalf("Failed to write large file: %v", err)
	}

	// Build manifest
	m, err := manifest.Scan(srcDir)
	if err != nil {
		t.Fatalf("Failed to scan directory: %v", err)
	}

	// Create output directory
	outputDir := filepath.Join(tempDir, "output")
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		t.Fatalf("Failed to create output dir: %v", err)
	}

	// Create mock transport pair
	t1, t2 := NewMockPair()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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

	// Use 64KB chunk size (non-default) and window > 1
	chunkSize := uint32(64 * 1024)
	windowSize := uint32(4) // Window > 1 to test windowed pipeline

	// Run sender and receiver in parallel
	senderDone := make(chan error, 1)
	receiverDone := make(chan struct {
		manifest manifest.Manifest
		err      error
	}, 1)

	// Start sender with non-default chunk size and window
	go func() {
		err := SendManifest(ctx, stream1, srcDir, m, chunkSize, windowSize, 0, nil, nil)
		senderDone <- err
	}()

	// Start receiver
	go func() {
		receivedManifest, err := RecvManifest(ctx, stream2, outputDir, nil)
		receiverDone <- struct {
			manifest manifest.Manifest
			err      error
		}{manifest: receivedManifest, err: err}
	}()

	// Wait for both to complete
	var senderErr error
	var receiverResult struct {
		manifest manifest.Manifest
		err      error
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
		t.Fatalf("SendManifest error: %v", senderErr)
	}
	if receiverResult.err != nil {
		t.Fatalf("RecvManifest error: %v", receiverResult.err)
	}

	// Verify output file exists and matches byte-for-byte
	outputFilePath := filepath.Join(outputDir, m.Root, "large.bin")
	if _, err := os.Stat(outputFilePath); err != nil {
		t.Fatalf("Output file does not exist: %v", err)
	}
	outputData, err := os.ReadFile(outputFilePath)
	if err != nil {
		t.Fatalf("Failed to read output file: %v", err)
	}
	if !reflect.DeepEqual(outputData, largeFileData) {
		t.Error("Output file does not match input byte-for-byte")
	}
}

func TestSendManifest_RecvManifest_DelayedACKs(t *testing.T) {
	// Test that sender does not deadlock if ACKs are delayed
	// Simulate receiver ACK every 32 chunks (much less frequent than default 8)
	tempDir, err := os.MkdirTemp("", "manifestproto_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create source tree with a file large enough to require multiple chunks
	srcDir := filepath.Join(tempDir, "src")
	if err := os.MkdirAll(srcDir, 0755); err != nil {
		t.Fatalf("Failed to create src dir: %v", err)
	}

	// Create a file that will require many chunks (e.g., 1MB with 64KB chunks = 16 chunks)
	largeFilePath := filepath.Join(srcDir, "large.bin")
	largeFileData := make([]byte, 1024*1024) // 1MB
	if _, err := rand.Read(largeFileData); err != nil {
		t.Fatalf("Failed to generate random data: %v", err)
	}
	if err := os.WriteFile(largeFilePath, largeFileData, 0644); err != nil {
		t.Fatalf("Failed to write large file: %v", err)
	}

	// Build manifest
	m, err := manifest.Scan(srcDir)
	if err != nil {
		t.Fatalf("Failed to scan directory: %v", err)
	}

	// Create output directory
	outputDir := filepath.Join(tempDir, "output")
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		t.Fatalf("Failed to create output dir: %v", err)
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

	// Use small chunk size and window to test delayed ACKs
	chunkSize := uint32(64 * 1024) // 64KB
	windowSize := uint32(8)        // Window of 8 chunks

	// Run sender and receiver in parallel
	senderDone := make(chan error, 1)
	receiverDone := make(chan struct {
		manifest manifest.Manifest
		err      error
	}, 1)

	// Start sender
	go func() {
		err := SendManifest(ctx, stream1, srcDir, m, chunkSize, windowSize, 0, nil, nil)
		senderDone <- err
	}()

	// Start receiver (will ACK every 8 chunks by default, which should work with window of 8)
	go func() {
		receivedManifest, err := RecvManifest(ctx, stream2, outputDir, nil)
		receiverDone <- struct {
			manifest manifest.Manifest
			err      error
		}{manifest: receivedManifest, err: err}
	}()

	// Wait for both to complete
	var senderErr error
	var receiverResult struct {
		manifest manifest.Manifest
		err      error
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
		t.Fatalf("SendManifest error: %v", senderErr)
	}
	if receiverResult.err != nil {
		t.Fatalf("RecvManifest error: %v", receiverResult.err)
	}

	// Verify output file exists and matches byte-for-byte
	outputFilePath := filepath.Join(outputDir, m.Root, "large.bin")
	if _, err := os.Stat(outputFilePath); err != nil {
		t.Fatalf("Output file does not exist: %v", err)
	}
	outputData, err := os.ReadFile(outputFilePath)
	if err != nil {
		t.Fatalf("Failed to read output file: %v", err)
	}
	if !reflect.DeepEqual(outputData, largeFileData) {
		t.Error("Output file does not match input byte-for-byte")
	}
}

func TestRecvManifest_InvalidMagic(t *testing.T) {
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

	tempDir, err := os.MkdirTemp("", "manifestproto_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Start RecvManifest in goroutine
	recvDone := make(chan error, 1)
	go func() {
		_, err := RecvManifest(ctx, stream2, tempDir, nil)
		recvDone <- err
	}()

	// Give RecvManifest a moment to start reading
	time.Sleep(10 * time.Millisecond)

	// Write invalid magic (exactly 4 bytes)
	stream1.Write([]byte("INVA"))
	stream1.Close()

	// Wait for RecvManifest to complete
	select {
	case err := <-recvDone:
		if err != ErrInvalidManifestMagic {
			t.Errorf("Expected ErrInvalidManifestMagic, got: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("Test timed out: %v", ctx.Err())
	}
}

