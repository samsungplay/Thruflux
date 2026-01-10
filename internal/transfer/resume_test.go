package transfer

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sheerbytes/sheerbytes/pkg/manifest"
)

func firstManifestFile(m manifest.Manifest) manifest.FileItem {
	for _, item := range m.Items {
		if !item.IsDir {
			return item
		}
	}
	return manifest.FileItem{}
}

func preparePartialFile(t *testing.T, srcDir string, m manifest.Manifest, outDir string, chunkSize uint32, completeChunks int, corrupt bool) string {
	t.Helper()
	item := firstManifestFile(m)
	if item.RelPath == "" {
		t.Fatal("missing file item in manifest")
	}

	data, err := os.ReadFile(filepath.Join(srcDir, item.RelPath))
	if err != nil {
		t.Fatalf("read source file: %v", err)
	}

	filePath := filepath.Join(outDir, m.Root, filepath.FromSlash(item.RelPath))
	if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	f, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("open partial file: %v", err)
	}
	defer f.Close()

	for i := 0; i < completeChunks; i++ {
		start := int64(i) * int64(chunkSize)
		if start >= int64(len(data)) {
			break
		}
		end := start + int64(chunkSize)
		if end > int64(len(data)) {
			end = int64(len(data))
		}
		chunk := append([]byte{}, data[start:end]...)
		if corrupt && i == completeChunks-1 && len(chunk) > 0 {
			chunk[len(chunk)-1] ^= 0xFF
		}
		if _, err := f.WriteAt(chunk, start); err != nil {
			t.Fatalf("write partial chunk: %v", err)
		}
	}
	if err := f.Truncate(item.Size); err != nil {
		t.Fatalf("truncate partial: %v", err)
	}

	sidecarPath := SidecarPath(outDir, m.Root, sidecarIdentifier(item))
	sc, err := LoadOrCreateSidecar(sidecarPath, item.ID, item.Size, chunkSize)
	if err != nil {
		t.Fatalf("prepare sidecar: %v", err)
	}
	for i := 0; i < completeChunks; i++ {
		sc.MarkComplete(uint32(i))
	}
	if err := sc.Flush(); err != nil {
		t.Fatalf("flush sidecar: %v", err)
	}

	return filePath
}

func TestResumeSkipsChunks(t *testing.T) {
	const chunkSize = 64
	srcDir := t.TempDir()
	outDir := t.TempDir()

	data := bytes.Repeat([]byte("a"), chunkSize*3+5)
	srcPath := filepath.Join(srcDir, "file.bin")
	if err := os.WriteFile(srcPath, data, 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	m, err := manifest.Scan(srcDir)
	if err != nil {
		t.Fatalf("manifest.Scan: %v", err)
	}

	partialPath := preparePartialFile(t, srcDir, m, outDir, chunkSize, 2, false)

	t1, t2 := NewMockPair()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	senderConn, err := t1.Dial(ctx, "peer2")
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	receiverConn, err := t2.Accept(ctx)
	if err != nil {
		t.Fatalf("Accept: %v", err)
	}

	recvErrCh := make(chan error, 1)
	go func() {
		_, err := RecvManifestMultiStream(ctx, receiverConn, outDir, Options{
			ParallelFiles:    1,
			Resume:           true,
			ResumeVerify:     "last",
			ResumeVerifyTail: 1,
		})
		recvErrCh <- err
	}()

	type stats struct {
		skipped  uint32
		total    uint32
		verified uint32
	}
	statsCh := make(chan stats, 1)

	sendErr := SendManifestMultiStream(ctx, senderConn, srcDir, m, Options{
		ChunkSize:        chunkSize,
		ParallelFiles:    1,
		Resume:           true,
		ResumeVerify:     "last",
		HashAlg:          "crc32c",
		ResumeVerifyTail: 1,
		ResumeStatsFn: func(relpath string, skippedChunks, totalChunks uint32, verifiedChunk uint32, totalBytes int64, chunkSize uint32) {
			statsCh <- stats{skipped: skippedChunks, total: totalChunks, verified: verifiedChunk}
		},
	})
	if sendErr != nil {
		t.Fatalf("SendManifestMultiStream: %v", sendErr)
	}

	if recvErr := <-recvErrCh; recvErr != nil {
		t.Fatalf("RecvManifestMultiStream: %v", recvErr)
	}

	got, err := os.ReadFile(partialPath)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("resume output mismatch")
	}

	select {
	case st := <-statsCh:
		if st.total == 0 || st.skipped != 1 {
			t.Fatalf("resume stats mismatch: skipped=%d total=%d", st.skipped, st.total)
		}
	default:
		t.Fatalf("expected resume stats")
	}
}

func TestResumeResendsCorruptChunk(t *testing.T) {
	const chunkSize = 32
	srcDir := t.TempDir()
	outDir := t.TempDir()

	data := bytes.Repeat([]byte("b"), chunkSize*2+10)
	srcPath := filepath.Join(srcDir, "file.bin")
	if err := os.WriteFile(srcPath, data, 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	m, err := manifest.Scan(srcDir)
	if err != nil {
		t.Fatalf("manifest.Scan: %v", err)
	}

	partialPath := preparePartialFile(t, srcDir, m, outDir, chunkSize, 2, true)

	t1, t2 := NewMockPair()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	senderConn, err := t1.Dial(ctx, "peer2")
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	receiverConn, err := t2.Accept(ctx)
	if err != nil {
		t.Fatalf("Accept: %v", err)
	}

	recvErrCh := make(chan error, 1)
	go func() {
		_, err := RecvManifestMultiStream(ctx, receiverConn, outDir, Options{
			ParallelFiles:    1,
			Resume:           true,
			ResumeVerify:     "last",
			ResumeVerifyTail: 1,
		})
		recvErrCh <- err
	}()

	statsCh := make(chan uint32, 1)
	sendErr := SendManifestMultiStream(ctx, senderConn, srcDir, m, Options{
		ChunkSize:        chunkSize,
		ParallelFiles:    1,
		Resume:           true,
		ResumeVerify:     "last",
		HashAlg:          "crc32c",
		ResumeVerifyTail: 1,
		ResumeStatsFn: func(relpath string, skippedChunks, totalChunks uint32, verifiedChunk uint32, totalBytes int64, chunkSize uint32) {
			statsCh <- skippedChunks
		},
	})
	if sendErr != nil {
		t.Fatalf("SendManifestMultiStream: %v", sendErr)
	}

	if recvErr := <-recvErrCh; recvErr != nil {
		t.Fatalf("RecvManifestMultiStream: %v", recvErr)
	}

	got, err := os.ReadFile(partialPath)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("resume output mismatch")
	}

	select {
	case skipped := <-statsCh:
		if skipped != 0 {
			t.Fatalf("expected zero skipped chunks, got %d", skipped)
		}
	default:
		t.Fatalf("expected resume stats")
	}
}
