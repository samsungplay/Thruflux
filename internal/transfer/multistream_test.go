package transfer

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sheerbytes/sheerbytes/pkg/manifest"
)

func TestSendRecvManifestMultiStream_ParallelFiles(t *testing.T) {
	srcDir, err := os.MkdirTemp("", "multistream_src_*")
	if err != nil {
		t.Fatalf("MkdirTemp src: %v", err)
	}
	defer os.RemoveAll(srcDir)

	if err := os.MkdirAll(filepath.Join(srcDir, "dir1"), 0755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	file1Path := filepath.Join(srcDir, "dir1", "file1.txt")
	file2Path := filepath.Join(srcDir, "file2.bin")
	if err := os.WriteFile(file1Path, []byte("hello multistream"), 0644); err != nil {
		t.Fatalf("WriteFile file1: %v", err)
	}
	if err := os.WriteFile(file2Path, []byte("more data here"), 0644); err != nil {
		t.Fatalf("WriteFile file2: %v", err)
	}

	m, err := manifest.Scan(srcDir)
	if err != nil {
		t.Fatalf("manifest.Scan: %v", err)
	}

	outputDir, err := os.MkdirTemp("", "multistream_out_*")
	if err != nil {
		t.Fatalf("MkdirTemp out: %v", err)
	}
	defer os.RemoveAll(outputDir)

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
		_, err := RecvManifestMultiStream(ctx, receiverConn, outputDir, Options{
			ParallelFiles: 2,
		})
		recvErrCh <- err
	}()

	sendErr := SendManifestMultiStream(ctx, senderConn, srcDir, m, Options{
		ParallelFiles: 2,
	})
	if sendErr != nil {
		t.Fatalf("SendManifestMultiStream: %v", sendErr)
	}

	if recvErr := <-recvErrCh; recvErr != nil {
		t.Fatalf("RecvManifestMultiStream: %v", recvErr)
	}

	rootOut := filepath.Join(outputDir, m.Root)
	got1, err := os.ReadFile(filepath.Join(rootOut, "dir1", "file1.txt"))
	if err != nil {
		t.Fatalf("ReadFile file1 out: %v", err)
	}
	if string(got1) != "hello multistream" {
		t.Fatalf("file1 content mismatch: %q", string(got1))
	}
	got2, err := os.ReadFile(filepath.Join(rootOut, "file2.bin"))
	if err != nil {
		t.Fatalf("ReadFile file2 out: %v", err)
	}
	if string(got2) != "more data here" {
		t.Fatalf("file2 content mismatch: %q", string(got2))
	}
}
