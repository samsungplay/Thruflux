package transfer

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSidecarRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "root", ".sheerbytes", "abcd.sbxmap")
	sc, err := LoadOrCreateSidecar(path, "abcd", 1024, 256)
	if err != nil {
		t.Fatalf("create sidecar: %v", err)
	}
	sc.MarkComplete(2)
	sc.MarkComplete(3)
	if err := sc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	sc2, err := LoadSidecar(path)
	if err != nil {
		t.Fatalf("load sidecar: %v", err)
	}
	if !sc2.IsComplete(2) || !sc2.IsComplete(3) {
		t.Fatalf("expected chunks marked complete")
	}
	if sc2.IsComplete(1) {
		t.Fatalf("unexpected chunk marked complete")
	}
}

func TestSidecarCorrupt(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "root", ".sheerbytes", "abcd.sbxmap")
	sc, err := CreateSidecar(path, "abcd", 1024, 256)
	if err != nil {
		t.Fatalf("create sidecar: %v", err)
	}
	if err := sc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	if err := os.WriteFile(path, []byte("corrupt"), 0644); err != nil {
		t.Fatalf("corrupt sidecar: %v", err)
	}

	sc2, err := LoadOrCreateSidecar(path, "abcd", 1024, 256)
	if err != nil {
		t.Fatalf("recreate sidecar: %v", err)
	}
	if sc2.IsComplete(0) {
		t.Fatalf("expected new sidecar to be empty")
	}
}
