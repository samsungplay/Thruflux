package manifest

import (
	"os"
	"path/filepath"
	"testing"
)

func TestManifestID_Deterministic(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a simple tree
	aPath := filepath.Join(tmpDir, "a.txt")
	if err := os.WriteFile(aPath, []byte("content"), 0644); err != nil {
		t.Fatalf("failed to create a.txt: %v", err)
	}

	bDir := filepath.Join(tmpDir, "b")
	if err := os.Mkdir(bDir, 0755); err != nil {
		t.Fatalf("failed to create b/: %v", err)
	}

	cPath := filepath.Join(bDir, "c.txt")
	if err := os.WriteFile(cPath, []byte("test"), 0644); err != nil {
		t.Fatalf("failed to create b/c.txt: %v", err)
	}

	// Scan multiple times
	manifest1, err := Scan(tmpDir)
	if err != nil {
		t.Fatalf("Scan() error = %v", err)
	}

	manifest2, err := Scan(tmpDir)
	if err != nil {
		t.Fatalf("Scan() error = %v", err)
	}

	// Generate IDs
	id1 := ManifestID(manifest1)
	id2 := ManifestID(manifest2)

	if id1 == "" {
		t.Error("ManifestID should not be empty")
	}

	if id1 != id2 {
		t.Errorf("ManifestID should be deterministic: got %s and %s", id1, id2)
	}

	if len(id1) != 16 {
		t.Errorf("ManifestID length = %d, want 16", len(id1))
	}
}

func TestManifestID_StableAcrossScans(t *testing.T) {
	tmpDir := t.TempDir()

	// Create files
	files := []string{"a.txt", "b.txt", "c.txt"}
	for _, f := range files {
		if err := os.WriteFile(filepath.Join(tmpDir, f), []byte("content"), 0644); err != nil {
			t.Fatalf("failed to create %s: %v", f, err)
		}
	}

	// Scan and get ID
	manifest1, err := Scan(tmpDir)
	if err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	id1 := ManifestID(manifest1)

	// Scan again (should be identical)
	manifest2, err := Scan(tmpDir)
	if err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	id2 := ManifestID(manifest2)

	if id1 != id2 {
		t.Errorf("ManifestID should be stable across scans: got %s and %s", id1, id2)
	}
}

func TestManifestID_EmptyManifest(t *testing.T) {
	tmpDir := t.TempDir()

	manifest, err := Scan(tmpDir)
	if err != nil {
		t.Fatalf("Scan() error = %v", err)
	}

	id := ManifestID(manifest)
	if id != "" {
		t.Errorf("ManifestID for empty manifest = %s, want empty string", id)
	}
}

func TestManifestID_DifferentManifests(t *testing.T) {
	tmpDir1 := t.TempDir()
	tmpDir2 := t.TempDir()

	// Create different files in each directory
	if err := os.WriteFile(filepath.Join(tmpDir1, "file1.txt"), []byte("content1"), 0644); err != nil {
		t.Fatalf("failed to create file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(tmpDir2, "file2.txt"), []byte("content2"), 0644); err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	manifest1, err := Scan(tmpDir1)
	if err != nil {
		t.Fatalf("Scan() error = %v", err)
	}

	manifest2, err := Scan(tmpDir2)
	if err != nil {
		t.Fatalf("Scan() error = %v", err)
	}

	id1 := ManifestID(manifest1)
	id2 := ManifestID(manifest2)

	if id1 == id2 {
		t.Error("Different manifests should have different IDs")
	}
}

func TestScan_SingleFile(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a single file
	filePath := filepath.Join(tmpDir, "single.txt")
	if err := os.WriteFile(filePath, []byte("test content"), 0644); err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	// Scan the file
	manifest, err := Scan(filePath)
	if err != nil {
		t.Fatalf("Scan() error = %v", err)
	}

	// Verify manifest
	if manifest.FileCount != 1 {
		t.Errorf("FileCount = %d, want 1", manifest.FileCount)
	}
	if manifest.FolderCount != 0 {
		t.Errorf("FolderCount = %d, want 0", manifest.FolderCount)
	}
	if manifest.TotalBytes != 12 {
		t.Errorf("TotalBytes = %d, want 12", manifest.TotalBytes)
	}
	if len(manifest.Items) != 1 {
		t.Fatalf("Items length = %d, want 1", len(manifest.Items))
	}

	item := manifest.Items[0]
	if item.RelPath != "single.txt" {
		t.Errorf("RelPath = %s, want single.txt", item.RelPath)
	}
	if item.IsDir {
		t.Error("IsDir should be false for a file")
	}
	if item.Size != 12 {
		t.Errorf("Size = %d, want 12", item.Size)
	}

	// Verify manifest ID
	id := ManifestID(manifest)
	if id == "" {
		t.Error("ManifestID should not be empty")
	}
	if len(id) != 16 {
		t.Errorf("ManifestID length = %d, want 16", len(id))
	}
}

