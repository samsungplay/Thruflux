package manifest

import (
	"os"
	"path/filepath"
	"sort"
	"testing"
)

func TestScanPaths_Basic(t *testing.T) {
	tmpDir := t.TempDir()

	// Create test structure:
	// root/
	//   a.txt
	//   dir1/
	//     x.txt
	//   dir2/
	//     y.txt
	rootDir := tmpDir
	aPath := filepath.Join(rootDir, "a.txt")
	dir1Path := filepath.Join(rootDir, "dir1")
	dir2Path := filepath.Join(rootDir, "dir2")
	xPath := filepath.Join(dir1Path, "x.txt")
	yPath := filepath.Join(dir2Path, "y.txt")

	if err := os.WriteFile(aPath, []byte("file a"), 0644); err != nil {
		t.Fatalf("failed to create a.txt: %v", err)
	}
	if err := os.Mkdir(dir1Path, 0755); err != nil {
		t.Fatalf("failed to create dir1: %v", err)
	}
	if err := os.WriteFile(xPath, []byte("file x"), 0644); err != nil {
		t.Fatalf("failed to create dir1/x.txt: %v", err)
	}
	if err := os.Mkdir(dir2Path, 0755); err != nil {
		t.Fatalf("failed to create dir2: %v", err)
	}
	if err := os.WriteFile(yPath, []byte("file y"), 0644); err != nil {
		t.Fatalf("failed to create dir2/y.txt: %v", err)
	}

	manifest, err := ScanPaths([]string{aPath, dir1Path, dir2Path})
	if err != nil {
		t.Fatalf("ScanPaths() error = %v", err)
	}

	// Verify root
	if manifest.Root != "selection" {
		t.Errorf("Root = %s, want 'selection'", manifest.Root)
	}

	// Verify items
	expectedItems := []string{
		"a.txt",
		"dir1",
		"dir1/x.txt",
		"dir2",
		"dir2/y.txt",
	}

	actualPaths := make([]string, len(manifest.Items))
	for i, item := range manifest.Items {
		actualPaths[i] = item.RelPath
	}
	sort.Strings(actualPaths)

	if len(actualPaths) != len(expectedItems) {
		t.Fatalf("Item count = %d, want %d. Items: %v", len(actualPaths), len(expectedItems), actualPaths)
	}

	for i, expected := range expectedItems {
		if actualPaths[i] != expected {
			t.Errorf("Item[%d] = %s, want %s", i, actualPaths[i], expected)
		}
	}

	// Verify counts
	expectedFileCount := 3 // a.txt, x.txt, y.txt
	if manifest.FileCount != expectedFileCount {
		t.Errorf("FileCount = %d, want %d", manifest.FileCount, expectedFileCount)
	}

	expectedFolderCount := 2 // dir1, dir2
	if manifest.FolderCount != expectedFolderCount {
		t.Errorf("FolderCount = %d, want %d", manifest.FolderCount, expectedFolderCount)
	}

	expectedTotalBytes := int64(6 + 6 + 6) // "file a" + "file x" + "file y"
	if manifest.TotalBytes != expectedTotalBytes {
		t.Errorf("TotalBytes = %d, want %d", manifest.TotalBytes, expectedTotalBytes)
	}

	// Verify deterministic ordering
	manifest2, err := ScanPaths([]string{aPath, dir1Path, dir2Path})
	if err != nil {
		t.Fatalf("ScanPaths() error = %v", err)
	}

	if len(manifest2.Items) != len(manifest.Items) {
		t.Errorf("Second scan item count = %d, want %d", len(manifest2.Items), len(manifest.Items))
	}

	for i := range manifest.Items {
		if manifest.Items[i].RelPath != manifest2.Items[i].RelPath {
			t.Errorf("Item[%d] RelPath not deterministic: %s vs %s", i, manifest.Items[i].RelPath, manifest2.Items[i].RelPath)
		}
	}
}

func TestScanPaths_Collision(t *testing.T) {
	tmpDir := t.TempDir()

	// Create two different folders with the same name in different parents
	parent1 := filepath.Join(tmpDir, "parent1")
	parent2 := filepath.Join(tmpDir, "parent2")
	sameName1 := filepath.Join(parent1, "same")
	sameName2 := filepath.Join(parent2, "same")

	if err := os.MkdirAll(sameName1, 0755); err != nil {
		t.Fatalf("failed to create same1: %v", err)
	}
	if err := os.MkdirAll(sameName2, 0755); err != nil {
		t.Fatalf("failed to create same2: %v", err)
	}

	// Add files to each
	file1 := filepath.Join(sameName1, "file1.txt")
	file2 := filepath.Join(sameName2, "file2.txt")
	if err := os.WriteFile(file1, []byte("content1"), 0644); err != nil {
		t.Fatalf("failed to create file1: %v", err)
	}
	if err := os.WriteFile(file2, []byte("content2"), 0644); err != nil {
		t.Fatalf("failed to create file2: %v", err)
	}

	manifest, err := ScanPaths([]string{sameName1, sameName2})
	if err != nil {
		t.Fatalf("ScanPaths() error = %v", err)
	}

	// Verify both directories are present with ordinal prefixes
	relPaths := make([]string, len(manifest.Items))
	for i, item := range manifest.Items {
		relPaths[i] = item.RelPath
	}

	// Check for disambiguation
	found1Same := false
	found2Same := false
	for _, relPath := range relPaths {
		if relPath == "1_same" || relPath == "1_same/file1.txt" {
			found1Same = true
		}
		if relPath == "2_same" || relPath == "2_same/file2.txt" {
			found2Same = true
		}
	}

	if !found1Same {
		t.Error("Expected to find '1_same' in paths")
	}
	if !found2Same {
		t.Error("Expected to find '2_same' in paths")
	}

	// Verify no duplicate RelPaths
	seen := make(map[string]bool)
	for _, relPath := range relPaths {
		if seen[relPath] {
			t.Errorf("Duplicate RelPath found: %s", relPath)
		}
		seen[relPath] = true
	}

	// Verify deterministic ordering - paths should be the same on multiple scans
	manifest2, err := ScanPaths([]string{sameName1, sameName2})
	if err != nil {
		t.Fatalf("ScanPaths() error = %v", err)
	}

	if len(manifest2.Items) != len(manifest.Items) {
		t.Errorf("Second scan item count = %d, want %d", len(manifest2.Items), len(manifest.Items))
	}

	for i := range manifest.Items {
		if manifest.Items[i].RelPath != manifest2.Items[i].RelPath {
			t.Errorf("Item[%d] RelPath not deterministic: %s vs %s", i, manifest.Items[i].RelPath, manifest2.Items[i].RelPath)
		}
	}
}

func TestScanPaths_SingleFile(t *testing.T) {
	tmpDir := t.TempDir()

	filePath := filepath.Join(tmpDir, "single.txt")
	if err := os.WriteFile(filePath, []byte("content"), 0644); err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	manifest, err := ScanPaths([]string{filePath})
	if err != nil {
		t.Fatalf("ScanPaths() error = %v", err)
	}

	if manifest.Root != "selection" {
		t.Errorf("Root = %s, want 'selection'", manifest.Root)
	}

	if len(manifest.Items) != 1 {
		t.Fatalf("Items length = %d, want 1", len(manifest.Items))
	}

	item := manifest.Items[0]
	if item.RelPath != "single.txt" {
		t.Errorf("RelPath = %s, want 'single.txt'", item.RelPath)
	}

	if item.IsDir {
		t.Error("IsDir should be false for a file")
	}

	if manifest.FileCount != 1 {
		t.Errorf("FileCount = %d, want 1", manifest.FileCount)
	}

	if manifest.FolderCount != 0 {
		t.Errorf("FolderCount = %d, want 0", manifest.FolderCount)
	}
}

func TestScanPaths_MixedFilesAndDirs(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a file and a directory
	filePath := filepath.Join(tmpDir, "file.txt")
	dirPath := filepath.Join(tmpDir, "dir")
	dirFile := filepath.Join(dirPath, "nested.txt")

	if err := os.WriteFile(filePath, []byte("file"), 0644); err != nil {
		t.Fatalf("failed to create file: %v", err)
	}
	if err := os.Mkdir(dirPath, 0755); err != nil {
		t.Fatalf("failed to create dir: %v", err)
	}
	if err := os.WriteFile(dirFile, []byte("nested"), 0644); err != nil {
		t.Fatalf("failed to create nested file: %v", err)
	}

	manifest, err := ScanPaths([]string{filePath, dirPath})
	if err != nil {
		t.Fatalf("ScanPaths() error = %v", err)
	}

	relPaths := make([]string, len(manifest.Items))
	for i, item := range manifest.Items {
		relPaths[i] = item.RelPath
	}

	// Should have: file.txt, dir, dir/nested.txt
	expected := []string{"dir", "dir/nested.txt", "file.txt"}
	if len(relPaths) != len(expected) {
		t.Fatalf("Item count = %d, want %d. Items: %v", len(relPaths), len(expected), relPaths)
	}

	sort.Strings(relPaths)
	for i, exp := range expected {
		if relPaths[i] != exp {
			t.Errorf("Item[%d] = %s, want %s", i, relPaths[i], exp)
		}
	}
}

