package manifest

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestScan_SimpleTree(t *testing.T) {
	// Create a temporary directory structure
	tmpDir := t.TempDir()

	// Create the tree:
	// root/
	//   a.txt (10 bytes)
	//   b/
	//     c.txt (5 bytes)
	aPath := filepath.Join(tmpDir, "a.txt")
	if err := os.WriteFile(aPath, []byte("0123456789"), 0644); err != nil {
		t.Fatalf("failed to create a.txt: %v", err)
	}

	bDir := filepath.Join(tmpDir, "b")
	if err := os.Mkdir(bDir, 0755); err != nil {
		t.Fatalf("failed to create b/: %v", err)
	}

	cPath := filepath.Join(bDir, "c.txt")
	if err := os.WriteFile(cPath, []byte("01234"), 0644); err != nil {
		t.Fatalf("failed to create b/c.txt: %v", err)
	}

	// Scan the directory
	manifest, err := Scan(tmpDir)
	if err != nil {
		t.Fatalf("Scan() error = %v", err)
	}

	// Verify root name
	expectedRoot := filepath.Base(tmpDir)
	if manifest.Root != expectedRoot {
		t.Errorf("Root = %s, want %s", manifest.Root, expectedRoot)
	}

	// Verify counts
	if manifest.FileCount != 2 {
		t.Errorf("FileCount = %d, want 2", manifest.FileCount)
	}
	if manifest.FolderCount != 1 {
		t.Errorf("FolderCount = %d, want 1", manifest.FolderCount)
	}
	if manifest.TotalBytes != 15 {
		t.Errorf("TotalBytes = %d, want 15", manifest.TotalBytes)
	}

	// Verify items are present and correctly ordered
	if len(manifest.Items) != 3 {
		t.Fatalf("Items length = %d, want 3", len(manifest.Items))
	}

	// Check deterministic ordering (sorted by RelPath)
	expectedItems := []FileItem{
		{RelPath: "a.txt", Size: 10, IsDir: false},
		{RelPath: "b", Size: 0, IsDir: true},
		{RelPath: "b/c.txt", Size: 5, IsDir: false},
	}

	for i, item := range manifest.Items {
		expected := expectedItems[i]
		if item.RelPath != expected.RelPath {
			t.Errorf("Items[%d].RelPath = %s, want %s", i, item.RelPath, expected.RelPath)
		}
		if item.Size != expected.Size {
			t.Errorf("Items[%d].Size = %d, want %d", i, item.Size, expected.Size)
		}
		if item.IsDir != expected.IsDir {
			t.Errorf("Items[%d].IsDir = %v, want %v", i, item.IsDir, expected.IsDir)
		}
		if item.ModTime <= 0 {
			t.Errorf("Items[%d].ModTime = %d, want > 0", i, item.ModTime)
		}
	}

	// Verify relative paths use forward slashes
	for _, item := range manifest.Items {
		if filepath.Separator == '\\' {
			// On Windows, verify no backslashes in RelPath
			for _, r := range item.RelPath {
				if r == '\\' {
					t.Errorf("Item %s contains backslash, should use forward slashes", item.RelPath)
					break
				}
			}
		}
	}
}

func TestScan_NonExistentPath(t *testing.T) {
	tmpDir := t.TempDir()
	nonExistent := filepath.Join(tmpDir, "does-not-exist")

	_, err := Scan(nonExistent)
	if err == nil {
		t.Fatal("Scan() expected error for non-existent path, got nil")
	}

	if err.Error()[:4] != "path" {
		t.Errorf("Scan() error = %v, want error about path not existing", err)
	}
}

func TestScan_DeterministicOrdering(t *testing.T) {
	tmpDir := t.TempDir()

	// Create files in non-alphabetical order to verify sorting
	files := []string{"z.txt", "a.txt", "m.txt", "1.txt"}
	for _, f := range files {
		path := filepath.Join(tmpDir, f)
		if err := os.WriteFile(path, []byte("test"), 0644); err != nil {
			t.Fatalf("failed to create %s: %v", f, err)
		}
	}

	manifest, err := Scan(tmpDir)
	if err != nil {
		t.Fatalf("Scan() error = %v", err)
	}

	// Verify items are sorted lexicographically
	expectedOrder := []string{"1.txt", "a.txt", "m.txt", "z.txt"}
	if len(manifest.Items) != len(expectedOrder) {
		t.Fatalf("Items length = %d, want %d", len(manifest.Items), len(expectedOrder))
	}

	for i, item := range manifest.Items {
		if item.RelPath != expectedOrder[i] {
			t.Errorf("Items[%d].RelPath = %s, want %s", i, item.RelPath, expectedOrder[i])
		}
	}
}

func TestScan_NestedDirectories(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a more complex tree:
	// root/
	//   dir1/
	//     file1.txt
	//   dir2/
	//     nested/
	//       file2.txt
	//   file3.txt

	dir1 := filepath.Join(tmpDir, "dir1")
	if err := os.Mkdir(dir1, 0755); err != nil {
		t.Fatalf("failed to create dir1: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir1, "file1.txt"), []byte("content1"), 0644); err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	dir2 := filepath.Join(tmpDir, "dir2")
	if err := os.Mkdir(dir2, 0755); err != nil {
		t.Fatalf("failed to create dir2: %v", err)
	}
	nested := filepath.Join(dir2, "nested")
	if err := os.Mkdir(nested, 0755); err != nil {
		t.Fatalf("failed to create nested: %v", err)
	}
	if err := os.WriteFile(filepath.Join(nested, "file2.txt"), []byte("content2"), 0644); err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	if err := os.WriteFile(filepath.Join(tmpDir, "file3.txt"), []byte("content3"), 0644); err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	manifest, err := Scan(tmpDir)
	if err != nil {
		t.Fatalf("Scan() error = %v", err)
	}

	// Verify counts
	if manifest.FileCount != 3 {
		t.Errorf("FileCount = %d, want 3", manifest.FileCount)
	}
	if manifest.FolderCount != 3 {
		t.Errorf("FolderCount = %d, want 3", manifest.FolderCount)
	}
	if manifest.TotalBytes != 24 { // 8 + 8 + 8 bytes ("content1" + "content2" + "content3")
		t.Errorf("TotalBytes = %d, want 24", manifest.TotalBytes)
	}

	// Verify all items are present and sorted
	expectedPaths := []string{
		"dir1",
		"dir1/file1.txt",
		"dir2",
		"dir2/nested",
		"dir2/nested/file2.txt",
		"file3.txt",
	}

	if len(manifest.Items) != len(expectedPaths) {
		t.Fatalf("Items length = %d, want %d", len(manifest.Items), len(expectedPaths))
	}

	for i, item := range manifest.Items {
		if item.RelPath != expectedPaths[i] {
			t.Errorf("Items[%d].RelPath = %s, want %s", i, item.RelPath, expectedPaths[i])
		}
	}

	// Verify directories have size 0 and IsDir=true
	for _, item := range manifest.Items {
		isDir := item.RelPath == "dir1" || item.RelPath == "dir2" || item.RelPath == "dir2/nested"
		if item.IsDir != isDir {
			t.Errorf("Item %s: IsDir = %v, want %v", item.RelPath, item.IsDir, isDir)
		}
		if item.IsDir && item.Size != 0 {
			t.Errorf("Item %s: directory should have size 0, got %d", item.RelPath, item.Size)
		}
	}
}

func TestScan_EmptyDirectory(t *testing.T) {
	tmpDir := t.TempDir()

	manifest, err := Scan(tmpDir)
	if err != nil {
		t.Fatalf("Scan() error = %v", err)
	}

	if len(manifest.Items) != 0 {
		t.Errorf("Items length = %d, want 0", len(manifest.Items))
	}
	if manifest.FileCount != 0 {
		t.Errorf("FileCount = %d, want 0", manifest.FileCount)
	}
	if manifest.FolderCount != 0 {
		t.Errorf("FolderCount = %d, want 0", manifest.FolderCount)
	}
	if manifest.TotalBytes != 0 {
		t.Errorf("TotalBytes = %d, want 0", manifest.TotalBytes)
	}
}

func TestScan_RootName(t *testing.T) {
	tmpDir := t.TempDir()

	manifest, err := Scan(tmpDir)
	if err != nil {
		t.Fatalf("Scan() error = %v", err)
	}

	// Root should be the base name of the temp directory
	expectedRoot := filepath.Base(tmpDir)
	if manifest.Root != expectedRoot {
		t.Errorf("Root = %s, want %s", manifest.Root, expectedRoot)
	}
	if manifest.Root == "" {
		t.Error("Root should not be empty")
	}
}

func TestScan_RelativePaths(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a simple structure
	subDir := filepath.Join(tmpDir, "subdir")
	if err := os.Mkdir(subDir, 0755); err != nil {
		t.Fatalf("failed to create subdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(subDir, "file.txt"), []byte("test"), 0644); err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	manifest, err := Scan(tmpDir)
	if err != nil {
		t.Fatalf("Scan() error = %v", err)
	}

	// Verify relative paths don't start with .. or absolute paths
	for _, item := range manifest.Items {
		if item.RelPath == "." || item.RelPath == ".." {
			t.Errorf("Item should not have RelPath = %s", item.RelPath)
		}
		if filepath.IsAbs(item.RelPath) {
			t.Errorf("Item RelPath should be relative: %s", item.RelPath)
		}
	}
}

func TestScan_ConsistentResults(t *testing.T) {
	tmpDir := t.TempDir()

	// Create files
	files := []string{"a.txt", "b.txt", "c.txt"}
	for _, f := range files {
		if err := os.WriteFile(filepath.Join(tmpDir, f), []byte("content"), 0644); err != nil {
			t.Fatalf("failed to create %s: %v", f, err)
		}
	}

	// Scan multiple times and verify consistent results
	manifest1, err := Scan(tmpDir)
	if err != nil {
		t.Fatalf("Scan() error = %v", err)
	}

	manifest2, err := Scan(tmpDir)
	if err != nil {
		t.Fatalf("Scan() error = %v", err)
	}

	if !reflect.DeepEqual(manifest1.Items, manifest2.Items) {
		t.Error("Multiple scans should produce identical results")
		t.Errorf("First scan: %+v", manifest1.Items)
		t.Errorf("Second scan: %+v", manifest2.Items)
	}
}

func TestScan_ItemIDs(t *testing.T) {
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

	manifest, err := Scan(tmpDir)
	if err != nil {
		t.Fatalf("Scan() error = %v", err)
	}

	// Assert IDs are non-empty for all items
	if len(manifest.Items) == 0 {
		t.Fatal("Expected items, got none")
	}

	for _, item := range manifest.Items {
		if item.ID == "" {
			t.Errorf("Item %s has empty ID", item.RelPath)
		}
		if len(item.ID) != 16 {
			t.Errorf("Item %s has ID length %d, want 16", item.RelPath, len(item.ID))
		}
	}

	// Assert directories also get IDs
	var dirFound bool
	for _, item := range manifest.Items {
		if item.IsDir {
			dirFound = true
			if item.ID == "" {
				t.Errorf("Directory %s has empty ID", item.RelPath)
			}
		}
	}
	if !dirFound {
		t.Error("Expected at least one directory in test tree")
	}
}

func TestScan_StableIDs(t *testing.T) {
	tmpDir := t.TempDir()

	// Create files
	aPath := filepath.Join(tmpDir, "a.txt")
	if err := os.WriteFile(aPath, []byte("content"), 0644); err != nil {
		t.Fatalf("failed to create a.txt: %v", err)
	}

	bPath := filepath.Join(tmpDir, "b.txt")
	if err := os.WriteFile(bPath, []byte("other"), 0644); err != nil {
		t.Fatalf("failed to create b.txt: %v", err)
	}

	// Scan twice
	manifest1, err := Scan(tmpDir)
	if err != nil {
		t.Fatalf("Scan() error = %v", err)
	}

	manifest2, err := Scan(tmpDir)
	if err != nil {
		t.Fatalf("Scan() error = %v", err)
	}

	// Assert same tree yields identical IDs
	if len(manifest1.Items) != len(manifest2.Items) {
		t.Fatalf("Item count mismatch: %d vs %d", len(manifest1.Items), len(manifest2.Items))
	}

	for i := range manifest1.Items {
		if manifest1.Items[i].ID != manifest2.Items[i].ID {
			t.Errorf("Item %s: ID changed between scans: %s -> %s",
				manifest1.Items[i].RelPath, manifest1.Items[i].ID, manifest2.Items[i].ID)
		}
		if manifest1.Items[i].RelPath != manifest2.Items[i].RelPath {
			t.Errorf("Item order changed: %s vs %s", manifest1.Items[i].RelPath, manifest2.Items[i].RelPath)
		}
	}
}

func TestScan_IDChangesWithSize(t *testing.T) {
	tmpDir := t.TempDir()

	filePath := filepath.Join(tmpDir, "test.txt")

	// Create file with initial content
	if err := os.WriteFile(filePath, []byte("initial"), 0644); err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	manifest1, err := Scan(tmpDir)
	if err != nil {
		t.Fatalf("Scan() error = %v", err)
	}

	if len(manifest1.Items) != 1 {
		t.Fatalf("Expected 1 item, got %d", len(manifest1.Items))
	}
	id1 := manifest1.Items[0].ID
	if id1 == "" {
		t.Fatal("ID should not be empty")
	}

	// Change file size
	if err := os.WriteFile(filePath, []byte("changed size"), 0644); err != nil {
		t.Fatalf("failed to modify file: %v", err)
	}

	manifest2, err := Scan(tmpDir)
	if err != nil {
		t.Fatalf("Scan() error = %v", err)
	}

	if len(manifest2.Items) != 1 {
		t.Fatalf("Expected 1 item, got %d", len(manifest2.Items))
	}
	id2 := manifest2.Items[0].ID

	if id1 == id2 {
		t.Error("ID should change when file size changes")
		t.Errorf("ID1: %s, ID2: %s", id1, id2)
	}
}

func TestScan_IDChangesWithModTime(t *testing.T) {
	tmpDir := t.TempDir()

	filePath := filepath.Join(tmpDir, "test.txt")

	// Create file
	if err := os.WriteFile(filePath, []byte("content"), 0644); err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	manifest1, err := Scan(tmpDir)
	if err != nil {
		t.Fatalf("Scan() error = %v", err)
	}

	if len(manifest1.Items) != 1 {
		t.Fatalf("Expected 1 item, got %d", len(manifest1.Items))
	}
	id1 := manifest1.Items[0].ID
	if id1 == "" {
		t.Fatal("ID should not be empty")
	}

	// Touch the file to change modtime (keep same size)
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	file.Close()

	// Wait a bit to ensure modtime changes (at least 1 second on some filesystems)
	// For testing, we'll use a different approach: write the same content back
	// which should update modtime but keep size the same
	time.Sleep(1100 * time.Millisecond)
	if err := os.WriteFile(filePath, []byte("content"), 0644); err != nil {
		t.Fatalf("failed to modify file: %v", err)
	}

	manifest2, err := Scan(tmpDir)
	if err != nil {
		t.Fatalf("Scan() error = %v", err)
	}

	if len(manifest2.Items) != 1 {
		t.Fatalf("Expected 1 item, got %d", len(manifest2.Items))
	}
	id2 := manifest2.Items[0].ID

	if id1 == id2 {
		t.Error("ID should change when modtime changes")
		t.Errorf("ID1: %s, ID2: %s", id1, id2)
	}

	// Verify size didn't change (to confirm it's the modtime that changed the ID)
	if manifest1.Items[0].Size != manifest2.Items[0].Size {
		t.Errorf("File size should not change, but got %d -> %d",
			manifest1.Items[0].Size, manifest2.Items[0].Size)
	}
}

