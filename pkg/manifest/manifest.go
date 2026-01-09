package manifest

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/fnv"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
)

// FileItem represents a single file or directory in the manifest.
type FileItem struct {
	RelPath string `json:"rel_path"` // Relative path with forward slashes
	Size    int64  `json:"size"`     // File size in bytes (0 for directories)
	ModTime int64  `json:"mod_time"` // Modification time as Unix seconds
	IsDir   bool   `json:"is_dir"`   // True if this is a directory
	ID      string `json:"id"`       // Deterministic ID (16 hex chars)
}

// Manifest represents a complete snapshot of a directory tree.
type Manifest struct {
	Root        string    `json:"root"`          // Base name of root path
	Items       []FileItem `json:"items"`        // All items, sorted by RelPath
	TotalBytes  int64     `json:"total_bytes"`   // Sum of file sizes (excluding dirs)
	FileCount   int       `json:"file_count"`    // Number of files
	FolderCount int       `json:"folder_count"`  // Number of directories
}

// Scan scans the directory tree rooted at rootPath and creates a manifest.
// The manifest will have deterministic ordering (items sorted by RelPath).
// Returns an error if rootPath does not exist or cannot be accessed.
// Unreadable files/directories are skipped with wrapped errors.
func Scan(rootPath string) (Manifest, error) {
	// Check if rootPath exists
	info, err := os.Stat(rootPath)
	if err != nil {
		if os.IsNotExist(err) {
			return Manifest{}, fmt.Errorf("path does not exist: %s", rootPath)
		}
		return Manifest{}, fmt.Errorf("cannot access path: %w", err)
	}

	// Get base name of root path
	root := filepath.Base(rootPath)
	if root == "." || root == "/" {
		absPath, err := filepath.Abs(rootPath)
		if err != nil {
			root = "root"
		} else {
			root = filepath.Base(absPath)
		}
	}

	// Normalize rootPath to absolute path
	absRoot, err := filepath.Abs(rootPath)
	if err != nil {
		return Manifest{}, fmt.Errorf("cannot get absolute path: %w", err)
	}

	manifest := Manifest{
		Root:        root,
		Items:       make([]FileItem, 0),
		TotalBytes:  0,
		FileCount:   0,
		FolderCount: 0,
	}

	var scanErrors []error

	// Handle single file case
	if !info.IsDir() {
		item := FileItem{
			RelPath: filepath.Base(absRoot),
			Size:    info.Size(),
			ModTime: info.ModTime().Unix(),
			IsDir:   false,
		}
		manifest.Items = append(manifest.Items, item)
		manifest.FileCount = 1
		manifest.TotalBytes = info.Size()

		// Sort items by RelPath (single item, but for consistency)
		sort.Slice(manifest.Items, func(i, j int) bool {
			return manifest.Items[i].RelPath < manifest.Items[j].RelPath
		})

		// Compute ID for the item
		manifest.Items[0].ID = computeID(manifest.Items[0])

		return manifest, nil
	}

	// Walk the directory tree
	err = filepath.WalkDir(absRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			// Wrap the error with the path
			relPath, relErr := filepath.Rel(absRoot, path)
			if relErr != nil {
				relPath = path
			}
			scanErrors = append(scanErrors, fmt.Errorf("cannot read %s: %w", relPath, err))
			// Continue walking even if this entry failed
			if d == nil || !d.IsDir() {
				return nil // Skip this entry but continue
			}
			return fs.SkipDir // Skip this directory if we can't read it
		}

		// Get relative path
		relPath, err := filepath.Rel(absRoot, path)
		if err != nil {
			return fmt.Errorf("cannot compute relative path: %w", err)
		}

		// Normalize to forward slashes
		relPath = filepath.ToSlash(relPath)
		
		// Skip the root itself (represented as ".")
		if relPath == "." {
			return nil
		}

		// Get file info
		info, err := d.Info()
		if err != nil {
			scanErrors = append(scanErrors, fmt.Errorf("cannot get info for %s: %w", relPath, err))
			return nil // Skip this entry but continue
		}

		size := info.Size()
		if d.IsDir() {
			size = 0 // Directories always have size 0
		}

		item := FileItem{
			RelPath: relPath,
			Size:    size,
			ModTime: info.ModTime().Unix(),
			IsDir:   d.IsDir(),
		}

		manifest.Items = append(manifest.Items, item)

		if d.IsDir() {
			manifest.FolderCount++
		} else {
			manifest.FileCount++
			manifest.TotalBytes += info.Size()
		}

		return nil
	})

	if err != nil {
		return Manifest{}, fmt.Errorf("error walking directory: %w", err)
	}

	// Sort items by RelPath for deterministic ordering
	sort.Slice(manifest.Items, func(i, j int) bool {
		return manifest.Items[i].RelPath < manifest.Items[j].RelPath
	})

	// Compute IDs for all items after sorting
	for i := range manifest.Items {
		manifest.Items[i].ID = computeID(manifest.Items[i])
	}

	// If we collected any scan errors, we still return the manifest but note the errors
	// For now, we'll include them in a wrapped error if there were any
	if len(scanErrors) > 0 {
		return manifest, fmt.Errorf("scan completed with %d error(s): %w", len(scanErrors), errors.Join(scanErrors...))
	}

	return manifest, nil
}

// ScanPaths scans multiple file and/or directory paths and creates a single manifest.
// The manifest will have deterministic ordering (items sorted by RelPath).
// Root is set to "selection".
// If multiple paths have the same base name, they are disambiguated by prefixing
// with an ordinal (1_, 2_, etc.) in deterministic order.
// Returns an error if any path does not exist or cannot be accessed.
// Unreadable files/directories are skipped with wrapped errors.
func ScanPaths(paths []string) (Manifest, error) {
	if len(paths) == 0 {
		return Manifest{}, fmt.Errorf("no paths provided")
	}

	manifest := Manifest{
		Root:        "selection",
		Items:       make([]FileItem, 0),
		TotalBytes:  0,
		FileCount:   0,
		FolderCount: 0,
	}

	var scanErrors []error

	// Track base names to detect collisions
	baseNameCount := make(map[string]int)
	baseNameOrder := make(map[string]int) // Track order of first occurrence

	// First pass: count occurrences of each base name
	for i, path := range paths {
		absPath, err := filepath.Abs(path)
		if err != nil {
			scanErrors = append(scanErrors, fmt.Errorf("cannot get absolute path for %s: %w", path, err))
			continue
		}

		baseName := filepath.Base(absPath)
		if baseName == "." || baseName == "/" {
			// Handle edge case where path is "." or "/"
			if baseName == "." {
				baseName = "current"
			} else {
				baseName = "root"
			}
		}

		if _, exists := baseNameOrder[baseName]; !exists {
			baseNameOrder[baseName] = i
		}
		baseNameCount[baseName]++
	}

	// Second pass: collect items with collision handling
	for i, path := range paths {
		absPath, err := filepath.Abs(path)
		if err != nil {
			continue // Already handled above
		}

		info, err := os.Stat(absPath)
		if err != nil {
			if os.IsNotExist(err) {
				scanErrors = append(scanErrors, fmt.Errorf("path does not exist: %s", path))
				continue
			}
			scanErrors = append(scanErrors, fmt.Errorf("cannot access path %s: %w", path, err))
			continue
		}

		baseName := filepath.Base(absPath)
		if baseName == "." || baseName == "/" {
			if baseName == "." {
				baseName = "current"
			} else {
				baseName = "root"
			}
		}

		// Determine prefix for collision resolution
		var prefix string
		if baseNameCount[baseName] > 1 {
			// Multiple paths with same base name - need to disambiguate
			// Count how many occurrences of this base name we've seen so far (in order)
			ordinal := 0
			for j := 0; j < i; j++ {
				otherAbsPath, err := filepath.Abs(paths[j])
				if err != nil {
					continue
				}
				otherBaseName := filepath.Base(otherAbsPath)
				if otherBaseName == "." || otherBaseName == "/" {
					if otherBaseName == "." {
						otherBaseName = "current"
					} else {
						otherBaseName = "root"
					}
				}
				if otherBaseName == baseName {
					ordinal++
				}
			}
			prefix = fmt.Sprintf("%d_", ordinal+1)
		}

		if !info.IsDir() {
			// Handle file
			relPath := prefix + baseName
			item := FileItem{
				RelPath: filepath.ToSlash(relPath),
				Size:    info.Size(),
				ModTime: info.ModTime().Unix(),
				IsDir:   false,
			}
			manifest.Items = append(manifest.Items, item)
			manifest.FileCount++
			manifest.TotalBytes += info.Size()
		} else {
			// Handle directory
			// Add the directory itself as an item
			dirRelPath := prefix + baseName
			dirItem := FileItem{
				RelPath: filepath.ToSlash(dirRelPath),
				Size:    0,
				ModTime: info.ModTime().Unix(),
				IsDir:   true,
			}
			manifest.Items = append(manifest.Items, dirItem)
			manifest.FolderCount++

			// Walk the directory tree
			err = filepath.WalkDir(absPath, func(walkPath string, d fs.DirEntry, err error) error {
				if err != nil {
					relPath, relErr := filepath.Rel(absPath, walkPath)
					if relErr != nil {
						relPath = walkPath
					}
					scanErrors = append(scanErrors, fmt.Errorf("cannot read %s: %w", relPath, err))
					if d == nil || !d.IsDir() {
						return nil
					}
					return fs.SkipDir
				}

				// Get relative path within the directory
				relPath, err := filepath.Rel(absPath, walkPath)
				if err != nil {
					return fmt.Errorf("cannot compute relative path: %w", err)
				}

				// Normalize to forward slashes
				relPath = filepath.ToSlash(relPath)

				// Skip the root itself (we already added it)
				if relPath == "." {
					return nil
				}

				// Prepend the directory base name (with prefix if needed)
				fullRelPath := prefix + baseName + "/" + relPath

				// Get file info
				info, err := d.Info()
				if err != nil {
					scanErrors = append(scanErrors, fmt.Errorf("cannot get info for %s: %w", fullRelPath, err))
					return nil
				}

				size := info.Size()
				if d.IsDir() {
					size = 0
				}

				item := FileItem{
					RelPath: filepath.ToSlash(fullRelPath),
					Size:    size,
					ModTime: info.ModTime().Unix(),
					IsDir:   d.IsDir(),
				}

				manifest.Items = append(manifest.Items, item)

				if d.IsDir() {
					manifest.FolderCount++
				} else {
					manifest.FileCount++
					manifest.TotalBytes += info.Size()
				}

				return nil
			})

			if err != nil {
				scanErrors = append(scanErrors, fmt.Errorf("error walking directory %s: %w", path, err))
			}
		}
	}

	// Sort items by RelPath for deterministic ordering
	sort.Slice(manifest.Items, func(i, j int) bool {
		return manifest.Items[i].RelPath < manifest.Items[j].RelPath
	})

	// Compute IDs for all items after sorting
	for i := range manifest.Items {
		manifest.Items[i].ID = computeID(manifest.Items[i])
	}

	// If we collected any scan errors, we still return the manifest but note the errors
	if len(scanErrors) > 0 {
		return manifest, fmt.Errorf("scan completed with %d error(s): %w", len(scanErrors), errors.Join(scanErrors...))
	}

	return manifest, nil
}

// computeID generates a deterministic 16-character hex ID for a FileItem.
// Uses FNV-1a 64-bit hash of: RelPath + "|" + Size + "|" + ModTime + "|" + IsDir
func computeID(item FileItem) string {
	h := fnv.New64a()
	
	// Build the input string: RelPath + "|" + Size + "|" + ModTime + "|" + IsDir
	input := fmt.Sprintf("%s|%d|%d|%t", item.RelPath, item.Size, item.ModTime, item.IsDir)
	h.Write([]byte(input))
	
	// Get the 64-bit hash value
	hashValue := h.Sum64()
	
	// Convert to 16-character hex string (8 bytes = 16 hex chars)
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, hashValue)
	id := hex.EncodeToString(buf)
	
	return id
}

// ManifestID generates a stable, deterministic 16-character hex ID for a manifest.
// Uses FNV-1a 64-bit hash of: Root + "|" + TotalBytes + "|" + FileCount + "|" + FolderCount + "|" + firstItemID + "|" + lastItemID
// Returns empty string if manifest has no items.
func ManifestID(m Manifest) string {
	if len(m.Items) == 0 {
		return ""
	}

	h := fnv.New64a()
	
	// Build the input string
	firstID := m.Items[0].ID
	lastID := m.Items[len(m.Items)-1].ID
	input := fmt.Sprintf("%s|%d|%d|%d|%s|%s", m.Root, m.TotalBytes, m.FileCount, m.FolderCount, firstID, lastID)
	h.Write([]byte(input))
	
	// Get the 64-bit hash value
	hashValue := h.Sum64()
	
	// Convert to 16-character hex string (8 bytes = 16 hex chars)
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, hashValue)
	id := hex.EncodeToString(buf)
	
	return id
}

