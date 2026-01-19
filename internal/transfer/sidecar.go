package transfer

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const (
	sidecarMagic   = "SBM2"
	sidecarVersion = uint16(1)
	sidecarSuffix  = ".sbxmap"
	sidecarDir     = ".thruflux_resumedata"
)

// Sidecar tracks chunk completion for resume.
type Sidecar struct {
	Path        string
	FileID      string
	FileSize    int64
	ChunkSize   uint32
	TotalChunks uint32
	bitmap      *Bitmap
	dirty       bool
	mu          sync.Mutex
}

// SidecarPath returns the sidecar path for an item ID.
func SidecarPath(outDir, root, fileID string) string {
	if fileID == "" {
		fileID = "unknown"
	}
	cleanRoot := strings.Trim(root, string(os.PathSeparator))
	baseDir := filepath.Join(outDir, cleanRoot, sidecarDir)
	return filepath.Join(baseDir, fileID+sidecarSuffix)
}

// LoadOrCreateSidecar loads an existing sidecar or creates a new one.
func LoadOrCreateSidecar(path, fileID string, fileSize int64, chunkSize uint32) (*Sidecar, error) {
	if chunkSize == 0 {
		return nil, fmt.Errorf("chunk size must be > 0")
	}
	sc, err := LoadSidecar(path)
	if err == nil {
		if sc.ChunkSize != chunkSize || sc.FileSize != fileSize || sc.FileID != fileID {
			_ = os.Remove(path)
			sc = nil
		}
	}
	if sc != nil {
		return sc, nil
	}
	return CreateSidecar(path, fileID, fileSize, chunkSize)
}

// LoadSidecar reads a sidecar from disk.
func LoadSidecar(path string) (*Sidecar, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(data) < 4+2 {
		return nil, fmt.Errorf("sidecar too small")
	}
	if string(data[:4]) != sidecarMagic {
		return nil, fmt.Errorf("invalid sidecar magic")
	}
	reader := bytes.NewReader(data[4:])
	var version uint16
	if err := binary.Read(reader, binary.BigEndian, &version); err != nil {
		return nil, err
	}
	if version != sidecarVersion {
		return nil, fmt.Errorf("unsupported sidecar version %d", version)
	}
	var chunkSize uint32
	if err := binary.Read(reader, binary.BigEndian, &chunkSize); err != nil {
		return nil, err
	}
	var fileSize uint64
	if err := binary.Read(reader, binary.BigEndian, &fileSize); err != nil {
		return nil, err
	}
	var totalChunks uint32
	if err := binary.Read(reader, binary.BigEndian, &totalChunks); err != nil {
		return nil, err
	}
	var fileIDLen uint16
	if err := binary.Read(reader, binary.BigEndian, &fileIDLen); err != nil {
		return nil, err
	}
	fileID := make([]byte, fileIDLen)
	if _, err := reader.Read(fileID); err != nil {
		return nil, err
	}
	var bitmapLen uint32
	if err := binary.Read(reader, binary.BigEndian, &bitmapLen); err != nil {
		return nil, err
	}
	bitmap := make([]byte, bitmapLen)
	if _, err := reader.Read(bitmap); err != nil {
		return nil, err
	}
	var crc uint32
	if err := binary.Read(reader, binary.BigEndian, &crc); err != nil {
		return nil, err
	}
	if checksum := crc32.Checksum(data[:len(data)-4], crc32cTable); checksum != crc {
		return nil, fmt.Errorf("sidecar checksum mismatch")
	}
	bm, err := BitmapFromBytes(bitmap, int(totalChunks))
	if err != nil {
		return nil, err
	}
	return &Sidecar{
		Path:        path,
		FileID:      string(fileID),
		FileSize:    int64(fileSize),
		ChunkSize:   chunkSize,
		TotalChunks: totalChunks,
		bitmap:      bm,
	}, nil
}

// CreateSidecar creates a new sidecar file.
func CreateSidecar(path, fileID string, fileSize int64, chunkSize uint32) (*Sidecar, error) {
	if chunkSize == 0 {
		return nil, fmt.Errorf("chunk size must be > 0")
	}
	totalChunks := uint32((fileSize + int64(chunkSize) - 1) / int64(chunkSize))
	if totalChunks == 0 {
		totalChunks = 1
	}
	sc := &Sidecar{
		Path:        path,
		FileID:      fileID,
		FileSize:    fileSize,
		ChunkSize:   chunkSize,
		TotalChunks: totalChunks,
		bitmap:      NewBitmap(int(totalChunks)),
		dirty:       true,
	}
	if err := sc.Flush(); err != nil {
		return nil, err
	}
	return sc, nil
}

// MarkComplete marks chunk i as complete.
func (s *Sidecar) MarkComplete(i uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s == nil || s.bitmap == nil {
		return
	}
	if i >= s.TotalChunks {
		return
	}
	s.bitmap.Set(int(i))
	s.dirty = true
}

// IsComplete reports if chunk i is marked complete.
func (s *Sidecar) IsComplete(i uint32) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s == nil || s.bitmap == nil {
		return false
	}
	return s.bitmap.Get(int(i))
}

// HighestComplete returns the highest chunk index marked complete.
func (s *Sidecar) HighestComplete() (int, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s == nil || s.bitmap == nil {
		return -1, false
	}
	return s.bitmap.HighestSetBit()
}

// HighestContiguous returns the highest contiguous completed chunk index.
func (s *Sidecar) HighestContiguous() (int, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s == nil || s.bitmap == nil || s.TotalChunks == 0 {
		return -1, false
	}
	last := -1
	for i := uint32(0); i < s.TotalChunks; i++ {
		if !s.bitmap.Get(int(i)) {
			break
		}
		last = int(i)
	}
	if last < 0 {
		return -1, false
	}
	return last, true
}

// MarshalBitmap returns the bitmap bytes.
func (s *Sidecar) MarshalBitmap() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s == nil || s.bitmap == nil {
		return nil
	}
	return s.bitmap.Marshal()
}

// Flush writes the sidecar to disk if dirty.
func (s *Sidecar) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s == nil || !s.dirty {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(s.Path), 0755); err != nil {
		return err
	}
	buf := new(bytes.Buffer)
	buf.WriteString(sidecarMagic)
	if err := binary.Write(buf, binary.BigEndian, sidecarVersion); err != nil {
		return err
	}
	if err := binary.Write(buf, binary.BigEndian, s.ChunkSize); err != nil {
		return err
	}
	if err := binary.Write(buf, binary.BigEndian, uint64(s.FileSize)); err != nil {
		return err
	}
	if err := binary.Write(buf, binary.BigEndian, s.TotalChunks); err != nil {
		return err
	}
	fileIDBytes := []byte(s.FileID)
	if err := binary.Write(buf, binary.BigEndian, uint16(len(fileIDBytes))); err != nil {
		return err
	}
	if _, err := buf.Write(fileIDBytes); err != nil {
		return err
	}
	bitmap := s.bitmap.Marshal()
	if err := binary.Write(buf, binary.BigEndian, uint32(len(bitmap))); err != nil {
		return err
	}
	if _, err := buf.Write(bitmap); err != nil {
		return err
	}
	data := buf.Bytes()
	crc := crc32.Checksum(data, crc32cTable)
	if err := binary.Write(buf, binary.BigEndian, crc); err != nil {
		return err
	}
	temp := s.Path + ".tmp"
	if err := os.WriteFile(temp, buf.Bytes(), 0644); err != nil {
		return err
	}
	if err := os.Rename(temp, s.Path); err != nil {
		return err
	}
	s.dirty = false
	return nil
}
