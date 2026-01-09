package transfer

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/sheerbytes/sheerbytes/internal/bufpool"
	"github.com/sheerbytes/sheerbytes/pkg/manifest"
)

const (
	// Manifest protocol constants
	manifestMagicBytes = "SBM1"

	// Record types
	recordTypeDir  = byte(0x01)
	recordTypeFile = byte(0x02)
	recordTypeEnd  = byte(0xFF)

	// Security limits
	maxRelPathLength = 1024

	// Chunked transfer constants
	DefaultChunkSize  = 4 * 1024 * 1024 // 4 MiB default chunk size
	DefaultWindowSize = 16              // Default window size in chunks
	ackMagic          = "ACK2"          // ACK magic bytes (cumulative ACK)
	eofMagic          = "EOF1"          // EOF magic bytes

	// Receiver ACK policy constants
	ackEveryNChunks = 8  // Send ACK every N chunks
	ackEveryTMs     = 50 // Send ACK every T milliseconds
)

var (
	// ErrInvalidManifestMagic indicates the magic bytes don't match
	ErrInvalidManifestMagic = errors.New("invalid manifest magic bytes")
	// ErrRelPathTooLong indicates the relative path exceeds the maximum length
	ErrRelPathTooLong = errors.New("relative path too long")
	// ErrInvalidRelPath indicates the relative path contains path traversal or is invalid
	ErrInvalidRelPath = errors.New("invalid relative path")
	// ErrInvalidRecordType indicates an unknown record type was received
	ErrInvalidRecordType = errors.New("invalid record type")
	// ErrManifestMismatch indicates the received manifest doesn't match expectations
	ErrManifestMismatch = errors.New("manifest mismatch")
	// ErrInvalidACK indicates an invalid ACK was received
	ErrInvalidACK = errors.New("invalid ACK")
	// ErrInvalidEOF indicates an invalid EOF marker was received
	ErrInvalidEOF = errors.New("invalid EOF marker")
)

// ProgressFn is a callback function for reporting transfer progress.
// It receives the relative path, bytes transferred so far, and total bytes.
type ProgressFn func(relpath string, bytesSent int64, total int64)

// WindowStatsFn is a callback function for reporting window statistics.
// It receives window size, chunk size, in-flight chunks, and highest ACKed chunk index.
type WindowStatsFn func(window, chunkSize, inflightChunks, highestAcked uint32)

// SendManifest sends a manifest and all its files/directories over the stream.
// It writes the protocol header (magic + manifest JSON), then sends each item
// in the manifest sequentially (directories first, then files with their content).
// If chunkSize is 0, DefaultChunkSize is used.
// If windowSize is 0, DefaultWindowSize is used.
// If readAhead is 0, it defaults to windowSize+4.
// If progressFn is not nil, it will be called periodically to report progress.
// If windowStatsFn is not nil, it will be called periodically to report window statistics.
func SendManifest(ctx context.Context, s Stream, rootPath string, m manifest.Manifest, chunkSize uint32, windowSize uint32, readAhead uint32, progressFn ProgressFn, windowStatsFn WindowStatsFn) error {
	if chunkSize == 0 {
		chunkSize = DefaultChunkSize
	}
	if windowSize == 0 {
		windowSize = DefaultWindowSize
	}
	// Write magic bytes
	if _, err := s.Write([]byte(manifestMagicBytes)); err != nil {
		return fmt.Errorf("failed to write magic: %w", err)
	}

	// Serialize manifest to JSON
	manifestJSON, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("failed to marshal manifest: %w", err)
	}

	// Write manifest JSON length (uint32, big endian)
	manifestJSONLen := uint32(len(manifestJSON))
	if err := binary.Write(s, binary.BigEndian, manifestJSONLen); err != nil {
		return fmt.Errorf("failed to write manifest JSON length: %w", err)
	}

	// Write manifest JSON
	if _, err := s.Write(manifestJSON); err != nil {
		return fmt.Errorf("failed to write manifest JSON: %w", err)
	}

	// Shared state for ACKs - track current file being sent
	// Use a SINGLE persistent ACK reader for the entire manifest
	var ackStateMu sync.Mutex
	var currentFileState *fileACKState
	ackErrChan := make(chan error, 1)
	ackCtx, ackCancel := context.WithCancel(ctx)
	defer ackCancel()

	// Start a SINGLE ACK reader goroutine for the entire manifest transfer
	// This reader persists across all files and never restarts between files
	go func() {
		for {
			select {
			case <-ackCtx.Done():
				return
			default:
			}

			// Read ACK2 magic
			ackMagicBuf := make([]byte, len(ackMagic))
			n, err := io.ReadFull(s, ackMagicBuf)
			if err != nil {
				if err == io.EOF || errors.Is(err, context.Canceled) {
					return
				}
				select {
				case ackErrChan <- fmt.Errorf("failed to read ACK magic: %w", err):
				default:
				}
				return
			}
			if n != len(ackMagic) {
				select {
				case ackErrChan <- fmt.Errorf("incomplete ACK magic: read %d bytes, expected %d", n, len(ackMagic)):
				default:
				}
				return
			}
			if string(ackMagicBuf) != ackMagic {
				// Log what we actually got for debugging
				select {
				case ackErrChan <- fmt.Errorf("%w: expected %s, got %q (bytes: %v)", ErrInvalidACK, ackMagic, string(ackMagicBuf), ackMagicBuf):
				default:
				}
				return
			}

			// Read cumulative ACK index
			var ackedIndex uint32
			if err := binary.Read(s, binary.BigEndian, &ackedIndex); err != nil {
				if err == io.EOF || errors.Is(err, context.Canceled) {
					return
				}
				select {
				case ackErrChan <- fmt.Errorf("failed to read ACK index: %w", err):
				default:
				}
				return
			}

			// Update highestAcked for the current file (whichever file is active)
			// Signal waiters when ACK progress is made (event-driven, no polling)
			ackStateMu.Lock()
			if currentFileState != nil {
				oldHighest := currentFileState.highestAcked
				if int32(ackedIndex) > currentFileState.highestAcked {
					currentFileState.highestAcked = int32(ackedIndex)
					// Signal waiters if we made progress (non-blocking send)
					if currentFileState.highestAcked > oldHighest && currentFileState.ackNotify != nil {
						select {
						case currentFileState.ackNotify <- struct{}{}:
						default:
							// Channel already has a notification, skip (prevents blocking)
						}
					}
				}
			}
			ackStateMu.Unlock()
		}
	}()

	// Process each item in the manifest
	for _, item := range m.Items {
		// Check context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-ackErrChan:
			return err
		default:
		}

		if item.IsDir {
			// Send directory record
			if err := sendDirRecord(ctx, s, item.RelPath); err != nil {
				return fmt.Errorf("failed to send dir record for %s: %w", item.RelPath, err)
			}
		} else {
			// Create ACK state for this file and set it as current
			// The persistent ACK reader will update this state
			// Use buffered channel for event-driven signaling (no polling)
			// Channel size of 1 allows non-blocking sends
			fileState := &fileACKState{
				highestAcked: -1,
				ackNotify:    make(chan struct{}, 1),
			}
			ackStateMu.Lock()
			currentFileState = fileState
			ackStateMu.Unlock()

			// Send file record with content (chunked, windowed, with read-ahead)
			filePath := filepath.Join(rootPath, filepath.FromSlash(item.RelPath))
			if err := sendFileRecordChunkedWindowed(ctx, s, item.RelPath, filePath, item.Size, chunkSize, windowSize, readAhead, progressFn, windowStatsFn, fileState, &ackStateMu, ackErrChan); err != nil {
				return fmt.Errorf("failed to send file record for %s: %w", item.RelPath, err)
			}

			// Clear current file state after completion (file is done, ACKs processed)
			ackStateMu.Lock()
			currentFileState = nil
			ackStateMu.Unlock()
		}
	}

	// Write end record
	if _, err := s.Write([]byte{recordTypeEnd}); err != nil {
		return fmt.Errorf("failed to write end record: %w", err)
	}

	return nil
}

// fileACKState tracks ACK state for a single file
// Uses a buffered channel for event-driven signaling when ACKs arrive (replaces polling)
// Channel-based approach chosen over sync.Cond to support context cancellation in select statements
type fileACKState struct {
	highestAcked int32
	ackNotify    chan struct{} // Buffered channel for ACK progress notifications
}

// sendFileChunksWindowed sends file chunks using the windowed, read-ahead pipeline
// and returns the computed CRC32.
func sendFileChunksWindowed(ctx context.Context, s Stream, relPath string, filePath string, fileSize int64, chunkSize uint32, windowSize uint32, readAhead uint32, progressFn ProgressFn, windowStatsFn WindowStatsFn, ackState *fileACKState, ackStateMu *sync.Mutex, ackErrChan chan error, disableWindow bool) (uint32, error) {
	if fileSize > maxFileSize {
		return 0, ErrFileSizeTooLarge
	}

	// Open file
	file, err := os.Open(filePath)
	if err != nil {
		return 0, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	// Calculate total number of chunks
	totalChunks := uint32((fileSize + int64(chunkSize) - 1) / int64(chunkSize))

	// Determine read-ahead depth (default: window+4, bounded)
	maxReadAhead := readAhead
	if maxReadAhead == 0 {
		maxReadAhead = windowSize + 4
	}
	if maxReadAhead < 1 {
		maxReadAhead = 1
	}
	if maxReadAhead > 256 {
		maxReadAhead = 256
	}

	// Create buffer pool for chunk buffers
	bufPool := bufpool.New(int(chunkSize))

	// Chunk data structure for read-ahead queue
	type chunkData struct {
		index uint32
		n     int
		buf   []byte
	}

	// Read-ahead channel (bounded to maxReadAhead)
	chunkChan := make(chan chunkData, maxReadAhead)
	readAheadErrChan := make(chan error, 1)

	// Start read-ahead goroutine
	readAheadCtx, readAheadCancel := context.WithCancel(ctx)
	defer readAheadCancel()

	go func() {
		defer close(chunkChan)
		defer readAheadCancel()

		var nextToRead uint32
		fileOffset := int64(0)

		for fileOffset < fileSize {
			// Check context cancellation
			select {
			case <-readAheadCtx.Done():
				return
			default:
			}

			// Determine chunk length
			remaining := fileSize - fileOffset
			chunkLen := uint32(chunkSize)
			if int64(chunkLen) > remaining {
				chunkLen = uint32(remaining)
			}

			// Get buffer from pool
			buf := bufPool.Get()

			// Read chunk from file
			n, err := io.ReadFull(file, buf[:chunkLen])
			if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
				readAheadErrChan <- fmt.Errorf("failed to read file: %w", err)
				bufPool.Put(buf) // Return buffer on error
				return
			}
			if n == 0 {
				bufPool.Put(buf) // Return buffer if nothing read
				break
			}

			// Send chunk data to sender
			// If channel is full, wait for space or cancellation
			select {
			case <-readAheadCtx.Done():
				bufPool.Put(buf)
				return
			case chunkChan <- chunkData{index: nextToRead, n: n, buf: buf}:
			}

			fileOffset += int64(n)
			nextToRead++
		}
	}()

	// Windowed pipeline state
	var nextToSend uint32

	// Compute CRC32 while sending chunks
	crc32Hash := crc32.NewIEEE()
	bytesSent := int64(0)

	// Send chunks in windowed pipeline
	for nextToSend < totalChunks {
		// Check context cancellation
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case err := <-ackErrChan:
			return 0, err
		case err := <-readAheadErrChan:
			return 0, err
		default:
		}

		// Check window: can we send more?
		currentHighestAcked := int32(-1)
		var inFlight uint32
		if !disableWindow {
			ackStateMu.Lock()
			currentHighestAcked = ackState.highestAcked
			if currentHighestAcked < 0 {
				inFlight = nextToSend
			} else {
				ackedCount := uint32(currentHighestAcked + 1)
				if nextToSend <= ackedCount {
					inFlight = 0
				} else {
					inFlight = nextToSend - ackedCount
				}
			}
			ackStateMu.Unlock()
		}

		// Report window stats
		if windowStatsFn != nil {
			acked := uint32(0)
			if currentHighestAcked >= 0 {
				acked = uint32(currentHighestAcked)
			}
			windowStatsFn(windowSize, chunkSize, inFlight, acked)
		}

		// Wait if window is full
		if !disableWindow && inFlight >= windowSize && nextToSend < totalChunks {
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case err := <-ackErrChan:
				return 0, err
			case err := <-readAheadErrChan:
				return 0, err
			case <-time.After(10 * time.Millisecond):
				continue
			}
		}

		// Get chunk from read-ahead queue
		var chunk chunkData
		var ok bool
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case err := <-ackErrChan:
			return 0, err
		case err := <-readAheadErrChan:
			return 0, err
		case chunk, ok = <-chunkChan:
			if !ok {
				if nextToSend < totalChunks {
					return 0, fmt.Errorf("read-ahead goroutine exited early: expected %d chunks, got %d", totalChunks, nextToSend)
				}
				break
			}
		}

		if !ok {
			break
		}

		// Verify chunk index matches expected
		if chunk.index != nextToSend {
			bufPool.Put(chunk.buf)
			return 0, fmt.Errorf("chunk index mismatch: expected %d, got %d", nextToSend, chunk.index)
		}

		// Update CRC32
		crc32Hash.Write(chunk.buf[:chunk.n])

		// Write chunk_index (uint32, big endian)
		if err := binary.Write(s, binary.BigEndian, nextToSend); err != nil {
			bufPool.Put(chunk.buf)
			return 0, fmt.Errorf("failed to write chunk index: %w", err)
		}

		// Write chunk_len (uint32, big endian)
		if err := binary.Write(s, binary.BigEndian, uint32(chunk.n)); err != nil {
			bufPool.Put(chunk.buf)
			return 0, fmt.Errorf("failed to write chunk length: %w", err)
		}

		// Write chunk bytes
		if _, err := s.Write(chunk.buf[:chunk.n]); err != nil {
			bufPool.Put(chunk.buf)
			return 0, fmt.Errorf("failed to write chunk bytes: %w", err)
		}

		// Return buffer to pool after sending (QUIC ensures delivery)
		bufPool.Put(chunk.buf)

		bytesSent += int64(chunk.n)
		nextToSend++

		if progressFn != nil {
			progressFn(relPath, bytesSent, fileSize)
		}
	}

	// Write EOF marker: "EOF1" + CRC32
	if _, err := s.Write([]byte(eofMagic)); err != nil {
		return 0, fmt.Errorf("failed to write EOF magic: %w", err)
	}

	crc32Value := crc32Hash.Sum32()
	if err := binary.Write(s, binary.BigEndian, crc32Value); err != nil {
		return 0, fmt.Errorf("failed to write CRC32: %w", err)
	}

	return crc32Value, nil
}

// receiveFileChunksWindowed receives file chunks, writes to disk, sends cumulative ACKs,
// and returns the computed CRC32.
func receiveFileChunksWindowed(ctx context.Context, s Stream, relPath string, filePath string, fileSize uint64, chunkSize uint32, progressFn ProgressFn, ackSender func(uint32) error) (uint32, error) {
	// Create output file
	outFile, err := os.Create(filePath)
	if err != nil {
		return 0, fmt.Errorf("failed to create output file %s: %w", filePath, err)
	}
	defer outFile.Close()

	// Preallocate file size for better performance
	if err := outFile.Truncate(int64(fileSize)); err != nil {
	}

	crc32Hash := crc32.NewIEEE()
	buffer := make([]byte, chunkSize)
	bytesReceived := uint64(0)
	expectedChunkIndex := uint32(0)
	highestContiguousIndex := uint32(0)

	var ackMu sync.Mutex
	chunksSinceLastAck := uint32(0)
	lastAckTime := time.Now()
	ackErrChan := make(chan error, 1)

	ackTimerCtx, ackTimerCancel := context.WithCancel(ctx)
	defer ackTimerCancel()
	ackTimerDone := make(chan struct{})
	go func() {
		defer close(ackTimerDone)
		ticker := time.NewTicker(ackEveryTMs * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ackTimerCtx.Done():
				return
			case <-ticker.C:
				ackMu.Lock()
				shouldAck := chunksSinceLastAck > 0 && time.Since(lastAckTime) >= ackEveryTMs*time.Millisecond
				ackIndex := highestContiguousIndex
				ackMu.Unlock()
				if shouldAck {
					ackMu.Lock()
					chunksSinceLastAck = 0
					lastAckTime = time.Now()
					ackMu.Unlock()
					if err := ackSender(ackIndex); err != nil {
						select {
						case ackErrChan <- err:
						default:
						}
						return
					}
				}
			}
		}
	}()

	sendAck := func(index uint32) error {
		if err := ackSender(index); err != nil {
			return err
		}
		ackMu.Lock()
		chunksSinceLastAck = 0
		lastAckTime = time.Now()
		ackMu.Unlock()
		return nil
	}

	for bytesReceived < fileSize {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case err := <-ackErrChan:
			return 0, err
		default:
		}

		var chunkIndex uint32
		if err := binary.Read(s, binary.BigEndian, &chunkIndex); err != nil {
			return 0, fmt.Errorf("failed to read chunk index: %w", err)
		}
		if chunkIndex != expectedChunkIndex {
			return 0, fmt.Errorf("chunk index mismatch: expected %d, got %d", expectedChunkIndex, chunkIndex)
		}

		var chunkLen uint32
		if err := binary.Read(s, binary.BigEndian, &chunkLen); err != nil {
			return 0, fmt.Errorf("failed to read chunk length: %w", err)
		}

		isFirstChunk := chunkIndex == 0
		if isFirstChunk {
			highestContiguousIndex = chunkIndex
			if err := sendAck(highestContiguousIndex); err != nil {
				return 0, err
			}
		}

		if chunkLen > chunkSize {
			return 0, fmt.Errorf("chunk length %d exceeds chunk size %d", chunkLen, chunkSize)
		}

		n, err := io.ReadFull(s, buffer[:chunkLen])
		if err != nil {
			return 0, fmt.Errorf("failed to read chunk bytes: %w", err)
		}
		if uint32(n) != chunkLen {
			return 0, fmt.Errorf("incomplete chunk: read %d bytes, expected %d", n, chunkLen)
		}

		crc32Hash.Write(buffer[:n])

		if _, err := outFile.Write(buffer[:n]); err != nil {
			return 0, fmt.Errorf("failed to write to file: %w", err)
		}

		bytesReceived += uint64(n)
		highestContiguousIndex = chunkIndex

		if progressFn != nil {
			progressFn(relPath, int64(bytesReceived), int64(fileSize))
		}

		if !isFirstChunk {
			ackMu.Lock()
			chunksSinceLastAck++
			shouldAck := chunksSinceLastAck >= ackEveryNChunks
			ackMu.Unlock()
			if shouldAck {
				if err := sendAck(highestContiguousIndex); err != nil {
					return 0, err
				}
			}
		}

		expectedChunkIndex++
	}

	ackTimerCancel()
	select {
	case <-ackTimerDone:
	default:
	}
	go func(index uint32) {
		_ = sendAck(index)
	}(highestContiguousIndex)

	eofMagicBuf := make([]byte, len(eofMagic))
	if _, err := io.ReadFull(s, eofMagicBuf); err != nil {
		return 0, fmt.Errorf("failed to read EOF magic: %w", err)
	}
	if string(eofMagicBuf) != eofMagic {
		return 0, fmt.Errorf("%w: expected %s, got %s", ErrInvalidEOF, eofMagic, string(eofMagicBuf))
	}

	var receivedCRC32 uint32
	if err := binary.Read(s, binary.BigEndian, &receivedCRC32); err != nil {
		return 0, fmt.Errorf("failed to read CRC32: %w", err)
	}

	computedCRC32 := crc32Hash.Sum32()
	if receivedCRC32 != computedCRC32 {
		return 0, ErrCRC32Mismatch
	}

	return computedCRC32, nil
}

// sendDirRecord sends a directory record.
func sendDirRecord(ctx context.Context, s Stream, relPath string) error {
	// Validate relpath
	if err := validateRelPath(relPath); err != nil {
		return err
	}

	// Write record type
	if _, err := s.Write([]byte{recordTypeDir}); err != nil {
		return fmt.Errorf("failed to write dir record type: %w", err)
	}

	// Write relpath length and bytes
	relPathBytes := []byte(relPath)
	relPathLen := uint16(len(relPathBytes))
	if err := binary.Write(s, binary.BigEndian, relPathLen); err != nil {
		return fmt.Errorf("failed to write relpath length: %w", err)
	}
	if _, err := s.Write(relPathBytes); err != nil {
		return fmt.Errorf("failed to write relpath: %w", err)
	}

	return nil
}

// sendFileRecordChunkedWindowed sends a file record with windowed chunked content and cumulative ACKs.
// It uses the provided fileACKState to track ACKs (which is updated by the manifest-level ACK reader).
// If readAhead is > 0, it uses read-ahead with buffer pooling for improved throughput.
func sendFileRecordChunkedWindowed(ctx context.Context, s Stream, relPath string, filePath string, fileSize int64, chunkSize uint32, windowSize uint32, readAhead uint32, progressFn ProgressFn, windowStatsFn WindowStatsFn, ackState *fileACKState, ackStateMu *sync.Mutex, ackErrChan chan error) error {
	// Validate relpath
	if err := validateRelPath(relPath); err != nil {
		return err
	}

	if fileSize > maxFileSize {
		return ErrFileSizeTooLarge
	}

	// Write record type
	if _, err := s.Write([]byte{recordTypeFile}); err != nil {
		return fmt.Errorf("failed to write file record type: %w", err)
	}

	// Write relpath length and bytes
	relPathBytes := []byte(relPath)
	relPathLen := uint16(len(relPathBytes))
	if err := binary.Write(s, binary.BigEndian, relPathLen); err != nil {
		return fmt.Errorf("failed to write relpath length: %w", err)
	}
	if _, err := s.Write(relPathBytes); err != nil {
		return fmt.Errorf("failed to write relpath: %w", err)
	}

	// Write file size (uint64, big endian)
	if err := binary.Write(s, binary.BigEndian, uint64(fileSize)); err != nil {
		return fmt.Errorf("failed to write file size: %w", err)
	}

	// Write chunk size (uint32, big endian)
	if err := binary.Write(s, binary.BigEndian, chunkSize); err != nil {
		return fmt.Errorf("failed to write chunk size: %w", err)
	}

	if _, err := sendFileChunksWindowed(ctx, s, relPath, filePath, fileSize, chunkSize, windowSize, readAhead, progressFn, windowStatsFn, ackState, ackStateMu, ackErrChan, false); err != nil {
		return err
	}

	return nil
}

// RecvManifest receives a manifest and all its files/directories from the stream.
// It reads the protocol header, reconstructs the manifest, creates directories
// and files as specified, and returns the received manifest.
// If progressFn is not nil, it will be called periodically to report progress.
func RecvManifest(ctx context.Context, s Stream, outDir string, progressFn ProgressFn) (manifest.Manifest, error) {
	var m manifest.Manifest

	// Read and validate magic bytes
	magicBuf := make([]byte, len(manifestMagicBytes))
	if _, err := io.ReadFull(s, magicBuf); err != nil {
		return m, fmt.Errorf("failed to read magic: %w", err)
	}
	if string(magicBuf) != manifestMagicBytes {
		return m, ErrInvalidManifestMagic
	}

	// Read manifest JSON length (uint32, big endian)
	var manifestJSONLen uint32
	if err := binary.Read(s, binary.BigEndian, &manifestJSONLen); err != nil {
		return m, fmt.Errorf("failed to read manifest JSON length: %w", err)
	}

	// Read manifest JSON
	manifestJSON := make([]byte, manifestJSONLen)
	if _, err := io.ReadFull(s, manifestJSON); err != nil {
		return m, fmt.Errorf("failed to read manifest JSON: %w", err)
	}

	// Unmarshal manifest
	if err := json.Unmarshal(manifestJSON, &m); err != nil {
		return m, fmt.Errorf("failed to unmarshal manifest: %w", err)
	}

	// Process records until end marker
	itemIndex := 0
	for {
		// Check context cancellation
		select {
		case <-ctx.Done():
			return m, ctx.Err()
		default:
		}

		// Read record type
		recordTypeBuf := make([]byte, 1)
		if _, err := io.ReadFull(s, recordTypeBuf); err != nil {
			return m, fmt.Errorf("failed to read record type: %w", err)
		}
		recordType := recordTypeBuf[0]

		if recordType == recordTypeEnd {
			break
		}

		if recordType == recordTypeDir {
			// Read directory record
			relPath, err := readRelPath(s)
			if err != nil {
				return m, fmt.Errorf("failed to read dir relpath: %w", err)
			}

			// Create directory
			dirPath := filepath.Join(outDir, m.Root, filepath.FromSlash(relPath))
			if err := os.MkdirAll(dirPath, 0755); err != nil {
				return m, fmt.Errorf("failed to create directory %s: %w", dirPath, err)
			}

			// Verify this matches the expected item in manifest
			if itemIndex >= len(m.Items) {
				return m, fmt.Errorf("received more items than in manifest")
			}
			expectedItem := m.Items[itemIndex]
			if expectedItem.RelPath != relPath || !expectedItem.IsDir {
				return m, fmt.Errorf("manifest mismatch: expected %v, got dir %s", expectedItem, relPath)
			}
			itemIndex++

		} else if recordType == recordTypeFile {
			// Read file record (chunked)
			relPath, err := readRelPath(s)
			if err != nil {
				return m, fmt.Errorf("failed to read file relpath: %w", err)
			}

			// Read file size (uint64, big endian)
			var fileSize uint64
			if err := binary.Read(s, binary.BigEndian, &fileSize); err != nil {
				return m, fmt.Errorf("failed to read file size: %w", err)
			}

			if fileSize > maxFileSize {
				return m, ErrFileSizeTooLarge
			}

			// Read chunk size (uint32, big endian)
			var chunkSize uint32
			if err := binary.Read(s, binary.BigEndian, &chunkSize); err != nil {
				return m, fmt.Errorf("failed to read chunk size: %w", err)
			}

			// Verify this matches the expected item in manifest
			if itemIndex >= len(m.Items) {
				return m, fmt.Errorf("received more items than in manifest")
			}
			expectedItem := m.Items[itemIndex]
			if expectedItem.RelPath != relPath || expectedItem.IsDir {
				return m, fmt.Errorf("manifest mismatch: expected %v, got file %s", expectedItem, relPath)
			}
			if expectedItem.Size != int64(fileSize) {
				return m, fmt.Errorf("manifest mismatch: expected size %d, got %d", expectedItem.Size, fileSize)
			}
			itemIndex++

			// Create parent directories
			filePath := filepath.Join(outDir, m.Root, filepath.FromSlash(relPath))
			parentDir := filepath.Dir(filePath)
			if err := os.MkdirAll(parentDir, 0755); err != nil {
				return m, fmt.Errorf("failed to create parent directory %s: %w", parentDir, err)
			}

			ackSender := func(index uint32) error {
				if _, err := s.Write([]byte(ackMagic)); err != nil {
					return fmt.Errorf("failed to write ACK magic: %w", err)
				}
				if err := binary.Write(s, binary.BigEndian, index); err != nil {
					return fmt.Errorf("failed to write ACK index: %w", err)
				}
				return nil
			}

			if _, err := receiveFileChunksWindowed(ctx, s, relPath, filePath, fileSize, chunkSize, progressFn, ackSender); err != nil {
				return m, err
			}

		} else {
			return m, fmt.Errorf("%w: 0x%02x", ErrInvalidRecordType, recordType)
		}
	}

	// Verify we processed all items
	if itemIndex != len(m.Items) {
		return m, fmt.Errorf("received fewer items than in manifest: got %d, expected %d", itemIndex, len(m.Items))
	}

	return m, nil
}

// readRelPath reads a relative path from the stream.
func readRelPath(s Stream) (string, error) {
	// Read relpath length (uint16, big endian)
	var relPathLen uint16
	if err := binary.Read(s, binary.BigEndian, &relPathLen); err != nil {
		return "", fmt.Errorf("failed to read relpath length: %w", err)
	}

	if relPathLen > maxRelPathLength {
		return "", ErrRelPathTooLong
	}

	// Read relpath bytes
	relPathBuf := make([]byte, relPathLen)
	if _, err := io.ReadFull(s, relPathBuf); err != nil {
		return "", fmt.Errorf("failed to read relpath: %w", err)
	}

	relPath := string(relPathBuf)

	// Validate relpath
	if err := validateRelPath(relPath); err != nil {
		return "", err
	}

	return relPath, nil
}

// validateRelPath ensures the relative path is safe:
// - Must not contain ".." segments
// - Must not be an absolute path
// - Must not exceed max length
func validateRelPath(relPath string) error {
	if len(relPath) > maxRelPathLength {
		return ErrRelPathTooLong
	}

	// Check for path traversal attempts
	if strings.Contains(relPath, "..") {
		return ErrInvalidRelPath
	}

	// Check for absolute paths (on Unix, starts with /; on Windows, check for drive letters)
	if filepath.IsAbs(relPath) {
		return ErrInvalidRelPath
	}

	// Check for empty path
	if relPath == "" {
		return ErrInvalidRelPath
	}

	// Additional check: ensure no path separators that could be used for traversal
	// We allow forward slashes (normalized from manifest) but check for backslashes
	// that might be used for traversal on Windows
	if strings.Contains(relPath, "\\") {
		// Check if it's just a backslash (which would be invalid)
		// or if it contains .. with backslashes
		normalized := filepath.ToSlash(relPath)
		if strings.Contains(normalized, "..") {
			return ErrInvalidRelPath
		}
	}

	return nil
}
