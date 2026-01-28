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
	"runtime"
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
	DefaultChunkSize    = 4 * 1024 * 1024 // 4 MiB default chunk size
	DefaultSendQueueMax = 64              // Bounded sender read-ahead queue depth
	eofMagic            = "EOF1"          // EOF magic bytes
	streamIOTimeout     = 10 * time.Minute
)

type readJob struct {
	file   *os.File
	offset int64
	buf    []byte
	result chan readResult
}

type readResult struct {
	n   int
	err error
}

type readPool struct {
	jobs chan readJob
}

var (
	globalReadPool     *readPool
	globalReadPoolOnce sync.Once
)

func defaultReadWorkers() int {
	workers := runtime.GOMAXPROCS(0)
	if workers < 1 {
		workers = 1
	}
	if workers > 4 {
		workers = 4
	}
	return workers
}

func newReadPool(workers int) *readPool {
	if workers < 1 {
		workers = 1
	}
	pool := &readPool{jobs: make(chan readJob, workers*4)}
	for i := 0; i < workers; i++ {
		go func() {
			for job := range pool.jobs {
				n, err := job.file.ReadAt(job.buf, job.offset)
				job.result <- readResult{n: n, err: err}
			}
		}()
	}
	return pool
}

func getReadPool() *readPool {
	globalReadPoolOnce.Do(func() {
		globalReadPool = newReadPool(defaultReadWorkers())
	})
	return globalReadPool
}

func readAtWithPool(ctx context.Context, file *os.File, offset int64, buf []byte) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	resultCh := make(chan readResult, 1)
	job := readJob{
		file:   file,
		offset: offset,
		buf:    buf,
		result: resultCh,
	}
	select {
	case getReadPool().jobs <- job:
	case <-time.After(10 * time.Minute):
		fmt.Fprintf(os.Stderr, "sender read queue timeout after 10m: offset=%d len=%d\n", offset, len(buf))
		os.Exit(1)
		return 0, fmt.Errorf("sender read queue timeout after 10m")
	case <-ctx.Done():
		return 0, ctx.Err()
	}
	select {
	case res := <-resultCh:
		return res.n, res.err
	case <-time.After(10 * time.Minute):
		fmt.Fprintf(os.Stderr, "sender read timeout after 10m: offset=%d len=%d\n", offset, len(buf))
		os.Exit(1)
		return 0, fmt.Errorf("sender read timeout after 10m")
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

type streamDeadlineSetter interface {
	SetReadDeadline(time.Time) error
	SetWriteDeadline(time.Time) error
	SetDeadline(time.Time) error
}

func readFullWithTimeout(ctx context.Context, s Stream, buf []byte, relPath string, phase string) error {
	if ds, ok := s.(streamDeadlineSetter); ok {
		deadline := time.Now().Add(streamIOTimeout)
		if dl, has := ctx.Deadline(); has && dl.Before(deadline) {
			deadline = dl
		}
		_ = ds.SetReadDeadline(deadline)
		n, err := io.ReadFull(s, buf)
		_ = ds.SetReadDeadline(time.Time{})
		if err != nil {
			return err
		}
		if n != len(buf) {
			return fmt.Errorf("short read: got %d want %d", n, len(buf))
		}
		return nil
	}

	type readResult struct {
		n   int
		err error
	}
	resultCh := make(chan readResult, 1)
	go func() {
		n, err := io.ReadFull(s, buf)
		resultCh <- readResult{n: n, err: err}
	}()
	select {
	case res := <-resultCh:
		if res.err != nil {
			return res.err
		}
		if res.n != len(buf) {
			return fmt.Errorf("short read: got %d want %d", res.n, len(buf))
		}
		return nil
	case <-time.After(streamIOTimeout):
		fmt.Fprintf(os.Stderr, "receiver read timeout after %s: file=%s phase=%s\n", streamIOTimeout, relPath, phase)
		os.Exit(1)
		return fmt.Errorf("receiver read timeout after %s", streamIOTimeout)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func writeFullWithTimeout(ctx context.Context, s Stream, buf []byte, relPath string, phase string) error {
	if ds, ok := s.(streamDeadlineSetter); ok {
		deadline := time.Now().Add(streamIOTimeout)
		if dl, has := ctx.Deadline(); has && dl.Before(deadline) {
			deadline = dl
		}
		_ = ds.SetWriteDeadline(deadline)
		written := 0
		for written < len(buf) {
			n, err := s.Write(buf[written:])
			if n > 0 {
				written += n
			}
			if err != nil {
				_ = ds.SetWriteDeadline(time.Time{})
				return err
			}
			select {
			case <-ctx.Done():
				_ = ds.SetWriteDeadline(time.Time{})
				return ctx.Err()
			default:
			}
		}
		_ = ds.SetWriteDeadline(time.Time{})
		return nil
	}

	type writeResult struct {
		n   int
		err error
	}
	resultCh := make(chan writeResult, 1)
	go func() {
		n, err := s.Write(buf)
		resultCh <- writeResult{n: n, err: err}
	}()
	select {
	case res := <-resultCh:
		if res.err != nil {
			return res.err
		}
		if res.n != len(buf) {
			return fmt.Errorf("short write: wrote %d, expected %d", res.n, len(buf))
		}
		return nil
	case <-time.After(streamIOTimeout):
		fmt.Fprintf(os.Stderr, "sender write timeout after %s: file=%s phase=%s\n", streamIOTimeout, relPath, phase)
		os.Exit(1)
		return fmt.Errorf("sender write timeout after %s", streamIOTimeout)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func writeUint32WithTimeout(ctx context.Context, s Stream, value uint32, relPath string, phase string) error {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], value)
	return writeFullWithTimeout(ctx, s, buf[:], relPath, phase)
}

func writeAtWithTimeout(ctx context.Context, f *os.File, buf []byte, offset int64, relPath string) error {
	type writeResult struct {
		n   int
		err error
	}
	resultCh := make(chan writeResult, 1)
	go func() {
		n, err := f.WriteAt(buf, offset)
		resultCh <- writeResult{n: n, err: err}
	}()
	select {
	case res := <-resultCh:
		if res.err != nil {
			return res.err
		}
		if res.n != len(buf) {
			return fmt.Errorf("short write: wrote %d, expected %d", res.n, len(buf))
		}
		return nil
	case <-time.After(10 * time.Minute):
		fmt.Fprintf(os.Stderr, "receiver write timeout after 10m: file=%s offset=%d len=%d\n", relPath, offset, len(buf))
		os.Exit(1)
		return fmt.Errorf("receiver write timeout after 10m")
	case <-ctx.Done():
		return ctx.Err()
	}
}

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
	// ErrInvalidEOF indicates an invalid EOF marker was received
	ErrInvalidEOF = errors.New("invalid EOF marker")
)

// ProgressFn is a callback function for reporting transfer progress.
// It receives the relative path, bytes transferred so far, and total bytes.
type ProgressFn func(relpath string, bytesSent int64, total int64)

// SendManifest sends a manifest and all its files/directories over the stream.
// It writes the protocol header (magic + manifest JSON), then sends each item
// in the manifest sequentially (directories first, then files with their content).
// If chunkSize is 0, DefaultChunkSize is used.
// If progressFn is not nil, it will be called periodically to report progress.
func SendManifest(ctx context.Context, s Stream, rootPath string, m manifest.Manifest, chunkSize uint32, progressFn ProgressFn) error {
	if chunkSize == 0 {
		chunkSize = DefaultChunkSize
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

	// Process each item in the manifest
	for _, item := range m.Items {
		// Check context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if item.IsDir {
			// Send directory record
			if err := sendDirRecord(ctx, s, item.RelPath); err != nil {
				return fmt.Errorf("failed to send dir record for %s: %w", item.RelPath, err)
			}
		} else {
			// Send file record with content (chunked)
			filePath := filepath.Join(rootPath, filepath.FromSlash(item.RelPath))
			if err := sendFileRecordChunked(ctx, s, item.RelPath, filePath, item.Size, chunkSize, progressFn); err != nil {
				return fmt.Errorf("failed to send file record for %s: %w", item.RelPath, err)
			}
		}
	}

	// Write end record
	if _, err := s.Write([]byte{recordTypeEnd}); err != nil {
		return fmt.Errorf("failed to write end record: %w", err)
	}

	return nil
}

type resumePlan struct {
	bitmap        *Bitmap
	forceSendFrom uint32
	totalChunks   uint32
	skippedChunks uint32
	verifiedChunk uint32
}

type resumeState struct {
	sidecar       *Sidecar
	totalChunks   uint32
	verifiedChunk uint32
	hasVerified   bool
	dirtyChunks   uint32
	lastFlush     time.Time
}

// sendFileChunksWindowed sends file chunks using the windowed, read-ahead pipeline
// and returns the computed CRC32.
func sendFileChunksWindowed(ctx context.Context, s Stream, relPath string, filePath string, fileSize int64, chunkSize uint32, progressFn ProgressFn, resume *resumePlan) (uint32, error) {
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
	if resume != nil {
		resume.totalChunks = totalChunks
	}

	fileCRC := crc32.New(crc32cTable)

	// Determine read-ahead depth (bounded, fixed)
	maxReadAhead := uint32(DefaultSendQueueMax)

	// Create buffer pool for chunk buffers
	bufPool := chunkPoolFor(chunkSize)
	if bufPool == nil {
		bufPool = bufpool.New(int(chunkSize))
	}

	// Chunk data structure for read-ahead queue
	type chunkData struct {
		index uint32
		n     int
		buf   []byte
		crc   uint32
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

			// Read chunk from file via global read pool
			n, err := readAtWithPool(readAheadCtx, file, fileOffset, buf[:chunkLen])
			if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
				readAheadErrChan <- fmt.Errorf("failed to read file: %w", err)
				bufPool.Put(buf) // Return buffer on error
				return
			}
			if n == 0 {
				bufPool.Put(buf) // Return buffer if nothing read
				break
			}

			// Calculate CRC in read-ahead
			chunkCRC := crc32.Checksum(buf[:n], crc32cTable)

			// Send chunk data to sender
			// If channel is full, wait for space or cancellation
			select {
			case <-readAheadCtx.Done():
				bufPool.Put(buf)
				return
			case chunkChan <- chunkData{index: nextToRead, n: n, buf: buf, crc: chunkCRC}:
			}
			fileOffset += int64(n)
			nextToRead++
		}
	}()

	// Windowed pipeline state
	var nextToSend uint32

	bytesProcessed := int64(0)

	// Send chunks in windowed pipeline
sendLoop:
	for nextToSend < totalChunks {
		// Check context cancellation
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case err := <-readAheadErrChan:
			return 0, err
		default:
		}

		// Get chunk from read-ahead queue
		var chunk chunkData
		var ok bool
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case err := <-readAheadErrChan:
			return 0, err
		case chunk, ok = <-chunkChan:
			if !ok {
				if nextToSend < totalChunks {
					return 0, fmt.Errorf("read-ahead goroutine exited early: expected %d chunks, got %d", totalChunks, nextToSend)
				}
			}
		}
		if !ok {
			break sendLoop
		}

		// Verify chunk index matches expected
		if chunk.index != nextToSend {
			bufPool.Put(chunk.buf)
			return 0, fmt.Errorf("chunk index mismatch: expected %d, got %d", nextToSend, chunk.index)
		}

		sendChunk := true
		if resume != nil && resume.bitmap != nil {
			if resume.bitmap.Get(int(nextToSend)) && nextToSend < resume.forceSendFrom {
				sendChunk = false
			}
		}

		if sendChunk {
			// CRC is already calculated in read-ahead
			chunkCRC := chunk.crc
			fileCRC.Write(chunk.buf[:chunk.n])

			var header [12]byte
			binary.BigEndian.PutUint32(header[0:4], nextToSend)
			binary.BigEndian.PutUint32(header[4:8], uint32(chunk.n))
			binary.BigEndian.PutUint32(header[8:12], chunkCRC)
			if err := writeFullWithTimeout(ctx, s, header[:], relPath, "chunk-header"); err != nil {
				bufPool.Put(chunk.buf)
				return 0, fmt.Errorf("failed to write chunk header: %w", err)
			}

			// Write chunk bytes
			if err := writeFullWithTimeout(ctx, s, chunk.buf[:chunk.n], relPath, "chunk-data"); err != nil {
				bufPool.Put(chunk.buf)
				return 0, fmt.Errorf("failed to write chunk bytes: %w", err)
			}
		} else if resume != nil {
			resume.skippedChunks++
		}

		// Return buffer to pool after sending (QUIC ensures delivery)
		bufPool.Put(chunk.buf)

		if sendChunk {
			bytesProcessed += int64(chunk.n)
		}
		nextToSend++

		if progressFn != nil {
			progressFn(relPath, bytesProcessed, fileSize)
		}
	}

	// Write EOF marker: "EOF1"
	if err := writeFullWithTimeout(ctx, s, []byte(eofMagic), relPath, "eof"); err != nil {
		return 0, fmt.Errorf("failed to write EOF magic: %w", err)
	}

	return fileCRC.Sum32(), nil
}

// receiveFileChunksWindowed receives file chunks, writes to disk,
// and returns the computed CRC32.
func receiveFileChunksWindowed(ctx context.Context, s Stream, relPath string, filePath string, fileSize uint64, chunkSize uint32, progressFn ProgressFn, resume *resumeState) (uint32, error) {
	outFile, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return 0, fmt.Errorf("failed to open output file %s: %w", filePath, err)
	}
	defer outFile.Close()

	if err := outFile.Truncate(int64(fileSize)); err != nil {
	}

	if chunkSize == 0 {
		return 0, fmt.Errorf("invalid chunk size %d", chunkSize)
	}
	bufPool := chunkPoolFor(chunkSize)
	if bufPool == nil {
		bufPool = bufpool.New(int(chunkSize))
	}
	bytesReceived := uint64(0)
	initialBytes := uint64(0)
	fileCRC := crc32.New(crc32cTable)
	if resume != nil && resume.sidecar != nil {
		resume.totalChunks = resume.sidecar.TotalChunks
	}
	if initialBytes > 0 {
		bytesReceived = initialBytes
		if progressFn != nil {
			progressFn(relPath, int64(bytesReceived), int64(fileSize))
		}
	}

	type recvChunk struct {
		index uint32
		n     int
		crc   uint32
		buf   []byte
	}
	const recvQueueDepth = 64
	readErrChan := make(chan error, 1)
	chunkChan := make(chan recvChunk, recvQueueDepth)
	readerCtx, readerCancel := context.WithCancel(ctx)
	defer readerCancel()
	sendReadErr := func(err error) {
		if err == nil {
			return
		}
		select {
		case readErrChan <- err:
		default:
		}
	}
	go func() {
		defer close(chunkChan)
		header := make([]byte, 4)
		metaBuf := make([]byte, 8)
		for {
			select {
			case <-readerCtx.Done():
				return
			default:
			}
			if err := readFullWithTimeout(readerCtx, s, header, relPath, "header"); err != nil {
				sendReadErr(fmt.Errorf("failed to read chunk header: %w", err))
				return
			}
			if string(header) == eofMagic {
				return
			}
			chunkIndex := binary.BigEndian.Uint32(header)
			if resume != nil && resume.totalChunks > 0 && chunkIndex >= resume.totalChunks {
				sendReadErr(fmt.Errorf("chunk index %d out of range", chunkIndex))
				return
			}
			if err := readFullWithTimeout(readerCtx, s, metaBuf, relPath, "chunk-meta"); err != nil {
				sendReadErr(fmt.Errorf("failed to read chunk meta: %w", err))
				return
			}
			chunkLen := binary.BigEndian.Uint32(metaBuf[0:4])
			if chunkLen > chunkSize {
				sendReadErr(fmt.Errorf("chunk length %d exceeds chunk size %d", chunkLen, chunkSize))
				return
			}
			chunkCRC := binary.BigEndian.Uint32(metaBuf[4:8])
			buf := bufPool.Get()
			if int(chunkLen) > len(buf) {
				bufPool.Put(buf)
				sendReadErr(fmt.Errorf("chunk length %d exceeds buffer size %d", chunkLen, len(buf)))
				return
			}
			if err := readFullWithTimeout(readerCtx, s, buf[:chunkLen], relPath, "data"); err != nil {
				bufPool.Put(buf)
				sendReadErr(fmt.Errorf("failed to read chunk bytes: %w", err))
				return
			}
			chunk := recvChunk{index: chunkIndex, n: int(chunkLen), crc: chunkCRC, buf: buf}
			select {
			case chunkChan <- chunk:
			case <-readerCtx.Done():
				bufPool.Put(buf)
				return
			}
		}
	}()

	// Sidecar flusher
	flushDone := make(chan struct{})
	if resume != nil && resume.sidecar != nil {
		go func() {
			defer close(flushDone)
			ticker := time.NewTicker(1 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-readerCtx.Done():
					return
				case <-ticker.C:
					_ = resume.sidecar.Flush()
				}
			}
		}()
	} else {
		close(flushDone)
	}

	// Strictly sequential write pipeline (in-order by chunk index)
	nextExpected := uint32(0)
	pending := make(map[uint32]recvChunk)

	writeChunk := func(chunk recvChunk) error {
		if crc32.Checksum(chunk.buf[:chunk.n], crc32cTable) != chunk.crc {
			bufPool.Put(chunk.buf)
			return ErrCRC32Mismatch
		}

		if err := writeAtWithTimeout(ctx, outFile, chunk.buf[:chunk.n], int64(chunk.index)*int64(chunkSize), relPath); err != nil {
			bufPool.Put(chunk.buf)
			return fmt.Errorf("failed to write to file: %w", err)
		}

		// Update Rolling File CRC
		bytesReceived += uint64(chunk.n)
		fileCRC.Write(chunk.buf[:chunk.n])

		if resume != nil && resume.sidecar != nil {
			resume.sidecar.MarkComplete(chunk.index)
		}
		bufPool.Put(chunk.buf)
		if progressFn != nil {
			progressFn(relPath, int64(bytesReceived), int64(fileSize))
		}
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case err := <-readErrChan:
			return 0, err
		case chunk, ok := <-chunkChan:
			if !ok {
				goto done
			}
			if chunk.index == nextExpected {
				if err := writeChunk(chunk); err != nil {
					return 0, err
				}
				nextExpected++
				for {
					if pendingChunk, ok := pending[nextExpected]; ok {
						delete(pending, nextExpected)
						if err := writeChunk(pendingChunk); err != nil {
							return 0, err
						}
						nextExpected++
						continue
					}
					break
				}
				continue
			}
			pending[chunk.index] = chunk
		}
	}

done:
	readerCancel() // Stop the reader

	<-flushDone // Wait for flusher to exit
	if resume != nil && resume.sidecar != nil {
		// Final flush
		if err := resume.sidecar.Flush(); err != nil {
			return 0, err
		}
	}

	if resume == nil || resume.sidecar == nil {
		if bytesReceived != fileSize {
			return 0, ErrCRC32Mismatch
		}
	}

	return fileCRC.Sum32(), nil
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

// sendFileRecordChunked sends a file record with chunked content.
func sendFileRecordChunked(ctx context.Context, s Stream, relPath string, filePath string, fileSize int64, chunkSize uint32, progressFn ProgressFn) error {
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

	if _, err := sendFileChunksWindowed(ctx, s, relPath, filePath, fileSize, chunkSize, progressFn, nil); err != nil {
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

			if _, err := receiveFileChunksWindowed(ctx, s, relPath, filePath, fileSize, chunkSize, progressFn, nil); err != nil {
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
