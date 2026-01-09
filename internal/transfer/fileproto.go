package transfer

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const (
	// Protocol constants
	magicBytes = "SBX1"
	
	// Security limits
	maxFilenameLength = 256
	maxFileSize       = 10 * 1024 * 1024 * 1024 * 1024 // 10TB
	bufferSize        = 64 * 1024                       // 64KB buffer for streaming
)

var (
	// ErrInvalidMagic indicates the magic bytes don't match
	ErrInvalidMagic = errors.New("invalid magic bytes")
	// ErrFilenameTooLong indicates the filename exceeds the maximum length
	ErrFilenameTooLong = errors.New("filename too long")
	// ErrFileSizeTooLarge indicates the file size exceeds the maximum allowed
	ErrFileSizeTooLarge = errors.New("file size too large")
	// ErrInvalidFilename indicates the filename contains path traversal or is invalid
	ErrInvalidFilename = errors.New("invalid filename")
	// ErrCRC32Mismatch indicates the CRC32 checksum doesn't match
	ErrCRC32Mismatch = errors.New("CRC32 checksum mismatch")
)

// SendFile sends a file over the stream using the file transfer protocol.
// It writes the header (magic, filename, size), streams the file content,
// computes CRC32 while streaming, and writes the trailing CRC32.
func SendFile(ctx context.Context, s Stream, filePath string) error {
	// Open the file
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	// Get file info
	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat file: %w", err)
	}

	fileSize := fileInfo.Size()
	if fileSize > maxFileSize {
		return ErrFileSizeTooLarge
	}

	// Get base filename (prevent path traversal)
	filename := filepath.Base(filePath)
	if err := validateFilename(filename); err != nil {
		return err
	}

	// Write magic bytes
	if _, err := s.Write([]byte(magicBytes)); err != nil {
		return fmt.Errorf("failed to write magic: %w", err)
	}

	// Write filename length (uint16, big endian)
	filenameBytes := []byte(filename)
	if len(filenameBytes) > maxFilenameLength {
		return ErrFilenameTooLong
	}

	filenameLen := uint16(len(filenameBytes))
	if err := binary.Write(s, binary.BigEndian, filenameLen); err != nil {
		return fmt.Errorf("failed to write filename length: %w", err)
	}

	// Write filename
	if _, err := s.Write(filenameBytes); err != nil {
		return fmt.Errorf("failed to write filename: %w", err)
	}

	// Write file size (uint64, big endian)
	if err := binary.Write(s, binary.BigEndian, uint64(fileSize)); err != nil {
		return fmt.Errorf("failed to write file size: %w", err)
	}

	// Stream file bytes and compute CRC32
	crc32Hash := crc32.NewIEEE()
	buffer := make([]byte, bufferSize)
	bytesRemaining := fileSize

	for bytesRemaining > 0 {
		// Check context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Read chunk from file
		chunkSize := bufferSize
		if int64(chunkSize) > bytesRemaining {
			chunkSize = int(bytesRemaining)
		}
		n, err := file.Read(buffer[:chunkSize])
		if err != nil && err != io.EOF {
			return fmt.Errorf("failed to read file: %w", err)
		}
		if n == 0 {
			break
		}

		// Update CRC32
		crc32Hash.Write(buffer[:n])

		// Write to stream
		if _, err := s.Write(buffer[:n]); err != nil {
			return fmt.Errorf("failed to write to stream: %w", err)
		}

		bytesRemaining -= int64(n)
	}

	// Write trailing CRC32 (uint32, big endian)
	crc32Value := crc32Hash.Sum32()
	if err := binary.Write(s, binary.BigEndian, crc32Value); err != nil {
		return fmt.Errorf("failed to write CRC32: %w", err)
	}

	return nil
}

// RecvFile receives a file from the stream using the file transfer protocol.
// It reads and validates the header, creates the output file, streams bytes to disk,
// verifies the CRC32, and returns the saved file path.
func RecvFile(ctx context.Context, s Stream, outDir string) (string, error) {
	// Read and validate magic bytes
	magicBuf := make([]byte, len(magicBytes))
	if _, err := io.ReadFull(s, magicBuf); err != nil {
		return "", fmt.Errorf("failed to read magic: %w", err)
	}
	if string(magicBuf) != magicBytes {
		return "", ErrInvalidMagic
	}

	// Read filename length (uint16, big endian)
	var filenameLen uint16
	if err := binary.Read(s, binary.BigEndian, &filenameLen); err != nil {
		return "", fmt.Errorf("failed to read filename length: %w", err)
	}

	if filenameLen > maxFilenameLength {
		return "", ErrFilenameTooLong
	}

	// Read filename
	filenameBuf := make([]byte, filenameLen)
	if _, err := io.ReadFull(s, filenameBuf); err != nil {
		return "", fmt.Errorf("failed to read filename: %w", err)
	}
	filename := string(filenameBuf)

	// Validate filename (prevent path traversal)
	if err := validateFilename(filename); err != nil {
		return "", err
	}

	// Read file size (uint64, big endian)
	var fileSize uint64
	if err := binary.Read(s, binary.BigEndian, &fileSize); err != nil {
		return "", fmt.Errorf("failed to read file size: %w", err)
	}

	if fileSize > maxFileSize {
		return "", ErrFileSizeTooLarge
	}

	// Create output file path (join with outDir, using base filename only)
	outputPath := filepath.Join(outDir, filename)

	// Create output file
	outFile, err := os.Create(outputPath)
	if err != nil {
		return "", fmt.Errorf("failed to create output file: %w", err)
	}
	defer outFile.Close()

	// Stream bytes to disk and compute CRC32
	crc32Hash := crc32.NewIEEE()
	buffer := make([]byte, bufferSize)
	bytesRemaining := fileSize

	for bytesRemaining > 0 {
		// Check context cancellation
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		default:
		}

		// Read chunk from stream
		chunkSize := bufferSize
		if uint64(chunkSize) > bytesRemaining {
			chunkSize = int(bytesRemaining)
		}
		n, err := io.ReadFull(s, buffer[:chunkSize])
		if err != nil && err != io.EOF {
			return "", fmt.Errorf("failed to read from stream: %w", err)
		}
		if n == 0 {
			break
		}

		// Update CRC32
		crc32Hash.Write(buffer[:n])

		// Write to file
		if _, err := outFile.Write(buffer[:n]); err != nil {
			return "", fmt.Errorf("failed to write to file: %w", err)
		}

		bytesRemaining -= uint64(n)
	}

	// Read trailing CRC32 (uint32, big endian)
	var receivedCRC32 uint32
	if err := binary.Read(s, binary.BigEndian, &receivedCRC32); err != nil {
		return "", fmt.Errorf("failed to read CRC32: %w", err)
	}

	// Verify CRC32
	computedCRC32 := crc32Hash.Sum32()
	if receivedCRC32 != computedCRC32 {
		return "", ErrCRC32Mismatch
	}

	return outputPath, nil
}

// validateFilename ensures the filename is safe:
// - Must be a base name (no path separators)
// - Must not be empty
// - Must not exceed max length
func validateFilename(filename string) error {
	if filename == "" {
		return ErrInvalidFilename
	}

	// Check for path traversal attempts
	if strings.Contains(filename, "/") || strings.Contains(filename, "\\") {
		return ErrInvalidFilename
	}

	// Check for parent directory references
	if filename == "." || filename == ".." {
		return ErrInvalidFilename
	}

	// Check length
	if len(filename) > maxFilenameLength {
		return ErrFilenameTooLong
	}

	return nil
}

