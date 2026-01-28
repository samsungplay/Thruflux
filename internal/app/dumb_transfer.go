package app

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/sheerbytes/sheerbytes/internal/transfer"
)

const dumbCopyBufferSize = 1024 * 1024

func sendDumbFile(ctx context.Context, conn transfer.Conn, filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat file: %w", err)
	}
	if info.IsDir() {
		return fmt.Errorf("dumb mode requires a single file, got directory: %s", filePath)
	}

	name := filepath.Base(filePath)
	nameBytes := []byte(name)
	if len(nameBytes) > 0xFFFF {
		return fmt.Errorf("file name too long")
	}

	stream, err := conn.OpenStream(ctx)
	if err != nil {
		return fmt.Errorf("failed to open stream: %w", err)
	}
	defer stream.Close()

	if err := binary.Write(stream, binary.BigEndian, uint16(len(nameBytes))); err != nil {
		return fmt.Errorf("failed to write name length: %w", err)
	}
	if _, err := stream.Write(nameBytes); err != nil {
		return fmt.Errorf("failed to write name: %w", err)
	}
	if err := binary.Write(stream, binary.BigEndian, uint64(info.Size())); err != nil {
		return fmt.Errorf("failed to write size: %w", err)
	}

	buf := make([]byte, dumbCopyBufferSize)
	if _, err := io.CopyBuffer(stream, file, buf); err != nil {
		return fmt.Errorf("failed to send file: %w", err)
	}
	return nil
}

func recvDumbFile(ctx context.Context, conn transfer.Conn, outDir string) (string, error) {
	stream, err := conn.AcceptStream(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to accept stream: %w", err)
	}
	defer stream.Close()

	var nameLen uint16
	if err := binary.Read(stream, binary.BigEndian, &nameLen); err != nil {
		return "", fmt.Errorf("failed to read name length: %w", err)
	}
	nameBuf := make([]byte, nameLen)
	if _, err := io.ReadFull(stream, nameBuf); err != nil {
		return "", fmt.Errorf("failed to read name: %w", err)
	}
	var size uint64
	if err := binary.Read(stream, binary.BigEndian, &size); err != nil {
		return "", fmt.Errorf("failed to read size: %w", err)
	}

	outPath := filepath.Join(outDir, string(nameBuf))
	outFile, err := os.Create(outPath)
	if err != nil {
		return "", fmt.Errorf("failed to create output file: %w", err)
	}
	defer outFile.Close()

	buf := make([]byte, dumbCopyBufferSize)
	if _, err := io.CopyBuffer(outFile, io.LimitReader(stream, int64(size)), buf); err != nil {
		return "", fmt.Errorf("failed to receive file: %w", err)
	}
	return outPath, nil
}
