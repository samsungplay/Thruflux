package app

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/sheerbytes/sheerbytes/internal/transfer"
)

const dumbCopyBufferSize = 8 * 1024 * 1024

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
}

func sendDumbData(ctx context.Context, conn transfer.Conn, name string, size int64) error {
	if size < 0 {
		return fmt.Errorf("invalid size")
	}
	nameBytes := []byte(name)
	if len(nameBytes) == 0 {
		nameBytes = []byte("mem.bin")
	}
	if len(nameBytes) > 0xFFFF {
		return fmt.Errorf("name too long")
	}

	stream, err := conn.OpenStream(ctx)
	if err != nil {
		return fmt.Errorf("failed to open stream: %w", err)
	}
	defer stream.Close()

	return sendDumbDataWriter(stream, nameBytes, size)
}

func sendDumbDataWriter(w io.Writer, nameBytes []byte, size int64) error {
	if err := binary.Write(w, binary.BigEndian, uint16(len(nameBytes))); err != nil {
		return fmt.Errorf("failed to write name length: %w", err)
	}
	if _, err := w.Write(nameBytes); err != nil {
		return fmt.Errorf("failed to write name: %w", err)
	}
	if err := binary.Write(w, binary.BigEndian, uint64(size)); err != nil {
		return fmt.Errorf("failed to write size: %w", err)
	}

	reader := io.LimitReader(zeroReader{}, size)
	buf := make([]byte, dumbCopyBufferSize)
	if _, err := io.CopyBuffer(w, reader, buf); err != nil {
		return fmt.Errorf("failed to send data: %w", err)
	}
	return nil
}

func recvDumbDiscard(ctx context.Context, conn transfer.Conn, progressFn func(string, int64, int64)) (string, error) {
	stream, err := conn.AcceptStream(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to accept stream: %w", err)
	}
	defer stream.Close()

	return recvDumbDiscardReader(stream, progressFn)
}

func recvDumbDiscardReader(r io.Reader, progressFn func(string, int64, int64)) (string, error) {
	var nameLen uint16
	if err := binary.Read(r, binary.BigEndian, &nameLen); err != nil {
		return "", fmt.Errorf("failed to read name length: %w", err)
	}
	nameBuf := make([]byte, nameLen)
	if _, err := io.ReadFull(r, nameBuf); err != nil {
		return "", fmt.Errorf("failed to read name: %w", err)
	}
	var size uint64
	if err := binary.Read(r, binary.BigEndian, &size); err != nil {
		return "", fmt.Errorf("failed to read size: %w", err)
	}

	total := int64(size)
	reader := io.LimitReader(r, total)
	buf := make([]byte, dumbCopyBufferSize)
	var received int64
	var lastProgress int64
	for received < total {
		n, err := reader.Read(buf)
		if n > 0 {
			received += int64(n)
			if progressFn != nil && shouldUpdateProgress(&lastProgress) {
				progressFn(string(nameBuf), received, total)
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", fmt.Errorf("failed to receive file: %w", err)
		}
	}
	if received != total {
		return "", fmt.Errorf("short read: got %d want %d", received, total)
	}
	if progressFn != nil {
		progressFn(string(nameBuf), received, total)
	}
	return string(nameBuf), nil
}
