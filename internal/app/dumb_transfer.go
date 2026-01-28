package app

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"sync"

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

func splitDumbSizes(total int64, parts int) []int64 {
	if parts < 1 {
		return []int64{total}
	}
	sizes := make([]int64, parts)
	if total <= 0 {
		return sizes
	}
	base := total / int64(parts)
	rem := total % int64(parts)
	for i := 0; i < parts; i++ {
		sizes[i] = base
		if int64(i) < rem {
			sizes[i]++
		}
	}
	return sizes
}

func sendDumbDataMulti(ctx context.Context, conns []transfer.Conn, name string, size int64) error {
	if len(conns) == 0 {
		return fmt.Errorf("no connections available")
	}
	if len(conns) == 1 {
		return sendDumbData(ctx, conns[0], name, size)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sizes := splitDumbSizes(size, len(conns))
	errCh := make(chan error, len(conns))
	var wg sync.WaitGroup

	for i, conn := range conns {
		partName := fmt.Sprintf("%s.part%02d", name, i+1)
		partSize := sizes[i]
		wg.Add(1)
		go func(c transfer.Conn, n string, sz int64) {
			defer wg.Done()
			if err := sendDumbData(ctx, c, n, sz); err != nil {
				errCh <- err
				cancel()
			}
		}(conn, partName, partSize)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return err
		}
	}
	return nil
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
