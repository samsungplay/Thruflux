package transfer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/sheerbytes/sheerbytes/pkg/manifest"
)

// Options configures multi-stream manifest transfers.
type Options struct {
	ChunkSize     uint32
	WindowSize    uint32
	ReadAhead     uint32
	ProgressFn    ProgressFn
	WindowStatsFn WindowStatsFn
}

type streamRegistry struct {
	mu      sync.Mutex
	streams map[uint64]Stream
	waiters map[uint64][]chan Stream
}

func newStreamRegistry() *streamRegistry {
	return &streamRegistry{
		streams: make(map[uint64]Stream),
		waiters: make(map[uint64][]chan Stream),
	}
}

func (r *streamRegistry) add(id uint64, s Stream) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if waiters, ok := r.waiters[id]; ok {
		for _, ch := range waiters {
			ch <- s
			close(ch)
		}
		delete(r.waiters, id)
		return
	}

	r.streams[id] = s
}

func (r *streamRegistry) wait(ctx context.Context, id uint64) (Stream, error) {
	r.mu.Lock()
	if s, ok := r.streams[id]; ok {
		delete(r.streams, id)
		r.mu.Unlock()
		return s, nil
	}

	ch := make(chan Stream, 1)
	r.waiters[id] = append(r.waiters[id], ch)
	r.mu.Unlock()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case s := <-ch:
		return s, nil
	}
}

type fileDoneRegistry struct {
	mu      sync.Mutex
	waiters map[uint64]chan FileDone
	pending map[uint64]FileDone
}

func newFileDoneRegistry() *fileDoneRegistry {
	return &fileDoneRegistry{
		waiters: make(map[uint64]chan FileDone),
		pending: make(map[uint64]FileDone),
	}
}

func (r *fileDoneRegistry) wait(ctx context.Context, id uint64) (FileDone, error) {
	r.mu.Lock()
	if msg, ok := r.pending[id]; ok {
		delete(r.pending, id)
		r.mu.Unlock()
		return msg, nil
	}
	ch := make(chan FileDone, 1)
	r.waiters[id] = ch
	r.mu.Unlock()

	select {
	case <-ctx.Done():
		return FileDone{}, ctx.Err()
	case msg := <-ch:
		return msg, nil
	}
}

func (r *fileDoneRegistry) deliver(msg FileDone) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if ch, ok := r.waiters[msg.StreamID]; ok {
		delete(r.waiters, msg.StreamID)
		ch <- msg
		close(ch)
		return
	}

	r.pending[msg.StreamID] = msg
}

func streamIDFromStream(s Stream) (uint64, error) {
	ider, ok := s.(StreamIDer)
	if !ok {
		return 0, errors.New("stream ID unavailable")
	}
	return ider.StreamID(), nil
}

// SendManifestMultiStream sends a manifest over a dedicated control stream and
// transfers each file over its own data stream (sequentially).
func SendManifestMultiStream(ctx context.Context, conn Conn, rootPath string, m manifest.Manifest, opts Options) error {
	chunkSize := opts.ChunkSize
	if chunkSize == 0 {
		chunkSize = DefaultChunkSize
	}
	windowSize := opts.WindowSize
	if windowSize == 0 {
		windowSize = DefaultWindowSize
	}

	readAhead := opts.ReadAhead
	if readAhead == 0 {
		readAhead = windowSize + 4
	}
	if readAhead < 1 {
		readAhead = 1
	}
	if readAhead > 256 {
		readAhead = 256
	}

	controlStream, err := conn.OpenStream(ctx)
	if err != nil {
		return fmt.Errorf("failed to open control stream: %w", err)
	}
	defer controlStream.Close()

	if err := writeControlHeader(controlStream, m); err != nil {
		return err
	}

	ackStates := make(map[uint64]*fileACKState)
	var ackStatesMu sync.Mutex
	ackErrChan := make(chan error, 1)
	doneRegistry := newFileDoneRegistry()

	ackCtx, ackCancel := context.WithCancel(ctx)
	defer ackCancel()

	go func() {
		for {
			select {
			case <-ackCtx.Done():
				return
			default:
			}

			msgType, msg, err := readControlMessage(controlStream)
			if err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, io.EOF) {
					return
				}
				select {
				case ackErrChan <- err:
				default:
				}
				return
			}

			switch msgType {
			case controlTypeAck2:
				ack := msg.(Ack2)
				ackStatesMu.Lock()
				if state, ok := ackStates[ack.StreamID]; ok {
					oldHighest := state.highestAcked
					if int32(ack.HighestContiguousChunk) > state.highestAcked {
						state.highestAcked = int32(ack.HighestContiguousChunk)
						if state.highestAcked > oldHighest && state.ackNotify != nil {
							select {
							case state.ackNotify <- struct{}{}:
							default:
							}
						}
					}
				}
				ackStatesMu.Unlock()
			case controlTypeFileDone:
				doneRegistry.deliver(msg.(FileDone))
			default:
				select {
				case ackErrChan <- fmt.Errorf("unexpected control message type: 0x%02x", msgType):
				default:
				}
				return
			}
		}
	}()

	for _, item := range m.Items {
		if item.IsDir {
			continue
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-ackErrChan:
			return err
		default:
		}

		dataStream, err := conn.OpenStream(ctx)
		if err != nil {
			return fmt.Errorf("failed to open data stream: %w", err)
		}

		streamID, err := streamIDFromStream(dataStream)
		if err != nil {
			dataStream.Close()
			return err
		}

		if err := writeFileBegin(controlStream, FileBegin{
			RelPath:   item.RelPath,
			FileSize:  uint64(item.Size),
			ChunkSize: chunkSize,
			StreamID:  streamID,
		}); err != nil {
			dataStream.Close()
			return err
		}

		fileState := &fileACKState{
			highestAcked: -1,
			ackNotify:    make(chan struct{}, 1),
		}
		ackStatesMu.Lock()
		ackStates[streamID] = fileState
		ackStatesMu.Unlock()

		filePath := filepath.Join(rootPath, filepath.FromSlash(item.RelPath))
		crc32Value, err := sendFileChunksWindowed(ctx, dataStream, item.RelPath, filePath, item.Size, chunkSize, windowSize, readAhead, opts.ProgressFn, opts.WindowStatsFn, fileState, &ackStatesMu, ackErrChan)
		dataStream.Close()
		if err != nil {
			return err
		}

		if err := writeFileEnd(controlStream, FileEnd{
			StreamID: streamID,
			CRC32:    crc32Value,
		}); err != nil {
			return err
		}

		fileDone, err := doneRegistry.wait(ctx, streamID)
		if err != nil {
			return err
		}
		if !fileDone.OK {
			if fileDone.ErrMsg == "" {
				return fmt.Errorf("receiver reported failure for %s", item.RelPath)
			}
			return fmt.Errorf("receiver reported failure for %s: %s", item.RelPath, fileDone.ErrMsg)
		}

		ackStatesMu.Lock()
		delete(ackStates, streamID)
		ackStatesMu.Unlock()
	}

	if err := writeControlEnd(controlStream); err != nil {
		return err
	}

	return nil
}

// RecvManifestMultiStream receives a manifest over the control stream and
// reads each file over a dedicated data stream (sequentially).
func RecvManifestMultiStream(ctx context.Context, conn Conn, outDir string, opts Options) (manifest.Manifest, error) {
	controlStream, err := conn.AcceptStream(ctx)
	if err != nil {
		return manifest.Manifest{}, fmt.Errorf("failed to accept control stream: %w", err)
	}
	defer controlStream.Close()

	m, err := readControlHeader(controlStream)
	if err != nil {
		return m, err
	}

	for _, item := range m.Items {
		if !item.IsDir {
			continue
		}
		dirPath := filepath.Join(outDir, m.Root, filepath.FromSlash(item.RelPath))
		if err := os.MkdirAll(dirPath, 0755); err != nil {
			return m, fmt.Errorf("failed to create directory %s: %w", dirPath, err)
		}
	}

	registry := newStreamRegistry()
	acceptErrChan := make(chan error, 1)

	acceptCtx, acceptCancel := context.WithCancel(ctx)
	defer acceptCancel()

	go func() {
		for {
			select {
			case <-acceptCtx.Done():
				return
			default:
			}

			stream, err := conn.AcceptStream(acceptCtx)
			if err != nil {
				select {
				case acceptErrChan <- err:
				default:
				}
				return
			}
			streamID, err := streamIDFromStream(stream)
			if err != nil {
				select {
				case acceptErrChan <- err:
				default:
				}
				return
			}
			registry.add(streamID, stream)
		}
	}()

	var controlWriteMu sync.Mutex

	fileItems := make([]manifest.FileItem, 0, len(m.Items))
	for _, item := range m.Items {
		if !item.IsDir {
			fileItems = append(fileItems, item)
		}
	}
	fileIndex := 0

	for fileIndex < len(fileItems) {
		select {
		case <-ctx.Done():
			return m, ctx.Err()
		case err := <-acceptErrChan:
			return m, err
		default:
		}

		msgType, msg, err := readControlMessage(controlStream)
		if err != nil {
			return m, err
		}
		if msgType != controlTypeFileBegin {
			return m, fmt.Errorf("expected FileBegin, got 0x%02x", msgType)
		}

		begin := msg.(FileBegin)
		if err := validateRelPath(begin.RelPath); err != nil {
			return m, err
		}

		expected := fileItems[fileIndex]
		if expected.RelPath != begin.RelPath || expected.Size != int64(begin.FileSize) {
			return m, fmt.Errorf("manifest mismatch: expected %s size %d, got %s size %d", expected.RelPath, expected.Size, begin.RelPath, begin.FileSize)
		}

		dataStream, err := registry.wait(ctx, begin.StreamID)
		if err != nil {
			return m, err
		}

		filePath := filepath.Join(outDir, m.Root, filepath.FromSlash(begin.RelPath))
		parentDir := filepath.Dir(filePath)
		if err := os.MkdirAll(parentDir, 0755); err != nil {
			dataStream.Close()
			return m, fmt.Errorf("failed to create parent directory %s: %w", parentDir, err)
		}

		ackSender := func(index uint32) error {
			controlWriteMu.Lock()
			defer controlWriteMu.Unlock()
			return writeAck2(controlStream, Ack2{
				StreamID:               begin.StreamID,
				HighestContiguousChunk: index,
			})
		}

		crc32Value, err := receiveFileChunksWindowed(ctx, dataStream, begin.RelPath, filePath, begin.FileSize, begin.ChunkSize, opts.ProgressFn, ackSender)
		dataStream.Close()
		if err != nil {
			controlWriteMu.Lock()
			_ = writeFileDone(controlStream, FileDone{
				StreamID: begin.StreamID,
				OK:       false,
				ErrMsg:   err.Error(),
			})
			controlWriteMu.Unlock()
			return m, err
		}

		msgType, msg, err = readControlMessage(controlStream)
		if err != nil {
			return m, err
		}
		if msgType != controlTypeFileEnd {
			return m, fmt.Errorf("expected FileEnd, got 0x%02x", msgType)
		}
		end := msg.(FileEnd)
		if end.StreamID != begin.StreamID {
			return m, fmt.Errorf("file end stream mismatch: expected %d, got %d", begin.StreamID, end.StreamID)
		}
		if end.CRC32 != crc32Value {
			controlWriteMu.Lock()
			_ = writeFileDone(controlStream, FileDone{
				StreamID: begin.StreamID,
				OK:       false,
				ErrMsg:   ErrCRC32Mismatch.Error(),
			})
			controlWriteMu.Unlock()
			return m, ErrCRC32Mismatch
		}

		controlWriteMu.Lock()
		if err := writeFileDone(controlStream, FileDone{
			StreamID: begin.StreamID,
			OK:       true,
		}); err != nil {
			controlWriteMu.Unlock()
			return m, err
		}
		controlWriteMu.Unlock()

		fileIndex++
	}

	msgType, _, err := readControlMessage(controlStream)
	if err != nil {
		return m, err
	}
	if msgType != controlTypeEnd {
		return m, fmt.Errorf("expected End, got 0x%02x", msgType)
	}

	return m, nil
}
