package transfer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/sheerbytes/sheerbytes/internal/scheduler"
	"github.com/sheerbytes/sheerbytes/pkg/manifest"
)

// Options configures multi-stream manifest transfers.
type Options struct {
	ChunkSize       uint32
	WindowSize      uint32
	ReadAhead       uint32
	ParallelFiles   int
	SmallThreshold  int64
	MediumThreshold int64
	SmallSlotFrac   float64
	AgingAfter      time.Duration
	ProgressFn      ProgressFn
	WindowStatsFn   WindowStatsFn
	TransferStatsFn TransferStatsFn
	WatchdogFn      WatchdogFn
}

// TransferStatsFn reports active/completed file counts and remaining bytes.
type TransferStatsFn func(activeFiles, completedFiles int, remainingBytes int64)

// WatchdogFn emits periodic watchdog logs for stalled transfers.
type WatchdogFn func(msg string, args ...any)

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
	parallelFiles := opts.ParallelFiles
	if parallelFiles < 1 {
		parallelFiles = 1
	}
	if parallelFiles > 32 {
		parallelFiles = 32
	}

	controlStream, err := conn.OpenStream(ctx)
	if err != nil {
		return fmt.Errorf("failed to open control stream: %w", err)
	}
	defer controlStream.Close()

	if err := writeControlHeader(controlStream, m); err != nil {
		return err
	}

	type sendTransferState struct {
		relPath      string
		lastProgress time.Time
		lastAcked    int32
	}
	activeTransfers := make(map[uint64]*sendTransferState)
	var activeMu sync.Mutex

	ackStates := make(map[uint64]*fileACKState)
	var ackStatesMu sync.Mutex
	ackErrChan := make(chan error, 1)
	doneRegistry := newFileDoneRegistry()

	ackCtx, ackCancel := context.WithCancel(ctx)
	defer ackCancel()

	fileItems := make([]manifest.FileItem, 0, len(m.Items))
	itemByRelPath := make(map[string]manifest.FileItem)
	var remainingBytes int64
	for _, item := range m.Items {
		if item.IsDir {
			continue
		}
		fileItems = append(fileItems, item)
		itemByRelPath[item.RelPath] = item
		remainingBytes += item.Size
	}

	sched := scheduler.NewHybridScheduler(scheduler.PolicyConfig{
		ParallelFiles:   parallelFiles,
		SmallThreshold:  opts.SmallThreshold,
		MediumThreshold: opts.MediumThreshold,
		SmallSlotFrac:   opts.SmallSlotFrac,
		AgingAfter:      opts.AgingAfter,
	})
	var schedMu sync.Mutex
	metaByRelPath := make(map[string]scheduler.FileMeta)
	keyByRelPath := make(map[string]scheduler.FileKey)
	keyByStreamID := make(map[uint64]scheduler.FileKey)

	now := time.Now()
	for _, item := range fileItems {
		key := scheduler.FileKey{StreamID: 0, RelPath: item.RelPath}
		meta := scheduler.FileMeta{
			RelPath:   item.RelPath,
			Size:      item.Size,
			Remaining: item.Size,
			AddedAt:   now,
		}
		sched.Add(key, meta)
		metaByRelPath[item.RelPath] = meta
		keyByRelPath[item.RelPath] = key
	}

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
				activeMu.Lock()
				if st, ok := activeTransfers[ack.StreamID]; ok {
					st.lastAcked = int32(ack.HighestContiguousChunk)
				}
				activeMu.Unlock()
				schedMu.Lock()
				if key, ok := keyByStreamID[ack.StreamID]; ok {
					item := itemByRelPath[key.RelPath]
					ackedBytes := int64(ack.HighestContiguousChunk+1) * int64(chunkSize)
					if ackedBytes > item.Size {
						ackedBytes = item.Size
					}
					remaining := item.Size - ackedBytes
					sched.UpdateRemaining(key, remaining)
					meta := metaByRelPath[key.RelPath]
					meta.Remaining = remaining
					metaByRelPath[key.RelPath] = meta
				}
				schedMu.Unlock()
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

	var statsMu sync.Mutex
	activeCount := 0
	completedCount := 0
	scheduledCount := 0
	totalFiles := len(fileItems)
	updateStats := func(active, completed int, remaining int64) {
		if opts.TransferStatsFn != nil {
			opts.TransferStatsFn(active, completed, remaining)
		}
	}

	sem := make(chan struct{}, parallelFiles)
	scheduleWake := make(chan struct{}, 1)
	var wg sync.WaitGroup
	var controlWriteMu sync.Mutex
	var errMu sync.Mutex
	var transferErr error
	transferCtx, transferCancel := context.WithCancel(ctx)
	defer transferCancel()

	setErr := func(err error) {
		if err == nil {
			return
		}
		errMu.Lock()
		if transferErr == nil {
			transferErr = err
			transferCancel()
		}
		errMu.Unlock()
	}

	go func() {
		select {
		case err := <-ackErrChan:
			setErr(err)
		case <-transferCtx.Done():
		}
	}()

	if opts.WatchdogFn != nil {
		go func() {
			ticker := time.NewTicker(5 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-transferCtx.Done():
					return
				case <-ticker.C:
					now := time.Now()
					var stalled []string
					activeMu.Lock()
					for streamID, st := range activeTransfers {
						if now.Sub(st.lastProgress) > 5*time.Second {
							stalled = append(stalled, fmt.Sprintf("%d:%s:ack=%d:idle=%s", streamID, st.relPath, st.lastAcked, now.Sub(st.lastProgress).Truncate(time.Second)))
						}
					}
					activeMu.Unlock()
					statsMu.Lock()
					active := activeCount
					completed := completedCount
					remaining := remainingBytes
					scheduled := scheduledCount
					total := totalFiles
					statsMu.Unlock()
					if remaining > 0 || active > 0 || scheduled < total {
						if len(stalled) > 0 {
							opts.WatchdogFn("transfer watchdog", "active_files", active, "completed_files", completed, "scheduled_files", scheduled, "total_files", total, "remaining_bytes", remaining, "stalled_streams", stalled)
						} else {
							opts.WatchdogFn("transfer watchdog", "active_files", active, "completed_files", completed, "scheduled_files", scheduled, "total_files", total, "remaining_bytes", remaining)
						}
					}
				}
			}
		}()
	}

	if opts.WatchdogFn != nil {
		go func() {
			ticker := time.NewTicker(1 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-transferCtx.Done():
					return
				case <-ticker.C:
					statsMu.Lock()
					completed := completedCount
					statsMu.Unlock()
					snapshot := sched.Snapshot()
					opts.WatchdogFn("scheduler snapshot", "snapshot", snapshot, "completed_files", completed)
				}
			}
		}()
	}

scheduleLoop:
	for {
		statsMu.Lock()
		doneCount := completedCount
		statsMu.Unlock()
		if doneCount >= totalFiles {
			break
		}
		if transferCtx.Err() != nil {
			break scheduleLoop
		}

		select {
		case sem <- struct{}{}:
		case <-transferCtx.Done():
			break scheduleLoop
		}

		schedMu.Lock()
		key, ok := sched.Next(time.Now())
		schedMu.Unlock()
		if !ok {
			<-sem
			select {
			case <-scheduleWake:
				continue
			case <-transferCtx.Done():
				break scheduleLoop
			}
		}

		item, ok := itemByRelPath[key.RelPath]
		if !ok {
			setErr(fmt.Errorf("missing manifest item for %s", key.RelPath))
			<-sem
			break scheduleLoop
		}

		var openTimer *time.Timer
		if opts.WatchdogFn != nil {
			relpath := item.RelPath
			openTimer = time.AfterFunc(5*time.Second, func() {
				opts.WatchdogFn("open stream stalled", "relpath", relpath)
			})
		}
		dataStream, err := conn.OpenStream(transferCtx)
		if openTimer != nil {
			openTimer.Stop()
		}
		if err != nil {
			setErr(fmt.Errorf("failed to open data stream: %w", err))
			<-sem
			break scheduleLoop
		}

		streamID, err := streamIDFromStream(dataStream)
		if err != nil {
			dataStream.Close()
			setErr(err)
			<-sem
			break scheduleLoop
		}

		schedMu.Lock()
		oldKey := keyByRelPath[item.RelPath]
		meta := metaByRelPath[item.RelPath]
		now := time.Now()
		meta.StartedAt = now
		meta.LastScheduledAt = now
		sched.Remove(oldKey)
		newKey := scheduler.FileKey{StreamID: streamID, RelPath: item.RelPath}
		sched.Add(newKey, meta)
		metaByRelPath[item.RelPath] = meta
		keyByRelPath[item.RelPath] = newKey
		keyByStreamID[streamID] = newKey
		schedMu.Unlock()

		controlWriteMu.Lock()
		err = writeFileBegin(controlStream, FileBegin{
			RelPath:   item.RelPath,
			FileSize:  uint64(item.Size),
			ChunkSize: chunkSize,
			StreamID:  streamID,
		})
		controlWriteMu.Unlock()
		if err != nil {
			dataStream.Close()
			setErr(err)
			<-sem
			break scheduleLoop
		}

		fileState := &fileACKState{
			highestAcked: -1,
			ackNotify:    make(chan struct{}, 1),
		}
		ackStatesMu.Lock()
		ackStates[streamID] = fileState
		ackStatesMu.Unlock()

		activeMu.Lock()
		activeTransfers[streamID] = &sendTransferState{
			relPath:      item.RelPath,
			lastProgress: time.Now(),
			lastAcked:    -1,
		}
		activeMu.Unlock()

		statsMu.Lock()
		scheduledCount++
		activeCount++
		active := activeCount
		completed := completedCount
		remaining := remainingBytes
		statsMu.Unlock()
		updateStats(active, completed, remaining)

		wg.Add(1)
		go func(item manifest.FileItem, streamID uint64, dataStream Stream, fileState *fileACKState) {
			defer wg.Done()
			defer func() { <-sem }()

			if transferCtx.Err() != nil {
				return
			}

			filePath := filepath.Join(rootPath, filepath.FromSlash(item.RelPath))
			progressFn := opts.ProgressFn
			if opts.WatchdogFn != nil {
				progressFn = func(relpath string, bytesSent int64, total int64) {
					activeMu.Lock()
					if st, ok := activeTransfers[streamID]; ok {
						st.lastProgress = time.Now()
					}
					activeMu.Unlock()
					if opts.ProgressFn != nil {
						opts.ProgressFn(relpath, bytesSent, total)
					}
				}
			}
			crc32Value, err := sendFileChunksWindowed(transferCtx, dataStream, item.RelPath, filePath, item.Size, chunkSize, windowSize, readAhead, progressFn, opts.WindowStatsFn, fileState, &ackStatesMu, ackErrChan, false)
			if err != nil {
				dataStream.Close()
				setErr(err)
				return
			}
			if opts.WatchdogFn != nil {
				opts.WatchdogFn("sender data eof written", "relpath", item.RelPath, "stream_id", streamID, "crc32", crc32Value)
			}
			if err := dataStream.Close(); err != nil {
				setErr(err)
				return
			}

			controlWriteMu.Lock()
			err = writeFileEnd(controlStream, FileEnd{
				StreamID: streamID,
				CRC32:    crc32Value,
			})
			controlWriteMu.Unlock()
			if err != nil {
				setErr(err)
				return
			}
			if opts.WatchdogFn != nil {
				opts.WatchdogFn("sender file end sent", "relpath", item.RelPath, "stream_id", streamID)
			}

			fileDone, err := doneRegistry.wait(transferCtx, streamID)
			if err != nil {
				setErr(err)
				return
			}
			if !fileDone.OK {
				if fileDone.ErrMsg == "" {
					setErr(fmt.Errorf("receiver reported failure for %s", item.RelPath))
				} else {
					setErr(fmt.Errorf("receiver reported failure for %s: %s", item.RelPath, fileDone.ErrMsg))
				}
				return
			}
			if opts.WatchdogFn != nil {
				opts.WatchdogFn("sender file done received", "relpath", item.RelPath, "stream_id", streamID)
			}

			ackStatesMu.Lock()
			delete(ackStates, streamID)
			ackStatesMu.Unlock()

			activeMu.Lock()
			delete(activeTransfers, streamID)
			activeMu.Unlock()

			schedMu.Lock()
			if key, ok := keyByStreamID[streamID]; ok {
				sched.Remove(key)
				delete(keyByStreamID, streamID)
			}
			schedMu.Unlock()

			statsMu.Lock()
			activeCount--
			completedCount++
			remainingBytes -= item.Size
			active := activeCount
			completed := completedCount
			remaining := remainingBytes
			statsMu.Unlock()
			updateStats(active, completed, remaining)
			select {
			case scheduleWake <- struct{}{}:
			default:
			}
		}(item, streamID, dataStream, fileState)
	}

	wg.Wait()
	if transferErr != nil {
		return transferErr
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

	fileItems := make([]manifest.FileItem, 0, len(m.Items))
	expectedFiles := make(map[string]int64)
	receivedFiles := make(map[string]struct{})
	for _, item := range m.Items {
		if item.IsDir {
			continue
		}
		fileItems = append(fileItems, item)
		expectedFiles[item.RelPath] = item.Size
	}
	remainingFiles := len(fileItems)
	parallelFiles := opts.ParallelFiles
	if parallelFiles < 1 {
		parallelFiles = 1
	}
	if parallelFiles > 32 {
		parallelFiles = 32
	}

	var remainingBytes int64
	for _, item := range fileItems {
		remainingBytes += item.Size
	}
	var statsMu sync.Mutex
	activeCount := 0
	completedCount := 0
	updateStats := func(active, completed int, remaining int64) {
		if opts.TransferStatsFn != nil {
			opts.TransferStatsFn(active, completed, remaining)
		}
	}

	type recvFileState struct {
		begin        FileBegin
		crc32        uint32
		hasCRC       bool
		endCRC       uint32
		hasEnd       bool
		lastProgress time.Time
	}
	stateMu := sync.Mutex{}
	stateByStream := make(map[uint64]*recvFileState)
	sem := make(chan struct{}, parallelFiles)
	var wg sync.WaitGroup
	var dirMu sync.Mutex
	beginQueue := make(chan FileBegin, len(fileItems))
	schedulerDone := make(chan struct{})
	type controlMsg struct {
		ack  *Ack2
		done *FileDone
	}
	controlWriteCh := make(chan controlMsg, parallelFiles*8)
	ackNotify := make(chan struct{}, 1)
	ackMu := sync.Mutex{}
	pendingAcks := make(map[uint64]uint32)
	recvCtx, recvCancel := context.WithCancel(ctx)
	defer recvCancel()
	var recvErr error
	var recvErrMu sync.Mutex
	setRecvErr := func(err error) {
		if err == nil {
			return
		}
		recvErrMu.Lock()
		if recvErr == nil {
			recvErr = err
			recvCancel()
		}
		recvErrMu.Unlock()
	}

	if opts.WatchdogFn != nil {
		go func() {
			ticker := time.NewTicker(5 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-recvCtx.Done():
					return
				case <-ticker.C:
					now := time.Now()
					var stalled []string
					stateMu.Lock()
					for streamID, st := range stateByStream {
						if now.Sub(st.lastProgress) > 5*time.Second {
							stalled = append(stalled, fmt.Sprintf("%d:%s:crc=%t:end=%t:idle=%s", streamID, st.begin.RelPath, st.hasCRC, st.hasEnd, now.Sub(st.lastProgress).Truncate(time.Second)))
						}
					}
					stateCount := len(stateByStream)
					stateMu.Unlock()
					statsMu.Lock()
					active := activeCount
					completed := completedCount
					remaining := remainingBytes
					statsMu.Unlock()
					if remaining > 0 || active > 0 || stateCount > 0 {
						if len(stalled) > 0 {
							opts.WatchdogFn("receive watchdog", "active_files", active, "completed_files", completed, "remaining_bytes", remaining, "active_streams", stateCount, "stalled_streams", stalled)
						} else {
							opts.WatchdogFn("receive watchdog", "active_files", active, "completed_files", completed, "remaining_bytes", remaining, "active_streams", stateCount)
						}
					}
				}
			}
		}()
	}

	go func() {
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()

		flushAcks := func() bool {
			ackMu.Lock()
			if len(pendingAcks) == 0 {
				ackMu.Unlock()
				return true
			}
			toSend := pendingAcks
			pendingAcks = make(map[uint64]uint32)
			ackMu.Unlock()

			for streamID, index := range toSend {
				if err := writeAck2(controlStream, Ack2{
					StreamID:               streamID,
					HighestContiguousChunk: index,
				}); err != nil {
					setRecvErr(err)
					return false
				}
			}
			return true
		}

		for {
			select {
			case <-recvCtx.Done():
				return
			case msg := <-controlWriteCh:
				if msg.ack != nil {
					if err := writeAck2(controlStream, *msg.ack); err != nil {
						setRecvErr(err)
						return
					}
				}
				if msg.done != nil {
					if err := writeFileDone(controlStream, *msg.done); err != nil {
						setRecvErr(err)
						return
					}
				}
			case <-ackNotify:
				if !flushAcks() {
					return
				}
			case <-ticker.C:
				if !flushAcks() {
					return
				}
			}
		}
	}()

	go func() {
		defer close(schedulerDone)
		for {
			select {
			case <-recvCtx.Done():
				return
			case begin, ok := <-beginQueue:
				if !ok {
					return
				}

				select {
				case sem <- struct{}{}:
				case <-recvCtx.Done():
					return
				}

				stateMu.Lock()
				state, ok := stateByStream[begin.StreamID]
				stateMu.Unlock()
				if !ok {
					setRecvErr(fmt.Errorf("missing state for stream %d", begin.StreamID))
					<-sem
					return
				}

				statsMu.Lock()
				activeCount++
				active := activeCount
				completed := completedCount
				remaining := remainingBytes
				statsMu.Unlock()
				updateStats(active, completed, remaining)

				wg.Add(1)
				go func(state *recvFileState) {
					defer wg.Done()
					defer func() { <-sem }()

					dataStream, err := registry.wait(recvCtx, state.begin.StreamID)
					if err != nil {
						setRecvErr(err)
						return
					}
					defer dataStream.Close()

					filePath := filepath.Join(outDir, m.Root, filepath.FromSlash(state.begin.RelPath))
					parentDir := filepath.Dir(filePath)
					dirMu.Lock()
					err = os.MkdirAll(parentDir, 0755)
					dirMu.Unlock()
					if err != nil {
						setRecvErr(fmt.Errorf("failed to create parent directory %s: %w", parentDir, err))
						return
					}

					ackSender := func(index uint32) error {
						ackMu.Lock()
						if current, ok := pendingAcks[state.begin.StreamID]; !ok || index > current {
							pendingAcks[state.begin.StreamID] = index
						}
						ackMu.Unlock()
						select {
						case ackNotify <- struct{}{}:
						default:
						}
						return nil
					}

					progressFn := opts.ProgressFn
					if opts.WatchdogFn != nil {
						progressFn = func(relpath string, bytesReceived int64, total int64) {
							stateMu.Lock()
							state.lastProgress = time.Now()
							stateMu.Unlock()
							if opts.ProgressFn != nil {
								opts.ProgressFn(relpath, bytesReceived, total)
							}
						}
					}
					crc32Value, err := receiveFileChunksWindowed(recvCtx, dataStream, state.begin.RelPath, filePath, state.begin.FileSize, state.begin.ChunkSize, progressFn, ackSender)
					if err != nil {
						select {
						case controlWriteCh <- controlMsg{done: &FileDone{
							StreamID: state.begin.StreamID,
							OK:       false,
							ErrMsg:   err.Error(),
						}}:
						case <-recvCtx.Done():
						}
						setRecvErr(err)
						return
					}
					if opts.WatchdogFn != nil {
						opts.WatchdogFn("receiver data eof read", "relpath", state.begin.RelPath, "stream_id", state.begin.StreamID, "crc32", crc32Value)
					}

					stateMu.Lock()
					state.crc32 = crc32Value
					state.hasCRC = true
					mismatch := state.hasEnd && state.endCRC != crc32Value
					deleteState := state.hasEnd
					stateMu.Unlock()
					if mismatch {
						select {
						case controlWriteCh <- controlMsg{done: &FileDone{
							StreamID: state.begin.StreamID,
							OK:       false,
							ErrMsg:   ErrCRC32Mismatch.Error(),
						}}:
						case <-recvCtx.Done():
						}
						setRecvErr(ErrCRC32Mismatch)
						return
					}

					select {
					case controlWriteCh <- controlMsg{done: &FileDone{
						StreamID: state.begin.StreamID,
						OK:       true,
					}}:
					case <-recvCtx.Done():
						return
					}
					if opts.WatchdogFn != nil {
						opts.WatchdogFn("receiver file done queued", "relpath", state.begin.RelPath, "stream_id", state.begin.StreamID)
					}

					statsMu.Lock()
					activeCount--
					completedCount++
					remainingBytes -= int64(state.begin.FileSize)
					active := activeCount
					completed := completedCount
					remaining := remainingBytes
					statsMu.Unlock()
					updateStats(active, completed, remaining)
					if deleteState {
						stateMu.Lock()
						delete(stateByStream, state.begin.StreamID)
						stateMu.Unlock()
					}
				}(state)
			}
		}
	}()

	for {
		select {
		case <-recvCtx.Done():
			if recvErr != nil {
				return m, recvErr
			}
			return m, recvCtx.Err()
		case err := <-acceptErrChan:
			return m, err
		default:
		}

		msgType, msg, err := readControlMessage(controlStream)
		if err != nil {
			return m, err
		}

		switch msgType {
		case controlTypeFileBegin:
			begin := msg.(FileBegin)
			if err := validateRelPath(begin.RelPath); err != nil {
				return m, err
			}
			expectedSize, ok := expectedFiles[begin.RelPath]
			if !ok {
				return m, fmt.Errorf("manifest mismatch: unexpected file %s size %d", begin.RelPath, begin.FileSize)
			}
			if expectedSize != int64(begin.FileSize) {
				return m, fmt.Errorf("manifest mismatch: expected %s size %d, got %s size %d", begin.RelPath, expectedSize, begin.RelPath, begin.FileSize)
			}
			if _, seen := receivedFiles[begin.RelPath]; seen {
				return m, fmt.Errorf("duplicate file begin for %s", begin.RelPath)
			}
			receivedFiles[begin.RelPath] = struct{}{}
			remainingFiles--

			state := &recvFileState{
				begin:        begin,
				lastProgress: time.Now(),
			}
			stateMu.Lock()
			stateByStream[begin.StreamID] = state
			stateMu.Unlock()
			select {
			case beginQueue <- begin:
			case <-recvCtx.Done():
				return m, recvCtx.Err()
			}

		case controlTypeFileEnd:
			end := msg.(FileEnd)
			stateMu.Lock()
			state, ok := stateByStream[end.StreamID]
			if ok {
				state.endCRC = end.CRC32
				state.hasEnd = true
				mismatch := state.hasCRC && state.crc32 != end.CRC32
				deleteState := state.hasCRC
				stateMu.Unlock()
				if mismatch {
					setRecvErr(ErrCRC32Mismatch)
					return m, ErrCRC32Mismatch
				}
				if deleteState {
					stateMu.Lock()
					delete(stateByStream, end.StreamID)
					stateMu.Unlock()
				}
				break
			}
			stateMu.Unlock()
			if !ok {
				return m, fmt.Errorf("file end for unknown stream %d", end.StreamID)
			}
		case controlTypeEnd:
			if remainingFiles != 0 {
				return m, fmt.Errorf("received fewer files than manifest: remaining %d", remainingFiles)
			}
			close(beginQueue)
			<-schedulerDone
			wg.Wait()
			if recvErr != nil {
				return m, recvErr
			}
			return m, nil
		default:
			return m, fmt.Errorf("unexpected control message type: 0x%02x", msgType)
		}
	}
}
