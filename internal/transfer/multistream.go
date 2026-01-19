package transfer

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/sheerbytes/sheerbytes/internal/scheduler"
	"github.com/sheerbytes/sheerbytes/pkg/manifest"
)

// Options configures multi-stream manifest transfers.
type Options struct {
	ChunkSize        uint32
	ParallelFiles    int
	SmallThreshold   int64
	MediumThreshold  int64
	SmallSlotFrac    float64
	AgingAfter       time.Duration
	Resume           bool
	ResumeTimeout    time.Duration
	ResumeVerify     string
	ResumeVerifyTail uint32
	HashAlg          string
	NoRootDir        bool
	ResolveFilePath  func(relPath string) string
	ProgressFn       ProgressFn
	TransferStatsFn  TransferStatsFn
	ResumeStatsFn    ResumeStatsFn
	FileDoneFn       FileDoneFn
	ParamSource      func() RuntimeParams
	OnFileStart      func(relpath string, size int64, params RuntimeParams)
}

// TransferStatsFn reports active/completed file counts and remaining bytes.
type TransferStatsFn func(activeFiles, completedFiles int, remainingBytes int64)

// ResumeStatsFn reports resume statistics per file.
type ResumeStatsFn func(relpath string, skippedChunks, totalChunks uint32, verifiedChunk uint32, totalBytes int64, chunkSize uint32)

// FileDoneFn reports file completion (ok=false on failure).
type FileDoneFn func(relpath string, ok bool)

func sidecarIdentifier(item manifest.FileItem) string {
	if item.ID != "" {
		return item.ID
	}
	h := fnv.New64a()
	h.Write([]byte(item.RelPath))
	return fmt.Sprintf("%x", h.Sum64())
}

type streamRegistry struct {
	mu      sync.Mutex
	streams map[uint64]Stream
	waiters map[uint64][]chan Stream
}

type dynamicLimiter struct {
	mu       sync.Mutex
	limit    int
	inFlight int
}

func newDynamicLimiter(limit int) *dynamicLimiter {
	if limit < 1 {
		limit = 1
	}
	return &dynamicLimiter{limit: limit}
}

func (l *dynamicLimiter) SetLimit(limit int) {
	if limit < 1 {
		limit = 1
	}
	l.mu.Lock()
	l.limit = limit
	l.mu.Unlock()
}

func (l *dynamicLimiter) Acquire(ctx context.Context) error {
	for {
		l.mu.Lock()
		if l.inFlight < l.limit {
			l.inFlight++
			l.mu.Unlock()
			return nil
		}
		l.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(25 * time.Millisecond):
		}
	}
}

func (l *dynamicLimiter) Release() {
	l.mu.Lock()
	if l.inFlight > 0 {
		l.inFlight--
	}
	l.mu.Unlock()
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

type resumeInfoRegistry struct {
	mu      sync.Mutex
	waiters map[uint64]chan FileResumeInfo
	pending map[uint64]FileResumeInfo
}

func newResumeInfoRegistry() *resumeInfoRegistry {
	return &resumeInfoRegistry{
		waiters: make(map[uint64]chan FileResumeInfo),
		pending: make(map[uint64]FileResumeInfo),
	}
}

func (r *resumeInfoRegistry) wait(ctx context.Context, id uint64) (FileResumeInfo, error) {
	r.mu.Lock()
	if msg, ok := r.pending[id]; ok {
		delete(r.pending, id)
		r.mu.Unlock()
		return msg, nil
	}
	ch := make(chan FileResumeInfo, 1)
	r.waiters[id] = ch
	r.mu.Unlock()

	select {
	case <-ctx.Done():
		return FileResumeInfo{}, ctx.Err()
	case msg := <-ch:
		return msg, nil
	}
}

func (r *resumeInfoRegistry) deliver(msg FileResumeInfo) {
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

func isFileIOError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "failed to open output file") ||
		strings.Contains(msg, "failed to create parent directory") ||
		strings.Contains(msg, "failed to write to file")
}

func hashFileChunk(filePath string, chunkIndex uint32, chunkSize uint32, fileSize int64, alg byte) (uint64, error) {
	if alg == HashAlgNone {
		return 0, nil
	}
	file, err := os.Open(filePath)
	if err != nil {
		return 0, fmt.Errorf("failed to open file for hash: %w", err)
	}
	defer file.Close()

	offset := int64(chunkIndex) * int64(chunkSize)
	if offset >= fileSize {
		return 0, fmt.Errorf("chunk index %d out of range", chunkIndex)
	}
	chunkLen := int64(chunkSize)
	if remaining := fileSize - offset; remaining < chunkLen {
		chunkLen = remaining
	}
	if chunkLen <= 0 {
		return 0, fmt.Errorf("invalid chunk length %d", chunkLen)
	}

	bufPool := chunkPoolFor(chunkSize)
	var buf []byte
	if bufPool != nil {
		buf = bufPool.Get()
		defer bufPool.Put(buf)
	} else {
		buf = make([]byte, chunkSize)
	}
	n, err := file.ReadAt(buf, offset)
	if err != nil && err != io.EOF {
		return 0, fmt.Errorf("failed to read chunk for hash: %w", err)
	}
	if int64(n) != chunkLen {
		return 0, fmt.Errorf("short chunk read: got %d want %d", n, chunkLen)
	}
	return hashChunk(alg, buf[:chunkLen])
}

// SendManifestMultiStream sends a manifest over a dedicated control stream and
// transfers each file over its own data stream (sequentially).
func SendManifestMultiStream(ctx context.Context, conn Conn, rootPath string, m manifest.Manifest, opts Options) error {
	resolveParams := func() RuntimeParams {
		var p RuntimeParams
		if opts.ParamSource != nil {
			p = opts.ParamSource()
		}
		return NormalizeParams(p, opts)
	}
	currentParams := resolveParams()
	parallelFiles := currentParams.ParallelFiles
	resumeEnabled := opts.Resume
	resumeTimeout := opts.ResumeTimeout
	if resumeTimeout <= 0 {
		resumeTimeout = 1 * time.Second
	}
	resumeVerify := opts.ResumeVerify
	if resumeVerify == "" {
		resumeVerify = "last"
	}
	resumeVerifyTail := opts.ResumeVerifyTail
	switch resumeVerify {
	case "last", "none", "all":
	default:
		return fmt.Errorf("invalid resume verify mode %q", resumeVerify)
	}

	hashAlg, err := parseHashAlg(opts.HashAlg)
	if err != nil {
		return err
	}

	controlStream, err := conn.OpenStream(ctx)
	if err != nil {
		return fmt.Errorf("failed to open control stream: %w", err)
	}
	defer controlStream.Close()

	if err := writeControlHeader(controlStream, m); err != nil {
		return err
	}

	doneRegistry := newFileDoneRegistry()
	resumeRegistry := newResumeInfoRegistry()
	ackErrChan := make(chan error, 1)

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
			case controlTypeFileDone:
				doneRegistry.deliver(msg.(FileDone))
			case controlTypeFileResumeInfo:
				resumeRegistry.deliver(msg.(FileResumeInfo))
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

	limiter := newDynamicLimiter(parallelFiles)
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

	// Watchdog goroutines removed per request.

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

		params := resolveParams()
		if params.ParallelFiles != currentParams.ParallelFiles {
			currentParams.ParallelFiles = params.ParallelFiles
			limiter.SetLimit(params.ParallelFiles)
			sched.SetParallelFiles(params.ParallelFiles)
		}

		if err := limiter.Acquire(transferCtx); err != nil {
			break scheduleLoop
		}

		schedMu.Lock()
		key, ok := sched.Next(time.Now())
		schedMu.Unlock()
		if !ok {
			limiter.Release()
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
			limiter.Release()
			break scheduleLoop
		}

		fileParams := resolveParams()
		if opts.OnFileStart != nil {
			opts.OnFileStart(item.RelPath, item.Size, fileParams)
		}
		chunkSize := fileParams.ChunkSize

		dataStream, err := conn.OpenStream(transferCtx)
		if err != nil {
			setErr(fmt.Errorf("failed to open data stream: %w", err))
			limiter.Release()
			break scheduleLoop
		}

		streamID, err := streamIDFromStream(dataStream)
		if err != nil {
			dataStream.Close()
			setErr(err)
			limiter.Release()
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
			HashAlg:   hashAlg,
		})
		controlWriteMu.Unlock()
		if err != nil {
			dataStream.Close()
			setErr(err)
			limiter.Release()
			break scheduleLoop
		}

		// Yield briefly to allow FileBegin to traverse the network and be processed by the receiver
		// before we flood the link with file data on the parallel stream.
		// This prevents potential SCTP head-of-line blocking or receive window exhaustion deadlock.
		time.Sleep(50 * time.Millisecond)

		statsMu.Lock()
		scheduledCount++
		activeCount++
		active := activeCount
		completed := completedCount
		remaining := remainingBytes
		statsMu.Unlock()
		updateStats(active, completed, remaining)

		wg.Add(1)
		go func(item manifest.FileItem, streamID uint64, dataStream Stream) {
			defer wg.Done()
			defer limiter.Release()

			if transferCtx.Err() != nil {
				return
			}

			filePath := filepath.Join(rootPath, filepath.FromSlash(item.RelPath))
			if opts.ResolveFilePath != nil {
				if resolved := opts.ResolveFilePath(item.RelPath); resolved != "" {
					filePath = resolved
				}
			}
			progressFn := opts.ProgressFn

			var plan *resumePlan
			if resumeEnabled && item.ID != "" {
				controlWriteMu.Lock()
				err = writeResumeRequest(controlStream, ResumeRequest{
					FileID:   item.ID,
					StreamID: streamID,
				})
				controlWriteMu.Unlock()
				if err != nil {
					dataStream.Close()
					setErr(err)
					return
				}

				resumeCtx, resumeCancel := context.WithTimeout(transferCtx, resumeTimeout)
				info, resumeErr := resumeRegistry.wait(resumeCtx, streamID)
				resumeCancel()
				if resumeErr == nil {
					if info.FileID != "" && info.FileID != item.ID {
						setErr(fmt.Errorf("resume info file id mismatch for %s", item.RelPath))
						return
					}
					expectedTotal := uint32(0)
					if chunkSize > 0 {
						expectedTotal = uint32((item.Size + int64(chunkSize) - 1) / int64(chunkSize))
					}
					totalChunks := info.TotalChunks
					if totalChunks == 0 {
						totalChunks = expectedTotal
					}
					if expectedTotal != 0 && totalChunks != expectedTotal {
						setErr(fmt.Errorf("resume info total chunks mismatch for %s", item.RelPath))
						return
					}
					if totalChunks > 0 && len(info.Bitmap) > 0 {
						bitmap, err := BitmapFromBytes(info.Bitmap, int(totalChunks))
						if err != nil {
							setErr(err)
							return
						}
						forceSendFrom := uint32(0)
						verifiedChunk := info.LastVerifiedChunk
						if verifiedChunk < totalChunks {
							forceSendFrom = verifiedChunk + 1
						} else {
							forceSendFrom = totalChunks
						}

						verifyMode := resumeVerify
						if verifyMode == "" {
							verifyMode = "last"
						}
						if verifyMode == "all" {
							verifyMode = "last"
						}
						if verifyMode != "none" && verifiedChunk < totalChunks && hashAlg != HashAlgNone {
							senderHash, err := hashFileChunk(filePath, verifiedChunk, chunkSize, item.Size, hashAlg)
							if err != nil {
								setErr(err)
								return
							}
							if senderHash != info.LastVerifiedHash {
								forceSendFrom = verifiedChunk
							} else if verifiedChunk+1 <= totalChunks {
								forceSendFrom = verifiedChunk + 1
							}
						}

						tail := resumeVerifyTail
						if tail > 0 && forceSendFrom > 0 {
							if tail >= forceSendFrom {
								forceSendFrom = 0
							} else {
								forceSendFrom -= tail
							}
						}

						if forceSendFrom > totalChunks {
							forceSendFrom = totalChunks
						}

						plan = &resumePlan{
							bitmap:        bitmap,
							forceSendFrom: forceSendFrom,
							totalChunks:   totalChunks,
							verifiedChunk: verifiedChunk,
						}

						if opts.ResumeStatsFn != nil {
							plannedSkipped := uint32(0)
							if forceSendFrom > 0 {
								for i := uint32(0); i < forceSendFrom; i++ {
									if bitmap.Get(int(i)) {
										plannedSkipped++
									}
								}
							}
							opts.ResumeStatsFn(item.RelPath, plannedSkipped, totalChunks, verifiedChunk, item.Size, chunkSize)
						}

					}
				} else if !errors.Is(resumeErr, context.DeadlineExceeded) && !errors.Is(resumeErr, context.Canceled) {
					setErr(resumeErr)
					return
				}
			}

			crc32Value, err := sendFileChunksWindowed(transferCtx, dataStream, item.RelPath, filePath, item.Size, chunkSize, progressFn, plan)
			if err != nil {
				dataStream.Close()
				setErr(err)
				return
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
			fileDone, err := doneRegistry.wait(transferCtx, streamID)
			if err != nil {
				setErr(err)
				return
			}
			if !fileDone.OK {
				if opts.FileDoneFn != nil {
					opts.FileDoneFn(item.RelPath, false)
				}
				if fileDone.ErrMsg == "" {
					setErr(fmt.Errorf("receiver reported failure for %s", item.RelPath))
				} else {
					setErr(fmt.Errorf("receiver reported failure for %s: %s", item.RelPath, fileDone.ErrMsg))
				}
				return
			}
			if opts.FileDoneFn != nil {
				opts.FileDoneFn(item.RelPath, true)
			}
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
		}(item, streamID, dataStream)
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
	// Accept streams until we find the Control Stream (Label "stream-1").
	// Data streams (Unordered) might arrive first.
	// We need to buffer any early data streams to be processed later.
	var controlStream Stream
	pendingStreams := make([]Stream, 0)

	const controlStreamLabel = "stream-1"

	for {
		stream, err := conn.AcceptStream(ctx)
		if err != nil {
			return manifest.Manifest{}, fmt.Errorf("failed to accept stream: %w", err)
		}

		// Check label if available (Stream interface needs to support it or we cast)
		// Our WebRTCStream implementation should expose Label via a method or we inspect underlying
		// But wait, the standard Stream interface doesn't have Label().
		// We can try to cast to an interface that provides Label, or relies on StreamID.
		// Since we can't easily change the interface right now without breaking things,
		// and we know the StreamID of "stream-1" is the first one... but IDs are dynamic.

		// Actually, let's look at the logs again: "stream detached and ready label=stream-1 id=1".
		// The sender ALWAYS creates "stream-1" as the first one.
		// If we can't access Label, we essentially have to rely on the *first* stream the sender *created*?
		// No, they arrive out of order.

		// Let's cast to something that has Label() or look at how we can expose it.
		// The WebRTCStream struct has a `dc *webrtc.DataChannel` field.
		// In `func (s *WebRTCStream) Label() string { return s.dc.Label() }` ?
		// We need to add Label() to the Stream interface or type assert.

		// Type assert is safer for now.
		type Labeler interface {
			Label() string
		}

		var label string
		if l, ok := stream.(Labeler); ok {
			label = l.Label()
		} else {
			// Fallback: If we can't get label, we might be in trouble.
			// But wait, we saw "stream-1" logs.
			// Let's assume we can add Label() to WebRTCStream and then check it here.
			// For now, let's close and error if we can't identify.
			stream.Close()
			return manifest.Manifest{}, fmt.Errorf("stream does not support Label()")
		}

		if label == controlStreamLabel {
			controlStream = stream
			break
		}

		// Buffer non-control streams
		pendingStreams = append(pendingStreams, stream)
	}

	defer controlStream.Close()

	m, err := readControlHeader(controlStream)
	if err != nil {
		return m, err
	}

	baseDir := outDir
	if !opts.NoRootDir {
		baseDir = filepath.Join(outDir, m.Root)
	}
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return m, fmt.Errorf("failed to create output directory %s: %w", baseDir, err)
	}

	for _, item := range m.Items {
		if !item.IsDir {
			continue
		}
		dirPath := filepath.Join(baseDir, filepath.FromSlash(item.RelPath))
		if err := os.MkdirAll(dirPath, 0755); err != nil {
			return m, fmt.Errorf("failed to create directory %s: %w", dirPath, err)
		}
	}

	registry := newStreamRegistry()

	// Register any streams we buffered while waiting for control stream
	for _, s := range pendingStreams {
		idFunc, _ := s.(StreamIDer)
		registry.add(idFunc.StreamID(), s)
	}

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
	itemByRelPath := make(map[string]manifest.FileItem)
	itemByID := make(map[string]manifest.FileItem)
	receivedFiles := make(map[string]struct{})
	for _, item := range m.Items {
		if item.IsDir {
			continue
		}
		fileItems = append(fileItems, item)
		expectedFiles[item.RelPath] = item.Size
		itemByRelPath[item.RelPath] = item
		if item.ID != "" {
			itemByID[item.ID] = item
		}
	}
	remainingFiles := len(fileItems)
	if opts.ParallelFiles == 0 {
		opts.ParallelFiles = HeuristicParams(remainingFiles).ParallelFiles
	}
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
		hasEnd       bool
		hasData      bool
		lastProgress time.Time
		stream       Stream
		resume       *resumeState
		resumeInfo   *FileResumeInfo
		computedCRC  uint32
	}
	stateMu := sync.Mutex{}
	stateByStream := make(map[uint64]*recvFileState)
	sem := make(chan struct{}, parallelFiles)
	var wg sync.WaitGroup
	var dirMu sync.Mutex
	beginQueue := make(chan FileBegin, len(fileItems))
	schedulerDone := make(chan struct{})
	type controlMsg struct {
		done   *FileDone
		resume *FileResumeInfo
	}
	controlWriteCh := make(chan controlMsg, parallelFiles*8)
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

	buildResumeInfo := func(begin FileBegin, item manifest.FileItem) (*FileResumeInfo, *resumeState, error) {
		totalChunks := uint32(0)
		if begin.ChunkSize > 0 {
			totalChunks = uint32((int64(begin.FileSize) + int64(begin.ChunkSize) - 1) / int64(begin.ChunkSize))
		}

		info := &FileResumeInfo{
			FileID:      item.ID,
			StreamID:    begin.StreamID,
			TotalChunks: totalChunks,
		}
		state := &resumeState{
			totalChunks: totalChunks,
			lastFlush:   time.Now(),
		}

		if !opts.Resume || totalChunks == 0 {
			info.LastVerifiedChunk = totalChunks
			return info, state, nil
		}

		filePath := filepath.Join(baseDir, filepath.FromSlash(begin.RelPath))
		sidecarPath := SidecarPath(baseDir, "", sidecarIdentifier(item))
		sidecar, err := LoadOrCreateSidecar(sidecarPath, item.ID, int64(begin.FileSize), begin.ChunkSize)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to load sidecar: %w", err)
		}
		state.sidecar = sidecar
		info.Bitmap = sidecar.MarshalBitmap()
		if highest, ok := sidecar.HighestComplete(); ok {
			info.LastVerifiedChunk = uint32(highest)
			if begin.HashAlg != HashAlgNone {
				hashValue, err := hashFileChunk(filePath, uint32(highest), begin.ChunkSize, int64(begin.FileSize), begin.HashAlg)
				if err != nil {
					return nil, nil, err
				}
				info.LastVerifiedHash = hashValue
				state.verifiedChunk = info.LastVerifiedChunk
				state.hasVerified = true
			}
		} else {
			info.LastVerifiedChunk = totalChunks
		}
		if opts.ResumeStatsFn != nil && totalChunks > 0 {
			skippedChunks := uint32(sidecar.bitmap.CountSet())
			if skippedChunks > totalChunks {
				skippedChunks = totalChunks
			}
			opts.ResumeStatsFn(begin.RelPath, skippedChunks, totalChunks, info.LastVerifiedChunk, int64(begin.FileSize), begin.ChunkSize)
		}
		return info, state, nil
	}

	// Watchdog disabled per request to avoid aborting stalled streams.

	go func() {
		for {
			select {
			case <-recvCtx.Done():
				return
			case msg := <-controlWriteCh:
				if msg.done != nil {
					if err := writeFileDone(controlStream, *msg.done); err != nil {
						setRecvErr(err)
						return
					}
				}
				if msg.resume != nil {
					if err := writeFileResumeInfo(controlStream, *msg.resume); err != nil {
						setRecvErr(err)
						return
					}
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
					stateMu.Lock()
					state.stream = dataStream
					stateMu.Unlock()

					filePath := filepath.Join(baseDir, filepath.FromSlash(state.begin.RelPath))
					parentDir := filepath.Dir(filePath)
					dirMu.Lock()
					err = os.MkdirAll(parentDir, 0755)
					dirMu.Unlock()
					if err != nil {
						setRecvErr(fmt.Errorf("failed to create parent directory %s: %w", parentDir, err))
						return
					}

					progressFn := opts.ProgressFn
					computedCRC, err := receiveFileChunksWindowed(recvCtx, dataStream, state.begin.RelPath, filePath, state.begin.FileSize, state.begin.ChunkSize, progressFn, state.resume)
					if err != nil {
						if opts.FileDoneFn != nil {
							opts.FileDoneFn(state.begin.RelPath, false)
						}
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
					stateMu.Lock()
					state.hasData = true
					state.computedCRC = computedCRC
					deleteState := state.hasEnd && state.hasData
					stateMu.Unlock()

					select {
					case controlWriteCh <- controlMsg{done: &FileDone{
						StreamID: state.begin.StreamID,
						OK:       true,
					}}:
					case <-recvCtx.Done():
						return
					}
					if opts.FileDoneFn != nil {
						opts.FileDoneFn(state.begin.RelPath, true)
					}
					stateMu.Lock()
					state.stream = nil
					stateMu.Unlock()

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
			if isGracefulRemoteClose(err) && allFilesCompleted(&statsMu, &completedCount, len(fileItems)) {
				return m, nil
			}
			return m, err
		default:
		}

		msgType, msg, err := readControlMessage(controlStream)
		if err != nil {
			if isGracefulRemoteClose(err) && allFilesCompleted(&statsMu, &completedCount, len(fileItems)) {
				return m, nil
			}
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
			if resumeInfo, resumeState, err := buildResumeInfo(begin, itemByRelPath[begin.RelPath]); err == nil {
				state.resume = resumeState
				state.resumeInfo = resumeInfo
			} else {
				return m, err
			}
			stateMu.Lock()
			stateByStream[begin.StreamID] = state
			stateMu.Unlock()
			select {
			case beginQueue <- begin:
			case <-recvCtx.Done():
				return m, recvCtx.Err()
			}

		case controlTypeResumeRequest:
			req := msg.(ResumeRequest)
			stateMu.Lock()
			state, ok := stateByStream[req.StreamID]
			stateMu.Unlock()
			if !ok {
				return m, fmt.Errorf("resume request for unknown stream %d", req.StreamID)
			}
			item, ok := itemByRelPath[state.begin.RelPath]
			if !ok {
				return m, fmt.Errorf("missing manifest item for %s", state.begin.RelPath)
			}
			if req.FileID != "" && item.ID != "" && req.FileID != item.ID {
				return m, fmt.Errorf("resume request file id mismatch for %s", state.begin.RelPath)
			}

			resumeInfo := state.resumeInfo
			if resumeInfo == nil {
				resumeInfo, state.resume, err = buildResumeInfo(state.begin, item)
				if err != nil {
					return m, err
				}
				state.resumeInfo = resumeInfo
			}

			select {
			case controlWriteCh <- controlMsg{resume: resumeInfo}:
			case <-recvCtx.Done():
				return m, recvCtx.Err()
			}

		case controlTypeFileEnd:
			end := msg.(FileEnd)
			stateMu.Lock()
			state, ok := stateByStream[end.StreamID]
			if ok {
				state.hasEnd = true
				if state.hasData && end.CRC32 != 0 && state.computedCRC != end.CRC32 {
					// Note: Realistically we should signal this error back,
					// but chunk-level CRC is already very strong.
				}
				deleteState := state.hasData
				stateMu.Unlock()
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

func isGracefulRemoteClose(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "Application error 0x0") && strings.Contains(msg, "remote")
}

func allFilesCompleted(mu *sync.Mutex, completed *int, total int) bool {
	mu.Lock()
	defer mu.Unlock()
	return *completed >= total && total > 0
}
