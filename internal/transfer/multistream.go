package transfer

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"container/list"
	"github.com/sheerbytes/sheerbytes/internal/bufpool"
	"github.com/sheerbytes/sheerbytes/internal/scheduler"
	"github.com/sheerbytes/sheerbytes/pkg/manifest"
)

// Options configures multi-stream manifest transfers.
type Options struct {
	ChunkSize                uint32
	ParallelFiles            int
	StripeMax                int
	SmallThreshold           int64
	MediumThreshold          int64
	SmallSlotFrac            float64
	AgingAfter               time.Duration
	Resume                   bool
	ResumeTimeout            time.Duration
	ResumeVerify             string
	ResumeVerifyTail         uint32
	HashAlg                  string
	NoRootDir                bool
	ResolveFilePath          func(relPath string) string
	ProgressFn               ProgressFn
	ProgressDeltaFn          ProgressDeltaFn
	TransferStatsFn          TransferStatsFn
	ResumeStatsFn            ResumeStatsFn
	FileDoneFn               FileDoneFn
	ParamSource              func() RuntimeParams
	OnFileStart              func(relpath string, size int64, params RuntimeParams)
	OnManifestFn             func(manifest.Manifest)
	OnResumeSnapshotStart    func(total int)
	OnResumeSnapshotProgress func(done, total int)
	OnResumeSnapshotDone     func()
}

// TransferStatsFn reports active/completed file counts and remaining bytes.
type TransferStatsFn func(activeFiles, completedFiles int, remainingBytes int64)

// ResumeStatsFn reports resume statistics per file.
type ResumeStatsFn func(relpath string, skippedChunks, totalChunks uint32, verifiedChunk uint32, totalBytes int64, chunkSize uint32)

// FileDoneFn reports file completion (ok=false on failure).
type FileDoneFn func(relpath string, ok bool)

const resumeHashUnknown = ^uint64(0)

const defaultResumeHashTimeout = 2 * time.Second
const resumeGracePeriod = 300 * time.Millisecond
const controlBatchMax = 32
const controlBatchFlushInterval = 2 * time.Millisecond

type resumeHashResult struct {
	hash uint64
	err  error
}

func hashFileChunkWithTimeout(filePath string, chunkIndex uint32, chunkSize uint32, fileSize int64, alg byte, timeout time.Duration) (uint64, bool, error) {
	if alg == HashAlgNone {
		return 0, false, nil
	}
	if timeout <= 0 {
		timeout = defaultResumeHashTimeout
	}
	resultCh := make(chan resumeHashResult, 1)
	go func() {
		hash, err := hashFileChunk(filePath, chunkIndex, chunkSize, fileSize, alg)
		resultCh <- resumeHashResult{hash: hash, err: err}
	}()
	select {
	case result := <-resultCh:
		return result.hash, true, result.err
	case <-time.After(timeout):
		return resumeHashUnknown, false, nil
	}
}

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

type fileWaitRegistry struct {
	mu      sync.Mutex
	waiters map[uint64][]chan struct{}
}

func newFileWaitRegistry() *fileWaitRegistry {
	return &fileWaitRegistry{
		waiters: make(map[uint64][]chan struct{}),
	}
}

func (r *fileWaitRegistry) wait(ctx context.Context, id uint64) bool {
	ch := make(chan struct{})
	r.mu.Lock()
	r.waiters[id] = append(r.waiters[id], ch)
	r.mu.Unlock()
	select {
	case <-ctx.Done():
		return false
	case <-ch:
		return true
	}
}

func (r *fileWaitRegistry) signal(id uint64) {
	r.mu.Lock()
	chans := r.waiters[id]
	delete(r.waiters, id)
	r.mu.Unlock()
	for _, ch := range chans {
		close(ch)
	}
}

const (
	dataChunkHeaderLen = 28
	dataFlagEnd        = 1 << 0
)

type recvChunkData struct {
	index uint32
	n     int
	buf   []byte
	pool  *bufpool.Pool
}

type sendFileState struct {
	key         uint64
	item        manifest.FileItem
	filePath    string
	chunkSize   uint32
	totalChunks uint32
	hashAlg     byte
	readyCh     chan struct{}
	readyOnce   sync.Once
	bytesSent   int64

	mu            sync.Mutex
	nextChunk     uint32
	inFlight      int
	scheduleDone  bool
	endSent       bool
	verifyPending bool
	resendPending bool
	resendChunk   uint32
	readyErr      error
	plan          *resumePlan
	file          *os.File
}

func (s *sendFileState) setReady(err error) {
	s.mu.Lock()
	if s.readyErr == nil && err != nil {
		s.readyErr = err
	}
	s.mu.Unlock()
	s.readyOnce.Do(func() {
		close(s.readyCh)
	})
}

func (s *sendFileState) waitReady(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.readyCh:
	}
	s.mu.Lock()
	err := s.readyErr
	s.mu.Unlock()
	return err
}

func (s *sendFileState) nextChunkToSend() (uint32, uint32, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.scheduleDone {
		if s.resendPending {
			idx := s.resendChunk
			s.resendPending = false
			s.inFlight++
			return idx, chunkSizeForIndex(s.item.Size, s.chunkSize, idx), true
		}
		if !s.verifyPending && s.inFlight == 0 && !s.endSent {
			s.endSent = true
			return 0, 0, true
		}
		return 0, 0, false
	}
	if s.resendPending {
		idx := s.resendChunk
		s.resendPending = false
		s.inFlight++
		return idx, chunkSizeForIndex(s.item.Size, s.chunkSize, idx), true
	}
	for s.nextChunk < s.totalChunks {
		idx := s.nextChunk
		s.nextChunk++
		if s.plan != nil && s.plan.bitmap != nil && s.plan.bitmap.Get(int(idx)) && idx < s.plan.forceSendFrom {
			s.plan.skippedChunks++
			continue
		}
		s.inFlight++
		if s.nextChunk >= s.totalChunks {
			s.scheduleDone = true
		}
		return idx, chunkSizeForIndex(s.item.Size, s.chunkSize, idx), true
	}
	s.scheduleDone = true
	if !s.verifyPending && s.inFlight == 0 && !s.endSent {
		s.endSent = true
		return 0, 0, true
	}
	return 0, 0, false
}

func (s *sendFileState) markChunkDone() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inFlight > 0 {
		s.inFlight--
	}
	if s.verifyPending {
		return false
	}
	if s.scheduleDone && s.inFlight == 0 && !s.endSent {
		s.endSent = true
		return true
	}
	return false
}

func (s *sendFileState) openFile() (*os.File, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.file != nil {
		return s.file, nil
	}
	f, err := os.Open(s.filePath)
	if err != nil {
		return nil, err
	}
	s.file = f
	return f, nil
}

func (s *sendFileState) closeFile() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.file != nil {
		_ = s.file.Close()
		s.file = nil
	}
}

type recvFileStateMux struct {
	key         uint64
	item        manifest.FileItem
	filePath    string
	chunkSize   uint32
	totalChunks uint32
	hashAlg     byte
	sidecar     *Sidecar
	resumeOnce  sync.Once
	resumeInfo  *FileResumeInfo
	fileCache   *fileCache
	baseBytes   int64
	writerOnce  sync.Once
	writerClose sync.Once
	writerCh    chan recvChunkData

	mu           sync.Mutex
	file         *os.File
	remaining    uint32
	endReceived  bool
	done         bool
	bytesRecv    int64
	bytesWritten int64
}

func (s *recvFileStateMux) openFile() (*os.File, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.file != nil {
		return s.file, nil
	}
	var (
		f   *os.File
		err error
	)
	if s.fileCache != nil {
		f, err = s.fileCache.pin(s.filePath)
	} else {
		f, err = os.OpenFile(s.filePath, os.O_RDWR|os.O_CREATE, 0644)
	}
	if err != nil {
		return nil, err
	}
	s.file = f
	return f, nil
}

func (s *recvFileStateMux) closeFile() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.file != nil {
		if s.fileCache != nil {
			s.fileCache.unpin(s.filePath)
		} else {
			_ = s.file.Close()
		}
		s.file = nil
	}
}

func (s *recvFileStateMux) endReceivedNow() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.endReceived
}

func (s *recvFileStateMux) closeWriter() {
	s.writerClose.Do(func() {
		if s.writerCh != nil {
			close(s.writerCh)
		}
	})
}

type fileCache struct {
	mu      sync.Mutex
	max     int
	lru     *list.List
	entries map[string]*fileCacheEntry
}

type fileCacheEntry struct {
	path   string
	file   *os.File
	pinned int
	elem   *list.Element
}

func newFileCache(max int) *fileCache {
	if max < 0 {
		max = 0
	}
	return &fileCache{
		max:     max,
		lru:     list.New(),
		entries: make(map[string]*fileCacheEntry),
	}
}

func (c *fileCache) ensureFile(path string, size int64) error {
	c.mu.Lock()
	if entry, ok := c.entries[path]; ok {
		c.lru.MoveToFront(entry.elem)
		err := entry.file.Truncate(size)
		c.mu.Unlock()
		return err
	}
	c.mu.Unlock()

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	if err := f.Truncate(size); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

func (c *fileCache) pin(path string) (*os.File, error) {
	c.mu.Lock()
	if entry, ok := c.entries[path]; ok {
		entry.pinned++
		c.lru.MoveToFront(entry.elem)
		f := entry.file
		c.mu.Unlock()
		return f, nil
	}
	c.mu.Unlock()

	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	entry := &fileCacheEntry{path: path, file: f, pinned: 1}
	c.mu.Lock()
	entry.elem = c.lru.PushFront(entry)
	c.entries[path] = entry
	c.evictLocked()
	c.mu.Unlock()
	return f, nil
}

func (c *fileCache) unpin(path string) {
	c.mu.Lock()
	if entry, ok := c.entries[path]; ok {
		if entry.pinned > 0 {
			entry.pinned--
		}
		c.evictLocked()
	}
	c.mu.Unlock()
}

func (c *fileCache) closeAll() {
	c.mu.Lock()
	for _, entry := range c.entries {
		_ = entry.file.Close()
	}
	c.entries = make(map[string]*fileCacheEntry)
	c.lru.Init()
	c.mu.Unlock()
}

func (c *fileCache) evictLocked() {
	if c.max == 0 {
		return
	}
	for c.lru.Len() > c.max {
		var victim *list.Element
		for e := c.lru.Back(); e != nil; e = e.Prev() {
			entry := e.Value.(*fileCacheEntry)
			if entry.pinned == 0 {
				victim = e
				break
			}
		}
		if victim == nil {
			return
		}
		entry := victim.Value.(*fileCacheEntry)
		delete(c.entries, entry.path)
		c.lru.Remove(victim)
		_ = entry.file.Close()
	}
}

func (s *recvFileStateMux) markChunkComplete(idx uint32, chunkLen uint32) (bool, int64) {
	var added int64
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sidecar != nil {
		if s.sidecar.MarkCompleteIfUnset(idx) {
			if s.remaining > 0 {
				s.remaining--
			}
			added = int64(chunkLen)
		}
	} else {
		if s.remaining > 0 {
			s.remaining--
			added = int64(chunkLen)
		}
	}
	if s.remaining == 0 {
		return true, added
	}
	return false, added
}

func (s *recvFileStateMux) markEndReceived() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.endReceived = true
	if s.remaining == 0 {
		return true
	}
	return false
}

func fileKeyForItem(item manifest.FileItem) uint64 {
	h := fnv.New64a()
	if item.ID != "" {
		_, _ = h.Write([]byte(item.ID))
	} else {
		_, _ = h.Write([]byte(item.RelPath))
	}
	return h.Sum64()
}

func chunkTotal(fileSize int64, chunkSize uint32) uint32 {
	if chunkSize == 0 {
		return 0
	}
	if fileSize <= 0 {
		return 0
	}
	return uint32((fileSize + int64(chunkSize) - 1) / int64(chunkSize))
}

func chunkSizeForIndex(fileSize int64, chunkSize uint32, idx uint32) uint32 {
	if chunkSize == 0 {
		return 0
	}
	offset := int64(idx) * int64(chunkSize)
	if offset >= fileSize {
		return 0
	}
	remaining := fileSize - offset
	if remaining < int64(chunkSize) {
		return uint32(remaining)
	}
	return chunkSize
}

func writeChunkFrame(ctx context.Context, s Stream, state *sendFileState, chunkIndex uint32, chunkLen uint32, chunkCRC uint32, data []byte, progressDeltaFn ProgressDeltaFn) error {
	var header [dataChunkHeaderLen]byte
	binary.BigEndian.PutUint64(header[0:8], state.key)
	binary.BigEndian.PutUint32(header[8:12], chunkIndex)
	binary.BigEndian.PutUint32(header[12:16], chunkLen)
	binary.BigEndian.PutUint32(header[16:20], chunkCRC)
	binary.BigEndian.PutUint32(header[20:24], state.chunkSize)
	header[24] = state.hashAlg
	header[25] = 0
	if err := writeFullWithTimeout(ctx, s, header[:], state.item.RelPath, "mux-header"); err != nil {
		return err
	}
	if err := writeFullWithTimeoutDelta(ctx, s, data[:chunkLen], state.item.RelPath, "mux-data", progressDeltaFn); err != nil {
		return err
	}
	return nil
}

func writeEndFrame(ctx context.Context, s Stream, state *sendFileState) error {
	var header [dataChunkHeaderLen]byte
	binary.BigEndian.PutUint64(header[0:8], state.key)
	binary.BigEndian.PutUint32(header[8:12], 0)
	binary.BigEndian.PutUint32(header[12:16], 0)
	binary.BigEndian.PutUint32(header[16:20], 0)
	binary.BigEndian.PutUint32(header[20:24], state.chunkSize)
	header[24] = state.hashAlg
	header[25] = dataFlagEnd
	if err := writeFullWithTimeout(ctx, s, header[:], state.item.RelPath, "mux-end"); err != nil {
		return err
	}
	return nil
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

func resetSidecarForMissingFile(sc *Sidecar, fileID string, fileSize int64, chunkSize uint32) (*Sidecar, error) {
	if sc == nil {
		return nil, nil
	}
	globalSidecarFlushRegistry.remove(sc)
	_ = os.Remove(sc.Path)
	fresh, err := CreateSidecar(sc.Path, fileID, fileSize, chunkSize)
	if err != nil {
		return nil, err
	}
	globalSidecarFlushRegistry.add(fresh)
	return fresh, nil
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
	parallelStreams := currentParams.ParallelFiles
	if parallelStreams < 1 {
		parallelStreams = 1
	}
	resumeEnabled := opts.Resume
	resumeTimeout := opts.ResumeTimeout
	if resumeTimeout < 0 {
		resumeTimeout = 0
	}
	resumeVerify := "none"
	resumeVerifyTail := uint32(0)
	hashAlg := HashAlgNone

	controlStream, err := conn.OpenStream(ctx)
	if err != nil {
		return fmt.Errorf("failed to open control stream: %w", err)
	}
	defer controlStream.Close()

	if err := writeControlHeader(controlStream, m); err != nil {
		return err
	}

	if err := writeDataStreams(controlStream, DataStreams{Count: uint16(parallelStreams)}); err != nil {
		return err
	}
	dataStreams := make([]Stream, parallelStreams)
	for i := 0; i < parallelStreams; i++ {
		stream, err := conn.OpenStream(ctx)
		if err != nil {
			return fmt.Errorf("failed to open data stream: %w", err)
		}
		dataStreams[i] = stream
	}

	doneRegistry := newFileDoneRegistry()
	resumeRegistry := newResumeInfoRegistry()
	var snapshotMu sync.Mutex
	var snapshotPending map[uint64]struct{}
	var snapshotDone chan struct{}
	ackErrChan := make(chan error, 1)

	ackCtx, ackCancel := context.WithCancel(ctx)
	defer ackCancel()

	fileItems := make([]manifest.FileItem, 0, len(m.Items))
	var remainingBytes int64
	for _, item := range m.Items {
		if item.IsDir {
			continue
		}
		fileItems = append(fileItems, item)
		remainingBytes += item.Size
	}

	sched := scheduler.NewHybridScheduler(scheduler.PolicyConfig{
		ParallelFiles:   parallelStreams,
		SmallThreshold:  opts.SmallThreshold,
		MediumThreshold: opts.MediumThreshold,
		SmallSlotFrac:   opts.SmallSlotFrac,
		AgingAfter:      opts.AgingAfter,
	})
	metaByRelPath := make(map[string]scheduler.FileMeta)
	keyByRelPath := make(map[string]scheduler.FileKey)

	now := time.Now()
	for _, item := range fileItems {
		key := scheduler.FileKey{StreamID: fileKeyForItem(item), RelPath: item.RelPath}
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

	stateByRelPath := make(map[string]*sendFileState)
	for _, item := range fileItems {
		filePath := filepath.Join(rootPath, filepath.FromSlash(item.RelPath))
		if opts.ResolveFilePath != nil {
			if resolved := opts.ResolveFilePath(item.RelPath); resolved != "" {
				filePath = resolved
			}
		}
		params := resolveParams()
		chunkSize := params.ChunkSize
		if chunkSize == 0 {
			chunkSize = DefaultChunkSize
		}
		state := &sendFileState{
			key:         fileKeyForItem(item),
			item:        item,
			filePath:    filePath,
			chunkSize:   chunkSize,
			totalChunks: chunkTotal(item.Size, chunkSize),
			hashAlg:     hashAlg,
			readyCh:     make(chan struct{}),
		}
		stateByRelPath[item.RelPath] = state
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

			handleControl := func(typ byte, payload any) bool {
				switch typ {
				case controlTypeFileDone:
					doneRegistry.deliver(payload.(FileDone))
					return true
				case controlTypeFileResumeInfo:
					info := payload.(FileResumeInfo)
					resumeRegistry.deliver(info)
					snapshotMu.Lock()
					if snapshotPending != nil {
						if _, ok := snapshotPending[info.StreamID]; ok {
							delete(snapshotPending, info.StreamID)
							if len(snapshotPending) == 0 && snapshotDone != nil {
								close(snapshotDone)
								snapshotDone = nil
								snapshotPending = nil
							}
						}
					}
					snapshotMu.Unlock()
					return true
				default:
					select {
					case ackErrChan <- fmt.Errorf("unexpected control message type: 0x%02x", typ):
					default:
					}
					return false
				}
			}

			if msgType == controlTypeBatch {
				batch := msg.(ControlBatch)
				for _, entry := range batch.Entries {
					if !handleControl(entry.Type, entry.Msg) {
						return
					}
				}
				continue
			}

			if !handleControl(msgType, msg) {
				return
			}
		}
	}()

	var statsMu sync.Mutex
	activeCount := 0
	completedCount := 0
	totalFiles := len(fileItems)
	doneCh := make(chan struct{})
	var doneOnce sync.Once
	signalDone := func() {
		doneOnce.Do(func() { close(doneCh) })
	}
	updateStats := func(active, completed int, remaining int64) {
		if opts.TransferStatsFn != nil {
			opts.TransferStatsFn(active, completed, remaining)
		}
	}

	scheduleWake := make(chan struct{}, parallelStreams*4+4)
	signalWake := func() {
		select {
		case scheduleWake <- struct{}{}:
		default:
		}
	}

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

	controlWriteCh := make(chan ControlEntry, parallelStreams*4+4)
	controlWriteErr := make(chan error, 1)
	controlWriteDone := make(chan struct{})
	go func() {
		defer close(controlWriteDone)
		batch := make([]ControlEntry, 0, controlBatchMax)
		timer := time.NewTimer(controlBatchFlushInterval)
		timer.Stop()
		timerActive := false
		flush := func() bool {
			if len(batch) == 0 {
				return true
			}
			if err := writeControlBatch(controlStream, ControlBatch{Entries: batch}); err != nil {
				select {
				case controlWriteErr <- err:
				default:
				}
				return false
			}
			batch = batch[:0]
			return true
		}
		for {
			select {
			case <-transferCtx.Done():
				_ = flush()
				return
			case entry, ok := <-controlWriteCh:
				if !ok {
					_ = flush()
					return
				}
				batch = append(batch, entry)
				if len(batch) >= controlBatchMax {
					if !flush() {
						return
					}
				}
				if !timerActive {
					timer.Reset(controlBatchFlushInterval)
					timerActive = true
				}
			case <-timer.C:
				timerActive = false
				if !flush() {
					return
				}
			}
		}
	}()
	go func() {
		select {
		case err := <-controlWriteErr:
			setErr(err)
		case <-transferCtx.Done():
		}
	}()
	sendControl := func(typ byte, msg any) error {
		select {
		case controlWriteCh <- ControlEntry{Type: typ, Msg: msg}:
			return nil
		case <-transferCtx.Done():
			return transferCtx.Err()
		}
	}

	useResumeSnapshot := resumeEnabled
	if resumeEnabled {
		entries := make([]ResumeSnapshotEntry, 0, len(fileItems))
		for _, item := range fileItems {
			state := stateByRelPath[item.RelPath]
			if state == nil {
				continue
			}
			chunkSize := state.chunkSize
			if chunkSize == 0 {
				chunkSize = DefaultChunkSize
				state.chunkSize = chunkSize
				state.totalChunks = chunkTotal(state.item.Size, chunkSize)
			}
			entries = append(entries, ResumeSnapshotEntry{
				FileID:    state.item.ID,
				StreamID:  state.key,
				ChunkSize: chunkSize,
				HashAlg:   hashAlg,
			})
		}
		if len(entries) > 0 {
			snapshotMu.Lock()
			snapshotPending = make(map[uint64]struct{}, len(entries))
			for _, entry := range entries {
				snapshotPending[entry.StreamID] = struct{}{}
			}
			snapshotDone = make(chan struct{})
			snapshotMu.Unlock()
			if err := sendControl(controlTypeResumeSnapshotRequest, ResumeSnapshotRequest{Entries: entries}); err != nil {
				useResumeSnapshot = false
				snapshotMu.Lock()
				if snapshotDone != nil {
					close(snapshotDone)
				}
				snapshotDone = nil
				snapshotPending = nil
				snapshotMu.Unlock()
				setErr(err)
			}
		} else {
			useResumeSnapshot = false
		}
	}
	if useResumeSnapshot && snapshotDone != nil {
		waitTimeout := resumeTimeout
		if waitTimeout <= 0 {
			waitTimeout = defaultResumeHashTimeout
		}
		timer := time.NewTimer(waitTimeout)
		defer timer.Stop()
		select {
		case <-transferCtx.Done():
		case <-snapshotDone:
		case <-timer.C:
		}
	}

	activeFiles := make([]*sendFileState, 0, parallelStreams)
	activeIdx := 0
	var schedMu sync.Mutex

	startResume := func(state *sendFileState, chunkSize uint32) {
		if !resumeEnabled || state.item.ID == "" || state.totalChunks == 0 {
			state.setReady(nil)
			signalWake()
			return
		}
		go func() {
			if !useResumeSnapshot {
				reqErr := sendControl(controlTypeResumeRequest, ResumeRequest{
					FileID:   state.item.ID,
					StreamID: state.key,
				})
				if reqErr != nil {
					state.setReady(reqErr)
					setErr(reqErr)
					signalWake()
					return
				}
			}

			resumeCtx := transferCtx
			var resumeCancel context.CancelFunc
			if resumeTimeout > 0 {
				resumeCtx, resumeCancel = context.WithTimeout(transferCtx, resumeTimeout)
			}
			if resumeCancel != nil {
				defer resumeCancel()
			}
			infoCh := make(chan FileResumeInfo, 1)
			errCh := make(chan error, 1)
			go func() {
				info, resumeErr := resumeRegistry.wait(resumeCtx, state.key)
				if resumeErr != nil {
					errCh <- resumeErr
					return
				}
				infoCh <- info
			}()

			applyResumeInfo := func(info FileResumeInfo) error {
				if info.FileID != "" && info.FileID != state.item.ID {
					return fmt.Errorf("resume info file id mismatch for %s", state.item.RelPath)
				}

				totalChunks := info.TotalChunks
				if totalChunks == 0 {
					totalChunks = state.totalChunks
				}
				if state.totalChunks != 0 && totalChunks != state.totalChunks {
					return fmt.Errorf("resume info total chunks mismatch for %s", state.item.RelPath)
				}

				var plan *resumePlan
				if totalChunks > 0 && len(info.Bitmap) > 0 {
					bitmap, err := BitmapFromBytes(info.Bitmap, int(totalChunks))
					if err != nil {
						return err
					}
					completedChunks := uint32(bitmap.CountSet())
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
					hashUnknown := info.LastVerifiedHash == resumeHashUnknown
					verifyNeeded := verifyMode != "none" && verifiedChunk < totalChunks && hashAlg != HashAlgNone && !hashUnknown

					allComplete := totalChunks > 0 && completedChunks >= totalChunks
					if allComplete {
						verifyNeeded = false
					}
					if !allComplete {
						tail := resumeVerifyTail
						if tail > 0 && forceSendFrom > 0 {
							if tail >= forceSendFrom {
								forceSendFrom = 0
							} else {
								forceSendFrom -= tail
							}
						}
					}

					if forceSendFrom > totalChunks {
						forceSendFrom = totalChunks
					}
					if hashUnknown && totalChunks > 0 && resumeVerifyTail > 0 && verifiedChunk < totalChunks {
						tail := resumeVerifyTail
						minForce := uint32(0)
						if totalChunks > tail {
							minForce = totalChunks - tail
						}
						if forceSendFrom > minForce {
							forceSendFrom = minForce
						}
					}

					plan = &resumePlan{
						bitmap:        bitmap,
						forceSendFrom: forceSendFrom,
						totalChunks:   totalChunks,
						verifiedChunk: verifiedChunk,
					}
					plannedSkipped := uint32(0)
					if forceSendFrom > 0 {
						for i := uint32(0); i < forceSendFrom; i++ {
							if bitmap.Get(int(i)) {
								plannedSkipped++
							}
						}
					}
					if verifyNeeded {
						state.mu.Lock()
						state.verifyPending = true
						state.mu.Unlock()
						go func(vChunk uint32, vHash uint64) {
							senderHash, err := hashFileChunk(state.filePath, vChunk, chunkSize, state.item.Size, hashAlg)
							state.mu.Lock()
							if err != nil {
								state.readyErr = err
								state.verifyPending = false
								state.mu.Unlock()
								setErr(err)
								signalWake()
								return
							}
							if senderHash != vHash {
								state.resendChunk = vChunk
								state.resendPending = true
							}
							state.verifyPending = false
							state.mu.Unlock()
							signalWake()
						}(verifiedChunk, info.LastVerifiedHash)
					}
					if opts.ResumeStatsFn != nil {
						opts.ResumeStatsFn(state.item.RelPath, plannedSkipped, totalChunks, verifiedChunk, state.item.Size, chunkSize)
					}
				}

				state.mu.Lock()
				state.plan = plan
				state.mu.Unlock()
				return nil
			}

			readySet := false
			grace := time.NewTimer(resumeGracePeriod)
			defer grace.Stop()
			for {
				select {
				case <-transferCtx.Done():
					if !readySet {
						state.setReady(nil)
						signalWake()
					}
					return
				case <-grace.C:
					if !readySet {
						state.setReady(nil)
						readySet = true
						signalWake()
					}
				case info := <-infoCh:
					if err := applyResumeInfo(info); err != nil {
						state.setReady(err)
						setErr(err)
						signalWake()
						return
					}
					if !readySet {
						state.setReady(nil)
						readySet = true
						signalWake()
					}
					return
				case resumeErr := <-errCh:
					if !errors.Is(resumeErr, context.DeadlineExceeded) && !errors.Is(resumeErr, context.Canceled) {
						state.setReady(resumeErr)
						setErr(resumeErr)
					} else if !readySet {
						state.setReady(nil)
						readySet = true
					}
					signalWake()
					return
				}
			}
		}()
	}

	activateNext := func() *sendFileState {
		key, ok := sched.Next(time.Now())
		if !ok {
			return nil
		}
		state, ok := stateByRelPath[key.RelPath]
		if !ok {
			setErr(fmt.Errorf("missing manifest item for %s", key.RelPath))
			return nil
		}

		params := resolveParams()
		if opts.OnFileStart != nil {
			opts.OnFileStart(state.item.RelPath, state.item.Size, params)
		}
		state.mu.Lock()
		if state.chunkSize == 0 {
			chunkSize := params.ChunkSize
			if chunkSize == 0 {
				chunkSize = DefaultChunkSize
			}
			state.chunkSize = chunkSize
			state.totalChunks = chunkTotal(state.item.Size, chunkSize)
		}
		state.mu.Unlock()

		meta := metaByRelPath[state.item.RelPath]
		now := time.Now()
		meta.StartedAt = now
		meta.LastScheduledAt = now
		sched.Add(key, meta)
		metaByRelPath[state.item.RelPath] = meta

		statsMu.Lock()
		activeCount++
		active := activeCount
		completed := completedCount
		remaining := remainingBytes
		statsMu.Unlock()
		updateStats(active, completed, remaining)

		startResume(state, state.chunkSize)
		return state
	}

	sendFileEnd := func(state *sendFileState, stream Stream) {
		if err := writeEndFrame(transferCtx, stream, state); err != nil {
			setErr(err)
			return
		}
		// Free the file slot immediately; FileDone becomes verification-only.
		schedMu.Lock()
		for i := 0; i < len(activeFiles); i++ {
			if activeFiles[i] == state {
				activeFiles = append(activeFiles[:i], activeFiles[i+1:]...)
				if activeIdx >= len(activeFiles) {
					activeIdx = 0
				}
				break
			}
		}
		if key, ok := keyByRelPath[state.item.RelPath]; ok {
			sched.Remove(key)
		}
		schedMu.Unlock()

		state.closeFile()

		statsMu.Lock()
		activeCount--
		completedCount++
		remainingBytes -= state.item.Size
		active := activeCount
		completed := completedCount
		remaining := remainingBytes
		statsMu.Unlock()
		updateStats(active, completed, remaining)
		if totalFiles > 0 && completed >= totalFiles {
			signalDone()
		}
		signalWake()

		go func() {
			fileDone, err := doneRegistry.wait(transferCtx, state.key)
			if err != nil {
				setErr(err)
				return
			}
			if !fileDone.OK {
				if opts.FileDoneFn != nil {
					opts.FileDoneFn(state.item.RelPath, false)
				}
				if fileDone.ErrMsg == "" {
					setErr(fmt.Errorf("receiver reported failure for %s", state.item.RelPath))
				} else {
					setErr(fmt.Errorf("receiver reported failure for %s: %s", state.item.RelPath, fileDone.ErrMsg))
				}
				return
			}
			if opts.FileDoneFn != nil {
				opts.FileDoneFn(state.item.RelPath, true)
			}
		}()
	}

	nextTask := func(ctx context.Context) (*sendFileState, uint32, uint32, bool) {
		for {
			if ctx.Err() != nil {
				return nil, 0, 0, false
			}
			select {
			case <-doneCh:
				return nil, 0, 0, false
			default:
			}
			schedMu.Lock()
			for len(activeFiles) < parallelStreams {
				state := activateNext()
				if state == nil {
					break
				}
				activeFiles = append(activeFiles, state)
			}

			n := len(activeFiles)
			for i := 0; i < n; i++ {
				if len(activeFiles) == 0 {
					break
				}
				if activeIdx >= len(activeFiles) {
					activeIdx = 0
				}
				state := activeFiles[activeIdx]
				activeIdx = (activeIdx + 1) % len(activeFiles)
				if state == nil {
					continue
				}
				select {
				case <-state.readyCh:
				default:
					continue
				}
				if err := state.waitReady(ctx); err != nil {
					schedMu.Unlock()
					setErr(err)
					return nil, 0, 0, false
				}
				idx, size, ok := state.nextChunkToSend()
				if ok {
					schedMu.Unlock()
					return state, idx, size, true
				}
			}
			schedMu.Unlock()

			statsMu.Lock()
			doneCount := completedCount
			statsMu.Unlock()
			if doneCount >= totalFiles && totalFiles > 0 {
				return nil, 0, 0, false
			}
			if totalFiles == 0 {
				return nil, 0, 0, false
			}
			select {
			case <-ctx.Done():
				return nil, 0, 0, false
			case <-doneCh:
				return nil, 0, 0, false
			case <-scheduleWake:
			case <-time.After(200 * time.Millisecond):
			}
		}
	}

	var wg sync.WaitGroup
	for _, stream := range dataStreams {
		wg.Add(1)
		go func(s Stream) {
			defer wg.Done()
			defer s.Close()
			for {
				state, chunkIndex, chunkLen, ok := nextTask(transferCtx)
				if !ok {
					return
				}
				if transferCtx.Err() != nil {
					return
				}
				f, err := state.openFile()
				if err != nil {
					setErr(fmt.Errorf("failed to open file %s: %w", state.item.RelPath, err))
					return
				}
				if chunkLen == 0 {
					sendFileEnd(state, s)
					continue
				}
				bufPool := chunkPoolFor(state.chunkSize)
				if bufPool == nil {
					bufPool = bufpool.New(int(state.chunkSize))
				}
				buf := bufPool.Get()
				if int(chunkLen) > len(buf) {
					bufPool.Put(buf)
					setErr(fmt.Errorf("chunk length %d exceeds buffer size %d", chunkLen, len(buf)))
					return
				}
				offset := int64(chunkIndex) * int64(state.chunkSize)
				n, err := readAtWithPool(transferCtx, f, offset, buf[:chunkLen])
				if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
					bufPool.Put(buf)
					setErr(fmt.Errorf("failed to read file %s: %w", state.item.RelPath, err))
					return
				}
				if n != int(chunkLen) {
					bufPool.Put(buf)
					if err == nil {
						err = io.ErrUnexpectedEOF
					}
					setErr(fmt.Errorf("short read for %s: got %d want %d", state.item.RelPath, n, chunkLen))
					return
				}
				if err := writeChunkFrame(transferCtx, s, state, chunkIndex, uint32(n), 0, buf[:n], opts.ProgressDeltaFn); err != nil {
					bufPool.Put(buf)
					setErr(err)
					return
				}
				bufPool.Put(buf)

				if opts.ProgressFn != nil {
					newTotal := atomic.AddInt64(&state.bytesSent, int64(n))
					opts.ProgressFn(state.item.RelPath, newTotal, state.item.Size)
				}

				if state.markChunkDone() {
					sendFileEnd(state, s)
				}
				signalWake()
			}
		}(stream)
	}

	wg.Wait()
	close(controlWriteCh)
	<-controlWriteDone
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
func RecvManifestMultiStreamLegacy(ctx context.Context, conn Conn, outDir string, opts Options) (manifest.Manifest, error) {
	controlStream, err := conn.AcceptStream(ctx)
	if err != nil {
		return manifest.Manifest{}, fmt.Errorf("failed to accept control stream: %w", err)
	}
	defer controlStream.Close()

	m, err := readControlHeader(controlStream)
	if err != nil {
		return m, err
	}

	rootedDir := filepath.Join(outDir, m.Root)
	baseDir := outDir
	if !opts.NoRootDir {
		baseDir = rootedDir
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

	type recvFileAggregate struct {
		relPath         string
		size            uint64
		chunkSize       uint32
		stripeCount     int
		stripesDone     int
		stripesSeen     map[uint16]struct{}
		active          bool
		failed          bool
		progressTotal   int64
		sidecar         *Sidecar
		resumeStatsOnce sync.Once
		mu              sync.Mutex
	}

	fileAggByRelPath := make(map[string]*recvFileAggregate)

	type recvFileState struct {
		begin        FileBegin
		hasEnd       bool
		hasData      bool
		lastProgress time.Time
		stream       Stream
		resume       *resumeState
		resumeInfo   *FileResumeInfo
		computedCRC  uint32
		fileAgg      *recvFileAggregate
	}
	stateMu := sync.Mutex{}
	stateByStream := make(map[uint64]*recvFileState)
	sem := make(chan struct{}, parallelFiles)
	var wg sync.WaitGroup
	var dirMu sync.Mutex
	queueCap := len(fileItems)
	if queueCap < 1 {
		queueCap = 1
	}
	if opts.ParallelFiles > 1 {
		queueCap = queueCap * opts.ParallelFiles
	}
	beginQueue := make(chan FileBegin, queueCap)
	schedulerDone := make(chan struct{})
	type controlMsg struct {
		done   *FileDone
		resume *FileResumeInfo
	}
	controlWriteCap := parallelFiles * 8
	if remainingFiles > 0 {
		controlWriteCap = remainingFiles * 2
		if controlWriteCap < parallelFiles*8 {
			controlWriteCap = parallelFiles * 8
		}
		if controlWriteCap < 64 {
			controlWriteCap = 64
		}
		if controlWriteCap > 8192 {
			controlWriteCap = 8192
		}
	}
	controlWriteCh := make(chan controlMsg, controlWriteCap)
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

	buildResumeInfo := func(begin FileBegin, item manifest.FileItem, fileAgg *recvFileAggregate) (*FileResumeInfo, *resumeState, error) {
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
		var sidecar *Sidecar
		if fileAgg != nil {
			fileAgg.mu.Lock()
			sidecar = fileAgg.sidecar
			if sidecar == nil {
				primary := SidecarPath(baseDir, "", sidecarIdentifier(item))
				fallback := ""
				if rootedDir != baseDir {
					fallback = SidecarPath(rootedDir, "", sidecarIdentifier(item))
				}
				loaded, err := LoadOrCreateSidecarWithFallback(primary, fallback, item.ID, int64(begin.FileSize), begin.ChunkSize)
				if err != nil {
					fileAgg.mu.Unlock()
					return nil, nil, fmt.Errorf("failed to load sidecar: %w", err)
				}
				globalSidecarFlushRegistry.add(loaded)
				fileAgg.sidecar = loaded
				sidecar = loaded
			}
			fileAgg.mu.Unlock()
		} else {
			primary := SidecarPath(baseDir, "", sidecarIdentifier(item))
			fallback := ""
			if rootedDir != baseDir {
				fallback = SidecarPath(rootedDir, "", sidecarIdentifier(item))
			}
			loaded, err := LoadOrCreateSidecarWithFallback(primary, fallback, item.ID, int64(begin.FileSize), begin.ChunkSize)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to load sidecar: %w", err)
			}
			globalSidecarFlushRegistry.add(loaded)
			sidecar = loaded
		}
		state.sidecar = sidecar
		highest, hasAny := sidecar.HighestComplete()
		if hasAny {
			if _, statErr := os.Stat(filePath); errors.Is(statErr, os.ErrNotExist) {
				fresh, resetErr := resetSidecarForMissingFile(sidecar, item.ID, int64(begin.FileSize), begin.ChunkSize)
				if resetErr != nil {
					return nil, nil, resetErr
				}
				if fileAgg != nil {
					fileAgg.mu.Lock()
					fileAgg.sidecar = fresh
					fileAgg.mu.Unlock()
				}
				sidecar = fresh
				state.sidecar = fresh
				info.Bitmap = nil
				info.LastVerifiedChunk = 0
				info.LastVerifiedHash = 0
				state.verifiedChunk = 0
				state.hasVerified = false
				hasAny = false
				highest = 0
			}
		}
		if hasAny {
			info.Bitmap = sidecar.MarshalBitmap()
			completedChunks := uint32(sidecar.bitmap.CountSet())
			allComplete := totalChunks > 0 && completedChunks >= totalChunks
			if allComplete {
				info.LastVerifiedChunk = totalChunks
			} else {
				info.LastVerifiedChunk = uint32(highest)
				if begin.HashAlg != HashAlgNone {
					hashValue, ok, err := hashFileChunkWithTimeout(filePath, uint32(highest), begin.ChunkSize, int64(begin.FileSize), begin.HashAlg, defaultResumeHashTimeout)
					if err != nil {
						if errors.Is(err, os.ErrNotExist) {
							fresh, resetErr := resetSidecarForMissingFile(sidecar, item.ID, int64(begin.FileSize), begin.ChunkSize)
							if resetErr != nil {
								return nil, nil, resetErr
							}
							if fileAgg != nil {
								fileAgg.mu.Lock()
								fileAgg.sidecar = fresh
								fileAgg.mu.Unlock()
							}
							sidecar = fresh
							state.sidecar = fresh
							info.Bitmap = nil
							info.LastVerifiedChunk = 0
							info.LastVerifiedHash = 0
							state.verifiedChunk = 0
							state.hasVerified = false
							hasAny = false
						} else {
							return nil, nil, err
						}
					}
					if hasAny {
						if ok {
							info.LastVerifiedHash = hashValue
							state.verifiedChunk = info.LastVerifiedChunk
							state.hasVerified = true
						} else {
							info.LastVerifiedHash = resumeHashUnknown
						}
					}
				}
			}
		}
		if opts.ResumeStatsFn != nil && totalChunks > 0 {
			callResume := func() {
				skippedChunks := uint32(0)
				if highest, ok := sidecar.HighestContiguous(); ok && highest >= 0 {
					skippedChunks = uint32(highest + 1)
				}
				if skippedChunks > totalChunks {
					skippedChunks = totalChunks
				}
				opts.ResumeStatsFn(begin.RelPath, skippedChunks, totalChunks, info.LastVerifiedChunk, int64(begin.FileSize), begin.ChunkSize)
			}
			if fileAgg != nil {
				fileAgg.resumeStatsOnce.Do(callResume)
			} else {
				callResume()
			}
			bitmapSnapshot := sidecar.MarshalBitmap()
			go func(relpath string, total uint32, totalBytes int64, chunkSize uint32) {
				if len(bitmapSnapshot) == 0 || total == 0 {
					return
				}
				bm, err := BitmapFromBytes(bitmapSnapshot, int(total))
				if err != nil {
					return
				}
				skipped := uint32(bm.CountSet())
				if skipped > total {
					skipped = total
				}
				opts.ResumeStatsFn(relpath, skipped, total, info.LastVerifiedChunk, totalBytes, chunkSize)
			}(begin.RelPath, totalChunks, int64(begin.FileSize), begin.ChunkSize)
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
				fileAgg := state.fileAgg
				if fileAgg == nil {
					setRecvErr(fmt.Errorf("missing file state for stream %d", begin.StreamID))
					<-sem
					return
				}
				started := false
				fileAgg.mu.Lock()
				if !fileAgg.active {
					fileAgg.active = true
					started = true
				}
				fileAgg.mu.Unlock()
				if started {
					statsMu.Lock()
					activeCount++
					active := activeCount
					completed := completedCount
					remaining := remainingBytes
					statsMu.Unlock()
					updateStats(active, completed, remaining)
				}

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
					progressDeltaFn := opts.ProgressDeltaFn
					var stripeProgressFn ProgressFn = progressFn
					var stripeProgressDeltaFn ProgressDeltaFn = progressDeltaFn
					if progressFn != nil && state.fileAgg != nil && state.fileAgg.stripeCount > 1 {
						var lastProgress int64
						stripeProgressFn = func(relpath string, bytesReceived int64, total int64) {
							delta := bytesReceived - lastProgress
							if delta <= 0 {
								return
							}
							lastProgress = bytesReceived
							newTotal := atomic.AddInt64(&state.fileAgg.progressTotal, delta)
							progressFn(relpath, newTotal, total)
						}
					}
					if progressDeltaFn != nil && state.fileAgg != nil && state.fileAgg.stripeCount > 1 {
						stripeProgressDeltaFn = func(relpath string, delta int64) {
							if delta <= 0 {
								return
							}
							atomic.AddInt64(&state.fileAgg.progressTotal, delta)
							progressDeltaFn(relpath, delta)
						}
					}

					computedCRC, err := receiveFileChunksWindowed(
						recvCtx,
						dataStream,
						state.begin.RelPath,
						filePath,
						state.begin.FileSize,
						state.begin.ChunkSize,
						stripeProgressFn,
						stripeProgressDeltaFn,
						state.resume,
						state.begin.StripeStart,
						state.begin.StripeChunks,
					)
					if err != nil {
						if state.fileAgg != nil {
							state.fileAgg.mu.Lock()
							shouldReport := !state.fileAgg.failed
							state.fileAgg.failed = true
							state.fileAgg.mu.Unlock()
							if shouldReport && opts.FileDoneFn != nil {
								opts.FileDoneFn(state.begin.RelPath, false)
							}
						} else if opts.FileDoneFn != nil {
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
					if state.fileAgg != nil {
						state.fileAgg.mu.Lock()
						state.fileAgg.stripesDone++
						done := state.fileAgg.stripesDone >= state.fileAgg.stripeCount && !state.fileAgg.failed
						state.fileAgg.mu.Unlock()
						if done && opts.FileDoneFn != nil {
							opts.FileDoneFn(state.begin.RelPath, true)
						}
					} else if opts.FileDoneFn != nil {
						opts.FileDoneFn(state.begin.RelPath, true)
					}
					stateMu.Lock()
					state.stream = nil
					stateMu.Unlock()

					if state.fileAgg != nil {
						state.fileAgg.mu.Lock()
						done := state.fileAgg.stripesDone >= state.fileAgg.stripeCount
						state.fileAgg.mu.Unlock()
						if done {
							statsMu.Lock()
							activeCount--
							completedCount++
							remainingBytes -= int64(state.begin.FileSize)
							active := activeCount
							completed := completedCount
							remaining := remainingBytes
							statsMu.Unlock()
							updateStats(active, completed, remaining)
						}
					} else {
						statsMu.Lock()
						activeCount--
						completedCount++
						remainingBytes -= int64(state.begin.FileSize)
						active := activeCount
						completed := completedCount
						remaining := remainingBytes
						statsMu.Unlock()
						updateStats(active, completed, remaining)
					}
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

		handleControl := func(typ byte, payload any) (bool, error) {
			switch typ {
			case controlTypeFileBegin:
				begin := payload.(FileBegin)
				if err := validateRelPath(begin.RelPath); err != nil {
					return false, err
				}
				expectedSize, ok := expectedFiles[begin.RelPath]
				if !ok {
					return false, fmt.Errorf("manifest mismatch: unexpected file %s size %d", begin.RelPath, begin.FileSize)
				}
				if expectedSize != int64(begin.FileSize) {
					return false, fmt.Errorf("manifest mismatch: expected %s size %d, got %s size %d", begin.RelPath, expectedSize, begin.RelPath, begin.FileSize)
				}
				stripeCount := int(begin.StripeCount)
				if stripeCount < 1 {
					stripeCount = 1
				}
				if stripeCount > 1 {
					if begin.StripeChunks == 0 {
						return false, fmt.Errorf("invalid stripe chunk count for %s", begin.RelPath)
					}
					if int(begin.StripeIndex) >= stripeCount {
						return false, fmt.Errorf("invalid stripe index %d for %s", begin.StripeIndex, begin.RelPath)
					}
				} else {
					begin.StripeIndex = 0
					begin.StripeStart = 0
					begin.StripeChunks = 0
				}

				fileAgg, ok := fileAggByRelPath[begin.RelPath]
				if !ok {
					fileAgg = &recvFileAggregate{
						relPath:     begin.RelPath,
						size:        begin.FileSize,
						chunkSize:   begin.ChunkSize,
						stripeCount: stripeCount,
						stripesSeen: make(map[uint16]struct{}),
					}
					fileAggByRelPath[begin.RelPath] = fileAgg
					remainingFiles--
				} else {
					if fileAgg.size != begin.FileSize || fileAgg.chunkSize != begin.ChunkSize {
						return false, fmt.Errorf("manifest mismatch: stripe size differs for %s", begin.RelPath)
					}
					if fileAgg.stripeCount != stripeCount {
						return false, fmt.Errorf("manifest mismatch: stripe count differs for %s", begin.RelPath)
					}
				}
				fileAgg.mu.Lock()
				if _, seen := fileAgg.stripesSeen[begin.StripeIndex]; seen {
					fileAgg.mu.Unlock()
					return false, fmt.Errorf("duplicate file begin for %s stripe %d", begin.RelPath, begin.StripeIndex)
				}
				fileAgg.stripesSeen[begin.StripeIndex] = struct{}{}
				fileAgg.mu.Unlock()

				state := &recvFileState{
					begin:        begin,
					lastProgress: time.Now(),
					fileAgg:      fileAgg,
				}
				if resumeInfo, resumeState, err := buildResumeInfo(begin, itemByRelPath[begin.RelPath], fileAgg); err == nil {
					state.resume = resumeState
					state.resumeInfo = resumeInfo
				} else {
					return false, err
				}
				stateMu.Lock()
				stateByStream[begin.StreamID] = state
				stateMu.Unlock()
				select {
				case beginQueue <- begin:
				case <-recvCtx.Done():
					return false, recvCtx.Err()
				}
				return false, nil

			case controlTypeResumeRequest:
				req := payload.(ResumeRequest)
				stateMu.Lock()
				state, ok := stateByStream[req.StreamID]
				stateMu.Unlock()
				if !ok {
					return false, fmt.Errorf("resume request for unknown stream %d", req.StreamID)
				}
				item, ok := itemByRelPath[state.begin.RelPath]
				if !ok {
					return false, fmt.Errorf("missing manifest item for %s", state.begin.RelPath)
				}
				if req.FileID != "" && item.ID != "" && req.FileID != item.ID {
					return false, fmt.Errorf("resume request file id mismatch for %s", state.begin.RelPath)
				}

				resumeInfo := state.resumeInfo
				if resumeInfo == nil {
					resumeInfo, state.resume, err = buildResumeInfo(state.begin, item, state.fileAgg)
					if err != nil {
						return false, err
					}
					state.resumeInfo = resumeInfo
				}

				select {
				case controlWriteCh <- controlMsg{resume: resumeInfo}:
				case <-recvCtx.Done():
					return false, recvCtx.Err()
				}
				return false, nil

			case controlTypeFileEnd:
				end := payload.(FileEnd)
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
					return false, nil
				}
				stateMu.Unlock()
				if !ok {
					return false, fmt.Errorf("file end for unknown stream %d", end.StreamID)
				}
				return false, nil
			case controlTypeEnd:
				if remainingFiles != 0 {
					return false, fmt.Errorf("received fewer files than manifest: remaining %d", remainingFiles)
				}
				close(beginQueue)
				<-schedulerDone
				wg.Wait()
				if recvErr != nil {
					return true, recvErr
				}
				return true, nil
			default:
				return false, fmt.Errorf("unexpected control message type: 0x%02x", typ)
			}
		}

		if msgType == controlTypeBatch {
			batch := msg.(ControlBatch)
			for _, entry := range batch.Entries {
				done, err := handleControl(entry.Type, entry.Msg)
				if err != nil {
					return m, err
				}
				if done {
					return m, nil
				}
			}
			continue
		}
		done, err := handleControl(msgType, msg)
		if err != nil {
			return m, err
		}
		if done {
			return m, nil
		}
	}
}

// RecvManifestMultiStream receives a manifest over the control stream and
// multiplexes file chunks over a fixed pool of data streams.
func RecvManifestMultiStream(ctx context.Context, conn Conn, outDir string, opts Options) (manifest.Manifest, error) {
	var m manifest.Manifest

	controlStream, err := conn.AcceptStream(ctx)
	if err != nil {
		return m, fmt.Errorf("failed to accept control stream: %w", err)
	}
	defer controlStream.Close()

	m, err = readControlHeader(controlStream)
	if err != nil {
		return m, err
	}
	if opts.OnManifestFn != nil {
		opts.OnManifestFn(m)
	}

	rootedDir := filepath.Join(outDir, m.Root)
	baseDir := outDir
	if !opts.NoRootDir {
		baseDir = rootedDir
	}
	if baseDir == "" {
		baseDir = "."
	}
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return m, fmt.Errorf("failed to create output directory %s: %w", baseDir, err)
	}
	dirSet := make(map[string]struct{})
	for _, item := range m.Items {
		if item.IsDir {
			dirPath := filepath.Join(baseDir, filepath.FromSlash(item.RelPath))
			dirSet[dirPath] = struct{}{}
			continue
		}
		parentDir := filepath.Dir(filepath.Join(baseDir, filepath.FromSlash(item.RelPath)))
		if parentDir != "" {
			dirSet[parentDir] = struct{}{}
		}
	}
	for dirPath := range dirSet {
		if err := os.MkdirAll(dirPath, 0755); err != nil {
			return m, fmt.Errorf("failed to create directory %s: %w", dirPath, err)
		}
	}

	expectedFiles := make(map[string]int64)
	itemByRelPath := make(map[string]manifest.FileItem)
	itemByKey := make(map[uint64]manifest.FileItem)
	var remainingBytes int64
	for _, item := range m.Items {
		if item.IsDir {
			continue
		}
		expectedFiles[item.RelPath] = item.Size
		itemByRelPath[item.RelPath] = item
		itemByKey[fileKeyForItem(item)] = item
		remainingBytes += item.Size
	}
	totalFiles := len(expectedFiles)

	type controlEvent struct {
		typ byte
		msg any
	}
	controlBuf := 64
	if totalFiles > 0 {
		controlBuf = totalFiles * 2
		if controlBuf < 64 {
			controlBuf = 64
		}
		if controlBuf > 4096 {
			controlBuf = 4096
		}
	}
	controlCh := make(chan controlEvent, controlBuf)
	controlErr := make(chan error, 1)
	go func() {
		for {
			msgType, msg, err := readControlMessage(controlStream)
			if err != nil {
				select {
				case controlErr <- err:
				default:
				}
				return
			}
			if msgType == controlTypeBatch {
				batch := msg.(ControlBatch)
				for _, entry := range batch.Entries {
					controlCh <- controlEvent{typ: entry.Type, msg: entry.Msg}
					if entry.Type == controlTypeEnd {
						return
					}
				}
				continue
			}
			controlCh <- controlEvent{typ: msgType, msg: msg}
			if msgType == controlTypeEnd {
				return
			}
		}
	}()

	pending := make([]controlEvent, 0)
	dataStreams := 0
	for dataStreams == 0 {
		select {
		case <-ctx.Done():
			return m, ctx.Err()
		case err := <-controlErr:
			return m, err
		case ev := <-controlCh:
			if ev.typ == controlTypeDataStreams {
				ds := ev.msg.(DataStreams)
				dataStreams = int(ds.Count)
			} else {
				pending = append(pending, ev)
			}
		}
	}
	if dataStreams < 1 {
		dataStreams = 1
	}
	writerQueueCap := dataStreams * 4
	if writerQueueCap < 32 {
		writerQueueCap = 32
	}
	if writerQueueCap > 512 {
		writerQueueCap = 512
	}
	maxOpenFiles := dataStreams * 4
	if maxOpenFiles < 32 {
		maxOpenFiles = 32
	}
	if maxOpenFiles > 1024 {
		maxOpenFiles = 1024
	}
	fileCache := newFileCache(maxOpenFiles)
	defer fileCache.closeAll()

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

	stateMu := sync.Mutex{}
	stateByKey := make(map[uint64]*recvFileStateMux)
	stateByRelPath := make(map[string]*recvFileStateMux)
	doneKeys := make(map[uint64]struct{})
	sidecarByKey := make(map[uint64]*Sidecar)
	sidecarMu := sync.Mutex{}
	snapshotRequested := false
	fileReady := newFileWaitRegistry()

	getSidecar := func(key uint64, item manifest.FileItem, chunkSize uint32) (*Sidecar, error) {
		if !opts.Resume || item.ID == "" || chunkSize == 0 {
			return nil, nil
		}
		sidecarMu.Lock()
		if sc, ok := sidecarByKey[key]; ok {
			sidecarMu.Unlock()
			return sc, nil
		}
		sidecarMu.Unlock()
		primary := SidecarPath(baseDir, "", sidecarIdentifier(item))
		fallback := ""
		if rootedDir != baseDir {
			fallback = SidecarPath(rootedDir, "", sidecarIdentifier(item))
		}
		loaded, err := LoadOrCreateSidecarWithFallback(primary, fallback, item.ID, item.Size, chunkSize)
		if err != nil {
			return nil, err
		}
		globalSidecarFlushRegistry.add(loaded)
		sidecarMu.Lock()
		if existing, ok := sidecarByKey[key]; ok {
			sidecarMu.Unlock()
			return existing, nil
		}
		sidecarByKey[key] = loaded
		sidecarMu.Unlock()
		return loaded, nil
	}

	var statsMu sync.Mutex
	activeCount := 0
	completedCount := 0
	doneCh := make(chan struct{}, 1)
	updateStats := func(active, completed int, remaining int64) {
		if opts.TransferStatsFn != nil {
			opts.TransferStatsFn(active, completed, remaining)
		}
	}

	type controlMsg struct {
		done   *FileDone
		resume *FileResumeInfo
	}
	controlWriteCap := dataStreams * 8
	if totalFiles > 0 {
		controlWriteCap = totalFiles * 2
		if controlWriteCap < dataStreams*8 {
			controlWriteCap = dataStreams * 8
		}
		if controlWriteCap < 64 {
			controlWriteCap = 64
		}
		if controlWriteCap > 8192 {
			controlWriteCap = 8192
		}
	}
	controlWriteCh := make(chan controlMsg, controlWriteCap)
	go func() {
		batch := make([]ControlEntry, 0, controlBatchMax)
		timer := time.NewTimer(controlBatchFlushInterval)
		timer.Stop()
		timerActive := false
		flush := func() bool {
			if len(batch) == 0 {
				return true
			}
			if err := writeControlBatch(controlStream, ControlBatch{Entries: batch}); err != nil {
				setRecvErr(err)
				return false
			}
			batch = batch[:0]
			return true
		}
		for {
			select {
			case <-recvCtx.Done():
				_ = flush()
				return
			case msg := <-controlWriteCh:
				if msg.done != nil {
					batch = append(batch, ControlEntry{Type: controlTypeFileDone, Msg: *msg.done})
				}
				if msg.resume != nil {
					batch = append(batch, ControlEntry{Type: controlTypeFileResumeInfo, Msg: *msg.resume})
				}
				if len(batch) >= controlBatchMax {
					if !flush() {
						return
					}
				}
				if !timerActive {
					timer.Reset(controlBatchFlushInterval)
					timerActive = true
				}
			case <-timer.C:
				timerActive = false
				if !flush() {
					return
				}
			}
		}
	}()

	if opts.Resume {
		go func() {
			ticker := time.NewTicker(1 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-recvCtx.Done():
					return
				case <-ticker.C:
					stateMu.Lock()
					states := make([]*recvFileStateMux, 0, len(stateByKey))
					for _, state := range stateByKey {
						if state.sidecar != nil {
							states = append(states, state)
						}
					}
					stateMu.Unlock()
					for _, state := range states {
						_ = state.sidecar.Flush()
					}
				}
			}
		}()
	}

	finalizeFile := func(state *recvFileStateMux, ok bool, errMsg string) {
		state.mu.Lock()
		if state.done {
			state.mu.Unlock()
			return
		}
		state.done = true
		state.mu.Unlock()

		if opts.Resume && state.sidecar != nil {
			if err := state.sidecar.Flush(); err != nil {
				ok = false
				if errMsg == "" {
					errMsg = fmt.Sprintf("failed to flush sidecar: %v", err)
				}
			}
			globalSidecarFlushRegistry.remove(state.sidecar)
		}

		if ok {
			writtenBytes := state.baseBytes + atomic.LoadInt64(&state.bytesWritten)
			if writtenBytes != state.item.Size {
				ok = false
				if errMsg == "" {
					errMsg = fmt.Sprintf("size mismatch: wrote %d expected %d", writtenBytes, state.item.Size)
				}
			}
		}

		if opts.FileDoneFn != nil {
			opts.FileDoneFn(state.item.RelPath, ok)
		}

		select {
		case controlWriteCh <- controlMsg{done: &FileDone{
			StreamID: state.key,
			OK:       ok,
			ErrMsg:   errMsg,
		}}:
		case <-recvCtx.Done():
		}

		state.closeWriter()
		state.closeFile()

		stateMu.Lock()
		delete(stateByKey, state.key)
		delete(stateByRelPath, state.item.RelPath)
		doneKeys[state.key] = struct{}{}
		stateMu.Unlock()

		statsMu.Lock()
		if activeCount > 0 {
			activeCount--
		}
		completedCount++
		remainingBytes -= state.item.Size
		active := activeCount
		completed := completedCount
		remaining := remainingBytes
		statsMu.Unlock()
		updateStats(active, completed, remaining)
		if totalFiles > 0 && completed >= totalFiles {
			select {
			case doneCh <- struct{}{}:
			default:
			}
		}
	}

	buildResumeInfo := func(state *recvFileStateMux) (*FileResumeInfo, error) {
		info := &FileResumeInfo{
			FileID:      state.item.ID,
			StreamID:    state.key,
			TotalChunks: state.totalChunks,
		}
		if !opts.Resume || state.totalChunks == 0 {
			info.LastVerifiedChunk = state.totalChunks
			return info, nil
		}
		if state.sidecar == nil {
			loaded, err := getSidecar(state.key, state.item, state.chunkSize)
			if err != nil {
				return nil, fmt.Errorf("failed to load sidecar: %w", err)
			}
			state.sidecar = loaded
		}
		if state.sidecar == nil {
			info.LastVerifiedChunk = state.totalChunks
			return info, nil
		}
		highest, hasAny := state.sidecar.HighestComplete()
		if hasAny {
			if _, statErr := os.Stat(state.filePath); errors.Is(statErr, os.ErrNotExist) {
				fresh, resetErr := resetSidecarForMissingFile(state.sidecar, state.item.ID, state.item.Size, state.chunkSize)
				if resetErr != nil {
					return nil, resetErr
				}
				state.sidecar = fresh
				info.Bitmap = nil
				info.LastVerifiedChunk = 0
				info.LastVerifiedHash = 0
				hasAny = false
				highest = 0
			}
		}
		if hasAny {
			info.Bitmap = state.sidecar.MarshalBitmap()
			completedChunks := uint32(state.sidecar.bitmap.CountSet())
			allComplete := state.totalChunks > 0 && completedChunks >= state.totalChunks
			if allComplete {
				info.LastVerifiedChunk = state.totalChunks
			} else {
				info.LastVerifiedChunk = uint32(highest)
				if state.hashAlg != HashAlgNone {
					hashValue, ok, err := hashFileChunkWithTimeout(state.filePath, uint32(highest), state.chunkSize, state.item.Size, state.hashAlg, defaultResumeHashTimeout)
					if err != nil {
						if errors.Is(err, os.ErrNotExist) {
							fresh, resetErr := resetSidecarForMissingFile(state.sidecar, state.item.ID, state.item.Size, state.chunkSize)
							if resetErr != nil {
								return nil, resetErr
							}
							state.sidecar = fresh
							info.Bitmap = nil
							info.LastVerifiedChunk = 0
							info.LastVerifiedHash = 0
							hasAny = false
						} else {
							return nil, err
						}
					}
					if hasAny {
						if ok {
							info.LastVerifiedHash = hashValue
						} else {
							info.LastVerifiedHash = resumeHashUnknown
						}
					}
				}
			}
		}
		if opts.ResumeStatsFn != nil && state.totalChunks > 0 && !snapshotRequested {
			state.resumeOnce.Do(func() {
				skippedChunks := uint32(0)
				if highest, ok := state.sidecar.HighestContiguous(); ok && highest >= 0 {
					skippedChunks = uint32(highest + 1)
				}
				if skippedChunks > state.totalChunks {
					skippedChunks = state.totalChunks
				}
				opts.ResumeStatsFn(state.item.RelPath, skippedChunks, state.totalChunks, info.LastVerifiedChunk, int64(state.item.Size), state.chunkSize)
				bitmapSnapshot := state.sidecar.MarshalBitmap()
				go func(relpath string, total uint32, totalBytes int64, chunkSize uint32, verifiedChunk uint32) {
					if len(bitmapSnapshot) == 0 || total == 0 {
						return
					}
					bm, err := BitmapFromBytes(bitmapSnapshot, int(total))
					if err != nil {
						return
					}
					skipped := uint32(bm.CountSet())
					if skipped > total {
						skipped = total
					}
					opts.ResumeStatsFn(relpath, skipped, total, verifiedChunk, totalBytes, chunkSize)
				}(state.item.RelPath, state.totalChunks, int64(state.item.Size), state.chunkSize, info.LastVerifiedChunk)
			})
		}
		return info, nil
	}

	buildSnapshotResumeInfo := func(entry ResumeSnapshotEntry) (*FileResumeInfo, error) {
		item, ok := itemByKey[entry.StreamID]
		if !ok {
			return nil, fmt.Errorf("resume snapshot for unknown file %d", entry.StreamID)
		}
		if entry.FileID != "" && item.ID != "" && entry.FileID != item.ID {
			return nil, fmt.Errorf("resume snapshot file id mismatch for %s", item.RelPath)
		}
		if entry.ChunkSize == 0 {
			return nil, fmt.Errorf("resume snapshot missing chunk size for %s", item.RelPath)
		}
		totalChunks := chunkTotal(item.Size, entry.ChunkSize)
		info := &FileResumeInfo{
			FileID:      item.ID,
			StreamID:    entry.StreamID,
			TotalChunks: totalChunks,
		}
		if !opts.Resume || totalChunks == 0 {
			info.LastVerifiedChunk = totalChunks
			return info, nil
		}
		sidecar, err := getSidecar(entry.StreamID, item, entry.ChunkSize)
		if err != nil {
			return nil, fmt.Errorf("failed to load sidecar: %w", err)
		}
		if sidecar == nil {
			info.LastVerifiedChunk = totalChunks
			return info, nil
		}
		filePath := filepath.Join(baseDir, filepath.FromSlash(item.RelPath))
		highest, hasAny := sidecar.HighestComplete()
		if hasAny {
			if _, statErr := os.Stat(filePath); errors.Is(statErr, os.ErrNotExist) {
				fresh, resetErr := resetSidecarForMissingFile(sidecar, item.ID, item.Size, entry.ChunkSize)
				if resetErr != nil {
					return nil, resetErr
				}
				sidecar = fresh
				sidecarMu.Lock()
				sidecarByKey[entry.StreamID] = sidecar
				sidecarMu.Unlock()
				info.Bitmap = nil
				info.LastVerifiedChunk = 0
				info.LastVerifiedHash = 0
				hasAny = false
				highest = 0
			}
		}
		if hasAny {
			info.Bitmap = sidecar.MarshalBitmap()
			completedChunks := uint32(sidecar.bitmap.CountSet())
			allComplete := totalChunks > 0 && completedChunks >= totalChunks
			if allComplete {
				info.LastVerifiedChunk = totalChunks
			} else {
				info.LastVerifiedChunk = uint32(highest)
				if entry.HashAlg != HashAlgNone {
					hashValue, ok, err := hashFileChunkWithTimeout(filePath, uint32(highest), entry.ChunkSize, item.Size, entry.HashAlg, defaultResumeHashTimeout)
					if err != nil {
						if errors.Is(err, os.ErrNotExist) {
							fresh, resetErr := resetSidecarForMissingFile(sidecar, item.ID, item.Size, entry.ChunkSize)
							if resetErr != nil {
								return nil, resetErr
							}
							sidecar = fresh
							sidecarMu.Lock()
							sidecarByKey[entry.StreamID] = sidecar
							sidecarMu.Unlock()
							info.Bitmap = nil
							info.LastVerifiedChunk = 0
							info.LastVerifiedHash = 0
							hasAny = false
						} else {
							return nil, err
						}
					}
					if hasAny {
						if ok {
							info.LastVerifiedHash = hashValue
						} else {
							info.LastVerifiedHash = resumeHashUnknown
						}
					}
				}
			}
		}
		if opts.ResumeStatsFn != nil && totalChunks > 0 {
			skippedChunks := uint32(0)
			if highest, ok := sidecar.HighestContiguous(); ok && highest >= 0 {
				skippedChunks = uint32(highest + 1)
			}
			if skippedChunks > totalChunks {
				skippedChunks = totalChunks
			}
			opts.ResumeStatsFn(item.RelPath, skippedChunks, totalChunks, info.LastVerifiedChunk, int64(item.Size), entry.ChunkSize)
			bitmapSnapshot := sidecar.MarshalBitmap()
			go func(relpath string, total uint32, totalBytes int64, chunkSize uint32, verifiedChunk uint32) {
				if len(bitmapSnapshot) == 0 || total == 0 {
					return
				}
				bm, err := BitmapFromBytes(bitmapSnapshot, int(total))
				if err != nil {
					return
				}
				skipped := uint32(bm.CountSet())
				if skipped > total {
					skipped = total
				}
				opts.ResumeStatsFn(relpath, skipped, total, verifiedChunk, totalBytes, chunkSize)
			}(item.RelPath, totalChunks, int64(item.Size), entry.ChunkSize, info.LastVerifiedChunk)
		}
		return info, nil
	}

	computeBaseBytes := func(item manifest.FileItem, chunkSize uint32, sidecar *Sidecar) int64 {
		if sidecar == nil || sidecar.bitmap == nil || chunkSize == 0 || item.Size <= 0 {
			return 0
		}
		completedChunks := sidecar.bitmap.CountSet()
		if completedChunks <= 0 {
			return 0
		}
		totalChunks := chunkTotal(item.Size, chunkSize)
		if totalChunks == 0 {
			return 0
		}
		baseBytes := int64(completedChunks) * int64(chunkSize)
		lastIndex := int(totalChunks - 1)
		if sidecar.bitmap.Get(lastIndex) {
			lastSize := chunkSizeForIndex(item.Size, chunkSize, totalChunks-1)
			if lastSize > 0 {
				baseBytes -= int64(chunkSize)
				baseBytes += int64(lastSize)
			}
		}
		if baseBytes > item.Size {
			baseBytes = item.Size
		}
		return baseBytes
	}

	createState := func(item manifest.FileItem, key uint64, chunkSize uint32, hashAlg byte) (*recvFileStateMux, error) {
		if chunkSize == 0 {
			return nil, fmt.Errorf("invalid chunk size 0 for %s", item.RelPath)
		}
		filePath := filepath.Join(baseDir, filepath.FromSlash(item.RelPath))
		if err := fileCache.ensureFile(filePath, int64(item.Size)); err != nil {
			return nil, fmt.Errorf("failed to prepare output file %s: %w", filePath, err)
		}
		totalChunks := chunkTotal(item.Size, chunkSize)
		state := &recvFileStateMux{
			key:         key,
			item:        item,
			filePath:    filePath,
			chunkSize:   chunkSize,
			totalChunks: totalChunks,
			hashAlg:     hashAlg,
			fileCache:   fileCache,
			remaining:   totalChunks,
		}
		if opts.Resume && item.ID != "" && chunkSize > 0 {
			sidecar, err := getSidecar(key, item, chunkSize)
			if err != nil {
				return nil, fmt.Errorf("failed to load sidecar: %w", err)
			}
			state.sidecar = sidecar
			if sidecar != nil {
				skipped := uint32(sidecar.bitmap.CountSet())
				if skipped > totalChunks {
					skipped = totalChunks
				}
				if totalChunks >= skipped {
					state.remaining = totalChunks - skipped
				}
				state.baseBytes = computeBaseBytes(item, chunkSize, sidecar)
			}
		}
		return state, nil
	}

	dataErrCh := make(chan error, dataStreams)
	reportDataErr := func(err error) {
		if err == nil {
			return
		}
		select {
		case dataErrCh <- err:
		default:
		}
	}

	startWriter := func(state *recvFileStateMux) {
		state.writerOnce.Do(func() {
			state.writerCh = make(chan recvChunkData, writerQueueCap)
			go func() {
				var completedBitmap *Bitmap
				if state.sidecar != nil && state.totalChunks > 0 {
					if snapshot := state.sidecar.MarshalBitmap(); len(snapshot) > 0 {
						if bm, err := BitmapFromBytes(snapshot, int(state.totalChunks)); err == nil {
							completedBitmap = bm
						}
					}
				}
				nextIndex := uint32(0)
				pending := make(map[uint32]recvChunkData)
				drain := func() {
					for _, c := range pending {
						if c.pool != nil {
							c.pool.Put(c.buf)
						}
					}
					pending = nil
					for {
						select {
						case c, ok := <-state.writerCh:
							if !ok {
								return
							}
							if c.pool != nil {
								c.pool.Put(c.buf)
							}
						default:
							return
						}
					}
				}
				flush := func() bool {
					for {
						chunk, ok := pending[nextIndex]
						if !ok {
							if completedBitmap != nil && nextIndex < state.totalChunks && completedBitmap.Get(int(nextIndex)) {
								nextIndex++
								continue
							}
							return true
						}
						delete(pending, nextIndex)
						f, err := state.openFile()
						if err != nil {
							if chunk.pool != nil {
								chunk.pool.Put(chunk.buf)
							}
							finalErr := fmt.Errorf("failed to open file %s: %w", state.item.RelPath, err)
							setRecvErr(finalErr)
							reportDataErr(finalErr)
							finalizeFile(state, false, finalErr.Error())
							drain()
							return false
						}
						offset := int64(nextIndex) * int64(state.chunkSize)
						if err := writeAtWithTimeout(recvCtx, f, chunk.buf[:chunk.n], offset, state.item.RelPath); err != nil {
							if chunk.pool != nil {
								chunk.pool.Put(chunk.buf)
							}
							setRecvErr(err)
							reportDataErr(err)
							finalizeFile(state, false, err.Error())
							drain()
							return false
						}
						if chunk.pool != nil {
							chunk.pool.Put(chunk.buf)
						}
						if completedBitmap != nil {
							completedBitmap.Set(int(nextIndex))
						}
						done, added := state.markChunkComplete(nextIndex, uint32(chunk.n))
						if added > 0 {
							atomic.AddInt64(&state.bytesWritten, added)
							if opts.ProgressFn != nil {
								newTotal := atomic.AddInt64(&state.bytesRecv, added)
								opts.ProgressFn(state.item.RelPath, newTotal, int64(state.item.Size))
							}
						}
						if done && state.endReceivedNow() {
							finalizeFile(state, true, "")
						}
						nextIndex++
					}
				}
				for {
					select {
					case <-recvCtx.Done():
						drain()
						return
					case chunk, ok := <-state.writerCh:
						if !ok {
							drain()
							return
						}
						pending[chunk.index] = chunk
					}
					if !flush() {
						return
					}
				}
			}()
		})
	}

	getOrCreateState := func(key uint64, chunkSize uint32, hashAlg byte) (*recvFileStateMux, error) {
		stateMu.Lock()
		state := stateByKey[key]
		stateMu.Unlock()
		if state != nil {
			state.mu.Lock()
			if state.chunkSize == 0 && chunkSize > 0 {
				state.chunkSize = chunkSize
				state.totalChunks = chunkTotal(state.item.Size, chunkSize)
				state.remaining = state.totalChunks
			}
			if state.hashAlg == 0 {
				state.hashAlg = hashAlg
			}
			state.mu.Unlock()
			if state.chunkSize > 0 && chunkSize > 0 && state.chunkSize != chunkSize {
				return nil, fmt.Errorf("chunk size mismatch for %s", state.item.RelPath)
			}
			return state, nil
		}
		item, ok := itemByKey[key]
		if !ok {
			return nil, fmt.Errorf("unknown file key %d", key)
		}
		newState, err := createState(item, key, chunkSize, hashAlg)
		if err != nil {
			return nil, err
		}
		stateMu.Lock()
		if existing := stateByKey[key]; existing != nil {
			stateMu.Unlock()
			return existing, nil
		}
		stateByKey[key] = newState
		stateByRelPath[item.RelPath] = newState
		stateMu.Unlock()
		startWriter(newState)
		fileReady.signal(key)

		statsMu.Lock()
		activeCount++
		active := activeCount
		completed := completedCount
		remaining := remainingBytes
		statsMu.Unlock()
		updateStats(active, completed, remaining)
		if opts.Resume && !snapshotRequested {
			info, err := buildResumeInfo(newState)
			if err != nil {
				return nil, err
			}
			newState.mu.Lock()
			newState.resumeInfo = info
			newState.mu.Unlock()
			select {
			case controlWriteCh <- controlMsg{resume: info}:
			case <-recvCtx.Done():
				return nil, recvCtx.Err()
			}
		}
		return newState, nil
	}

	handleResumeSnapshotRequest := func(req ResumeSnapshotRequest) error {
		snapshotRequested = true
		if len(req.Entries) > 0 && opts.OnResumeSnapshotStart != nil {
			opts.OnResumeSnapshotStart(len(req.Entries))
		}
		done := 0
		for _, entry := range req.Entries {
			info, err := buildSnapshotResumeInfo(entry)
			if err != nil {
				return err
			}
			select {
			case controlWriteCh <- controlMsg{resume: info}:
			case <-recvCtx.Done():
				return recvCtx.Err()
			}
			done++
			if opts.OnResumeSnapshotProgress != nil {
				opts.OnResumeSnapshotProgress(done, len(req.Entries))
			}
		}
		if len(req.Entries) > 0 && opts.OnResumeSnapshotDone != nil {
			opts.OnResumeSnapshotDone()
		}
		return nil
	}

	handleResumeRequest := func(req ResumeRequest) error {
		stateMu.Lock()
		state, ok := stateByKey[req.StreamID]
		stateMu.Unlock()
		if !ok {
			if !fileReady.wait(recvCtx, req.StreamID) {
				return fmt.Errorf("resume request for unknown file %d", req.StreamID)
			}
			stateMu.Lock()
			state, ok = stateByKey[req.StreamID]
			stateMu.Unlock()
			if !ok {
				return fmt.Errorf("resume request for unknown file %d", req.StreamID)
			}
		}
		if req.FileID != "" && state.item.ID != "" && req.FileID != state.item.ID {
			return fmt.Errorf("resume request file id mismatch for %s", state.item.RelPath)
		}
		state.mu.Lock()
		info := state.resumeInfo
		state.mu.Unlock()
		if info == nil {
			var err error
			info, err = buildResumeInfo(state)
			if err != nil {
				return err
			}
			state.mu.Lock()
			state.resumeInfo = info
			state.mu.Unlock()
		}
		select {
		case controlWriteCh <- controlMsg{resume: info}:
		case <-recvCtx.Done():
			return recvCtx.Err()
		}
		return nil
	}

	handleFileEnd := func(end FileEnd) error {
		stateMu.Lock()
		state, ok := stateByKey[end.StreamID]
		if !ok {
			_, done := doneKeys[end.StreamID]
			stateMu.Unlock()
			if done {
				return nil
			}
			return fmt.Errorf("file end for unknown file %d", end.StreamID)
		}
		stateMu.Unlock()
		if state.markEndReceived() {
			finalizeFile(state, true, "")
		}
		return nil
	}

	handleControl := func(ev controlEvent) error {
		switch ev.typ {
		case controlTypeResumeSnapshotRequest:
			return handleResumeSnapshotRequest(ev.msg.(ResumeSnapshotRequest))
		case controlTypeResumeRequest:
			return handleResumeRequest(ev.msg.(ResumeRequest))
		case controlTypeFileEnd:
			return handleFileEnd(ev.msg.(FileEnd))
		case controlTypeEnd:
			return nil
		default:
			return fmt.Errorf("unexpected control message type: 0x%02x", ev.typ)
		}
	}

	for _, ev := range pending {
		if err := handleControl(ev); err != nil {
			setRecvErr(err)
			return m, err
		}
	}

	waitForCompletion := func() bool {
		if allFilesCompleted(&statsMu, &completedCount, totalFiles) {
			return true
		}
		if totalFiles == 0 {
			return true
		}
		timer := time.NewTimer(2 * time.Second)
		defer timer.Stop()
		for {
			select {
			case <-doneCh:
				return true
			case <-recvCtx.Done():
				return allFilesCompleted(&statsMu, &completedCount, totalFiles)
			case <-timer.C:
				return allFilesCompleted(&statsMu, &completedCount, totalFiles)
			}
		}
	}

	handleDataStream := func(s Stream) {
		defer s.Close()
		header := make([]byte, dataChunkHeaderLen)
		for {
			if err := readFullWithTimeout(recvCtx, s, header, "", "mux-header"); err != nil {
				if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, context.Canceled) {
					return
				}
				reportDataErr(err)
				return
			}
			fileKey := binary.BigEndian.Uint64(header[0:8])
			chunkIndex := binary.BigEndian.Uint32(header[8:12])
			chunkLen := binary.BigEndian.Uint32(header[12:16])
			chunkSize := binary.BigEndian.Uint32(header[20:24])
			hashAlg := header[24]
			flags := header[25]
			if (flags&dataFlagEnd) != 0 || chunkLen == 0 {
				state, err := getOrCreateState(fileKey, chunkSize, hashAlg)
				if err != nil {
					reportDataErr(err)
					return
				}
				if state.markEndReceived() {
					finalizeFile(state, true, "")
				}
				continue
			}
			state, err := getOrCreateState(fileKey, chunkSize, hashAlg)
			if err != nil {
				reportDataErr(err)
				return
			}
			if state.totalChunks > 0 && chunkIndex >= state.totalChunks {
				err := fmt.Errorf("chunk index %d out of range for %s", chunkIndex, state.item.RelPath)
				finalizeFile(state, false, err.Error())
				reportDataErr(err)
				return
			}
			if state.chunkSize > 0 && chunkLen > state.chunkSize {
				err := fmt.Errorf("chunk length %d exceeds chunk size %d for %s", chunkLen, state.chunkSize, state.item.RelPath)
				finalizeFile(state, false, err.Error())
				reportDataErr(err)
				return
			}
			bufPool := chunkPoolFor(state.chunkSize)
			if bufPool == nil {
				bufPool = bufpool.New(int(state.chunkSize))
			}
			buf := bufPool.Get()
			if int(chunkLen) > len(buf) {
				bufPool.Put(buf)
				reportDataErr(fmt.Errorf("chunk length %d exceeds buffer size %d", chunkLen, len(buf)))
				return
			}
			deltaFn := opts.ProgressDeltaFn
			if err := readFullWithTimeoutDelta(recvCtx, s, buf[:chunkLen], state.item.RelPath, "mux-data", deltaFn); err != nil {
				bufPool.Put(buf)
				finalizeFile(state, false, err.Error())
				reportDataErr(err)
				return
			}
			startWriter(state)
			chunk := recvChunkData{
				index: chunkIndex,
				n:     int(chunkLen),
				buf:   buf,
				pool:  bufPool,
			}
			select {
			case state.writerCh <- chunk:
			case <-recvCtx.Done():
				bufPool.Put(buf)
				return
			}
		}
	}

	go func() {
		for {
			stream, err := conn.AcceptStream(recvCtx)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				if isGracefulRemoteClose(err) {
					reportDataErr(err)
					return
				}
				setRecvErr(err)
				reportDataErr(err)
				return
			}
			go handleDataStream(stream)
		}
	}()

	endReceived := false
	for {
		select {
		case <-recvCtx.Done():
			if recvErr != nil {
				return m, recvErr
			}
			return m, recvCtx.Err()
		case <-doneCh:
			if endReceived && completedCount >= totalFiles {
				return m, nil
			}
		case err := <-controlErr:
			if err != nil && !errors.Is(err, io.EOF) {
				if isGracefulRemoteClose(err) && waitForCompletion() {
					return m, nil
				}
				return m, err
			}
			if completedCount >= totalFiles {
				return m, nil
			}
			return m, err
		case err := <-dataErrCh:
			if err != nil {
				if isGracefulRemoteClose(err) && waitForCompletion() {
					return m, nil
				}
				setRecvErr(err)
				return m, err
			}
		case ev := <-controlCh:
			if ev.typ == controlTypeEnd {
				endReceived = true
				if completedCount >= totalFiles {
					return m, nil
				}
				continue
			}
			if err := handleControl(ev); err != nil {
				setRecvErr(err)
				return m, err
			}
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
