package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"hybrid_distributed_store/internal/config"
)

// WriteTask represents an asynchronous write operation payload.
type WriteTask struct {
	Key        string
	Data       []byte
	Reader     io.Reader
	SizeBytes  int64
	WriteClass string
	EnqueuedAt time.Time
	Done       chan writeTaskResult
}

type writeTaskResult struct {
	SizeBytes int64
	Err       error
}

type durableWriteTiming struct {
	Total time.Duration
	Write time.Duration
	Sync  time.Duration
}

const (
	storageDurabilitySync      = "sync"
	storageDurabilityDataSync  = "data_sync"
	storageDurabilityGroupSync = "group_sync"
	storageDurabilityWrite     = "write"
)

type groupSyncRequest struct {
	file *os.File
	done chan error
}

type groupSyncer struct {
	interval time.Duration
	maxBatch int
	requests chan groupSyncRequest
}

// storageEngine handles raw file I/O operations with asynchronous write support.
type storageEngine struct {
	storageDir      string
	port            string
	nodeName        string
	totalOperations int64
	lock            sync.RWMutex

	// writeQueue buffers incoming write requests for background processing.
	foregroundWriteQueue          chan *WriteTask
	backgroundWriteQueue          chan *WriteTask
	ioWorkerCount                 int
	durabilityMode                string
	groupSyncer                   *groupSyncer
	queuedWriteBytes              int64
	queuedForegroundWriteBytes    int64
	queuedBackgroundWriteBytes    int64
	maxQueuedWriteBytes           int64
	maxBackgroundQueuedWriteBytes int64
}

// newStorageEngine initializes the storage directory and the async engine.
func newStorageEngine(port, nodeName, storageDir string) *storageEngine {
	return newStorageEngineWithDurability(port, nodeName, storageDir, config.StorageMaxQueuedWriteBytes, config.StorageIOWorkers, config.StorageDurabilityMode)
}

func normalizeStorageIOWorkers(count int) int {
	if count < 1 {
		return 1
	}
	return count
}

func newStorageEngineWithConfig(port, nodeName, storageDir string, maxQueuedWriteBytes int64, ioWorkers int) *storageEngine {
	return newStorageEngineWithDurability(port, nodeName, storageDir, maxQueuedWriteBytes, ioWorkers, config.StorageDurabilityMode)
}

func normalizeStorageDurabilityMode(mode string) string {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case storageDurabilityDataSync:
		return storageDurabilityDataSync
	case storageDurabilityGroupSync:
		return storageDurabilityGroupSync
	case storageDurabilityWrite:
		return storageDurabilityWrite
	default:
		return storageDurabilitySync
	}
}

func normalizeStorageWriteClass(writeClass string) string {
	switch strings.ToLower(strings.TrimSpace(writeClass)) {
	case config.StorageWriteClassBackground:
		return config.StorageWriteClassBackground
	default:
		return config.StorageWriteClassForeground
	}
}

func normalizeStorageGroupSyncInterval(ms int) time.Duration {
	if ms < 0 {
		ms = 0
	}
	return time.Duration(ms) * time.Millisecond
}

func normalizeStorageGroupSyncMaxBatch(maxBatch int) int {
	if maxBatch < 1 {
		return 1
	}
	return maxBatch
}

func newGroupSyncer(interval time.Duration, maxBatch int) *groupSyncer {
	syncer := &groupSyncer{
		interval: interval,
		maxBatch: normalizeStorageGroupSyncMaxBatch(maxBatch),
		requests: make(chan groupSyncRequest, 4096),
	}
	go syncer.run()
	return syncer
}

func (g *groupSyncer) sync(file *os.File) error {
	if g == nil {
		return file.Sync()
	}
	done := make(chan error, 1)
	g.requests <- groupSyncRequest{file: file, done: done}
	return <-done
}

func (g *groupSyncer) run() {
	for first := range g.requests {
		batch := []groupSyncRequest{first}
		timer := time.NewTimer(g.interval)
		collecting := true
		for collecting && len(batch) < g.maxBatch {
			select {
			case req := <-g.requests:
				batch = append(batch, req)
			case <-timer.C:
				collecting = false
			}
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}

		for _, req := range batch {
			err := req.file.Sync()
			req.done <- err
			close(req.done)
		}
	}
}

func newStorageEngineWithDurability(port, nodeName, storageDir string, maxQueuedWriteBytes int64, ioWorkers int, durabilityMode string) *storageEngine {
	// Ensure the storage directory exists
	if err := os.MkdirAll(storageDir, 0755); err != nil {
		log.Fatalf("Failed to create storage directory %s: %v", storageDir, err)
	}

	log.Printf("Storage Node (PID: %d, Port: %s) Started.", os.Getpid(), port)
	log.Printf("Data persistence path: %s", storageDir)

	ioWorkerCount := normalizeStorageIOWorkers(ioWorkers)
	normalizedDurabilityMode := normalizeStorageDurabilityMode(durabilityMode)
	var syncer *groupSyncer
	if normalizedDurabilityMode == storageDurabilityGroupSync {
		syncer = newGroupSyncer(
			normalizeStorageGroupSyncInterval(config.StorageGroupSyncIntervalMs),
			config.StorageGroupSyncMaxBatch,
		)
		log.Printf(
			"[Storage Durability] group_sync enabled interval_ms=%d max_batch=%d",
			config.StorageGroupSyncIntervalMs,
			normalizeStorageGroupSyncMaxBatch(config.StorageGroupSyncMaxBatch),
		)
	}
	engine := &storageEngine{
		storageDir:                    storageDir,
		port:                          port,
		nodeName:                      nodeName,
		ioWorkerCount:                 ioWorkerCount,
		durabilityMode:                normalizedDurabilityMode,
		groupSyncer:                   syncer,
		maxQueuedWriteBytes:           maxQueuedWriteBytes,
		maxBackgroundQueuedWriteBytes: config.StorageBackgroundMaxQueuedWriteBytes,
		// Keep separate foreground/background channels so migration writes cannot sit ahead of user PUTs.
		foregroundWriteQueue: make(chan *WriteTask, 5000),
		backgroundWriteQueue: make(chan *WriteTask, 5000),
	}

	for workerID := 1; workerID <= ioWorkerCount; workerID++ {
		go engine.startIoWorker(workerID)
	}

	return engine
}

func writeFileDurably(path string, data []byte) (durableWriteTiming, error) {
	return writeFileWithDurability(path, data, storageDurabilitySync)
}

func writeFileWithDurability(path string, data []byte, durabilityMode string) (durableWriteTiming, error) {
	_, timing, err := writeReaderWithDurability(path, bytes.NewReader(data), durabilityMode)
	return timing, err
}

func writeReaderWithDurability(path string, reader io.Reader, durabilityMode string) (int64, durableWriteTiming, error) {
	return writeReaderWithSyncer(path, reader, durabilityMode, nil)
}

func writeReaderWithSyncer(path string, reader io.Reader, durabilityMode string, syncer *groupSyncer) (int64, durableWriteTiming, error) {
	start := time.Now()
	timing := durableWriteTiming{}
	durabilityMode = normalizeStorageDurabilityMode(durabilityMode)

	// Ensure parent directories exist for nested keys.
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		timing.Total = time.Since(start)
		return 0, timing, err
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		timing.Total = time.Since(start)
		return 0, timing, err
	}
	defer func() { _ = f.Close() }()

	writeStart := time.Now()
	writtenBytes, err := io.Copy(f, reader)
	if err != nil {
		timing.Write = time.Since(writeStart)
		timing.Total = time.Since(start)
		return writtenBytes, timing, err
	}
	timing.Write = time.Since(writeStart)

	if durabilityMode == storageDurabilitySync || durabilityMode == storageDurabilityDataSync || durabilityMode == storageDurabilityGroupSync {
		syncStart := time.Now()
		syncErr := error(nil)
		if durabilityMode == storageDurabilityGroupSync {
			syncErr = syncer.sync(f)
		} else if durabilityMode == storageDurabilityDataSync {
			syncErr = syncFileDataOnly(f)
		} else {
			syncErr = f.Sync()
		}
		if syncErr != nil {
			timing.Sync = time.Since(syncStart)
			timing.Total = time.Since(start)
			return writtenBytes, timing, syncErr
		}
		timing.Sync = time.Since(syncStart)
	}
	timing.Total = time.Since(start)
	return writtenBytes, timing, nil
}

// startIoWorker consumes write tasks from the queue and performs blocking durable disk I/O.
func (s *storageEngine) startIoWorker(workerID int) {
	log.Printf("[Async IO] Worker %d/%d started. Waiting for tasks...", workerID, s.ioWorkerCount)
	for {
		task, ok := s.nextWriteTask()
		if !ok {
			return
		}
		taskSize := task.SizeBytes
		if taskSize <= 0 && task.Data != nil {
			taskSize = int64(len(task.Data))
		}
		queueWait := time.Duration(0)
		if !task.EnqueuedAt.IsZero() {
			queueWait = time.Since(task.EnqueuedAt)
		}
		writeErr := error(nil)
		timing := durableWriteTiming{}
		writtenBytes := int64(0)

		filePath, err := s._getSafePath(task.Key)
		if err != nil {
			log.Printf("[Async IO] Error resolving path for key %s: %v", task.Key, err)
			writeErr = err
		} else {
			// Perform the blocking disk write operation according to the configured durability mode.
			if task.Reader != nil {
				writtenBytes, timing, err = writeReaderWithSyncer(filePath, task.Reader, s.durabilityMode, s.groupSyncer)
			} else {
				writtenBytes = int64(len(task.Data))
				writtenBytes, timing, err = writeReaderWithSyncer(filePath, bytes.NewReader(task.Data), s.durabilityMode, s.groupSyncer)
			}
			if err != nil {
				log.Printf("[Async IO] Disk Write Failed for %s: %v", filePath, err)
				writeErr = err
			}
		}
		log.Printf(
			"[Storage Phase] op=STORE worker_id=%d write_class=%s durability_mode=%s key=%s size_bytes=%d queue_wait_ms=%d durable_write_ms=%d file_write_ms=%d fsync_ms=%d err=%v",
			workerID,
			normalizeStorageWriteClass(task.WriteClass),
			s.durabilityMode,
			task.Key,
			writtenBytes,
			queueWait.Milliseconds(),
			timing.Total.Milliseconds(),
			timing.Write.Milliseconds(),
			timing.Sync.Milliseconds(),
			writeErr,
		)

		// Update metrics (attempt count)
		s.lock.Lock()
		s.totalOperations++
		s.lock.Unlock()

		s.releaseQueuedWriteBytes(taskSize)
		s.releaseQueuedWriteClassBytes(taskSize, task.WriteClass)
		if task.Done != nil {
			task.Done <- writeTaskResult{SizeBytes: writtenBytes, Err: writeErr}
			close(task.Done)
		}
	}
}

func (s *storageEngine) nextWriteTask() (*WriteTask, bool) {
	select {
	case task, ok := <-s.foregroundWriteQueue:
		return task, ok
	default:
	}
	select {
	case task, ok := <-s.foregroundWriteQueue:
		return task, ok
	case task, ok := <-s.backgroundWriteQueue:
		return task, ok
	}
}

func (s *storageEngine) totalWriteQueueDepth() int {
	return len(s.foregroundWriteQueue) + len(s.backgroundWriteQueue)
}

func (s *storageEngine) currentQueuedWriteBytes() int64 {
	return atomic.LoadInt64(&s.queuedWriteBytes)
}

func (s *storageEngine) currentQueuedForegroundWriteBytes() int64 {
	return atomic.LoadInt64(&s.queuedForegroundWriteBytes)
}

func (s *storageEngine) currentQueuedBackgroundWriteBytes() int64 {
	return atomic.LoadInt64(&s.queuedBackgroundWriteBytes)
}

func (s *storageEngine) reserveQueuedWriteBytes(bytes int64) bool {
	if bytes < 0 {
		bytes = 0
	}
	limit := atomic.LoadInt64(&s.maxQueuedWriteBytes)
	if limit <= 0 {
		atomic.AddInt64(&s.queuedWriteBytes, bytes)
		return true
	}
	for {
		current := atomic.LoadInt64(&s.queuedWriteBytes)
		next := current + bytes
		if next > limit {
			return false
		}
		if atomic.CompareAndSwapInt64(&s.queuedWriteBytes, current, next) {
			return true
		}
	}
}

func (s *storageEngine) reserveQueuedWriteClassBytes(bytes int64, writeClass string) bool {
	if bytes < 0 {
		bytes = 0
	}
	writeClass = normalizeStorageWriteClass(writeClass)
	if writeClass != config.StorageWriteClassBackground {
		atomic.AddInt64(&s.queuedForegroundWriteBytes, bytes)
		return true
	}
	limit := atomic.LoadInt64(&s.maxBackgroundQueuedWriteBytes)
	if limit <= 0 {
		atomic.AddInt64(&s.queuedBackgroundWriteBytes, bytes)
		return true
	}
	for {
		current := atomic.LoadInt64(&s.queuedBackgroundWriteBytes)
		next := current + bytes
		if next > limit {
			return false
		}
		if atomic.CompareAndSwapInt64(&s.queuedBackgroundWriteBytes, current, next) {
			return true
		}
	}
}

func (s *storageEngine) releaseQueuedWriteBytes(bytes int64) {
	if bytes <= 0 {
		return
	}
	next := atomic.AddInt64(&s.queuedWriteBytes, -bytes)
	if next < 0 {
		atomic.StoreInt64(&s.queuedWriteBytes, 0)
	}
}

func (s *storageEngine) releaseQueuedWriteClassBytes(bytes int64, writeClass string) {
	if bytes <= 0 {
		return
	}
	var target *int64
	if normalizeStorageWriteClass(writeClass) == config.StorageWriteClassBackground {
		target = &s.queuedBackgroundWriteBytes
	} else {
		target = &s.queuedForegroundWriteBytes
	}
	next := atomic.AddInt64(target, -bytes)
	if next < 0 {
		atomic.StoreInt64(target, 0)
	}
}

// _getSafePath prevents directory traversal attacks.
func (s *storageEngine) _getSafePath(key string) (string, error) {
	safeKey := filepath.Clean(key)
	if strings.Contains(safeKey, "..") || strings.HasPrefix(safeKey, "/") {
		return "", fmt.Errorf("invalid key: %s", key)
	}
	return filepath.Join(s.storageDir, safeKey), nil
}

// store enqueues a write task and waits for durable completion before returning.
func (s *storageEngine) store(ctx context.Context, key string, data []byte) (int, error) {
	size, err := s.storeStream(ctx, key, bytes.NewReader(data), int64(len(data)))
	return int(size), err
}

// storeStream enqueues a request body stream and waits for durable completion before returning.
func (s *storageEngine) storeStream(ctx context.Context, key string, reader io.Reader, sizeBytes int64) (int64, error) {
	return s.storeStreamWithClass(ctx, key, reader, sizeBytes, config.StorageWriteClassForeground)
}

// storeStreamWithClass enqueues a foreground or background request body stream and waits for durable completion.
func (s *storageEngine) storeStreamWithClass(ctx context.Context, key string, reader io.Reader, sizeBytes int64, writeClass string) (int64, error) {
	_, err := s._getSafePath(key)
	if err != nil {
		return 0, err
	}
	writeClass = normalizeStorageWriteClass(writeClass)
	dataBytes := sizeBytes
	if dataBytes < 0 {
		dataBytes = 0
	}
	if !s.reserveQueuedWriteBytes(dataBytes) {
		return 0, fmt.Errorf(
			"storage node overloaded (queued write bytes %d + request bytes %d exceeds limit %d)",
			s.currentQueuedWriteBytes(),
			dataBytes,
			atomic.LoadInt64(&s.maxQueuedWriteBytes),
		)
	}
	if !s.reserveQueuedWriteClassBytes(dataBytes, writeClass) {
		s.releaseQueuedWriteBytes(dataBytes)
		return 0, fmt.Errorf(
			"storage node overloaded (%s queued write bytes %d + request bytes %d exceeds limit %d)",
			writeClass,
			s.currentQueuedBackgroundWriteBytes(),
			dataBytes,
			atomic.LoadInt64(&s.maxBackgroundQueuedWriteBytes),
		)
	}

	task := &WriteTask{
		Key:        key,
		Reader:     reader,
		SizeBytes:  dataBytes,
		WriteClass: writeClass,
		EnqueuedAt: time.Now(),
		Done:       make(chan writeTaskResult, 1),
	}
	queue := s.foregroundWriteQueue
	if writeClass == config.StorageWriteClassBackground {
		queue = s.backgroundWriteQueue
	}

	// Non-blocking enqueue
	select {
	case queue <- task:
		// Wait until worker reports durable write result.
		select {
		case result := <-task.Done:
			if result.Err != nil {
				return 0, result.Err
			}
			return result.SizeBytes, nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	default:
		s.releaseQueuedWriteBytes(dataBytes)
		s.releaseQueuedWriteClassBytes(dataBytes, writeClass)
		log.Printf("[Async IO] %s queue full; dropping request for key: %s", writeClass, key)
		return 0, fmt.Errorf("storage node overloaded (%s queue full)", writeClass)
	}
}

// retrieve reads data from disk synchronously.
func (s *storageEngine) retrieve(key string) ([]byte, error) {
	filePath, err := s._getSafePath(key)
	if err != nil {
		return nil, err
	}

	data, err := os.ReadFile(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			// Log resolved path details for not-found diagnostics.
			log.Printf("[Storage Debug] 404 Not Found. Key: '%s' | Resolved Path: '%s' | BaseDir: '%s'", key, filePath, s.storageDir)
			return nil, nil
		}
		log.Printf("Error reading file %s: %v", filePath, err)
		return nil, err
	}
	return data, nil
}

// delete removes data from disk synchronously.
func (s *storageEngine) delete(key string) (bool, error) {
	filePath, err := s._getSafePath(key)
	if err != nil {
		return false, err
	}

	err = os.Remove(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		log.Printf("Error deleting file %s: %v", filePath, err)
		return false, err
	}
	return true, nil
}

// getInfo returns current statistics.
func (s *storageEngine) getInfo() (map[string]interface{}, error) {
	s.lock.RLock()
	ops := s.totalOperations
	s.lock.RUnlock()

	var totalKeys int64 = 0
	var totalSize int64 = 0

	entries, err := os.ReadDir(s.storageDir)
	if err != nil {
		log.Printf("Error scanning storage dir: %v", err)
		return nil, err
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			info, err := entry.Info()
			if err == nil {
				totalKeys++
				totalSize += info.Size()
			}
		}
	}

	return map[string]interface{}{
		"total_keys":                        totalKeys,
		"total_size":                        totalSize,
		"total_operations":                  ops,
		"storage_path":                      s.storageDir,
		"write_queue_depth":                 s.totalWriteQueueDepth(),
		"write_queue_cap":                   cap(s.foregroundWriteQueue) + cap(s.backgroundWriteQueue),
		"foreground_queue_depth":            len(s.foregroundWriteQueue),
		"foreground_queue_cap":              cap(s.foregroundWriteQueue),
		"background_queue_depth":            len(s.backgroundWriteQueue),
		"background_queue_cap":              cap(s.backgroundWriteQueue),
		"io_workers":                        s.ioWorkerCount,
		"durability_mode":                   s.durabilityMode,
		"queued_write_bytes":                s.currentQueuedWriteBytes(),
		"foreground_queued_write_bytes":     s.currentQueuedForegroundWriteBytes(),
		"background_queued_write_bytes":     s.currentQueuedBackgroundWriteBytes(),
		"max_queued_write_bytes":            atomic.LoadInt64(&s.maxQueuedWriteBytes),
		"max_background_queued_write_bytes": atomic.LoadInt64(&s.maxBackgroundQueuedWriteBytes),
	}, nil
}
