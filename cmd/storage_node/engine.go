package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// WriteTask represents an asynchronous write operation payload.
type WriteTask struct {
	Key  string
	Data []byte
	Done chan error
}

// storageEngine handles raw file I/O operations with asynchronous write support.
type storageEngine struct {
	storageDir      string
	port            string
	nodeName        string
	totalOperations int64
	lock            sync.RWMutex

	// writeQueue buffers incoming write requests for background processing.
	writeQueue chan *WriteTask
}

// newStorageEngine initializes the storage directory and the async engine.
func newStorageEngine(port, nodeName, storageDir string) *storageEngine {
	// Ensure the storage directory exists
	if err := os.MkdirAll(storageDir, 0755); err != nil {
		log.Fatalf("Failed to create storage directory %s: %v", storageDir, err)
	}

	log.Printf("Storage Node (PID: %d, Port: %s) Started.", os.Getpid(), port)
	log.Printf("Data persistence path: %s", storageDir)

	engine := &storageEngine{
		storageDir: storageDir,
		port:       port,
		nodeName:   nodeName,
		// Initialize a buffered channel with a capacity of 5000 to handle burst traffic.
		writeQueue: make(chan *WriteTask, 5000),
	}

	// Start the background I/O worker to consume tasks.
	go engine.startIoWorker()

	return engine
}

func writeFileDurably(path string, data []byte) error {
	// Ensure parent directories exist for nested keys.
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	if _, err := f.Write(data); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	return nil
}

// startIoWorker consumes write tasks from the queue and performs blocking durable disk I/O.
func (s *storageEngine) startIoWorker() {
	log.Println("[Async IO] Worker started. Waiting for tasks...")
	for task := range s.writeQueue {
		writeErr := error(nil)

		filePath, err := s._getSafePath(task.Key)
		if err != nil {
			log.Printf("[Async IO] Error resolving path for key %s: %v", task.Key, err)
			writeErr = err
		} else {
			// Perform the blocking disk write operation with fsync before ACK.
			if err := writeFileDurably(filePath, task.Data); err != nil {
				log.Printf("[Async IO] Disk Write Failed for %s: %v", filePath, err)
				writeErr = err
			}
		}

		// Update metrics (attempt count)
		s.lock.Lock()
		s.totalOperations++
		s.lock.Unlock()

		if task.Done != nil {
			task.Done <- writeErr
			close(task.Done)
		}
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
	_, err := s._getSafePath(key)
	if err != nil {
		return 0, err
	}

	task := &WriteTask{
		Key:  key,
		Data: data,
		Done: make(chan error, 1),
	}

	// Non-blocking enqueue
	select {
	case s.writeQueue <- task:
		// Wait until worker reports durable write result.
		select {
		case err := <-task.Done:
			if err != nil {
				return 0, err
			}
			return len(data), nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	default:
		log.Printf("[Async IO] Queue Full! Dropping request for key: %s", key)
		return 0, fmt.Errorf("storage node overloaded (queue full)")
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
		"total_keys":        totalKeys,
		"total_size":        totalSize,
		"total_operations":  ops,
		"storage_path":      s.storageDir,
		"write_queue_depth": len(s.writeQueue),
		"write_queue_cap":   cap(s.writeQueue),
	}, nil
}
