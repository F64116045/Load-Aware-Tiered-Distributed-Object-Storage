package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"

	"hybrid_distributed_store/internal/config"
	"hybrid_distributed_store/internal/meta"
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

func getDiskBytes(path string) (int64, int64) {
	var fs syscall.Statfs_t
	if err := syscall.Statfs(path, &fs); err != nil {
		return 0, 0
	}
	freeBytes := int64(fs.Bavail) * int64(fs.Bsize)
	totalBytes := int64(fs.Blocks) * int64(fs.Bsize)
	return freeBytes, totalBytes
}

func parseCPULoad(loadavg string, cpuCount int) float64 {
	fields := strings.Fields(loadavg)
	if len(fields) == 0 {
		return 0
	}
	load1, err := strconv.ParseFloat(fields[0], 64)
	if err != nil {
		return 0
	}
	if cpuCount <= 0 {
		return load1
	}
	return load1 / float64(cpuCount)
}

func getCPULoad() float64 {
	data, err := os.ReadFile("/proc/loadavg")
	if err != nil {
		return 0
	}
	return parseCPULoad(string(data), runtime.NumCPU())
}

func registerAndHeartbeatMeta(ctx context.Context, metaStore meta.Repository, nodeURL string, storage *storageEngine) {
	if metaStore == nil {
		return
	}
	log.Printf("[%s] Starting metadata heartbeat...", nodeURL)

	ticker := time.NewTicker(config.NodeHeartbeatInterval)
	defer ticker.Stop()

	upsert := func(status string) {
		hbCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		freeBytes, totalBytes := getDiskBytes(storage.storageDir)
		err := metaStore.UpsertNodeHeartbeat(
			hbCtx,
			nodeURL, // Keep URL as node_id so API can directly use it as endpoint.
			freeBytes,
			totalBytes,
			len(storage.writeQueue),
			getCPULoad(),
			status,
		)
		if err != nil {
			log.Printf("[%s] metadata heartbeat failed: %v", nodeURL, err)
		}
	}

	upsert("UP")
	for {
		select {
		case <-ctx.Done():
			upsert("DOWN")
			return
		case <-ticker.C:
			upsert("UP")
		}
	}
}

// --- Main Entry Point ---

func main() {
	_ = config.Colors
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	nodePort := os.Getenv("NODE_PORT")
	nodeName := os.Getenv("NODE_NAME")
	storageDir := os.Getenv("STORAGE_DIR")
	if nodePort == "" || nodeName == "" || storageDir == "" {
		log.Fatal("Error: NODE_PORT, NODE_NAME, and STORAGE_DIR must be set.")
	}

	metaStore, metaErr := meta.NewRepository(meta.Config{
		Endpoint:        config.MetaEndpoint,
		RequireEndpoint: config.MetaRequireEndpoint,
		AuthToken:       config.MetaRPCAuthToken,
		Enabled:         config.MetaEnabled,
		DSN:             config.MetaDSN,
	})
	if metaErr != nil {
		if config.MetaEnabled && config.MetaRequireEndpoint {
			log.Fatalf("Metadata store init failed with META_REQUIRE_ENDPOINT=true: %v", metaErr)
		}
		log.Printf("Metadata store init failed: %v", metaErr)
		metaStore = nil
	} else if config.MetaEnabled {
		pingCtx, pingCancel := context.WithTimeout(context.Background(), 3*time.Second)
		if err := metaStore.Ping(pingCtx); err != nil {
			if config.MetaRequireEndpoint {
				pingCancel()
				log.Fatalf("Metadata ping failed with META_REQUIRE_ENDPOINT=true: %v", err)
			}
			log.Printf("Metadata ping failed: %v", err)
			metaStore = nil
		}
		pingCancel()
	}
	defer func() {
		if metaStore != nil {
			_ = metaStore.Close()
		}
	}()

	storage := newStorageEngine(nodePort, nodeName, storageDir)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	internalURL := fmt.Sprintf("http://%s:%s", nodeName, nodePort)
	go registerAndHeartbeatMeta(ctx, metaStore, internalURL, storage)

	// 5. Start Gin Server
	gin.SetMode(gin.ReleaseMode)

	// Use gin.New() + Logger + Recovery for Debugging
	router := gin.New()
	router.Use(gin.Recovery())
	router.Use(gin.Logger()) // [ADDED] 開啟 Access Log

	// 6. Bind Routes
	router.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"status":  "healthy",
			"service": fmt.Sprintf("storage_node_%s", storage.port),
		})
	})

	router.GET("/info", func(c *gin.Context) {
		info, err := storage.getInfo()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, info)
	})

	router.POST("/store", func(c *gin.Context) {
		key := c.Query("key")
		if key == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Missing 'key' query parameter"})
			return
		}

		data, err := io.ReadAll(c.Request.Body)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Failed to read body"})
			return
		}

		size, err := storage.store(c.Request.Context(), key, data)
		if err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": err.Error()})
			return
		}

		info, _ := storage.getInfo()
		c.JSON(http.StatusOK, gin.H{
			"status":     "ok",
			"key":        key,
			"size":       size,
			"total_keys": info["total_keys"],
		})
	})

	// [FIX] 提取 Handler 並同時支援 GET 和 HEAD 方法
	retrieveHandler := func(c *gin.Context) {
		key := c.Param("key")
		data, err := storage.retrieve(key)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		if data == nil {
			c.JSON(http.StatusNotFound, gin.H{"detail": "Key not found"})
			return
		}
		// 如果是 HEAD 請求，不需要回傳 Body，只要狀態碼即可
		if c.Request.Method == http.MethodHead {
			c.Status(http.StatusOK)
			return
		}
		c.Data(http.StatusOK, "application/octet-stream", data)
	}

	// 註冊兩個 Method
	router.GET("/retrieve/:key", retrieveHandler)
	router.HEAD("/retrieve/:key", retrieveHandler)

	router.DELETE("/delete/:key", func(c *gin.Context) {
		key := c.Param("key")
		deleted, err := storage.delete(key)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		if !deleted {
			c.JSON(http.StatusOK, gin.H{"status": "ok", "key": key, "detail": "not_found"})
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "ok", "key": key, "message": "deleted"})
	})

	// 7. Start Server
	listenAddr := "0.0.0.0:" + nodePort
	log.Printf("[%s] Gin Server starting on %s", nodeName, listenAddr)
	if err := router.Run(listenAddr); err != nil {
		log.Fatalf("[%s] Critical Error: Gin failed to start: %v", nodeName, err)
	}
}
