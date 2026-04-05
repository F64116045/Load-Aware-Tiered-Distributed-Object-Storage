package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"

	"hybrid_distributed_store/internal/config"
	"hybrid_distributed_store/internal/meta"
)

// --- Service Discovery Globals ---
var (
	ActiveNodeURLs  = make(map[string]string)
	NodeListLock    = &sync.RWMutex{}
	NodesReadyEvent = make(chan struct{})
	nodesReady      = false
)

var errMetadataNotFound = errors.New("metadata not found")

type metadataLookupMetrics struct {
	postgresNormalizedHit uint64
	notFound              uint64
	errorCount            uint64
}

var lookupMetrics metadataLookupMetrics

func recordMetadataLookupHit(source string) {
	switch source {
	case "postgres_normalized":
		atomic.AddUint64(&lookupMetrics.postgresNormalizedHit, 1)
	}
}

func recordMetadataLookupNotFound() {
	atomic.AddUint64(&lookupMetrics.notFound, 1)
}

func recordMetadataLookupError() {
	atomic.AddUint64(&lookupMetrics.errorCount, 1)
}

func metadataLookupSnapshot() gin.H {
	return gin.H{
		"postgres_normalized_hit": atomic.LoadUint64(&lookupMetrics.postgresNormalizedHit),
		"not_found":               atomic.LoadUint64(&lookupMetrics.notFound),
		"error_count":             atomic.LoadUint64(&lookupMetrics.errorCount),
	}
}

func replaceActiveNodes(nodeURLs []string) {
	newMap := make(map[string]string, len(nodeURLs))
	for _, nodeURL := range nodeURLs {
		if nodeURL == "" {
			continue
		}
		newMap[nodeURL] = nodeURL
	}

	NodeListLock.Lock()
	ActiveNodeURLs = newMap
	if len(ActiveNodeURLs) >= config.K && !nodesReady {
		nodesReady = true
		close(NodesReadyEvent)
	}
	if len(ActiveNodeURLs) < config.K && nodesReady {
		nodesReady = false
		NodesReadyEvent = make(chan struct{})
	}
	NodeListLock.Unlock()
}

// watchNodesFromMetadata polls node heartbeats and updates active node list.
func watchNodesFromMetadata(ctx context.Context, metaStore meta.Repository) {
	log.Printf("%s[API] Service Discovery started. Source: metadata_repository%s\n", config.Colors["CYAN"], config.Colors["RESET"])

	defer func() {
		if r := recover(); r != nil {
			log.Printf("%s[API-PG-Watcher] PANIC: %v%s\n", config.Colors["RED"], r, config.Colors["RESET"])
			log.Println(string(debug.Stack()))
		}
		log.Printf("%s[API-PG-Watcher] Service Discovery stopped.%s\n", config.Colors["RED"], config.Colors["RESET"])
	}()

	load := func() {
		loadCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()

		nodeURLs, err := metaStore.ListHealthyNodeIDs(loadCtx, config.NodeHeartbeatStaleSec)
		if err != nil {
			log.Printf("%s[API] metadata node fetch failed: %v%s\n", config.Colors["RED"], err, config.Colors["RESET"])
			return
		}
		replaceActiveNodes(nodeURLs)
	}

	load()
	ticker := time.NewTicker(config.NodeHeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			load()
		}
	}
}

// getDynamicNodes ensures enough nodes are available for operations.
func getDynamicNodes(c *gin.Context) ([]string, []string, error) {
	select {
	case <-NodesReadyEvent:
	case <-time.After(30 * time.Second):
		log.Printf("%s[API] Timeout waiting for storage nodes%s\n", config.Colors["RED"], config.Colors["RESET"])
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Service unavailable: Storage node registration timeout"})
		return nil, nil, fmt.Errorf("service unavailable")
	}

	NodeListLock.RLock()
	allNodes := make([]string, 0, len(ActiveNodeURLs))
	for _, url := range ActiveNodeURLs {
		allNodes = append(allNodes, url)
	}
	sort.Strings(allNodes)
	NodeListLock.RUnlock()

	replicaTarget := config.HotReplicaCount
	if replicaTarget <= 0 {
		replicaTarget = 3
	}
	writeQuorum := config.HotWriteQuorum
	if writeQuorum <= 0 {
		writeQuorum = 1
	}
	if writeQuorum > replicaTarget {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error":  "Service unavailable: invalid hot write configuration",
			"detail": fmt.Sprintf("HOT_WRITE_QUORUM(%d) cannot exceed HOT_REPLICA_COUNT(%d)", writeQuorum, replicaTarget),
		})
		return nil, nil, fmt.Errorf("invalid hot write configuration")
	}

	replicaNodes := allNodes
	if len(allNodes) > replicaTarget {
		replicaNodes = allNodes[:replicaTarget]
	}
	ecNodes := allNodes

	if len(replicaNodes) < replicaTarget {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error":  "Service unavailable: Insufficient replica nodes",
			"detail": fmt.Sprintf("need %d healthy replica nodes", replicaTarget),
		})
		return nil, nil, fmt.Errorf("service unavailable")
	}
	if len(ecNodes) < config.K+config.M {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": fmt.Sprintf("Service unavailable: Insufficient EC nodes (need %d)", config.K+config.M)})
		return nil, nil, fmt.Errorf("service unavailable")
	}

	return replicaNodes, ecNodes, nil
}

// PanicRecoveryMiddleware handles unhandled panics to prevent server crash.
func PanicRecoveryMiddleware(c *gin.Context) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("%s[API] PANIC: %v%s\n", config.Colors["RED"], r, config.Colors["RESET"])
			debug.PrintStack()

			var errorMsg string
			if err, ok := r.(error); ok {
				errorMsg = err.Error()
			} else {
				errorMsg = fmt.Sprintf("%v", r)
			}

			c.JSON(http.StatusInternalServerError, gin.H{
				"error":  "Internal Server Error",
				"detail": errorMsg,
			})
			c.Abort()
		}
	}()
	c.Next()
}

func loadMetadata(ctx context.Context, key string, metaStore meta.Repository) (map[string]interface{}, string, error) {
	if !config.MetaEnabled || metaStore == nil {
		recordMetadataLookupError()
		return nil, "", fmt.Errorf("metadata store unavailable")
	}

	pgNormalizedMeta, err := metaStore.GetNormalizedMetadata(ctx, key)
	if err != nil {
		recordMetadataLookupError()
		return nil, "", err
	}
	if len(pgNormalizedMeta) == 0 {
		recordMetadataLookupNotFound()
		return nil, "", errMetadataNotFound
	}

	recordMetadataLookupHit("postgres_normalized")
	return pgNormalizedMeta, "postgres_normalized", nil
}

type v2ObjectRouteDeps struct {
	getDynamicNodes              func(c *gin.Context) ([]string, []string, error)
	writeReplicationWithMetadata func(ctx context.Context, replicaNodes []string, key string, data []byte, metadata map[string]interface{}) (map[string]interface{}, error)
	loadMetadata                 func(ctx context.Context, key string) (map[string]interface{}, string, error)
	readReplication              func(ctx context.Context, replicaNodes []string, key string) ([]byte, error)
	readEC                       func(ctx context.Context, ecNodes []string, metadata map[string]interface{}) ([]byte, error)
	deleteReplication            func(ctx context.Context, replicaNodes []string, key string) (int, error)
	deleteEC                     func(ctx context.Context, ecNodes []string, metadata map[string]interface{}) (int, error)
	deleteNormalizedMetadata     func(ctx context.Context, key string) error
	now                          func() time.Time
}

type adminTaskRouteDeps struct {
	metadataAvailable func() bool
	listTasks         func(ctx context.Context, state, taskType string, limit int) ([]meta.TieringTask, error)
	listStateCounts   func(ctx context.Context, taskType string) (map[string]int64, error)
	requeueNow        func(ctx context.Context, taskID string) (bool, error)
	cancelTask        func(ctx context.Context, taskID, reason string) (bool, error)
}

func registerV2ObjectRoutes(router gin.IRoutes, deps v2ObjectRouteDeps) {
	nowFn := deps.now
	if nowFn == nil {
		nowFn = time.Now
	}

	router.PUT("/v2/objects/:id", func(c *gin.Context) {
		start := nowFn()
		objectID := strings.TrimSpace(c.Param("id"))
		if objectID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid object id"})
			return
		}

		replicaNodes, _, err := deps.getDynamicNodes(c)
		if err != nil {
			return
		}

		bodyBytes, err := io.ReadAll(c.Request.Body)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "failed to read request body"})
			return
		}
		contentType := strings.TrimSpace(c.GetHeader("Content-Type"))
		if contentType == "" {
			contentType = "application/octet-stream"
		}

		opResult, opErr := deps.writeReplicationWithMetadata(
			c.Request.Context(),
			replicaNodes,
			objectID,
			bodyBytes,
			map[string]interface{}{
				"content_type": contentType,
			},
		)
		if opErr != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": opErr.Error()})
			return
		}
		if opResult == nil {
			opResult = map[string]interface{}{}
		}

		opResult["status"] = "ok"
		opResult["object_id"] = objectID
		opResult["tier"] = "HOT"
		opResult["strategy"] = string(config.StrategyReplication)
		opResult["size_bytes"] = len(bodyBytes)
		opResult["content_type"] = contentType
		opResult["latency_ms"] = nowFn().Sub(start).Milliseconds()
		c.JSON(http.StatusCreated, opResult)
	})

	router.GET("/v2/objects/:id", func(c *gin.Context) {
		objectID := strings.TrimSpace(c.Param("id"))
		if objectID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid object id"})
			return
		}

		replicaNodes, ecNodes, err := deps.getDynamicNodes(c)
		if err != nil {
			return
		}

		metadata, _, err := deps.loadMetadata(c.Request.Context(), objectID)
		if err != nil {
			if errors.Is(err, errMetadataNotFound) {
				c.JSON(http.StatusNotFound, gin.H{"detail": fmt.Sprintf("Metadata not found for key '%s'", objectID)})
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		strategyStr, _ := metadata["strategy"].(string)
		var dataBytes []byte
		switch config.StorageStrategy(strategyStr) {
		case config.StrategyReplication:
			dataBytes, err = deps.readReplication(c.Request.Context(), replicaNodes, objectID)
		case config.StrategyEC:
			dataBytes, err = deps.readEC(c.Request.Context(), ecNodes, metadata)
		default:
			c.JSON(http.StatusConflict, gin.H{
				"error":    "object is not binary-readable via /v2/objects",
				"strategy": strategyStr,
			})
			return
		}
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"detail": err.Error()})
			return
		}

		contentType := "application/octet-stream"
		if ct, ok := metadata["content_type"].(string); ok && strings.TrimSpace(ct) != "" {
			contentType = strings.TrimSpace(ct)
		}
		c.Data(http.StatusOK, contentType, dataBytes)
	})

	router.DELETE("/v2/objects/:id", func(c *gin.Context) {
		start := nowFn()
		objectID := strings.TrimSpace(c.Param("id"))
		if objectID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid object id"})
			return
		}

		if deps.deleteReplication == nil || deps.deleteEC == nil || deps.deleteNormalizedMetadata == nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "delete dependencies are not configured"})
			return
		}

		replicaNodes, ecNodes, err := deps.getDynamicNodes(c)
		if err != nil {
			return
		}

		metadata, _, err := deps.loadMetadata(c.Request.Context(), objectID)
		if err != nil {
			if errors.Is(err, errMetadataNotFound) {
				c.JSON(http.StatusNotFound, gin.H{"detail": fmt.Sprintf("Metadata not found for key '%s'", objectID)})
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		strategyStr, _ := metadata["strategy"].(string)
		result := gin.H{
			"status":    "ok",
			"object_id": objectID,
			"strategy":  strategyStr,
		}

		switch config.StorageStrategy(strategyStr) {
		case config.StrategyReplication:
			deleted, delErr := deps.deleteReplication(c.Request.Context(), replicaNodes, objectID)
			if delErr != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": delErr.Error()})
				return
			}
			result["nodes_deleted"] = deleted
		case config.StrategyEC:
			deleted, delErr := deps.deleteEC(c.Request.Context(), ecNodes, metadata)
			if delErr != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": delErr.Error()})
				return
			}
			result["chunks_deleted"] = deleted
		default:
			c.JSON(http.StatusConflict, gin.H{
				"error":    "object strategy is not deletable via /v2/objects",
				"strategy": strategyStr,
			})
			return
		}

		if err := deps.deleteNormalizedMetadata(c.Request.Context(), objectID); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("delete metadata failed: %v", err)})
			return
		}

		result["latency_ms"] = nowFn().Sub(start).Milliseconds()
		c.JSON(http.StatusOK, result)
	})
}

func registerAdminTaskRoutes(router gin.IRoutes, deps adminTaskRouteDeps) {
	router.GET("/v2/admin/tasks", func(c *gin.Context) {
		if deps.metadataAvailable == nil || !deps.metadataAvailable() {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "metadata store unavailable"})
			return
		}

		state := strings.TrimSpace(c.Query("state"))
		taskType := strings.TrimSpace(c.Query("task_type"))
		objectID := strings.TrimSpace(c.Query("object_id"))
		limit := 100
		limitSpecified := false
		if raw := strings.TrimSpace(c.Query("limit")); raw != "" {
			parsed, err := strconv.Atoi(raw)
			if err != nil || parsed <= 0 {
				c.JSON(http.StatusBadRequest, gin.H{"error": "invalid limit"})
				return
			}
			limit = parsed
			limitSpecified = true
		}
		if objectID != "" && !limitSpecified {
			// When filtering by object_id, fetch a larger window for admin troubleshooting.
			limit = 1000
		}

		tasks, err := deps.listTasks(c.Request.Context(), state, taskType, limit)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		if objectID != "" {
			filtered := make([]meta.TieringTask, 0, len(tasks))
			for _, t := range tasks {
				if t.ObjectID != objectID {
					continue
				}
				filtered = append(filtered, t)
			}
			tasks = filtered
		}
		stateCounts, err := deps.listStateCounts(c.Request.Context(), taskType)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		out := make([]gin.H, 0, len(tasks))
		for _, t := range tasks {
			lastErr := ""
			if t.LastError.Valid {
				lastErr = t.LastError.String
			}
			retryNowAllowed := t.TaskState == "PENDING" || t.TaskState == "RUNNING" || t.TaskState == "RETRY_WAIT" || t.TaskState == "FAILED"
			cancelAllowed := t.TaskState == "PENDING" || t.TaskState == "RUNNING" || t.TaskState == "RETRY_WAIT"
			var startedAt interface{}
			if t.StartedAt.Valid {
				startedAt = t.StartedAt.Time
			}
			var finishedAt interface{}
			if t.FinishedAt.Valid {
				finishedAt = t.FinishedAt.Time
			}
			retryLimitReached := t.RetryCount >= config.TieringTaskMaxRetryCount
			out = append(out, gin.H{
				"task_id":             t.TaskID,
				"object_id":           t.ObjectID,
				"version":             t.Version,
				"task_type":           t.TaskType,
				"task_state":          t.TaskState,
				"priority":            t.Priority,
				"retry_count":         t.RetryCount,
				"max_retry_count":     config.TieringTaskMaxRetryCount,
				"retry_limit_reached": retryLimitReached,
				"last_error":          lastErr,
				"scheduled_at":        t.ScheduledAt,
				"started_at":          startedAt,
				"finished_at":         finishedAt,
				"actions": gin.H{
					"retry_now": retryNowAllowed,
					"cancel":    cancelAllowed,
				},
			})
		}

		c.JSON(http.StatusOK, gin.H{
			"count": len(out),
			"filters": gin.H{
				"object_id": objectID,
				"state":     state,
				"task_type": taskType,
				"limit":     limit,
			},
			"state_counts":    stateCounts,
			"max_retry_count": config.TieringTaskMaxRetryCount,
			"tasks":           out,
		})
	})

	router.POST("/v2/admin/tasks/:id/retry-now", func(c *gin.Context) {
		if deps.metadataAvailable == nil || !deps.metadataAvailable() {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "metadata store unavailable"})
			return
		}

		taskID := strings.TrimSpace(c.Param("id"))
		if taskID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid task id"})
			return
		}

		ok, err := deps.requeueNow(c.Request.Context(), taskID)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		if !ok {
			c.JSON(http.StatusNotFound, gin.H{
				"error":   "task not found or not requeueable",
				"task_id": taskID,
			})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"status":  "ok",
			"task_id": taskID,
			"action":  "requeued_now",
		})
	})

	router.POST("/v2/admin/tasks/:id/cancel", func(c *gin.Context) {
		if deps.metadataAvailable == nil || !deps.metadataAvailable() {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "metadata store unavailable"})
			return
		}

		taskID := strings.TrimSpace(c.Param("id"))
		if taskID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid task id"})
			return
		}

		reason := strings.TrimSpace(c.Query("reason"))
		if reason == "" {
			var body struct {
				Reason string `json:"reason"`
			}
			if err := c.ShouldBindJSON(&body); err == nil {
				reason = strings.TrimSpace(body.Reason)
			}
		}
		if reason == "" {
			reason = "cancelled_by_admin"
		}

		ok, err := deps.cancelTask(c.Request.Context(), taskID, reason)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		if !ok {
			c.JSON(http.StatusNotFound, gin.H{
				"error":   "task not found or not cancellable",
				"task_id": taskID,
			})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"status":  "ok",
			"task_id": taskID,
			"action":  "cancelled",
			"reason":  reason,
		})
	})
}

func main() {
	log.Printf("%sAPI Gateway (PID: %d) Starting...%s\n", config.Colors["GREEN"], os.Getpid(), config.Colors["RESET"])
	runtime, cleanup := initAppRuntime()
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startNodeDiscovery(ctx, runtime)

	router := buildRouter(runtime)

	// 4. Start Server
	log.Println("[API] Starting Gin Server on 0.0.0.0:8000...")
	if err := router.Run("0.0.0.0:8000"); err != nil {
		log.Fatalf("Gin Server failed to start: %v", err)
	}
}
