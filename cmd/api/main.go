package main

import (
	"context"
	"encoding/json"
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
	etcd "go.etcd.io/etcd/client/v3"

	"hybrid_distributed_store/internal/config"
	"hybrid_distributed_store/internal/ec"
	etcdclient "hybrid_distributed_store/internal/etcd"
	"hybrid_distributed_store/internal/httpclient"
	"hybrid_distributed_store/internal/meta"
	"hybrid_distributed_store/internal/mq"
	"hybrid_distributed_store/internal/readservice"
	"hybrid_distributed_store/internal/storageops"
	"hybrid_distributed_store/internal/utils"
	"hybrid_distributed_store/internal/writeservice"
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
	etcdHit               uint64
	notFound              uint64
	errorCount            uint64
}

var lookupMetrics metadataLookupMetrics

func recordMetadataLookupHit(source string) {
	switch source {
	case "postgres_normalized":
		atomic.AddUint64(&lookupMetrics.postgresNormalizedHit, 1)
	case "etcd":
		atomic.AddUint64(&lookupMetrics.etcdHit, 1)
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
		"etcd_hit":                atomic.LoadUint64(&lookupMetrics.etcdHit),
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

// watchNodesTask monitors Etcd for storage node registration/deregistration.
func watchNodesTask(ctx context.Context, etcdClient *etcd.Client) {
	keyPrefix := "nodes/health/"
	log.Printf("%s[API] Service Discovery started. Watching prefix: '%s'%s\n", config.Colors["CYAN"], keyPrefix, config.Colors["RESET"])

	defer func() {
		if r := recover(); r != nil {
			log.Printf("%s[API-Watcher] PANIC: %v%s\n", config.Colors["RED"], r, config.Colors["RESET"])
			log.Println(string(debug.Stack()))
		}
		log.Printf("%s[API-Watcher] Service Discovery stopped.%s\n", config.Colors["RED"], config.Colors["RESET"])
	}()

	// 1. Initial Fetch
	rangeResp, err := etcdClient.Get(ctx, keyPrefix, etcd.WithPrefix())
	if err != nil {
		log.Printf("%s[API] Initial node fetch failed: %v%s\n", config.Colors["RED"], err, config.Colors["RESET"])
	} else {
		NodeListLock.Lock()
		ActiveNodeURLs = make(map[string]string)
		for _, kv := range rangeResp.Kvs {
			parts := strings.Split(string(kv.Key), "/")
			if len(parts) < 3 {
				continue
			}
			nodeName := parts[2]
			nodeURL := string(kv.Value)
			if _, ok := config.ExpectedNodeNames[nodeName]; ok {
				ActiveNodeURLs[nodeName] = nodeURL
			}
		}
		log.Printf("%s[API] Initial Nodes: %v%s\n", config.Colors["CYAN"], getKeys(ActiveNodeURLs), config.Colors["RESET"])

		if len(ActiveNodeURLs) >= config.K && !nodesReady {
			nodesReady = true
			close(NodesReadyEvent)
		}
		NodeListLock.Unlock()
	}

	// 2. Watch Loop
	watchChan := etcdClient.Watch(ctx, keyPrefix, etcd.WithPrefix())
	for watchResp := range watchChan {
		if err := watchResp.Err(); err != nil {
			log.Printf("%s[API] Watch error: %v%s\n", config.Colors["RED"], err, config.Colors["RESET"])
			time.Sleep(2 * time.Second)
			continue
		}

		NodeListLock.Lock()
		for _, event := range watchResp.Events {
			parts := strings.Split(string(event.Kv.Key), "/")
			if len(parts) < 3 {
				continue
			}
			nodeName := parts[2]

			if _, ok := config.ExpectedNodeNames[nodeName]; !ok {
				continue
			}

			if event.Type == etcd.EventTypePut {
				nodeURL := string(event.Kv.Value)
				if _, exists := ActiveNodeURLs[nodeName]; !exists {
					log.Printf("%s[API] New Node Detected: %s%s\n", config.Colors["GREEN"], nodeName, config.Colors["RESET"])
					ActiveNodeURLs[nodeName] = nodeURL
					if len(ActiveNodeURLs) >= config.K && !nodesReady {
						nodesReady = true
						close(NodesReadyEvent)
					}
				}
			} else if event.Type == etcd.EventTypeDelete {
				if _, exists := ActiveNodeURLs[nodeName]; exists {
					log.Printf("%s[API] Node Lost: %s%s\n", config.Colors["RED"], nodeName, config.Colors["RESET"])
					delete(ActiveNodeURLs, nodeName)
					if len(ActiveNodeURLs) < config.K && nodesReady {
						nodesReady = false
						NodesReadyEvent = make(chan struct{})
					}
				}
			}
		}
		NodeListLock.Unlock()
	}
}

// watchNodesFromPostgres polls node heartbeats and updates active node list.
func watchNodesFromPostgres(ctx context.Context, metaStore *meta.Store) {
	log.Printf("%s[API] Service Discovery started. Source: postgres%s\n", config.Colors["CYAN"], config.Colors["RESET"])

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
			log.Printf("%s[API] PostgreSQL node fetch failed: %v%s\n", config.Colors["RED"], err, config.Colors["RESET"])
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

	replicaNodes := allNodes
	if len(allNodes) > 3 {
		replicaNodes = allNodes[:3]
	}
	ecNodes := allNodes

	if len(replicaNodes) < 3 {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Service unavailable: Insufficient replica nodes (need 3)"})
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

func getKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func loadMetadata(ctx context.Context, key string, etcdClient *etcd.Client, metaStore *meta.Store, source string) (map[string]interface{}, string, error) {
	readFromPostgres := source == "auto" || source == "postgres"
	readFromEtcd := source == "auto" || source == "etcd"

	if readFromPostgres && config.MetaEnabled && metaStore != nil {
		pgNormalizedMeta, err := metaStore.GetNormalizedMetadata(ctx, key)
		if err != nil {
			recordMetadataLookupError()
			return nil, "", err
		}
		if len(pgNormalizedMeta) > 0 {
			recordMetadataLookupHit("postgres_normalized")
			return pgNormalizedMeta, "postgres_normalized", nil
		}
		if source == "postgres" {
			recordMetadataLookupNotFound()
			return nil, "", errMetadataNotFound
		}
	}

	if !readFromEtcd {
		recordMetadataLookupNotFound()
		return nil, "", errMetadataNotFound
	}
	if etcdClient == nil {
		if source == "etcd" {
			recordMetadataLookupError()
			return nil, "", fmt.Errorf("etcd client unavailable")
		}
		recordMetadataLookupNotFound()
		return nil, "", errMetadataNotFound
	}

	rangeResp, err := etcdClient.Get(ctx, fmt.Sprintf("metadata/%s", key))
	if err != nil {
		recordMetadataLookupError()
		return nil, "", fmt.Errorf("etcd query failed: %v", err)
	}
	if len(rangeResp.Kvs) == 0 {
		recordMetadataLookupNotFound()
		return nil, "", errMetadataNotFound
	}

	var metadata map[string]interface{}
	if err := json.Unmarshal(rangeResp.Kvs[0].Value, &metadata); err != nil {
		recordMetadataLookupError()
		return nil, "", fmt.Errorf("metadata parse failed: %v", err)
	}
	recordMetadataLookupHit("etcd")
	return metadata, "etcd", nil
}

type v2ObjectRouteDeps struct {
	getDynamicNodes              func(c *gin.Context) ([]string, []string, error)
	writeReplicationWithMetadata func(ctx context.Context, replicaNodes []string, key string, data []byte, metadata map[string]interface{}) (map[string]interface{}, error)
	loadMetadata                 func(ctx context.Context, key string) (map[string]interface{}, string, error)
	readReplication              func(ctx context.Context, replicaNodes []string, key string) ([]byte, error)
	readEC                       func(ctx context.Context, ecNodes []string, metadata map[string]interface{}) ([]byte, error)
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
}

func registerAdminTaskRoutes(router gin.IRoutes, deps adminTaskRouteDeps) {
	router.GET("/v2/admin/tasks", func(c *gin.Context) {
		if deps.metadataAvailable == nil || !deps.metadataAvailable() {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "metadata store unavailable"})
			return
		}

		state := strings.TrimSpace(c.Query("state"))
		taskType := strings.TrimSpace(c.Query("task_type"))
		limit := 100
		if raw := strings.TrimSpace(c.Query("limit")); raw != "" {
			parsed, err := strconv.Atoi(raw)
			if err != nil || parsed <= 0 {
				c.JSON(http.StatusBadRequest, gin.H{"error": "invalid limit"})
				return
			}
			limit = parsed
		}

		tasks, err := deps.listTasks(c.Request.Context(), state, taskType, limit)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
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
			out = append(out, gin.H{
				"task_id":      t.TaskID,
				"object_id":    t.ObjectID,
				"version":      t.Version,
				"task_type":    t.TaskType,
				"task_state":   t.TaskState,
				"priority":     t.Priority,
				"retry_count":  t.RetryCount,
				"last_error":   lastErr,
				"scheduled_at": t.ScheduledAt,
				"started_at":   startedAt,
				"finished_at":  finishedAt,
				"actions": gin.H{
					"retry_now": retryNowAllowed,
					"cancel":    cancelAllowed,
				},
			})
		}

		c.JSON(http.StatusOK, gin.H{
			"count": len(out),
			"filters": gin.H{
				"state":     state,
				"task_type": taskType,
				"limit":     limit,
			},
			"state_counts": stateCounts,
			"tasks":        out,
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

	// 1. Initialize Services
	var etcdClient *etcd.Client
	requiresEtcd := !(config.MetaSource == "postgres" && config.NodeDiscoverySource == "postgres")
	if requiresEtcd {
		etcdClient = etcdclient.GetClient()
	}
	httpClient := httpclient.GetClient()

	metadataStatus := "disabled"
	metadataErr := ""
	nodeDiscoveryActive := config.NodeDiscoverySource
	metaStore, err := meta.NewStore(meta.Config{
		Enabled:         config.MetaEnabled,
		Driver:          config.MetaDriver,
		DSN:             config.MetaDSN,
		MaxOpenConns:    config.MetaMaxOpenConns,
		MaxIdleConns:    config.MetaMaxIdleConns,
		ConnMaxLifetime: config.MetaConnMaxLifetime,
	})
	if err != nil {
		metadataStatus = "down"
		metadataErr = err.Error()
		log.Printf("[API] Metadata init failed: %v", err)
	} else if config.MetaEnabled {
		pingCtx, pingCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer pingCancel()
		if pingErr := metaStore.Ping(pingCtx); pingErr != nil {
			metadataStatus = "down"
			metadataErr = pingErr.Error()
			log.Printf("[API] Metadata ping failed: %v", pingErr)
		} else {
			metadataStatus = "up"
			if config.MetaAutoMigrate {
				migrateCtx, migrateCancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer migrateCancel()
				if migrateErr := meta.NewMigrator(metaStore).Up(migrateCtx); migrateErr != nil {
					metadataStatus = "down"
					metadataErr = fmt.Sprintf("auto_migrate_failed: %v", migrateErr)
					log.Printf("[API] Metadata auto migration failed: %v", migrateErr)
				} else {
					log.Printf("[API] Metadata auto migration completed.")
				}
			}
		}
	}
	defer func() {
		if metaStore != nil {
			_ = metaStore.Close()
		}
	}()

	// Initialize WAL client only when synchronous WAL is enabled.
	var mqClient *mq.Client
	if config.WALEnabled {
		mqClient = mq.NewClient(false)
		defer mqClient.Close()
	} else {
		log.Printf("[API] WAL disabled (WAL_ENABLED=false). Skipping Redpanda client init.")
	}

	utilsSvc := utils.NewService()
	ecDriver := ec.NewService()
	storageOpsSvc := storageops.NewService(httpClient)

	readSvc := readservice.NewService(httpClient, ecDriver, utilsSvc)

	// Inject mqClient into WriteService
	writeSvc := writeservice.NewService(etcdClient, mqClient, httpClient, readSvc, ecDriver, utilsSvc, metaStore)

	// 2. Start Service Discovery
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	switch config.NodeDiscoverySource {
	case "postgres":
		if config.MetaEnabled && metaStore != nil {
			go watchNodesFromPostgres(ctx, metaStore)
		} else {
			nodeDiscoveryActive = "etcd_fallback"
			log.Printf("%s[API] NODE_DISCOVERY_SOURCE=postgres but metadata is unavailable, fallback to etcd%s\n", config.Colors["YELLOW"], config.Colors["RESET"])
			if etcdClient != nil {
				go watchNodesTask(ctx, etcdClient)
			}
		}
	case "etcd":
		if etcdClient != nil {
			go watchNodesTask(ctx, etcdClient)
		}
	default: // auto
		if config.MetaEnabled && metaStore != nil {
			nodeDiscoveryActive = "postgres"
			go watchNodesFromPostgres(ctx, metaStore)
		} else {
			nodeDiscoveryActive = "etcd"
			if etcdClient != nil {
				go watchNodesTask(ctx, etcdClient)
			}
		}
	}

	// 3. Setup Router
	// Use gin.New() to avoid default Logger middleware which impacts performance benchmarks
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery()) // Keep recovery for stability
	router.Use(PanicRecoveryMiddleware)

	// --- v2 Generic Object Endpoints (binary body, replication-first) ---
	registerV2ObjectRoutes(router, v2ObjectRouteDeps{
		getDynamicNodes: getDynamicNodes,
		writeReplicationWithMetadata: func(ctx context.Context, replicaNodes []string, key string, data []byte, metadata map[string]interface{}) (map[string]interface{}, error) {
			return writeSvc.WriteReplicationWithMetadata(ctx, replicaNodes, key, data, metadata)
		},
		loadMetadata: func(ctx context.Context, key string) (map[string]interface{}, string, error) {
			return loadMetadata(ctx, key, etcdClient, metaStore, config.MetaSource)
		},
		readReplication: readSvc.ReadReplication,
		readEC:          readSvc.ReadEC,
		now:             time.Now,
	})

	registerLegacyRoutes(router, legacyRouteDeps{
		getDynamicNodes: getDynamicNodes,
		loadMetadata: func(ctx context.Context, key string) (map[string]interface{}, string, error) {
			return loadMetadata(ctx, key, etcdClient, metaStore, config.MetaSource)
		},
		writeReplication:  writeSvc.WriteReplication,
		writeEC:           writeSvc.WriteEC,
		writeFieldHybrid:  writeSvc.WriteFieldHybrid,
		serialize:         utilsSvc.Serialize,
		deserialize:       utilsSvc.Deserialize,
		readReplication:   readSvc.ReadReplication,
		readEC:            readSvc.ReadEC,
		readFieldHybrid:   readSvc.ReadFieldHybrid,
		deleteReplication: storageOpsSvc.DeleteReplication,
		deleteEC:          storageOpsSvc.DeleteEC,
		deleteFieldHybrid: storageOpsSvc.DeleteFieldHybrid,
		deleteNormalizedMetadata: func(ctx context.Context, key string) error {
			if !config.MetaEnabled || metaStore == nil {
				return nil
			}
			return metaStore.DeleteNormalizedMetadata(ctx, key)
		},
		deleteEtcdMetadata: func(ctx context.Context, key string) error {
			if config.MetaSource == "postgres" || etcdClient == nil {
				return nil
			}
			_, err := etcdClient.Delete(ctx, fmt.Sprintf("metadata/%s", key))
			return err
		},
		getActiveNodes: getActiveNodeURLs,
	})

	registerAdminObservabilityRoutes(router, adminObservabilityRouteDeps{
		metadataStatus:      metadataStatus,
		metadataErr:         metadataErr,
		nodeDiscoveryActive: nodeDiscoveryActive,
		getActiveNodeCount:  getActiveNodeCount,
		metaStore:           metaStore,
	})

	registerAdminTaskRoutes(router, adminTaskRouteDeps{
		metadataAvailable: func() bool { return config.MetaEnabled && metaStore != nil },
		listTasks:         metaStore.ListTieringTasks,
		listStateCounts:   metaStore.ListTieringTaskStateCounts,
		requeueNow:        metaStore.RequeueTieringTaskNow,
		cancelTask:        metaStore.CancelTieringTask,
	})

	registerAdminMetadataRoutes(router, metaStore)

	// 4. Start Server
	log.Println("[API] Starting Gin Server on 0.0.0.0:8000...")
	if err := router.Run("0.0.0.0:8000"); err != nil {
		log.Fatalf("Gin Server failed to start: %v", err)
	}
}
