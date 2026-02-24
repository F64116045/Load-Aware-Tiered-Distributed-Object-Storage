package main

import (
	"context"
	"database/sql"
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
	"hybrid_distributed_store/internal/monitoringservice"
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

	// [FIX] Initialize Redpanda Client with isConsumer=false
	mqClient := mq.NewClient(false)
	defer mqClient.Close()

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
	router.PUT("/v2/objects/:id", func(c *gin.Context) {
		start := time.Now()
		objectID := strings.TrimSpace(c.Param("id"))
		if objectID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid object id"})
			return
		}

		replicaNodes, _, err := getDynamicNodes(c)
		if err != nil {
			return
		}

		bodyBytes, err := io.ReadAll(c.Request.Body)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "failed to read request body"})
			return
		}

		opResult, opErr := writeSvc.WriteReplication(c.Request.Context(), replicaNodes, objectID, bodyBytes)
		if opErr != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": opErr.Error()})
			return
		}
		if opResult == nil {
			opResult = map[string]interface{}{}
		}

		contentType := strings.TrimSpace(c.GetHeader("Content-Type"))
		if contentType == "" {
			contentType = "application/octet-stream"
		}

		opResult["status"] = "ok"
		opResult["object_id"] = objectID
		opResult["tier"] = "HOT"
		opResult["strategy"] = string(config.StrategyReplication)
		opResult["size_bytes"] = len(bodyBytes)
		opResult["content_type"] = contentType
		opResult["latency_ms"] = time.Since(start).Milliseconds()
		c.JSON(http.StatusCreated, opResult)
	})

	router.GET("/v2/objects/:id", func(c *gin.Context) {
		objectID := strings.TrimSpace(c.Param("id"))
		if objectID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid object id"})
			return
		}

		replicaNodes, ecNodes, err := getDynamicNodes(c)
		if err != nil {
			return
		}

		metadata, _, err := loadMetadata(c.Request.Context(), objectID, etcdClient, metaStore, config.MetaSource)
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
			dataBytes, err = readSvc.ReadReplication(c.Request.Context(), replicaNodes, objectID)
		case config.StrategyEC:
			dataBytes, err = readSvc.ReadEC(c.Request.Context(), ecNodes, metadata)
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

	// --- Write Endpoint ---
	router.POST("/write", func(c *gin.Context) {
		start := time.Now()
		key := c.Query("key")
		strategy := config.StorageStrategy(c.DefaultQuery("strategy", string(config.StrategyReplication)))
		hotOnlyStr := c.Query("hot_only")
		isHotOnly := strings.ToLower(hotOnlyStr) == "true"

		replicaNodes, ecNodes, err := getDynamicNodes(c)
		if err != nil {
			return
		}

		var opResult map[string]interface{}
		var opErr error
		var dataDict map[string]interface{}

		contentType := c.GetHeader("Content-Type")
		if !strings.HasPrefix(contentType, "application/json") {
			c.JSON(http.StatusUnsupportedMediaType, gin.H{
				"error":  "Invalid Content-Type",
				"detail": "Must be application/json",
			})
			return
		}

		if err := c.ShouldBindJSON(&dataDict); err != nil {
			c.JSON(http.StatusUnprocessableEntity, gin.H{"error": "Invalid JSON body"})
			return
		}
		if (dataDict == nil || len(dataDict) == 0) && c.Request.ContentLength > 0 {
			c.JSON(http.StatusUnprocessableEntity, gin.H{"error": "JSON body cannot be empty"})
			return
		}

		switch strategy {
		case config.StrategyReplication, config.StrategyEC:
			bodyBytes, errSer := utilsSvc.Serialize(dataDict)
			if errSer != nil {
				panic(fmt.Errorf("JSON serialization failed: %v", errSer))
			}
			if strategy == config.StrategyReplication {
				opResult, opErr = writeSvc.WriteReplication(c.Request.Context(), replicaNodes, key, bodyBytes)
			} else {
				opResult, opErr = writeSvc.WriteEC(c.Request.Context(), ecNodes, key, bodyBytes)
			}
		case config.StrategyFieldHybrid:
			opResult, opErr = writeSvc.WriteFieldHybrid(c.Request.Context(), replicaNodes, ecNodes, key, dataDict, isHotOnly)
		default:
			c.JSON(http.StatusUnprocessableEntity, gin.H{"error": "Invalid strategy"})
			return
		}

		if opErr != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": opErr.Error()})
			return
		}

		if opResult == nil {
			opResult = make(map[string]interface{})
		}

		opResult["status"] = "ok"
		opResult["strategy"] = string(strategy)
		opResult["key"] = key
		opResult["latency_ms"] = time.Since(start).Milliseconds()
		c.JSON(http.StatusOK, opResult)
	})

	// --- Read Endpoint ---
	router.GET("/read/:key", func(c *gin.Context) {
		key := c.Param("key")
		replicaNodes, ecNodes, err := getDynamicNodes(c)
		if err != nil {
			return
		}

		metadata, _, err := loadMetadata(c.Request.Context(), key, etcdClient, metaStore, config.MetaSource)
		if err != nil {
			if errors.Is(err, errMetadataNotFound) {
				c.JSON(http.StatusNotFound, gin.H{"detail": fmt.Sprintf("Metadata not found for key '%s'", key)})
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		strategyStr, _ := metadata["strategy"].(string)
		switch config.StorageStrategy(strategyStr) {
		case config.StrategyReplication, config.StrategyEC:
			var dataBytes []byte
			var errRead error
			if config.StorageStrategy(strategyStr) == config.StrategyReplication {
				dataBytes, errRead = readSvc.ReadReplication(c.Request.Context(), replicaNodes, key)
			} else {
				dataBytes, errRead = readSvc.ReadEC(c.Request.Context(), ecNodes, metadata)
			}

			if errRead != nil {
				c.JSON(http.StatusNotFound, gin.H{"detail": errRead.Error()})
				return
			}

			dataDict, errDes := utilsSvc.Deserialize(dataBytes)
			if errDes != nil {
				log.Printf("[Error] Key: %s, Parse failed: %v", key, errDes)

				c.JSON(http.StatusInternalServerError, gin.H{
					"status":  "error",
					"code":    500,
					"message": "Data check failed",
					"detail":  "The data retrieved is corrupted. Please retry.",
				})
				return
			} else {
				c.JSON(http.StatusOK, dataDict)
			}

		case config.StrategyFieldHybrid:
			dataDict, err := readSvc.ReadFieldHybrid(c.Request.Context(), replicaNodes, ecNodes, metadata)
			if err != nil {
				c.JSON(http.StatusNotFound, gin.H{"detail": err.Error()})
				return
			}
			c.JSON(http.StatusOK, dataDict)

		default:
			c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Unknown strategy in metadata: %s", strategyStr)})
		}
	})

	// --- Delete Endpoint ---
	router.DELETE("/delete/:key", func(c *gin.Context) {
		start := time.Now()
		key := c.Param("key")
		replicaNodes, ecNodes, err := getDynamicNodes(c)
		if err != nil {
			return
		}

		result := make(map[string]interface{})
		strategyStr := "N/A"
		etcdKey := fmt.Sprintf("metadata/%s", key)

		metadata, _, metaErr := loadMetadata(c.Request.Context(), key, etcdClient, metaStore, config.MetaSource)
		if metaErr != nil && !errors.Is(metaErr, errMetadataNotFound) {
			c.JSON(http.StatusInternalServerError, gin.H{"error": metaErr.Error()})
			return
		}
		if metadata == nil {
			metadata = make(map[string]interface{})
		}

		if len(metadata) > 0 {
			log.Printf("%sDelete [Index Found] key=%s%s\n", config.Colors["RED"], key, config.Colors["RESET"])
			strategyStr, _ = metadata["strategy"].(string)

			var hotCount, coldCount int
			var delErr error

			switch config.StorageStrategy(strategyStr) {
			case config.StrategyReplication:
				hotCount, delErr = storageOpsSvc.DeleteReplication(c.Request.Context(), replicaNodes, key)
				result["nodes_deleted"] = hotCount
			case config.StrategyEC:
				coldCount, delErr = storageOpsSvc.DeleteEC(c.Request.Context(), ecNodes, metadata)
				result["chunks_deleted"] = coldCount
			case config.StrategyFieldHybrid:
				hotCount, coldCount, delErr = storageOpsSvc.DeleteFieldHybrid(c.Request.Context(), replicaNodes, ecNodes, metadata)
				result["hot_nodes_deleted"] = hotCount
				result["cold_chunks_deleted"] = coldCount
			default:
				c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Unknown strategy: %s", strategyStr)})
				return
			}

			if delErr != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Data plane deletion failed: %v", delErr)})
				return
			}

			if config.MetaEnabled && metaStore != nil {
				if err := metaStore.DeleteNormalizedMetadata(c.Request.Context(), key); err != nil {
					c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Metadata(Postgres normalized) deletion failed: %v", err)})
					return
				}
			}
			if config.MetaSource != "postgres" && etcdClient != nil {
				_, err = etcdClient.Delete(c.Request.Context(), etcdKey)
				if err != nil {
					c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Metadata(Etcd) deletion failed: %v", err)})
					return
				}
			}

		} else {
			log.Printf("%sDelete [Index Not Found] key=%s. Executing Blind Delete...%s\n", config.Colors["YELLOW"], key, config.Colors["RESET"])
			strategyStr = "blind_delete"

			blindMetadata := map[string]interface{}{"key_name": key}
			storageOpsSvc.DeleteReplication(c.Request.Context(), replicaNodes, key)
			storageOpsSvc.DeleteEC(c.Request.Context(), ecNodes, blindMetadata)
			storageOpsSvc.DeleteFieldHybrid(c.Request.Context(), replicaNodes, ecNodes, blindMetadata)

			result["detail"] = "key_not_found_or_zombie_cleaned"
		}

		result["status"] = "ok"
		result["strategy"] = strategyStr
		result["key"] = key
		result["latency_ms"] = time.Since(start).Milliseconds()
		c.JSON(http.StatusOK, result)
	})

	// --- Monitoring Endpoints ---
	router.GET("/node_status", func(c *gin.Context) {
		var currentNodes []string
		NodeListLock.RLock()
		for _, url := range ActiveNodeURLs {
			currentNodes = append(currentNodes, url)
		}
		NodeListLock.RUnlock()

		if len(currentNodes) == 0 {
			c.JSON(http.StatusOK, gin.H{})
			return
		}

		statusMap, err := monitoringservice.FetchNodeStatus(c.Request.Context(), currentNodes)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to fetch node status: %v", err)})
			return
		}
		c.JSON(http.StatusOK, statusMap)
	})

	router.GET("/storage_usage", func(c *gin.Context) {
		var currentNodes []string
		NodeListLock.RLock()
		for _, url := range ActiveNodeURLs {
			currentNodes = append(currentNodes, url)
		}
		NodeListLock.RUnlock()

		if len(currentNodes) == 0 {
			c.JSON(http.StatusOK, gin.H{"total_system_size": 0, "active_nodes_with_data": 0, "total_nodes_queried": 0})
			return
		}

		usageMap, err := monitoringservice.FetchStorageUsage(c.Request.Context(), currentNodes)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to fetch storage usage: %v", err)})
			return
		}
		c.JSON(http.StatusOK, usageMap)
	})

	router.GET("/health", func(c *gin.Context) {
		hostname, _ := os.Hostname()
		apiStatus := "healthy"
		if config.MetaEnabled && metadataStatus != "up" {
			apiStatus = "degraded"
		}
		c.JSON(http.StatusOK, gin.H{
			"status":   apiStatus,
			"service":  "api_gateway",
			"hostname": hostname,
			"metadata": gin.H{
				"enabled":      config.MetaEnabled,
				"status":       metadataStatus,
				"driver":       config.MetaDriver,
				"source":       config.MetaSource,
				"auto_migrate": config.MetaAutoMigrate,
				"error":        metadataErr,
				"lookup":       metadataLookupSnapshot(),
			},
			"node_discovery": gin.H{
				"configured_source": config.NodeDiscoverySource,
				"active_source":     nodeDiscoveryActive,
				"stale_sec":         config.NodeHeartbeatStaleSec,
			},
		})
	})

	router.GET("/v2/admin/metrics-snapshot", func(c *gin.Context) {
		NodeListLock.RLock()
		activeNodes := len(ActiveNodeURLs)
		NodeListLock.RUnlock()
		c.JSON(http.StatusOK, gin.H{
			"metadata_lookup": metadataLookupSnapshot(),
			"node_discovery": gin.H{
				"configured_source": config.NodeDiscoverySource,
				"active_source":     nodeDiscoveryActive,
				"active_node_count": activeNodes,
			},
			"timestamp_unix": time.Now().Unix(),
		})
	})

	router.GET("/v2/admin/tasks", func(c *gin.Context) {
		if !config.MetaEnabled || metaStore == nil {
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

		tasks, err := metaStore.ListTieringTasks(c.Request.Context(), state, taskType, limit)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		stateCounts, err := metaStore.ListTieringTaskStateCounts(c.Request.Context(), taskType)
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
		if !config.MetaEnabled || metaStore == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "metadata store unavailable"})
			return
		}

		taskID := strings.TrimSpace(c.Param("id"))
		if taskID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid task id"})
			return
		}

		ok, err := metaStore.RequeueTieringTaskNow(c.Request.Context(), taskID)
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
		if !config.MetaEnabled || metaStore == nil {
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

		ok, err := metaStore.CancelTieringTask(c.Request.Context(), taskID, reason)
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

	router.GET("/v2/admin/nodes", func(c *gin.Context) {
		if !config.MetaEnabled || metaStore == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "metadata store unavailable"})
			return
		}

		limit := 100
		if raw := strings.TrimSpace(c.Query("limit")); raw != "" {
			parsed, err := strconv.Atoi(raw)
			if err != nil || parsed <= 0 {
				c.JSON(http.StatusBadRequest, gin.H{"error": "invalid limit"})
				return
			}
			limit = parsed
		}

		nodes, err := metaStore.ListNodeHeartbeats(c.Request.Context(), limit)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		staleWindow := time.Duration(config.NodeHeartbeatStaleSec) * time.Second
		now := time.Now()
		out := make([]gin.H, 0, len(nodes))
		for _, n := range nodes {
			isStale := now.Sub(n.LastSeenAt) > staleWindow
			out = append(out, gin.H{
				"node_id":        n.NodeID,
				"status":         n.Status,
				"last_seen_at":   n.LastSeenAt,
				"is_stale":       isStale,
				"free_bytes":     n.FreeBytes,
				"io_queue_depth": n.IOQueueDepth,
				"cpu_load":       n.CPULoad,
			})
		}

		c.JSON(http.StatusOK, gin.H{
			"count":     len(out),
			"stale_sec": config.NodeHeartbeatStaleSec,
			"nodes":     out,
			"source":    "postgres",
			"generated": now.Unix(),
		})
	})

	router.GET("/v2/admin/objects/:id", func(c *gin.Context) {
		if !config.MetaEnabled || metaStore == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "metadata store unavailable"})
			return
		}

		objectID := strings.TrimSpace(c.Param("id"))
		if objectID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid object id"})
			return
		}

		view, err := metaStore.GetObjectAdminView(c.Request.Context(), objectID)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		if view == nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "object not found"})
			return
		}

		var version interface{}
		if view.Version != nil {
			version = gin.H{
				"version":         view.Version.Version,
				"size_bytes":      view.Version.SizeBytes,
				"checksum_sha256": view.Version.ChecksumSHA256,
				"tier":            view.Version.Tier,
				"encoding_k":      nullInt64OrNil(view.Version.EncodingK),
				"encoding_m":      nullInt64OrNil(view.Version.EncodingM),
				"created_at":      view.Version.CreatedAt,
			}
		}

		replicas := make([]gin.H, 0, len(view.ReplicaLocations))
		for _, r := range view.ReplicaLocations {
			replicas = append(replicas, gin.H{
				"node_id": r.NodeID,
				"path":    r.Path,
				"status":  r.Status,
			})
		}

		shards := make([]gin.H, 0, len(view.ECShardLocations))
		for _, s := range view.ECShardLocations {
			shards = append(shards, gin.H{
				"shard_index": s.ShardIndex,
				"node_id":     s.NodeID,
				"path":        s.Path,
				"status":      s.Status,
			})
		}

		c.JSON(http.StatusOK, gin.H{
			"object_id":          view.ObjectID,
			"current_version":    view.CurrentVersion,
			"state":              view.State,
			"created_at":         view.CreatedAt,
			"updated_at":         view.UpdatedAt,
			"version":            version,
			"replica_locations":  replicas,
			"ec_shard_locations": shards,
		})
	})

	// 4. Start Server
	log.Println("[API] Starting Gin Server on 0.0.0.0:8000...")
	if err := router.Run("0.0.0.0:8000"); err != nil {
		log.Fatalf("Gin Server failed to start: %v", err)
	}
}

func nullInt64OrNil(v sql.NullInt64) interface{} {
	if !v.Valid {
		return nil
	}
	return v.Int64
}
