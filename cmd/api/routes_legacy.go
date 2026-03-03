package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"hybrid_distributed_store/internal/config"
	"hybrid_distributed_store/internal/monitoringservice"
)

type legacyRouteDeps struct {
	getDynamicNodes func(c *gin.Context) ([]string, []string, error)
	loadMetadata    func(ctx context.Context, key string) (map[string]interface{}, string, error)

	writeReplication func(ctx context.Context, replicaNodes []string, key string, value []byte) (map[string]interface{}, error)
	writeEC          func(ctx context.Context, ecNodes []string, key string, value []byte) (map[string]interface{}, error)
	writeFieldHybrid func(ctx context.Context, replicaNodes, ecNodes []string, key string, dataDict map[string]interface{}, hotOnly bool) (map[string]interface{}, error)

	serialize   func(data map[string]interface{}) ([]byte, error)
	deserialize func(data []byte) (map[string]interface{}, error)

	readReplication func(ctx context.Context, replicaNodes []string, key string) ([]byte, error)
	readEC          func(ctx context.Context, ecNodes []string, metadata map[string]interface{}) ([]byte, error)
	readFieldHybrid func(ctx context.Context, replicaNodes, ecNodes []string, metadata map[string]interface{}) (map[string]interface{}, error)

	deleteReplication func(ctx context.Context, replicaNodes []string, key string) (int, error)
	deleteEC          func(ctx context.Context, ecNodes []string, metadata map[string]interface{}) (int, error)
	deleteFieldHybrid func(ctx context.Context, replicaNodes, ecNodes []string, metadata map[string]interface{}) (int, int, error)

	deleteNormalizedMetadata func(ctx context.Context, key string) error
	deleteEtcdMetadata       func(ctx context.Context, key string) error

	getActiveNodes func() []string
}

func registerLegacyRoutes(router gin.IRoutes, deps legacyRouteDeps) {
	router.POST("/write", func(c *gin.Context) {
		start := time.Now()
		key := c.Query("key")
		strategy := config.StorageStrategy(c.DefaultQuery("strategy", string(config.StrategyReplication)))
		hotOnlyStr := c.Query("hot_only")
		isHotOnly := strings.ToLower(hotOnlyStr) == "true"

		replicaNodes, ecNodes, err := deps.getDynamicNodes(c)
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
			bodyBytes, errSer := deps.serialize(dataDict)
			if errSer != nil {
				panic(fmt.Errorf("JSON serialization failed: %v", errSer))
			}
			if strategy == config.StrategyReplication {
				opResult, opErr = deps.writeReplication(c.Request.Context(), replicaNodes, key, bodyBytes)
			} else {
				opResult, opErr = deps.writeEC(c.Request.Context(), ecNodes, key, bodyBytes)
			}
		case config.StrategyFieldHybrid:
			opResult, opErr = deps.writeFieldHybrid(c.Request.Context(), replicaNodes, ecNodes, key, dataDict, isHotOnly)
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

	router.GET("/read/:key", func(c *gin.Context) {
		key := c.Param("key")
		replicaNodes, ecNodes, err := deps.getDynamicNodes(c)
		if err != nil {
			return
		}

		metadata, _, err := deps.loadMetadata(c.Request.Context(), key)
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
				dataBytes, errRead = deps.readReplication(c.Request.Context(), replicaNodes, key)
			} else {
				dataBytes, errRead = deps.readEC(c.Request.Context(), ecNodes, metadata)
			}

			if errRead != nil {
				c.JSON(http.StatusNotFound, gin.H{"detail": errRead.Error()})
				return
			}

			dataDict, errDes := deps.deserialize(dataBytes)
			if errDes != nil {
				log.Printf("[Error] Key: %s, Parse failed: %v", key, errDes)
				c.JSON(http.StatusInternalServerError, gin.H{
					"status":  "error",
					"code":    500,
					"message": "Data check failed",
					"detail":  "The data retrieved is corrupted. Please retry.",
				})
				return
			}
			c.JSON(http.StatusOK, dataDict)

		case config.StrategyFieldHybrid:
			dataDict, err := deps.readFieldHybrid(c.Request.Context(), replicaNodes, ecNodes, metadata)
			if err != nil {
				c.JSON(http.StatusNotFound, gin.H{"detail": err.Error()})
				return
			}
			c.JSON(http.StatusOK, dataDict)

		default:
			c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Unknown strategy in metadata: %s", strategyStr)})
		}
	})

	router.DELETE("/delete/:key", func(c *gin.Context) {
		start := time.Now()
		key := c.Param("key")
		replicaNodes, ecNodes, err := deps.getDynamicNodes(c)
		if err != nil {
			return
		}

		result := make(map[string]interface{})
		strategyStr := "N/A"
		metadata, _, metaErr := deps.loadMetadata(c.Request.Context(), key)
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
				hotCount, delErr = deps.deleteReplication(c.Request.Context(), replicaNodes, key)
				result["nodes_deleted"] = hotCount
			case config.StrategyEC:
				coldCount, delErr = deps.deleteEC(c.Request.Context(), ecNodes, metadata)
				result["chunks_deleted"] = coldCount
			case config.StrategyFieldHybrid:
				hotCount, coldCount, delErr = deps.deleteFieldHybrid(c.Request.Context(), replicaNodes, ecNodes, metadata)
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
			if err := deps.deleteNormalizedMetadata(c.Request.Context(), key); err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Metadata(Postgres normalized) deletion failed: %v", err)})
				return
			}
			if err := deps.deleteEtcdMetadata(c.Request.Context(), key); err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Metadata(Etcd) deletion failed: %v", err)})
				return
			}
		} else {
			log.Printf("%sDelete [Index Not Found] key=%s. Executing Blind Delete...%s\n", config.Colors["YELLOW"], key, config.Colors["RESET"])
			strategyStr = "blind_delete"

			blindMetadata := map[string]interface{}{"key_name": key}
			deps.deleteReplication(c.Request.Context(), replicaNodes, key)
			deps.deleteEC(c.Request.Context(), ecNodes, blindMetadata)
			deps.deleteFieldHybrid(c.Request.Context(), replicaNodes, ecNodes, blindMetadata)
			result["detail"] = "key_not_found_or_zombie_cleaned"
		}

		result["status"] = "ok"
		result["strategy"] = strategyStr
		result["key"] = key
		result["latency_ms"] = time.Since(start).Milliseconds()
		c.JSON(http.StatusOK, result)
	})

	router.GET("/node_status", func(c *gin.Context) {
		currentNodes := deps.getActiveNodes()
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
		currentNodes := deps.getActiveNodes()
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
}
