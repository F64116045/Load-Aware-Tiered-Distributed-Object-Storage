package main

import (
	"context"
	"database/sql"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"hybrid_distributed_store/internal/config"
	"hybrid_distributed_store/internal/meta"
)

type adminObservabilityRouteDeps struct {
	metadataStatus       string
	metadataErr          string
	nodeDiscoveryActive  string
	getActiveNodeCount   func() int
	metaStore            meta.Repository
	tieringLeaderLockKey int64
}

func registerAdminObservabilityRoutes(router gin.IRoutes, deps adminObservabilityRouteDeps) {
	router.GET("/health", func(c *gin.Context) {
		hostname, _ := os.Hostname()
		apiStatus := "healthy"
		if config.MetaEnabled && deps.metadataStatus != "up" {
			apiStatus = "degraded"
		}
		c.JSON(http.StatusOK, gin.H{
			"status":   apiStatus,
			"service":  "api_gateway",
			"hostname": hostname,
			"metadata": gin.H{
				"enabled":      config.MetaEnabled,
				"status":       deps.metadataStatus,
				"driver":       config.MetaDriver,
				"source":       metadataSourceLabel(),
				"backend":      config.MetaBackend,
				"endpoint":     strings.TrimSpace(config.MetaEndpoint),
				"auto_migrate": config.MetaAutoMigrate,
				"error":        deps.metadataErr,
				"lookup":       metadataLookupSnapshot(),
			},
			"node_discovery": gin.H{
				"configured_source": metadataSourceLabel(),
				"active_source":     deps.nodeDiscoveryActive,
				"stale_sec":         config.NodeHeartbeatStaleSec,
			},
		})
	})

	router.GET("/v2/admin/metrics-snapshot", func(c *gin.Context) {
		leader := gin.H{
			"lock_key": config.TieringPolicyLeaderLockKey,
			"enabled":  config.MetaEnabled && deps.metaStore != nil,
		}
		if config.MetaEnabled && deps.metaStore != nil {
			leaderCtx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
			state, err := deps.metaStore.GetTieringLeaderState(leaderCtx, deps.tieringLeaderLockKey)
			cancel()
			if err != nil {
				leader["error"] = err.Error()
			} else if state != nil {
				lastBeatAgoSec := int(time.Since(state.LastHeartbeatAt).Seconds())
				leader["leader_id"] = state.LeaderID
				leader["scanner_status"] = state.ScannerStatus
				leader["acquired_at"] = state.AcquiredAt
				leader["last_heartbeat_at"] = state.LastHeartbeatAt
				leader["last_heartbeat_ago_sec"] = lastBeatAgoSec
				leader["is_stale"] = lastBeatAgoSec > config.TieringLeaderStaleSec
				leader["stale_sec"] = config.TieringLeaderStaleSec
			}
		}

		c.JSON(http.StatusOK, gin.H{
			"metadata_lookup": metadataLookupSnapshot(),
			"node_discovery": gin.H{
				"configured_source": metadataSourceLabel(),
				"active_source":     deps.nodeDiscoveryActive,
				"active_node_count": deps.getActiveNodeCount(),
			},
			"tiering_leader": leader,
			"timestamp_unix": time.Now().Unix(),
		})
	})
}

func registerAdminMetadataRoutes(router gin.IRoutes, metaStore meta.Repository) {
	router.GET("/v2/admin/leader", func(c *gin.Context) {
		if !config.MetaEnabled || metaStore == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "metadata store unavailable"})
			return
		}

		leaderCtx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
		state, err := metaStore.GetTieringLeaderState(leaderCtx, config.TieringPolicyLeaderLockKey)
		cancel()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		if state == nil {
			c.JSON(http.StatusOK, gin.H{
				"lock_key": config.TieringPolicyLeaderLockKey,
				"leader":   nil,
			})
			return
		}

		lastBeatAgoSec := int(time.Since(state.LastHeartbeatAt).Seconds())
		c.JSON(http.StatusOK, gin.H{
			"lock_key": config.TieringPolicyLeaderLockKey,
			"leader": gin.H{
				"leader_id":              state.LeaderID,
				"scanner_status":         state.ScannerStatus,
				"acquired_at":            state.AcquiredAt,
				"last_heartbeat_at":      state.LastHeartbeatAt,
				"last_heartbeat_ago_sec": lastBeatAgoSec,
				"is_stale":               lastBeatAgoSec > config.TieringLeaderStaleSec,
				"stale_sec":              config.TieringLeaderStaleSec,
			},
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
			"source":    metadataSourceLabel(),
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
				"content_type":    nullStringOrNil(view.Version.ContentType),
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
}

func nullInt64OrNil(v sql.NullInt64) interface{} {
	if !v.Valid {
		return nil
	}
	return v.Int64
}

func nullStringOrNil(v sql.NullString) interface{} {
	if !v.Valid {
		return nil
	}
	return v.String
}

func metadataSourceLabel() string {
	if strings.TrimSpace(config.MetaEndpoint) != "" {
		return "meta_service"
	}
	backend := strings.TrimSpace(config.MetaBackend)
	if backend == "" {
		return "postgres"
	}
	return backend
}
