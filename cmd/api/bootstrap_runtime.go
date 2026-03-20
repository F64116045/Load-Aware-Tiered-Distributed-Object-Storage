package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/gin-gonic/gin"

	"hybrid_distributed_store/internal/config"
	"hybrid_distributed_store/internal/ec"
	"hybrid_distributed_store/internal/httpclient"
	"hybrid_distributed_store/internal/interfaces"
	"hybrid_distributed_store/internal/meta"
	"hybrid_distributed_store/internal/readservice"
	"hybrid_distributed_store/internal/storageops"
	"hybrid_distributed_store/internal/utils"
	"hybrid_distributed_store/internal/writeservice"
)

type appRuntime struct {
	metaStore *meta.Store

	utilsSvc      interfaces.IUtilsSvc
	readSvc       interfaces.IReadService
	storageOpsSvc interfaces.IStorageOps
	writeSvc      *writeservice.Service

	metadataStatus      string
	metadataErr         string
	nodeDiscoveryActive string
}

func initAppRuntime() (*appRuntime, func()) {
	rt := &appRuntime{
		metadataStatus:      "disabled",
		nodeDiscoveryActive: "postgres",
	}
	httpClient := httpclient.GetClient()

	metaStore, err := meta.NewStore(meta.Config{
		Enabled:         config.MetaEnabled,
		Driver:          config.MetaDriver,
		DSN:             config.MetaDSN,
		MaxOpenConns:    config.MetaMaxOpenConns,
		MaxIdleConns:    config.MetaMaxIdleConns,
		ConnMaxLifetime: config.MetaConnMaxLifetime,
	})
	if err != nil {
		rt.metadataStatus = "down"
		rt.metadataErr = err.Error()
		log.Printf("[API] Metadata init failed: %v", err)
	} else if config.MetaEnabled {
		pingCtx, pingCancel := context.WithTimeout(context.Background(), 3*time.Second)
		pingErr := metaStore.Ping(pingCtx)
		pingCancel()
		if pingErr != nil {
			rt.metadataStatus = "down"
			rt.metadataErr = pingErr.Error()
			log.Printf("[API] Metadata ping failed: %v", pingErr)
		} else {
			rt.metadataStatus = "up"
			if config.MetaAutoMigrate {
				migrateCtx, migrateCancel := context.WithTimeout(context.Background(), 30*time.Second)
				migrateErr := meta.NewMigrator(metaStore).Up(migrateCtx)
				migrateCancel()
				if migrateErr != nil {
					rt.metadataStatus = "down"
					rt.metadataErr = fmt.Sprintf("auto_migrate_failed: %v", migrateErr)
					log.Printf("[API] Metadata auto migration failed: %v", migrateErr)
				} else {
					log.Printf("[API] Metadata auto migration completed.")
				}
			}
		}
	}
	rt.metaStore = metaStore

	rt.utilsSvc = utils.NewService()
	ecDriver := ec.NewService()
	rt.storageOpsSvc = storageops.NewService(httpClient)
	rt.readSvc = readservice.NewService(httpClient, ecDriver, rt.utilsSvc)
	rt.writeSvc = writeservice.NewService(httpClient, ecDriver, rt.utilsSvc, rt.metaStore)

	cleanup := func() {
		if rt.metaStore != nil {
			_ = rt.metaStore.Close()
		}
	}
	return rt, cleanup
}

func startNodeDiscovery(ctx context.Context, rt *appRuntime) {
	if rt == nil {
		return
	}
	if config.MetaEnabled && rt.metaStore != nil {
		go watchNodesFromPostgres(ctx, rt.metaStore)
		return
	}
	rt.nodeDiscoveryActive = "postgres_unavailable"
	log.Printf("%s[API] metadata unavailable; node discovery not started%s\n", config.Colors["YELLOW"], config.Colors["RESET"])
}

func buildRouter(rt *appRuntime) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())
	router.Use(PanicRecoveryMiddleware)

	registerV2ObjectRoutes(router, v2ObjectRouteDeps{
		getDynamicNodes: getDynamicNodes,
		writeReplicationWithMetadata: func(ctx context.Context, replicaNodes []string, key string, data []byte, metadata map[string]interface{}) (map[string]interface{}, error) {
			return rt.writeSvc.WriteReplicationWithMetadata(ctx, replicaNodes, key, data, metadata)
		},
		loadMetadata: func(ctx context.Context, key string) (map[string]interface{}, string, error) {
			return loadMetadata(ctx, key, rt.metaStore)
		},
		readReplication: rt.readSvc.ReadReplication,
		readEC:          rt.readSvc.ReadEC,
		now:             time.Now,
	})

	registerLegacyRoutes(router, legacyRouteDeps{
		getDynamicNodes: getDynamicNodes,
		loadMetadata: func(ctx context.Context, key string) (map[string]interface{}, string, error) {
			return loadMetadata(ctx, key, rt.metaStore)
		},
		writeReplication:  rt.writeSvc.WriteReplication,
		writeEC:           rt.writeSvc.WriteEC,
		serialize:         rt.utilsSvc.Serialize,
		deserialize:       rt.utilsSvc.Deserialize,
		readReplication:   rt.readSvc.ReadReplication,
		readEC:            rt.readSvc.ReadEC,
		deleteReplication: rt.storageOpsSvc.DeleteReplication,
		deleteEC:          rt.storageOpsSvc.DeleteEC,
		deleteNormalizedMetadata: func(ctx context.Context, key string) error {
			if !config.MetaEnabled || rt.metaStore == nil {
				return nil
			}
			return rt.metaStore.DeleteNormalizedMetadata(ctx, key)
		},
		getActiveNodes: getActiveNodeURLs,
	})

	registerAdminObservabilityRoutes(router, adminObservabilityRouteDeps{
		metadataStatus:       rt.metadataStatus,
		metadataErr:          rt.metadataErr,
		nodeDiscoveryActive:  rt.nodeDiscoveryActive,
		getActiveNodeCount:   getActiveNodeCount,
		metaStore:            rt.metaStore,
		tieringLeaderLockKey: config.TieringPolicyLeaderLockKey,
	})

	registerAdminTaskRoutes(router, adminTaskRouteDeps{
		metadataAvailable: func() bool { return config.MetaEnabled && rt.metaStore != nil },
		listTasks: func(ctx context.Context, state, taskType string, limit int) ([]meta.TieringTask, error) {
			if rt.metaStore == nil {
				return nil, nil
			}
			return rt.metaStore.ListTieringTasks(ctx, state, taskType, limit)
		},
		listStateCounts: func(ctx context.Context, taskType string) (map[string]int64, error) {
			if rt.metaStore == nil {
				return map[string]int64{}, nil
			}
			return rt.metaStore.ListTieringTaskStateCounts(ctx, taskType)
		},
		requeueNow: func(ctx context.Context, taskID string) (bool, error) {
			if rt.metaStore == nil {
				return false, nil
			}
			return rt.metaStore.RequeueTieringTaskNow(ctx, taskID)
		},
		cancelTask: func(ctx context.Context, taskID, reason string) (bool, error) {
			if rt.metaStore == nil {
				return false, nil
			}
			return rt.metaStore.CancelTieringTask(ctx, taskID, reason)
		},
	})

	registerAdminMetadataRoutes(router, rt.metaStore)
	return router
}
