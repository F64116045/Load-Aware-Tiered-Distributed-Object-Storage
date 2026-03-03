package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/gin-gonic/gin"
	etcd "go.etcd.io/etcd/client/v3"

	"hybrid_distributed_store/internal/config"
	"hybrid_distributed_store/internal/ec"
	etcdclient "hybrid_distributed_store/internal/etcd"
	"hybrid_distributed_store/internal/httpclient"
	"hybrid_distributed_store/internal/interfaces"
	"hybrid_distributed_store/internal/meta"
	"hybrid_distributed_store/internal/mq"
	"hybrid_distributed_store/internal/readservice"
	"hybrid_distributed_store/internal/storageops"
	"hybrid_distributed_store/internal/utils"
	"hybrid_distributed_store/internal/writeservice"
)

type appRuntime struct {
	etcdClient *etcd.Client
	metaStore  *meta.Store
	mqClient   *mq.Client

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
		nodeDiscoveryActive: config.NodeDiscoverySource,
	}

	requiresEtcd := !(config.MetaSource == "postgres" && config.NodeDiscoverySource == "postgres")
	if requiresEtcd {
		rt.etcdClient = etcdclient.GetClient()
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

	if config.WALEnabled {
		rt.mqClient = mq.NewClient(false)
	} else {
		log.Printf("[API] WAL disabled (WAL_ENABLED=false). Skipping Redpanda client init.")
	}

	rt.utilsSvc = utils.NewService()
	ecDriver := ec.NewService()
	rt.storageOpsSvc = storageops.NewService(httpClient)
	rt.readSvc = readservice.NewService(httpClient, ecDriver, rt.utilsSvc)
	rt.writeSvc = writeservice.NewService(rt.etcdClient, rt.mqClient, httpClient, ecDriver, rt.utilsSvc, rt.metaStore)

	cleanup := func() {
		if rt.mqClient != nil {
			rt.mqClient.Close()
		}
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
	switch config.NodeDiscoverySource {
	case "postgres":
		if config.MetaEnabled && rt.metaStore != nil {
			go watchNodesFromPostgres(ctx, rt.metaStore)
		} else {
			rt.nodeDiscoveryActive = "etcd_fallback"
			log.Printf("%s[API] NODE_DISCOVERY_SOURCE=postgres but metadata is unavailable, fallback to etcd%s\n", config.Colors["YELLOW"], config.Colors["RESET"])
			if rt.etcdClient != nil {
				go watchNodesTask(ctx, rt.etcdClient)
			}
		}
	case "etcd":
		if rt.etcdClient != nil {
			go watchNodesTask(ctx, rt.etcdClient)
		}
	default: // auto
		if config.MetaEnabled && rt.metaStore != nil {
			rt.nodeDiscoveryActive = "postgres"
			go watchNodesFromPostgres(ctx, rt.metaStore)
		} else {
			rt.nodeDiscoveryActive = "etcd"
			if rt.etcdClient != nil {
				go watchNodesTask(ctx, rt.etcdClient)
			}
		}
	}
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
			return loadMetadata(ctx, key, rt.etcdClient, rt.metaStore, config.MetaSource)
		},
		readReplication: rt.readSvc.ReadReplication,
		readEC:          rt.readSvc.ReadEC,
		now:             time.Now,
	})

	registerLegacyRoutes(router, legacyRouteDeps{
		getDynamicNodes: getDynamicNodes,
		loadMetadata: func(ctx context.Context, key string) (map[string]interface{}, string, error) {
			return loadMetadata(ctx, key, rt.etcdClient, rt.metaStore, config.MetaSource)
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
		deleteEtcdMetadata: func(ctx context.Context, key string) error {
			if config.MetaSource == "postgres" || rt.etcdClient == nil {
				return nil
			}
			_, err := rt.etcdClient.Delete(ctx, fmt.Sprintf("metadata/%s", key))
			return err
		},
		getActiveNodes: getActiveNodeURLs,
	})

	registerAdminObservabilityRoutes(router, adminObservabilityRouteDeps{
		metadataStatus:      rt.metadataStatus,
		metadataErr:         rt.metadataErr,
		nodeDiscoveryActive: rt.nodeDiscoveryActive,
		getActiveNodeCount:  getActiveNodeCount,
		metaStore:           rt.metaStore,
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
