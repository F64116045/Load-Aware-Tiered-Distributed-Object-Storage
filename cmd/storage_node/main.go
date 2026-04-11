package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/gin-gonic/gin"

	"hybrid_distributed_store/internal/config"
	"hybrid_distributed_store/internal/meta"
)

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
	registerRoutes(router, storage)

	// 7. Start Server
	listenAddr := "0.0.0.0:" + nodePort
	log.Printf("[%s] Gin Server starting on %s", nodeName, listenAddr)
	if err := router.Run(listenAddr); err != nil {
		log.Fatalf("[%s] Critical Error: Gin failed to start: %v", nodeName, err)
	}
}
