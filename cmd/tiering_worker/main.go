package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"hybrid_distributed_store/internal/config"
	"hybrid_distributed_store/internal/ec"
	"hybrid_distributed_store/internal/httpclient"
	"hybrid_distributed_store/internal/meta"
	"hybrid_distributed_store/internal/tiering"
)

func envDurationSec(key string, fallback time.Duration) time.Duration {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v <= 0 {
		return fallback
	}
	return time.Duration(v) * time.Second
}

func main() {
	log.Println("[TieringWorker] Starting...")

	store, err := meta.NewStore(meta.Config{
		Enabled:         config.MetaEnabled,
		Driver:          config.MetaDriver,
		DSN:             config.MetaDSN,
		MaxOpenConns:    config.MetaMaxOpenConns,
		MaxIdleConns:    config.MetaMaxIdleConns,
		ConnMaxLifetime: config.MetaConnMaxLifetime,
	})
	if err != nil {
		log.Fatalf("[TieringWorker] metadata store init failed: %v", err)
	}
	defer store.Close()

	pingCtx, pingCancel := context.WithTimeout(context.Background(), 3*time.Second)
	if err := store.Ping(pingCtx); err != nil {
		pingCancel()
		log.Fatalf("[TieringWorker] metadata ping failed: %v", err)
	}
	pingCancel()

	pollInterval := envDurationSec("TIERING_WORKER_POLL_SEC", 2*time.Second)
	taskType := os.Getenv("TIERING_WORKER_TASK_TYPE")
	if taskType == "" {
		taskType = tiering.TaskTypeReplicationToEC
	}

	processor := tiering.NewReplicationToECProcessor(store, httpclient.GetClient(), ec.NewService())
	worker := tiering.NewWorker(store, processor, pollInterval, taskType)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := worker.Run(ctx); err != nil {
		log.Fatalf("[TieringWorker] run failed: %v", err)
	}
	log.Println("[TieringWorker] Stopped.")
}
