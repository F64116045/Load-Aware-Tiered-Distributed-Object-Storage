package main

import (
	"context"
	"errors"
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

func resolveWorkerID() string {
	if id := os.Getenv("TIERING_WORKER_ID"); id != "" {
		return id
	}
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		return "unknown-tiering-worker"
	}
	return hostname
}

func runScannerAsLeader(
	ctx context.Context,
	store meta.Repository,
	scanner *tiering.PolicyScanner,
	workerID string,
	lockKey int64,
	retryInterval time.Duration,
) error {
	if scanner == nil || store == nil {
		return nil
	}
	if retryInterval <= 0 {
		retryInterval = 2 * time.Second
	}

	ticker := time.NewTicker(retryInterval)
	defer ticker.Stop()

	var lock meta.LeaderLock
	var scannerCancel context.CancelFunc

	stopScanner := func() {
		if scannerCancel != nil {
			scannerCancel()
			scannerCancel = nil
		}
	}
	releaseLock := func() {
		if lock == nil {
			return
		}
		markCtx, markCancel := context.WithTimeout(context.Background(), 2*time.Second)
		if err := store.MarkTieringLeaderStopped(markCtx, lockKey, workerID, "STOPPED"); err != nil {
			log.Printf("[TieringPolicy] mark leader stopped failed: %v", err)
		}
		markCancel()

		releaseCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := lock.Release(releaseCtx); err != nil {
			log.Printf("[TieringPolicy] advisory lock release failed: %v", err)
		}
		lock = nil
	}

	tryBecomeLeader := func() {
		acquireCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()

		newLock, acquired, err := store.TryAcquireLeaderLock(acquireCtx, lockKey)
		if err != nil {
			log.Printf("[TieringPolicy] advisory lock acquire failed: %v", err)
			return
		}
		if !acquired {
			return
		}

		lock = newLock
		upsertCtx, upsertCancel := context.WithTimeout(ctx, 2*time.Second)
		if err := store.UpsertTieringLeaderState(upsertCtx, lockKey, workerID, "LEADING"); err != nil {
			log.Printf("[TieringPolicy] leader state upsert failed: %v", err)
		}
		upsertCancel()

		scannerCtx, cancelScanner := context.WithCancel(ctx)
		scannerCancel = cancelScanner
		log.Printf("[TieringPolicy] became leader (worker_id=%s lock_key=%d); scanner started", workerID, lockKey)
		go func() {
			if err := scanner.Run(scannerCtx); err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("[TieringPolicy] scanner exited with error: %v", err)
			}
		}()
	}

	tryBecomeLeader()
	for {
		select {
		case <-ctx.Done():
			stopScanner()
			releaseLock()
			return nil
		case <-ticker.C:
			if lock == nil {
				tryBecomeLeader()
				continue
			}
			pingCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
			pingErr := lock.Ping(pingCtx)
			cancel()
			if pingErr == nil {
				beatCtx, beatCancel := context.WithTimeout(ctx, 2*time.Second)
				if err := store.UpsertTieringLeaderState(beatCtx, lockKey, workerID, "LEADING"); err != nil {
					log.Printf("[TieringPolicy] leader heartbeat upsert failed: %v", err)
				}
				beatCancel()
				continue
			}

			log.Printf("[TieringPolicy] leader lock session lost; stop scanner and retry leadership: %v", pingErr)
			stopScanner()
			markCtx, markCancel := context.WithTimeout(context.Background(), 2*time.Second)
			if err := store.MarkTieringLeaderStopped(markCtx, lockKey, workerID, "LOCK_LOST"); err != nil {
				log.Printf("[TieringPolicy] mark leader lock-lost failed: %v", err)
			}
			markCancel()
			releaseLock()
		}
	}
}

func main() {
	log.Println("[TieringWorker] Starting...")

	store, err := meta.NewRepository(meta.Config{
		Endpoint:        config.MetaEndpoint,
		RequireEndpoint: config.MetaRequireEndpoint,
		AuthToken:       config.MetaRPCAuthToken,
		Enabled:         config.MetaEnabled,
		DSN:             config.MetaDSN,
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
	policyPeriod := envDurationSec("TIERING_POLICY_PERIOD_SEC", time.Duration(config.TieringPeriodSec)*time.Second)
	policyLeaderRetry := envDurationSec("TIERING_POLICY_LEADER_RETRY_SEC", 2*time.Second)
	policyLeaderLockKey := config.TieringPolicyLeaderLockKey
	workerID := resolveWorkerID()
	taskType := os.Getenv("TIERING_WORKER_TASK_TYPE")
	if taskType == "" || taskType == "ALL" {
		taskType = ""
	}

	replToECProcessor := tiering.NewReplicationToECProcessor(store, httpclient.GetClient(), ec.NewService())
	replRepairProcessor := tiering.NewReplicationRepairProcessor(store, httpclient.GetClient(), ec.NewService())
	replGCProcessor := tiering.NewReplicationGCProcessor(store, httpclient.GetClient())
	oldVersionGCProcessor := tiering.NewOldVersionGCProcessor(store, httpclient.GetClient())
	processor := tiering.NewProcessorMux(replToECProcessor, replRepairProcessor, replGCProcessor, oldVersionGCProcessor)
	worker := tiering.NewWorker(store, processor, pollInterval, taskType)
	scanner := tiering.NewPolicyScanner(
		store,
		tiering.PolicyScannerConfig{
			PeriodicInterval:          policyPeriod,
			ThresholdCheckInterval:    time.Duration(config.TieringThresholdCheckSec) * time.Second,
			ThresholdCooldown:         time.Duration(config.TieringThresholdCooldownSec) * time.Second,
			TriggerMode:               config.TieringTriggerModeSetting,
			PolicyVariant:             config.TieringPolicyVariantSetting,
			AgeThresholdSec:           config.AgeThresholdSec,
			MaxObjects:                config.MaxObjectsPerRound,
			MaxBytes:                  config.MaxBytesPerRound,
			IdleStableRounds:          config.TieringIdleStableRounds,
			IdleCPUPercent:            config.TieringIdleCPUPct,
			IdleMemoryPercent:         config.TieringIdleMemoryPct,
			IdleIOWaitPercent:         config.TieringIdleIOWaitPct,
			IdleQueueDepth:            config.TieringIdleQueueDepth,
			HotPressureDiskPct:        config.HotPressureDiskPct,
			HotPressureQueueDepth:     config.HotPressureQueueDepth,
			HeartbeatStaleSec:         config.NodeHeartbeatStaleSec,
			RepairEnabled:             config.RepairScanEnabled,
			RepairMaxObjects:          config.RepairMaxObjectsPerRound,
			OldVersionReaperEnabled:   config.OldVersionReaperEnabled,
			OldVersionRetentionN:      config.OldVersionRetentionCount,
			OldVersionRetentionAge:    config.OldVersionRetentionAgeSec,
			OldVersionMaxTasks:        config.OldVersionReaperMaxTasksPerRound,
			TaskHistoryReaperEnabled:  config.TieringTaskHistoryReaperEnabled,
			TaskHistoryRetentionSec:   config.TieringTaskHistoryRetentionSec,
			TaskHistoryReaperMaxTasks: config.TieringTaskHistoryReaperMaxTasksPerRound,
			TaskHistoryReaperInterval: time.Duration(config.TieringTaskHistoryReaperIntervalSec) * time.Second,
		},
	)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	go func() {
		if err := runScannerAsLeader(ctx, store, scanner, workerID, policyLeaderLockKey, policyLeaderRetry); err != nil {
			log.Printf("[TieringPolicy] scanner stopped with error: %v", err)
		}
	}()

	if err := worker.Run(ctx); err != nil {
		log.Fatalf("[TieringWorker] run failed: %v", err)
	}
	log.Println("[TieringWorker] Stopped.")
}
