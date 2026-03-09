package writeservice

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"

	"hybrid_distributed_store/internal/config"
	"hybrid_distributed_store/internal/interfaces"
	"hybrid_distributed_store/internal/meta"
	"hybrid_distributed_store/internal/mq"
)

// Service implements write logic for active strategies (replication/ec),
// with optional WAL and metadata persistence.
type Service struct {
	etcd       interfaces.IEtcdClient
	mq         *mq.Client
	walProduce func(ctx context.Context, key string, value []byte) error
	http       interfaces.IHttpClient
	ec         interfaces.IEcDriver
	utils      interfaces.IUtilsSvc
	meta       *meta.Store
}

// NewService creates a new WriteService.
func NewService(
	etcd interfaces.IEtcdClient,
	mqClient *mq.Client,
	http interfaces.IHttpClient,
	ec interfaces.IEcDriver,
	utils interfaces.IUtilsSvc,
	metaStore *meta.Store,
) *Service {
	return &Service{
		etcd: etcd,
		mq:   mqClient,
		walProduce: func(ctx context.Context, key string, value []byte) error {
			if mqClient == nil {
				return fmt.Errorf("wal client is nil")
			}
			return mqClient.ProduceSync(ctx, key, value)
		},
		http:  http,
		ec:    ec,
		utils: utils,
		meta:  metaStore,
	}
}

func computeSHA256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

var errMetadataNotFound = errors.New("metadata not found")

func (s *Service) loadExistingMetadata(ctx context.Context, key string) (map[string]interface{}, string, error) {
	metaKey := fmt.Sprintf("metadata/%s", key)

	readFromPostgres := config.MetaSource == "auto" || config.MetaSource == "postgres"
	readFromEtcd := config.MetaSource == "auto" || config.MetaSource == "etcd"

	if readFromPostgres && config.MetaEnabled && s.meta != nil {
		pgNormalizedMeta, err := s.meta.GetNormalizedMetadata(ctx, key)
		if err != nil {
			return nil, "", err
		}
		if len(pgNormalizedMeta) > 0 {
			return pgNormalizedMeta, "postgres_normalized", nil
		}
		if config.MetaSource == "postgres" {
			return nil, "", errMetadataNotFound
		}
	}

	if !readFromEtcd {
		return nil, "", errMetadataNotFound
	}
	if s.etcd == nil {
		if config.MetaSource == "etcd" {
			return nil, "", fmt.Errorf("etcd client unavailable")
		}
		return nil, "", errMetadataNotFound
	}

	resp, err := s.etcd.Get(ctx, metaKey)
	if err != nil {
		return nil, "", fmt.Errorf("etcd query failed: %v", err)
	}
	if len(resp.Kvs) == 0 {
		return nil, "", errMetadataNotFound
	}

	var metadata map[string]interface{}
	if err := json.Unmarshal(resp.Kvs[0].Value, &metadata); err != nil {
		return nil, "", fmt.Errorf("metadata parse failed: %v", err)
	}
	return metadata, "etcd", nil
}

// --- Write-Ahead Log Helpers ---

func (s *Service) createWALEntry(
	ctx context.Context,
	key string,
	strategy config.StorageStrategy,
	metadataDetails map[string]interface{},
) (string, error) {
	if !config.WALEnabled {
		// WAL is intentionally disabled in postgres-first profile.
		return "", nil
	}

	txnID := fmt.Sprintf("txn:%s", uuid.New().String())

	logEntry := map[string]interface{}{
		"txn_id":    txnID,
		"status":    "PENDING",
		"key_name":  key,
		"strategy":  string(strategy),
		"timestamp": time.Now().Unix(),
		"details":   metadataDetails,
	}

	valBytes, err := json.Marshal(logEntry)
	if err != nil {
		return "", fmt.Errorf("failed to serialize WAL entry: %v", err)
	}

	if s.walProduce == nil {
		return "", fmt.Errorf("wal producer not configured")
	}
	if err := s.walProduce(ctx, key, valBytes); err != nil {
		return "", fmt.Errorf("failed to write WAL to Redpanda: %v", err)
	}

	return txnID, nil
}

func (s *Service) finalizeWALEntry(
	ctx context.Context,
	txnID string,
	success bool,
	mainKey string,
	metadata map[string]interface{},
) error {

	if !success {
		return nil
	}

	metaKey := fmt.Sprintf("metadata/%s", mainKey)
	metaBytes, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to serialize final metadata: %v", err)
	}

	if config.MetaEnabled && s.meta != nil {
		if err := s.meta.UpsertNormalizedMetadata(ctx, mainKey, metadata); err != nil {
			return fmt.Errorf("failed to commit normalized metadata to Postgres: %v", err)
		}
		if err := s.enqueueTieringTaskIfEligible(ctx, mainKey, metadata); err != nil {
			// Tiering is best effort for now; foreground write must remain available.
			log.Printf("[TieringEnqueue] skip key=%s: %v", mainKey, err)
		}
	}

	if config.MetaSource != "postgres" {
		if s.etcd == nil {
			return fmt.Errorf("etcd client unavailable for metadata compatibility write")
		}
		_, err = s.etcd.Put(ctx, metaKey, string(metaBytes))
		if err != nil {
			return fmt.Errorf("failed to commit metadata to Etcd: %v", err)
		}
	}

	return nil
}

func (s *Service) enqueueTieringTaskIfEligible(ctx context.Context, objectID string, metadata map[string]interface{}) error {
	if s == nil || s.meta == nil {
		return nil
	}

	strategy, _ := metadata["strategy"].(string)
	if strategy != string(config.StrategyReplication) {
		return nil
	}

	hotVersion := toInt64(metadata["hot_version"], 0)
	if hotVersion <= 0 {
		return fmt.Errorf("invalid hot_version for object %s", objectID)
	}

	priority := 100
	scheduledAt := time.Now().Add(time.Duration(config.AgeThresholdSec) * time.Second)
	taskID := fmt.Sprintf("repl2ec:%s:%d", objectID, hotVersion)

	if err := s.meta.EnqueueTieringTask(
		ctx,
		taskID,
		objectID,
		hotVersion,
		"REPL_TO_EC",
		priority,
		scheduledAt,
	); err != nil {
		return err
	}

	return nil
}

func toInt64(v interface{}, fallback int64) int64 {
	switch t := v.(type) {
	case int:
		return int64(t)
	case int32:
		return int64(t)
	case int64:
		return t
	case float32:
		return int64(t)
	case float64:
		return int64(t)
	default:
		return fallback
	}
}

// --- Strategy A: Replication ---

func (s *Service) WriteReplication(
	ctx context.Context,
	replicaNodes []string,
	key string,
	value []byte,
) (map[string]interface{}, error) {
	return s.writeReplication(ctx, replicaNodes, key, value, nil)
}

// WriteReplicationWithMetadata writes replicated bytes and persists additional metadata fields.
func (s *Service) WriteReplicationWithMetadata(
	ctx context.Context,
	replicaNodes []string,
	key string,
	value []byte,
	extraMeta map[string]interface{},
) (map[string]interface{}, error) {
	return s.writeReplication(ctx, replicaNodes, key, value, extraMeta)
}

func (s *Service) writeReplication(
	ctx context.Context,
	replicaNodes []string,
	key string,
	value []byte,
	extraMeta map[string]interface{},
) (map[string]interface{}, error) {
	walMeta := map[string]interface{}{
		"strategy": config.StrategyReplication,
	}

	txnID, err := s.createWALEntry(ctx, key, config.StrategyReplication, walMeta)
	if err != nil {
		return nil, err
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	success := 0
	// 用來記錄哪些節點寫入成功，方便 Healer 除錯或 API 回傳
	writtenNodes := []string{}

	for _, url := range replicaNodes {
		wg.Add(1)
		go func(n string) {
			defer wg.Done()
			req, _ := http.NewRequestWithContext(ctx, "POST",
				fmt.Sprintf("%s/store?key=%s", n, key),
				bytes.NewReader(value),
			)
			req.Header.Set("Content-Type", "application/octet-stream")

			resp, err := s.http.Do(req)
			if err == nil && resp.StatusCode == http.StatusOK {
				mu.Lock()
				success++
				writtenNodes = append(writtenNodes, n)
				mu.Unlock()
			} else {
				log.Printf("[WriteReplication] Failed to write to %s: %v", n, err)
			}
		}(url)
	}
	wg.Wait()

	// [CHANGE] Best Effort: 只要有 1 個寫入成功，我們就 Commit。
	// 剩下的交給 Healer 去修復。
	if success == 0 {
		return nil, fmt.Errorf("replication failed entirely: 0/%d nodes responded", len(replicaNodes))
	}

	finalMeta := map[string]interface{}{
		"strategy":        string(config.StrategyReplication),
		"hot_version":     time.Now().UnixNano(),
		"cold_version":    0,
		"cold_hash":       "",
		"original_length": len(value),
		"replica_nodes":   writtenNodes,
	}
	for k, v := range extraMeta {
		finalMeta[k] = v
	}

	// [ADDED] Explicitly mark as dirty if partial failure occurred
	isDirty := success < len(replicaNodes)
	if isDirty {
		finalMeta["is_dirty"] = true
		log.Printf("[PartialWrite] Key=%s marked dirty. %d/%d replicas written.", key, success, len(replicaNodes))
	}

	if err := s.finalizeWALEntry(ctx, txnID, true, key, finalMeta); err != nil {
		return nil, err
	}

	return map[string]interface{}{
		"nodes_written": writtenNodes,
		"status":        "committed",
		"partial":       isDirty,
	}, nil
}

// --- Strategy B: Erasure Coding (EC) ---

func (s *Service) WriteEC(
	ctx context.Context,
	ecNodes []string,
	key string,
	value []byte,
) (map[string]interface{}, error) {

	chunkPrefix := fmt.Sprintf("%s_cold_chunk_", key)

	walMeta := map[string]interface{}{
		"strategy":        config.StrategyEC,
		"k":               config.K,
		"m":               config.M,
		"chunk_prefix":    chunkPrefix,
		"original_length": len(value),
		"key_name":        key,
	}

	txnID, err := s.createWALEntry(ctx, key, config.StrategyEC, walMeta)
	if err != nil {
		return nil, err
	}

	chunks, err := s.ec.Split(value)
	if err != nil {
		return nil, fmt.Errorf("EC split failed: %v", err)
	}
	if err := s.ec.Encode(chunks); err != nil {
		return nil, fmt.Errorf("EC encode failed: %v", err)
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	success := 0
	totalChunks := len(chunks)

	for i := range chunks {
		if i >= len(ecNodes) {
			break
		}
		wg.Add(1)
		go func(idx int, c []byte) {
			defer wg.Done()
			url := ecNodes[idx]
			req, _ := http.NewRequestWithContext(ctx, "POST",
				fmt.Sprintf("%s/store?key=%s%d", url, chunkPrefix, idx),
				bytes.NewReader(c),
			)
			req.Header.Set("Content-Type", "application/octet-stream")

			resp, err := s.http.Do(req)
			if err == nil && resp.StatusCode == http.StatusOK {
				mu.Lock()
				success++
				mu.Unlock()
			}
		}(i, chunks[i])
	}
	wg.Wait()

	// [CHANGE] EC Constraint: 必須至少寫入 K 份，資料才是可讀的。
	// 如果少於 K，就算 Commit 了也是壞檔，所以這裡必須報錯。
	if success < config.K {
		return nil, fmt.Errorf("EC write critical failure: %d/%d (Need at least %d to recover)", success, totalChunks, config.K)
	}

	finalMeta := map[string]interface{}{}
	for k, v := range walMeta {
		finalMeta[k] = v
	}
	finalMeta["hot_version"] = 0
	finalMeta["cold_version"] = time.Now().UnixNano()
	finalMeta["cold_hash"] = computeSHA256Hex(value)

	// [ADDED] Mark as dirty if any chunk failed
	isDirty := success < totalChunks
	if isDirty {
		finalMeta["is_dirty"] = true
		log.Printf("[PartialWrite] Key=%s marked dirty. %d/%d chunks written.", key, success, totalChunks)
	}

	if err := s.finalizeWALEntry(ctx, txnID, true, key, finalMeta); err != nil {
		return nil, err
	}

	return map[string]interface{}{
		"chunks_written": success,
		"total_chunks":   totalChunks,
		"partial":        isDirty,
	}, nil
}
