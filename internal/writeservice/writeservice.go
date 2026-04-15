package writeservice

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	neturl "net/url"
	"strconv"
	"sync"
	"time"

	"hybrid_distributed_store/internal/config"
	"hybrid_distributed_store/internal/interfaces"
	"hybrid_distributed_store/internal/meta"
)

// Service implements write logic for active strategies (replication/ec),
// with metadata persistence.
type Service struct {
	http  interfaces.IHttpClient
	ec    interfaces.IEcDriver
	utils interfaces.IUtilsSvc
	meta  meta.Repository
}

// NewService creates a new WriteService.
func NewService(
	http interfaces.IHttpClient,
	ec interfaces.IEcDriver,
	utils interfaces.IUtilsSvc,
	metaStore meta.Repository,
) *Service {
	return &Service{
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

func (s *Service) finalizeMetadata(
	ctx context.Context,
	mainKey string,
	metadata map[string]interface{},
) error {
	if !config.MetaEnabled {
		return nil
	}
	if s.meta == nil {
		return fmt.Errorf("metadata store unavailable")
	}

	if err := s.meta.UpsertNormalizedMetadata(ctx, mainKey, metadata); err != nil {
		return fmt.Errorf("failed to commit normalized metadata: %v", err)
	}
	if !config.TieringEnqueueOnWrite {
		return nil
	}
	if err := s.enqueueTieringTaskIfEligible(ctx, mainKey, metadata); err != nil {
		// Tiering is best effort for now; foreground write must remain available.
		log.Printf("[TieringEnqueue] skip key=%s: %v", mainKey, err)
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

	if isDirty, _ := metadata["is_dirty"].(bool); isDirty {
		repairTaskID := fmt.Sprintf("repair-repl:%s:%d", objectID, hotVersion)
		if err := s.meta.EnqueueTieringTask(
			ctx,
			repairTaskID,
			objectID,
			hotVersion,
			"REPAIR",
			200,
			time.Now(),
		); err != nil {
			return fmt.Errorf("enqueue repair task failed: %w", err)
		}
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
	case json.Number:
		n, err := t.Int64()
		if err != nil {
			return fallback
		}
		return n
	case float32:
		return int64(t)
	case float64:
		return int64(t)
	case string:
		n, err := strconv.ParseInt(t, 10, 64)
		if err != nil {
			return fallback
		}
		return n
	default:
		return fallback
	}
}

func resolveHotWriteQuorum(replicaCount int) int {
	quorum := config.HotWriteQuorum
	if quorum <= 0 {
		quorum = 1
	}
	if replicaCount <= 0 {
		return quorum
	}
	if quorum > replicaCount {
		return replicaCount + 1
	}
	return quorum
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
	if len(replicaNodes) == 0 {
		return nil, fmt.Errorf("replication failed: no replica nodes available")
	}

	version := time.Now().UnixNano()
	hotReplicaPath := meta.BuildHotReplicaPath(key, version)
	if hotReplicaPath == "" {
		hotReplicaPath = key
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	success := 0
	// 用來記錄哪些節點寫入成功，方便 Healer 除錯或 API 回傳
	writtenNodes := []string{}

	for _, nodeURL := range replicaNodes {
		wg.Add(1)
		go func(n string) {
			defer wg.Done()
			req, _ := http.NewRequestWithContext(ctx, "POST",
				fmt.Sprintf("%s/store?key=%s", n, neturl.QueryEscape(hotReplicaPath)),
				bytes.NewReader(value),
			)
			req.Header.Set("Content-Type", "application/octet-stream")

			resp, err := s.http.Do(req)
			if err == nil {
				_ = resp.Body.Close()
			}
			if err == nil && resp.StatusCode == http.StatusOK {
				mu.Lock()
				success++
				writtenNodes = append(writtenNodes, n)
				mu.Unlock()
			} else {
				log.Printf("[WriteReplication] Failed to write to %s: %v", n, err)
			}
		}(nodeURL)
	}
	wg.Wait()

	requiredQuorum := resolveHotWriteQuorum(len(replicaNodes))
	if success < requiredQuorum {
		return nil, fmt.Errorf("hot write quorum not met: %d/%d successful (required %d)", success, len(replicaNodes), requiredQuorum)
	}

	finalMeta := map[string]interface{}{
		"strategy": string(config.StrategyReplication),
		// Keep version fields as decimal strings to avoid int64 precision loss
		// when metadata crosses JSON map[string]interface{} RPC boundaries.
		"hot_version":     strconv.FormatInt(version, 10),
		"cold_version":    "0",
		"cold_hash":       "",
		"original_length": len(value),
		"replica_nodes":   writtenNodes,
		"hot_key":         hotReplicaPath,
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

	if err := s.finalizeMetadata(ctx, key, finalMeta); err != nil {
		return nil, err
	}

	return map[string]interface{}{
		"nodes_written":  writtenNodes,
		"status":         "committed",
		"partial":        isDirty,
		"write_quorum":   fmt.Sprintf("%d/%d", requiredQuorum, len(replicaNodes)),
		"writes_success": success,
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

	writeMeta := map[string]interface{}{
		"strategy":        config.StrategyEC,
		"k":               config.K,
		"m":               config.M,
		"chunk_prefix":    chunkPrefix,
		"original_length": len(value),
		"key_name":        key,
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
			nodeURL := ecNodes[idx]
			chunkKey := fmt.Sprintf("%s%d", chunkPrefix, idx)
			req, _ := http.NewRequestWithContext(ctx, "POST",
				fmt.Sprintf("%s/store?key=%s", nodeURL, neturl.QueryEscape(chunkKey)),
				bytes.NewReader(c),
			)
			req.Header.Set("Content-Type", "application/octet-stream")

			resp, err := s.http.Do(req)
			if err == nil {
				_ = resp.Body.Close()
			}
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
	for k, v := range writeMeta {
		finalMeta[k] = v
	}
	finalMeta["hot_version"] = "0"
	finalMeta["cold_version"] = strconv.FormatInt(time.Now().UnixNano(), 10)
	finalMeta["cold_hash"] = computeSHA256Hex(value)

	// [ADDED] Mark as dirty if any chunk failed
	isDirty := success < totalChunks
	if isDirty {
		finalMeta["is_dirty"] = true
		log.Printf("[PartialWrite] Key=%s marked dirty. %d/%d chunks written.", key, success, totalChunks)
	}

	if err := s.finalizeMetadata(ctx, key, finalMeta); err != nil {
		return nil, err
	}

	return map[string]interface{}{
		"chunks_written": success,
		"total_chunks":   totalChunks,
		"partial":        isDirty,
	}, nil
}
