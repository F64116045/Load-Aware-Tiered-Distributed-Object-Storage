package tiering

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"sync"
	"time"

	"hybrid_distributed_store/internal/config"
	"hybrid_distributed_store/internal/interfaces"
	"hybrid_distributed_store/internal/meta"
)

// ReplicationToECProcessor executes REPL_TO_EC migration for one object version.
type ReplicationToECProcessor struct {
	store meta.Repository
	http  *http.Client
	ec    interfaces.IEcDriver

	throttleBytesPerSec int64
	throttleMu          sync.Mutex
	nextIOAt            time.Time
}

// NewReplicationToECProcessor constructs a processor implementation.
func NewReplicationToECProcessor(store meta.Repository, httpClient *http.Client, ecDriver interfaces.IEcDriver) *ReplicationToECProcessor {
	limitMBps := config.WorkerBWLimitMBPS
	throttleBytesPerSec := int64(0)
	if limitMBps > 0 {
		throttleBytesPerSec = limitMBps * 1024 * 1024
	}

	return &ReplicationToECProcessor{
		store:               store,
		http:                httpClient,
		ec:                  ecDriver,
		throttleBytesPerSec: throttleBytesPerSec,
	}
}

// ProcessReplicationToEC migrates one replicated object version into EC shards.
func (p *ReplicationToECProcessor) ProcessReplicationToEC(ctx context.Context, task *meta.TieringTask) error {
	if task == nil {
		return fmt.Errorf("nil task")
	}
	if p.store == nil || p.http == nil || p.ec == nil {
		return fmt.Errorf("processor dependency is nil")
	}

	snapshot, err := p.store.GetObjectVersionSnapshot(ctx, task.ObjectID, task.Version)
	if err != nil {
		return err
	}
	if snapshot == nil {
		// Treat missing snapshot as task error so it is visible/retriable instead of silently DONE.
		return fmt.Errorf("object snapshot missing: object=%s version=%d task=%s", task.ObjectID, task.Version, task.TaskID)
	}
	if snapshot.CurrentVersion != task.Version {
		// Task targets an old version; safe to skip.
		log.Printf("[TieringWorker] task=%s object=%s stale task version=%d current=%d, skip", task.TaskID, task.ObjectID, task.Version, snapshot.CurrentVersion)
		return nil
	}
	if snapshot.Tier == "EC" && snapshot.State == "EC_ACTIVE" {
		// Already promoted by previous run.
		return nil
	}

	if err := p.store.MarkObjectMigrating(ctx, task.ObjectID, task.Version); err != nil {
		return err
	}

	nodes, err := p.store.ListHealthyNodeIDs(ctx, config.NodeHeartbeatStaleSec)
	if err != nil {
		return err
	}
	if len(nodes) < config.K {
		return fmt.Errorf("insufficient healthy nodes for migration: have=%d need_at_least=%d", len(nodes), config.K)
	}

	replicas, err := p.store.ListActiveReplicaLocations(ctx, task.ObjectID, task.Version)
	if err != nil {
		return err
	}
	sourceBytes, err := p.fetchFromAnyReplica(ctx, replicas, nodes, task.ObjectID, task.Version)
	if err != nil {
		return err
	}
	p.throttle(ctx, int64(len(sourceBytes)))

	shards, err := p.ec.Split(sourceBytes)
	if err != nil {
		return fmt.Errorf("ec split failed: %w", err)
	}
	if err := p.ec.Encode(shards); err != nil {
		return fmt.Errorf("ec encode failed: %w", err)
	}

	success, locations := p.writeShards(ctx, nodes, task.ObjectID, task.Version, shards)
	if success < config.K {
		return fmt.Errorf("ec shard write insufficient: success=%d required=%d", success, config.K)
	}

	checksum := sha256.Sum256(sourceBytes)
	if err := p.store.PromoteObjectVersionToEC(
		ctx,
		task.ObjectID,
		task.Version,
		hex.EncodeToString(checksum[:]),
		config.K,
		config.M,
		locations,
	); err != nil {
		return err
	}
	if err := p.enqueueReplicationGCTask(ctx, task.ObjectID, task.Version); err != nil {
		return err
	}

	return nil
}

func (p *ReplicationToECProcessor) enqueueReplicationGCTask(ctx context.Context, objectID string, version int64) error {
	gcTaskID := fmt.Sprintf("gc-repl:%s:%d", objectID, version)
	return p.store.EnqueueTieringTask(
		ctx,
		gcTaskID,
		objectID,
		version,
		TaskTypeGC,
		90,
		time.Now(),
	)
}

func (p *ReplicationToECProcessor) fetchFromAnyReplica(
	ctx context.Context,
	replicas []meta.ReplicaLocation,
	healthyNodes []string,
	objectID string,
	version int64,
) ([]byte, error) {
	tryFetch := func(nodeID, key string) ([]byte, bool) {
		if nodeID == "" || key == "" {
			return nil, false
		}
		req, err := http.NewRequestWithContext(
			ctx,
			http.MethodGet,
			fmt.Sprintf("%s/retrieve/%s", nodeID, url.PathEscape(key)),
			nil,
		)
		if err != nil {
			return nil, false
		}
		resp, err := p.http.Do(req)
		if err != nil {
			return nil, false
		}
		data, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil || resp.StatusCode != http.StatusOK {
			return nil, false
		}
		return data, true
	}

	for _, r := range replicas {
		key := r.Path
		if key == "" {
			key = meta.BuildHotReplicaPath(objectID, version)
		}
		if key == "" {
			key = objectID
		}
		if data, ok := tryFetch(r.NodeID, key); ok {
			return data, nil
		}
		// Backward compatibility for pre-versioned writes.
		if key != objectID {
			if data, ok := tryFetch(r.NodeID, objectID); ok {
				return data, nil
			}
		}
	}

	// Compatibility fallback when replica rows are missing or stale.
	versionedKey := meta.BuildHotReplicaPath(objectID, version)
	for _, n := range healthyNodes {
		if data, ok := tryFetch(n, versionedKey); ok {
			return data, nil
		}
		if data, ok := tryFetch(n, objectID); ok {
			return data, nil
		}
	}
	return nil, fmt.Errorf("failed to fetch source object from active replicas")
}

func (p *ReplicationToECProcessor) writeShards(
	ctx context.Context,
	nodes []string,
	objectID string,
	version int64,
	shards [][]byte,
) (int, []meta.ECShardLocation) {
	maxWrites := len(shards)
	if len(nodes) < maxWrites {
		maxWrites = len(nodes)
	}

	success := 0
	locations := make([]meta.ECShardLocation, 0, maxWrites)
	for i := 0; i < maxWrites; i++ {
		chunkKey := fmt.Sprintf("%s_cold_chunk_%d", objectID, i)
		node := nodes[i]
		endpoint := fmt.Sprintf("%s/store?key=%s", node, url.QueryEscape(chunkKey))

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(shards[i]))
		if err != nil {
			continue
		}
		p.throttle(ctx, int64(len(shards[i])))
		req.Header.Set("Content-Type", "application/octet-stream")
		resp, err := p.http.Do(req)
		if err != nil {
			continue
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			continue
		}

		success++
		locations = append(locations, meta.ECShardLocation{
			ShardIndex: i,
			NodeID:     node,
			Path:       chunkKey,
			Status:     "ACTIVE",
		})
	}

	log.Printf("[TieringWorker] object=%s version=%d ec_shards_written=%d/%d", objectID, version, success, len(shards))
	return success, locations
}

func (p *ReplicationToECProcessor) throttle(ctx context.Context, bytes int64) {
	if p == nil || p.throttleBytesPerSec <= 0 || bytes <= 0 {
		return
	}
	delay := time.Duration((float64(bytes) / float64(p.throttleBytesPerSec)) * float64(time.Second))
	if delay <= 0 {
		return
	}

	p.throttleMu.Lock()
	now := time.Now()
	waitUntil := p.nextIOAt
	if waitUntil.Before(now) {
		waitUntil = now
	}
	p.nextIOAt = waitUntil.Add(delay)
	p.throttleMu.Unlock()

	wait := time.Until(waitUntil)
	if wait <= 0 {
		return
	}

	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}
