package tiering

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"

	"hybrid_distributed_store/internal/config"
	"hybrid_distributed_store/internal/interfaces"
	"hybrid_distributed_store/internal/meta"
	"hybrid_distributed_store/internal/placement"
)

// ReplicationRepairProcessor heals missing placements for current object version.
// It currently supports:
// 1) HOT replica repair (replication strategy)
// 2) EC shard repair (reconstruct and write missing shards)
type ReplicationRepairProcessor struct {
	store meta.Repository
	http  *http.Client
	ec    interfaces.IEcDriver
}

// NewReplicationRepairProcessor constructs a REPAIR processor implementation.
func NewReplicationRepairProcessor(store meta.Repository, httpClient *http.Client, ecDriver interfaces.IEcDriver) *ReplicationRepairProcessor {
	return &ReplicationRepairProcessor{
		store: store,
		http:  httpClient,
		ec:    ecDriver,
	}
}

// ProcessReplicationRepair restores missing HOT replicas or EC shards.
func (p *ReplicationRepairProcessor) ProcessReplicationRepair(ctx context.Context, task *meta.TieringTask) error {
	if task == nil {
		return fmt.Errorf("nil task")
	}
	if p.store == nil || p.http == nil || p.ec == nil {
		return fmt.Errorf("repair processor dependency is nil")
	}

	snapshot, err := p.store.GetObjectVersionSnapshot(ctx, task.ObjectID, task.Version)
	if err != nil {
		return err
	}
	if snapshot == nil {
		return nil
	}
	if snapshot.CurrentVersion != task.Version {
		log.Printf("[TieringWorker][REPAIR] task=%s object=%s stale version=%d current=%d skip", task.TaskID, task.ObjectID, task.Version, snapshot.CurrentVersion)
		return nil
	}
	switch snapshot.Tier {
	case "HOT":
		return p.processHOTRepair(ctx, task)
	case "EC":
		return p.processECRepair(ctx, task)
	default:
		// Unknown tier: skip to keep REPAIR idempotent/safe.
		return nil
	}
}

func (p *ReplicationRepairProcessor) processHOTRepair(ctx context.Context, task *meta.TieringTask) error {
	replicas, err := p.store.ListActiveReplicaLocations(ctx, task.ObjectID, task.Version)
	if err != nil {
		return err
	}
	if len(replicas) == 0 {
		return fmt.Errorf("no active replicas available for repair source")
	}

	targetReplicaCount := config.HotReplicaCount
	if targetReplicaCount <= 0 {
		targetReplicaCount = 1
	}
	if len(replicas) >= targetReplicaCount {
		return nil
	}

	healthyNodes, err := p.store.ListHealthyNodeIDs(ctx, config.NodeHeartbeatStaleSec)
	if err != nil {
		return err
	}
	if len(healthyNodes) == 0 {
		return fmt.Errorf("no healthy nodes available for repair targets")
	}

	activeSet := make(map[string]struct{}, len(replicas))
	for _, r := range replicas {
		activeSet[r.NodeID] = struct{}{}
	}

	need := targetReplicaCount - len(replicas)
	targetNodes := p.pickRepairTargetNodes(placement.HotRepairKey(task.ObjectID, task.Version), healthyNodes, activeSet, need)
	if len(targetNodes) == 0 {
		return fmt.Errorf("no candidate nodes to place repaired replicas")
	}

	payload, err := p.fetchFromActiveReplicas(ctx, task.ObjectID, task.Version, replicas)
	if err != nil {
		return err
	}

	repairedNodeIDs := p.writeReplicas(ctx, task.ObjectID, task.Version, payload, targetNodes)
	if len(repairedNodeIDs) == 0 {
		return fmt.Errorf("repair write failed for all candidate nodes")
	}

	if err := p.store.UpsertReplicaLocations(ctx, task.ObjectID, task.Version, repairedNodeIDs); err != nil {
		return err
	}

	totalActive := len(replicas) + len(repairedNodeIDs)
	log.Printf(
		"[TieringWorker][REPAIR] object=%s version=%d repaired=%d active=%d/%d",
		task.ObjectID,
		task.Version,
		len(repairedNodeIDs),
		totalActive,
		targetReplicaCount,
	)
	if totalActive < targetReplicaCount {
		return fmt.Errorf("repair partial: active=%d target=%d", totalActive, targetReplicaCount)
	}
	return nil
}

func (p *ReplicationRepairProcessor) processECRepair(ctx context.Context, task *meta.TieringTask) error {
	view, err := p.store.GetObjectAdminView(ctx, task.ObjectID)
	if err != nil {
		return err
	}
	if view == nil || view.Version == nil {
		return nil
	}
	if view.CurrentVersion != task.Version || view.Version.Version != task.Version {
		return nil
	}
	if view.Version.Tier != "EC" {
		return nil
	}

	k := config.K
	m := config.M
	if view.Version.EncodingK != nil && *view.Version.EncodingK > 0 {
		k = *view.Version.EncodingK
	}
	if view.Version.EncodingM != nil && *view.Version.EncodingM > 0 {
		m = *view.Version.EncodingM
	}
	totalShards := k + m
	if totalShards <= 0 {
		return fmt.Errorf("invalid ec parameters k=%d m=%d", k, m)
	}

	activeByIndex := make(map[int]meta.ECShardLocation, totalShards)
	usedNodeSet := make(map[string]struct{}, totalShards)
	for _, shard := range view.ECShardLocations {
		if shard.Status != "ACTIVE" {
			continue
		}
		if shard.ShardIndex < 0 || shard.ShardIndex >= totalShards {
			continue
		}
		if shard.NodeID == "" || shard.Path == "" {
			continue
		}
		activeByIndex[shard.ShardIndex] = meta.ECShardLocation{
			ShardIndex: shard.ShardIndex,
			NodeID:     shard.NodeID,
			Path:       shard.Path,
			Status:     shard.Status,
		}
		usedNodeSet[shard.NodeID] = struct{}{}
	}

	missingIndices := make([]int, 0)
	for i := 0; i < totalShards; i++ {
		if _, ok := activeByIndex[i]; !ok {
			missingIndices = append(missingIndices, i)
		}
	}
	if len(missingIndices) == 0 {
		return nil
	}

	shards := make([][]byte, totalShards)
	for idx, loc := range activeByIndex {
		payload, fetchErr := p.fetchSingleBlob(ctx, loc.NodeID, loc.Path)
		if fetchErr != nil {
			continue
		}
		shards[idx] = payload
	}

	available := 0
	for i := 0; i < totalShards; i++ {
		if len(shards[i]) > 0 {
			available++
		}
	}
	if available < k {
		return fmt.Errorf("insufficient shards to reconstruct: have=%d need=%d", available, k)
	}
	if err := p.ec.Reconstruct(shards); err != nil {
		return fmt.Errorf("ec reconstruct failed: %w", err)
	}

	healthyNodes, err := p.store.ListHealthyNodeIDs(ctx, config.NodeHeartbeatStaleSec)
	if err != nil {
		return err
	}
	if len(healthyNodes) == 0 {
		return fmt.Errorf("no healthy nodes available for ec repair")
	}
	targetNodes := p.pickRepairTargetNodes(placement.ECRepairKey(task.ObjectID, task.Version), healthyNodes, usedNodeSet, len(missingIndices))
	if len(targetNodes) < len(missingIndices) {
		return fmt.Errorf("not enough target nodes for ec repair: need=%d got=%d", len(missingIndices), len(targetNodes))
	}

	repairedLocations := make([]meta.ECShardLocation, 0, len(missingIndices))
	for i, shardIndex := range missingIndices {
		payload := shards[shardIndex]
		if len(payload) == 0 {
			return fmt.Errorf("missing reconstructed shard payload index=%d", shardIndex)
		}
		path := fmt.Sprintf("%s_cold_chunk_%d", task.ObjectID, shardIndex)
		nodeID := targetNodes[i]

		if err := p.writeSingleBlob(ctx, nodeID, path, payload); err != nil {
			return fmt.Errorf("write repaired ec shard failed index=%d node=%s: %w", shardIndex, nodeID, err)
		}

		repairedLocations = append(repairedLocations, meta.ECShardLocation{
			ShardIndex: shardIndex,
			NodeID:     nodeID,
			Path:       path,
			Status:     "ACTIVE",
		})
	}

	if err := p.store.PromoteObjectVersionToEC(ctx, task.ObjectID, task.Version, "", k, m, repairedLocations); err != nil {
		return err
	}

	log.Printf(
		"[TieringWorker][REPAIR][EC] object=%s version=%d repaired_shards=%d total=%d",
		task.ObjectID,
		task.Version,
		len(repairedLocations),
		totalShards,
	)
	return nil
}

func (p *ReplicationRepairProcessor) fetchFromActiveReplicas(ctx context.Context, objectID string, version int64, replicas []meta.ReplicaLocation) ([]byte, error) {
	for _, r := range replicas {
		key := r.Path
		if key == "" {
			key = meta.BuildHotReplicaPath(objectID, version)
		}
		if key == "" {
			key = objectID
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/retrieve/%s", r.NodeID, url.PathEscape(key)), nil)
		if err != nil {
			continue
		}
		resp, err := p.http.Do(req)
		if err != nil {
			continue
		}
		data, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			continue
		}
		if resp.StatusCode == http.StatusOK {
			return data, nil
		}
		// Backward compatibility for old replica keys.
		if key != objectID {
			req, err = http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/retrieve/%s", r.NodeID, url.PathEscape(objectID)), nil)
			if err != nil {
				continue
			}
			resp, err = p.http.Do(req)
			if err != nil {
				continue
			}
			data, readErr = io.ReadAll(resp.Body)
			resp.Body.Close()
			if readErr == nil && resp.StatusCode == http.StatusOK {
				return data, nil
			}
		}
	}
	return nil, fmt.Errorf("failed to fetch source bytes from active replicas")
}

func (p *ReplicationRepairProcessor) fetchSingleBlob(ctx context.Context, nodeID, key string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/retrieve/%s", nodeID, url.PathEscape(key)), nil)
	if err != nil {
		return nil, err
	}
	resp, err := p.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status=%d", resp.StatusCode)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (p *ReplicationRepairProcessor) writeReplicas(ctx context.Context, objectID string, version int64, payload []byte, targets []string) []string {
	out := make([]string, 0, len(targets))
	replicaPath := meta.BuildHotReplicaPath(objectID, version)
	if replicaPath == "" {
		replicaPath = objectID
	}
	for _, nodeID := range targets {
		if err := p.writeSingleBlob(ctx, nodeID, replicaPath, payload); err != nil {
			continue
		}
		out = append(out, nodeID)
	}
	return out
}

func (p *ReplicationRepairProcessor) writeSingleBlob(ctx context.Context, nodeID, key string, payload []byte) error {
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		fmt.Sprintf("%s/store?key=%s", nodeID, url.QueryEscape(key)),
		bytes.NewReader(payload),
	)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := p.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status=%d", resp.StatusCode)
	}
	return nil
}

func (p *ReplicationRepairProcessor) pickRepairTargetNodes(key string, healthyNodes []string, usedNodeSet map[string]struct{}, need int) []string {
	if need <= 0 {
		return nil
	}
	unused := make([]string, 0, len(healthyNodes))
	for _, nodeID := range healthyNodes {
		if _, used := usedNodeSet[nodeID]; used {
			continue
		}
		unused = append(unused, nodeID)
	}

	out := placement.SelectByRendezvous(key, unused, need)
	if len(out) >= need {
		return out
	}
	selected := make(map[string]struct{}, len(out))
	for _, nodeID := range out {
		selected[nodeID] = struct{}{}
	}
	fallback := make([]string, 0, len(healthyNodes))
	for _, nodeID := range healthyNodes {
		if _, ok := selected[nodeID]; ok {
			continue
		}
		fallback = append(fallback, nodeID)
	}
	out = append(out, placement.SelectByRendezvous(key+":fallback", fallback, need-len(out))...)
	return out
}
