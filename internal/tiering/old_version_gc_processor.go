package tiering

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/url"

	"hybrid_distributed_store/internal/config"
	"hybrid_distributed_store/internal/meta"
)

// OldVersionGCProcessor reaps old object versions based on retention policy.
type OldVersionGCProcessor struct {
	store meta.Repository
	http  *http.Client
}

func NewOldVersionGCProcessor(store meta.Repository, httpClient *http.Client) *OldVersionGCProcessor {
	return &OldVersionGCProcessor{
		store: store,
		http:  httpClient,
	}
}

func (p *OldVersionGCProcessor) ProcessOldVersionGC(ctx context.Context, task *meta.TieringTask) error {
	if task == nil {
		return fmt.Errorf("nil task")
	}
	if p.store == nil || p.http == nil {
		return fmt.Errorf("old-version gc processor dependency is nil")
	}

	view, err := p.store.GetObjectVersionGCView(ctx, task.ObjectID, task.Version)
	if err != nil {
		return err
	}
	if view == nil {
		return nil
	}
	if view.CurrentVersion == task.Version {
		log.Printf("[TieringWorker][GC_OLD] skip current version object=%s version=%d", task.ObjectID, task.Version)
		return nil
	}

	switch view.Tier {
	case "HOT":
		if err := p.deleteHOTVersion(ctx, task.ObjectID, task.Version, view.ReplicaLocations); err != nil {
			return err
		}
	case "EC":
		if err := p.deleteECVersion(ctx, view.ECShardLocations); err != nil {
			return err
		}
	}

	if err := p.store.PurgeObjectVersionMetadata(ctx, task.ObjectID, task.Version); err != nil {
		return err
	}
	log.Printf("[TieringWorker][GC_OLD] object=%s version=%d tier=%s purged", task.ObjectID, task.Version, view.Tier)
	return nil
}

func (p *OldVersionGCProcessor) deleteHOTVersion(ctx context.Context, objectID string, version int64, replicas []meta.ReplicaLocation) error {
	if len(replicas) > 0 {
		for _, r := range replicas {
			key := r.Path
			if key == "" {
				key = meta.BuildHotReplicaPath(objectID, version)
			}
			if key == "" {
				key = objectID
			}
			if err := p.deleteBlob(ctx, r.NodeID, key); err != nil {
				return err
			}
			if key != objectID {
				// Best-effort cleanup for legacy keys.
				_ = p.deleteBlob(ctx, r.NodeID, objectID)
			}
		}
		return nil
	}

	// Fallback for missing placement rows: scrub on all healthy nodes.
	nodes, err := p.store.ListHealthyNodeIDs(ctx, config.NodeHeartbeatStaleSec)
	if err != nil {
		return err
	}
	versionedKey := meta.BuildHotReplicaPath(objectID, version)
	if versionedKey == "" {
		versionedKey = objectID
	}
	for _, nodeID := range nodes {
		if err := p.deleteBlob(ctx, nodeID, versionedKey); err != nil {
			return err
		}
		if versionedKey != objectID {
			_ = p.deleteBlob(ctx, nodeID, objectID)
		}
	}
	return nil
}

func (p *OldVersionGCProcessor) deleteECVersion(ctx context.Context, shards []meta.ECShardLocation) error {
	for _, shard := range shards {
		if shard.NodeID == "" || shard.Path == "" {
			continue
		}
		if err := p.deleteBlob(ctx, shard.NodeID, shard.Path); err != nil {
			return err
		}
	}
	return nil
}

func (p *OldVersionGCProcessor) deleteBlob(ctx context.Context, nodeID, key string) error {
	if nodeID == "" || key == "" {
		return nil
	}
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodDelete,
		fmt.Sprintf("%s/delete/%s", nodeID, url.PathEscape(key)),
		nil,
	)
	if err != nil {
		return err
	}
	resp, err := p.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("delete blob failed node=%s key=%s status=%d", nodeID, key, resp.StatusCode)
	}
	return nil
}
