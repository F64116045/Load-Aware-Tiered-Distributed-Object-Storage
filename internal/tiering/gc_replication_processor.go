package tiering

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/url"

	"hybrid_distributed_store/internal/meta"
)

// ReplicationGCProcessor executes GC tasks that clean old HOT replicas after EC promotion.
type ReplicationGCProcessor struct {
	store meta.Repository
	http  *http.Client
}

// NewReplicationGCProcessor constructs a GC processor implementation.
func NewReplicationGCProcessor(store meta.Repository, httpClient *http.Client) *ReplicationGCProcessor {
	return &ReplicationGCProcessor{
		store: store,
		http:  httpClient,
	}
}

// ProcessReplicationGC deletes replicated blobs and marks replica rows as DELETED.
func (p *ReplicationGCProcessor) ProcessReplicationGC(ctx context.Context, task *meta.TieringTask) error {
	if task == nil {
		return fmt.Errorf("nil task")
	}
	if p.store == nil || p.http == nil {
		return fmt.Errorf("gc processor dependency is nil")
	}

	snapshot, err := p.store.GetObjectVersionSnapshot(ctx, task.ObjectID, task.Version)
	if err != nil {
		return err
	}
	if snapshot == nil {
		return nil
	}
	if snapshot.CurrentVersion != task.Version {
		// GC only applies to current version rows to avoid deleting blobs for newer writes.
		log.Printf("[TieringWorker][GC] task=%s object=%s stale version=%d current=%d skip", task.TaskID, task.ObjectID, task.Version, snapshot.CurrentVersion)
		return nil
	}
	if snapshot.Tier != "EC" || snapshot.State != "EC_ACTIVE" {
		return fmt.Errorf("object %s version %d not ready for replica gc (state=%s tier=%s)", task.ObjectID, task.Version, snapshot.State, snapshot.Tier)
	}

	replicas, err := p.store.ListActiveReplicaLocations(ctx, task.ObjectID, task.Version)
	if err != nil {
		return err
	}
	if len(replicas) == 0 {
		return nil
	}

	deletedNodeIDs := make([]string, 0, len(replicas))
	for _, r := range replicas {
		key := r.Path
		if key == "" {
			key = task.ObjectID
		}
		req, reqErr := http.NewRequestWithContext(
			ctx,
			http.MethodDelete,
			fmt.Sprintf("%s/delete/%s", r.NodeID, url.PathEscape(key)),
			nil,
		)
		if reqErr != nil {
			return fmt.Errorf("build gc delete request failed: %w", reqErr)
		}
		resp, doErr := p.http.Do(req)
		if doErr != nil {
			return fmt.Errorf("gc delete request failed for node=%s: %w", r.NodeID, doErr)
		}
		_ = resp.Body.Close()

		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNotFound {
			return fmt.Errorf("gc delete failed for node=%s status=%d", r.NodeID, resp.StatusCode)
		}
		deletedNodeIDs = append(deletedNodeIDs, r.NodeID)
	}

	if err := p.store.MarkReplicaLocationsDeleted(ctx, task.ObjectID, task.Version, deletedNodeIDs); err != nil {
		return err
	}
	log.Printf("[TieringWorker][GC] object=%s version=%d replicas_deleted=%d", task.ObjectID, task.Version, len(deletedNodeIDs))
	return nil
}
