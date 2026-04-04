package meta

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func newTestRocksStore(t *testing.T) *RocksStore {
	t.Helper()

	dir := filepath.Join(t.TempDir(), "meta-rocks")
	store, err := NewRocksStore(Config{
		Enabled: true,
		Backend: "rocksdb",
		DSN:     dir,
	})
	if err != nil {
		t.Fatalf("new rocks store failed: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})
	return store
}

func TestRocksStore_NormalizedMetadataRoundTrip(t *testing.T) {
	t.Parallel()

	store := newTestRocksStore(t)
	ctx := context.Background()

	objectID := "obj-roundtrip"
	input := map[string]interface{}{
		"strategy":        "replication",
		"hot_version":     int64(7),
		"cold_hash":       "hash-7",
		"original_length": int64(2048),
		"content_type":    "application/octet-stream",
		"k":               4,
		"m":               2,
		"replica_nodes":   []string{"node-a", "node-b"},
	}

	if err := store.UpsertNormalizedMetadata(ctx, objectID, input); err != nil {
		t.Fatalf("upsert metadata failed: %v", err)
	}

	got, err := store.GetNormalizedMetadata(ctx, objectID)
	if err != nil {
		t.Fatalf("get metadata failed: %v", err)
	}
	if got == nil {
		t.Fatalf("metadata should not be nil")
	}
	if got["strategy"] != "replication" {
		t.Fatalf("unexpected strategy: %v", got["strategy"])
	}
	if toInt64(got["hot_version"], 0) != 7 {
		t.Fatalf("unexpected hot version: %v", got["hot_version"])
	}
	if toInt64(got["cold_version"], 0) != 0 {
		t.Fatalf("unexpected cold version: %v", got["cold_version"])
	}

	view, err := store.GetObjectAdminView(ctx, objectID)
	if err != nil {
		t.Fatalf("get admin view failed: %v", err)
	}
	if view == nil {
		t.Fatalf("admin view should not be nil")
	}
	if view.State != "HOT_ACTIVE" {
		t.Fatalf("unexpected object state: %s", view.State)
	}
	if view.Version == nil || view.Version.Tier != "HOT" {
		t.Fatalf("unexpected version tier: %+v", view.Version)
	}
	if len(view.ReplicaLocations) != 2 {
		t.Fatalf("unexpected replica count: %d", len(view.ReplicaLocations))
	}

	if err := store.DeleteNormalizedMetadata(ctx, objectID); err != nil {
		t.Fatalf("delete metadata failed: %v", err)
	}
	got, err = store.GetNormalizedMetadata(ctx, objectID)
	if err != nil {
		t.Fatalf("get metadata after delete failed: %v", err)
	}
	if got != nil {
		t.Fatalf("metadata should be nil after delete, got=%v", got)
	}
}

func TestRocksStore_TieringTaskLifecycle(t *testing.T) {
	t.Parallel()

	store := newTestRocksStore(t)
	ctx := context.Background()

	objectID := "obj-tiering"
	version := int64(101)
	if err := store.UpsertNormalizedMetadata(ctx, objectID, map[string]interface{}{
		"strategy":      "replication",
		"hot_version":   version,
		"cold_hash":     "hash-101",
		"replica_nodes": []string{"node-a", "node-b"},
	}); err != nil {
		t.Fatalf("upsert metadata failed: %v", err)
	}

	taskID := fmt.Sprintf("repl2ec:%s:%d", objectID, version)
	if err := store.EnqueueTieringTask(ctx, taskID, objectID, version, "REPL_TO_EC", 100, time.Now()); err != nil {
		t.Fatalf("enqueue task failed: %v", err)
	}

	claimed, err := store.ClaimNextTieringTask(ctx, "REPL_TO_EC")
	if err != nil {
		t.Fatalf("claim task failed: %v", err)
	}
	if claimed == nil {
		t.Fatalf("expected claimed task")
	}
	if claimed.TaskID != taskID || claimed.TaskState != "RUNNING" {
		t.Fatalf("unexpected claimed task: %+v", claimed)
	}

	snap, err := store.GetObjectVersionSnapshot(ctx, objectID, version)
	if err != nil {
		t.Fatalf("get snapshot failed: %v", err)
	}
	if snap == nil {
		t.Fatalf("expected snapshot")
	}
	if snap.CurrentVersion != version || snap.Tier != "HOT" {
		t.Fatalf("unexpected snapshot before migration: %+v", snap)
	}

	if err := store.MarkObjectMigrating(ctx, objectID, version); err != nil {
		t.Fatalf("mark object migrating failed: %v", err)
	}

	locations := []ECShardLocation{
		{ShardIndex: 0, NodeID: "node-c", Path: "obj-tiering_cold_0", Status: "ACTIVE"},
		{ShardIndex: 1, NodeID: "node-d", Path: "obj-tiering_cold_1", Status: "ACTIVE"},
	}
	if err := store.PromoteObjectVersionToEC(ctx, objectID, version, "hash-101-ec", 4, 2, locations); err != nil {
		t.Fatalf("promote object to ec failed: %v", err)
	}
	if err := store.MarkTieringTaskDone(ctx, taskID); err != nil {
		t.Fatalf("mark task done failed: %v", err)
	}

	got, err := store.GetNormalizedMetadata(ctx, objectID)
	if err != nil {
		t.Fatalf("get metadata after promote failed: %v", err)
	}
	if got == nil {
		t.Fatalf("metadata should not be nil after promote")
	}
	if got["strategy"] != "ec" {
		t.Fatalf("unexpected strategy after promote: %v", got["strategy"])
	}
	if toInt64(got["cold_version"], 0) != version {
		t.Fatalf("unexpected cold version after promote: %v", got["cold_version"])
	}

	view, err := store.GetObjectAdminView(ctx, objectID)
	if err != nil {
		t.Fatalf("get admin view after promote failed: %v", err)
	}
	if view == nil || view.State != "EC_ACTIVE" {
		t.Fatalf("unexpected object state after promote: %+v", view)
	}
	if len(view.ECShardLocations) != len(locations) {
		t.Fatalf("unexpected ec shard count: %d", len(view.ECShardLocations))
	}

	active, err := store.ListActiveReplicaLocations(ctx, objectID, version)
	if err != nil {
		t.Fatalf("list active replicas failed: %v", err)
	}
	if len(active) != 2 {
		t.Fatalf("unexpected active replica count before gc: %d", len(active))
	}

	if err := store.MarkReplicaLocationsDeleted(ctx, objectID, version, []string{"node-a"}); err != nil {
		t.Fatalf("mark replica deleted failed: %v", err)
	}
	active, err = store.ListActiveReplicaLocations(ctx, objectID, version)
	if err != nil {
		t.Fatalf("list active replicas after gc failed: %v", err)
	}
	if len(active) != 1 {
		t.Fatalf("unexpected active replica count after gc: %d", len(active))
	}
}

func TestRocksStore_PolicyHeartbeatAndLeaderState(t *testing.T) {
	t.Parallel()

	store := newTestRocksStore(t)
	ctx := context.Background()

	objectID := "obj-policy"
	version := int64(202)
	if err := store.UpsertNormalizedMetadata(ctx, objectID, map[string]interface{}{
		"strategy":      "replication",
		"hot_version":   version,
		"cold_hash":     "hash-202",
		"replica_nodes": []string{"node-x"},
	}); err != nil {
		t.Fatalf("upsert metadata failed: %v", err)
	}

	enqueued, err := store.EnqueueTieringCandidatesA1(ctx, 0, 10)
	if err != nil {
		t.Fatalf("enqueue policy candidates failed: %v", err)
	}
	if enqueued != 1 {
		t.Fatalf("unexpected enqueued count: %d", enqueued)
	}

	snap, err := store.GetObjectVersionSnapshot(ctx, objectID, version)
	if err != nil {
		t.Fatalf("get snapshot failed: %v", err)
	}
	if snap == nil || snap.State != "MIGRATION_PENDING" {
		t.Fatalf("unexpected state after policy enqueue: %+v", snap)
	}

	if err := store.UpsertNodeHeartbeat(ctx, "node-x", 12345, 1, 0.3, "UP"); err != nil {
		t.Fatalf("upsert heartbeat failed: %v", err)
	}
	nodes, err := store.ListHealthyNodeIDs(ctx, 60)
	if err != nil {
		t.Fatalf("list healthy nodes failed: %v", err)
	}
	if len(nodes) != 1 || nodes[0] != "node-x" {
		t.Fatalf("unexpected healthy nodes: %v", nodes)
	}
	heartbeats, err := store.ListNodeHeartbeats(ctx, 10)
	if err != nil {
		t.Fatalf("list node heartbeats failed: %v", err)
	}
	if len(heartbeats) != 1 {
		t.Fatalf("unexpected heartbeat count: %d", len(heartbeats))
	}

	const lockKey int64 = 42042
	if err := store.UpsertTieringLeaderState(ctx, lockKey, "worker-1", "LEADING"); err != nil {
		t.Fatalf("upsert leader state failed: %v", err)
	}
	leader, err := store.GetTieringLeaderState(ctx, lockKey)
	if err != nil {
		t.Fatalf("get leader state failed: %v", err)
	}
	if leader == nil || leader.LeaderID != "worker-1" || leader.ScannerStatus != "LEADING" {
		t.Fatalf("unexpected leader state: %+v", leader)
	}

	if err := store.MarkTieringLeaderStopped(ctx, lockKey, "worker-1", "STOPPED"); err != nil {
		t.Fatalf("mark leader stopped failed: %v", err)
	}
	leader, err = store.GetTieringLeaderState(ctx, lockKey)
	if err != nil {
		t.Fatalf("get leader state after stop failed: %v", err)
	}
	if leader == nil || leader.ScannerStatus != "STOPPED" {
		t.Fatalf("unexpected leader state after stop: %+v", leader)
	}
}

func TestRocksStore_EnqueueRepairCandidates_HOTAndRequeue(t *testing.T) {
	t.Parallel()

	store := newTestRocksStore(t)
	ctx := context.Background()

	objectID := "obj-repair-hot"
	version := int64(303)
	if err := store.UpsertNormalizedMetadata(ctx, objectID, map[string]interface{}{
		"strategy":      "replication",
		"hot_version":   version,
		"cold_hash":     "hash-303",
		"replica_nodes": []string{"node-a"},
	}); err != nil {
		t.Fatalf("upsert metadata failed: %v", err)
	}

	enqueued, err := store.EnqueueRepairCandidates(ctx, 10)
	if err != nil {
		t.Fatalf("enqueue repair candidates failed: %v", err)
	}
	if enqueued != 1 {
		t.Fatalf("unexpected enqueued count: %d", enqueued)
	}

	taskID := fmt.Sprintf("repair-repl:%s:%d", objectID, version)
	tasks, err := store.ListTieringTasks(ctx, "", "REPAIR", 20)
	if err != nil {
		t.Fatalf("list tiering tasks failed: %v", err)
	}
	found := false
	for _, task := range tasks {
		if task.TaskID == taskID {
			found = true
			if task.TaskState != "PENDING" {
				t.Fatalf("unexpected task state: %s", task.TaskState)
			}
		}
	}
	if !found {
		t.Fatalf("expected repair task: %s", taskID)
	}

	if err := store.MarkTieringTaskDone(ctx, taskID); err != nil {
		t.Fatalf("mark task done failed: %v", err)
	}

	enqueued, err = store.EnqueueRepairCandidates(ctx, 10)
	if err != nil {
		t.Fatalf("enqueue repair candidates second round failed: %v", err)
	}
	if enqueued != 1 {
		t.Fatalf("expected done->pending requeue, got=%d", enqueued)
	}
}

func TestRocksStore_EnqueueRepairCandidates_EC(t *testing.T) {
	t.Parallel()

	store := newTestRocksStore(t)
	ctx := context.Background()

	objectID := "obj-repair-ec"
	version := int64(404)
	if err := store.UpsertNormalizedMetadata(ctx, objectID, map[string]interface{}{
		"strategy":      "replication",
		"hot_version":   version,
		"cold_hash":     "hash-404",
		"replica_nodes": []string{"node-a"},
	}); err != nil {
		t.Fatalf("upsert metadata failed: %v", err)
	}

	if err := store.PromoteObjectVersionToEC(ctx, objectID, version, "", 4, 2, []ECShardLocation{
		{ShardIndex: 0, NodeID: "node-a", Path: objectID + "_cold_chunk_0", Status: "ACTIVE"},
		{ShardIndex: 1, NodeID: "node-b", Path: objectID + "_cold_chunk_1", Status: "ACTIVE"},
	}); err != nil {
		t.Fatalf("promote object to ec failed: %v", err)
	}

	enqueued, err := store.EnqueueRepairCandidates(ctx, 10)
	if err != nil {
		t.Fatalf("enqueue repair candidates failed: %v", err)
	}
	if enqueued != 1 {
		t.Fatalf("unexpected enqueued count: %d", enqueued)
	}

	taskID := fmt.Sprintf("repair-ec:%s:%d", objectID, version)
	tasks, err := store.ListTieringTasks(ctx, "", "REPAIR", 20)
	if err != nil {
		t.Fatalf("list tiering tasks failed: %v", err)
	}
	found := false
	for _, task := range tasks {
		if task.TaskID == taskID {
			found = true
			if task.TaskState != "PENDING" {
				t.Fatalf("unexpected task state: %s", task.TaskState)
			}
		}
	}
	if !found {
		t.Fatalf("expected repair task: %s", taskID)
	}
}
