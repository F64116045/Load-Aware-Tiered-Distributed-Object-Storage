package tiering

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"hybrid_distributed_store/internal/config"
	"hybrid_distributed_store/internal/ec"
	"hybrid_distributed_store/internal/meta"
)

func TestReplicationToECProcessor_UsesVersionedReplicaPath(t *testing.T) {
	store, err := meta.NewTiKVStore(meta.Config{
		Enabled: true,
		DSN:     "memory://repl-to-ec-versioned-path",
	})
	if err != nil {
		t.Fatalf("new tikv store failed: %v", err)
	}
	defer store.Close()

	objectID := "obj-migrate"
	version := int64(123456789)
	payload := []byte("payload-for-migration")
	hotPath := meta.BuildHotReplicaPath(objectID, version)

	nodeCount := config.K + config.M
	if nodeCount < 4 {
		nodeCount = 4
	}

	servers := make([]*httptest.Server, 0, nodeCount)
	for i := 0; i < nodeCount; i++ {
		initial := map[string][]byte{}
		if i == 0 {
			initial[hotPath] = payload
		}
		node := newTestStorageNode(initial)
		srv := httptest.NewServer(http.HandlerFunc(node.handler))
		servers = append(servers, srv)
		defer srv.Close()
	}

	ctx := context.Background()
	for _, srv := range servers {
		if err := store.UpsertNodeHeartbeat(ctx, srv.URL, 100, 1000, 0, 0, 25, 0, "UP"); err != nil {
			t.Fatalf("upsert node heartbeat failed: %v", err)
		}
	}

	if err := store.UpsertNormalizedMetadata(ctx, objectID, map[string]interface{}{
		"strategy":        "replication",
		"hot_version":     version,
		"hot_key":         hotPath,
		"original_length": len(payload),
		"replica_nodes":   []string{servers[0].URL},
	}); err != nil {
		t.Fatalf("upsert metadata failed: %v", err)
	}

	processor := NewReplicationToECProcessor(store, &http.Client{}, ec.NewService())
	if err := processor.ProcessReplicationToEC(ctx, &meta.TieringTask{
		TaskID:   "repl2ec:obj-migrate:123456789",
		ObjectID: objectID,
		Version:  version,
		TaskType: TaskTypeReplicationToEC,
	}); err != nil {
		t.Fatalf("process repl_to_ec failed: %v", err)
	}

	view, err := store.GetObjectAdminView(ctx, objectID)
	if err != nil {
		t.Fatalf("get object admin view failed: %v", err)
	}
	if view == nil || view.Version == nil {
		t.Fatalf("expected object/version view")
	}
	if view.State != "EC_ACTIVE" {
		t.Fatalf("expected state EC_ACTIVE, got %q", view.State)
	}
	if view.Version.Tier != "EC" {
		t.Fatalf("expected tier EC, got %q", view.Version.Tier)
	}
	if len(view.ECShardLocations) < config.K {
		t.Fatalf("expected at least %d active shards, got %d", config.K, len(view.ECShardLocations))
	}

	tasks, err := store.ListTieringTasks(ctx, "PENDING", TaskTypeGC, 10)
	if err != nil {
		t.Fatalf("list tiering tasks failed: %v", err)
	}
	if len(tasks) == 0 {
		t.Fatalf("expected pending GC task after promotion")
	}
}
