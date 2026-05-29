package tiering

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"hybrid_distributed_store/internal/config"
	"hybrid_distributed_store/internal/ec"
	"hybrid_distributed_store/internal/meta"
	"hybrid_distributed_store/internal/placement"
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

	nodeCount := config.K + config.M + 3
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
	serverURLs := make([]string, 0, len(servers))
	for _, srv := range servers {
		serverURLs = append(serverURLs, srv.URL)
		if err := store.UpsertNodeHeartbeat(ctx, srv.URL, 100, 1000, 0, 0, 0, 25, 0, "UP"); err != nil {
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
	expectedShardNodes := placement.SelectByRendezvous(placement.ECShardKey(objectID, version), serverURLs, config.K+config.M)
	if len(view.ECShardLocations) != len(expectedShardNodes) {
		t.Fatalf("expected %d active shards, got %d", len(expectedShardNodes), len(view.ECShardLocations))
	}
	for _, shard := range view.ECShardLocations {
		if shard.ShardIndex < 0 || shard.ShardIndex >= len(expectedShardNodes) {
			t.Fatalf("unexpected shard index in placement: %+v", shard)
		}
		if shard.NodeID != expectedShardNodes[shard.ShardIndex] {
			t.Fatalf("shard %d placed on %s, want %s", shard.ShardIndex, shard.NodeID, expectedShardNodes[shard.ShardIndex])
		}
	}

	tasks, err := store.ListTieringTasks(ctx, "PENDING", TaskTypeGC, 10)
	if err != nil {
		t.Fatalf("list tiering tasks failed: %v", err)
	}
	if len(tasks) == 0 {
		t.Fatalf("expected pending GC task after promotion")
	}
}

func TestReplicationToECProcessor_ThrottleReservesWindow(t *testing.T) {
	t.Parallel()

	base := time.Unix(1700000000, 0)
	p := &ReplicationToECProcessor{
		throttleBytesPerSec: 100, // bytes/sec
		nowFn: func() time.Time {
			return base
		},
	}
	var (
		mu     sync.Mutex
		sleeps []time.Duration
	)
	p.sleepFn = func(ctx context.Context, d time.Duration) {
		mu.Lock()
		sleeps = append(sleeps, d)
		mu.Unlock()
	}

	// first call consumes window but should not wait
	p.throttle(context.Background(), 50)
	// second call at same time should wait 500ms
	p.throttle(context.Background(), 50)

	mu.Lock()
	defer mu.Unlock()
	if len(sleeps) != 1 {
		t.Fatalf("expected exactly one sleep, got=%d", len(sleeps))
	}
	if sleeps[0] != 500*time.Millisecond {
		t.Fatalf("unexpected sleep duration: got=%s want=%s", sleeps[0], 500*time.Millisecond)
	}
}

func TestReplicationToECProcessor_ThrottleDisabled(t *testing.T) {
	t.Parallel()

	p := &ReplicationToECProcessor{
		throttleBytesPerSec: 0,
		nowFn:               time.Now,
		sleepFn: func(ctx context.Context, d time.Duration) {
			t.Fatalf("sleep should not be called when throttle is disabled")
		},
	}
	p.throttle(context.Background(), 1024)
}

func TestReplicationToECProcessor_WriteShardsHonorsParallelism(t *testing.T) {
	t.Parallel()

	var active int64
	var maxActive int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		current := atomic.AddInt64(&active, 1)
		for {
			previous := atomic.LoadInt64(&maxActive)
			if current <= previous || atomic.CompareAndSwapInt64(&maxActive, previous, current) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		atomic.AddInt64(&active, -1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	shards := [][]byte{
		[]byte("shard-0"),
		[]byte("shard-1"),
		[]byte("shard-2"),
		[]byte("shard-3"),
		[]byte("shard-4"),
		[]byte("shard-5"),
	}
	nodes := make([]string, len(shards))
	for i := range nodes {
		nodes[i] = server.URL
	}

	processor := &ReplicationToECProcessor{
		http:                  server.Client(),
		shardWriteParallelism: 2,
		nowFn:                 time.Now,
		sleepFn:               sleepWithContext,
	}
	success, locations := processor.writeShards(context.Background(), nodes, "obj-parallel", 7, shards)
	if success != len(shards) {
		t.Fatalf("success=%d want %d", success, len(shards))
	}
	if len(locations) != len(shards) {
		t.Fatalf("locations=%d want %d", len(locations), len(shards))
	}
	if got := atomic.LoadInt64(&maxActive); got > 2 {
		t.Fatalf("max concurrent writes=%d want <=2", got)
	}
	if got := atomic.LoadInt64(&maxActive); got < 2 {
		t.Fatalf("expected parallel writes, max concurrent writes=%d", got)
	}
	for i, loc := range locations {
		if loc.ShardIndex != i {
			t.Fatalf("locations not sorted: index %d has shard %d", i, loc.ShardIndex)
		}
	}
}

func TestReplicationToECProcessor_NormalizesShardWriteParallelism(t *testing.T) {
	t.Parallel()

	if got := normalizeECShardWriteParallelism(0); got != 1 {
		t.Fatalf("normalizeECShardWriteParallelism(0)=%d want 1", got)
	}
	if got := normalizeECShardWriteParallelism(-2); got != 1 {
		t.Fatalf("normalizeECShardWriteParallelism(-2)=%d want 1", got)
	}
	if got := normalizeECShardWriteParallelism(4); got != 4 {
		t.Fatalf("normalizeECShardWriteParallelism(4)=%d want 4", got)
	}
}
