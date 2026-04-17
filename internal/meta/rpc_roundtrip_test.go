package meta

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"
)

func TestRPCClientServerRoundTrip(t *testing.T) {
	store, err := NewTiKVStore(Config{
		Enabled: true,
		DSN:     "memory://rpc-roundtrip",
	})
	if err != nil {
		t.Fatalf("new tikv store failed: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	rpcServer := NewRPCServer(store, "")
	t.Cleanup(func() { _ = rpcServer.Close() })

	srv := httptest.NewServer(rpcServer.Handler())
	t.Cleanup(srv.Close)

	client := NewRPCClient(srv.URL, "")
	ctx := context.Background()

	if err := client.Ping(ctx); err != nil {
		t.Fatalf("rpc ping failed: %v", err)
	}

	if err := client.UpsertNodeHeartbeat(ctx, "node-rpc", 100, 1000, 0, 0.1, 32.5, 1.2, "UP"); err != nil {
		t.Fatalf("upsert node heartbeat failed: %v", err)
	}
	nodes, err := client.ListHealthyNodeIDs(ctx, 60)
	if err != nil {
		t.Fatalf("list healthy nodes failed: %v", err)
	}
	if len(nodes) != 1 || nodes[0] != "node-rpc" {
		t.Fatalf("unexpected healthy nodes: %v", nodes)
	}

	objectID := "obj-rpc"
	version := int64(303)
	if err := client.UpsertNormalizedMetadata(ctx, objectID, map[string]interface{}{
		"strategy":        "replication",
		"hot_version":     version,
		"cold_hash":       "hash-303",
		"original_length": 16,
		"replica_nodes":   []string{"node-rpc"},
	}); err != nil {
		t.Fatalf("upsert metadata failed: %v", err)
	}
	a2Count, err := client.EnqueueTieringCandidatesA2(ctx, 0, 1, 10)
	if err != nil {
		t.Fatalf("enqueue A2 candidates failed: %v", err)
	}
	if a2Count != 0 {
		t.Fatalf("expected A2 enqueued=0 before due time, got=%d", a2Count)
	}
	if _, err := client.EnqueueTieringCandidatesA3(ctx, 0, 10, 1024); err != nil {
		t.Fatalf("enqueue A3 candidates failed: %v", err)
	}

	taskID := "repl2ec:obj-rpc:303"
	if err := client.EnqueueTieringTask(ctx, taskID, objectID, version, "REPL_TO_EC", 100, time.Now()); err != nil {
		t.Fatalf("enqueue task failed: %v", err)
	}
	repairCount, err := client.EnqueueRepairCandidates(ctx, 10)
	if err != nil {
		t.Fatalf("enqueue repair candidates failed: %v", err)
	}
	if repairCount <= 0 {
		t.Fatalf("expected repair candidates to be enqueued, got=%d", repairCount)
	}
	task, err := client.ClaimNextTieringTask(ctx, "REPL_TO_EC")
	if err != nil {
		t.Fatalf("claim task failed: %v", err)
	}
	if task == nil || task.TaskID != taskID {
		t.Fatalf("unexpected claimed task: %+v", task)
	}

	lock, acquired, err := client.TryAcquireLeaderLock(ctx, 42042)
	if err != nil {
		t.Fatalf("try acquire leader lock failed: %v", err)
	}
	if !acquired || lock == nil {
		t.Fatalf("expected acquired leader lock")
	}
	if err := lock.Ping(ctx); err != nil {
		t.Fatalf("leader lock ping failed: %v", err)
	}
	if err := lock.Release(ctx); err != nil {
		t.Fatalf("leader lock release failed: %v", err)
	}
}

func TestRPCClientServerAuthToken(t *testing.T) {
	t.Parallel()

	store, err := NewTiKVStore(Config{
		Enabled: true,
		DSN:     "memory://rpc-auth",
	})
	if err != nil {
		t.Fatalf("new tikv store failed: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	rpcServer := NewRPCServer(store, "secret-token")
	t.Cleanup(func() { _ = rpcServer.Close() })

	srv := httptest.NewServer(rpcServer.Handler())
	t.Cleanup(srv.Close)

	ctx := context.Background()
	unauthorized := NewRPCClient(srv.URL, "")
	if err := unauthorized.Ping(ctx); err == nil {
		t.Fatalf("expected unauthorized rpc call to fail")
	}

	authorized := NewRPCClient(srv.URL, "secret-token")
	if err := authorized.Ping(ctx); err != nil {
		t.Fatalf("expected authorized rpc ping success, got: %v", err)
	}
}

func TestLeaderLockTokenSurvivesReplicaSwitch(t *testing.T) {
	store, err := NewTiKVStore(Config{
		Enabled: true,
		DSN:     "memory://rpc-lock-replica-switch",
	})
	if err != nil {
		t.Fatalf("new tikv store failed: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	s1 := NewRPCServer(store, "token-secret")
	s2 := NewRPCServer(store, "token-secret")
	t.Cleanup(func() { _ = s1.Close() })
	t.Cleanup(func() { _ = s2.Close() })

	h1 := httptest.NewServer(s1.Handler())
	h2 := httptest.NewServer(s2.Handler())
	t.Cleanup(h1.Close)
	t.Cleanup(h2.Close)

	c1 := NewRPCClient(h1.URL, "token-secret")
	c2 := NewRPCClient(h2.URL, "token-secret")
	ctx := context.Background()

	lock, acquired, err := c1.TryAcquireLeaderLock(ctx, 55001)
	if err != nil {
		t.Fatalf("try acquire leader lock failed: %v", err)
	}
	if !acquired || lock == nil {
		t.Fatalf("expected acquired lock")
	}

	lock1, ok := lock.(*rpcLeaderLock)
	if !ok || lock1.token == "" {
		t.Fatalf("unexpected lock type/token: %#v", lock)
	}

	// Simulate LB switching to another meta_service replica on heartbeat path.
	lock2 := &rpcLeaderLock{client: c2, token: lock1.token}
	if err := lock2.Ping(ctx); err != nil {
		t.Fatalf("ping via second replica failed: %v", err)
	}
	if err := lock2.Release(ctx); err != nil {
		t.Fatalf("release via second replica failed: %v", err)
	}
}
