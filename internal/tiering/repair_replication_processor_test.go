package tiering

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"hybrid_distributed_store/internal/config"
	"hybrid_distributed_store/internal/ec"
	"hybrid_distributed_store/internal/meta"
)

type testStorageNode struct {
	mu   sync.Mutex
	data map[string][]byte
}

func newTestStorageNode(initial map[string][]byte) *testStorageNode {
	cloned := make(map[string][]byte, len(initial))
	for k, v := range initial {
		b := make([]byte, len(v))
		copy(b, v)
		cloned[k] = b
	}
	return &testStorageNode{data: cloned}
}

func (n *testStorageNode) handler(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/retrieve/"):
		raw := strings.TrimPrefix(r.URL.Path, "/retrieve/")
		key, err := url.PathUnescape(raw)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		n.mu.Lock()
		value, ok := n.data[key]
		n.mu.Unlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(value)
		return

	case r.Method == http.MethodPost && r.URL.Path == "/store":
		key := r.URL.Query().Get("key")
		if key == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		payload, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		n.mu.Lock()
		n.data[key] = payload
		n.mu.Unlock()
		w.WriteHeader(http.StatusOK)
		return

	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func (n *testStorageNode) get(key string) ([]byte, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	v, ok := n.data[key]
	if !ok {
		return nil, false
	}
	out := make([]byte, len(v))
	copy(out, v)
	return out, true
}

func (n *testStorageNode) put(key string, payload []byte) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	b := make([]byte, len(payload))
	copy(b, payload)
	n.data[key] = b
	return nil
}

func TestReplicationRepairProcessor_RepairsMissingReplica(t *testing.T) {
	origHotReplicaCount := config.HotReplicaCount
	defer func() {
		config.HotReplicaCount = origHotReplicaCount
	}()
	config.HotReplicaCount = 3

	store, err := meta.NewRocksStore(meta.Config{
		Enabled: true,
		Backend: "rocksdb",
		DSN:     filepath.Join(t.TempDir(), "meta"),
	})
	if err != nil {
		t.Fatalf("new rocks store failed: %v", err)
	}
	defer store.Close()

	payload := []byte("repair-payload")
	nodeA := newTestStorageNode(map[string][]byte{"obj-repair": payload})
	nodeB := newTestStorageNode(map[string][]byte{"obj-repair": payload})
	nodeC := newTestStorageNode(nil)

	srvA := httptest.NewServer(http.HandlerFunc(nodeA.handler))
	defer srvA.Close()
	srvB := httptest.NewServer(http.HandlerFunc(nodeB.handler))
	defer srvB.Close()
	srvC := httptest.NewServer(http.HandlerFunc(nodeC.handler))
	defer srvC.Close()

	ctx := context.Background()
	for _, nodeURL := range []string{srvA.URL, srvB.URL, srvC.URL} {
		if err := store.UpsertNodeHeartbeat(ctx, nodeURL, 100, 1000, 0, 0, "UP"); err != nil {
			t.Fatalf("upsert node heartbeat failed: %v", err)
		}
	}

	const version = int64(777)
	if err := store.UpsertNormalizedMetadata(ctx, "obj-repair", map[string]interface{}{
		"strategy":        "replication",
		"hot_version":     version,
		"original_length": len(payload),
		"replica_nodes":   []string{srvA.URL, srvB.URL},
	}); err != nil {
		t.Fatalf("upsert metadata failed: %v", err)
	}

	processor := NewReplicationRepairProcessor(store, &http.Client{}, ec.NewService())
	if err := processor.ProcessReplicationRepair(ctx, &meta.TieringTask{
		TaskID:   "repair-repl:obj-repair:777",
		ObjectID: "obj-repair",
		Version:  version,
		TaskType: TaskTypeRepair,
	}); err != nil {
		t.Fatalf("repair processor failed: %v", err)
	}

	got, ok := nodeC.get("obj-repair")
	if !ok {
		t.Fatalf("expected repaired payload on target node")
	}
	if string(got) != string(payload) {
		t.Fatalf("unexpected repaired payload: %q", string(got))
	}

	replicas, err := store.ListActiveReplicaLocations(ctx, "obj-repair", version)
	if err != nil {
		t.Fatalf("list active replicas failed: %v", err)
	}
	if len(replicas) != 3 {
		t.Fatalf("expected 3 active replicas after repair, got %d", len(replicas))
	}
}

func TestReplicationRepairProcessor_RepairsMissingECShard(t *testing.T) {
	store, err := meta.NewRocksStore(meta.Config{
		Enabled: true,
		Backend: "rocksdb",
		DSN:     filepath.Join(t.TempDir(), "meta-ec"),
	})
	if err != nil {
		t.Fatalf("new rocks store failed: %v", err)
	}
	defer store.Close()

	ecDriver := ec.NewService()
	payload := []byte("ec-repair-payload-abcdefghijklmnopqrstuvwxyz")
	shards, err := ecDriver.Split(payload)
	if err != nil {
		t.Fatalf("ec split failed: %v", err)
	}
	if err := ecDriver.Encode(shards); err != nil {
		t.Fatalf("ec encode failed: %v", err)
	}

	nodes := make([]*testStorageNode, 0, config.K+config.M)
	servers := make([]*httptest.Server, 0, config.K+config.M)
	for i := 0; i < config.K+config.M; i++ {
		nodes = append(nodes, newTestStorageNode(nil))
	}
	for i := 0; i < config.K+config.M; i++ {
		srv := httptest.NewServer(http.HandlerFunc(nodes[i].handler))
		servers = append(servers, srv)
		defer srv.Close()
	}

	objectID := "obj-ec-repair"
	const version = int64(888)
	missingIndex := config.K + config.M - 1

	ctx := context.Background()
	for i, srv := range servers {
		if err := store.UpsertNodeHeartbeat(ctx, srv.URL, 100, 1000, 0, 0, "UP"); err != nil {
			t.Fatalf("upsert node heartbeat failed idx=%d: %v", i, err)
		}
	}

	if err := store.UpsertNormalizedMetadata(ctx, objectID, map[string]interface{}{
		"strategy":        "replication",
		"hot_version":     version,
		"original_length": len(payload),
		"replica_nodes":   []string{servers[0].URL},
	}); err != nil {
		t.Fatalf("upsert metadata failed: %v", err)
	}

	locations := make([]meta.ECShardLocation, 0, config.K+config.M-1)
	for i := 0; i < config.K+config.M; i++ {
		chunkKey := objectID + "_cold_chunk_" + fmt.Sprint(i)
		if i != missingIndex {
			if err := nodes[i].put(chunkKey, shards[i]); err != nil {
				t.Fatalf("seed shard failed index=%d: %v", i, err)
			}
			locations = append(locations, meta.ECShardLocation{
				ShardIndex: i,
				NodeID:     servers[i].URL,
				Path:       chunkKey,
				Status:     "ACTIVE",
			})
		}
	}

	if err := store.PromoteObjectVersionToEC(ctx, objectID, version, "", config.K, config.M, locations); err != nil {
		t.Fatalf("promote object to ec failed: %v", err)
	}

	processor := NewReplicationRepairProcessor(store, &http.Client{}, ec.NewService())
	if err := processor.ProcessReplicationRepair(ctx, &meta.TieringTask{
		TaskID:   "repair-ec:obj-ec-repair:888",
		ObjectID: objectID,
		Version:  version,
		TaskType: TaskTypeRepair,
	}); err != nil {
		t.Fatalf("repair processor failed: %v", err)
	}

	view, err := store.GetObjectAdminView(ctx, objectID)
	if err != nil {
		t.Fatalf("get object admin view failed: %v", err)
	}
	if view == nil {
		t.Fatalf("expected object admin view")
	}
	if len(view.ECShardLocations) != config.K+config.M {
		t.Fatalf("expected full ec shard rows=%d, got %d", config.K+config.M, len(view.ECShardLocations))
	}

	repairedPath := objectID + "_cold_chunk_" + fmt.Sprint(missingIndex)
	foundRepairedBlob := false
	for _, node := range nodes {
		got, ok := node.get(repairedPath)
		if !ok {
			continue
		}
		foundRepairedBlob = true
		if !bytes.Equal(got, shards[missingIndex]) {
			t.Fatalf("repaired shard payload mismatch")
		}
		break
	}
	if !foundRepairedBlob {
		t.Fatalf("expected repaired shard blob at key=%s", repairedPath)
	}
}
