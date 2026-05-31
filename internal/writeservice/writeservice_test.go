package writeservice

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"hybrid_distributed_store/internal/config"
	"hybrid_distributed_store/internal/meta"
)

type mockHTTPClient struct {
	statusCode    int
	shouldFail    bool
	perHostStatus map[string]int
	perHostFail   map[string]bool
	perHostDelay  map[string]time.Duration
}

func (m *mockHTTPClient) Do(req *http.Request) (*http.Response, error) {
	host := req.URL.Host
	if m.perHostDelay != nil && m.perHostDelay[host] > 0 {
		select {
		case <-time.After(m.perHostDelay[host]):
		case <-req.Context().Done():
			return nil, req.Context().Err()
		}
	}
	if m.perHostFail != nil && m.perHostFail[host] {
		return nil, fmt.Errorf("mock network error")
	}
	if m.perHostStatus != nil {
		if status, ok := m.perHostStatus[host]; ok {
			return &http.Response{
				StatusCode: status,
				Body:       io.NopCloser(strings.NewReader("ok")),
				Header:     make(http.Header),
			}, nil
		}
	}
	if m.shouldFail {
		return nil, fmt.Errorf("mock network error")
	}
	return &http.Response{
		StatusCode: m.statusCode,
		Body:       io.NopCloser(strings.NewReader("ok")),
		Header:     make(http.Header),
	}, nil
}

type recordingHTTPClient struct {
	statusCode int
	mu         sync.Mutex
	urls       []string
}

func (m *recordingHTTPClient) Do(req *http.Request) (*http.Response, error) {
	m.mu.Lock()
	m.urls = append(m.urls, req.URL.String())
	m.mu.Unlock()
	return &http.Response{
		StatusCode: m.statusCode,
		Body:       io.NopCloser(strings.NewReader("ok")),
		Header:     make(http.Header),
	}, nil
}

type mockECDriver struct{}

func (m *mockECDriver) Split(data []byte) ([][]byte, error) { return nil, nil }
func (m *mockECDriver) Encode(shards [][]byte) error        { return nil }
func (m *mockECDriver) Reconstruct(shards [][]byte) error   { return nil }

type mockUtils struct{}

func (m *mockUtils) Serialize(data map[string]interface{}) ([]byte, error)   { return nil, nil }
func (m *mockUtils) Deserialize(data []byte) (map[string]interface{}, error) { return nil, nil }
func (m *mockUtils) MapsAreEqual(map1, map2 map[string]interface{}) bool     { return true }

func createMockService(httpClient *mockHTTPClient) *Service {
	return NewService(httpClient, &mockECDriver{}, &mockUtils{}, nil)
}

func TestWriteReplication_SuccessWhenMetadataDisabled(t *testing.T) {
	origMetaEnabled := config.MetaEnabled
	origWriteQuorum := config.HotWriteQuorum
	defer func() {
		config.MetaEnabled = origMetaEnabled
		config.HotWriteQuorum = origWriteQuorum
	}()
	config.MetaEnabled = false
	config.HotWriteQuorum = 1

	svc := createMockService(&mockHTTPClient{statusCode: http.StatusOK})
	_, err := svc.WriteReplication(context.Background(), []string{"http://node1"}, "k1", []byte("data"))
	if err != nil {
		t.Fatalf("WriteReplication() expected success, got error: %v", err)
	}
}

func TestWriteReplication_AcceptsNoContentStoreAck(t *testing.T) {
	origMetaEnabled := config.MetaEnabled
	origWriteQuorum := config.HotWriteQuorum
	defer func() {
		config.MetaEnabled = origMetaEnabled
		config.HotWriteQuorum = origWriteQuorum
	}()
	config.MetaEnabled = false
	config.HotWriteQuorum = 1

	svc := createMockService(&mockHTTPClient{statusCode: http.StatusNoContent})
	_, err := svc.WriteReplication(context.Background(), []string{"http://node1"}, "k1", []byte("data"))
	if err != nil {
		t.Fatalf("WriteReplication() expected success for 204 store ack, got error: %v", err)
	}
}

func TestWriteReplication_StorageFails(t *testing.T) {
	origMetaEnabled := config.MetaEnabled
	origWriteQuorum := config.HotWriteQuorum
	defer func() {
		config.MetaEnabled = origMetaEnabled
		config.HotWriteQuorum = origWriteQuorum
	}()
	config.MetaEnabled = false
	config.HotWriteQuorum = 1

	svc := createMockService(&mockHTTPClient{statusCode: http.StatusInternalServerError})
	_, err := svc.WriteReplication(context.Background(), []string{"http://node1"}, "k1", []byte("data"))
	if err == nil {
		t.Fatalf("WriteReplication() expected error when all replica writes fail")
	}
}

func TestWriteReplication_MetadataUnavailableWhenEnabled(t *testing.T) {
	origMetaEnabled := config.MetaEnabled
	origWriteQuorum := config.HotWriteQuorum
	defer func() {
		config.MetaEnabled = origMetaEnabled
		config.HotWriteQuorum = origWriteQuorum
	}()
	config.MetaEnabled = true
	config.HotWriteQuorum = 1

	svc := createMockService(&mockHTTPClient{statusCode: http.StatusOK})
	_, err := svc.WriteReplication(context.Background(), []string{"http://node1"}, "k1", []byte("data"))
	if err == nil {
		t.Fatalf("WriteReplication() expected error when metadata is enabled but store is nil")
	}
}

func TestWriteReplication_QuorumNotMet(t *testing.T) {
	origMetaEnabled := config.MetaEnabled
	origWriteQuorum := config.HotWriteQuorum
	defer func() {
		config.MetaEnabled = origMetaEnabled
		config.HotWriteQuorum = origWriteQuorum
	}()
	config.MetaEnabled = false
	config.HotWriteQuorum = 2

	svc := createMockService(&mockHTTPClient{
		perHostStatus: map[string]int{
			"node1": http.StatusOK,
			"node2": http.StatusInternalServerError,
			"node3": http.StatusInternalServerError,
		},
	})
	_, err := svc.WriteReplication(context.Background(), []string{"http://node1", "http://node2", "http://node3"}, "k1", []byte("data"))
	if err == nil {
		t.Fatalf("WriteReplication() expected error when quorum is not met")
	}
	if !strings.Contains(err.Error(), "quorum") {
		t.Fatalf("expected quorum error, got: %v", err)
	}
}

func TestWriteReplication_QuorumMetWithPartial(t *testing.T) {
	origMetaEnabled := config.MetaEnabled
	origWriteQuorum := config.HotWriteQuorum
	defer func() {
		config.MetaEnabled = origMetaEnabled
		config.HotWriteQuorum = origWriteQuorum
	}()
	config.MetaEnabled = false
	config.HotWriteQuorum = 2

	svc := createMockService(&mockHTTPClient{
		perHostStatus: map[string]int{
			"node1": http.StatusOK,
			"node2": http.StatusOK,
			"node3": http.StatusInternalServerError,
		},
	})
	out, err := svc.WriteReplication(context.Background(), []string{"http://node1", "http://node2", "http://node3"}, "k1", []byte("data"))
	if err != nil {
		t.Fatalf("WriteReplication() expected success, got error: %v", err)
	}
	if out == nil {
		t.Fatalf("WriteReplication() expected result map")
	}
	if out["partial"] != true {
		t.Fatalf("expected partial=true, got: %v", out["partial"])
	}
	if out["write_quorum"] != "2/3" {
		t.Fatalf("expected write_quorum=2/3, got: %v", out["write_quorum"])
	}
}

func TestWriteReplication_ReturnsAfterQuorumBeforeSlowReplica(t *testing.T) {
	origMetaEnabled := config.MetaEnabled
	origWriteQuorum := config.HotWriteQuorum
	defer func() {
		config.MetaEnabled = origMetaEnabled
		config.HotWriteQuorum = origWriteQuorum
	}()
	config.MetaEnabled = false
	config.HotWriteQuorum = 2

	svc := createMockService(&mockHTTPClient{
		statusCode: http.StatusOK,
		perHostDelay: map[string]time.Duration{
			"node3": 250 * time.Millisecond,
		},
	})

	start := time.Now()
	out, err := svc.WriteReplication(
		context.Background(),
		[]string{"http://node1", "http://node2", "http://node3"},
		"k1",
		[]byte("data"),
	)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("WriteReplication() expected success, got error: %v", err)
	}
	if elapsed >= 150*time.Millisecond {
		t.Fatalf("WriteReplication() should return after quorum without waiting for slow replica, elapsed=%s", elapsed)
	}
	if out["quorum_returned"] != true {
		t.Fatalf("expected quorum_returned=true, got: %v", out["quorum_returned"])
	}
	if out["all_replica_observed"] != false {
		t.Fatalf("expected all_replica_observed=false, got: %v", out["all_replica_observed"])
	}
	if out["foreground_writes_success"] != 2 {
		t.Fatalf("expected foreground_writes_success=2, got: %v", out["foreground_writes_success"])
	}
}

func TestWriteReplication_LateReplicaUpdatesMetadata(t *testing.T) {
	origMetaEnabled := config.MetaEnabled
	origWriteQuorum := config.HotWriteQuorum
	defer func() {
		config.MetaEnabled = origMetaEnabled
		config.HotWriteQuorum = origWriteQuorum
	}()
	config.MetaEnabled = true
	config.HotWriteQuorum = 2

	store, err := meta.NewTiKVStore(meta.Config{
		Enabled: true,
		DSN:     "memory://writeservice-late-replica",
	})
	if err != nil {
		t.Fatalf("new tikv store failed: %v", err)
	}
	defer store.Close()

	svc := NewService(&mockHTTPClient{
		statusCode: http.StatusOK,
		perHostDelay: map[string]time.Duration{
			"node3": 30 * time.Millisecond,
		},
	}, &mockECDriver{}, &mockUtils{}, store)

	if _, err := svc.WriteReplication(
		context.Background(),
		[]string{"http://node1", "http://node2", "http://node3"},
		"obj-late-replica",
		[]byte("payload"),
	); err != nil {
		t.Fatalf("write replication failed: %v", err)
	}

	deadline := time.Now().Add(500 * time.Millisecond)
	for {
		view, err := store.GetObjectAdminView(context.Background(), "obj-late-replica")
		if err != nil {
			t.Fatalf("get object admin view failed: %v", err)
		}
		if view != nil && len(view.ReplicaLocations) == 3 {
			return
		}
		if time.Now().After(deadline) {
			if view == nil {
				t.Fatalf("expected object admin view")
			}
			t.Fatalf("expected late replica metadata to reach 3 placements, got=%d", len(view.ReplicaLocations))
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestWriteReplication_NoDirectTaskEnqueueOnWrite(t *testing.T) {
	origMetaEnabled := config.MetaEnabled
	origWriteQuorum := config.HotWriteQuorum
	defer func() {
		config.MetaEnabled = origMetaEnabled
		config.HotWriteQuorum = origWriteQuorum
	}()
	config.MetaEnabled = true
	config.HotWriteQuorum = 2

	store, err := meta.NewTiKVStore(meta.Config{
		Enabled: true,
		DSN:     "memory://writeservice-partial",
	})
	if err != nil {
		t.Fatalf("new tikv store failed: %v", err)
	}
	defer store.Close()

	svc := NewService(&mockHTTPClient{
		perHostStatus: map[string]int{
			"node1": http.StatusOK,
			"node2": http.StatusOK,
			"node3": http.StatusInternalServerError,
		},
	}, &mockECDriver{}, &mockUtils{}, store)

	if _, err := svc.WriteReplication(
		context.Background(),
		[]string{"http://node1", "http://node2", "http://node3"},
		"obj-partial",
		[]byte("payload"),
	); err != nil {
		t.Fatalf("write replication failed: %v", err)
	}

	view, err := store.GetObjectAdminView(context.Background(), "obj-partial")
	if err != nil {
		t.Fatalf("get object admin view failed: %v", err)
	}
	if view == nil {
		t.Fatalf("expected object admin view")
	}
	if view.CurrentVersion <= 0 {
		t.Fatalf("expected current version > 0, got=%d", view.CurrentVersion)
	}

	tasks, err := store.ListTieringTasks(context.Background(), "", "", 20)
	if err != nil {
		t.Fatalf("list tiering tasks failed: %v", err)
	}
	if len(tasks) != 0 {
		t.Fatalf("expected no direct tiering tasks on foreground write, got=%d", len(tasks))
	}
}

func TestWriteReplication_EscapesSpecialKey(t *testing.T) {
	origMetaEnabled := config.MetaEnabled
	origWriteQuorum := config.HotWriteQuorum
	defer func() {
		config.MetaEnabled = origMetaEnabled
		config.HotWriteQuorum = origWriteQuorum
	}()
	config.MetaEnabled = false
	config.HotWriteQuorum = 1

	rec := &recordingHTTPClient{statusCode: http.StatusOK}
	svc := createMockService(&mockHTTPClient{statusCode: http.StatusOK})
	svc.http = rec

	key := "a&b"
	if _, err := svc.WriteReplication(context.Background(), []string{"http://node1"}, key, []byte("data")); err != nil {
		t.Fatalf("WriteReplication() expected success, got error: %v", err)
	}

	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.urls) != 1 {
		t.Fatalf("expected exactly 1 outbound write request, got %d", len(rec.urls))
	}
	if !strings.Contains(rec.urls[0], "/store?key=hot%2Fa%26b%2F") {
		t.Fatalf("expected escaped versioned hot key in request URL, got: %s", rec.urls[0])
	}
}
