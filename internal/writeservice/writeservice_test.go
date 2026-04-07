package writeservice

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	"hybrid_distributed_store/internal/config"
	"hybrid_distributed_store/internal/meta"
)

type mockHTTPClient struct {
	statusCode    int
	shouldFail    bool
	perHostStatus map[string]int
	perHostFail   map[string]bool
}

func (m *mockHTTPClient) Do(req *http.Request) (*http.Response, error) {
	host := req.URL.Host
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

func TestWriteReplication_PartialEnqueuesRepairTask(t *testing.T) {
	origMetaEnabled := config.MetaEnabled
	origWriteQuorum := config.HotWriteQuorum
	origAgeThresholdSec := config.AgeThresholdSec
	defer func() {
		config.MetaEnabled = origMetaEnabled
		config.HotWriteQuorum = origWriteQuorum
		config.AgeThresholdSec = origAgeThresholdSec
	}()
	config.MetaEnabled = true
	config.HotWriteQuorum = 2
	config.AgeThresholdSec = 3600

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
	replTaskID := fmt.Sprintf("repl2ec:%s:%d", "obj-partial", view.CurrentVersion)
	repairTaskID := fmt.Sprintf("repair-repl:%s:%d", "obj-partial", view.CurrentVersion)

	tasks, err := store.ListTieringTasks(context.Background(), "", "", 20)
	if err != nil {
		t.Fatalf("list tiering tasks failed: %v", err)
	}
	foundRepl := false
	foundRepair := false
	for _, task := range tasks {
		if task.TaskID == replTaskID && task.TaskType == "REPL_TO_EC" {
			foundRepl = true
		}
		if task.TaskID == repairTaskID && task.TaskType == "REPAIR" {
			foundRepair = true
		}
	}
	if !foundRepl {
		t.Fatalf("expected repl-to-ec task %s", replTaskID)
	}
	if !foundRepair {
		t.Fatalf("expected repair task %s", repairTaskID)
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
	if !strings.Contains(rec.urls[0], "/store?key=a%26b") {
		t.Fatalf("expected escaped key in request URL, got: %s", rec.urls[0])
	}
}
