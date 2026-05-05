package storageops

import (
	"context"
	"io"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"testing"

	"hybrid_distributed_store/internal/interfaces"
)

// --- 1. Mocks Definitions ---

// MockHttpClient simulates an HTTP client that records called URLs.
type MockHttpClient struct {
	CalledURLs map[string]int
	mu         sync.RWMutex
}

// NewMockHttpClient creates a new instance of the mock client.
func NewMockHttpClient() *MockHttpClient {
	return &MockHttpClient{
		CalledURLs: make(map[string]int),
	}
}

// Do implements the IHttpClient interface.
// It records the URL and returns a 200 OK response.
func (m *MockHttpClient) Do(req *http.Request) (*http.Response, error) {
	m.mu.Lock()
	m.CalledURLs[req.URL.String()]++
	m.mu.Unlock()

	// Simulate success
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader("")),
		Header:     make(http.Header),
	}, nil
}

// createMockService helper to inject the mock client into the service.
func createMockService(http interfaces.IHttpClient) *Service {
	// Ensure *Service satisfies the interface
	var _ interfaces.IStorageOps = (*Service)(nil)

	return &Service{
		http: http,
	}
}

// --- 2. Test Cases ---

func TestDeleteReplication(t *testing.T) {
	// --- 1. Arrange ---
	mockHttp := NewMockHttpClient()
	svc := createMockService(mockHttp)

	nodes := []string{"http://n1", "http://n2", "http://n3"}
	key := "test_rep_key"
	ctx := context.Background()

	// --- 2. Act ---
	count, err := svc.DeleteReplication(ctx, nodes, key)

	// --- 3. Assert ---
	if err != nil {
		t.Fatalf("DeleteReplication() expected success, got error: %v", err)
	}
	if count != 3 {
		t.Errorf("Expected successCount 3, got %d", count)
	}

	expectedURLs := map[string]int{
		"http://n1/delete/test_rep_key": 1,
		"http://n2/delete/test_rep_key": 1,
		"http://n3/delete/test_rep_key": 1,
	}
	if !reflect.DeepEqual(mockHttp.CalledURLs, expectedURLs) {
		t.Errorf("URL call mismatch:\nExpected: %v\nActual:   %v", expectedURLs, mockHttp.CalledURLs)
	}
}

func TestDeleteEC(t *testing.T) {
	// --- 1. Arrange ---
	mockHttp := NewMockHttpClient()
	svc := createMockService(mockHttp)

	nodes := []string{"http://n1", "http://n2", "http://n3", "http://n4", "http://n5", "http://n6"}
	metadata := map[string]interface{}{
		"key_name":     "test_ec_key",
		"chunk_prefix": "my_ec_prefix_",
	}
	ctx := context.Background()

	// --- 2. Act ---
	count, err := svc.DeleteEC(ctx, nodes, metadata)

	// --- 3. Assert ---
	if err != nil {
		t.Fatalf("DeleteEC() expected success, got error: %v", err)
	}
	if count != 6 {
		t.Errorf("Expected successCount 6, got %d", count)
	}

	expectedURLs := map[string]int{
		"http://n1/delete/my_ec_prefix_0": 1,
		"http://n2/delete/my_ec_prefix_1": 1,
		"http://n3/delete/my_ec_prefix_2": 1,
		"http://n4/delete/my_ec_prefix_3": 1,
		"http://n5/delete/my_ec_prefix_4": 1,
		"http://n6/delete/my_ec_prefix_5": 1,
	}
	if !reflect.DeepEqual(mockHttp.CalledURLs, expectedURLs) {
		t.Errorf("URL call mismatch:\nExpected: %v\nActual:   %v", expectedURLs, mockHttp.CalledURLs)
	}
}

func TestDeleteEC_UsesShardPlacements(t *testing.T) {
	mockHttp := NewMockHttpClient()
	svc := createMockService(mockHttp)

	nodes := []string{"http://fallback1", "http://fallback2"}
	metadata := map[string]interface{}{
		"key_name":     "test_ec_key",
		"chunk_prefix": "fallback_prefix_",
		"ec_shards": []map[string]interface{}{
			{"shard_index": 0, "node_id": "http://n4", "path": "placed_chunk_0", "status": "ACTIVE"},
			{"shard_index": 1, "node_id": "http://n2", "path": "placed_chunk_1", "status": "ACTIVE"},
			{"shard_index": 2, "node_id": "http://n6", "path": "placed_chunk_2", "status": "ACTIVE"},
			{"shard_index": 3, "node_id": "http://n1", "path": "placed_chunk_3", "status": "ACTIVE"},
			{"shard_index": 4, "node_id": "http://n5", "path": "placed_chunk_4", "status": "ACTIVE"},
			{"shard_index": 5, "node_id": "http://n3", "path": "placed_chunk_5", "status": "ACTIVE"},
			{"shard_index": 6, "node_id": "http://n7", "path": "inactive_chunk", "status": "DELETED"},
		},
	}

	count, err := svc.DeleteEC(context.Background(), nodes, metadata)
	if err != nil {
		t.Fatalf("DeleteEC() expected success with placements, got error: %v", err)
	}
	if count != 6 {
		t.Fatalf("expected successCount 6, got %d", count)
	}

	expectedURLs := map[string]int{
		"http://n4/delete/placed_chunk_0": 1,
		"http://n2/delete/placed_chunk_1": 1,
		"http://n6/delete/placed_chunk_2": 1,
		"http://n1/delete/placed_chunk_3": 1,
		"http://n5/delete/placed_chunk_4": 1,
		"http://n3/delete/placed_chunk_5": 1,
	}
	if !reflect.DeepEqual(mockHttp.CalledURLs, expectedURLs) {
		t.Errorf("URL call mismatch:\nExpected: %v\nActual:   %v", expectedURLs, mockHttp.CalledURLs)
	}
}

func TestDeleteReplication_EscapesSpecialKey(t *testing.T) {
	mockHttp := NewMockHttpClient()
	svc := createMockService(mockHttp)

	nodes := []string{"http://n1"}
	key := "a/b"
	ctx := context.Background()

	count, err := svc.DeleteReplication(ctx, nodes, key)
	if err != nil {
		t.Fatalf("DeleteReplication() expected success, got error: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected successCount 1, got %d", count)
	}

	expectedURLs := map[string]int{
		"http://n1/delete/a%2Fb": 1,
	}
	if !reflect.DeepEqual(mockHttp.CalledURLs, expectedURLs) {
		t.Errorf("URL call mismatch:\nExpected: %v\nActual:   %v", expectedURLs, mockHttp.CalledURLs)
	}
}
