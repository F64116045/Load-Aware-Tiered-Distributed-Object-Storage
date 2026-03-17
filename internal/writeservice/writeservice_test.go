package writeservice

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"hybrid_distributed_store/internal/config"
)

type mockHTTPClient struct {
	statusCode int
	shouldFail bool
}

func (m *mockHTTPClient) Do(req *http.Request) (*http.Response, error) {
	if m.shouldFail {
		return nil, fmt.Errorf("mock network error")
	}
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
	defer func() { config.MetaEnabled = origMetaEnabled }()
	config.MetaEnabled = false

	svc := createMockService(&mockHTTPClient{statusCode: http.StatusOK})
	_, err := svc.WriteReplication(context.Background(), []string{"http://node1"}, "k1", []byte("data"))
	if err != nil {
		t.Fatalf("WriteReplication() expected success, got error: %v", err)
	}
}

func TestWriteReplication_StorageFails(t *testing.T) {
	origMetaEnabled := config.MetaEnabled
	defer func() { config.MetaEnabled = origMetaEnabled }()
	config.MetaEnabled = false

	svc := createMockService(&mockHTTPClient{statusCode: http.StatusInternalServerError})
	_, err := svc.WriteReplication(context.Background(), []string{"http://node1"}, "k1", []byte("data"))
	if err == nil {
		t.Fatalf("WriteReplication() expected error when all replica writes fail")
	}
}

func TestWriteReplication_MetadataUnavailableWhenEnabled(t *testing.T) {
	origMetaEnabled := config.MetaEnabled
	defer func() { config.MetaEnabled = origMetaEnabled }()
	config.MetaEnabled = true

	svc := createMockService(&mockHTTPClient{statusCode: http.StatusOK})
	_, err := svc.WriteReplication(context.Background(), []string{"http://node1"}, "k1", []byte("data"))
	if err == nil {
		t.Fatalf("WriteReplication() expected error when metadata is enabled but store is nil")
	}
}
