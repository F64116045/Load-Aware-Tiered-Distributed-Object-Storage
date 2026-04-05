package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"hybrid_distributed_store/internal/config"
)

func newV2ObjectsTestRouter(deps v2ObjectRouteDeps) *gin.Engine {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	registerV2ObjectRoutes(router, deps)
	return router
}

func TestV2PutObject_DefaultContentType(t *testing.T) {
	var gotKey string
	var gotData []byte
	var gotContentType string

	router := newV2ObjectsTestRouter(v2ObjectRouteDeps{
		getDynamicNodes: func(c *gin.Context) ([]string, []string, error) {
			return []string{"n1", "n2", "n3"}, []string{"n1", "n2", "n3", "n4", "n5", "n6"}, nil
		},
		writeReplicationWithMetadata: func(ctx context.Context, replicaNodes []string, key string, data []byte, metadata map[string]interface{}) (map[string]interface{}, error) {
			gotKey = key
			gotData = append([]byte(nil), data...)
			if ct, ok := metadata["content_type"].(string); ok {
				gotContentType = ct
			}
			return map[string]interface{}{"write_quorum": "2/3"}, nil
		},
		loadMetadata:    func(ctx context.Context, key string) (map[string]interface{}, string, error) { return nil, "", nil },
		readReplication: func(ctx context.Context, replicaNodes []string, key string) ([]byte, error) { return nil, nil },
		readEC: func(ctx context.Context, ecNodes []string, metadata map[string]interface{}) ([]byte, error) {
			return nil, nil
		},
		now: func() time.Time { return time.Unix(100, 0) },
	})

	req := httptest.NewRequest(http.MethodPut, "/v2/objects/o1", strings.NewReader("abc"))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected %d, got %d, body=%s", http.StatusCreated, rec.Code, rec.Body.String())
	}
	if gotKey != "o1" {
		t.Fatalf("expected key o1, got %q", gotKey)
	}
	if string(gotData) != "abc" {
		t.Fatalf("expected data abc, got %q", string(gotData))
	}
	if gotContentType != "application/octet-stream" {
		t.Fatalf("expected default content_type application/octet-stream, got %q", gotContentType)
	}

	var resp map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp["object_id"] != "o1" {
		t.Fatalf("expected object_id o1, got %v", resp["object_id"])
	}
	if resp["strategy"] != string(config.StrategyReplication) {
		t.Fatalf("expected strategy %s, got %v", config.StrategyReplication, resp["strategy"])
	}
	if resp["content_type"] != "application/octet-stream" {
		t.Fatalf("expected content_type application/octet-stream, got %v", resp["content_type"])
	}
}

func TestV2GetObject_ReplicationAndEC(t *testing.T) {
	t.Run("replication", func(t *testing.T) {
		router := newV2ObjectsTestRouter(v2ObjectRouteDeps{
			getDynamicNodes: func(c *gin.Context) ([]string, []string, error) {
				return []string{"n1", "n2", "n3"}, []string{"n1", "n2", "n3", "n4", "n5", "n6"}, nil
			},
			writeReplicationWithMetadata: func(ctx context.Context, replicaNodes []string, key string, data []byte, metadata map[string]interface{}) (map[string]interface{}, error) {
				return nil, nil
			},
			loadMetadata: func(ctx context.Context, key string) (map[string]interface{}, string, error) {
				return map[string]interface{}{
					"strategy":     string(config.StrategyReplication),
					"content_type": "image/png",
				}, "postgres_normalized", nil
			},
			readReplication: func(ctx context.Context, replicaNodes []string, key string) ([]byte, error) {
				return []byte("replica-data"), nil
			},
			readEC: func(ctx context.Context, ecNodes []string, metadata map[string]interface{}) ([]byte, error) {
				return nil, nil
			},
			now: time.Now,
		})

		req := httptest.NewRequest(http.MethodGet, "/v2/objects/o2", nil)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected %d, got %d, body=%s", http.StatusOK, rec.Code, rec.Body.String())
		}
		if got := rec.Header().Get("Content-Type"); !strings.HasPrefix(got, "image/png") {
			t.Fatalf("expected content-type image/png, got %q", got)
		}
		if rec.Body.String() != "replica-data" {
			t.Fatalf("expected body replica-data, got %q", rec.Body.String())
		}
	})

	t.Run("ec", func(t *testing.T) {
		router := newV2ObjectsTestRouter(v2ObjectRouteDeps{
			getDynamicNodes: func(c *gin.Context) ([]string, []string, error) {
				return []string{"n1", "n2", "n3"}, []string{"n1", "n2", "n3", "n4", "n5", "n6"}, nil
			},
			writeReplicationWithMetadata: func(ctx context.Context, replicaNodes []string, key string, data []byte, metadata map[string]interface{}) (map[string]interface{}, error) {
				return nil, nil
			},
			loadMetadata: func(ctx context.Context, key string) (map[string]interface{}, string, error) {
				return map[string]interface{}{
					"strategy": string(config.StrategyEC),
				}, "postgres_normalized", nil
			},
			readReplication: func(ctx context.Context, replicaNodes []string, key string) ([]byte, error) {
				return nil, nil
			},
			readEC: func(ctx context.Context, ecNodes []string, metadata map[string]interface{}) ([]byte, error) {
				return []byte("ec-data"), nil
			},
			now: time.Now,
		})

		req := httptest.NewRequest(http.MethodGet, "/v2/objects/o3", nil)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected %d, got %d, body=%s", http.StatusOK, rec.Code, rec.Body.String())
		}
		if got := rec.Header().Get("Content-Type"); !strings.HasPrefix(got, "application/octet-stream") {
			t.Fatalf("expected default content-type application/octet-stream, got %q", got)
		}
		if rec.Body.String() != "ec-data" {
			t.Fatalf("expected body ec-data, got %q", rec.Body.String())
		}
	})
}

func TestV2GetObject_MetadataNotFoundAndConflict(t *testing.T) {
	t.Run("metadata_not_found", func(t *testing.T) {
		router := newV2ObjectsTestRouter(v2ObjectRouteDeps{
			getDynamicNodes: func(c *gin.Context) ([]string, []string, error) {
				return []string{"n1", "n2", "n3"}, []string{"n1", "n2", "n3", "n4", "n5", "n6"}, nil
			},
			writeReplicationWithMetadata: func(ctx context.Context, replicaNodes []string, key string, data []byte, metadata map[string]interface{}) (map[string]interface{}, error) {
				return nil, nil
			},
			loadMetadata: func(ctx context.Context, key string) (map[string]interface{}, string, error) {
				return nil, "", errMetadataNotFound
			},
			readReplication: func(ctx context.Context, replicaNodes []string, key string) ([]byte, error) {
				return nil, nil
			},
			readEC: func(ctx context.Context, ecNodes []string, metadata map[string]interface{}) ([]byte, error) {
				return nil, nil
			},
			now: time.Now,
		})

		req := httptest.NewRequest(http.MethodGet, "/v2/objects/missing", nil)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusNotFound {
			t.Fatalf("expected %d, got %d, body=%s", http.StatusNotFound, rec.Code, rec.Body.String())
		}
	})

	t.Run("strategy_conflict", func(t *testing.T) {
		router := newV2ObjectsTestRouter(v2ObjectRouteDeps{
			getDynamicNodes: func(c *gin.Context) ([]string, []string, error) {
				return []string{"n1", "n2", "n3"}, []string{"n1", "n2", "n3", "n4", "n5", "n6"}, nil
			},
			writeReplicationWithMetadata: func(ctx context.Context, replicaNodes []string, key string, data []byte, metadata map[string]interface{}) (map[string]interface{}, error) {
				return nil, nil
			},
			loadMetadata: func(ctx context.Context, key string) (map[string]interface{}, string, error) {
				return map[string]interface{}{"strategy": "field_hybrid"}, "postgres_normalized", nil
			},
			readReplication: func(ctx context.Context, replicaNodes []string, key string) ([]byte, error) {
				return nil, errors.New("should not be called")
			},
			readEC: func(ctx context.Context, ecNodes []string, metadata map[string]interface{}) ([]byte, error) {
				return nil, errors.New("should not be called")
			},
			now: time.Now,
		})

		req := httptest.NewRequest(http.MethodGet, "/v2/objects/conflict", nil)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusConflict {
			t.Fatalf("expected %d, got %d, body=%s", http.StatusConflict, rec.Code, rec.Body.String())
		}
	})
}

func TestV2Object_ErrorPaths(t *testing.T) {
	t.Run("put_write_error_returns_500", func(t *testing.T) {
		router := newV2ObjectsTestRouter(v2ObjectRouteDeps{
			getDynamicNodes: func(c *gin.Context) ([]string, []string, error) {
				return []string{"n1", "n2", "n3"}, []string{"n1", "n2", "n3", "n4", "n5", "n6"}, nil
			},
			writeReplicationWithMetadata: func(ctx context.Context, replicaNodes []string, key string, data []byte, metadata map[string]interface{}) (map[string]interface{}, error) {
				return nil, errors.New("write failed")
			},
			loadMetadata:    func(ctx context.Context, key string) (map[string]interface{}, string, error) { return nil, "", nil },
			readReplication: func(ctx context.Context, replicaNodes []string, key string) ([]byte, error) { return nil, nil },
			readEC: func(ctx context.Context, ecNodes []string, metadata map[string]interface{}) ([]byte, error) {
				return nil, nil
			},
			now: time.Now,
		})

		req := httptest.NewRequest(http.MethodPut, "/v2/objects/o4", strings.NewReader("abc"))
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusInternalServerError {
			t.Fatalf("expected %d, got %d, body=%s", http.StatusInternalServerError, rec.Code, rec.Body.String())
		}
	})

	t.Run("get_metadata_internal_error_returns_500", func(t *testing.T) {
		router := newV2ObjectsTestRouter(v2ObjectRouteDeps{
			getDynamicNodes: func(c *gin.Context) ([]string, []string, error) {
				return []string{"n1", "n2", "n3"}, []string{"n1", "n2", "n3", "n4", "n5", "n6"}, nil
			},
			writeReplicationWithMetadata: func(ctx context.Context, replicaNodes []string, key string, data []byte, metadata map[string]interface{}) (map[string]interface{}, error) {
				return nil, nil
			},
			loadMetadata: func(ctx context.Context, key string) (map[string]interface{}, string, error) {
				return nil, "", errors.New("db down")
			},
			readReplication: func(ctx context.Context, replicaNodes []string, key string) ([]byte, error) {
				return nil, nil
			},
			readEC: func(ctx context.Context, ecNodes []string, metadata map[string]interface{}) ([]byte, error) {
				return nil, nil
			},
			now: time.Now,
		})

		req := httptest.NewRequest(http.MethodGet, "/v2/objects/o5", nil)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusInternalServerError {
			t.Fatalf("expected %d, got %d, body=%s", http.StatusInternalServerError, rec.Code, rec.Body.String())
		}
	})

	t.Run("get_replication_read_error_returns_404", func(t *testing.T) {
		router := newV2ObjectsTestRouter(v2ObjectRouteDeps{
			getDynamicNodes: func(c *gin.Context) ([]string, []string, error) {
				return []string{"n1", "n2", "n3"}, []string{"n1", "n2", "n3", "n4", "n5", "n6"}, nil
			},
			writeReplicationWithMetadata: func(ctx context.Context, replicaNodes []string, key string, data []byte, metadata map[string]interface{}) (map[string]interface{}, error) {
				return nil, nil
			},
			loadMetadata: func(ctx context.Context, key string) (map[string]interface{}, string, error) {
				return map[string]interface{}{"strategy": string(config.StrategyReplication)}, "postgres_normalized", nil
			},
			readReplication: func(ctx context.Context, replicaNodes []string, key string) ([]byte, error) {
				return nil, errors.New("read failed")
			},
			readEC: func(ctx context.Context, ecNodes []string, metadata map[string]interface{}) ([]byte, error) {
				return nil, nil
			},
			now: time.Now,
		})

		req := httptest.NewRequest(http.MethodGet, "/v2/objects/o6", nil)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusNotFound {
			t.Fatalf("expected %d, got %d, body=%s", http.StatusNotFound, rec.Code, rec.Body.String())
		}
	})

	t.Run("get_ec_read_error_returns_404", func(t *testing.T) {
		router := newV2ObjectsTestRouter(v2ObjectRouteDeps{
			getDynamicNodes: func(c *gin.Context) ([]string, []string, error) {
				return []string{"n1", "n2", "n3"}, []string{"n1", "n2", "n3", "n4", "n5", "n6"}, nil
			},
			writeReplicationWithMetadata: func(ctx context.Context, replicaNodes []string, key string, data []byte, metadata map[string]interface{}) (map[string]interface{}, error) {
				return nil, nil
			},
			loadMetadata: func(ctx context.Context, key string) (map[string]interface{}, string, error) {
				return map[string]interface{}{"strategy": string(config.StrategyEC)}, "postgres_normalized", nil
			},
			readReplication: func(ctx context.Context, replicaNodes []string, key string) ([]byte, error) {
				return nil, nil
			},
			readEC: func(ctx context.Context, ecNodes []string, metadata map[string]interface{}) ([]byte, error) {
				return nil, errors.New("decode failed")
			},
			now: time.Now,
		})

		req := httptest.NewRequest(http.MethodGet, "/v2/objects/o7", nil)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusNotFound {
			t.Fatalf("expected %d, got %d, body=%s", http.StatusNotFound, rec.Code, rec.Body.String())
		}
	})
}

func TestV2DeleteObject_ReplicationAndEC(t *testing.T) {
	t.Run("replication", func(t *testing.T) {
		var deletedMetaKey string
		router := newV2ObjectsTestRouter(v2ObjectRouteDeps{
			getDynamicNodes: func(c *gin.Context) ([]string, []string, error) {
				return []string{"n1", "n2", "n3"}, []string{"n1", "n2", "n3", "n4", "n5", "n6"}, nil
			},
			writeReplicationWithMetadata: func(ctx context.Context, replicaNodes []string, key string, data []byte, metadata map[string]interface{}) (map[string]interface{}, error) {
				return nil, nil
			},
			loadMetadata: func(ctx context.Context, key string) (map[string]interface{}, string, error) {
				return map[string]interface{}{"strategy": string(config.StrategyReplication)}, "postgres_normalized", nil
			},
			readReplication: func(ctx context.Context, replicaNodes []string, key string) ([]byte, error) {
				return nil, nil
			},
			readEC: func(ctx context.Context, ecNodes []string, metadata map[string]interface{}) ([]byte, error) {
				return nil, nil
			},
			deleteReplication: func(ctx context.Context, replicaNodes []string, key string) (int, error) {
				if key != "d1" {
					t.Fatalf("unexpected delete key: %s", key)
				}
				return 3, nil
			},
			deleteEC: func(ctx context.Context, ecNodes []string, metadata map[string]interface{}) (int, error) {
				return 0, errors.New("should not be called")
			},
			deleteNormalizedMetadata: func(ctx context.Context, key string) error {
				deletedMetaKey = key
				return nil
			},
			now: time.Now,
		})

		req := httptest.NewRequest(http.MethodDelete, "/v2/objects/d1", nil)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected %d, got %d, body=%s", http.StatusOK, rec.Code, rec.Body.String())
		}
		if deletedMetaKey != "d1" {
			t.Fatalf("expected deleted metadata key d1, got %q", deletedMetaKey)
		}
	})

	t.Run("ec", func(t *testing.T) {
		router := newV2ObjectsTestRouter(v2ObjectRouteDeps{
			getDynamicNodes: func(c *gin.Context) ([]string, []string, error) {
				return []string{"n1", "n2", "n3"}, []string{"n1", "n2", "n3", "n4", "n5", "n6"}, nil
			},
			writeReplicationWithMetadata: func(ctx context.Context, replicaNodes []string, key string, data []byte, metadata map[string]interface{}) (map[string]interface{}, error) {
				return nil, nil
			},
			loadMetadata: func(ctx context.Context, key string) (map[string]interface{}, string, error) {
				return map[string]interface{}{
					"strategy":     string(config.StrategyEC),
					"chunk_prefix": "d2_cold_chunk_",
				}, "postgres_normalized", nil
			},
			readReplication: func(ctx context.Context, replicaNodes []string, key string) ([]byte, error) {
				return nil, nil
			},
			readEC: func(ctx context.Context, ecNodes []string, metadata map[string]interface{}) ([]byte, error) {
				return nil, nil
			},
			deleteReplication: func(ctx context.Context, replicaNodes []string, key string) (int, error) {
				return 0, errors.New("should not be called")
			},
			deleteEC: func(ctx context.Context, ecNodes []string, metadata map[string]interface{}) (int, error) {
				return 6, nil
			},
			deleteNormalizedMetadata: func(ctx context.Context, key string) error {
				return nil
			},
			now: time.Now,
		})

		req := httptest.NewRequest(http.MethodDelete, "/v2/objects/d2", nil)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected %d, got %d, body=%s", http.StatusOK, rec.Code, rec.Body.String())
		}
	})
}

func TestV2DeleteObject_ErrorPaths(t *testing.T) {
	t.Run("metadata_not_found", func(t *testing.T) {
		router := newV2ObjectsTestRouter(v2ObjectRouteDeps{
			getDynamicNodes: func(c *gin.Context) ([]string, []string, error) {
				return []string{"n1", "n2", "n3"}, []string{"n1", "n2", "n3", "n4", "n5", "n6"}, nil
			},
			writeReplicationWithMetadata: func(ctx context.Context, replicaNodes []string, key string, data []byte, metadata map[string]interface{}) (map[string]interface{}, error) {
				return nil, nil
			},
			loadMetadata: func(ctx context.Context, key string) (map[string]interface{}, string, error) {
				return nil, "", errMetadataNotFound
			},
			readReplication: func(ctx context.Context, replicaNodes []string, key string) ([]byte, error) {
				return nil, nil
			},
			readEC: func(ctx context.Context, ecNodes []string, metadata map[string]interface{}) ([]byte, error) {
				return nil, nil
			},
			deleteReplication: func(ctx context.Context, replicaNodes []string, key string) (int, error) {
				return 0, nil
			},
			deleteEC: func(ctx context.Context, ecNodes []string, metadata map[string]interface{}) (int, error) {
				return 0, nil
			},
			deleteNormalizedMetadata: func(ctx context.Context, key string) error {
				return nil
			},
			now: time.Now,
		})

		req := httptest.NewRequest(http.MethodDelete, "/v2/objects/notfound", nil)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusNotFound {
			t.Fatalf("expected %d, got %d, body=%s", http.StatusNotFound, rec.Code, rec.Body.String())
		}
	})

	t.Run("strategy_conflict", func(t *testing.T) {
		router := newV2ObjectsTestRouter(v2ObjectRouteDeps{
			getDynamicNodes: func(c *gin.Context) ([]string, []string, error) {
				return []string{"n1", "n2", "n3"}, []string{"n1", "n2", "n3", "n4", "n5", "n6"}, nil
			},
			writeReplicationWithMetadata: func(ctx context.Context, replicaNodes []string, key string, data []byte, metadata map[string]interface{}) (map[string]interface{}, error) {
				return nil, nil
			},
			loadMetadata: func(ctx context.Context, key string) (map[string]interface{}, string, error) {
				return map[string]interface{}{"strategy": "field_hybrid"}, "postgres_normalized", nil
			},
			readReplication: func(ctx context.Context, replicaNodes []string, key string) ([]byte, error) {
				return nil, nil
			},
			readEC: func(ctx context.Context, ecNodes []string, metadata map[string]interface{}) ([]byte, error) {
				return nil, nil
			},
			deleteReplication: func(ctx context.Context, replicaNodes []string, key string) (int, error) {
				return 0, nil
			},
			deleteEC: func(ctx context.Context, ecNodes []string, metadata map[string]interface{}) (int, error) {
				return 0, nil
			},
			deleteNormalizedMetadata: func(ctx context.Context, key string) error {
				return nil
			},
			now: time.Now,
		})

		req := httptest.NewRequest(http.MethodDelete, "/v2/objects/conflict", nil)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusConflict {
			t.Fatalf("expected %d, got %d, body=%s", http.StatusConflict, rec.Code, rec.Body.String())
		}
	})
}
