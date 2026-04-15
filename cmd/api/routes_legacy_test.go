package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func newLegacyTestRouter(deps legacyRouteDeps) *gin.Engine {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	registerLegacyRoutes(router, deps)
	return router
}

func baseLegacyDeps() legacyRouteDeps {
	return legacyRouteDeps{
		getDynamicNodes: func(c *gin.Context) ([]string, []string, error) {
			return []string{"n1", "n2", "n3"}, []string{"n1", "n2", "n3", "n4", "n5", "n6"}, nil
		},
		loadMetadata: func(ctx context.Context, key string) (map[string]interface{}, string, error) {
			return nil, "", errors.New("not implemented")
		},
		writeReplication: func(ctx context.Context, replicaNodes []string, key string, value []byte) (map[string]interface{}, error) {
			return map[string]interface{}{}, nil
		},
		writeEC: func(ctx context.Context, ecNodes []string, key string, value []byte) (map[string]interface{}, error) {
			return map[string]interface{}{}, nil
		},
		serialize: func(data map[string]interface{}) ([]byte, error) { return []byte(`{"ok":1}`), nil },
		deserialize: func(data []byte) (map[string]interface{}, error) {
			return map[string]interface{}{"ok": 1}, nil
		},
		readReplication: func(ctx context.Context, replicaNodes []string, key string) ([]byte, error) { return nil, nil },
		readEC: func(ctx context.Context, ecNodes []string, metadata map[string]interface{}) ([]byte, error) {
			return nil, nil
		},
		deleteReplication: func(ctx context.Context, replicaNodes []string, key string) (int, error) {
			return 1, nil
		},
		deleteEC: func(ctx context.Context, ecNodes []string, metadata map[string]interface{}) (int, error) {
			return 1, nil
		},
		deleteNormalizedMetadata: func(ctx context.Context, key string) error { return nil },
		getActiveNodes:           func() []string { return nil },
	}
}

func TestLegacyWrite_UnknownStrategyRejected(t *testing.T) {
	deps := baseLegacyDeps()
	router := newLegacyTestRouter(deps)

	req := httptest.NewRequest(http.MethodPost, "/write?key=k1&strategy=unknown_strategy", strings.NewReader(`{"a":1}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected %d got %d body=%s", http.StatusUnprocessableEntity, rec.Code, rec.Body.String())
	}
}

func TestLegacyWrite_ECRejected(t *testing.T) {
	deps := baseLegacyDeps()
	router := newLegacyTestRouter(deps)

	req := httptest.NewRequest(http.MethodPost, "/write?key=k1&strategy=ec", strings.NewReader(`{"a":1}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected %d got %d body=%s", http.StatusUnprocessableEntity, rec.Code, rec.Body.String())
	}
}

func TestLegacyReadAndDelete_UnknownStrategyRejected(t *testing.T) {
	t.Run("read", func(t *testing.T) {
		deps := baseLegacyDeps()
		deps.loadMetadata = func(ctx context.Context, key string) (map[string]interface{}, string, error) {
			return map[string]interface{}{"strategy": "unknown_strategy"}, "normalized_metadata", nil
		}
		router := newLegacyTestRouter(deps)

		req := httptest.NewRequest(http.MethodGet, "/read/k1", nil)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusConflict {
			t.Fatalf("expected %d got %d body=%s", http.StatusConflict, rec.Code, rec.Body.String())
		}
	})

	t.Run("delete", func(t *testing.T) {
		deps := baseLegacyDeps()
		deps.loadMetadata = func(ctx context.Context, key string) (map[string]interface{}, string, error) {
			return map[string]interface{}{"strategy": "unknown_strategy"}, "normalized_metadata", nil
		}
		router := newLegacyTestRouter(deps)

		req := httptest.NewRequest(http.MethodDelete, "/delete/k1", nil)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusConflict {
			t.Fatalf("expected %d got %d body=%s", http.StatusConflict, rec.Code, rec.Body.String())
		}
	})
}

func TestLegacyReadDelete_UseHotKeyFromMetadata(t *testing.T) {
	deps := baseLegacyDeps()
	const hotKey = "hot/k1/00000000000000000099"
	var gotReadKey string
	var gotDeleteKey string

	deps.loadMetadata = func(ctx context.Context, key string) (map[string]interface{}, string, error) {
		return map[string]interface{}{
			"strategy": string("replication"),
			"hot_key":  hotKey,
		}, "normalized_metadata", nil
	}
	deps.readReplication = func(ctx context.Context, replicaNodes []string, key string) ([]byte, error) {
		gotReadKey = key
		return []byte(`{"ok":1}`), nil
	}
	deps.deserialize = func(data []byte) (map[string]interface{}, error) {
		return map[string]interface{}{"ok": 1}, nil
	}
	deps.deleteReplication = func(ctx context.Context, replicaNodes []string, key string) (int, error) {
		gotDeleteKey = key
		return 1, nil
	}

	router := newLegacyTestRouter(deps)

	readReq := httptest.NewRequest(http.MethodGet, "/read/k1", nil)
	readRec := httptest.NewRecorder()
	router.ServeHTTP(readRec, readReq)
	if readRec.Code != http.StatusOK {
		t.Fatalf("expected %d got %d body=%s", http.StatusOK, readRec.Code, readRec.Body.String())
	}
	if gotReadKey != hotKey {
		t.Fatalf("expected read hot_key %q got %q", hotKey, gotReadKey)
	}

	delReq := httptest.NewRequest(http.MethodDelete, "/delete/k1", nil)
	delRec := httptest.NewRecorder()
	router.ServeHTTP(delRec, delReq)
	if delRec.Code != http.StatusOK {
		t.Fatalf("expected %d got %d body=%s", http.StatusOK, delRec.Code, delRec.Body.String())
	}
	if gotDeleteKey != hotKey {
		t.Fatalf("expected delete hot_key %q got %q", hotKey, gotDeleteKey)
	}
}
