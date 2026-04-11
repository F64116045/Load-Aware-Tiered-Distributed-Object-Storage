package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func newTestRouter(t *testing.T) (*gin.Engine, *storageEngine) {
	t.Helper()
	gin.SetMode(gin.TestMode)

	storage := newStorageEngine("19001", "test-node", t.TempDir())
	router := gin.New()
	registerRoutes(router, storage)
	return router, storage
}

func TestRoutesHealth(t *testing.T) {
	t.Parallel()
	router, _ := newTestRouter(t)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"status":"healthy"`) {
		t.Fatalf("health response missing healthy status: %s", rec.Body.String())
	}
}

func TestRoutesStoreRetrieveDeleteRoundTrip(t *testing.T) {
	t.Parallel()
	router, _ := newTestRouter(t)

	putReq := httptest.NewRequest(http.MethodPost, "/store?key=test-key", strings.NewReader("hello-bytes"))
	putRec := httptest.NewRecorder()
	router.ServeHTTP(putRec, putReq)
	if putRec.Code != http.StatusOK {
		t.Fatalf("store status=%d body=%s", putRec.Code, putRec.Body.String())
	}

	getReq := httptest.NewRequest(http.MethodGet, "/retrieve/test-key", nil)
	getRec := httptest.NewRecorder()
	router.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("retrieve status=%d body=%s", getRec.Code, getRec.Body.String())
	}
	if got := getRec.Body.String(); got != "hello-bytes" {
		t.Fatalf("retrieve payload mismatch: got=%q", got)
	}

	headReq := httptest.NewRequest(http.MethodHead, "/retrieve/test-key", nil)
	headRec := httptest.NewRecorder()
	router.ServeHTTP(headRec, headReq)
	if headRec.Code != http.StatusOK {
		t.Fatalf("head status=%d", headRec.Code)
	}
	if body, _ := io.ReadAll(headRec.Body); len(body) != 0 {
		t.Fatalf("head should not return body, got=%q", string(body))
	}

	delReq := httptest.NewRequest(http.MethodDelete, "/delete/test-key", nil)
	delRec := httptest.NewRecorder()
	router.ServeHTTP(delRec, delReq)
	if delRec.Code != http.StatusOK {
		t.Fatalf("delete status=%d body=%s", delRec.Code, delRec.Body.String())
	}

	getAfterReq := httptest.NewRequest(http.MethodGet, "/retrieve/test-key", nil)
	getAfterRec := httptest.NewRecorder()
	router.ServeHTTP(getAfterRec, getAfterReq)
	if getAfterRec.Code != http.StatusNotFound {
		t.Fatalf("retrieve-after-delete status=%d want=%d body=%s", getAfterRec.Code, http.StatusNotFound, getAfterRec.Body.String())
	}
}
