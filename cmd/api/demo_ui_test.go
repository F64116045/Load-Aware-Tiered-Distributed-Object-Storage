package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestDemoUIRoutesServeIndex(t *testing.T) {
	t.Parallel()
	gin.SetMode(gin.TestMode)

	router := gin.New()
	registerDemoUIRoutes(router)

	req := httptest.NewRequest(http.MethodGet, "/demo/", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d want=%d", rec.Code, http.StatusOK)
	}
	if !strings.Contains(rec.Body.String(), "Hybrid Object Storage Demo") {
		t.Fatalf("missing ui title in response")
	}
}

func TestDemoUIRoutesServeAssets(t *testing.T) {
	t.Parallel()
	gin.SetMode(gin.TestMode)

	router := gin.New()
	registerDemoUIRoutes(router)

	req := httptest.NewRequest(http.MethodGet, "/demo/assets/app.js", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d want=%d", rec.Code, http.StatusOK)
	}
	if !strings.Contains(rec.Body.String(), "const logPanel") {
		t.Fatalf("expected app.js content")
	}
}
