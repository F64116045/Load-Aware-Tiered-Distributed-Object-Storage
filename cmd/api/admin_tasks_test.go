package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"hybrid_distributed_store/internal/meta"
)

func newAdminTasksTestRouter(deps adminTaskRouteDeps) *gin.Engine {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	registerAdminTaskRoutes(router, deps)
	return router
}

func TestAdminTasksList_Success(t *testing.T) {
	now := time.Unix(1234, 0)
	router := newAdminTasksTestRouter(adminTaskRouteDeps{
		metadataAvailable: func() bool { return true },
		listTasks: func(ctx context.Context, state, taskType string, limit int) ([]meta.TieringTask, error) {
			if state != "PENDING" || taskType != "REPL_TO_EC" || limit != 10 {
				t.Fatalf("unexpected filters state=%q taskType=%q limit=%d", state, taskType, limit)
			}
			return []meta.TieringTask{
				{
					TaskID:      "t1",
					ObjectID:    "o1",
					Version:     1,
					TaskType:    "REPL_TO_EC",
					TaskState:   "PENDING",
					Priority:    100,
					RetryCount:  0,
					LastError:   sql.NullString{},
					ScheduledAt: now,
				},
				{
					TaskID:      "t2",
					ObjectID:    "o2",
					Version:     2,
					TaskType:    "REPL_TO_EC",
					TaskState:   "DONE",
					Priority:    50,
					RetryCount:  1,
					LastError:   sql.NullString{Valid: true, String: "x"},
					ScheduledAt: now,
				},
			}, nil
		},
		listStateCounts: func(ctx context.Context, taskType string) (map[string]int64, error) {
			if taskType != "REPL_TO_EC" {
				t.Fatalf("unexpected taskType for state counts: %q", taskType)
			}
			return map[string]int64{"PENDING": 1, "DONE": 1}, nil
		},
		requeueNow: func(ctx context.Context, taskID string) (bool, error) { return false, nil },
		cancelTask: func(ctx context.Context, taskID, reason string) (bool, error) { return false, nil },
	})

	req := httptest.NewRequest(http.MethodGet, "/v2/admin/tasks?state=PENDING&task_type=REPL_TO_EC&limit=10", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected %d got %d body=%s", http.StatusOK, rec.Code, rec.Body.String())
	}

	var resp map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response failed: %v", err)
	}
	if int(resp["count"].(float64)) != 2 {
		t.Fatalf("expected count=2 got %v", resp["count"])
	}
	tasks := resp["tasks"].([]interface{})
	actions0 := tasks[0].(map[string]interface{})["actions"].(map[string]interface{})
	actions1 := tasks[1].(map[string]interface{})["actions"].(map[string]interface{})
	if actions0["retry_now"] != true || actions0["cancel"] != true {
		t.Fatalf("expected PENDING actions true/true, got %+v", actions0)
	}
	if actions1["retry_now"] != false || actions1["cancel"] != false {
		t.Fatalf("expected DONE actions false/false, got %+v", actions1)
	}
}

func TestAdminTasksList_InvalidLimitAndUnavailable(t *testing.T) {
	t.Run("invalid_limit", func(t *testing.T) {
		router := newAdminTasksTestRouter(adminTaskRouteDeps{
			metadataAvailable: func() bool { return true },
			listTasks: func(ctx context.Context, state, taskType string, limit int) ([]meta.TieringTask, error) {
				return nil, nil
			},
			listStateCounts: func(ctx context.Context, taskType string) (map[string]int64, error) { return nil, nil },
			requeueNow:      func(ctx context.Context, taskID string) (bool, error) { return false, nil },
			cancelTask:      func(ctx context.Context, taskID, reason string) (bool, error) { return false, nil },
		})
		req := httptest.NewRequest(http.MethodGet, "/v2/admin/tasks?limit=abc", nil)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected %d got %d body=%s", http.StatusBadRequest, rec.Code, rec.Body.String())
		}
	})

	t.Run("metadata_unavailable", func(t *testing.T) {
		router := newAdminTasksTestRouter(adminTaskRouteDeps{
			metadataAvailable: func() bool { return false },
			listTasks: func(ctx context.Context, state, taskType string, limit int) ([]meta.TieringTask, error) {
				return nil, nil
			},
			listStateCounts: func(ctx context.Context, taskType string) (map[string]int64, error) { return nil, nil },
			requeueNow:      func(ctx context.Context, taskID string) (bool, error) { return false, nil },
			cancelTask:      func(ctx context.Context, taskID, reason string) (bool, error) { return false, nil },
		})
		req := httptest.NewRequest(http.MethodGet, "/v2/admin/tasks", nil)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("expected %d got %d body=%s", http.StatusServiceUnavailable, rec.Code, rec.Body.String())
		}
	})
}

func TestAdminTaskActions_RetryAndCancel(t *testing.T) {
	t.Run("retry_success_and_not_found", func(t *testing.T) {
		router := newAdminTasksTestRouter(adminTaskRouteDeps{
			metadataAvailable: func() bool { return true },
			listTasks: func(ctx context.Context, state, taskType string, limit int) ([]meta.TieringTask, error) {
				return nil, nil
			},
			listStateCounts: func(ctx context.Context, taskType string) (map[string]int64, error) { return nil, nil },
			requeueNow: func(ctx context.Context, taskID string) (bool, error) {
				if taskID == "t-ok" {
					return true, nil
				}
				if taskID == "t-err" {
					return false, errors.New("db error")
				}
				return false, nil
			},
			cancelTask: func(ctx context.Context, taskID, reason string) (bool, error) { return false, nil },
		})

		req := httptest.NewRequest(http.MethodPost, "/v2/admin/tasks/t-ok/retry-now", nil)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("expected %d got %d body=%s", http.StatusOK, rec.Code, rec.Body.String())
		}

		req2 := httptest.NewRequest(http.MethodPost, "/v2/admin/tasks/t-miss/retry-now", nil)
		rec2 := httptest.NewRecorder()
		router.ServeHTTP(rec2, req2)
		if rec2.Code != http.StatusNotFound {
			t.Fatalf("expected %d got %d body=%s", http.StatusNotFound, rec2.Code, rec2.Body.String())
		}

		req3 := httptest.NewRequest(http.MethodPost, "/v2/admin/tasks/t-err/retry-now", nil)
		rec3 := httptest.NewRecorder()
		router.ServeHTTP(rec3, req3)
		if rec3.Code != http.StatusInternalServerError {
			t.Fatalf("expected %d got %d body=%s", http.StatusInternalServerError, rec3.Code, rec3.Body.String())
		}
	})

	t.Run("cancel_reason_fallback_and_query", func(t *testing.T) {
		var gotReason string
		router := newAdminTasksTestRouter(adminTaskRouteDeps{
			metadataAvailable: func() bool { return true },
			listTasks: func(ctx context.Context, state, taskType string, limit int) ([]meta.TieringTask, error) {
				return nil, nil
			},
			listStateCounts: func(ctx context.Context, taskType string) (map[string]int64, error) { return nil, nil },
			requeueNow:      func(ctx context.Context, taskID string) (bool, error) { return false, nil },
			cancelTask: func(ctx context.Context, taskID, reason string) (bool, error) {
				gotReason = reason
				if taskID == "t-miss" {
					return false, nil
				}
				return true, nil
			},
		})

		req := httptest.NewRequest(http.MethodPost, "/v2/admin/tasks/t1/cancel", strings.NewReader(`{}`))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("expected %d got %d body=%s", http.StatusOK, rec.Code, rec.Body.String())
		}
		if gotReason != "cancelled_by_admin" {
			t.Fatalf("expected default reason cancelled_by_admin got %q", gotReason)
		}

		req2 := httptest.NewRequest(http.MethodPost, "/v2/admin/tasks/t2/cancel?reason=manual_stop", nil)
		rec2 := httptest.NewRecorder()
		router.ServeHTTP(rec2, req2)
		if rec2.Code != http.StatusOK {
			t.Fatalf("expected %d got %d body=%s", http.StatusOK, rec2.Code, rec2.Body.String())
		}
		if gotReason != "manual_stop" {
			t.Fatalf("expected reason manual_stop got %q", gotReason)
		}

		req3 := httptest.NewRequest(http.MethodPost, "/v2/admin/tasks/t-miss/cancel", nil)
		rec3 := httptest.NewRecorder()
		router.ServeHTTP(rec3, req3)
		if rec3.Code != http.StatusNotFound {
			t.Fatalf("expected %d got %d body=%s", http.StatusNotFound, rec3.Code, rec3.Body.String())
		}
	})
}
