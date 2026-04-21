package meta

import (
	"context"
	"testing"
	"time"
)

func newMemoryTaskStore(t *testing.T, dsn string) *TiKVStore {
	t.Helper()
	store, err := NewTiKVStore(Config{
		Enabled: true,
		DSN:     dsn,
	})
	if err != nil {
		t.Fatalf("new memory task store failed: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func TestClaimNextTieringTask_IndexOrdering(t *testing.T) {
	store := newMemoryTaskStore(t, "memory://task-index-ordering")
	ctx := context.Background()
	now := time.Now()

	if err := store.EnqueueTieringTask(ctx, "t-future", "o1", 1, "REPL_TO_EC", 999, now.Add(5*time.Minute)); err != nil {
		t.Fatalf("enqueue future failed: %v", err)
	}
	if err := store.EnqueueTieringTask(ctx, "t-low", "o2", 1, "REPL_TO_EC", 10, now.Add(-time.Second)); err != nil {
		t.Fatalf("enqueue low failed: %v", err)
	}
	if err := store.EnqueueTieringTask(ctx, "t-high", "o3", 1, "REPL_TO_EC", 200, now.Add(-time.Second)); err != nil {
		t.Fatalf("enqueue high failed: %v", err)
	}

	task, err := store.ClaimNextTieringTask(ctx, "")
	if err != nil {
		t.Fatalf("first claim failed: %v", err)
	}
	if task == nil || task.TaskID != "t-high" {
		t.Fatalf("expected t-high first, got %+v", task)
	}

	task, err = store.ClaimNextTieringTask(ctx, "")
	if err != nil {
		t.Fatalf("second claim failed: %v", err)
	}
	if task == nil || task.TaskID != "t-low" {
		t.Fatalf("expected t-low second, got %+v", task)
	}

	task, err = store.ClaimNextTieringTask(ctx, "")
	if err != nil {
		t.Fatalf("third claim failed: %v", err)
	}
	if task != nil {
		t.Fatalf("expected no runnable task, got %+v", task)
	}
}

func TestClaimNextTieringTask_FilterByType(t *testing.T) {
	store := newMemoryTaskStore(t, "memory://task-index-filter")
	ctx := context.Background()
	now := time.Now()

	if err := store.EnqueueTieringTask(ctx, "t-gc", "o1", 1, "GC", 300, now); err != nil {
		t.Fatalf("enqueue gc failed: %v", err)
	}
	if err := store.EnqueueTieringTask(ctx, "t-repair", "o2", 1, "REPAIR", 100, now); err != nil {
		t.Fatalf("enqueue repair failed: %v", err)
	}

	task, err := store.ClaimNextTieringTask(ctx, "REPAIR")
	if err != nil {
		t.Fatalf("filtered claim failed: %v", err)
	}
	if task == nil || task.TaskID != "t-repair" {
		t.Fatalf("expected t-repair for filtered claim, got %+v", task)
	}
}

func TestRetryAndRequeueMoveBetweenWaitAndReady(t *testing.T) {
	store := newMemoryTaskStore(t, "memory://task-index-retry-requeue")
	ctx := context.Background()
	now := time.Now()

	if err := store.EnqueueTieringTask(ctx, "t1", "o1", 1, "REPL_TO_EC", 100, now); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}
	task, err := store.ClaimNextTieringTask(ctx, "")
	if err != nil {
		t.Fatalf("initial claim failed: %v", err)
	}
	if task == nil || task.TaskID != "t1" {
		t.Fatalf("expected t1 claimed, got %+v", task)
	}

	if err := store.MarkTieringTaskRetry(ctx, "t1", "transient", now.Add(10*time.Minute)); err != nil {
		t.Fatalf("mark retry failed: %v", err)
	}
	task, err = store.ClaimNextTieringTask(ctx, "")
	if err != nil {
		t.Fatalf("claim after retry failed: %v", err)
	}
	if task != nil {
		t.Fatalf("expected no claimable task before retry due time, got %+v", task)
	}

	changed, err := store.RequeueTieringTaskNow(ctx, "t1")
	if err != nil {
		t.Fatalf("requeue now failed: %v", err)
	}
	if !changed {
		t.Fatalf("expected requeue-now to change task")
	}
	task, err = store.ClaimNextTieringTask(ctx, "")
	if err != nil {
		t.Fatalf("claim after requeue-now failed: %v", err)
	}
	if task == nil || task.TaskID != "t1" {
		t.Fatalf("expected t1 claimed after requeue-now, got %+v", task)
	}
}

func TestClaimFallbackForLegacyTaskWithoutIndex(t *testing.T) {
	store := newMemoryTaskStore(t, "memory://task-index-legacy")
	ctx := context.Background()

	store.mu.Lock()
	rec := &tiKVTaskRecord{
		TaskID:      "legacy-task",
		ObjectID:    "legacy-obj",
		Version:     1,
		TaskType:    "REPAIR",
		TaskState:   "PENDING",
		Priority:    100,
		RetryCount:  0,
		ScheduledAt: time.Now().Add(-time.Second),
	}
	if err := store.putJSON(tiKVTaskKey(rec.TaskID), rec); err != nil {
		store.mu.Unlock()
		t.Fatalf("write legacy task row failed: %v", err)
	}
	store.mu.Unlock()

	task, err := store.ClaimNextTieringTask(ctx, "")
	if err != nil {
		t.Fatalf("claim legacy task failed: %v", err)
	}
	if task == nil || task.TaskID != "legacy-task" {
		t.Fatalf("expected legacy task claimed via fallback, got %+v", task)
	}
}
