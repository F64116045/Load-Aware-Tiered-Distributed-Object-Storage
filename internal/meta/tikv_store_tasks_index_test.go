package meta

import (
	"context"
	"testing"
	"time"

	"hybrid_distributed_store/internal/config"
	kvstore "hybrid_distributed_store/internal/meta/kvstore"
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

func TestLegacyTaskWithoutIndexIsNotClaimed(t *testing.T) {
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
	if task != nil {
		t.Fatalf("expected legacy task to stay unclaimed without runnable index, got %+v", task)
	}

	changed, err := store.RequeueTieringTaskNow(ctx, "legacy-task")
	if err != nil {
		t.Fatalf("requeue legacy task failed: %v", err)
	}
	if !changed {
		t.Fatalf("expected requeue to re-index legacy task")
	}
	task, err = store.ClaimNextTieringTask(ctx, "")
	if err != nil {
		t.Fatalf("claim re-indexed legacy task failed: %v", err)
	}
	if task == nil || task.TaskID != "legacy-task" {
		t.Fatalf("expected legacy task claimed after re-index, got %+v", task)
	}
}

func TestPurgeTerminalTieringTasks_PurgesOldTerminalAndRepairsStaleIndex(t *testing.T) {
	store := newMemoryTaskStore(t, "memory://task-history-reaper")
	ctx := context.Background()
	now := time.Now()

	oldTs := now.Add(-48 * time.Hour)
	recentTs := now.Add(-2 * time.Hour)

	store.mu.Lock()
	recDoneOld := &tiKVTaskRecord{
		TaskID:      "done-old",
		ObjectID:    "obj-1",
		Version:     1,
		TaskType:    "REPAIR",
		TaskState:   "DONE",
		Priority:    100,
		RetryCount:  0,
		ScheduledAt: oldTs,
		FinishedAt:  &oldTs,
	}
	if err := store.writeTaskRecordWithRunnableIndexLocked(nil, recDoneOld, now); err != nil {
		store.mu.Unlock()
		t.Fatalf("write done-old failed: %v", err)
	}
	recFailedOld := &tiKVTaskRecord{
		TaskID:      "failed-old",
		ObjectID:    "obj-2",
		Version:     1,
		TaskType:    "REPAIR",
		TaskState:   "FAILED",
		Priority:    100,
		RetryCount:  2,
		ScheduledAt: oldTs,
		FinishedAt:  &oldTs,
	}
	if err := store.writeTaskRecordWithRunnableIndexLocked(nil, recFailedOld, now); err != nil {
		store.mu.Unlock()
		t.Fatalf("write failed-old failed: %v", err)
	}
	recDoneRecent := &tiKVTaskRecord{
		TaskID:      "done-recent",
		ObjectID:    "obj-3",
		Version:     1,
		TaskType:    "REPAIR",
		TaskState:   "DONE",
		Priority:    100,
		RetryCount:  0,
		ScheduledAt: recentTs,
		FinishedAt:  &recentTs,
	}
	if err := store.writeTaskRecordWithRunnableIndexLocked(nil, recDoneRecent, now); err != nil {
		store.mu.Unlock()
		t.Fatalf("write done-recent failed: %v", err)
	}
	recPending := &tiKVTaskRecord{
		TaskID:      "pending-live",
		ObjectID:    "obj-4",
		Version:     1,
		TaskType:    "REPAIR",
		TaskState:   "PENDING",
		Priority:    80,
		RetryCount:  0,
		ScheduledAt: oldTs,
	}
	if err := store.writeTaskRecordWithRunnableIndexLocked(nil, recPending, now); err != nil {
		store.mu.Unlock()
		t.Fatalf("write pending-live failed: %v", err)
	}
	if err := store.kv.Set([]byte(tiKVTaskTerminalKey(oldTs, recPending.TaskID)), []byte{1}, kvstore.Sync); err != nil {
		store.mu.Unlock()
		t.Fatalf("inject stale terminal index failed: %v", err)
	}
	store.mu.Unlock()

	purged, err := store.PurgeTerminalTieringTasks(ctx, now.Add(-24*time.Hour), 10)
	if err != nil {
		t.Fatalf("purge terminal tasks failed: %v", err)
	}
	if purged != 2 {
		t.Fatalf("expected purged=2, got=%d", purged)
	}

	store.mu.RLock()
	defer store.mu.RUnlock()

	if _, found, err := store.getTaskRecord(tiKVTaskKey("done-old")); err != nil || found {
		t.Fatalf("expected done-old removed, found=%v err=%v", found, err)
	}
	if _, found, err := store.getTaskRecord(tiKVTaskKey("failed-old")); err != nil || found {
		t.Fatalf("expected failed-old removed, found=%v err=%v", found, err)
	}
	if _, found, err := store.getTaskRecord(tiKVTaskKey("done-recent")); err != nil || !found {
		t.Fatalf("expected done-recent kept, found=%v err=%v", found, err)
	}
	if _, found, err := store.getTaskRecord(tiKVTaskKey("pending-live")); err != nil || !found {
		t.Fatalf("expected pending-live kept, found=%v err=%v", found, err)
	}

	if _, closer, err := store.kv.Get([]byte(tiKVTaskTerminalKey(oldTs, "pending-live"))); err == nil {
		_ = closer.Close()
		t.Fatalf("expected stale terminal index for pending-live removed")
	}
	if _, closer, err := store.kv.Get([]byte(tiKVTaskReadyKey("REPAIR", 80, oldTs, "pending-live"))); err != nil {
		t.Fatalf("expected pending-live ready index kept/rebuilt: %v", err)
	} else {
		_ = closer.Close()
	}
	if _, closer, err := store.kv.Get([]byte(tiKVTaskTerminalKey(recentTs, "done-recent"))); err != nil {
		t.Fatalf("expected done-recent terminal index kept: %v", err)
	} else {
		_ = closer.Close()
	}
}

func TestPurgeTerminalTieringTasks_RespectsLimit(t *testing.T) {
	store := newMemoryTaskStore(t, "memory://task-history-reaper-limit")
	ctx := context.Background()
	now := time.Now()

	store.mu.Lock()
	for i := 0; i < 3; i++ {
		ts := now.Add(-time.Duration(72-i) * time.Hour)
		rec := &tiKVTaskRecord{
			TaskID:      "done-limit-" + string(rune('a'+i)),
			ObjectID:    "obj-limit",
			Version:     int64(i + 1),
			TaskType:    "REPAIR",
			TaskState:   "DONE",
			Priority:    10,
			RetryCount:  0,
			ScheduledAt: ts,
			FinishedAt:  &ts,
		}
		if err := store.writeTaskRecordWithRunnableIndexLocked(nil, rec, now); err != nil {
			store.mu.Unlock()
			t.Fatalf("write test task failed: %v", err)
		}
	}
	store.mu.Unlock()

	purged, err := store.PurgeTerminalTieringTasks(ctx, now.Add(-24*time.Hour), 2)
	if err != nil {
		t.Fatalf("purge terminal tasks failed: %v", err)
	}
	if purged != 2 {
		t.Fatalf("expected purged=2 by limit, got=%d", purged)
	}

	store.mu.RLock()
	defer store.mu.RUnlock()
	remaining := 0
	for i := 0; i < 3; i++ {
		taskID := "done-limit-" + string(rune('a'+i))
		if _, found, err := store.getTaskRecord(tiKVTaskKey(taskID)); err != nil {
			t.Fatalf("get task %s failed: %v", taskID, err)
		} else if found {
			remaining++
		}
	}
	if remaining != 1 {
		t.Fatalf("expected one remaining task after limited purge, got=%d", remaining)
	}
}

func TestClaimNextTieringTask_PromoteWaitBurstAdaptive(t *testing.T) {
	store := newMemoryTaskStore(t, "memory://task-claim-promote-burst")
	ctx := context.Background()

	oldBase := config.TieringTaskWaitPromoteBase
	oldRounds := config.TieringTaskWaitPromoteBurstRounds
	oldAdaptiveMax := config.TieringTaskWaitPromoteAdaptiveMax
	config.TieringTaskWaitPromoteBase = 1
	config.TieringTaskWaitPromoteBurstRounds = 4
	config.TieringTaskWaitPromoteAdaptiveMax = 8
	t.Cleanup(func() {
		config.TieringTaskWaitPromoteBase = oldBase
		config.TieringTaskWaitPromoteBurstRounds = oldRounds
		config.TieringTaskWaitPromoteAdaptiveMax = oldAdaptiveMax
	})

	future := time.Now().Add(80 * time.Millisecond)
	for i := 0; i < 3; i++ {
		taskID := "wait-burst-" + string(rune('a'+i))
		if err := store.EnqueueTieringTask(ctx, taskID, "obj-wait", int64(i+1), "REPAIR", 100-i, future); err != nil {
			t.Fatalf("enqueue future task %s failed: %v", taskID, err)
		}
	}

	time.Sleep(120 * time.Millisecond)
	task, err := store.ClaimNextTieringTask(ctx, "")
	if err != nil {
		t.Fatalf("claim with burst promote failed: %v", err)
	}
	if task == nil {
		t.Fatalf("expected claimed task after due promote")
	}

	countPrefix := func(prefix string) int {
		store.mu.RLock()
		defer store.mu.RUnlock()
		it, err := store.newPrefixIter(prefix)
		if err != nil {
			t.Fatalf("newPrefixIter(%q) failed: %v", prefix, err)
		}
		defer it.Close()
		n := 0
		for it.First(); it.Valid(); it.Next() {
			n++
		}
		if err := it.Error(); err != nil {
			t.Fatalf("iterator(%q) failed: %v", prefix, err)
		}
		return n
	}

	waitCount := countPrefix(tiKVTaskWaitPrefix())
	readyCount := countPrefix(tiKVTaskReadyPrefix())
	if waitCount != 0 {
		t.Fatalf("expected wait queue drained in one claim burst, waitCount=%d", waitCount)
	}
	if readyCount != 2 {
		t.Fatalf("expected remaining two tasks in ready index after one claim, readyCount=%d", readyCount)
	}
}
