package tiering

import (
	"context"
	"sync"
	"testing"
	"time"

	"hybrid_distributed_store/internal/config"
	"hybrid_distributed_store/internal/meta"
)

type policyScannerStubStore struct {
	mu sync.Mutex

	aCalls int
	bCalls int
	cCalls int

	lastA struct {
		age int
		max int
	}
	lastB struct {
		age      int
		max      int
		maxBytes int64
	}
	lastC struct {
		age      int
		max      int
		maxBytes int64
	}

	repairCalls   int
	lastRepairMax int

	oldVersionCalls int
	lastOldVersion  struct {
		keepLatest int
		minAgeSec  int
		maxTasks   int
	}

	historyReaperCalls int
	lastHistoryReaper  struct {
		olderThan time.Time
		limit     int
	}

	heartbeats []meta.NodeHeartbeatSnapshot
}

func (s *policyScannerStubStore) EnqueueTieringCandidatesStrategyA(ctx context.Context, ageThresholdSec int, maxObjects int) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.aCalls++
	s.lastA.age = ageThresholdSec
	s.lastA.max = maxObjects
	return 1, nil
}

func (s *policyScannerStubStore) EnqueueTieringCandidatesStrategyB(ctx context.Context, ageThresholdSec int, maxObjects int, maxBytes int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.bCalls++
	s.lastB.age = ageThresholdSec
	s.lastB.max = maxObjects
	s.lastB.maxBytes = maxBytes
	return 1, nil
}

func (s *policyScannerStubStore) EnqueueTieringCandidatesStrategyC(ctx context.Context, ageThresholdSec int, maxObjects int, maxBytes int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cCalls++
	s.lastC.age = ageThresholdSec
	s.lastC.max = maxObjects
	s.lastC.maxBytes = maxBytes
	return 1, nil
}

func (s *policyScannerStubStore) EnqueueRepairCandidates(ctx context.Context, maxObjects int) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.repairCalls++
	s.lastRepairMax = maxObjects
	return 1, nil
}

func (s *policyScannerStubStore) EnqueueOldVersionGCCandidates(ctx context.Context, keepLatest int, minAgeSec int, maxTasks int) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.oldVersionCalls++
	s.lastOldVersion.keepLatest = keepLatest
	s.lastOldVersion.minAgeSec = minAgeSec
	s.lastOldVersion.maxTasks = maxTasks
	return 1, nil
}

func (s *policyScannerStubStore) PurgeTerminalTieringTasks(ctx context.Context, olderThan time.Time, limit int) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.historyReaperCalls++
	s.lastHistoryReaper.olderThan = olderThan
	s.lastHistoryReaper.limit = limit
	return 1, nil
}

func (s *policyScannerStubStore) ListNodeHeartbeats(ctx context.Context, limit int) ([]meta.NodeHeartbeatSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]meta.NodeHeartbeatSnapshot, len(s.heartbeats))
	copy(out, s.heartbeats)
	return out, nil
}

func TestPolicyScanner_EnqueuePolicyDispatch(t *testing.T) {
	t.Parallel()

	t.Run("A", func(t *testing.T) {
		store := &policyScannerStubStore{}
		scanner := NewPolicyScanner(store, PolicyScannerConfig{
			PolicyVariant:           config.TieringPolicyA,
			AgeThresholdSec:         10,
			MaxObjects:              20,
			RepairEnabled:           true,
			RepairMaxObjects:        7,
			OldVersionReaperEnabled: true,
			OldVersionRetentionN:    2,
			OldVersionRetentionAge:  3600,
			OldVersionMaxTasks:      9,
		})

		scanner.runPolicyAndRepair(context.Background(), "test")

		if store.aCalls != 1 || store.bCalls != 0 || store.cCalls != 0 {
			t.Fatalf("unexpected dispatch counts: a=%d b=%d c=%d", store.aCalls, store.bCalls, store.cCalls)
		}
		if store.lastA.age != 10 || store.lastA.max != 20 {
			t.Fatalf("unexpected strategy-A args: %+v", store.lastA)
		}
		if store.repairCalls != 1 || store.lastRepairMax != 7 {
			t.Fatalf("unexpected repair calls=%d max=%d", store.repairCalls, store.lastRepairMax)
		}
		if store.oldVersionCalls != 1 {
			t.Fatalf("unexpected old-version calls=%d", store.oldVersionCalls)
		}
		if store.lastOldVersion.keepLatest != 2 || store.lastOldVersion.minAgeSec != 3600 || store.lastOldVersion.maxTasks != 9 {
			t.Fatalf("unexpected old-version args: %+v", store.lastOldVersion)
		}
	})

	t.Run("B", func(t *testing.T) {
		store := &policyScannerStubStore{}
		scanner := NewPolicyScanner(store, PolicyScannerConfig{
			PolicyVariant:         config.TieringPolicyB,
			AgeThresholdSec:       11,
			MaxObjects:            21,
			MaxBytes:              2048,
			RepairEnabled:         false,
			RepairMaxObjects:      99,
			HotPressureDiskPct:    80,
			HotPressureQueueDepth: 1000,
		})

		scanner.runPolicyAndRepair(context.Background(), "test")

		if store.aCalls != 0 || store.bCalls != 1 || store.cCalls != 0 {
			t.Fatalf("unexpected dispatch counts: a=%d b=%d c=%d", store.aCalls, store.bCalls, store.cCalls)
		}
		if store.lastB.age != 11 || store.lastB.max != 21 || store.lastB.maxBytes != 2048 {
			t.Fatalf("unexpected strategy-B args: %+v", store.lastB)
		}
		if store.repairCalls != 0 {
			t.Fatalf("repair should be disabled, got calls=%d", store.repairCalls)
		}
	})

	t.Run("C", func(t *testing.T) {
		store := &policyScannerStubStore{}
		store.heartbeats = []meta.NodeHeartbeatSnapshot{
			{
				NodeID:        "n1",
				Status:        "UP",
				LastSeenAt:    time.Now(),
				IOQueueDepth:  1,
				CPULoad:       0.10,
				MemoryUsedPct: 30,
				DiskIOWaitPct: 1,
				TotalBytes:    100,
				FreeBytes:     90,
			},
		}
		scanner := NewPolicyScanner(store, PolicyScannerConfig{
			PolicyVariant:     config.TieringPolicyC,
			AgeThresholdSec:   12,
			MaxObjects:        22,
			MaxBytes:          4096,
			RepairEnabled:     false,
			IdleStableRounds:  1,
			IdleCPUPercent:    70,
			IdleMemoryPercent: 80,
			IdleIOWaitPercent: 20,
			IdleQueueDepth:    16,
			HeartbeatStaleSec: 30,
		})

		scanner.runPolicyAndRepair(context.Background(), "test")

		if store.aCalls != 0 || store.bCalls != 0 || store.cCalls != 1 {
			t.Fatalf("unexpected dispatch counts: a=%d b=%d c=%d", store.aCalls, store.bCalls, store.cCalls)
		}
		if store.lastC.age != 12 || store.lastC.max != 22 || store.lastC.maxBytes != 4096 {
			t.Fatalf("unexpected strategy-C args: %+v", store.lastC)
		}
	})
}

func TestPolicyScanner_ThresholdCooldown(t *testing.T) {
	t.Parallel()

	store := &policyScannerStubStore{
		heartbeats: []meta.NodeHeartbeatSnapshot{
			{
				NodeID:        "n1",
				Status:        "UP",
				LastSeenAt:    time.Now(),
				TotalBytes:    100,
				FreeBytes:     10,
				IOQueueDepth:  1,
				CPULoad:       0.10,
				MemoryUsedPct: 35,
				DiskIOWaitPct: 2,
			},
		},
	}
	scanner := NewPolicyScanner(store, PolicyScannerConfig{
		PolicyVariant:     config.TieringPolicyA,
		AgeThresholdSec:   1,
		MaxObjects:        100,
		TriggerMode:       config.TieringTriggerThreshold,
		ThresholdCooldown: 2 * time.Second,
		HeartbeatStaleSec: 30,
		IdleStableRounds:  1,
		IdleCPUPercent:    70,
		IdleMemoryPercent: 80,
		IdleIOWaitPercent: 20,
		IdleQueueDepth:    16,
		RepairEnabled:     false,
	})

	scanner.runThresholdPass(context.Background(), "test")
	if store.aCalls != 1 {
		t.Fatalf("first threshold pass should trigger, calls=%d", store.aCalls)
	}

	scanner.runThresholdPass(context.Background(), "test")
	if store.aCalls != 1 {
		t.Fatalf("second threshold pass should be blocked by cooldown, calls=%d", store.aCalls)
	}

	scanner.lastThresholdTrigger = time.Now().Add(-3 * time.Second)
	scanner.runThresholdPass(context.Background(), "test")
	if store.aCalls != 2 {
		t.Fatalf("third threshold pass should trigger after cooldown, calls=%d", store.aCalls)
	}
}

func TestPolicyScanner_IsIdleWindow_StaleHeartbeatIgnored(t *testing.T) {
	t.Parallel()

	now := time.Now()
	store := &policyScannerStubStore{
		heartbeats: []meta.NodeHeartbeatSnapshot{
			{
				NodeID:        "stale-high-queue",
				Status:        "UP",
				LastSeenAt:    now.Add(-2 * time.Minute),
				IOQueueDepth:  9999,
				CPULoad:       0.99,
				MemoryUsedPct: 99,
				DiskIOWaitPct: 99,
				TotalBytes:    100,
				FreeBytes:     1,
			},
			{
				NodeID:        "live-low-queue",
				Status:        "UP",
				LastSeenAt:    now,
				IOQueueDepth:  1,
				CPULoad:       0.20,
				MemoryUsedPct: 45,
				DiskIOWaitPct: 1,
				TotalBytes:    100,
				FreeBytes:     80,
			},
		},
	}
	scanner := NewPolicyScanner(store, PolicyScannerConfig{
		IdleCPUPercent:    70,
		IdleMemoryPercent: 80,
		IdleIOWaitPercent: 20,
		IdleQueueDepth:    16,
		HeartbeatStaleSec: 10,
	})

	idle, _, err := scanner.isIdleWindow(context.Background())
	if err != nil {
		t.Fatalf("isIdleWindow returned error: %v", err)
	}
	if !idle {
		t.Fatalf("stale heartbeat should be ignored, expected idle=true")
	}
}

func TestPolicyScanner_IdleStableRounds_ResetOnBusySample(t *testing.T) {
	t.Parallel()

	now := time.Now()
	store := &policyScannerStubStore{
		heartbeats: []meta.NodeHeartbeatSnapshot{
			{
				NodeID:        "n1",
				Status:        "UP",
				LastSeenAt:    now,
				IOQueueDepth:  1,
				CPULoad:       0.10,
				MemoryUsedPct: 30,
				DiskIOWaitPct: 1,
			},
		},
	}
	scanner := NewPolicyScanner(store, PolicyScannerConfig{
		TriggerMode:       config.TieringTriggerThreshold,
		PolicyVariant:     config.TieringPolicyA,
		AgeThresholdSec:   1,
		MaxObjects:        100,
		ThresholdCooldown: time.Millisecond,
		IdleStableRounds:  2,
		IdleCPUPercent:    70,
		IdleMemoryPercent: 80,
		IdleIOWaitPercent: 20,
		IdleQueueDepth:    16,
		HeartbeatStaleSec: 30,
	})

	// first idle sample: counter=1, not triggered
	scanner.runThresholdPass(context.Background(), "test")
	if store.aCalls != 0 {
		t.Fatalf("expected no trigger on first idle sample, got=%d", store.aCalls)
	}

	// make one busy sample -> counter reset
	store.mu.Lock()
	store.heartbeats[0].MemoryUsedPct = 95
	store.mu.Unlock()
	scanner.runThresholdPass(context.Background(), "test")
	if store.aCalls != 0 {
		t.Fatalf("expected no trigger on busy sample, got=%d", store.aCalls)
	}

	// back to idle, first idle after reset: counter=1
	store.mu.Lock()
	store.heartbeats[0].MemoryUsedPct = 30
	store.mu.Unlock()
	scanner.runThresholdPass(context.Background(), "test")
	if store.aCalls != 0 {
		t.Fatalf("expected no trigger on first idle sample after reset, got=%d", store.aCalls)
	}

	// second consecutive idle sample -> trigger
	scanner.lastThresholdTrigger = time.Time{}
	scanner.runThresholdPass(context.Background(), "test")
	if store.aCalls != 1 {
		t.Fatalf("expected trigger after 2 consecutive idle samples, got=%d", store.aCalls)
	}
}

func TestPolicyScanner_OldVersionRunsWhenRepairDisabled(t *testing.T) {
	t.Parallel()

	store := &policyScannerStubStore{}
	scanner := NewPolicyScanner(store, PolicyScannerConfig{
		PolicyVariant:           config.TieringPolicyA,
		AgeThresholdSec:         10,
		MaxObjects:              20,
		RepairEnabled:           false,
		OldVersionReaperEnabled: true,
		OldVersionRetentionN:    2,
		OldVersionRetentionAge:  3600,
		OldVersionMaxTasks:      9,
	})

	scanner.runPolicyAndRepair(context.Background(), "test")

	if store.aCalls != 1 {
		t.Fatalf("expected policy scan to run once, calls=%d", store.aCalls)
	}
	if store.oldVersionCalls != 1 {
		t.Fatalf("expected old-version scan even when repair disabled, calls=%d", store.oldVersionCalls)
	}
}

func TestPolicyScanner_TaskHistoryReaperInterval(t *testing.T) {
	t.Parallel()

	store := &policyScannerStubStore{}
	scanner := NewPolicyScanner(store, PolicyScannerConfig{
		PolicyVariant:             config.TieringPolicyA,
		AgeThresholdSec:           10,
		MaxObjects:                20,
		RepairEnabled:             false,
		OldVersionReaperEnabled:   false,
		TaskHistoryReaperEnabled:  true,
		TaskHistoryRetentionSec:   60,
		TaskHistoryReaperMaxTasks: 7,
		TaskHistoryReaperInterval: time.Hour,
	})

	before := time.Now().Add(-70 * time.Second)
	after := time.Now().Add(-50 * time.Second)

	scanner.runPolicyAndRepair(context.Background(), "test")
	if store.historyReaperCalls != 1 {
		t.Fatalf("expected first task-history reaper call, got=%d", store.historyReaperCalls)
	}
	if store.lastHistoryReaper.limit != 7 {
		t.Fatalf("unexpected task-history reaper limit=%d", store.lastHistoryReaper.limit)
	}
	if store.lastHistoryReaper.olderThan.Before(before) || store.lastHistoryReaper.olderThan.After(after) {
		t.Fatalf("unexpected olderThan cutoff=%s", store.lastHistoryReaper.olderThan)
	}

	scanner.runPolicyAndRepair(context.Background(), "test")
	if store.historyReaperCalls != 1 {
		t.Fatalf("expected interval throttle, got calls=%d", store.historyReaperCalls)
	}
}

func TestPolicyScanner_StrategyCBusyStillRunsMaintenanceLane(t *testing.T) {
	t.Parallel()

	store := &policyScannerStubStore{
		heartbeats: []meta.NodeHeartbeatSnapshot{
			{
				NodeID:        "n1",
				Status:        "UP",
				LastSeenAt:    time.Now(),
				IOQueueDepth:  128,
				CPULoad:       0.90,
				MemoryUsedPct: 90,
				DiskIOWaitPct: 50,
			},
		},
	}
	scanner := NewPolicyScanner(store, PolicyScannerConfig{
		PolicyVariant:             config.TieringPolicyC,
		AgeThresholdSec:           1,
		MaxObjects:                50,
		MaxBytes:                  1024,
		IdleStableRounds:          1,
		IdleCPUPercent:            70,
		IdleMemoryPercent:         80,
		IdleIOWaitPercent:         20,
		IdleQueueDepth:            16,
		HeartbeatStaleSec:         30,
		RepairEnabled:             true,
		RepairMaxObjects:          11,
		OldVersionReaperEnabled:   true,
		OldVersionRetentionN:      2,
		OldVersionRetentionAge:    3600,
		OldVersionMaxTasks:        13,
		TaskHistoryReaperEnabled:  true,
		TaskHistoryRetentionSec:   60,
		TaskHistoryReaperMaxTasks: 7,
		TaskHistoryReaperInterval: time.Minute,
	})

	scanner.runPolicyAndRepair(context.Background(), "test")

	if store.cCalls != 0 {
		t.Fatalf("expected strategy-C tiering lane blocked by busy gate, cCalls=%d", store.cCalls)
	}
	if store.repairCalls != 1 || store.lastRepairMax != 11 {
		t.Fatalf("expected maintenance repair lane to run once, calls=%d max=%d", store.repairCalls, store.lastRepairMax)
	}
	if store.oldVersionCalls != 1 || store.lastOldVersion.maxTasks != 13 {
		t.Fatalf("expected maintenance old-version lane to run once, calls=%d maxTasks=%d", store.oldVersionCalls, store.lastOldVersion.maxTasks)
	}
	if store.historyReaperCalls != 1 || store.lastHistoryReaper.limit != 7 {
		t.Fatalf("expected maintenance task-history lane to run once, calls=%d limit=%d", store.historyReaperCalls, store.lastHistoryReaper.limit)
	}
}
