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

	a1Calls int
	a2Calls int
	a3Calls int

	lastA1 struct {
		age int
		max int
	}
	lastA2 struct {
		age  int
		size int64
		max  int
	}
	lastA3 struct {
		age      int
		max      int
		maxBytes int64
	}

	repairCalls   int
	lastRepairMax int

	heartbeats []meta.NodeHeartbeatSnapshot
}

func (s *policyScannerStubStore) EnqueueTieringCandidatesA1(ctx context.Context, ageThresholdSec int, maxObjects int) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.a1Calls++
	s.lastA1.age = ageThresholdSec
	s.lastA1.max = maxObjects
	return 1, nil
}

func (s *policyScannerStubStore) EnqueueTieringCandidatesA2(ctx context.Context, ageThresholdSec int, sizeThresholdBytes int64, maxObjects int) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.a2Calls++
	s.lastA2.age = ageThresholdSec
	s.lastA2.size = sizeThresholdBytes
	s.lastA2.max = maxObjects
	return 1, nil
}

func (s *policyScannerStubStore) EnqueueTieringCandidatesA3(ctx context.Context, ageThresholdSec int, maxObjects int, maxBytes int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.a3Calls++
	s.lastA3.age = ageThresholdSec
	s.lastA3.max = maxObjects
	s.lastA3.maxBytes = maxBytes
	return 1, nil
}

func (s *policyScannerStubStore) EnqueueRepairCandidates(ctx context.Context, maxObjects int) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.repairCalls++
	s.lastRepairMax = maxObjects
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

	t.Run("A1", func(t *testing.T) {
		store := &policyScannerStubStore{}
		scanner := NewPolicyScanner(store, PolicyScannerConfig{
			PolicyVariant:    config.TieringPolicyA1,
			AgeThresholdSec:  10,
			MaxObjects:       20,
			RepairEnabled:    true,
			RepairMaxObjects: 7,
		})

		scanner.runPolicyAndRepair(context.Background(), "test")

		if store.a1Calls != 1 || store.a2Calls != 0 || store.a3Calls != 0 {
			t.Fatalf("unexpected dispatch counts: a1=%d a2=%d a3=%d", store.a1Calls, store.a2Calls, store.a3Calls)
		}
		if store.lastA1.age != 10 || store.lastA1.max != 20 {
			t.Fatalf("unexpected A1 args: %+v", store.lastA1)
		}
		if store.repairCalls != 1 || store.lastRepairMax != 7 {
			t.Fatalf("unexpected repair calls=%d max=%d", store.repairCalls, store.lastRepairMax)
		}
	})

	t.Run("A2", func(t *testing.T) {
		store := &policyScannerStubStore{}
		scanner := NewPolicyScanner(store, PolicyScannerConfig{
			PolicyVariant:         config.TieringPolicyA2,
			AgeThresholdSec:       11,
			SizeThresholdBytes:    2048,
			MaxObjects:            21,
			RepairEnabled:         false,
			RepairMaxObjects:      99,
			HotPressureDiskPct:    80,
			HotPressureQueueDepth: 1000,
		})

		scanner.runPolicyAndRepair(context.Background(), "test")

		if store.a1Calls != 0 || store.a2Calls != 1 || store.a3Calls != 0 {
			t.Fatalf("unexpected dispatch counts: a1=%d a2=%d a3=%d", store.a1Calls, store.a2Calls, store.a3Calls)
		}
		if store.lastA2.age != 11 || store.lastA2.size != 2048 || store.lastA2.max != 21 {
			t.Fatalf("unexpected A2 args: %+v", store.lastA2)
		}
		if store.repairCalls != 0 {
			t.Fatalf("repair should be disabled, got calls=%d", store.repairCalls)
		}
	})

	t.Run("A3", func(t *testing.T) {
		store := &policyScannerStubStore{}
		scanner := NewPolicyScanner(store, PolicyScannerConfig{
			PolicyVariant:   config.TieringPolicyA3,
			AgeThresholdSec: 12,
			MaxObjects:      22,
			MaxBytes:        4096,
			RepairEnabled:   false,
		})

		scanner.runPolicyAndRepair(context.Background(), "test")

		if store.a1Calls != 0 || store.a2Calls != 0 || store.a3Calls != 1 {
			t.Fatalf("unexpected dispatch counts: a1=%d a2=%d a3=%d", store.a1Calls, store.a2Calls, store.a3Calls)
		}
		if store.lastA3.age != 12 || store.lastA3.max != 22 || store.lastA3.maxBytes != 4096 {
			t.Fatalf("unexpected A3 args: %+v", store.lastA3)
		}
	})
}

func TestPolicyScanner_ThresholdCooldown(t *testing.T) {
	t.Parallel()

	store := &policyScannerStubStore{
		heartbeats: []meta.NodeHeartbeatSnapshot{
			{
				NodeID:       "n1",
				Status:       "UP",
				LastSeenAt:   time.Now(),
				TotalBytes:   100,
				FreeBytes:    10,
				IOQueueDepth: 5,
			},
		},
	}
	scanner := NewPolicyScanner(store, PolicyScannerConfig{
		PolicyVariant:         config.TieringPolicyA1,
		AgeThresholdSec:       1,
		MaxObjects:            100,
		TriggerMode:           config.TieringTriggerThreshold,
		ThresholdCooldown:     2 * time.Second,
		HotPressureDiskPct:    80,
		HotPressureQueueDepth: 0,
		HeartbeatStaleSec:     30,
		RepairEnabled:         false,
	})

	scanner.runThresholdPass(context.Background(), "test")
	if store.a1Calls != 1 {
		t.Fatalf("first threshold pass should trigger, calls=%d", store.a1Calls)
	}

	scanner.runThresholdPass(context.Background(), "test")
	if store.a1Calls != 1 {
		t.Fatalf("second threshold pass should be blocked by cooldown, calls=%d", store.a1Calls)
	}

	scanner.lastThresholdTrigger = time.Now().Add(-3 * time.Second)
	scanner.runThresholdPass(context.Background(), "test")
	if store.a1Calls != 2 {
		t.Fatalf("third threshold pass should trigger after cooldown, calls=%d", store.a1Calls)
	}
}

func TestPolicyScanner_IsUnderPressure_StaleHeartbeatIgnored(t *testing.T) {
	t.Parallel()

	now := time.Now()
	store := &policyScannerStubStore{
		heartbeats: []meta.NodeHeartbeatSnapshot{
			{
				NodeID:       "stale-high-queue",
				Status:       "UP",
				LastSeenAt:   now.Add(-2 * time.Minute),
				IOQueueDepth: 9999,
				TotalBytes:   100,
				FreeBytes:    1,
			},
			{
				NodeID:       "live-low-queue",
				Status:       "UP",
				LastSeenAt:   now,
				IOQueueDepth: 1,
				TotalBytes:   100,
				FreeBytes:    80,
			},
		},
	}
	scanner := NewPolicyScanner(store, PolicyScannerConfig{
		HotPressureQueueDepth: 100,
		HotPressureDiskPct:    90,
		HeartbeatStaleSec:     10,
	})

	underPressure, _, err := scanner.isUnderPressure(context.Background())
	if err != nil {
		t.Fatalf("isUnderPressure returned error: %v", err)
	}
	if underPressure {
		t.Fatalf("stale heartbeat should be ignored, expected underPressure=false")
	}
}
