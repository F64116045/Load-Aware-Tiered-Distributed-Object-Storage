package meta

import (
	"context"
	"fmt"
	"testing"

	"hybrid_distributed_store/internal/config"
)

func TestEnqueueTieringCandidatesStrategyA_DueBurstAdaptiveScan(t *testing.T) {
	store := newMemoryTaskStore(t, "memory://due-burst-adaptive")
	ctx := context.Background()

	oldAge := config.AgeThresholdSec
	oldMaxScan := config.TieringDueIndexMaxScan
	oldBurstRounds := config.TieringDueIndexBurstRounds
	oldAdaptiveMax := config.TieringDueIndexAdaptiveMaxScan
	config.AgeThresholdSec = 0
	config.TieringDueIndexMaxScan = 1
	config.TieringDueIndexBurstRounds = 4
	config.TieringDueIndexAdaptiveMaxScan = 8
	t.Cleanup(func() {
		config.AgeThresholdSec = oldAge
		config.TieringDueIndexMaxScan = oldMaxScan
		config.TieringDueIndexBurstRounds = oldBurstRounds
		config.TieringDueIndexAdaptiveMaxScan = oldAdaptiveMax
	})

	for i := 0; i < 4; i++ {
		objectID := fmt.Sprintf("obj-due-burst-%d", i)
		if err := store.UpsertNormalizedMetadata(ctx, objectID, map[string]interface{}{
			"strategy":        "replication",
			"hot_version":     int64(1),
			"cold_hash":       fmt.Sprintf("hash-%d", i),
			"original_length": int64(16 + i),
			"replica_nodes":   []string{"node-a", "node-b", "node-c"},
		}); err != nil {
			t.Fatalf("upsert metadata %s failed: %v", objectID, err)
		}
	}

	enqueued, err := store.EnqueueTieringCandidatesStrategyA(ctx, 0, 4)
	if err != nil {
		t.Fatalf("enqueue strategy-A with burst scan failed: %v", err)
	}
	if enqueued != 4 {
		t.Fatalf("expected 4 tasks enqueued with burst scan, got=%d", enqueued)
	}

	tasks, err := store.ListTieringTasks(ctx, "PENDING", "REPL_TO_EC", 10)
	if err != nil {
		t.Fatalf("list tiering tasks failed: %v", err)
	}
	if len(tasks) != 4 {
		t.Fatalf("expected 4 pending REPL_TO_EC tasks, got=%d", len(tasks))
	}
}
