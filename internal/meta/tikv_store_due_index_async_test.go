package meta

import (
	"context"
	"testing"
	"time"

	"hybrid_distributed_store/internal/config"
)

func TestUpsertNormalizedMetadata_AsyncDueIndexStillVisibleToScanner(t *testing.T) {
	store := newMemoryTaskStore(t, "memory://async-due-index")
	ctx := context.Background()

	oldMode := config.TieringDueIndexCommitMode
	oldAge := config.AgeThresholdSec
	config.TieringDueIndexCommitMode = config.TieringDueIndexCommitAsync
	config.AgeThresholdSec = 0
	t.Cleanup(func() {
		config.TieringDueIndexCommitMode = oldMode
		config.AgeThresholdSec = oldAge
	})

	if err := store.UpsertNormalizedMetadata(ctx, "obj-async-due", map[string]interface{}{
		"strategy":        "replication",
		"hot_version":     int64(1),
		"cold_hash":       "hash",
		"original_length": int64(1024),
		"replica_nodes":   []string{"node-a", "node-b", "node-c"},
	}); err != nil {
		t.Fatalf("upsert metadata failed: %v", err)
	}

	deadline := time.Now().Add(time.Second)
	for {
		enqueued, err := store.EnqueueTieringCandidatesStrategyA(ctx, 0, 1)
		if err != nil {
			t.Fatalf("enqueue strategy-A failed: %v", err)
		}
		if enqueued == 1 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("async due index was not visible before timeout")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
