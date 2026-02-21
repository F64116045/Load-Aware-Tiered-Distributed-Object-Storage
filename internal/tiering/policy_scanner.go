package tiering

import (
	"context"
	"log"
	"time"

	"hybrid_distributed_store/internal/meta"
)

// PolicyScanner runs periodic A1 candidate selection and task enqueue.
type PolicyScanner struct {
	store           *meta.Store
	period          time.Duration
	ageThresholdSec int
	maxObjects      int
}

// NewPolicyScanner creates periodic scanner for age-based tiering.
func NewPolicyScanner(store *meta.Store, period time.Duration, ageThresholdSec, maxObjects int) *PolicyScanner {
	if period <= 0 {
		period = 5 * time.Minute
	}
	if maxObjects <= 0 {
		maxObjects = 200
	}
	return &PolicyScanner{
		store:           store,
		period:          period,
		ageThresholdSec: ageThresholdSec,
		maxObjects:      maxObjects,
	}
}

// Run starts scanner loop until context cancellation.
func (s *PolicyScanner) Run(ctx context.Context) error {
	if s.store == nil {
		return nil
	}

	ticker := time.NewTicker(s.period)
	defer ticker.Stop()

	runOnce := func() {
		count, err := s.store.EnqueueTieringCandidatesA1(ctx, s.ageThresholdSec, s.maxObjects)
		if err != nil {
			log.Printf("[TieringPolicy] A1 scan failed: %v", err)
			return
		}
		if count > 0 {
			log.Printf("[TieringPolicy] A1 enqueued %d tasks", count)
		}
	}

	runOnce()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			runOnce()
		}
	}
}
