package tiering

import (
	"context"
	"log"
	"time"

	"hybrid_distributed_store/internal/meta"
)

// PolicyScanner runs periodic A1 candidate selection and task enqueue.
type PolicyScanner struct {
	store           meta.Repository
	period          time.Duration
	ageThresholdSec int
	maxObjects      int
	repairEnabled   bool
	repairMax       int
}

// NewPolicyScanner creates periodic scanner for age-based tiering.
func NewPolicyScanner(
	store meta.Repository,
	period time.Duration,
	ageThresholdSec, maxObjects int,
	repairEnabled bool,
	repairMax int,
) *PolicyScanner {
	if period <= 0 {
		period = 5 * time.Minute
	}
	if maxObjects <= 0 {
		maxObjects = 200
	}
	if repairMax <= 0 {
		repairMax = 200
	}
	return &PolicyScanner{
		store:           store,
		period:          period,
		ageThresholdSec: ageThresholdSec,
		maxObjects:      maxObjects,
		repairEnabled:   repairEnabled,
		repairMax:       repairMax,
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
		} else if count > 0 {
			log.Printf("[TieringPolicy] A1 enqueued %d tasks", count)
		}

		if !s.repairEnabled {
			return
		}
		repairCount, repairErr := s.store.EnqueueRepairCandidates(ctx, s.repairMax)
		if repairErr != nil {
			log.Printf("[TieringPolicy] repair scan failed: %v", repairErr)
			return
		}
		if repairCount > 0 {
			log.Printf("[TieringPolicy] repair enqueued %d tasks", repairCount)
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
