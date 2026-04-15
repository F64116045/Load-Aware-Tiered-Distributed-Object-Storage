package tiering

import (
	"context"
	"fmt"
	"log"
	"time"

	"hybrid_distributed_store/internal/config"
	"hybrid_distributed_store/internal/meta"
)

type PolicyScannerConfig struct {
	PeriodicInterval       time.Duration
	ThresholdCheckInterval time.Duration
	ThresholdCooldown      time.Duration

	TriggerMode   config.TieringTriggerMode
	PolicyVariant config.TieringPolicyVariant

	AgeThresholdSec    int
	SizeThresholdBytes int64
	MaxObjects         int
	MaxBytes           int64

	HotPressureDiskPct    int
	HotPressureQueueDepth int
	HeartbeatStaleSec     int

	RepairEnabled    bool
	RepairMaxObjects int

	OldVersionReaperEnabled bool
	OldVersionRetentionN    int
	OldVersionRetentionAge  int
	OldVersionMaxTasks      int
}

type PolicyScanStore interface {
	EnqueueTieringCandidatesA1(ctx context.Context, ageThresholdSec int, maxObjects int) (int, error)
	EnqueueTieringCandidatesA2(ctx context.Context, ageThresholdSec int, sizeThresholdBytes int64, maxObjects int) (int, error)
	EnqueueTieringCandidatesA3(ctx context.Context, ageThresholdSec int, maxObjects int, maxBytes int64) (int, error)
	EnqueueRepairCandidates(ctx context.Context, maxObjects int) (int, error)
	EnqueueOldVersionGCCandidates(ctx context.Context, keepLatest int, minAgeSec int, maxTasks int) (int, error)
	ListNodeHeartbeats(ctx context.Context, limit int) ([]meta.NodeHeartbeatSnapshot, error)
}

// PolicyScanner runs policy-based tiering candidate selection and optional repair scans.
type PolicyScanner struct {
	store PolicyScanStore
	cfg   PolicyScannerConfig

	lastThresholdTrigger time.Time
}

func NewPolicyScanner(store PolicyScanStore, cfg PolicyScannerConfig) *PolicyScanner {
	if cfg.PeriodicInterval <= 0 {
		cfg.PeriodicInterval = 5 * time.Minute
	}
	if cfg.ThresholdCheckInterval <= 0 {
		cfg.ThresholdCheckInterval = 10 * time.Second
	}
	if cfg.ThresholdCooldown <= 0 {
		cfg.ThresholdCooldown = 60 * time.Second
	}
	if cfg.MaxObjects <= 0 {
		cfg.MaxObjects = 200
	}
	if cfg.RepairMaxObjects <= 0 {
		cfg.RepairMaxObjects = 200
	}
	if cfg.OldVersionRetentionN <= 0 {
		cfg.OldVersionRetentionN = 1
	}
	if cfg.OldVersionMaxTasks <= 0 {
		cfg.OldVersionMaxTasks = 200
	}
	if cfg.HeartbeatStaleSec <= 0 {
		cfg.HeartbeatStaleSec = 15
	}
	if cfg.PolicyVariant == "" {
		cfg.PolicyVariant = config.TieringPolicyA1
	}
	if cfg.TriggerMode == "" {
		cfg.TriggerMode = config.TieringTriggerPeriodic
	}

	return &PolicyScanner{
		store: store,
		cfg:   cfg,
	}
}

// Run starts scanner loop until context cancellation.
func (s *PolicyScanner) Run(ctx context.Context) error {
	if s == nil || s.store == nil {
		return nil
	}

	var periodicTicker *time.Ticker
	var thresholdTicker *time.Ticker
	var periodicTick <-chan time.Time
	var thresholdTick <-chan time.Time

	if s.cfg.TriggerMode == config.TieringTriggerPeriodic || s.cfg.TriggerMode == config.TieringTriggerHybrid {
		periodicTicker = time.NewTicker(s.cfg.PeriodicInterval)
		periodicTick = periodicTicker.C
		defer periodicTicker.Stop()

		s.runPolicyAndRepair(ctx, "periodic:init")
	}

	if s.cfg.TriggerMode == config.TieringTriggerThreshold || s.cfg.TriggerMode == config.TieringTriggerHybrid {
		thresholdTicker = time.NewTicker(s.cfg.ThresholdCheckInterval)
		thresholdTick = thresholdTicker.C
		defer thresholdTicker.Stop()

		s.runThresholdPass(ctx, "threshold:init")
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-periodicTick:
			s.runPolicyAndRepair(ctx, "periodic")
		case <-thresholdTick:
			s.runThresholdPass(ctx, "threshold")
		}
	}
}

func (s *PolicyScanner) runThresholdPass(ctx context.Context, source string) {
	underPressure, reason, err := s.isUnderPressure(ctx)
	if err != nil {
		log.Printf("[TieringPolicy] %s pressure check failed: %v", source, err)
		return
	}
	if !underPressure {
		return
	}

	now := time.Now()
	if !s.lastThresholdTrigger.IsZero() && now.Sub(s.lastThresholdTrigger) < s.cfg.ThresholdCooldown {
		return
	}
	s.lastThresholdTrigger = now

	s.runPolicyAndRepair(ctx, fmt.Sprintf("%s:%s", source, reason))
}

func (s *PolicyScanner) runPolicyAndRepair(ctx context.Context, source string) {
	count, err := s.enqueueTieringPolicy(ctx)
	if err != nil {
		log.Printf("[TieringPolicy] %s %s scan failed: %v", source, s.cfg.PolicyVariant, err)
	} else if count > 0 {
		log.Printf("[TieringPolicy] %s %s enqueued %d tasks", source, s.cfg.PolicyVariant, count)
	}

	if !s.cfg.RepairEnabled {
		return
	}
	repairCount, repairErr := s.store.EnqueueRepairCandidates(ctx, s.cfg.RepairMaxObjects)
	if repairErr != nil {
		log.Printf("[TieringPolicy] %s repair scan failed: %v", source, repairErr)
		return
	}
	if repairCount > 0 {
		log.Printf("[TieringPolicy] %s repair enqueued %d tasks", source, repairCount)
	}

	if !s.cfg.OldVersionReaperEnabled {
		return
	}
	gcCount, gcErr := s.store.EnqueueOldVersionGCCandidates(
		ctx,
		s.cfg.OldVersionRetentionN,
		s.cfg.OldVersionRetentionAge,
		s.cfg.OldVersionMaxTasks,
	)
	if gcErr != nil {
		log.Printf("[TieringPolicy] %s old-version-gc scan failed: %v", source, gcErr)
		return
	}
	if gcCount > 0 {
		log.Printf("[TieringPolicy] %s old-version-gc enqueued %d tasks", source, gcCount)
	}
}

func (s *PolicyScanner) enqueueTieringPolicy(ctx context.Context) (int, error) {
	switch s.cfg.PolicyVariant {
	case config.TieringPolicyA2:
		return s.store.EnqueueTieringCandidatesA2(
			ctx,
			s.cfg.AgeThresholdSec,
			s.cfg.SizeThresholdBytes,
			s.cfg.MaxObjects,
		)
	case config.TieringPolicyA3:
		return s.store.EnqueueTieringCandidatesA3(
			ctx,
			s.cfg.AgeThresholdSec,
			s.cfg.MaxObjects,
			s.cfg.MaxBytes,
		)
	case config.TieringPolicyA1:
		fallthrough
	default:
		return s.store.EnqueueTieringCandidatesA1(
			ctx,
			s.cfg.AgeThresholdSec,
			s.cfg.MaxObjects,
		)
	}
}

func (s *PolicyScanner) isUnderPressure(ctx context.Context) (bool, string, error) {
	nodes, err := s.store.ListNodeHeartbeats(ctx, 1000)
	if err != nil {
		return false, "", err
	}
	if len(nodes) == 0 {
		return false, "no_heartbeats", nil
	}

	staleWindow := time.Duration(s.cfg.HeartbeatStaleSec) * time.Second
	now := time.Now()
	liveCount := 0

	for _, n := range nodes {
		if n.Status != "UP" {
			continue
		}
		if staleWindow > 0 && now.Sub(n.LastSeenAt) > staleWindow {
			continue
		}
		liveCount++

		if s.cfg.HotPressureQueueDepth > 0 && n.IOQueueDepth >= s.cfg.HotPressureQueueDepth {
			return true, fmt.Sprintf("queue_depth node=%s depth=%d threshold=%d", n.NodeID, n.IOQueueDepth, s.cfg.HotPressureQueueDepth), nil
		}

		if s.cfg.HotPressureDiskPct > 0 && n.TotalBytes > 0 {
			usedBytes := n.TotalBytes - n.FreeBytes
			if usedBytes < 0 {
				usedBytes = 0
			}
			usedPct := int((float64(usedBytes) / float64(n.TotalBytes)) * 100)
			if usedPct >= s.cfg.HotPressureDiskPct {
				return true, fmt.Sprintf("disk_pct node=%s used_pct=%d threshold=%d", n.NodeID, usedPct, s.cfg.HotPressureDiskPct), nil
			}
		}
	}

	if liveCount == 0 {
		return false, "no_live_nodes", nil
	}
	return false, "below_threshold", nil
}
