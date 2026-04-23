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
	IdleStableRounds       int

	TriggerMode   config.TieringTriggerMode
	PolicyVariant config.TieringPolicyVariant

	AgeThresholdSec int
	MaxObjects      int
	MaxBytes        int64

	HotPressureDiskPct    int
	HotPressureQueueDepth int
	HeartbeatStaleSec     int
	IdleCPUPercent        float64
	IdleMemoryPercent     float64
	IdleIOWaitPercent     float64
	IdleQueueDepth        int

	RepairEnabled    bool
	RepairMaxObjects int

	OldVersionReaperEnabled bool
	OldVersionRetentionN    int
	OldVersionRetentionAge  int
	OldVersionMaxTasks      int

	TaskHistoryReaperEnabled  bool
	TaskHistoryRetentionSec   int
	TaskHistoryReaperMaxTasks int
	TaskHistoryReaperInterval time.Duration
}

type PolicyScanStore interface {
	EnqueueTieringCandidatesStrategyA(ctx context.Context, ageThresholdSec int, maxObjects int) (int, error)
	EnqueueTieringCandidatesStrategyB(ctx context.Context, ageThresholdSec int, maxObjects int, maxBytes int64) (int, error)
	EnqueueTieringCandidatesStrategyC(ctx context.Context, ageThresholdSec int, maxObjects int, maxBytes int64) (int, error)
	EnqueueRepairCandidates(ctx context.Context, maxObjects int) (int, error)
	EnqueueOldVersionGCCandidates(ctx context.Context, keepLatest int, minAgeSec int, maxTasks int) (int, error)
	PurgeTerminalTieringTasks(ctx context.Context, olderThan time.Time, limit int) (int, error)
	ListNodeHeartbeats(ctx context.Context, limit int) ([]meta.NodeHeartbeatSnapshot, error)
}

// PolicyScanner runs policy-based tiering candidate selection and optional repair scans.
type PolicyScanner struct {
	store PolicyScanStore
	cfg   PolicyScannerConfig

	lastThresholdTrigger time.Time
	lastHistoryReaperRun time.Time
	idleStableCount      int
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
	if cfg.IdleStableRounds <= 0 {
		cfg.IdleStableRounds = 3
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
	if cfg.TaskHistoryRetentionSec <= 0 {
		cfg.TaskHistoryRetentionSec = 7 * 24 * 3600
	}
	if cfg.TaskHistoryReaperMaxTasks <= 0 {
		cfg.TaskHistoryReaperMaxTasks = 200
	}
	if cfg.TaskHistoryReaperInterval <= 0 {
		cfg.TaskHistoryReaperInterval = 15 * time.Minute
	}
	if cfg.HeartbeatStaleSec <= 0 {
		cfg.HeartbeatStaleSec = 15
	}
	if cfg.IdleCPUPercent <= 0 {
		cfg.IdleCPUPercent = 70
	}
	if cfg.IdleMemoryPercent <= 0 {
		cfg.IdleMemoryPercent = 80
	}
	if cfg.IdleIOWaitPercent <= 0 {
		cfg.IdleIOWaitPercent = 20
	}
	if cfg.IdleQueueDepth <= 0 {
		cfg.IdleQueueDepth = 16
	}
	if cfg.PolicyVariant == "" {
		cfg.PolicyVariant = config.TieringPolicyA
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
	if s.cfg.PolicyVariant == config.TieringPolicyC {
		// Strategy-C controls admission itself through idle window gating.
		// Threshold loop in this case is only a wake-up tick source.
		s.runPolicyAndRepair(ctx, source)
		return
	}

	idle, reason, err := s.isIdleWindow(ctx)
	if err != nil {
		log.Printf("[TieringPolicy] %s idle-window check failed: %v", source, err)
		return
	}
	if !idle {
		s.idleStableCount = 0
		return
	}
	s.idleStableCount++
	if s.idleStableCount < s.cfg.IdleStableRounds {
		return
	}

	now := time.Now()
	if !s.lastThresholdTrigger.IsZero() && now.Sub(s.lastThresholdTrigger) < s.cfg.ThresholdCooldown {
		return
	}
	s.lastThresholdTrigger = now

	s.runPolicyAndRepair(ctx, fmt.Sprintf("%s:%s stable=%d", source, reason, s.idleStableCount))
}

func (s *PolicyScanner) runPolicyAndRepair(ctx context.Context, source string) {
	tieringAllowed := true
	tieringSource := source
	if s.cfg.PolicyVariant == config.TieringPolicyC {
		ready, gateReason, err := s.strategyCGatePass(ctx)
		if err != nil {
			log.Printf("[TieringPolicy] %s strategy=C gate check failed: %v", source, err)
			tieringAllowed = false
		} else if !ready {
			tieringAllowed = false
		} else {
			tieringSource = fmt.Sprintf("%s:%s", source, gateReason)
		}
	}

	if tieringAllowed {
		count, err := s.enqueueTieringPolicy(ctx)
		if err != nil {
			log.Printf("[TieringPolicy] %s %s scan failed: %v", tieringSource, s.cfg.PolicyVariant, err)
		} else if count > 0 {
			log.Printf("[TieringPolicy] %s %s enqueued %d tasks", tieringSource, s.cfg.PolicyVariant, count)
		}
	}

	if s.cfg.RepairEnabled {
		repairCount, repairErr := s.store.EnqueueRepairCandidates(ctx, s.cfg.RepairMaxObjects)
		if repairErr != nil {
			log.Printf("[TieringPolicy] %s repair scan failed: %v", source, repairErr)
		} else if repairCount > 0 {
			log.Printf("[TieringPolicy] %s repair enqueued %d tasks", source, repairCount)
		}
	}

	if s.cfg.OldVersionReaperEnabled {
		gcCount, gcErr := s.store.EnqueueOldVersionGCCandidates(
			ctx,
			s.cfg.OldVersionRetentionN,
			s.cfg.OldVersionRetentionAge,
			s.cfg.OldVersionMaxTasks,
		)
		if gcErr != nil {
			log.Printf("[TieringPolicy] %s old-version-gc scan failed: %v", source, gcErr)
		} else if gcCount > 0 {
			log.Printf("[TieringPolicy] %s old-version-gc enqueued %d tasks", source, gcCount)
		}
	}

	s.runTaskHistoryReaper(ctx, source)
}

func (s *PolicyScanner) enqueueTieringPolicy(ctx context.Context) (int, error) {
	switch s.cfg.PolicyVariant {
	case config.TieringPolicyB:
		return s.store.EnqueueTieringCandidatesStrategyB(
			ctx,
			s.cfg.AgeThresholdSec,
			s.cfg.MaxObjects,
			s.cfg.MaxBytes,
		)
	case config.TieringPolicyC:
		return s.store.EnqueueTieringCandidatesStrategyC(
			ctx,
			s.cfg.AgeThresholdSec,
			s.cfg.MaxObjects,
			s.cfg.MaxBytes,
		)
	case config.TieringPolicyA:
		fallthrough
	default:
		return s.store.EnqueueTieringCandidatesStrategyA(
			ctx,
			s.cfg.AgeThresholdSec,
			s.cfg.MaxObjects,
		)
	}
}

func (s *PolicyScanner) runTaskHistoryReaper(ctx context.Context, source string) {
	if !s.cfg.TaskHistoryReaperEnabled || s.cfg.TaskHistoryRetentionSec <= 0 || s.cfg.TaskHistoryReaperMaxTasks <= 0 {
		return
	}
	now := time.Now()
	if !s.lastHistoryReaperRun.IsZero() && now.Sub(s.lastHistoryReaperRun) < s.cfg.TaskHistoryReaperInterval {
		return
	}
	s.lastHistoryReaperRun = now

	olderThan := now.Add(-time.Duration(s.cfg.TaskHistoryRetentionSec) * time.Second)
	purged, err := s.store.PurgeTerminalTieringTasks(ctx, olderThan, s.cfg.TaskHistoryReaperMaxTasks)
	if err != nil {
		log.Printf("[TieringPolicy] %s task-history reaper failed: %v", source, err)
		return
	}
	if purged > 0 {
		log.Printf(
			"[TieringPolicy] %s task-history reaper purged %d terminal tasks older_than=%s",
			source,
			purged,
			olderThan.UTC().Format(time.RFC3339),
		)
	}
}

func (s *PolicyScanner) strategyCGatePass(ctx context.Context) (bool, string, error) {
	idle, reason, err := s.isIdleWindow(ctx)
	if err != nil {
		return false, "", err
	}
	if !idle {
		s.idleStableCount = 0
		return false, reason, nil
	}

	s.idleStableCount++
	if s.idleStableCount < s.cfg.IdleStableRounds {
		return false, fmt.Sprintf("%s stable=%d/%d", reason, s.idleStableCount, s.cfg.IdleStableRounds), nil
	}

	now := time.Now()
	if !s.lastThresholdTrigger.IsZero() && now.Sub(s.lastThresholdTrigger) < s.cfg.ThresholdCooldown {
		return false, fmt.Sprintf("cooldown remain=%s", s.cfg.ThresholdCooldown-now.Sub(s.lastThresholdTrigger)), nil
	}
	s.lastThresholdTrigger = now
	return true, fmt.Sprintf("%s stable=%d", reason, s.idleStableCount), nil
}

func (s *PolicyScanner) isIdleWindow(ctx context.Context) (bool, string, error) {
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

		cpuPercent := n.CPULoad * 100
		if s.cfg.IdleCPUPercent > 0 && cpuPercent >= s.cfg.IdleCPUPercent {
			return false, fmt.Sprintf("cpu_busy node=%s cpu_pct=%.2f threshold=%.2f", n.NodeID, cpuPercent, s.cfg.IdleCPUPercent), nil
		}
		if s.cfg.IdleQueueDepth > 0 && n.IOQueueDepth >= s.cfg.IdleQueueDepth {
			return false, fmt.Sprintf("queue_busy node=%s depth=%d threshold=%d", n.NodeID, n.IOQueueDepth, s.cfg.IdleQueueDepth), nil
		}
		if s.cfg.IdleIOWaitPercent > 0 && n.DiskIOWaitPct >= s.cfg.IdleIOWaitPercent {
			return false, fmt.Sprintf("iowait_busy node=%s iowait_pct=%.2f threshold=%.2f", n.NodeID, n.DiskIOWaitPct, s.cfg.IdleIOWaitPercent), nil
		}
		if s.cfg.IdleMemoryPercent > 0 && n.MemoryUsedPct >= s.cfg.IdleMemoryPercent {
			return false, fmt.Sprintf("memory_busy node=%s mem_pct=%.2f threshold=%.2f", n.NodeID, n.MemoryUsedPct, s.cfg.IdleMemoryPercent), nil
		}
	}

	if liveCount == 0 {
		return false, "no_live_nodes", nil
	}
	return true, "idle_window", nil
}
