package meta

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// LeaderLock abstracts scanner leader lock lifecycle.
type LeaderLock interface {
	Ping(ctx context.Context) error
	Release(ctx context.Context) error
}

// Repository defines metadata capabilities consumed by runtime components.
// This is the extension point for metadata backend implementations.
type Repository interface {
	Ping(ctx context.Context) error
	Close() error

	UpsertNodeHeartbeat(ctx context.Context, nodeID string, freeBytes int64, totalBytes int64, ioQueueDepth int, cpuLoad float64, status string) error
	ListHealthyNodeIDs(ctx context.Context, staleSec int) ([]string, error)
	ListNodeHeartbeats(ctx context.Context, limit int) ([]NodeHeartbeatSnapshot, error)

	UpsertTieringLeaderState(ctx context.Context, lockKey int64, leaderID, status string) error
	MarkTieringLeaderStopped(ctx context.Context, lockKey int64, leaderID, status string) error
	GetTieringLeaderState(ctx context.Context, lockKey int64) (*TieringLeaderState, error)
	TryAcquireLeaderLock(ctx context.Context, key int64) (LeaderLock, bool, error)

	UpsertNormalizedMetadata(ctx context.Context, objectID string, metadata map[string]interface{}) error
	GetNormalizedMetadata(ctx context.Context, objectID string) (map[string]interface{}, error)
	DeleteNormalizedMetadata(ctx context.Context, objectID string) error

	GetObjectAdminView(ctx context.Context, objectID string) (*ObjectAdminView, error)
	GetTieringIndexStats(ctx context.Context) (*TieringIndexStats, error)

	EnqueueTieringTask(ctx context.Context, taskID, objectID string, version int64, taskType string, priority int, scheduledAt time.Time) error
	ListTieringTasks(ctx context.Context, taskState, taskType string, limit int) ([]TieringTask, error)
	ListTieringTaskStateCounts(ctx context.Context, taskType string) (map[string]int64, error)
	RequeueTieringTaskNow(ctx context.Context, taskID string) (bool, error)
	CancelTieringTask(ctx context.Context, taskID, reason string) (bool, error)
	ClaimNextTieringTask(ctx context.Context, taskType string) (*TieringTask, error)
	MarkTieringTaskDone(ctx context.Context, taskID string) error
	MarkTieringTaskRetry(ctx context.Context, taskID, lastErr string, nextRunAt time.Time) error
	MarkTieringTaskFailed(ctx context.Context, taskID, lastErr string) error

	EnqueueTieringCandidatesA1(ctx context.Context, ageThresholdSec int, maxObjects int) (int, error)
	EnqueueTieringCandidatesA2(ctx context.Context, ageThresholdSec int, sizeThresholdBytes int64, maxObjects int) (int, error)
	EnqueueTieringCandidatesA3(ctx context.Context, ageThresholdSec int, maxObjects int, maxBytes int64) (int, error)
	EnqueueRepairCandidates(ctx context.Context, maxObjects int) (int, error)
	EnqueueOldVersionGCCandidates(ctx context.Context, keepLatest int, minAgeSec int, maxTasks int) (int, error)
	GetObjectVersionSnapshot(ctx context.Context, objectID string, taskVersion int64) (*ObjectVersionSnapshot, error)
	GetObjectVersionGCView(ctx context.Context, objectID string, version int64) (*ObjectVersionGCView, error)
	PurgeObjectVersionMetadata(ctx context.Context, objectID string, version int64) error
	MarkObjectMigrating(ctx context.Context, objectID string, version int64) error
	PromoteObjectVersionToEC(ctx context.Context, objectID string, version int64, checksum string, k int, m int, locations []ECShardLocation) error
	ListActiveReplicaLocations(ctx context.Context, objectID string, version int64) ([]ReplicaLocation, error)
	UpsertReplicaLocations(ctx context.Context, objectID string, version int64, nodeIDs []string) error
	MarkReplicaLocationsDeleted(ctx context.Context, objectID string, version int64, nodeIDs []string) error
}

// NewRepository creates metadata repository by configured backend.
func NewRepository(cfg Config) (Repository, error) {
	if !cfg.Enabled {
		return &TiKVStore{}, nil
	}

	endpoint := strings.TrimSpace(cfg.Endpoint)
	if cfg.RequireEndpoint && endpoint == "" {
		return nil, fmt.Errorf("meta endpoint is required but empty")
	}
	if endpoint != "" {
		return NewRPCClient(endpoint, cfg.AuthToken), nil
	}
	if strings.TrimSpace(cfg.DSN) == "" {
		return nil, fmt.Errorf("meta dsn is required when META_ENDPOINT is empty")
	}
	return NewTiKVStore(cfg)
}
