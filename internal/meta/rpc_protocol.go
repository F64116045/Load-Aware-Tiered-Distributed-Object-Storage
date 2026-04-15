package meta

import (
	"encoding/json"
	"time"
)

const (
	rpcMethodPing                        = "ping"
	rpcMethodUpsertNodeHeartbeat         = "upsert_node_heartbeat"
	rpcMethodListHealthyNodeIDs          = "list_healthy_node_ids"
	rpcMethodListNodeHeartbeats          = "list_node_heartbeats"
	rpcMethodUpsertTieringLeaderState    = "upsert_tiering_leader_state"
	rpcMethodMarkTieringLeaderStopped    = "mark_tiering_leader_stopped"
	rpcMethodGetTieringLeaderState       = "get_tiering_leader_state"
	rpcMethodTryAcquireLeaderLock        = "try_acquire_leader_lock"
	rpcMethodLeaderLockPing              = "leader_lock_ping"
	rpcMethodLeaderLockRelease           = "leader_lock_release"
	rpcMethodUpsertNormalizedMetadata    = "upsert_normalized_metadata"
	rpcMethodGetNormalizedMetadata       = "get_normalized_metadata"
	rpcMethodDeleteNormalizedMetadata    = "delete_normalized_metadata"
	rpcMethodGetObjectAdminView          = "get_object_admin_view"
	rpcMethodEnqueueTieringTask          = "enqueue_tiering_task"
	rpcMethodListTieringTasks            = "list_tiering_tasks"
	rpcMethodListTieringTaskStateCount   = "list_tiering_task_state_counts"
	rpcMethodRequeueTieringTaskNow       = "requeue_tiering_task_now"
	rpcMethodCancelTieringTask           = "cancel_tiering_task"
	rpcMethodClaimNextTieringTask        = "claim_next_tiering_task"
	rpcMethodMarkTieringTaskDone         = "mark_tiering_task_done"
	rpcMethodMarkTieringTaskRetry        = "mark_tiering_task_retry"
	rpcMethodMarkTieringTaskFailed       = "mark_tiering_task_failed"
	rpcMethodEnqueueTieringCandidatesA1  = "enqueue_tiering_candidates_a1"
	rpcMethodEnqueueTieringCandidatesA2  = "enqueue_tiering_candidates_a2"
	rpcMethodEnqueueTieringCandidatesA3  = "enqueue_tiering_candidates_a3"
	rpcMethodEnqueueRepairCandidates     = "enqueue_repair_candidates"
	rpcMethodEnqueueOldVersionGCCands    = "enqueue_old_version_gc_candidates"
	rpcMethodGetObjectVersionSnapshot    = "get_object_version_snapshot"
	rpcMethodGetObjectVersionGCView      = "get_object_version_gc_view"
	rpcMethodPurgeObjectVersionMetadata  = "purge_object_version_metadata"
	rpcMethodMarkObjectMigrating         = "mark_object_migrating"
	rpcMethodPromoteObjectVersionToEC    = "promote_object_version_to_ec"
	rpcMethodListActiveReplicaLocations  = "list_active_replica_locations"
	rpcMethodUpsertReplicaLocations      = "upsert_replica_locations"
	rpcMethodMarkReplicaLocationsDeleted = "mark_replica_locations_deleted"
	rpcMethodGetTieringIndexStats        = "get_tiering_index_stats"
)

type rpcRequest struct {
	Method string          `json:"method"`
	Params json.RawMessage `json:"params,omitempty"`
}

type rpcResponse struct {
	OK     bool            `json:"ok"`
	Error  string          `json:"error,omitempty"`
	Result json.RawMessage `json:"result,omitempty"`
}

type rpcNodeHeartbeatArgs struct {
	NodeID       string  `json:"node_id"`
	FreeBytes    int64   `json:"free_bytes"`
	TotalBytes   int64   `json:"total_bytes"`
	IOQueueDepth int     `json:"io_queue_depth"`
	CPULoad      float64 `json:"cpu_load"`
	Status       string  `json:"status"`
}

type rpcListHealthyNodeIDsArgs struct {
	StaleSec int `json:"stale_sec"`
}

type rpcListNodeHeartbeatsArgs struct {
	Limit int `json:"limit"`
}

type rpcLeaderStateUpsertArgs struct {
	LockKey  int64  `json:"lock_key"`
	LeaderID string `json:"leader_id"`
	Status   string `json:"status"`
}

type rpcLeaderStateGetArgs struct {
	LockKey int64 `json:"lock_key"`
}

type rpcTryAcquireLeaderLockArgs struct {
	Key int64 `json:"key"`
}

type rpcTryAcquireLeaderLockResult struct {
	Acquired bool   `json:"acquired"`
	Token    string `json:"token,omitempty"`
}

type rpcLeaderLockTokenArgs struct {
	Token string `json:"token"`
}

type rpcUpsertNormalizedMetadataArgs struct {
	ObjectID string                 `json:"object_id"`
	Metadata map[string]interface{} `json:"metadata"`
}

type rpcObjectIDArgs struct {
	ObjectID string `json:"object_id"`
}

type rpcEnqueueTieringTaskArgs struct {
	TaskID      string    `json:"task_id"`
	ObjectID    string    `json:"object_id"`
	Version     int64     `json:"version"`
	TaskType    string    `json:"task_type"`
	Priority    int       `json:"priority"`
	ScheduledAt time.Time `json:"scheduled_at"`
}

type rpcListTieringTasksArgs struct {
	TaskState string `json:"task_state"`
	TaskType  string `json:"task_type"`
	Limit     int    `json:"limit"`
}

type rpcListTieringTaskStateCountsArgs struct {
	TaskType string `json:"task_type"`
}

type rpcTaskIDArgs struct {
	TaskID string `json:"task_id"`
}

type rpcCancelTieringTaskArgs struct {
	TaskID string `json:"task_id"`
	Reason string `json:"reason"`
}

type rpcClaimNextTieringTaskArgs struct {
	TaskType string `json:"task_type"`
}

type rpcMarkTieringTaskRetryArgs struct {
	TaskID    string    `json:"task_id"`
	LastError string    `json:"last_error"`
	NextRunAt time.Time `json:"next_run_at"`
}

type rpcMarkTieringTaskFailedArgs struct {
	TaskID    string `json:"task_id"`
	LastError string `json:"last_error"`
}

type rpcEnqueueTieringCandidatesA1Args struct {
	AgeThresholdSec int `json:"age_threshold_sec"`
	MaxObjects      int `json:"max_objects"`
}

type rpcEnqueueTieringCandidatesA2Args struct {
	AgeThresholdSec    int   `json:"age_threshold_sec"`
	SizeThresholdBytes int64 `json:"size_threshold_bytes"`
	MaxObjects         int   `json:"max_objects"`
}

type rpcEnqueueTieringCandidatesA3Args struct {
	AgeThresholdSec int   `json:"age_threshold_sec"`
	MaxObjects      int   `json:"max_objects"`
	MaxBytes        int64 `json:"max_bytes"`
}

type rpcEnqueueRepairCandidatesArgs struct {
	MaxObjects int `json:"max_objects"`
}

type rpcEnqueueOldVersionGCCandidatesArgs struct {
	KeepLatest int `json:"keep_latest"`
	MinAgeSec  int `json:"min_age_sec"`
	MaxTasks   int `json:"max_tasks"`
}

type rpcGetObjectVersionSnapshotArgs struct {
	ObjectID    string `json:"object_id"`
	TaskVersion int64  `json:"task_version"`
}

type rpcGetObjectVersionGCViewArgs struct {
	ObjectID string `json:"object_id"`
	Version  int64  `json:"version"`
}

type rpcPurgeObjectVersionMetadataArgs struct {
	ObjectID string `json:"object_id"`
	Version  int64  `json:"version"`
}

type rpcMarkObjectMigratingArgs struct {
	ObjectID string `json:"object_id"`
	Version  int64  `json:"version"`
}

type rpcPromoteObjectVersionToECArgs struct {
	ObjectID  string            `json:"object_id"`
	Version   int64             `json:"version"`
	Checksum  string            `json:"checksum"`
	K         int               `json:"k"`
	M         int               `json:"m"`
	Locations []ECShardLocation `json:"locations"`
}

type rpcListActiveReplicaLocationsArgs struct {
	ObjectID string `json:"object_id"`
	Version  int64  `json:"version"`
}

type rpcUpsertReplicaLocationsArgs struct {
	ObjectID string   `json:"object_id"`
	Version  int64    `json:"version"`
	NodeIDs  []string `json:"node_ids"`
}

type rpcMarkReplicaLocationsDeletedArgs struct {
	ObjectID string   `json:"object_id"`
	Version  int64    `json:"version"`
	NodeIDs  []string `json:"node_ids"`
}

type rpcBoolResult struct {
	Value bool `json:"value"`
}

type rpcIntResult struct {
	Value int `json:"value"`
}
