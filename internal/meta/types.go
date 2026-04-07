package meta

import "time"

// NodeHeartbeatSnapshot is an admin-facing view of node liveness/load.
type NodeHeartbeatSnapshot struct {
	NodeID       string
	LastSeenAt   time.Time
	FreeBytes    int64
	TotalBytes   int64
	IOQueueDepth int
	CPULoad      float64
	Status       string
}

// TieringLeaderState is the admin-visible scanner leadership snapshot.
type TieringLeaderState struct {
	LockKey         int64
	LeaderID        string
	ScannerStatus   string
	AcquiredAt      time.Time
	LastHeartbeatAt time.Time
}

// TieringTask is the metadata record consumed by tiering workers.
type TieringTask struct {
	TaskID      string
	ObjectID    string
	Version     int64
	TaskType    string
	TaskState   string
	Priority    int
	RetryCount  int
	LastError   *string
	ScheduledAt time.Time
	StartedAt   *time.Time
	FinishedAt  *time.Time
}

// ObjectVersionSnapshot describes metadata state used by tiering processor decisions.
type ObjectVersionSnapshot struct {
	ObjectID       string
	CurrentVersion int64
	TaskVersion    int64
	State          string
	Tier           string
}

// ECShardLocation describes one shard placement record.
type ECShardLocation struct {
	ShardIndex int
	NodeID     string
	Path       string
	Status     string
}

// ReplicaLocation describes one HOT replica placement.
type ReplicaLocation struct {
	NodeID string
	Path   string
	Status string
}

// ObjectAdminView is an admin-focused snapshot for a single object.
type ObjectAdminView struct {
	ObjectID         string
	CurrentVersion   int64
	State            string
	CreatedAt        time.Time
	UpdatedAt        time.Time
	Version          *ObjectVersionAdminView
	ReplicaLocations []ReplicaLocationAdminView
	ECShardLocations []ECShardLocationAdminView
}

// ObjectVersionAdminView describes current object version metadata.
type ObjectVersionAdminView struct {
	Version        int64
	SizeBytes      int64
	ChecksumSHA256 string
	Tier           string
	ContentType    *string
	EncodingK      *int
	EncodingM      *int
	CreatedAt      time.Time
}

// ReplicaLocationAdminView is one row from replica_locations.
type ReplicaLocationAdminView struct {
	NodeID string
	Path   string
	Status string
}

// ECShardLocationAdminView is one row from ec_shard_locations.
type ECShardLocationAdminView struct {
	ShardIndex int
	NodeID     string
	Path       string
	Status     string
}
