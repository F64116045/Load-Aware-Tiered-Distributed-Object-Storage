package meta

import "time"

const (
	tiKVPrefixObject  = "obj/"
	tiKVPrefixObjVer  = "objv/"
	tiKVPrefixReplica = "repl/"
	tiKVPrefixECShard = "ec/"
	tiKVPrefixTask    = "task/"
	tiKVPrefixHB      = "hb/"
	tiKVPrefixLeader  = "leader/"
	tiKVPrefixLk      = "leader_lock/"
)

type tiKVObjectRecord struct {
	ObjectID       string    `json:"object_id"`
	TenantID       string    `json:"tenant_id"`
	CurrentVersion int64     `json:"current_version"`
	State          string    `json:"state"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

type tiKVObjectVersionRecord struct {
	ObjectID       string    `json:"object_id"`
	Version        int64     `json:"version"`
	SizeBytes      int64     `json:"size_bytes"`
	ChecksumSHA256 string    `json:"checksum_sha256"`
	Tier           string    `json:"tier"`
	ContentType    *string   `json:"content_type,omitempty"`
	EncodingK      *int      `json:"encoding_k,omitempty"`
	EncodingM      *int      `json:"encoding_m,omitempty"`
	CreatedAt      time.Time `json:"created_at"`
}

type tiKVReplicaRecord struct {
	ObjectID string `json:"object_id"`
	Version  int64  `json:"version"`
	NodeID   string `json:"node_id"`
	Path     string `json:"path"`
	Status   string `json:"status"`
}

type tiKVECShardRecord struct {
	ObjectID   string `json:"object_id"`
	Version    int64  `json:"version"`
	ShardIndex int    `json:"shard_index"`
	NodeID     string `json:"node_id"`
	Path       string `json:"path"`
	Status     string `json:"status"`
}

type tiKVTaskRecord struct {
	TaskID      string     `json:"task_id"`
	ObjectID    string     `json:"object_id"`
	Version     int64      `json:"version"`
	TaskType    string     `json:"task_type"`
	TaskState   string     `json:"task_state"`
	Priority    int        `json:"priority"`
	RetryCount  int        `json:"retry_count"`
	LastError   *string    `json:"last_error,omitempty"`
	ScheduledAt time.Time  `json:"scheduled_at"`
	StartedAt   *time.Time `json:"started_at,omitempty"`
	FinishedAt  *time.Time `json:"finished_at,omitempty"`
}
