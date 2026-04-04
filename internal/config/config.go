package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// Colors for terminal output
var Colors = map[string]string{
	"GREEN":   "\033[92m",
	"RED":     "\033[91m",
	"YELLOW":  "\033[93m",
	"CYAN":    "\033[96m",
	"MAGENTA": "\033[95m",
	"BOLD":    "\033[1m",
	"RESET":   "\033[0m",
}

// Erasure Coding parameters (Reed-Solomon)
const (
	K = 4 // Number of Data Shards
	M = 2 // Number of Parity Shards
)

// StorageStrategy defines the method used for data persistence
type StorageStrategy string

const (
	StrategyReplication StorageStrategy = "replication"
	StrategyEC          StorageStrategy = "ec"
)

// ExpectedNodeNames stores the set of valid storage node identifiers
var ExpectedNodeNames = map[string]bool{}

var (
	// MetaEnabled controls whether metadata repository integration is enabled.
	MetaEnabled = getEnvBool("META_ENABLED", false)
	// MetaEndpoint points to remote metadata service (RPC). Empty means local backend.
	MetaEndpoint = getEnv("META_ENDPOINT", "")
	// MetaRequireEndpoint enforces remote metadata service usage when metadata is enabled.
	MetaRequireEndpoint = getEnvBool("META_REQUIRE_ENDPOINT", false)
	// MetaRPCAuthToken is an optional shared-secret token for metadata RPC calls.
	MetaRPCAuthToken = getEnv("META_RPC_AUTH_TOKEN", "")
	// MetaBackend selects metadata backend implementation.
	MetaBackend = getEnv("META_BACKEND", "postgres")
	// MetaAutoMigrate controls whether API runs metadata migration on startup.
	MetaAutoMigrate = getEnvBool("META_AUTO_MIGRATE", false)
	// MetaDriver is the database/sql driver name.
	MetaDriver = getEnv("META_DRIVER", "postgres")
	// MetaDSN is the metadata DB connection string.
	MetaDSN = getEnv("META_DSN", "")
	// MetaMaxOpenConns controls the DB pool max open connections.
	MetaMaxOpenConns = getEnvInt("META_MAX_OPEN_CONNS", 20)
	// MetaMaxIdleConns controls the DB pool max idle connections.
	MetaMaxIdleConns = getEnvInt("META_MAX_IDLE_CONNS", 10)
	// MetaConnMaxLifetime controls DB connection max lifetime.
	MetaConnMaxLifetime = time.Duration(getEnvInt("META_CONN_MAX_LIFETIME_SEC", 300)) * time.Second
	// NodeHeartbeatInterval controls storage-node heartbeat report interval.
	NodeHeartbeatInterval = time.Duration(getEnvInt("NODE_HEARTBEAT_INTERVAL_SEC", 3)) * time.Second
	// NodeHeartbeatStaleSec defines staleness threshold when listing healthy nodes.
	NodeHeartbeatStaleSec = getEnvInt("NODE_HEARTBEAT_STALE_SEC", 15)
	// HotReplicaCount is the number of replicas targeted by foreground hot writes.
	HotReplicaCount = getEnvInt("HOT_REPLICA_COUNT", 3)
	// HotWriteQuorum is the minimum number of successful replica writes for ACK.
	HotWriteQuorum = getEnvInt("HOT_WRITE_QUORUM", 2)
	// AgeThresholdSec defines when HOT objects become eligible for tiering (A1 baseline).
	AgeThresholdSec = getEnvInt("AGE_THRESHOLD_SEC", 3600)
	// TieringPeriodSec defines periodic policy scan interval.
	TieringPeriodSec = getEnvInt("TIERING_PERIOD_SEC", 300)
	// MaxObjectsPerRound caps A1 periodic enqueue count per round.
	MaxObjectsPerRound = getEnvInt("MAX_OBJECTS_PER_ROUND", 200)
	// RepairScanEnabled controls periodic repair candidate scanning.
	RepairScanEnabled = getEnvBool("REPAIR_SCAN_ENABLED", true)
	// RepairMaxObjectsPerRound caps periodic repair enqueue count per round.
	RepairMaxObjectsPerRound = getEnvInt("REPAIR_MAX_OBJECTS_PER_ROUND", 200)
	// TieringTaskMaxRetryCount caps automatic retries before task becomes FAILED.
	TieringTaskMaxRetryCount = getEnvInt("TIERING_TASK_MAX_RETRY_COUNT", 8)
	// TieringPolicyLeaderLockKey is the advisory lock key for scanner leader election.
	TieringPolicyLeaderLockKey = int64(getEnvInt("TIERING_POLICY_LEADER_LOCK_KEY", 42042))
	// TieringLeaderStaleSec marks leader heartbeat stale threshold for admin observability.
	TieringLeaderStaleSec = getEnvInt("TIERING_LEADER_STALE_SEC", 10)
)

func init() {
	// Load expected node names from environment variable or use default fallback
	names := os.Getenv("NODE_NAMES_CSV")
	if names == "" {
		names = "storage_node_1,storage_node_2,storage_node_3,storage_node_4,storage_node_5,storage_node_6"
	}

	for _, name := range strings.Split(names, ",") {
		ExpectedNodeNames[name] = true
	}
}

func getEnv(key, fallback string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	return v
}

func getEnvInt(key string, fallback int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return parsed
}

func getEnvBool(key string, fallback bool) bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv(key)))
	if v == "" {
		return fallback
	}
	switch v {
	case "1", "true", "yes", "y", "on":
		return true
	case "0", "false", "no", "n", "off":
		return false
	default:
		return fallback
	}
}
