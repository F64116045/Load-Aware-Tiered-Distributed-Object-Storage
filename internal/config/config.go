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

// Etcd related constants for keys and prefixes
const (
	// EtcdNodePrefix: Prefix for storage node health registration
	// Example: "nodes/health/storage_node_1"
	EtcdNodePrefix = "nodes/health/"

	// EtcdMetadataPrefix: Prefix for storing object metadata
	// Example: "metadata/my_key"
	EtcdMetadataPrefix = "metadata/"

	// EtcdWALPrefix: Prefix for Write-Ahead Log entries during transactions
	// Example: "txn/uuid-123"
	EtcdWALPrefix = "txn/"

)

// ExpectedNodeNames stores the set of valid storage node identifiers
var ExpectedNodeNames = map[string]bool{}

var (
	// WALEnabled controls whether synchronous WAL (Redpanda) is enabled on write path.
	WALEnabled = getEnvBool("WAL_ENABLED", false)
	// MetaEnabled controls whether PostgreSQL-backed metadata service is enabled.
	MetaEnabled = getEnvBool("META_ENABLED", false)
	// MetaAutoMigrate controls whether API runs metadata migration on startup.
	MetaAutoMigrate = getEnvBool("META_AUTO_MIGRATE", false)
	// MetaDriver is the database/sql driver name.
	MetaDriver = getEnv("META_DRIVER", "postgres")
	// MetaSource controls read source selection for metadata: postgres|etcd|auto.
	MetaSource = normalizeMetaSource(getEnv("META_SOURCE", "auto"))
	// MetaDSN is the metadata DB connection string.
	MetaDSN = getEnv("META_DSN", "")
	// MetaMaxOpenConns controls the DB pool max open connections.
	MetaMaxOpenConns = getEnvInt("META_MAX_OPEN_CONNS", 20)
	// MetaMaxIdleConns controls the DB pool max idle connections.
	MetaMaxIdleConns = getEnvInt("META_MAX_IDLE_CONNS", 10)
	// MetaConnMaxLifetime controls DB connection max lifetime.
	MetaConnMaxLifetime = time.Duration(getEnvInt("META_CONN_MAX_LIFETIME_SEC", 300)) * time.Second
	// NodeDiscoverySource controls node discovery source: postgres|etcd|auto.
	NodeDiscoverySource = normalizeNodeDiscoverySource(getEnv("NODE_DISCOVERY_SOURCE", "auto"))
	// NodeHeartbeatInterval controls storage-node heartbeat report interval.
	NodeHeartbeatInterval = time.Duration(getEnvInt("NODE_HEARTBEAT_INTERVAL_SEC", 3)) * time.Second
	// NodeHeartbeatStaleSec defines staleness threshold when listing healthy nodes.
	NodeHeartbeatStaleSec = getEnvInt("NODE_HEARTBEAT_STALE_SEC", 15)
	// AgeThresholdSec defines when HOT objects become eligible for tiering (A1 baseline).
	AgeThresholdSec = getEnvInt("AGE_THRESHOLD_SEC", 3600)
	// TieringPeriodSec defines periodic policy scan interval.
	TieringPeriodSec = getEnvInt("TIERING_PERIOD_SEC", 300)
	// MaxObjectsPerRound caps A1 periodic enqueue count per round.
	MaxObjectsPerRound = getEnvInt("MAX_OBJECTS_PER_ROUND", 200)
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

func normalizeMetaSource(v string) string {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "postgres", "etcd", "auto":
		return strings.ToLower(strings.TrimSpace(v))
	default:
		return "auto"
	}
}

func normalizeNodeDiscoverySource(v string) string {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "postgres", "etcd", "auto":
		return strings.ToLower(strings.TrimSpace(v))
	default:
		return "auto"
	}
}
