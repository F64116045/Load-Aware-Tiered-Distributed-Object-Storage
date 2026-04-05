package config

import (
	"os"
	"sort"
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

type TieringPolicyVariant string

const (
	TieringPolicyA1 TieringPolicyVariant = "A1"
	TieringPolicyA2 TieringPolicyVariant = "A2"
	TieringPolicyA3 TieringPolicyVariant = "A3"
)

type TieringTriggerMode string

const (
	TieringTriggerPeriodic  TieringTriggerMode = "periodic"
	TieringTriggerThreshold TieringTriggerMode = "threshold"
	TieringTriggerHybrid    TieringTriggerMode = "hybrid"
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
	// MaxBytesPerRound caps total bytes selected in one A3 round (<=0 means unlimited).
	MaxBytesPerRound = getEnvInt64("MAX_BYTES_PER_ROUND", 1073741824)
	// SizeThresholdBytes is used by A2 selection policy.
	SizeThresholdBytes = getEnvInt64("SIZE_THRESHOLD_BYTES", 1048576)
	// TieringPolicyVariantSetting selects candidate policy: A1, A2, A3.
	TieringPolicyVariantSetting = normalizeTieringPolicyVariant(getEnv("TIERING_POLICY_VARIANT", string(TieringPolicyA1)))
	// TieringTriggerModeSetting selects trigger mode: periodic, threshold, hybrid.
	TieringTriggerModeSetting = normalizeTieringTriggerMode(getEnv("TIERING_TRIGGER_MODE", string(TieringTriggerPeriodic)))
	// TieringThresholdCheckSec is threshold trigger sampling interval.
	TieringThresholdCheckSec = getEnvInt("TIERING_THRESHOLD_CHECK_SEC", 10)
	// TieringThresholdCooldownSec prevents threshold-trigger storms.
	TieringThresholdCooldownSec = getEnvInt("TIERING_THRESHOLD_COOLDOWN_SEC", getEnvInt("THRESHOLD_COOLDOWN_SEC", 60))
	// HotPressureDiskPct is the per-node used-disk trigger threshold.
	HotPressureDiskPct = getEnvInt("HOT_PRESSURE_DISK_PCT", 80)
	// HotPressureQueueDepth is the per-node IO queue-depth trigger threshold.
	HotPressureQueueDepth = getEnvInt("HOT_PRESSURE_QUEUE_DEPTH", 1000)
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

func getEnvInt64(key string, fallback int64) int64 {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	parsed, err := strconv.ParseInt(v, 10, 64)
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

func normalizeTieringPolicyVariant(raw string) TieringPolicyVariant {
	v := strings.ToUpper(strings.TrimSpace(raw))
	switch TieringPolicyVariant(v) {
	case TieringPolicyA1, TieringPolicyA2, TieringPolicyA3:
		return TieringPolicyVariant(v)
	default:
		return TieringPolicyA1
	}
}

func normalizeTieringTriggerMode(raw string) TieringTriggerMode {
	v := strings.ToLower(strings.TrimSpace(raw))
	switch TieringTriggerMode(v) {
	case TieringTriggerPeriodic, TieringTriggerThreshold, TieringTriggerHybrid:
		return TieringTriggerMode(v)
	default:
		return TieringTriggerPeriodic
	}
}

func TieringPolicyVariants() []TieringPolicyVariant {
	out := []TieringPolicyVariant{TieringPolicyA1, TieringPolicyA2, TieringPolicyA3}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
