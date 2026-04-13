package meta

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"hybrid_distributed_store/internal/config"
)

func resolveVersion(metadata map[string]interface{}) int64 {
	hot := toInt64(metadata["hot_version"], 0)
	cold := toInt64(metadata["cold_version"], 0)
	if hot > cold {
		return hot
	}
	if cold > 0 {
		return cold
	}
	return time.Now().UnixNano()
}

func resolveState(metadata map[string]interface{}) string {
	switch toString(metadata["strategy"], "") {
	case "ec":
		return "EC_ACTIVE"
	default:
		return "HOT_ACTIVE"
	}
}

func resolveTier(metadata map[string]interface{}) string {
	switch toString(metadata["strategy"], "") {
	case "replication":
		return "HOT"
	case "ec":
		return "EC"
	default:
		return "HOT"
	}
}

func toString(v interface{}, fallback string) string {
	if v == nil {
		return fallback
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fallback
}

func toInt64(v interface{}, fallback int64) int64 {
	switch x := v.(type) {
	case int:
		return int64(x)
	case int32:
		return int64(x)
	case int64:
		return x
	case json.Number:
		n, err := x.Int64()
		if err != nil {
			return fallback
		}
		return n
	case float64:
		return int64(x)
	case string:
		n, err := strconv.ParseInt(x, 10, 64)
		if err != nil {
			return fallback
		}
		return n
	default:
		return fallback
	}
}

func toNullableInt(v interface{}) interface{} {
	switch x := v.(type) {
	case int:
		return x
	case int32:
		return int(x)
	case int64:
		return int(x)
	case float64:
		return int(x)
	default:
		return nil
	}
}

func toNullableString(v interface{}) interface{} {
	s := toString(v, "")
	if s == "" {
		return nil
	}
	return s
}

func toStringSlice(v interface{}) []string {
	if v == nil {
		return nil
	}
	switch xs := v.(type) {
	case []string:
		return xs
	case []interface{}:
		out := make([]string, 0, len(xs))
		for _, item := range xs {
			if s, ok := item.(string); ok && s != "" {
				out = append(out, s)
			}
		}
		return out
	default:
		return nil
	}
}

func strategyFromTier(tier string) string {
	switch tier {
	case "HOT":
		return string(config.StrategyReplication)
	case "EC":
		return string(config.StrategyEC)
	default:
		return string(config.StrategyReplication)
	}
}

// BuildHotReplicaPath returns the versioned blob key used for HOT replica storage.
// Versioned paths decouple physical blob identity from logical object id and make
// multi-version cleanup/repair operations deterministic.
func BuildHotReplicaPath(objectID string, version int64) string {
	objectID = strings.TrimSpace(objectID)
	if objectID == "" {
		return ""
	}
	if version <= 0 {
		return objectID
	}
	return fmt.Sprintf("hot/%s/%020d", objectID, version)
}
