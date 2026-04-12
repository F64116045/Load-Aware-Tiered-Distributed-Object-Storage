package meta

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

func tiKVObjectKey(objectID string) string {
	return tiKVPrefixObject + objectID
}

func tiKVObjectVersionKey(objectID string, version int64) string {
	return tiKVPrefixObjVer + objectID + "/" + tiKVEncodeInt64(version)
}

func tiKVObjectVersionPrefix(objectID string) string {
	return tiKVPrefixObjVer + objectID + "/"
}

func tiKVReplicaKey(objectID string, version int64, nodeID string) string {
	return tiKVPrefixReplica + objectID + "/" + tiKVEncodeInt64(version) + "/" + nodeID
}

func tiKVReplicaPrefix(objectID string) string {
	return tiKVPrefixReplica + objectID + "/"
}

func tiKVReplicaVersionPrefix(objectID string, version int64) string {
	return tiKVPrefixReplica + objectID + "/" + tiKVEncodeInt64(version) + "/"
}

func tiKVECShardKey(objectID string, version int64, shardIndex int) string {
	return tiKVPrefixECShard + objectID + "/" + tiKVEncodeInt64(version) + "/" + tiKVEncodeInt(shardIndex)
}

func tiKVECShardPrefix(objectID string) string {
	return tiKVPrefixECShard + objectID + "/"
}

func tiKVECShardVersionPrefix(objectID string, version int64) string {
	return tiKVPrefixECShard + objectID + "/" + tiKVEncodeInt64(version) + "/"
}

func tiKVTaskKey(taskID string) string {
	return tiKVPrefixTask + taskID
}

func tiKVHeartbeatKey(nodeID string) string {
	return tiKVPrefixHB + nodeID
}

func tiKVLeaderKey(lockKey int64) string {
	return tiKVPrefixLeader + strconv.FormatInt(lockKey, 10)
}

func tiKVLeaderLockKey(lockKey int64) string {
	return tiKVPrefixLk + strconv.FormatInt(lockKey, 10)
}

func tiKVTierDueKey(eligibleAt time.Time, objectID string, version int64) string {
	return tiKVPrefixTierDue + tiKVEncodeInt64(eligibleAt.UnixNano()) + "/" + objectID + "/" + tiKVEncodeInt64(version)
}

func tiKVTierDuePrefix() string {
	return tiKVPrefixTierDue
}

func tiKVTierDueRefKey(objectID string, version int64) string {
	return tiKVPrefixTierRef + objectID + "/" + tiKVEncodeInt64(version)
}

func tiKVTierDueRefPrefix(objectID string) string {
	return tiKVPrefixTierRef + objectID + "/"
}

func tiKVParseTierDueKey(key string) (int64, string, int64, bool) {
	if !strings.HasPrefix(key, tiKVPrefixTierDue) {
		return 0, "", 0, false
	}
	parts := strings.Split(strings.TrimPrefix(key, tiKVPrefixTierDue), "/")
	if len(parts) != 3 {
		return 0, "", 0, false
	}
	ts, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, "", 0, false
	}
	ver, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return 0, "", 0, false
	}
	return ts, parts[1], ver, true
}

func tiKVEncodeInt64(v int64) string {
	return fmt.Sprintf("%020d", v)
}

func tiKVEncodeInt(v int) string {
	return fmt.Sprintf("%010d", v)
}

func tiKVPrefixUpperBound(prefix []byte) []byte {
	out := append([]byte(nil), prefix...)
	for i := len(out) - 1; i >= 0; i-- {
		if out[i] != 0xFF {
			out[i]++
			return out[:i+1]
		}
	}
	return nil
}
