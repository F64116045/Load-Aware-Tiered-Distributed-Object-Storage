package meta

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	tiKVTaskPriorityBias int64 = 1000000000
	tiKVTaskPriorityMax  int64 = 2000000000
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

func tiKVTaskReadyPrefix() string {
	return tiKVPrefixTaskRdy
}

func tiKVTaskWaitPrefix() string {
	return tiKVPrefixTaskWtg
}

func tiKVTaskTerminalPrefix() string {
	return tiKVPrefixTaskTer
}

func tiKVTaskReadyKey(taskType string, priority int, scheduledAt time.Time, taskID string) string {
	return tiKVPrefixTaskRdy +
		tiKVEncodePriorityDesc(priority) + "/" +
		tiKVEncodeInt64(scheduledAt.UnixNano()) + "/" +
		taskType + "/" +
		taskID
}

func tiKVTaskWaitKey(taskType string, scheduledAt time.Time, taskID string) string {
	return tiKVPrefixTaskWtg +
		tiKVEncodeInt64(scheduledAt.UnixNano()) + "/" +
		taskType + "/" +
		taskID
}

func tiKVTaskTerminalKey(finishedAt time.Time, taskID string) string {
	return tiKVPrefixTaskTer +
		tiKVEncodeInt64(finishedAt.UnixNano()) + "/" +
		taskID
}

func tiKVParseTaskReadyKey(key string) (scheduledAtUnixNano int64, taskType string, taskID string, ok bool) {
	if !strings.HasPrefix(key, tiKVPrefixTaskRdy) {
		return 0, "", "", false
	}
	parts := strings.Split(strings.TrimPrefix(key, tiKVPrefixTaskRdy), "/")
	if len(parts) < 4 {
		return 0, "", "", false
	}
	scheduledAtUnixNano, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, "", "", false
	}
	taskType = parts[2]
	taskID = strings.Join(parts[3:], "/")
	if taskID == "" {
		return 0, "", "", false
	}
	return scheduledAtUnixNano, taskType, taskID, true
}

func tiKVParseTaskWaitKey(key string) (scheduledAtUnixNano int64, taskType string, taskID string, ok bool) {
	if !strings.HasPrefix(key, tiKVPrefixTaskWtg) {
		return 0, "", "", false
	}
	parts := strings.Split(strings.TrimPrefix(key, tiKVPrefixTaskWtg), "/")
	if len(parts) < 3 {
		return 0, "", "", false
	}
	scheduledAtUnixNano, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, "", "", false
	}
	taskType = parts[1]
	taskID = strings.Join(parts[2:], "/")
	if taskID == "" {
		return 0, "", "", false
	}
	return scheduledAtUnixNano, taskType, taskID, true
}

func tiKVParseTaskTerminalKey(key string) (finishedAtUnixNano int64, taskID string, ok bool) {
	if !strings.HasPrefix(key, tiKVPrefixTaskTer) {
		return 0, "", false
	}
	parts := strings.Split(strings.TrimPrefix(key, tiKVPrefixTaskTer), "/")
	if len(parts) < 2 {
		return 0, "", false
	}
	finishedAtUnixNano, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, "", false
	}
	taskID = strings.Join(parts[1:], "/")
	if taskID == "" {
		return 0, "", false
	}
	return finishedAtUnixNano, taskID, true
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

func tiKVEncodePriorityDesc(priority int) string {
	normalized := int64(priority) + tiKVTaskPriorityBias
	if normalized < 0 {
		normalized = 0
	}
	if normalized > tiKVTaskPriorityMax {
		normalized = tiKVTaskPriorityMax
	}
	inverted := tiKVTaskPriorityMax - normalized
	return fmt.Sprintf("%010d", inverted)
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
