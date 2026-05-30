package main

import (
	"sort"
	"strings"

	"hybrid_distributed_store/internal/meta"
	"hybrid_distributed_store/internal/placement"
)

type nodeLoadSnapshot struct {
	ioQueueDepth  int
	ioQueueBytes  int64
	cpuLoad       float64
	memoryUsedPct float64
	diskIOWaitPct float64
	recentWriteMS float64
}

type hotReplicaCandidate struct {
	nodeID         string
	rendezvousRank int
	loadPenalty    int64
}

func nodeLoadSnapshotFromHeartbeat(rec meta.NodeHeartbeatSnapshot) nodeLoadSnapshot {
	return nodeLoadSnapshot{
		ioQueueDepth:  rec.IOQueueDepth,
		ioQueueBytes:  rec.IOQueueBytes,
		cpuLoad:       rec.CPULoad,
		memoryUsedPct: rec.MemoryUsedPct,
		diskIOWaitPct: rec.DiskIOWaitPct,
	}
}

func selectHotReplicaNodes(objectID string, allNodes []string, loads map[string]nodeLoadSnapshot, replicaTarget int, loadAware bool) []string {
	if replicaTarget <= 0 {
		return nil
	}
	ranked := placement.SelectByRendezvous(placement.HotReplicaKey(objectID), allNodes, len(allNodes))
	if !loadAware || len(loads) == 0 || len(ranked) <= replicaTarget {
		if replicaTarget > len(ranked) {
			replicaTarget = len(ranked)
		}
		return append([]string(nil), ranked[:replicaTarget]...)
	}

	candidates := make([]hotReplicaCandidate, 0, len(ranked))
	for rank, nodeID := range ranked {
		load, ok := loads[nodeID]
		penalty := int64(0)
		if ok {
			penalty = hotReplicaLoadPenalty(load)
		}
		candidates = append(candidates, hotReplicaCandidate{
			nodeID:         nodeID,
			rendezvousRank: rank,
			loadPenalty:    penalty,
		})
	}

	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].loadPenalty != candidates[j].loadPenalty {
			return candidates[i].loadPenalty < candidates[j].loadPenalty
		}
		if candidates[i].rendezvousRank != candidates[j].rendezvousRank {
			return candidates[i].rendezvousRank < candidates[j].rendezvousRank
		}
		return strings.Compare(candidates[i].nodeID, candidates[j].nodeID) < 0
	})

	if replicaTarget > len(candidates) {
		replicaTarget = len(candidates)
	}
	out := make([]string, 0, replicaTarget)
	for i := 0; i < replicaTarget; i++ {
		out = append(out, candidates[i].nodeID)
	}
	return out
}

func hotReplicaLoadPenalty(load nodeLoadSnapshot) int64 {
	depth := load.ioQueueDepth
	if depth < 0 {
		depth = 0
	}
	bytesMiB := load.ioQueueBytes / (1024 * 1024)
	if bytesMiB < 0 {
		bytesMiB = 0
	}
	return int64(depth)*1000 +
		bytesMiB*100 +
		loadBucket(load.recentWriteMS, 25)*100 +
		loadBucket(load.diskIOWaitPct, 5)*20 +
		loadBucket(load.cpuLoad, 10)*10 +
		loadBucket(load.memoryUsedPct, 25)
}

func loadBucket(value float64, bucketSize float64) int64 {
	if value <= 0 || bucketSize <= 0 {
		return 0
	}
	return int64(value / bucketSize)
}

func recordHotReplicaWriteLatencies(opResult map[string]interface{}) {
	if opResult == nil {
		return
	}
	samples := replicaWriteLatencySamples(opResult["replica_write_ms"])
	if len(samples) == 0 {
		return
	}

	NodeListLock.Lock()
	if ActiveNodeRecentWriteMS == nil {
		ActiveNodeRecentWriteMS = make(map[string]float64, len(samples))
	}
	for nodeID, sampleMS := range samples {
		if nodeID == "" || sampleMS <= 0 {
			continue
		}
		if current, ok := ActiveNodeRecentWriteMS[nodeID]; ok && current > 0 {
			ActiveNodeRecentWriteMS[nodeID] = current*0.75 + sampleMS*0.25
			continue
		}
		ActiveNodeRecentWriteMS[nodeID] = sampleMS
	}
	NodeListLock.Unlock()
}

func replicaWriteLatencySamples(raw interface{}) map[string]float64 {
	out := map[string]float64{}
	switch values := raw.(type) {
	case map[string]int64:
		for nodeID, value := range values {
			if value > 0 {
				out[nodeID] = float64(value)
			}
		}
	case map[string]int:
		for nodeID, value := range values {
			if value > 0 {
				out[nodeID] = float64(value)
			}
		}
	case map[string]float64:
		for nodeID, value := range values {
			if value > 0 {
				out[nodeID] = value
			}
		}
	case map[string]interface{}:
		for nodeID, value := range values {
			if sample, ok := numericLatencyValue(value); ok && sample > 0 {
				out[nodeID] = sample
			}
		}
	}
	return out
}

func numericLatencyValue(value interface{}) (float64, bool) {
	switch typed := value.(type) {
	case int:
		return float64(typed), true
	case int8:
		return float64(typed), true
	case int16:
		return float64(typed), true
	case int32:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case uint:
		return float64(typed), true
	case uint8:
		return float64(typed), true
	case uint16:
		return float64(typed), true
	case uint32:
		return float64(typed), true
	case uint64:
		return float64(typed), true
	case float32:
		return float64(typed), true
	case float64:
		return typed, true
	default:
		return 0, false
	}
}
