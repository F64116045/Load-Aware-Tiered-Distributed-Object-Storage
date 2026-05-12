package placement

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
	"strings"
)

type scoredNode struct {
	nodeID string
	score  uint64
}

// SelectByRendezvous returns up to count nodes ranked by rendezvous hashing.
// It is deterministic for the same key and node set, ignores empty/duplicate
// node ids, and never mutates the input slice.
func SelectByRendezvous(key string, nodes []string, count int) []string {
	if count <= 0 || len(nodes) == 0 {
		return nil
	}

	unique := uniqueSortedNodes(nodes)
	if len(unique) == 0 {
		return nil
	}

	ranked := make([]scoredNode, 0, len(unique))
	for _, nodeID := range unique {
		ranked = append(ranked, scoredNode{
			nodeID: nodeID,
			score:  rendezvousScore(key, nodeID),
		})
	}

	sort.Slice(ranked, func(i, j int) bool {
		if ranked[i].score != ranked[j].score {
			return ranked[i].score > ranked[j].score
		}
		return ranked[i].nodeID < ranked[j].nodeID
	})

	if count > len(ranked) {
		count = len(ranked)
	}
	out := make([]string, 0, count)
	for i := 0; i < count; i++ {
		out = append(out, ranked[i].nodeID)
	}
	return out
}

func HotReplicaKey(objectID string) string {
	return "hot:" + strings.TrimSpace(objectID)
}

func ECShardKey(objectID string, version int64) string {
	return fmt.Sprintf("ec:%s:%020d", strings.TrimSpace(objectID), version)
}

func HotRepairKey(objectID string, version int64) string {
	return fmt.Sprintf("hot-repair:%s:%020d", strings.TrimSpace(objectID), version)
}

func ECRepairKey(objectID string, version int64) string {
	return fmt.Sprintf("ec-repair:%s:%020d", strings.TrimSpace(objectID), version)
}

func uniqueSortedNodes(nodes []string) []string {
	seen := make(map[string]struct{}, len(nodes))
	unique := make([]string, 0, len(nodes))
	for _, nodeID := range nodes {
		nodeID = strings.TrimSpace(nodeID)
		if nodeID == "" {
			continue
		}
		if _, ok := seen[nodeID]; ok {
			continue
		}
		seen[nodeID] = struct{}{}
		unique = append(unique, nodeID)
	}
	sort.Strings(unique)
	return unique
}

func rendezvousScore(key string, nodeID string) uint64 {
	h := sha256.New()
	_, _ = h.Write([]byte(key))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(nodeID))
	sum := h.Sum(nil)
	return binary.BigEndian.Uint64(sum[:8])
}
