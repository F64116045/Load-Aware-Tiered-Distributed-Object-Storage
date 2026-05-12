package placement

import (
	"reflect"
	"testing"
)

func TestSelectByRendezvousDeterministicAndInputSafe(t *testing.T) {
	nodes := []string{"node-3", "node-1", "node-2", "node-2", "", "node-4"}
	original := append([]string(nil), nodes...)

	first := SelectByRendezvous(HotReplicaKey("object-a"), nodes, 3)
	second := SelectByRendezvous(HotReplicaKey("object-a"), nodes, 3)

	if !reflect.DeepEqual(first, second) {
		t.Fatalf("selection must be deterministic: first=%v second=%v", first, second)
	}
	if len(first) != 3 {
		t.Fatalf("expected 3 selected nodes, got=%d selection=%v", len(first), first)
	}
	if !reflect.DeepEqual(nodes, original) {
		t.Fatalf("selection mutated input: got=%v want=%v", nodes, original)
	}
	seen := map[string]struct{}{}
	for _, nodeID := range first {
		if nodeID == "" {
			t.Fatalf("selection included empty node id: %v", first)
		}
		if _, ok := seen[nodeID]; ok {
			t.Fatalf("selection included duplicate node id: %v", first)
		}
		seen[nodeID] = struct{}{}
	}
}

func TestSelectByRendezvousHandlesBounds(t *testing.T) {
	nodes := []string{"node-1", "node-2"}
	if got := SelectByRendezvous("k", nodes, 0); len(got) != 0 {
		t.Fatalf("expected empty selection for count=0, got=%v", got)
	}
	if got := SelectByRendezvous("k", nodes, 10); len(got) != 2 {
		t.Fatalf("expected all unique nodes when count exceeds size, got=%v", got)
	}
}

func TestSelectByRendezvousSpreadsObjectsAcrossCluster(t *testing.T) {
	nodes := []string{
		"http://storage_node_1:8001",
		"http://storage_node_2:8002",
		"http://storage_node_3:8003",
		"http://storage_node_4:8004",
		"http://storage_node_5:8005",
		"http://storage_node_6:8006",
	}

	used := map[string]struct{}{}
	for _, objectID := range []string{"obj-a", "obj-b", "obj-c", "obj-d", "obj-e", "obj-f", "obj-g", "obj-h"} {
		for _, nodeID := range SelectByRendezvous(HotReplicaKey(objectID), nodes, 3) {
			used[nodeID] = struct{}{}
		}
	}

	if len(used) <= 3 {
		t.Fatalf("expected placement to use nodes beyond a fixed first-N subset, used=%v", used)
	}
}
