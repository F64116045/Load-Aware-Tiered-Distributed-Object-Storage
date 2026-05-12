package main

import "testing"

func TestSelectDynamicNodesForObjectBalancesHotReplicas(t *testing.T) {
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
		replicas, ecNodes, err := selectDynamicNodesForObject(objectID, nodes, 3, 2, 6)
		if err != nil {
			t.Fatalf("selectDynamicNodesForObject(%q) failed: %v", objectID, err)
		}
		if len(replicas) != 3 {
			t.Fatalf("expected 3 replicas for %q, got=%v", objectID, replicas)
		}
		if len(ecNodes) != 6 {
			t.Fatalf("expected 6 ec nodes for %q, got=%v", objectID, ecNodes)
		}
		for _, nodeID := range replicas {
			used[nodeID] = struct{}{}
		}
	}

	if len(used) <= 3 {
		t.Fatalf("expected HOT placement to use more than a fixed first-N subset, used=%v", used)
	}
}

func TestSelectDynamicNodesForObjectRejectsInvalidInputs(t *testing.T) {
	nodes := []string{"n1", "n2", "n3"}
	if _, _, err := selectDynamicNodesForObject("obj", nodes, 3, 4, 3); err == nil {
		t.Fatalf("expected invalid quorum error")
	}
	if _, _, err := selectDynamicNodesForObject("obj", nodes[:2], 3, 2, 2); err == nil {
		t.Fatalf("expected insufficient replica node error")
	}
	if _, _, err := selectDynamicNodesForObject("obj", nodes, 3, 2, 4); err == nil {
		t.Fatalf("expected insufficient EC node error")
	}
}

func TestHotReplicaNodesFromMetadata(t *testing.T) {
	metadata := map[string]interface{}{
		"hot_replicas": []interface{}{
			map[string]interface{}{"node_id": "n2", "path": "hot/o/1", "status": "ACTIVE"},
			map[string]interface{}{"node_id": "n1", "path": "hot/o/1", "status": "DELETED"},
			map[string]interface{}{"node_id": "n3", "path": "hot/o/1"},
			map[string]interface{}{"node_id": "n2", "path": "hot/o/1", "status": "ACTIVE"},
		},
	}

	got := hotReplicaNodesFromMetadata(metadata)
	want := []string{"n2", "n3"}
	if len(got) != len(want) {
		t.Fatalf("unexpected replica nodes: got=%v want=%v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("unexpected replica nodes: got=%v want=%v", got, want)
		}
	}

	fallback := hotReplicaNodesFromMetadata(map[string]interface{}{
		"replica_nodes": []interface{}{"n4", "n5", "n4"},
	})
	if len(fallback) != 2 || fallback[0] != "n4" || fallback[1] != "n5" {
		t.Fatalf("unexpected replica_nodes fallback: %v", fallback)
	}
}
