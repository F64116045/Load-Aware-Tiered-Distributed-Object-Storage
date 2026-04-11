package main

import (
	"context"
	"testing"
)

func TestStorageEngineSafePath(t *testing.T) {
	t.Parallel()

	st := newStorageEngine("19002", "test-node", t.TempDir())

	if _, err := st._getSafePath("../escape"); err == nil {
		t.Fatalf("expected traversal key to be rejected")
	}
	if _, err := st._getSafePath("/abs/path"); err == nil {
		t.Fatalf("expected absolute path key to be rejected")
	}
	if _, err := st._getSafePath("nested/object.bin"); err != nil {
		t.Fatalf("expected normal key to pass, got err=%v", err)
	}
}

func TestStorageEngineStoreRetrieveDeleteRoundTrip(t *testing.T) {
	t.Parallel()

	st := newStorageEngine("19003", "test-node", t.TempDir())
	ctx := context.Background()

	size, err := st.store(ctx, "k1", []byte("payload"))
	if err != nil {
		t.Fatalf("store failed: %v", err)
	}
	if size != len("payload") {
		t.Fatalf("store size mismatch: got=%d", size)
	}

	data, err := st.retrieve("k1")
	if err != nil {
		t.Fatalf("retrieve failed: %v", err)
	}
	if string(data) != "payload" {
		t.Fatalf("retrieve payload mismatch: got=%q", string(data))
	}

	deleted, err := st.delete("k1")
	if err != nil {
		t.Fatalf("delete failed: %v", err)
	}
	if !deleted {
		t.Fatalf("expected deleted=true")
	}

	data, err = st.retrieve("k1")
	if err != nil {
		t.Fatalf("retrieve after delete failed: %v", err)
	}
	if data != nil {
		t.Fatalf("expected nil data after delete, got=%q", string(data))
	}
}
