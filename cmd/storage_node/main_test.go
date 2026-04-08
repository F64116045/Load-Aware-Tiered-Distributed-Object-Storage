package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestStoreWaitsForDurableWrite(t *testing.T) {
	storageDir := t.TempDir()
	engine := newStorageEngine("19001", "test-node", storageDir)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	payload := []byte("hello-durable")
	size, err := engine.store(ctx, "obj-1", payload)
	if err != nil {
		t.Fatalf("store() returned error: %v", err)
	}
	if size != len(payload) {
		t.Fatalf("store() size mismatch: got=%d want=%d", size, len(payload))
	}

	got, err := os.ReadFile(filepath.Join(storageDir, "obj-1"))
	if err != nil {
		t.Fatalf("read persisted file failed: %v", err)
	}
	if string(got) != string(payload) {
		t.Fatalf("persisted payload mismatch: got=%q want=%q", string(got), string(payload))
	}
}

func TestStoreRejectsUnsafeKey(t *testing.T) {
	storageDir := t.TempDir()
	engine := newStorageEngine("19002", "test-node", storageDir)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if _, err := engine.store(ctx, "../bad", []byte("x")); err == nil {
		t.Fatalf("store() expected error for unsafe key")
	}
}
