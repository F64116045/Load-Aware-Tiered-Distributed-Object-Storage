package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
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

func TestStorageEngineRejectsWhenQueuedBytesWouldExceedLimit(t *testing.T) {
	t.Parallel()

	st := newStorageEngine("19004", "test-node", t.TempDir())
	atomic.StoreInt64(&st.maxQueuedWriteBytes, 3)

	_, err := st.store(context.Background(), "too-large", []byte("payload"))
	if err == nil {
		t.Fatalf("expected byte-aware queue limit error")
	}
	if !strings.Contains(err.Error(), "queued write bytes") {
		t.Fatalf("expected queued byte limit error, got: %v", err)
	}
	if got := st.currentQueuedWriteBytes(); got != 0 {
		t.Fatalf("queued bytes should be released after rejected store, got=%d", got)
	}
}

func TestStorageEngineInfoIncludesQueuedByteCapacity(t *testing.T) {
	t.Parallel()

	st := newStorageEngine("19005", "test-node", t.TempDir())
	atomic.StoreInt64(&st.maxQueuedWriteBytes, 12345)

	if _, err := st.store(context.Background(), "k1", []byte("payload")); err != nil {
		t.Fatalf("store failed: %v", err)
	}
	info, err := st.getInfo()
	if err != nil {
		t.Fatalf("getInfo failed: %v", err)
	}
	if got := info["queued_write_bytes"]; got != int64(0) {
		t.Fatalf("queued_write_bytes=%v want 0", got)
	}
	if got := info["max_queued_write_bytes"]; got != int64(12345) {
		t.Fatalf("max_queued_write_bytes=%v want 12345", got)
	}
}

func TestStorageEngineInfoIncludesConfiguredIOWorkers(t *testing.T) {
	t.Parallel()

	st := newStorageEngineWithConfig("19006", "test-node", t.TempDir(), 12345, 3)

	info, err := st.getInfo()
	if err != nil {
		t.Fatalf("getInfo failed: %v", err)
	}
	if got := info["io_workers"]; got != 3 {
		t.Fatalf("io_workers=%v want 3", got)
	}
	if got := info["max_queued_write_bytes"]; got != int64(12345) {
		t.Fatalf("max_queued_write_bytes=%v want 12345", got)
	}
}

func TestStorageEngineInfoIncludesDurabilityMode(t *testing.T) {
	t.Parallel()

	st := newStorageEngineWithDurability("19007", "test-node", t.TempDir(), 12345, 1, storageDurabilityWrite)

	info, err := st.getInfo()
	if err != nil {
		t.Fatalf("getInfo failed: %v", err)
	}
	if got := info["durability_mode"]; got != storageDurabilityWrite {
		t.Fatalf("durability_mode=%v want %s", got, storageDurabilityWrite)
	}
}

func TestStorageEngineNormalizesInvalidIOWorkerCount(t *testing.T) {
	t.Parallel()

	if got := normalizeStorageIOWorkers(0); got != 1 {
		t.Fatalf("normalizeStorageIOWorkers(0)=%d want 1", got)
	}
	if got := normalizeStorageIOWorkers(-3); got != 1 {
		t.Fatalf("normalizeStorageIOWorkers(-3)=%d want 1", got)
	}
	if got := normalizeStorageIOWorkers(4); got != 4 {
		t.Fatalf("normalizeStorageIOWorkers(4)=%d want 4", got)
	}
}

func TestStorageEngineNormalizesDurabilityMode(t *testing.T) {
	t.Parallel()

	if got := normalizeStorageDurabilityMode("write"); got != storageDurabilityWrite {
		t.Fatalf("normalizeStorageDurabilityMode(write)=%s want %s", got, storageDurabilityWrite)
	}
	if got := normalizeStorageDurabilityMode(" WRITE "); got != storageDurabilityWrite {
		t.Fatalf("normalizeStorageDurabilityMode(WRITE)=%s want %s", got, storageDurabilityWrite)
	}
	if got := normalizeStorageDurabilityMode("sync"); got != storageDurabilitySync {
		t.Fatalf("normalizeStorageDurabilityMode(sync)=%s want %s", got, storageDurabilitySync)
	}
	if got := normalizeStorageDurabilityMode("unknown"); got != storageDurabilitySync {
		t.Fatalf("normalizeStorageDurabilityMode(unknown)=%s want %s", got, storageDurabilitySync)
	}
}

func TestWriteFileWithDurabilityWriteModeWritesPayload(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "nested", "payload.bin")
	if _, err := writeFileWithDurability(path, []byte("payload"), storageDurabilityWrite); err != nil {
		t.Fatalf("writeFileWithDurability failed: %v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read payload failed: %v", err)
	}
	if string(got) != "payload" {
		t.Fatalf("payload mismatch: got=%q", string(got))
	}
}
