package kvstore

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestParsePDAddrs(t *testing.T) {
	t.Parallel()

	addrs, err := parsePDAddrs("127.0.0.1:2379, 127.0.0.1:2380")
	if err != nil {
		t.Fatalf("parsePDAddrs returned error: %v", err)
	}
	if len(addrs) != 2 {
		t.Fatalf("expected 2 addresses, got %d", len(addrs))
	}
	if addrs[0] != "127.0.0.1:2379" || addrs[1] != "127.0.0.1:2380" {
		t.Fatalf("unexpected addresses: %#v", addrs)
	}

	if _, err := parsePDAddrs(""); err == nil {
		t.Fatalf("expected error for empty dsn")
	}
}

func TestLockValueRoundTrip(t *testing.T) {
	t.Parallel()

	owner := []byte("worker-a")
	exp := time.Now().Add(3 * time.Second).Round(0)

	raw, err := marshalLockValue(owner, exp)
	if err != nil {
		t.Fatalf("marshalLockValue returned error: %v", err)
	}
	gotOwner, gotExp, err := unmarshalLockValue(raw)
	if err != nil {
		t.Fatalf("unmarshalLockValue returned error: %v", err)
	}
	if gotOwner != string(owner) {
		t.Fatalf("owner mismatch: got=%q want=%q", gotOwner, string(owner))
	}
	if gotExp != exp.UnixNano() {
		t.Fatalf("expiry mismatch: got=%d want=%d", gotExp, exp.UnixNano())
	}
}

func TestLockValueBackwardCompatibleFallback(t *testing.T) {
	t.Parallel()

	gotOwner, gotExp, err := unmarshalLockValue([]byte("legacy-owner-token"))
	if err != nil {
		t.Fatalf("fallback unmarshal returned error: %v", err)
	}
	if gotOwner != "legacy-owner-token" {
		t.Fatalf("owner mismatch: got=%q", gotOwner)
	}
	if gotExp != 0 {
		t.Fatalf("fallback expiry must be 0, got=%d", gotExp)
	}
}

func TestRunInTxnMemoryCommitAndRollback(t *testing.T) {
	t.Parallel()

	client, err := Open("memory://txn-test", nil)
	if err != nil {
		t.Fatalf("open memory client failed: %v", err)
	}
	ctx := context.Background()
	key := []byte("task/t1")

	if err := client.RunInTxn(ctx, func(txn *Txn) error {
		if err := txn.Set(key, []byte("running")); err != nil {
			return err
		}
		got, err := txn.Get(ctx, key)
		if err != nil {
			return err
		}
		if string(got) != "running" {
			t.Fatalf("txn read-own-write mismatch: got=%q", string(got))
		}
		return nil
	}); err != nil {
		t.Fatalf("commit transaction failed: %v", err)
	}

	got, closer, err := client.Get(key)
	if err != nil {
		t.Fatalf("expected committed value: %v", err)
	}
	_ = closer.Close()
	if string(got) != "running" {
		t.Fatalf("committed value mismatch: got=%q", string(got))
	}

	rollbackErr := errors.New("rollback")
	if err := client.RunInTxn(ctx, func(txn *Txn) error {
		if err := txn.Set(key, []byte("failed")); err != nil {
			return err
		}
		return rollbackErr
	}); !errors.Is(err, rollbackErr) {
		t.Fatalf("expected rollback error, got=%v", err)
	}

	got, closer, err = client.Get(key)
	if err != nil {
		t.Fatalf("expected original value after rollback: %v", err)
	}
	_ = closer.Close()
	if string(got) != "running" {
		t.Fatalf("rollback leaked value: got=%q", string(got))
	}
}
