package kvstore

import (
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
