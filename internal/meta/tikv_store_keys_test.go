package meta

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestTiKVEncodeInt64_LexicographicOrderMatchesNumeric(t *testing.T) {
	t.Parallel()

	cases := []int64{0, 1, 2, 9, 10, 11, 99, 100, 101, 9999}
	for i := 0; i < len(cases)-1; i++ {
		a := tiKVEncodeInt64(cases[i])
		b := tiKVEncodeInt64(cases[i+1])
		if !(a < b) {
			t.Fatalf("expected %d < %d lexicographically, got %q >= %q", cases[i], cases[i+1], a, b)
		}
	}
}

func TestTiKVPrefixUpperBound_PrefixRange(t *testing.T) {
	t.Parallel()

	prefix := []byte("obj/")
	upper := tiKVPrefixUpperBound(prefix)
	if upper == nil {
		t.Fatalf("upper bound should not be nil for prefix=%q", prefix)
	}

	withinPrefix := [][]byte{
		[]byte("obj/a"),
		[]byte("obj/zzz"),
		[]byte("obj/\xff\xff"),
	}
	for _, k := range withinPrefix {
		if bytes.Compare(k, upper) >= 0 {
			t.Fatalf("key %q should be below upper bound %q", k, upper)
		}
	}

	nonPrefix := []byte("obk/")
	if bytes.Compare(nonPrefix, upper) < 0 {
		t.Fatalf("non-prefix key %q should not fall inside prefix range ending at %q", nonPrefix, upper)
	}
}

func TestTiKVKeyBuilders(t *testing.T) {
	t.Parallel()

	if got, want := tiKVObjectKey("o1"), "obj/o1"; got != want {
		t.Fatalf("tiKVObjectKey mismatch: got=%q want=%q", got, want)
	}
	if got, want := tiKVTaskKey("t1"), "task/t1"; got != want {
		t.Fatalf("tiKVTaskKey mismatch: got=%q want=%q", got, want)
	}
	ready := tiKVTaskReadyKey("REPAIR", 200, time.Unix(0, 12345), "repair:obj:7")
	if got, want := ready, "task_ready/0999999800/00000000000000012345/REPAIR/repair:obj:7"; got != want {
		t.Fatalf("tiKVTaskReadyKey mismatch: got=%q want=%q", got, want)
	}
	wait := tiKVTaskWaitKey("REPAIR", time.Unix(0, 12345), "repair:obj:7")
	if got, want := wait, "task_wait/00000000000000012345/REPAIR/repair:obj:7"; got != want {
		t.Fatalf("tiKVTaskWaitKey mismatch: got=%q want=%q", got, want)
	}
	if got, want := tiKVReplicaKey("o1", 7, "n1"), "repl/o1/00000000000000000007/n1"; got != want {
		t.Fatalf("tiKVReplicaKey mismatch: got=%q want=%q", got, want)
	}
	if got, want := tiKVECShardKey("o1", 7, 3), "ec/o1/00000000000000000007/0000000003"; got != want {
		t.Fatalf("tiKVECShardKey mismatch: got=%q want=%q", got, want)
	}
}

func TestTiKVParseTaskIndexKeys(t *testing.T) {
	t.Parallel()

	readyKey := "task_ready/0999999800/00000000000000012345/REPAIR/repair:obj:7"
	waitKey := "task_wait/00000000000000012345/REPAIR/repair:obj:7"

	readySchedule, readyType, readyID, ok := tiKVParseTaskReadyKey(readyKey)
	if !ok {
		t.Fatalf("expected ready key parse success")
	}
	if readySchedule != 12345 {
		t.Fatalf("ready schedule mismatch: got=%d", readySchedule)
	}
	if readyType != "REPAIR" {
		t.Fatalf("ready type mismatch: got=%q", readyType)
	}
	if readyID != "repair:obj:7" {
		t.Fatalf("ready task id mismatch: got=%q", readyID)
	}

	waitSchedule, waitType, waitID, ok := tiKVParseTaskWaitKey(waitKey)
	if !ok {
		t.Fatalf("expected wait key parse success")
	}
	if waitSchedule != 12345 {
		t.Fatalf("wait schedule mismatch: got=%d", waitSchedule)
	}
	if waitType != "REPAIR" {
		t.Fatalf("wait type mismatch: got=%q", waitType)
	}
	if waitID != "repair:obj:7" {
		t.Fatalf("wait task id mismatch: got=%q", waitID)
	}
}

func TestTiKVNewLockOwnerToken_Format(t *testing.T) {
	t.Parallel()

	token := string(tiKVNewLockOwnerToken())
	if token == "" {
		t.Fatalf("token should not be empty")
	}
	if strings.HasPrefix(token, "owner-") {
		return
	}
	if len(token) != 32 {
		t.Fatalf("hex token should be 32 chars, got=%d token=%q", len(token), token)
	}
	for _, ch := range token {
		if (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f') {
			continue
		}
		t.Fatalf("token contains non-hex char: %q in %q", ch, token)
	}
}
