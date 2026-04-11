package meta

import (
	"encoding/json"
	"testing"
)

func TestResolveVersion(t *testing.T) {
	t.Parallel()

	if got := resolveVersion(map[string]interface{}{"hot_version": int64(9), "cold_version": int64(3)}); got != 9 {
		t.Fatalf("expected hot version to win, got=%d", got)
	}
	if got := resolveVersion(map[string]interface{}{"hot_version": int64(2), "cold_version": int64(7)}); got != 7 {
		t.Fatalf("expected cold version to win when larger, got=%d", got)
	}
	if got := resolveVersion(map[string]interface{}{"hot_version": int64(0), "cold_version": int64(0)}); got <= 0 {
		t.Fatalf("expected fallback generated version > 0, got=%d", got)
	}
}

func TestResolveStateAndTier(t *testing.T) {
	t.Parallel()

	if got := resolveState(map[string]interface{}{"strategy": "ec"}); got != "EC_ACTIVE" {
		t.Fatalf("resolveState ec mismatch: got=%q", got)
	}
	if got := resolveState(map[string]interface{}{"strategy": "replication"}); got != "HOT_ACTIVE" {
		t.Fatalf("resolveState replication mismatch: got=%q", got)
	}

	if got := resolveTier(map[string]interface{}{"strategy": "ec"}); got != "EC" {
		t.Fatalf("resolveTier ec mismatch: got=%q", got)
	}
	if got := resolveTier(map[string]interface{}{"strategy": "replication"}); got != "HOT" {
		t.Fatalf("resolveTier replication mismatch: got=%q", got)
	}
	if got := resolveTier(map[string]interface{}{"strategy": "unknown"}); got != "HOT" {
		t.Fatalf("resolveTier default mismatch: got=%q", got)
	}
}

func TestToInt64(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   interface{}
		want int64
	}{
		{name: "int", in: int(3), want: 3},
		{name: "int32", in: int32(4), want: 4},
		{name: "int64", in: int64(5), want: 5},
		{name: "json_number", in: json.Number("7"), want: 7},
		{name: "float64", in: float64(8.9), want: 8},
		{name: "string", in: "11", want: 11},
		{name: "bad_string", in: "x", want: 99},
		{name: "nil", in: nil, want: 99},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := toInt64(tt.in, 99); got != tt.want {
				t.Fatalf("toInt64(%v)=%d want=%d", tt.in, got, tt.want)
			}
		})
	}
}

func TestToNullableAndSliceHelpers(t *testing.T) {
	t.Parallel()

	if got := toNullableInt(3); got != 3 {
		t.Fatalf("toNullableInt int mismatch: got=%v", got)
	}
	if got := toNullableInt("x"); got != nil {
		t.Fatalf("toNullableInt invalid should be nil, got=%v", got)
	}
	if got := toNullableString("abc"); got != "abc" {
		t.Fatalf("toNullableString mismatch: got=%v", got)
	}
	if got := toNullableString(""); got != nil {
		t.Fatalf("toNullableString empty should be nil, got=%v", got)
	}

	list := toStringSlice([]interface{}{"a", "", "b", 7, "c"})
	if len(list) != 3 || list[0] != "a" || list[1] != "b" || list[2] != "c" {
		t.Fatalf("toStringSlice mismatch: got=%v", list)
	}
}

func TestStrategyFromTier(t *testing.T) {
	t.Parallel()

	if got := strategyFromTier("HOT"); got != "replication" {
		t.Fatalf("strategyFromTier HOT mismatch: got=%q", got)
	}
	if got := strategyFromTier("EC"); got != "ec" {
		t.Fatalf("strategyFromTier EC mismatch: got=%q", got)
	}
	if got := strategyFromTier("UNKNOWN"); got != "replication" {
		t.Fatalf("strategyFromTier default mismatch: got=%q", got)
	}
}
