package utils

import (
	"encoding/json"
	"io"
	"log"
	"reflect"
	"testing"
)

func TestSerialize(t *testing.T) {
	svc := NewService()

	testCases := []struct {
		name      string
		inputData map[string]interface{}
		expect    []byte
		expectErr bool
	}{
		{
			name:      "Standard Map",
			inputData: map[string]interface{}{"a": "b", "c": 123},
			expect:    []byte(`{"a":"b","c":123}`),
			expectErr: false,
		},
		{
			name:      "Empty Map",
			inputData: map[string]interface{}{},
			expect:    []byte(`{}`),
			expectErr: false,
		},
		{
			name:      "Nil Map",
			inputData: nil,
			expect:    []byte(`null`),
			expectErr: false,
		},
		{
			name:      "Invalid Type (Channel)",
			inputData: map[string]interface{}{"a": make(chan int)},
			expect:    nil,
			expectErr: true, // json.Marshal fails on channels
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gotBytes, gotErr := svc.Serialize(tc.inputData)

			if tc.expectErr {
				if gotErr == nil {
					t.Errorf("Expected error, got nil")
				}
				return
			}

			if gotErr != nil {
				t.Errorf("Expected success, got error: %v", gotErr)
				return
			}

			// JSON marshaling order is not guaranteed, so we unmarshal back to compare maps
			var gotMap, expectMap map[string]interface{}
			if err := json.Unmarshal(gotBytes, &gotMap); err != nil {
				t.Fatalf("Failed to unmarshal result: %v", err)
			}
			if err := json.Unmarshal(tc.expect, &expectMap); err != nil {
				t.Fatalf("Failed to unmarshal expectation: %v", err)
			}

			if !reflect.DeepEqual(gotMap, expectMap) {
				t.Errorf("Serialization mismatch:\nExpected: %s\nGot:      %s", string(tc.expect), string(gotBytes))
			}
		})
	}
}

func TestDeserialize(t *testing.T) {
	svc := NewService()

	testCases := []struct {
		name      string
		inputData []byte
		expect    map[string]interface{}
		expectErr bool
	}{
		{
			name:      "Standard JSON",
			inputData: []byte(`{"a": "b"}`),
			expect:    map[string]interface{}{"a": "b"},
			expectErr: false,
		},
		{
			name:      "EC Padding (Trim NULL bytes)",
			inputData: []byte("{\"b\": 123}\x00\x00\x00"),        // 3 null bytes at end
			expect:    map[string]interface{}{"b": float64(123)}, // JSON numbers are float64 by default
			expectErr: false,
		},
		{
			name:      "Invalid JSON",
			inputData: []byte(`{"a": `),
			expect:    nil,
			expectErr: true,
		},
		{
			name:      "Empty Byte Array",
			inputData: []byte(``),
			expect:    nil,
			expectErr: true,
		},
		{
			name:      "Empty JSON Object",
			inputData: []byte(`{}`),
			expect:    map[string]interface{}{},
			expectErr: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Discard logs to keep test output clean
			log.SetOutput(io.Discard)

			gotMap, gotErr := svc.Deserialize(tc.inputData)

			if tc.expectErr {
				if gotErr == nil {
					t.Errorf("Expected error, got nil")
				}
				return
			}

			if gotErr != nil {
				t.Errorf("Expected success, got error: %v", gotErr)
				return
			}

			if !reflect.DeepEqual(gotMap, tc.expect) {
				t.Errorf("Deserialization mismatch:\nExpected: %v\nGot:      %v", tc.expect, gotMap)
			}
		})
	}
}

func TestMapsAreEqual(t *testing.T) {
	svc := NewService()

	testCases := []struct {
		name   string
		map1   map[string]interface{}
		map2   map[string]interface{}
		expect bool
	}{
		{
			name:   "Identical",
			map1:   map[string]interface{}{"a": 1, "b": "c"},
			map2:   map[string]interface{}{"a": 1, "b": "c"},
			expect: true,
		},
		{
			name:   "Different Order (Should match)",
			map1:   map[string]interface{}{"a": 1, "b": "c"},
			map2:   map[string]interface{}{"b": "c", "a": 1},
			expect: true,
		},
		{
			name:   "Different Values",
			map1:   map[string]interface{}{"a": 1},
			map2:   map[string]interface{}{"a": 2},
			expect: false,
		},
		{
			name:   "Different Keys",
			map1:   map[string]interface{}{"a": 1},
			map2:   map[string]interface{}{"b": 1},
			expect: false,
		},
		{
			name:   "Nil vs Empty (Not Equal)",
			map1:   nil,
			map2:   map[string]interface{}{},
			expect: false,
		},
		{
			name:   "Empty vs Nil (Not Equal)",
			map1:   map[string]interface{}{},
			map2:   nil,
			expect: false,
		},
		{
			name:   "Nil vs Nil",
			map1:   nil,
			map2:   nil,
			expect: true,
		},
		{
			name:   "Empty vs Empty",
			map1:   map[string]interface{}{},
			map2:   map[string]interface{}{},
			expect: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := svc.MapsAreEqual(tc.map1, tc.map2)
			if got != tc.expect {
				t.Errorf("Equality check failed:\nMap1: %v\nMap2: %v\nExpected: %v, Got: %v",
					tc.map1, tc.map2, tc.expect, got)
			}
		})
	}
}
