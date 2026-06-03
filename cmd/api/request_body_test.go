package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestReadAPIRequestBodyKnownLength(t *testing.T) {
	req := httptest.NewRequest(http.MethodPut, "/v2/objects/o1", strings.NewReader("payload"))

	body, err := readAPIRequestBody(req)
	if err != nil {
		t.Fatalf("readAPIRequestBody failed: %v", err)
	}
	if string(body) != "payload" {
		t.Fatalf("body mismatch: got=%q", string(body))
	}
}

func TestReadAPIRequestBodyTruncatedKnownLength(t *testing.T) {
	req := httptest.NewRequest(http.MethodPut, "/v2/objects/o1", strings.NewReader("abc"))
	req.ContentLength = 5

	if _, err := readAPIRequestBody(req); err == nil {
		t.Fatalf("expected truncated known-length body to fail")
	}
}

func TestReadAPIRequestBodyUnknownLength(t *testing.T) {
	req := httptest.NewRequest(http.MethodPut, "/v2/objects/o1", strings.NewReader("streamed"))
	req.ContentLength = -1

	body, err := readAPIRequestBody(req)
	if err != nil {
		t.Fatalf("readAPIRequestBody failed: %v", err)
	}
	if string(body) != "streamed" {
		t.Fatalf("body mismatch: got=%q", string(body))
	}
}
