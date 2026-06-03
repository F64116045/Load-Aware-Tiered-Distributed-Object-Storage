package main

import (
	"fmt"
	"io"
	"net/http"
)

const apiRequestBodyPreallocMaxBytes int64 = 64 * 1024 * 1024

func readAPIRequestBody(req *http.Request) ([]byte, error) {
	if req == nil || req.Body == nil {
		return nil, nil
	}
	if req.ContentLength <= 0 || req.ContentLength > apiRequestBodyPreallocMaxBytes {
		return io.ReadAll(req.Body)
	}

	body := make([]byte, int(req.ContentLength))
	if _, err := io.ReadFull(req.Body, body); err != nil {
		return nil, fmt.Errorf("read fixed-size request body failed: %w", err)
	}
	return body, nil
}
