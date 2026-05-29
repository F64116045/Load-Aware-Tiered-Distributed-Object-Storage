package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

func keyFromPathParam(c *gin.Context, name string) (string, error) {
	raw := strings.TrimPrefix(c.Param(name), "/")
	if raw == "" {
		return "", fmt.Errorf("missing key")
	}
	key, err := url.PathUnescape(raw)
	if err != nil {
		return "", fmt.Errorf("invalid key escape: %w", err)
	}
	return key, nil
}

func registerRoutes(router gin.IRoutes, storage *storageEngine) {
	router.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"status":  "healthy",
			"service": "storage_node_" + storage.port,
		})
	})

	router.GET("/info", func(c *gin.Context) {
		info, err := storage.getInfo()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, info)
	})

	router.POST("/store", func(c *gin.Context) {
		start := time.Now()
		key := c.Query("key")
		if key == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Missing 'key' query parameter"})
			return
		}

		bodyReadStart := time.Now()
		data, err := io.ReadAll(c.Request.Body)
		bodyReadDuration := time.Since(bodyReadStart)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Failed to read body"})
			return
		}

		storeStart := time.Now()
		size, err := storage.store(c.Request.Context(), key, data)
		storeDuration := time.Since(storeStart)
		if err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": err.Error()})
			return
		}
		totalDuration := time.Since(start)
		log.Printf(
			"[Storage Route Phase] op=STORE key=%s size_bytes=%d body_read_ms=%d store_wait_ms=%d total_ms=%d",
			key,
			len(data),
			bodyReadDuration.Milliseconds(),
			storeDuration.Milliseconds(),
			totalDuration.Milliseconds(),
		)

		info, _ := storage.getInfo()
		c.JSON(http.StatusOK, gin.H{
			"status":                 "ok",
			"key":                    key,
			"size":                   size,
			"total_keys":             info["total_keys"],
			"io_workers":             info["io_workers"],
			"queued_write_bytes":     info["queued_write_bytes"],
			"max_queued_write_bytes": info["max_queued_write_bytes"],
			"phase_latency_ms": gin.H{
				"body_read":  bodyReadDuration.Milliseconds(),
				"store_wait": storeDuration.Milliseconds(),
				"total":      totalDuration.Milliseconds(),
			},
		})
	})

	retrieveHandler := func(c *gin.Context) {
		key, err := keyFromPathParam(c, "key")
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		data, err := storage.retrieve(key)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		if data == nil {
			c.JSON(http.StatusNotFound, gin.H{"detail": "Key not found"})
			return
		}
		if c.Request.Method == http.MethodHead {
			c.Status(http.StatusOK)
			return
		}
		c.Data(http.StatusOK, "application/octet-stream", data)
	}
	router.GET("/retrieve/*key", retrieveHandler)
	router.HEAD("/retrieve/*key", retrieveHandler)

	router.DELETE("/delete/*key", func(c *gin.Context) {
		key, err := keyFromPathParam(c, "key")
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		deleted, err := storage.delete(key)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		if !deleted {
			c.JSON(http.StatusOK, gin.H{"status": "ok", "key": key, "detail": "not_found"})
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "ok", "key": key, "message": "deleted"})
	})
}
