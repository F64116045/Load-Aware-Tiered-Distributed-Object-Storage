package main

import (
	"context"
	"log"
	"os"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"hybrid_distributed_store/internal/config"
	"hybrid_distributed_store/internal/meta"
)

func getDiskBytes(path string) (int64, int64) {
	var fs syscall.Statfs_t
	if err := syscall.Statfs(path, &fs); err != nil {
		return 0, 0
	}
	freeBytes := int64(fs.Bavail) * int64(fs.Bsize)
	totalBytes := int64(fs.Blocks) * int64(fs.Bsize)
	return freeBytes, totalBytes
}

func parseCPULoad(loadavg string, cpuCount int) float64 {
	fields := strings.Fields(loadavg)
	if len(fields) == 0 {
		return 0
	}
	load1, err := strconv.ParseFloat(fields[0], 64)
	if err != nil {
		return 0
	}
	if cpuCount <= 0 {
		return load1
	}
	return load1 / float64(cpuCount)
}

func getCPULoad() float64 {
	data, err := os.ReadFile("/proc/loadavg")
	if err != nil {
		return 0
	}
	return parseCPULoad(string(data), runtime.NumCPU())
}

func registerAndHeartbeatMeta(ctx context.Context, metaStore meta.Repository, nodeURL string, storage *storageEngine) {
	if metaStore == nil {
		return
	}
	log.Printf("[%s] Starting metadata heartbeat...", nodeURL)

	ticker := time.NewTicker(config.NodeHeartbeatInterval)
	defer ticker.Stop()

	upsert := func(status string) {
		hbCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		freeBytes, totalBytes := getDiskBytes(storage.storageDir)
		err := metaStore.UpsertNodeHeartbeat(
			hbCtx,
			nodeURL, // Keep URL as node_id so API can directly use it as endpoint.
			freeBytes,
			totalBytes,
			len(storage.writeQueue),
			getCPULoad(),
			status,
		)
		if err != nil {
			log.Printf("[%s] metadata heartbeat failed: %v", nodeURL, err)
		}
	}

	upsert("UP")
	for {
		select {
		case <-ctx.Done():
			upsert("DOWN")
			return
		case <-ticker.C:
			upsert("UP")
		}
	}
}
