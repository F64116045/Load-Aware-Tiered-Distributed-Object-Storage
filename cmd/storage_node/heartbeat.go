package main

import (
	"context"
	"fmt"
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

func getMemoryUsedPct() float64 {
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0
	}
	var memTotalKB int64
	var memAvailableKB int64
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		switch fields[0] {
		case "MemTotal:":
			if v, parseErr := strconv.ParseInt(fields[1], 10, 64); parseErr == nil {
				memTotalKB = v
			}
		case "MemAvailable:":
			if v, parseErr := strconv.ParseInt(fields[1], 10, 64); parseErr == nil {
				memAvailableKB = v
			}
		}
	}
	if memTotalKB <= 0 {
		return 0
	}
	used := memTotalKB - memAvailableKB
	if used < 0 {
		used = 0
	}
	return (float64(used) / float64(memTotalKB)) * 100
}

type cpuStatSample struct {
	total  uint64
	iowait uint64
}

func readCPUStatSample() (cpuStatSample, error) {
	data, err := os.ReadFile("/proc/stat")
	if err != nil {
		return cpuStatSample{}, err
	}
	line := strings.SplitN(string(data), "\n", 2)[0]
	fields := strings.Fields(line)
	// cpu user nice system idle iowait irq softirq steal guest guest_nice
	if len(fields) < 6 || fields[0] != "cpu" {
		return cpuStatSample{}, fmt.Errorf("unexpected /proc/stat cpu line")
	}

	var total uint64
	values := make([]uint64, 0, len(fields)-1)
	for _, f := range fields[1:] {
		v, parseErr := strconv.ParseUint(f, 10, 64)
		if parseErr != nil {
			return cpuStatSample{}, parseErr
		}
		values = append(values, v)
		total += v
	}

	return cpuStatSample{
		total:  total,
		iowait: values[4],
	}, nil
}

func computeIOWaitPct(prev *cpuStatSample) float64 {
	curr, err := readCPUStatSample()
	if err != nil {
		return 0
	}
	if prev == nil || prev.total == 0 {
		if prev != nil {
			*prev = curr
		}
		return 0
	}

	deltaTotal := curr.total - prev.total
	deltaIOWait := curr.iowait - prev.iowait
	*prev = curr

	if deltaTotal == 0 {
		return 0
	}
	return (float64(deltaIOWait) / float64(deltaTotal)) * 100
}

func registerAndHeartbeatMeta(ctx context.Context, metaStore meta.Repository, nodeURL string, storage *storageEngine) {
	if metaStore == nil {
		return
	}
	log.Printf("[%s] Starting metadata heartbeat...", nodeURL)

	ticker := time.NewTicker(config.NodeHeartbeatInterval)
	defer ticker.Stop()
	prevCPUStat := cpuStatSample{}

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
			getMemoryUsedPct(),
			computeIOWaitPct(&prevCPUStat),
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
