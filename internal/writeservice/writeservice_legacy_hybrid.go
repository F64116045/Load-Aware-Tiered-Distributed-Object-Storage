//go:build !no_legacy_hybrid
// +build !no_legacy_hybrid

package writeservice

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"

	"hybrid_distributed_store/internal/config"
)

// WriteFieldHybrid is a legacy write strategy and is deprecated.
// Deprecated: Use replication-first writes with background REPL->EC tiering.
func (s *Service) WriteFieldHybrid(
	ctx context.Context,
	replicaNodes, ecNodes []string,
	key string,
	dataDict map[string]interface{},
	hotOnly bool,
) (map[string]interface{}, error) {

	traceID := uuid.New().String()

	hotKey := fmt.Sprintf("%s_hot", key)
	coldPrefix := fmt.Sprintf("%s_cold_chunk_", key)

	newHot, newCold := s.utils.SeparateHotColdFields(dataDict)

	oldColdHash := ""
	var oldColdVersion int64 = 0

	oldMeta, _, err := s.loadExistingMetadata(ctx, key)
	if err != nil && !errors.Is(err, errMetadataNotFound) {
		return nil, fmt.Errorf("load existing metadata failed: %v", err)
	}
	if oldMeta != nil {
		if v, ok := oldMeta["cold_hash"].(string); ok {
			oldColdHash = v
		}
		switch v := oldMeta["cold_version"].(type) {
		case float64:
			oldColdVersion = int64(v)
		case int64:
			oldColdVersion = v
		case int:
			oldColdVersion = int64(v)
		case int32:
			oldColdVersion = int64(v)
		}
	}

	newColdBytes, _ := s.utils.Serialize(newCold)
	newColdHash := computeSHA256Hex(newColdBytes)

	isPureHot := (newColdHash == oldColdHash)
	if hotOnly {
		isPureHot = true
	}

	log.Printf("[HYBRID] Start TraceID=%s Key=%s PureHot=%v\n", traceID, key, isPureHot)

	walMeta := map[string]interface{}{
		"strategy":        config.StrategyFieldHybrid,
		"hot_key":         hotKey,
		"cold_prefix":     coldPrefix,
		"k":               config.K,
		"m":               config.M,
		"original_length": len(newColdBytes),
		"key_name":        key,
		"trace_id":        traceID,
	}

	txnID, err := s.createWALEntry(ctx, key, config.StrategyFieldHybrid, walMeta)
	if err != nil {
		return nil, err
	}

	hotBytes, _ := s.utils.Serialize(newHot)
	var wg sync.WaitGroup
	var hotMu sync.Mutex
	hotSuccess := 0

	for _, url := range replicaNodes {
		wg.Add(1)
		go func(n string) {
			defer wg.Done()
			req, _ := http.NewRequestWithContext(ctx, "POST",
				fmt.Sprintf("%s/store?key=%s", n, hotKey),
				bytes.NewReader(hotBytes),
			)
			req.Header.Set("Content-Type", "application/octet-stream")

			resp, err := s.http.Do(req)
			if err == nil && resp.StatusCode == http.StatusOK {
				hotMu.Lock()
				hotSuccess++
				hotMu.Unlock()
			}
		}(url)
	}

	var coldMu sync.Mutex
	coldSuccess := 0
	totalCold := 0

	if !isPureHot {
		chunks, err := s.ec.Split(newColdBytes)
		if err != nil {
			return nil, fmt.Errorf("EC split failed: %v", err)
		}
		if err := s.ec.Encode(chunks); err != nil {
			return nil, fmt.Errorf("EC encode failed: %v", err)
		}

		totalCold = len(chunks)

		for i := range chunks {
			wg.Add(1)
			go func(idx int, c []byte) {
				defer wg.Done()
				url := ecNodes[idx]
				req, _ := http.NewRequestWithContext(ctx, "POST",
					fmt.Sprintf("%s/store?key=%s%d", url, coldPrefix, idx),
					bytes.NewReader(c),
				)
				req.Header.Set("Content-Type", "application/octet-stream")

				resp, err := s.http.Do(req)
				if err == nil && resp.StatusCode == http.StatusOK {
					coldMu.Lock()
					coldSuccess++
					coldMu.Unlock()
				}
			}(i, chunks[i])
		}
	}

	wg.Wait()
	if hotSuccess == 0 {
		return nil, fmt.Errorf("Hybrid hot write failed entirely")
	}
	if !isPureHot && coldSuccess < config.K {
		return nil, fmt.Errorf("Hybrid cold write critical failure: %d/%d", coldSuccess, totalCold)
	}

	finalMeta := map[string]interface{}{}
	for k, v := range walMeta {
		finalMeta[k] = v
	}

	finalMeta["hot_version"] = time.Now().UnixNano()
	if isPureHot {
		finalMeta["cold_version"] = oldColdVersion
		finalMeta["cold_hash"] = oldColdHash
	} else {
		finalMeta["cold_version"] = time.Now().UnixNano()
		finalMeta["cold_hash"] = newColdHash
	}

	isDirty := false
	if hotSuccess < len(replicaNodes) {
		isDirty = true
	}
	if !isPureHot && coldSuccess < totalCold {
		isDirty = true
	}
	if isDirty {
		finalMeta["is_dirty"] = true
		log.Printf("[PartialWrite] Hybrid Key=%s marked dirty. Hot: %d/%d, Cold: %d/%d", key, hotSuccess, len(replicaNodes), coldSuccess, totalCold)
	}

	if err := s.finalizeWALEntry(ctx, txnID, true, key, finalMeta); err != nil {
		return nil, err
	}

	opType := "Cold Update"
	if isPureHot {
		opType = "Pure Hot Update"
	}

	return map[string]interface{}{
		"trace_id":            traceID,
		"is_pure_hot_update":  isPureHot,
		"hot_nodes_written":   hotSuccess,
		"cold_chunks_written": coldSuccess,
		"operation_type":      opType,
		"partial":             isDirty,
	}, nil
}
