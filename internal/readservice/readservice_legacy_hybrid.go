//go:build !no_legacy_hybrid
// +build !no_legacy_hybrid

package readservice

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"

	"hybrid_distributed_store/internal/config"
)

// GetExistingColdFields is a legacy helper for field-hybrid updates and is deprecated.
// Deprecated: Use replication-first writes with background REPL->EC tiering.
func (s *Service) GetExistingColdFields(ctx context.Context, ecNodes []string, metadata map[string]interface{}) (map[string]interface{}, error) {
	log.Printf("  %s[ReadService] Fetching existing cold fields concurrently...%s\n", config.Colors["CYAN"], config.Colors["RESET"])

	k, err := s.getIntFromMetadata(metadata, "k")
	if err != nil {
		k = config.K
	}
	originalLength, err := s.getIntFromMetadata(metadata, "original_length")
	if err != nil {
		return nil, fmt.Errorf("metadata missing 'original_length', cannot safely decode")
	}

	coldPrefix, _ := metadata["cold_prefix"].(string)
	if coldPrefix == "" {
		keyName, _ := metadata["key_name"].(string)
		coldPrefix = fmt.Sprintf("%s_cold_chunk_", keyName)
	}

	chunks := make([][]byte, len(ecNodes))
	var wg sync.WaitGroup
	var mutex sync.Mutex
	healthyCount := 0

	for i, nodeURL := range ecNodes {
		wg.Add(1)
		go func(index int, url string) {
			defer wg.Done()
			chunkKey := fmt.Sprintf("%s%d", coldPrefix, index)

			req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("%s/retrieve/%s", url, chunkKey), nil)
			if err != nil {
				return
			}

			resp, err := s.http.Do(req)
			if err != nil {
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode == http.StatusOK {
				data, err := io.ReadAll(resp.Body)
				if err != nil {
					return
				}
				mutex.Lock()
				chunks[index] = data
				healthyCount++
				mutex.Unlock()
			}
		}(i, nodeURL)
	}
	wg.Wait()

	if healthyCount < k {
		log.Printf("  %s[ReadService] Insufficient chunks. Need %d, got %d%s\n", config.Colors["RED"], k, healthyCount, config.Colors["RESET"])
		return nil, fmt.Errorf("insufficient chunks (need %d, got %d)", k, healthyCount)
	}

	if err := s.ec.Reconstruct(chunks); err != nil {
		log.Printf("  %s[ReadService] EC Reconstruction failed: %v%s\n", config.Colors["RED"], err, config.Colors["RESET"])
		return nil, err
	}

	var coldData bytes.Buffer
	for i := 0; i < k; i++ {
		if chunks[i] == nil {
			return nil, fmt.Errorf("chunk %d is nil after reconstruction", i)
		}
		coldData.Write(chunks[i])
	}

	unpaddedData := coldData.Bytes()
	if len(unpaddedData) > originalLength {
		unpaddedData = unpaddedData[:originalLength]
	} else if len(unpaddedData) < originalLength {
		log.Printf("  %s[ReadService] Warning: Reconstructed data length (%d) < Original length (%d)%s\n", config.Colors["YELLOW"], len(unpaddedData), originalLength, config.Colors["RESET"])
	}

	coldFields, err := s.utils.Deserialize(unpaddedData)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize cold fields: %v", err)
	}
	return coldFields, nil
}

// ReadFieldHybrid is a legacy read strategy and is deprecated.
// Deprecated: Use replication-first writes with background REPL->EC tiering.
func (s *Service) ReadFieldHybrid(ctx context.Context, replicaNodes, ecNodes []string, metadata map[string]interface{}) (map[string]interface{}, error) {
	log.Printf("%s[ReadService] Strategy: Field Hybrid%s\n", config.Colors["GREEN"], config.Colors["RESET"])

	var wg sync.WaitGroup
	var hotFields map[string]interface{}
	var coldFields map[string]interface{}
	var hotErr, coldErr error

	hotKey, _ := metadata["hot_key"].(string)
	if hotKey == "" {
		return nil, fmt.Errorf("metadata missing 'hot_key'")
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		hotDataBytes, err := s.ReadReplication(ctx, replicaNodes, hotKey)
		if err != nil {
			hotErr = fmt.Errorf("failed to read hot data: %v", err)
			return
		}
		hotFields, hotErr = s.utils.Deserialize(hotDataBytes)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		coldDataBytes, err := s.ReadEC(ctx, ecNodes, metadata)
		if err != nil {
			coldErr = fmt.Errorf("failed to read cold data: %v", err)
			return
		}
		coldFields, coldErr = s.utils.Deserialize(coldDataBytes)
	}()

	wg.Wait()
	if hotErr != nil {
		return nil, hotErr
	}
	if coldErr != nil {
		return nil, coldErr
	}

	return s.utils.MergeHotColdFields(hotFields, coldFields), nil
}
