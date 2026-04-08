package readservice

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	neturl "net/url"
	"sync"

	"hybrid_distributed_store/internal/config"
	"hybrid_distributed_store/internal/interfaces"
)

// Service implements IReadService.
// It orchestrates data retrieval across Replication and EC strategies.
type Service struct {
	http  interfaces.IHttpClient
	ec    interfaces.IEcDriver
	utils interfaces.IUtilsSvc
}

// NewService creates a new ReadService with dependency injection.
func NewService(
	http interfaces.IHttpClient,
	ec interfaces.IEcDriver,
	utils interfaces.IUtilsSvc,
) interfaces.IReadService {
	return &Service{
		http:  http,
		ec:    ec,
		utils: utils,
	}
}

// CheckFirstWrite checks if a hot key already exists in the replica nodes.
// Returns true if it's the first write (key not found), false otherwise.
func (s *Service) CheckFirstWrite(ctx context.Context, replicaNodes []string, hotKey string) (bool, error) {
	resultChan := make(chan bool, 1)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup

	// Query all replica nodes in parallel
	for _, nodeURL := range replicaNodes {
		wg.Add(1)
		go func(nodeURL string) {
			defer wg.Done()
			req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("%s/retrieve/%s", nodeURL, neturl.PathEscape(hotKey)), nil)
			if err != nil {
				return
			}
			resp, err := s.http.Do(req)
			if err != nil {
				return
			}
			defer resp.Body.Close()

			// If any node has the data, it's NOT a first write.
			if resp.StatusCode == http.StatusOK {
				select {
				case resultChan <- false:
					cancel()
				case <-ctx.Done():
				}
			}
		}(nodeURL)
	}
	wg.Wait()

	select {
	case result := <-resultChan:
		return result, nil
	default:
		// If no node returned 200 OK, treat as first write
		return true, nil
	}
}

// ReadReplication implements Strategy A: Simple Replication.
// It returns the data from the first responsive node.
func (s *Service) ReadReplication(ctx context.Context, replicaNodes []string, key string) ([]byte, error) {
	log.Printf("%s[ReadService] Strategy: Replication (key=%s)%s\n", config.Colors["GREEN"], key, config.Colors["RESET"])

	resultChan := make(chan []byte, 1)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup

	for _, nodeURL := range replicaNodes {
		wg.Add(1)
		go func(nodeURL string) {
			defer wg.Done()
			req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("%s/retrieve/%s", nodeURL, neturl.PathEscape(key)), nil)
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
				if err == nil {
					select {
					case resultChan <- data:
						cancel() // Stop other requests
					case <-ctx.Done():
					}
				}
			}
		}(nodeURL)
	}
	wg.Wait()

	select {
	case data := <-resultChan:
		return data, nil
	default:
		return nil, fmt.Errorf("key '%s' not found on any replica nodes", key)
	}
}

// ReadEC implements Strategy B: Erasure Coding.
// It fetches shards in parallel, reconstructs missing ones, and removes padding.
func (s *Service) ReadEC(ctx context.Context, ecNodes []string, metadata map[string]interface{}) ([]byte, error) {
	log.Printf("%s[ReadService] Strategy: EC (metadata=%v)%s\n", config.Colors["GREEN"], metadata, config.Colors["RESET"])

	k, err := s.getIntFromMetadata(metadata, "k")
	if err != nil {
		k = config.K
	}
	originalLength, err := s.getIntFromMetadata(metadata, "original_length")
	if err != nil {
		return nil, fmt.Errorf("metadata missing 'original_length'")
	}

	chunkPrefix, _ := metadata["cold_prefix"].(string)
	if chunkPrefix == "" {
		chunkPrefix, _ = metadata["chunk_prefix"].(string)
	}
	if chunkPrefix == "" {
		return nil, fmt.Errorf("metadata missing 'cold_prefix' or 'chunk_prefix'")
	}

	// Parallel Fetch
	chunks := make([][]byte, len(ecNodes))
	var wg sync.WaitGroup
	var mutex sync.Mutex
	healthyCount := 0

	for i, nodeURL := range ecNodes {
		wg.Add(1)
		go func(index int, nodeURL string) {
			defer wg.Done()
			chunkKey := fmt.Sprintf("%s%d", chunkPrefix, index)

			req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("%s/retrieve/%s", nodeURL, neturl.PathEscape(chunkKey)), nil)
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
		return nil, fmt.Errorf("insufficient chunks (need %d, got %d)", k, healthyCount)
	}

	if err := s.ec.Reconstruct(chunks); err != nil {
		return nil, fmt.Errorf("EC reconstruction failed: %v", err)
	}

	var data bytes.Buffer
	for i := 0; i < k; i++ {
		if chunks[i] == nil {
			return nil, fmt.Errorf("chunk %d is nil post-reconstruction", i)
		}
		data.Write(chunks[i])
	}

	// Remove Padding
	unpaddedData := data.Bytes()
	if len(unpaddedData) < originalLength {
		return nil, fmt.Errorf("data corruption: reconstructed length (%d) < original (%d)", len(unpaddedData), originalLength)
	}

	// Truncate logic
	unpaddedData = unpaddedData[:originalLength]
	log.Printf("  %s[ReadService] ReadEC truncated data length = %d%s\n", config.Colors["YELLOW"], len(unpaddedData), config.Colors["RESET"])

	return unpaddedData, nil
}

// helper: getIntFromMetadata safely extracts an integer from the generic map.
func (s *Service) getIntFromMetadata(metadata map[string]interface{}, key string) (int, error) {
	if metadata == nil {
		return 0, fmt.Errorf("metadata is nil")
	}
	value, ok := metadata[key]
	if !ok {
		return 0, fmt.Errorf("key '%s' not found in metadata", key)
	}
	switch v := value.(type) {
	case float64:
		return int(v), nil
	case int:
		return v, nil
	case int32:
		return int(v), nil
	case int64:
		return int(v), nil
	default:
		return 0, fmt.Errorf("key '%s' is of unexpected type %T", key, value)
	}
}
