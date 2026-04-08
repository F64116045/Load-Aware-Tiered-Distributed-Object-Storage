package storageops

import (
	"context"
	"fmt"
	"log"
	"net/http"
	neturl "net/url"
	"sync"

	"hybrid_distributed_store/internal/config"
	"hybrid_distributed_store/internal/interfaces"
)

// Service implements IStorageOps.
type Service struct {
	http interfaces.IHttpClient
}

// NewService creates a new StorageOps service with dependency injection.
func NewService(http interfaces.IHttpClient) interfaces.IStorageOps {
	return &Service{
		http: http,
	}
}

// DeleteReplication deletes the key from all replica nodes concurrently.
func (s *Service) DeleteReplication(ctx context.Context, replicaNodes []string, key string) (int, error) {
	log.Printf("%s[StorageOps] Deleting (Replication) key=%s%s\n", config.Colors["RED"], key, config.Colors["RESET"])

	var wg sync.WaitGroup
	successCount := 0
	var mutex sync.Mutex
	client := s.http

	for _, nodeURL := range replicaNodes {
		wg.Add(1)
		go func(nodeURL string) {
			defer wg.Done()

			req, err := http.NewRequestWithContext(ctx, "DELETE", fmt.Sprintf("%s/delete/%s", nodeURL, neturl.PathEscape(key)), nil)
			if err != nil {
				log.Printf("[%s] Delete failed (req creation error): %v\n", nodeURL, err)
				return
			}

			resp, err := client.Do(req)
			if err != nil {
				log.Printf("[%s] Delete failed (network error): %v\n", nodeURL, err)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusNotFound {
				// Count 404 as success (idempotent delete)
				mutex.Lock()
				successCount++
				mutex.Unlock()
			} else {
				log.Printf("[%s] Delete failed (status: %d)\n", nodeURL, resp.StatusCode)
			}
		}(nodeURL)
	}

	wg.Wait()
	return successCount, nil
}

// DeleteEC deletes all shards associated with the key from EC nodes concurrently.
func (s *Service) DeleteEC(ctx context.Context, ecNodes []string, metadata map[string]interface{}) (int, error) {
	log.Printf("%s[StorageOps] Deleting (EC) key (metadata: %v)%s\n", config.Colors["RED"], metadata, config.Colors["RESET"])

	var chunkPrefix string
	if prefix, ok := metadata["chunk_prefix"].(string); ok {
		chunkPrefix = prefix
	} else {
		// Fallback for Healer rollback or legacy metadata
		keyName, _ := metadata["key_name"].(string)
		chunkPrefix = fmt.Sprintf("%s_chunk_", keyName)
	}

	var wg sync.WaitGroup
	successCount := 0
	var mutex sync.Mutex
	client := s.http

	for i, nodeURL := range ecNodes {
		wg.Add(1)
		go func(nodeURL string, chunkIndex int) {
			defer wg.Done()

			chunkKey := fmt.Sprintf("%s%d", chunkPrefix, chunkIndex)
			req, err := http.NewRequestWithContext(ctx, "DELETE", fmt.Sprintf("%s/delete/%s", nodeURL, neturl.PathEscape(chunkKey)), nil)
			if err != nil {
				log.Printf("[%s] EC Delete failed (req creation error): %v\n", nodeURL, err)
				return
			}

			resp, err := client.Do(req)
			if err != nil {
				log.Printf("[%s] EC Delete failed (network error): %v\n", nodeURL, err)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusNotFound {
				mutex.Lock()
				successCount++
				mutex.Unlock()
			} else {
				log.Printf("[%s] EC Delete failed (status: %d)\n", nodeURL, resp.StatusCode)
			}
		}(nodeURL, i)
	}

	wg.Wait()
	return successCount, nil
}
