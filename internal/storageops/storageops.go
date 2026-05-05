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

type ecShardPlacement struct {
	index  int
	nodeID string
	path   string
}

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

	placements := parseECShardPlacements(metadata)
	if len(placements) > 0 {
		return s.deleteECByPlacement(ctx, placements)
	}

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

func (s *Service) deleteECByPlacement(ctx context.Context, placements []ecShardPlacement) (int, error) {
	var wg sync.WaitGroup
	successCount := 0
	var mutex sync.Mutex
	client := s.http

	for _, placement := range placements {
		if placement.nodeID == "" || placement.path == "" {
			continue
		}
		wg.Add(1)
		go func(loc ecShardPlacement) {
			defer wg.Done()

			req, err := http.NewRequestWithContext(ctx, "DELETE", fmt.Sprintf("%s/delete/%s", loc.nodeID, neturl.PathEscape(loc.path)), nil)
			if err != nil {
				log.Printf("[%s] EC placement delete failed (req creation error): %v\n", loc.nodeID, err)
				return
			}

			resp, err := client.Do(req)
			if err != nil {
				log.Printf("[%s] EC placement delete failed (network error): %v\n", loc.nodeID, err)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusNotFound {
				mutex.Lock()
				successCount++
				mutex.Unlock()
			} else {
				log.Printf("[%s] EC placement delete failed (status: %d)\n", loc.nodeID, resp.StatusCode)
			}
		}(placement)
	}

	wg.Wait()
	return successCount, nil
}

func parseECShardPlacements(metadata map[string]interface{}) []ecShardPlacement {
	if metadata == nil {
		return nil
	}
	raw, ok := metadata["ec_shards"]
	if !ok {
		return nil
	}
	rows, ok := raw.([]interface{})
	if !ok {
		if typed, ok := raw.([]map[string]interface{}); ok {
			rows = make([]interface{}, 0, len(typed))
			for _, row := range typed {
				rows = append(rows, row)
			}
		}
	}
	if len(rows) == 0 {
		return nil
	}

	out := make([]ecShardPlacement, 0, len(rows))
	for _, row := range rows {
		fields, ok := row.(map[string]interface{})
		if !ok {
			continue
		}
		index, ok := intFromAny(fields["shard_index"])
		if !ok {
			continue
		}
		nodeID, _ := fields["node_id"].(string)
		path, _ := fields["path"].(string)
		status, _ := fields["status"].(string)
		if status != "" && status != "ACTIVE" {
			continue
		}
		if nodeID == "" || path == "" {
			continue
		}
		out = append(out, ecShardPlacement{
			index:  index,
			nodeID: nodeID,
			path:   path,
		})
	}
	return out
}

func intFromAny(value interface{}) (int, bool) {
	switch v := value.(type) {
	case float64:
		return int(v), true
	case int:
		return v, true
	case int32:
		return int(v), true
	case int64:
		return int(v), true
	default:
		return 0, false
	}
}
