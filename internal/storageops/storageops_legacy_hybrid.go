package storageops

import (
	"context"
	"fmt"
	"log"
	"sync"

	"hybrid_distributed_store/internal/config"
)

// DeleteFieldHybrid is a legacy delete strategy and is deprecated.
// Deprecated: Use replication-first writes with background REPL->EC tiering.
func (s *Service) DeleteFieldHybrid(ctx context.Context, replicaNodes, ecNodes []string, metadata map[string]interface{}) (int, int, error) {
	log.Printf("%s[StorageOps] Deleting (Field Hybrid) key (metadata: %v)%s\n", config.Colors["RED"], metadata, config.Colors["RESET"])

	keyName := "unknown_key"
	if k, ok := metadata["key_name"].(string); ok {
		keyName = k
	}

	var hotKey string
	if hk, ok := metadata["hot_key"].(string); ok {
		hotKey = hk
	} else {
		hotKey = fmt.Sprintf("%s_hot", keyName)
	}

	var coldPrefix string
	if cp, ok := metadata["cold_prefix"].(string); ok {
		coldPrefix = cp
	} else {
		coldPrefix = fmt.Sprintf("%s_cold_chunk_", keyName)
	}

	ecMetadata := map[string]interface{}{
		"chunk_prefix": coldPrefix,
		"key_name":     keyName,
	}

	var wg sync.WaitGroup
	var hotSuccess, coldSuccess int
	var hotErr, coldErr error

	wg.Add(2)
	go func() {
		defer wg.Done()
		hotSuccess, hotErr = s.DeleteReplication(ctx, replicaNodes, hotKey)
	}()
	go func() {
		defer wg.Done()
		coldSuccess, coldErr = s.DeleteEC(ctx, ecNodes, ecMetadata)
	}()
	wg.Wait()

	if hotErr != nil {
		return 0, 0, hotErr
	}
	if coldErr != nil {
		return 0, 0, coldErr
	}
	return hotSuccess, coldSuccess, nil
}
