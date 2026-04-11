package meta

import (
	"context"
	"fmt"
	"sort"
	"time"

	kvstore "hybrid_distributed_store/internal/meta/kvstore"
)

func (s *TiKVStore) UpsertNormalizedMetadata(ctx context.Context, objectID string, metadata map[string]interface{}) error {
	if s == nil || s.kv == nil {
		return nil
	}
	if objectID == "" {
		return fmt.Errorf("object id is empty")
	}

	version := resolveVersion(metadata)
	state := resolveState(metadata)
	tier := resolveTier(metadata)
	sizeBytes := toInt64(metadata["original_length"], 0)
	checksum := toString(metadata["cold_hash"], "")
	contentType := toNullableString(metadata["content_type"])
	encodingK := toNullableInt(metadata["k"])
	encodingM := toNullableInt(metadata["m"])

	now := time.Now()

	s.mu.Lock()
	defer s.mu.Unlock()

	objKey := tiKVObjectKey(objectID)
	obj, found, err := s.getObjectRecord(objKey)
	if err != nil {
		return err
	}
	if !found {
		obj = &tiKVObjectRecord{
			ObjectID:  objectID,
			TenantID:  "default",
			CreatedAt: now,
		}
	}
	obj.CurrentVersion = version
	obj.State = state
	if obj.TenantID == "" {
		obj.TenantID = "default"
	}
	obj.UpdatedAt = now

	verRec := &tiKVObjectVersionRecord{
		ObjectID:       objectID,
		Version:        version,
		SizeBytes:      sizeBytes,
		ChecksumSHA256: checksum,
		Tier:           tier,
		CreatedAt:      now,
	}
	if v, ok := contentType.(string); ok {
		verRec.ContentType = &v
	}
	if v, ok := encodingK.(int); ok {
		vv := v
		verRec.EncodingK = &vv
	}
	if v, ok := encodingM.(int); ok {
		vv := v
		verRec.EncodingM = &vv
	}

	b := s.kv.NewBatch()
	defer b.Close()

	if err := s.batchPutJSON(b, objKey, obj); err != nil {
		return err
	}
	if err := s.batchPutJSON(b, tiKVObjectVersionKey(objectID, version), verRec); err != nil {
		return err
	}

	if tier == "HOT" {
		replicaNodes := toStringSlice(metadata["replica_nodes"])
		for _, nodeID := range replicaNodes {
			if nodeID == "" {
				continue
			}
			rec := tiKVReplicaRecord{
				ObjectID: objectID,
				Version:  version,
				NodeID:   nodeID,
				Path:     objectID,
				Status:   "ACTIVE",
			}
			if err := s.batchPutJSON(b, tiKVReplicaKey(objectID, version, nodeID), &rec); err != nil {
				return err
			}
		}
	}

	if err := b.Commit(kvstore.Sync); err != nil {
		return fmt.Errorf("commit normalized metadata batch failed: %w", err)
	}
	return nil
}

func (s *TiKVStore) GetNormalizedMetadata(ctx context.Context, objectID string) (map[string]interface{}, error) {
	if s == nil || s.kv == nil {
		return nil, nil
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	obj, found, err := s.getObjectRecord(tiKVObjectKey(objectID))
	if err != nil || !found {
		return nil, err
	}
	ver, found, err := s.getObjectVersionRecord(tiKVObjectVersionKey(objectID, obj.CurrentVersion))
	if err != nil || !found {
		return nil, err
	}

	meta := map[string]interface{}{
		"key_name":     objectID,
		"strategy":     strategyFromTier(ver.Tier),
		"cold_hash":    ver.ChecksumSHA256,
		"hot_key":      fmt.Sprintf("%s_hot", objectID),
		"cold_prefix":  fmt.Sprintf("%s_cold_chunk_", objectID),
		"chunk_prefix": fmt.Sprintf("%s_cold_chunk_", objectID),
	}

	switch obj.State {
	case "EC_ACTIVE":
		meta["hot_version"] = int64(0)
		meta["cold_version"] = obj.CurrentVersion
	default:
		meta["hot_version"] = obj.CurrentVersion
		meta["cold_version"] = int64(0)
	}

	if ver.SizeBytes > 0 {
		meta["original_length"] = ver.SizeBytes
	}
	if ver.ContentType != nil && *ver.ContentType != "" {
		meta["content_type"] = *ver.ContentType
	}
	if ver.EncodingK != nil {
		meta["k"] = *ver.EncodingK
	}
	if ver.EncodingM != nil {
		meta["m"] = *ver.EncodingM
	}
	if _, ok := meta["k"]; !ok {
		meta["k"] = 4
	}
	if _, ok := meta["m"]; !ok {
		meta["m"] = 2
	}
	return meta, nil
}

func (s *TiKVStore) DeleteNormalizedMetadata(ctx context.Context, objectID string) error {
	if s == nil || s.kv == nil {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	b := s.kv.NewBatch()
	defer b.Close()

	_ = b.Delete([]byte(tiKVObjectKey(objectID)), kvstore.NoSync)
	if err := s.batchDeletePrefix(b, tiKVObjectVersionPrefix(objectID)); err != nil {
		return err
	}
	if err := s.batchDeletePrefix(b, tiKVReplicaPrefix(objectID)); err != nil {
		return err
	}
	if err := s.batchDeletePrefix(b, tiKVECShardPrefix(objectID)); err != nil {
		return err
	}
	if err := b.Commit(kvstore.Sync); err != nil {
		return fmt.Errorf("commit delete normalized metadata failed: %w", err)
	}
	return nil
}

func (s *TiKVStore) GetObjectAdminView(ctx context.Context, objectID string) (*ObjectAdminView, error) {
	if s == nil || s.kv == nil {
		return nil, nil
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	obj, found, err := s.getObjectRecord(tiKVObjectKey(objectID))
	if err != nil || !found {
		return nil, err
	}

	out := &ObjectAdminView{
		ObjectID:       obj.ObjectID,
		CurrentVersion: obj.CurrentVersion,
		State:          obj.State,
		CreatedAt:      obj.CreatedAt,
		UpdatedAt:      obj.UpdatedAt,
	}

	ver, found, err := s.getObjectVersionRecord(tiKVObjectVersionKey(objectID, obj.CurrentVersion))
	if err != nil {
		return nil, err
	}
	if found {
		v := &ObjectVersionAdminView{
			Version:        ver.Version,
			SizeBytes:      ver.SizeBytes,
			ChecksumSHA256: ver.ChecksumSHA256,
			Tier:           ver.Tier,
			CreatedAt:      ver.CreatedAt,
		}
		if ver.ContentType != nil {
			contentType := *ver.ContentType
			v.ContentType = &contentType
		}
		if ver.EncodingK != nil {
			encodingK := *ver.EncodingK
			v.EncodingK = &encodingK
		}
		if ver.EncodingM != nil {
			encodingM := *ver.EncodingM
			v.EncodingM = &encodingM
		}
		out.Version = v
	}

	replicas, err := s.listReplicaRecords(objectID, obj.CurrentVersion, "")
	if err != nil {
		return nil, err
	}
	repOut := make([]ReplicaLocationAdminView, 0, len(replicas))
	for _, r := range replicas {
		repOut = append(repOut, ReplicaLocationAdminView{
			NodeID: r.NodeID,
			Path:   r.Path,
			Status: r.Status,
		})
	}
	sort.Slice(repOut, func(i, j int) bool { return repOut[i].NodeID < repOut[j].NodeID })
	out.ReplicaLocations = repOut

	shards, err := s.listECShardRecords(objectID, obj.CurrentVersion)
	if err != nil {
		return nil, err
	}
	ecOut := make([]ECShardLocationAdminView, 0, len(shards))
	for _, sh := range shards {
		ecOut = append(ecOut, ECShardLocationAdminView{
			ShardIndex: sh.ShardIndex,
			NodeID:     sh.NodeID,
			Path:       sh.Path,
			Status:     sh.Status,
		})
	}
	sort.Slice(ecOut, func(i, j int) bool { return ecOut[i].ShardIndex < ecOut[j].ShardIndex })
	out.ECShardLocations = ecOut

	return out, nil
}
