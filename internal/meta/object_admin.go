package meta

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// ObjectAdminView is an admin-focused snapshot for a single object.
type ObjectAdminView struct {
	ObjectID         string
	CurrentVersion   int64
	State            string
	CreatedAt        time.Time
	UpdatedAt        time.Time
	Version          *ObjectVersionAdminView
	ReplicaLocations []ReplicaLocationAdminView
	ECShardLocations []ECShardLocationAdminView
}

// ObjectVersionAdminView describes current object version metadata.
type ObjectVersionAdminView struct {
	Version        int64
	SizeBytes      int64
	ChecksumSHA256 string
	Tier           string
	ContentType    sql.NullString
	EncodingK      sql.NullInt64
	EncodingM      sql.NullInt64
	CreatedAt      time.Time
}

// ReplicaLocationAdminView is one row from replica_locations.
type ReplicaLocationAdminView struct {
	NodeID string
	Path   string
	Status string
}

// ECShardLocationAdminView is one row from ec_shard_locations.
type ECShardLocationAdminView struct {
	ShardIndex int
	NodeID     string
	Path       string
	Status     string
}

// GetObjectAdminView returns joined admin metadata for the object's current version.
// Returns (nil, nil) when object does not exist.
func (s *Store) GetObjectAdminView(ctx context.Context, objectID string) (*ObjectAdminView, error) {
	if s == nil || s.db == nil {
		return nil, nil
	}

	view := &ObjectAdminView{}
	const objectQ = `
SELECT object_id, current_version, state, created_at, updated_at
FROM objects
WHERE object_id = $1
`
	if err := s.db.QueryRowContext(ctx, objectQ, objectID).Scan(
		&view.ObjectID,
		&view.CurrentVersion,
		&view.State,
		&view.CreatedAt,
		&view.UpdatedAt,
	); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("query object admin view failed: %w", err)
	}

	version, err := s.loadCurrentVersionAdminView(ctx, objectID, view.CurrentVersion)
	if err != nil {
		return nil, err
	}
	view.Version = version

	replicas, err := s.loadReplicaLocationsAdmin(ctx, objectID, view.CurrentVersion)
	if err != nil {
		return nil, err
	}
	view.ReplicaLocations = replicas

	shards, err := s.loadECShardLocationsAdmin(ctx, objectID, view.CurrentVersion)
	if err != nil {
		return nil, err
	}
	view.ECShardLocations = shards

	return view, nil
}

func (s *Store) loadCurrentVersionAdminView(ctx context.Context, objectID string, version int64) (*ObjectVersionAdminView, error) {
	const q = `
SELECT version, size_bytes, checksum_sha256, tier, content_type, encoding_k, encoding_m, created_at
FROM object_versions
WHERE object_id = $1
  AND version = $2
`
	out := &ObjectVersionAdminView{}
	if err := s.db.QueryRowContext(ctx, q, objectID, version).Scan(
		&out.Version,
		&out.SizeBytes,
		&out.ChecksumSHA256,
		&out.Tier,
		&out.ContentType,
		&out.EncodingK,
		&out.EncodingM,
		&out.CreatedAt,
	); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("query current object version failed: %w", err)
	}
	return out, nil
}

func (s *Store) loadReplicaLocationsAdmin(ctx context.Context, objectID string, version int64) ([]ReplicaLocationAdminView, error) {
	const q = `
SELECT node_id, path, status
FROM replica_locations
WHERE object_id = $1
  AND version = $2
ORDER BY node_id ASC
`
	rows, err := s.db.QueryContext(ctx, q, objectID, version)
	if err != nil {
		return nil, fmt.Errorf("query replica locations failed: %w", err)
	}
	defer rows.Close()

	out := make([]ReplicaLocationAdminView, 0)
	for rows.Next() {
		var r ReplicaLocationAdminView
		if err := rows.Scan(&r.NodeID, &r.Path, &r.Status); err != nil {
			return nil, fmt.Errorf("scan replica location failed: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate replica locations failed: %w", err)
	}
	return out, nil
}

func (s *Store) loadECShardLocationsAdmin(ctx context.Context, objectID string, version int64) ([]ECShardLocationAdminView, error) {
	const q = `
SELECT shard_index, node_id, path, status
FROM ec_shard_locations
WHERE object_id = $1
  AND version = $2
ORDER BY shard_index ASC
`
	rows, err := s.db.QueryContext(ctx, q, objectID, version)
	if err != nil {
		return nil, fmt.Errorf("query ec shard locations failed: %w", err)
	}
	defer rows.Close()

	out := make([]ECShardLocationAdminView, 0)
	for rows.Next() {
		var r ECShardLocationAdminView
		if err := rows.Scan(&r.ShardIndex, &r.NodeID, &r.Path, &r.Status); err != nil {
			return nil, fmt.Errorf("scan ec shard location failed: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate ec shard locations failed: %w", err)
	}
	return out, nil
}
