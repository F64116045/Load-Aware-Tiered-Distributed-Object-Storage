package meta

import (
	"context"
	"fmt"
	"strings"
)

// ReplicaLocation describes one HOT replica placement.
type ReplicaLocation struct {
	NodeID string
	Path   string
	Status string
}

// ListActiveReplicaLocations returns ACTIVE replica rows for one object version.
func (s *Store) ListActiveReplicaLocations(ctx context.Context, objectID string, version int64) ([]ReplicaLocation, error) {
	if s == nil || s.db == nil {
		return nil, nil
	}

	const q = `
SELECT node_id, path, status
FROM replica_locations
WHERE object_id = $1
  AND version = $2
  AND status = 'ACTIVE'
ORDER BY node_id ASC
`
	rows, err := s.db.QueryContext(ctx, q, objectID, version)
	if err != nil {
		return nil, fmt.Errorf("list active replica locations failed: %w", err)
	}
	defer rows.Close()

	out := make([]ReplicaLocation, 0)
	for rows.Next() {
		var r ReplicaLocation
		if err := rows.Scan(&r.NodeID, &r.Path, &r.Status); err != nil {
			return nil, fmt.Errorf("scan active replica location failed: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate active replica locations failed: %w", err)
	}
	return out, nil
}

// UpsertReplicaLocations ensures replica rows exist as ACTIVE for one object version.
func (s *Store) UpsertReplicaLocations(ctx context.Context, objectID string, version int64, nodeIDs []string) error {
	if s == nil || s.db == nil {
		return nil
	}
	if len(nodeIDs) == 0 {
		return nil
	}

	const q = `
INSERT INTO replica_locations (object_id, version, node_id, path, status)
VALUES ($1, $2, $3, $4, 'ACTIVE')
ON CONFLICT (object_id, version, node_id)
DO UPDATE SET
	path = EXCLUDED.path,
	status = EXCLUDED.status
`
	for _, nodeID := range nodeIDs {
		if nodeID == "" {
			continue
		}
		if _, err := s.db.ExecContext(ctx, q, objectID, version, nodeID, objectID); err != nil {
			return fmt.Errorf("upsert replica locations failed: %w", err)
		}
	}
	return nil
}

// MarkReplicaLocationsDeleted marks selected object-version replicas as DELETED.
func (s *Store) MarkReplicaLocationsDeleted(ctx context.Context, objectID string, version int64, nodeIDs []string) error {
	if s == nil || s.db == nil {
		return nil
	}
	if len(nodeIDs) == 0 {
		return nil
	}

	placeholders := make([]string, 0, len(nodeIDs))
	args := make([]interface{}, 0, len(nodeIDs)+2)
	args = append(args, objectID, version)
	for i, nodeID := range nodeIDs {
		placeholders = append(placeholders, fmt.Sprintf("$%d", i+3))
		args = append(args, nodeID)
	}

	q := fmt.Sprintf(`
UPDATE replica_locations
SET status = 'DELETED'
WHERE object_id = $1
  AND version = $2
  AND node_id IN (%s)
`, strings.Join(placeholders, ","))

	if _, err := s.db.ExecContext(ctx, q, args...); err != nil {
		return fmt.Errorf("mark replica locations deleted failed: %w", err)
	}
	return nil
}
