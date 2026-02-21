DROP INDEX IF EXISTS idx_node_heartbeats_status_last_seen;
DROP INDEX IF EXISTS idx_object_versions_tier_created_at;
DROP INDEX IF EXISTS idx_tiering_tasks_state_scheduled_priority;
DROP INDEX IF EXISTS idx_objects_state_updated_at;

DROP TABLE IF EXISTS write_journal;
DROP TABLE IF EXISTS node_heartbeats;
DROP TABLE IF EXISTS tiering_tasks;
DROP TABLE IF EXISTS ec_shard_locations;
DROP TABLE IF EXISTS replica_locations;
DROP TABLE IF EXISTS object_versions;
DROP TABLE IF EXISTS objects;
