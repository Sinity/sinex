-- Down migration for 00000000000002_create_core_tables.sql
-- Drops all objects created in the up migration

-- Drop triggers
DROP TRIGGER IF EXISTS set_event_clusters_updated_at ON core.event_clusters;
DROP TRIGGER IF EXISTS set_event_annotations_updated_at ON core.event_annotations;
DROP TRIGGER IF EXISTS update_cache_on_use ON core.embedding_cache;

-- Drop views
DROP VIEW IF EXISTS public.ops_log;

-- Drop indexes for new tables
DROP INDEX IF EXISTS idx_event_cluster_members_event;
DROP INDEX IF EXISTS idx_event_clusters_time;
DROP INDEX IF EXISTS idx_event_clusters_type;
DROP INDEX IF EXISTS idx_event_relations_confidence;
DROP INDEX IF EXISTS idx_event_relations_type;
DROP INDEX IF EXISTS idx_event_relations_to;
DROP INDEX IF EXISTS idx_event_relations_from;
DROP INDEX IF EXISTS idx_event_annotations_search;
DROP INDEX IF EXISTS idx_event_annotations_created;
DROP INDEX IF EXISTS idx_event_annotations_type;
DROP INDEX IF EXISTS idx_event_annotations_event;
DROP INDEX IF EXISTS idx_tags_name;
DROP INDEX IF EXISTS idx_tags_parent;
DROP INDEX IF EXISTS idx_blobs_verification;
DROP INDEX IF EXISTS idx_blobs_checksum_blake3;
DROP INDEX IF EXISTS idx_blobs_checksum_sha256;
DROP INDEX IF EXISTS idx_blobs_annex_key;

-- Drop indexes for existing tables
DROP INDEX IF EXISTS idx_entity_relations_created_from;
DROP INDEX IF EXISTS idx_entity_relations_valid;
DROP INDEX IF EXISTS idx_entity_relations_type;
DROP INDEX IF EXISTS idx_entity_relations_to;
DROP INDEX IF EXISTS idx_entity_relations_from;
DROP INDEX IF EXISTS idx_entities_created_from;
DROP INDEX IF EXISTS idx_entities_canonical;
DROP INDEX IF EXISTS idx_entities_name;
DROP INDEX IF EXISTS idx_entities_type;
DROP INDEX IF EXISTS idx_operations_log_time;
DROP INDEX IF EXISTS idx_operations_log_type;
DROP INDEX IF EXISTS idx_processor_checkpoints_consumer;
DROP INDEX IF EXISTS idx_processor_checkpoints_processor;
DROP INDEX IF EXISTS idx_processor_checkpoints_updated;
DROP INDEX IF EXISTS idx_source_material_checksum;
DROP INDEX IF EXISTS idx_source_material_uri;
DROP INDEX IF EXISTS idx_source_material_type_time;
DROP INDEX IF EXISTS idx_processor_manifests_time_range;
DROP INDEX IF EXISTS idx_processor_manifests_active;
DROP INDEX IF EXISTS idx_core_events_time_range;
DROP INDEX IF EXISTS idx_core_events_associated_blobs;
DROP INDEX IF EXISTS idx_core_events_processor;
DROP INDEX IF EXISTS idx_core_events_anchor_byte;
DROP INDEX IF EXISTS idx_core_events_source_material;
DROP INDEX IF EXISTS idx_core_events_source_event_ids;
DROP INDEX IF EXISTS idx_core_events_source_type_orig;
DROP INDEX IF EXISTS idx_core_events_source_type_ingest;

-- Drop tables (order matters due to foreign keys)
DROP TABLE IF EXISTS core.event_cluster_members;
DROP TABLE IF EXISTS core.event_clusters;
DROP TABLE IF EXISTS core.event_relations;
DROP TABLE IF EXISTS core.event_annotations;
DROP TABLE IF EXISTS core.tags;
DROP TABLE IF EXISTS core.blobs;
DROP TABLE IF EXISTS core.entity_relations;
DROP TABLE IF EXISTS core.entities;
DROP TABLE IF EXISTS core.embedding_cache;
DROP TABLE IF EXISTS core.embedding_models;
DROP TABLE IF EXISTS core.operations_log;
DROP TABLE IF EXISTS core.processor_checkpoints;
DROP TABLE IF EXISTS core.events;
DROP TABLE IF EXISTS raw.source_material_registry;
DROP TABLE IF EXISTS core.processor_manifests;

-- Drop functions
DROP FUNCTION IF EXISTS set_current_timestamp();
DROP FUNCTION IF EXISTS ulid_to_timestamptz(ULID);
DROP FUNCTION IF EXISTS update_embedding_cache_last_used();