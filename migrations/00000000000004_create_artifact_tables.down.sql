-- Rollback migration from core artifacts to km schema
-- Migration: 00000000000010_migrate_km_to_core_artifacts.down.sql

-- Drop triggers
DROP TRIGGER IF EXISTS set_artifacts_updated_at ON core.artifacts;

-- Drop indexes
DROP INDEX IF EXISTS idx_event_embeddings_vector;
DROP INDEX IF EXISTS idx_event_embeddings_event;
DROP INDEX IF EXISTS idx_artifact_embeddings_vector;
DROP INDEX IF EXISTS idx_artifact_embeddings_artifact;
DROP INDEX IF EXISTS idx_event_artifact_refs_artifact;
DROP INDEX IF EXISTS idx_artifact_event_sources_event;
DROP INDEX IF EXISTS idx_artifact_relations_type;
DROP INDEX IF EXISTS idx_artifact_relations_to;
DROP INDEX IF EXISTS idx_artifact_relations_from;
DROP INDEX IF EXISTS idx_artifact_tags_tag;
DROP INDEX IF EXISTS idx_artifact_contents_extracted_search;
DROP INDEX IF EXISTS idx_artifact_contents_content_search;
DROP INDEX IF EXISTS idx_artifact_contents_artifact_id;
DROP INDEX IF EXISTS idx_core_artifacts_blob_id;
DROP INDEX IF EXISTS idx_core_artifacts_deleted_at;
DROP INDEX IF EXISTS idx_core_artifacts_metadata;
DROP INDEX IF EXISTS idx_core_artifacts_updated_at;
DROP INDEX IF EXISTS idx_core_artifacts_created_at;
DROP INDEX IF EXISTS idx_core_artifacts_type;

-- Drop tables (order matters due to foreign keys)
DROP TABLE IF EXISTS core.event_embeddings;
DROP TABLE IF EXISTS core.artifact_embeddings;
DROP TABLE IF EXISTS core.event_artifact_refs;
DROP TABLE IF EXISTS core.artifact_event_sources;
DROP TABLE IF EXISTS core.artifact_relations;
DROP TABLE IF EXISTS core.artifact_tags;
DROP TABLE IF EXISTS core.artifact_contents;
DROP TABLE IF EXISTS core.artifacts;

-- Note: This doesn't recreate the km schema tables - that would need to be done separately