-- Drop tables in reverse order to respect foreign key constraints

-- Drop triggers first
DROP TRIGGER IF EXISTS set_entities_updated_at ON core.entities;
DROP TRIGGER IF EXISTS set_artifacts_updated_at ON core.artifacts;

-- Drop indexes (automatically dropped with tables, but being explicit)

-- Drop tables
DROP TABLE IF EXISTS core.artifact_tags;
DROP TABLE IF EXISTS core.tags;
DROP TABLE IF EXISTS core.entity_relations;
DROP TABLE IF EXISTS core.entities;
DROP TABLE IF EXISTS core.artifact_contents;
DROP TABLE IF EXISTS core.artifacts;
DROP TABLE IF EXISTS core.blobs;

-- Note: We don't drop the core schema as other tables might use it