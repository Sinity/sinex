-- Drop triggers
DROP TRIGGER IF EXISTS set_event_clusters_updated_at ON core.event_clusters;
DROP TRIGGER IF EXISTS set_event_annotations_updated_at ON core.event_annotations;

-- Drop tables in reverse dependency order
DROP TABLE IF EXISTS core.event_artifact_refs;
DROP TABLE IF EXISTS core.artifact_event_sources;
DROP TABLE IF EXISTS core.event_cluster_members;
DROP TABLE IF EXISTS core.event_clusters;
DROP TABLE IF EXISTS core.artifact_relations;
DROP TABLE IF EXISTS core.event_annotations;
DROP TABLE IF EXISTS core.event_relations;