use crate::schema::{
    ArchivedEvents, Blobs, EmbeddingCache, EmbeddingModels, Entities, EntityRelations,
    EventAnnotations, EventClusterMembers, EventClusters, EventEmbeddings, EventPayloadSchemas,
    EventRelations, Events, GitopsSchemaSource, OperationsLog, ProcessorCheckpoints,
    ProcessorManifests, SchemaCompatibility, SourceMaterials, Tags, ValidationCache,
};
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Create extensions
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
                CREATE EXTENSION IF NOT EXISTS "timescaledb" CASCADE;
                CREATE EXTENSION IF NOT EXISTS "pg_jsonschema";
                CREATE EXTENSION IF NOT EXISTS "ulid";
                CREATE EXTENSION IF NOT EXISTS "vector";
                CREATE EXTENSION IF NOT EXISTS "pgcrypto";
                "#,
            )
            .await?;

        // Create schemas
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                CREATE SCHEMA IF NOT EXISTS core;
                CREATE SCHEMA IF NOT EXISTS raw;
                CREATE SCHEMA IF NOT EXISTS audit;
                CREATE SCHEMA IF NOT EXISTS kg;
                CREATE SCHEMA IF NOT EXISTS sinex_schemas;
                CREATE SCHEMA IF NOT EXISTS metrics;
                CREATE SCHEMA IF NOT EXISTS sinex;
                CREATE SCHEMA IF NOT EXISTS synthesis;
                CREATE SCHEMA IF NOT EXISTS sinex_router;
                "#,
            )
            .await?;

        // Create helper function for updated_at triggers
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                CREATE OR REPLACE FUNCTION set_current_timestamp()
                RETURNS TRIGGER AS $$
                BEGIN
                    NEW.updated_at = NOW();
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;
                "#,
            )
            .await?;

        // Create ULID functions
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                CREATE OR REPLACE FUNCTION ulid_to_timestamptz(ulid_val ULID) 
                RETURNS TIMESTAMPTZ AS $$
                BEGIN
                    RETURN ulid_val::timestamp;
                END;
                $$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;
                "#,
            )
            .await?;

        // Create processor manifests
        manager
            .get_connection()
            .execute_unprepared(&ProcessorManifests::create_table())
            .await?;

        // Create processor manifests indexes
        for index_sql in ProcessorManifests::create_indexes() {
            manager
                .get_connection()
                .execute_unprepared(&index_sql)
                .await?;
        }

        // Create source material registry
        manager
            .get_connection()
            .execute_unprepared(&SourceMaterials::create_table())
            .await?;

        // Create source material indexes
        for index_sql in SourceMaterials::create_indexes() {
            manager
                .get_connection()
                .execute_unprepared(&index_sql)
                .await?;
        }

        // Create events table
        manager
            .get_connection()
            .execute_unprepared(&Events::create_table())
            .await?;

        // Add ts_ingest generated column
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                ALTER TABLE core.events 
                ADD COLUMN ts_ingest TIMESTAMPTZ NOT NULL GENERATED ALWAYS AS (id::timestamp) STORED;
                "#
            )
            .await?;

        // Create TimescaleDB hypertable with ULID partitioning
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                SELECT create_hypertable(
                    'core.events',
                    by_range('id', partition_func => 'ulid_to_timestamptz'::regproc)
                );
                "#,
            )
            .await?;

        // Create events indexes
        for index_sql in Events::create_indexes() {
            manager
                .get_connection()
                .execute_unprepared(&index_sql)
                .await?;
        }

        // Create events constraints
        // Add Provenance XOR CHECK constraint - ensures event has exactly one of source_material_id OR source_event_ids, never both, never neither
        manager
            .get_connection()
            .execute_unprepared(&Events::create_provenance_constraint())
            .await?;

        // Create archived events
        manager
            .get_connection()
            .execute_unprepared(&ArchivedEvents::create_table())
            .await?;

        for index_sql in ArchivedEvents::create_indexes() {
            manager
                .get_connection()
                .execute_unprepared(&index_sql)
                .await?;
        }

        // Create processor checkpoints
        manager
            .get_connection()
            .execute_unprepared(&ProcessorCheckpoints::create_table())
            .await?;

        for index_sql in ProcessorCheckpoints::create_indexes() {
            manager
                .get_connection()
                .execute_unprepared(&index_sql)
                .await?;
        }

        // Create processor checkpoints constraints
        for constraint_sql in ProcessorCheckpoints::create_constraints() {
            manager
                .get_connection()
                .execute_unprepared(&constraint_sql)
                .await?;
        }

        // Create operations log
        manager
            .get_connection()
            .execute_unprepared(&OperationsLog::create_table())
            .await?;

        for index_sql in OperationsLog::create_indexes() {
            manager
                .get_connection()
                .execute_unprepared(&index_sql)
                .await?;
        }

        // Create event payload schemas
        manager
            .get_connection()
            .execute_unprepared(&EventPayloadSchemas::create_table())
            .await?;

        for index_sql in EventPayloadSchemas::create_indexes() {
            manager
                .get_connection()
                .execute_unprepared(&index_sql)
                .await?;
        }

        // Create schema compatibility table
        manager
            .get_connection()
            .execute_unprepared(&SchemaCompatibility::create_table())
            .await?;

        for index_sql in SchemaCompatibility::create_indexes() {
            manager
                .get_connection()
                .execute_unprepared(&index_sql)
                .await?;
        }

        // Create gitops schema sources table
        manager
            .get_connection()
            .execute_unprepared(&GitopsSchemaSource::create_table())
            .await?;

        for index_sql in GitopsSchemaSource::create_indexes() {
            manager
                .get_connection()
                .execute_unprepared(&index_sql)
                .await?;
        }

        // Create validation cache table
        manager
            .get_connection()
            .execute_unprepared(&ValidationCache::create_table())
            .await?;

        for index_sql in ValidationCache::create_indexes() {
            manager
                .get_connection()
                .execute_unprepared(&index_sql)
                .await?;
        }

        // Create embedding models
        manager
            .get_connection()
            .execute_unprepared(&EmbeddingModels::create_table())
            .await?;

        for index_sql in EmbeddingModels::create_indexes() {
            manager
                .get_connection()
                .execute_unprepared(&index_sql)
                .await?;
        }

        // Create embedding cache
        manager
            .get_connection()
            .execute_unprepared(&EmbeddingCache::create_table())
            .await?;

        for index_sql in EmbeddingCache::create_indexes() {
            manager
                .get_connection()
                .execute_unprepared(&index_sql)
                .await?;
        }

        // TODO: Fix embedding cache trigger - references non-existent columns
        // Commenting out for now to allow migrations to run
        /*
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                CREATE OR REPLACE FUNCTION update_embedding_cache_last_used()
                RETURNS TRIGGER AS $$
                BEGIN
                    NEW.last_used_at = NOW();
                    NEW.use_count = OLD.use_count + 1;
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;

                CREATE TRIGGER update_cache_on_use
                    BEFORE UPDATE ON core.embedding_cache
                    FOR EACH ROW
                    WHEN (OLD.embedding IS NOT DISTINCT FROM NEW.embedding)
                    EXECUTE FUNCTION update_embedding_cache_last_used();
                "#,
            )
            .await?;
        */

        // Create entities
        manager
            .get_connection()
            .execute_unprepared(&Entities::create_table())
            .await?;

        for index_sql in Entities::create_indexes() {
            manager
                .get_connection()
                .execute_unprepared(&index_sql)
                .await?;
        }

        // Create entity relations
        manager
            .get_connection()
            .execute_unprepared(&EntityRelations::create_table())
            .await?;

        for index_sql in EntityRelations::create_indexes() {
            manager
                .get_connection()
                .execute_unprepared(&index_sql)
                .await?;
        }

        // Create event annotations
        manager
            .get_connection()
            .execute_unprepared(&EventAnnotations::create_table())
            .await?;

        for index_sql in EventAnnotations::create_indexes() {
            manager
                .get_connection()
                .execute_unprepared(&index_sql)
                .await?;
        }

        // Create blobs
        manager
            .get_connection()
            .execute_unprepared(&Blobs::create_table())
            .await?;

        for index_sql in Blobs::create_indexes() {
            manager
                .get_connection()
                .execute_unprepared(&index_sql)
                .await?;
        }

        // Create tags
        manager
            .get_connection()
            .execute_unprepared(&Tags::create_table())
            .await?;

        for index_sql in Tags::create_indexes() {
            manager
                .get_connection()
                .execute_unprepared(&index_sql)
                .await?;
        }

        // Create event relations
        manager
            .get_connection()
            .execute_unprepared(&EventRelations::create_table())
            .await?;

        for index_sql in EventRelations::create_indexes() {
            manager
                .get_connection()
                .execute_unprepared(&index_sql)
                .await?;
        }

        // Create event clusters
        manager
            .get_connection()
            .execute_unprepared(&EventClusters::create_table())
            .await?;

        for index_sql in EventClusters::create_indexes() {
            manager
                .get_connection()
                .execute_unprepared(&index_sql)
                .await?;
        }

        // Create event cluster members
        manager
            .get_connection()
            .execute_unprepared(&EventClusterMembers::create_table())
            .await?;

        for index_sql in EventClusterMembers::create_indexes() {
            manager
                .get_connection()
                .execute_unprepared(&index_sql)
                .await?;
        }

        // Note: Artifact table creation removed in Phase 1.3 cleanup
        // The artifact system has been replaced by the synthesis architecture

        // Create event embeddings
        manager
            .get_connection()
            .execute_unprepared(&EventEmbeddings::create_table())
            .await?;

        for index_sql in EventEmbeddings::create_indexes() {
            manager
                .get_connection()
                .execute_unprepared(&index_sql)
                .await?;
        }

        // Create triggers for updated_at
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                CREATE TRIGGER set_event_annotations_updated_at 
                    BEFORE UPDATE ON core.event_annotations 
                    FOR EACH ROW 
                    EXECUTE FUNCTION set_current_timestamp();

                CREATE TRIGGER set_event_clusters_updated_at 
                    BEFORE UPDATE ON core.event_clusters 
                    FOR EACH ROW 
                    EXECUTE FUNCTION set_current_timestamp();
                "#,
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Drop everything in reverse order
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                -- Drop tables
                DROP TABLE IF EXISTS core.event_embeddings CASCADE;
                DROP TABLE IF EXISTS core.artifact_embeddings CASCADE;
                DROP TABLE IF EXISTS core.event_artifact_refs CASCADE;
                DROP TABLE IF EXISTS core.artifact_event_sources CASCADE;
                DROP TABLE IF EXISTS core.artifact_relations CASCADE;
                DROP TABLE IF EXISTS core.artifact_tags CASCADE;
                DROP TABLE IF EXISTS core.artifact_contents CASCADE;
                DROP TABLE IF EXISTS core.artifacts CASCADE;
                DROP TABLE IF EXISTS core.event_cluster_members CASCADE;
                DROP TABLE IF EXISTS core.event_clusters CASCADE;
                DROP TABLE IF EXISTS core.event_relations CASCADE;
                DROP TABLE IF EXISTS core.tags CASCADE;
                DROP TABLE IF EXISTS core.blobs CASCADE;
                DROP TABLE IF EXISTS core.event_annotations CASCADE;
                DROP TABLE IF EXISTS core.entity_relations CASCADE;
                DROP TABLE IF EXISTS core.entities CASCADE;
                DROP TABLE IF EXISTS core.embedding_cache CASCADE;
                DROP TABLE IF EXISTS core.embedding_models CASCADE;
                DROP TABLE IF EXISTS sinex_schemas.event_payload_schemas CASCADE;
                DROP TABLE IF EXISTS core.operations_log CASCADE;
                DROP TABLE IF EXISTS core.processor_checkpoints CASCADE;
                DROP TABLE IF EXISTS core.archived_events CASCADE;
                DROP TABLE IF EXISTS core.events CASCADE;
                DROP TABLE IF EXISTS raw.source_material_registry CASCADE;
                DROP TABLE IF EXISTS core.processor_manifests CASCADE;
                
                -- Drop functions
                DROP FUNCTION IF EXISTS update_embedding_cache_last_used CASCADE;
                DROP FUNCTION IF EXISTS ulid_to_timestamptz CASCADE;
                DROP FUNCTION IF EXISTS set_current_timestamp CASCADE;
                
                -- Drop schemas
                DROP SCHEMA IF EXISTS sinex_router CASCADE;
                DROP SCHEMA IF EXISTS synthesis CASCADE;
                DROP SCHEMA IF EXISTS sinex CASCADE;
                DROP SCHEMA IF EXISTS metrics CASCADE;
                DROP SCHEMA IF EXISTS sinex_schemas CASCADE;
                DROP SCHEMA IF EXISTS audit CASCADE;
                DROP SCHEMA IF EXISTS raw CASCADE;
                DROP SCHEMA IF EXISTS core CASCADE;
                "#,
            )
            .await?;

        Ok(())
    }
}
