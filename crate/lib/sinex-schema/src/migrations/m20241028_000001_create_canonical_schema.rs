//! The single, canonical, "squashed" migration for creating the entire Sinex database schema.
//!
//! This migration is the culmination of all architectural refinements. It builds the
//! complete v7.0 schema from scratch, establishing all tables, indexes, functions,
//! and triggers in the correct dependency order.

use crate::schema::*;
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    /// Applies the full canonical Sinex schema to the database.
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // TimescaleDB is required. We install the server's available version to avoid version drift.
        // --- Phase 1: Setup Schemas and Helper Functions ---
        // These are required before any tables can be created.
        // Core extensions and schemas (install server-available version of TimescaleDB)
        manager.get_connection().execute_unprepared(
            r#"
            DO $$
            BEGIN
              IF EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'ulid') THEN
                EXECUTE 'CREATE EXTENSION IF NOT EXISTS "ulid"';
              ELSIF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'ulid') THEN
                RAISE EXCEPTION 'ULID extension not available on server';
              END IF;
            END$$;

            DO $$
            BEGIN
              IF EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'pg_jsonschema') THEN
                EXECUTE 'CREATE EXTENSION IF NOT EXISTS "pg_jsonschema"';
              ELSE
                RAISE NOTICE 'pg_jsonschema extension not available on server; skipping JSON schema validation hooks';
              END IF;
            END$$;

            DO $$
            BEGIN
              IF EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'vector') THEN
                EXECUTE 'CREATE EXTENSION IF NOT EXISTS "vector"';
              ELSE
                RAISE NOTICE 'pgvector extension not available on server; skipping vector similarity support';
              END IF;
            END$$;

            DO $$
            DECLARE v text;
            BEGIN
              SELECT default_version INTO v FROM pg_available_extensions WHERE name = 'timescaledb';
              IF v IS NULL THEN
                RAISE EXCEPTION 'TimescaleDB extension not available on server';
              END IF;
              EXECUTE format('CREATE EXTENSION IF NOT EXISTS timescaledb WITH VERSION %L CASCADE', v);
            END$$;

            CREATE SCHEMA IF NOT EXISTS core;
            CREATE SCHEMA IF NOT EXISTS raw;
            CREATE SCHEMA IF NOT EXISTS audit;
            CREATE SCHEMA IF NOT EXISTS sinex_schemas;
            CREATE SCHEMA IF NOT EXISTS metrics;

            -- Ensure ulid type exposes binary send/receive functions for SQL clients
            CREATE OR REPLACE FUNCTION public.ulid_send(ulid)
            RETURNS bytea
            AS 'uuid_send'
            LANGUAGE internal
            IMMUTABLE STRICT PARALLEL SAFE;

            CREATE OR REPLACE FUNCTION public.ulid_recv(internal)
            RETURNS ulid
            AS 'uuid_recv'
            LANGUAGE internal
            IMMUTABLE STRICT PARALLEL SAFE;

            ALTER TYPE ulid SET (
                SEND = ulid_send,
                RECEIVE = ulid_recv
            );

            CREATE OR REPLACE FUNCTION public.ulid_to_timestamptz(id_val ULID) RETURNS TIMESTAMPTZ AS 'SELECT id_val::timestamp' LANGUAGE sql IMMUTABLE;
            CREATE OR REPLACE FUNCTION public.set_current_timestamp_updated_at() RETURNS TRIGGER AS 'BEGIN NEW.updated_at = NOW(); RETURN NEW; END;' LANGUAGE plpgsql;
            "#
        ).await?;

        // --- Phase 2: Create Tables in Dependency Order ---
        // Tables without foreign keys are created first.
        manager
            .create_table(Blobs::create_table_statement())
            .await?;
        manager
            .create_table(EventPayloadSchemas::create_table_statement())
            .await?;
        manager
            .create_table(SourceMaterialRegistry::create_table_statement())
            .await?;
        manager
            .create_table(Events::create_table_statement())
            .await?;
        manager
            .get_connection()
            .execute_unprepared(Events::create_hypertable_sql())
            .await?;
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                DO $$
                BEGIN
                    -- Replace legacy whitespace constraints with normalized checks that trim all whitespace characters.
                    ALTER TABLE core.events DROP CONSTRAINT IF EXISTS events_source_nonblank;
                    ALTER TABLE core.events DROP CONSTRAINT IF EXISTS events_source_check;
                    ALTER TABLE core.events DROP CONSTRAINT IF EXISTS core_events_source_check;
                    ALTER TABLE core.events ADD CONSTRAINT events_source_nonblank CHECK (length(BTRIM(source, E' \t\n\r\v\f')) > 0);

                    ALTER TABLE core.events DROP CONSTRAINT IF EXISTS events_event_type_nonblank;
                    ALTER TABLE core.events DROP CONSTRAINT IF EXISTS events_event_type_check;
                    ALTER TABLE core.events DROP CONSTRAINT IF EXISTS core_events_event_type_check;
                    ALTER TABLE core.events ADD CONSTRAINT events_event_type_nonblank CHECK (length(BTRIM(event_type, E' \t\n\r\v\f')) > 0);
                END
                $$;
                "#,
            )
            .await?;
        manager
            .get_connection()
            .execute_unprepared(&ArchivedEvents::create_table_sql())
            .await?;
        manager
            .create_table(OperationsLog::create_table_statement())
            .await?;
        // High-level operations API (start/complete/fail) used by repositories
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                -- Operations API helpers
                CREATE OR REPLACE FUNCTION core.start_operation(p_operation_type TEXT, p_operator TEXT, p_scope JSONB)
                RETURNS ULID AS $$
                DECLARE
                    v_operation_id ULID;
                BEGIN
                    v_operation_id := gen_ulid();
                    INSERT INTO core.operations_log (id, operation_type, operator, scope, result_status)
                    VALUES (v_operation_id, p_operation_type, p_operator, p_scope, 'running');
                    RETURN v_operation_id;
                END;
                $$ LANGUAGE plpgsql;

                CREATE OR REPLACE FUNCTION core.complete_operation(p_operation_id ULID, p_summary JSONB)
                RETURNS VOID AS $$
                BEGIN
                    UPDATE core.operations_log
                    SET result_status = 'success',
                        result_message = p_summary->>'message',
                        duration_ms = EXTRACT(MILLISECONDS FROM (NOW() - (id::timestamp)))::integer,
                        preview_summary = COALESCE(preview_summary, '{}'::jsonb) || p_summary
                    WHERE id = p_operation_id;
                END;
                $$ LANGUAGE plpgsql;

                CREATE OR REPLACE FUNCTION core.fail_operation(p_operation_id ULID, p_error JSONB)
                RETURNS VOID AS $$
                BEGIN
                    UPDATE core.operations_log
                    SET result_status = 'failure',
                        result_message = p_error->>'error',
                        duration_ms = EXTRACT(MILLISECONDS FROM (NOW() - (id::timestamp)))::integer,
                        preview_summary = COALESCE(preview_summary, '{}'::jsonb) || p_error
                    WHERE id = p_operation_id;
                END;
                $$ LANGUAGE plpgsql;
                "#,
            )
            .await?;

        manager
            .get_connection()
            .execute_unprepared(
                r#"
                CREATE OR REPLACE FUNCTION core.expand_cascade(temp_table TEXT, max_depth INTEGER)
                RETURNS INTEGER AS $$
                DECLARE
                    current_depth INTEGER := 0;
                    rows_inserted INTEGER;
                BEGIN
                    LOOP
                        IF current_depth >= max_depth THEN
                            EXIT;
                        END IF;

                        EXECUTE format(
                            'WITH current_level AS (
                                SELECT id
                                FROM %I
                                WHERE depth = $1 AND processed = FALSE
                            ),
                            children AS (
                                SELECT DISTINCT e.id, COALESCE(e.source_event_ids, ''{}''::ulid[]) AS parent_ids
                                FROM core.events e
                                JOIN current_level cl ON e.source_event_ids && ARRAY[cl.id]
                                WHERE NOT EXISTS (SELECT 1 FROM %I existing WHERE existing.id = e.id)
                            )
                            INSERT INTO %I (id, depth, parent_ids, processed)
                            SELECT c.id, $1 + 1, c.parent_ids, FALSE
                            FROM children c',
                            temp_table, temp_table, temp_table
                        )
                        USING current_depth;

                        GET DIAGNOSTICS rows_inserted = ROW_COUNT;

                        EXECUTE format('UPDATE %I SET processed = TRUE WHERE depth = $1', temp_table)
                            USING current_depth;

                        EXIT WHEN rows_inserted = 0;
                        current_depth := current_depth + 1;
                    END LOOP;

                    RETURN current_depth;
                END;
                $$ LANGUAGE plpgsql;
                "#,
            )
            .await?;
        manager
            .create_table(ProcessorCheckpoints::create_table_statement())
            .await?;
        manager
            .create_table(Entities::create_table_statement())
            .await?;
        manager.create_table(Tags::create_table_statement()).await?;
        manager
            .create_table(EmbeddingModels::create_table_statement())
            .await?;
        manager
            .create_table(EventClusters::create_table_statement())
            .await?;
        manager
            .create_table(SatelliteInstances::create_table_statement())
            .await?;
        manager
            .create_table(SatelliteSignals::create_table_statement())
            .await?;
        manager
            .create_table(SensorJobs::create_table_statement())
            .await?;
        manager
            .create_table(ProcessorManifests::create_table_statement())
            .await?;
        manager
            .create_table(GitopsSchemaSources::create_table_statement())
            .await?;
        manager
            .create_table(ValidationCache::create_table_statement())
            .await?;

        // Tables with foreign keys are created next.
        manager
            .create_table(TemporalLedger::create_table_statement())
            .await?;
        manager
            .create_table(EntityRelations::create_table_statement())
            .await?;
        manager
            .create_table(TaggedItems::create_table_statement())
            .await?;
        manager
            .create_table(EventAnnotations::create_table_statement())
            .await?;
        manager
            .create_table(EmbeddingCache::create_table_statement())
            .await?;
        manager
            .create_table(EventEmbeddings::create_table_statement())
            .await?;
        manager
            .create_table(EventClusterMembers::create_table_statement())
            .await?;
        manager
            .create_table(ServiceLeadership::create_table_statement())
            .await?;
        manager
            .create_table(SensorStates::create_table_statement())
            .await?;
        manager
            .create_table(TransactionalOutbox::create_table_statement())
            .await?;

        // Coordination indexes to keep leadership queries fast
        manager
            .get_connection()
            .execute_unprepared(SatelliteInstances::create_indexes_sql())
            .await?;
        manager
            .get_connection()
            .execute_unprepared(SatelliteSignals::create_indexes_sql())
            .await?;
        manager
            .get_connection()
            .execute_unprepared(ServiceLeadership::create_indexes_sql())
            .await?;

        // --- Phase 3: Apply Foreign Keys and Triggers ---
        // This is done after all tables exist to avoid dependency issues.

        // Archive and append-only triggers
        manager
            .get_connection()
            .execute_unprepared(ArchivedEvents::create_archive_trigger_sql())
            .await?;
        manager
            .get_connection()
            .execute_unprepared(TemporalLedger::create_append_only_trigger_sql())
            .await?;

        // Apply updated_at triggers to all tables that have the column
        manager
            .get_connection()
            .execute_unprepared(&Entities::create_updated_at_trigger_sql())
            .await?;
        manager
            .get_connection()
            .execute_unprepared(&EntityRelations::create_updated_at_trigger_sql())
            .await?;
        manager
            .get_connection()
            .execute_unprepared(&EventAnnotations::create_updated_at_trigger_sql())
            .await?;
        manager
            .get_connection()
            .execute_unprepared(&SensorJobs::create_updated_at_trigger_sql())
            .await?;
        manager
            .get_connection()
            .execute_unprepared(&SensorStates::create_updated_at_trigger_sql())
            .await?;
        manager
            .get_connection()
            .execute_unprepared(&ProcessorCheckpoints::create_updated_at_trigger_sql())
            .await?;
        manager
            .get_connection()
            .execute_unprepared(&EventPayloadSchemas::create_updated_at_trigger_sql())
            .await?;
        manager
            .get_connection()
            .execute_unprepared(&GitopsSchemaSources::create_updated_at_trigger_sql())
            .await?;

        // --- Phase 4: Create Indexes ---
        // Indexes are created last for maximum performance during the initial data load.
        for index in SourceMaterialRegistry::create_indexes() {
            manager.create_index(index).await?;
        }
        for index in Events::create_indexes() {
            manager.create_index(index).await?;
        }
        for index_sql in Events::create_gin_indexes_sql() {
            manager
                .get_connection()
                .execute_unprepared(&index_sql)
                .await?;
        }
        for index in Blobs::create_indexes() {
            manager.create_index(index).await?;
        }
        for index in TemporalLedger::create_indexes() {
            manager.create_index(index).await?;
        }
        for index in Entities::create_indexes() {
            manager.create_index(index).await?;
        }
        for index_sql in Entities::create_gin_indexes_sql() {
            manager
                .get_connection()
                .execute_unprepared(&index_sql)
                .await?;
        }
        for index in EntityRelations::create_indexes() {
            manager.create_index(index).await?;
        }
        for index in TaggedItems::create_indexes() {
            manager.create_index(index).await?;
        }
        for index in EventAnnotations::create_indexes() {
            manager.create_index(index).await?;
        }
        for index_sql in EventAnnotations::create_gin_indexes_sql() {
            manager
                .get_connection()
                .execute_unprepared(&index_sql)
                .await?;
        }
        for index in EmbeddingModels::create_indexes() {
            manager.create_index(index).await?;
        }
        for index in EmbeddingCache::create_indexes() {
            manager.create_index(index).await?;
        }
        for index_sql in EmbeddingCache::create_indexes_sql() {
            manager
                .get_connection()
                .execute_unprepared(&index_sql)
                .await?;
        }
        for index in EventEmbeddings::create_indexes() {
            manager.create_index(index).await?;
        }
        for index_sql in EventEmbeddings::create_indexes_sql() {
            manager
                .get_connection()
                .execute_unprepared(&index_sql)
                .await?;
        }
        for index in SensorJobs::create_indexes() {
            manager.create_index(index).await?;
        }
        for index in SensorStates::create_indexes() {
            manager.create_index(index).await?;
        }
        for index in TransactionalOutbox::create_indexes() {
            manager.create_index(index).await?;
        }
        for index in ProcessorCheckpoints::create_indexes() {
            manager.create_index(index).await?;
        }
        for index in EventPayloadSchemas::create_indexes() {
            manager.create_index(index).await?;
        }
        for index in ProcessorManifests::create_indexes() {
            manager.create_index(index).await?;
        }
        for index in GitopsSchemaSources::create_indexes() {
            manager.create_index(index).await?;
        }

        Ok(())
    }

    /// Reverts the entire canonical schema.
    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Drop everything in reverse dependency order.
        manager
            .get_connection()
            .execute_unprepared(
                r#"
            DROP SCHEMA IF EXISTS core CASCADE;
            DROP SCHEMA IF EXISTS raw CASCADE;
            DROP SCHEMA IF EXISTS audit CASCADE;
            DROP SCHEMA IF EXISTS sinex_schemas CASCADE;
            DROP SCHEMA IF EXISTS metrics CASCADE;
            DROP FUNCTION IF EXISTS public.ulid_to_timestamptz(ULID);
            DROP FUNCTION IF EXISTS public.set_current_timestamp_updated_at();
            "#,
            )
            .await?;
        Ok(())
    }
}
