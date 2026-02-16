//! The single, canonical, "squashed" migration for creating the entire Sinex database schema.
//!
//! This migration is the culmination of all architectural refinements. It builds the
//! complete v7.0 schema from scratch, establishing all tables, indexes, functions,
//! and triggers in the correct dependency order.

use crate::schema::{
    ArchivedEvents, Blobs, EmbeddingCache, EmbeddingModels, Entities, EntityRelations,
    EventAnnotations, EventClusterMembers, EventClusters, EventEmbeddings, EventPayloadSchemas,
    Events, GitopsSchemaSources, OperationsLog, ProcessorManifests, SourceMaterialRegistry,
    TaggedItems, Tags, TemporalLedger, ValidationCache,
};
use sea_orm::{DatabaseBackend, Statement};
use sea_orm_migration::prelude::*;
use std::env;
const REQUIRED_EXTENSIONS: &[&str] = &["ulid", "pg_jsonschema", "vector", "timescaledb", "pg_trgm"];

#[derive(DeriveMigrationName)]
pub(crate) struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    /// Applies the full canonical Sinex schema to the database.
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        ensure_required_extensions(manager.get_connection()).await?;

        // --- Phase 1: Setup Schemas and Helper Functions ---
        // These are required before any tables can be created.
        manager.get_connection().execute_unprepared(
            r"
            CREATE SCHEMA IF NOT EXISTS core;
            CREATE SCHEMA IF NOT EXISTS raw;
            CREATE SCHEMA IF NOT EXISTS audit;
            CREATE SCHEMA IF NOT EXISTS sinex_schemas;
            CREATE SCHEMA IF NOT EXISTS metrics;

            CREATE OR REPLACE FUNCTION public.set_current_timestamp_updated_at() RETURNS TRIGGER AS 'BEGIN NEW.updated_at = NOW(); RETURN NEW; END;' LANGUAGE plpgsql;

            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1
                    FROM pg_proc p
                    JOIN pg_namespace n ON n.oid = p.pronamespace
                    WHERE n.nspname = 'public'
                      AND p.proname = 'ulid_to_timestamptz'
                      AND p.pronargs = 1
                ) THEN
                    CREATE FUNCTION public.ulid_to_timestamptz(input ULID)
                    RETURNS TIMESTAMPTZ
                    AS 'SELECT input::timestamp'
                    LANGUAGE sql
                    IMMUTABLE;
                END IF;
            END;
            $$;

            CREATE TABLE IF NOT EXISTS sinex_schemas.dlq_events (
                dlq_id ULID PRIMARY KEY DEFAULT gen_ulid(),
                failed_event_id ULID NOT NULL,
                automaton_name TEXT NOT NULL,
                agent_name TEXT,
                source TEXT NOT NULL,
                event_type TEXT NOT NULL,
                error_category TEXT NOT NULL CHECK (error_category IN ('retryable','permanent','system','user')),
                failure_reason TEXT NOT NULL,
                original_event_payload JSONB NOT NULL,
                additional_metadata JSONB,
                retry_count INTEGER NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
                failed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                last_retry_at TIMESTAMPTZ,
                next_retry_at TIMESTAMPTZ,
                resolved_at TIMESTAMPTZ,
                resolved_by TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE INDEX IF NOT EXISTS idx_dlq_events_automaton ON sinex_schemas.dlq_events (automaton_name);
            CREATE INDEX IF NOT EXISTS idx_dlq_events_resolved ON sinex_schemas.dlq_events (resolved_at);
            CREATE INDEX IF NOT EXISTS idx_dlq_events_category ON sinex_schemas.dlq_events (error_category);

            DROP TRIGGER IF EXISTS set_timestamp ON sinex_schemas.dlq_events;
            CREATE TRIGGER set_timestamp
                BEFORE UPDATE ON sinex_schemas.dlq_events
                FOR EACH ROW
                EXECUTE FUNCTION public.set_current_timestamp_updated_at();
            "
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
                r"
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
                ",
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
                r"
                -- Operations API helpers
                CREATE OR REPLACE FUNCTION core.start_operation(p_operation_type TEXT, p_operator TEXT, p_scope JSONB, p_scope_window tstzrange DEFAULT NULL)
                RETURNS ULID AS $$
                DECLARE
                    v_operation_id ULID;
                BEGIN
                    v_operation_id := gen_ulid();
                    INSERT INTO core.operations_log (id, operation_type, operator, scope, scope_window, result_status)
                    VALUES (v_operation_id, p_operation_type, p_operator, p_scope, p_scope_window, 'running');
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

                CREATE OR REPLACE FUNCTION core.prepare_cascade_session(p_session_id TEXT, p_drop_on_commit BOOLEAN DEFAULT FALSE)
                RETURNS TEXT AS $$
                DECLARE
                    v_table TEXT := format('cascade_analysis_%s', p_session_id);
                    v_clause TEXT := CASE WHEN p_drop_on_commit THEN ' ON COMMIT DROP' ELSE '' END;
                    v_create TEXT;
                BEGIN
                    IF p_session_id !~ '^[A-Za-z0-9_]+$' THEN
                        RAISE EXCEPTION 'Invalid session identifier: %', p_session_id;
                    END IF;

                    IF p_drop_on_commit THEN
                        v_create := format(
                            'CREATE TEMP TABLE %I (
                                id ULID PRIMARY KEY,
                                depth INT NOT NULL DEFAULT 0,
                                parent_ids ULID[] DEFAULT ''{}''::ULID[],
                                child_ids ULID[],
                                is_archived BOOLEAN DEFAULT FALSE,
                                is_live BOOLEAN DEFAULT TRUE,
                                processed BOOLEAN DEFAULT FALSE
                            )%s',
                            v_table,
                            v_clause
                        );
                        EXECUTE v_create;
                    ELSE
                        v_create := format(
                            'CREATE TEMP TABLE IF NOT EXISTS %I (
                                id ULID PRIMARY KEY,
                                depth INT NOT NULL DEFAULT 0,
                                parent_ids ULID[] DEFAULT ''{}''::ULID[],
                                child_ids ULID[],
                                is_archived BOOLEAN DEFAULT FALSE,
                                is_live BOOLEAN DEFAULT TRUE,
                                processed BOOLEAN DEFAULT FALSE
                            )',
                            v_table
                        );
                        EXECUTE v_create;
                        EXECUTE format(
                            'CREATE INDEX IF NOT EXISTS %I ON %I (depth)',
                            'idx_' || v_table || '_depth',
                            v_table
                        );
                        EXECUTE format(
                            'CREATE INDEX IF NOT EXISTS %I ON %I (processed)',
                            'idx_' || v_table || '_processed',
                            v_table
                        );
                    END IF;

                    RETURN v_table;
                END;
                $$ LANGUAGE plpgsql;

                CREATE OR REPLACE FUNCTION core.cascade_populate_roots(p_table TEXT, p_event_ids ULID[])
                RETURNS BIGINT AS $$
                DECLARE
                    v_sql TEXT;
                    v_rows BIGINT;
                BEGIN
                    IF p_table !~ '^cascade_analysis_[A-Za-z0-9_]+$' THEN
                        RAISE EXCEPTION 'Invalid cascade table name: %', p_table;
                    END IF;

                    v_sql := format(
                        'INSERT INTO %I (id, depth, parent_ids, processed)
                         SELECT e.id, 0, COALESCE(e.source_event_ids, ''{}''::ULID[]), FALSE
                         FROM core.events e
                         WHERE e.id = ANY($1::ulid[])
                         ON CONFLICT DO NOTHING',
                        p_table
                    );
                    EXECUTE v_sql USING p_event_ids;
                    GET DIAGNOSTICS v_rows = ROW_COUNT;
                    RETURN COALESCE(v_rows, 0);
                END;
                $$ LANGUAGE plpgsql;

                CREATE OR REPLACE FUNCTION core.cascade_count_nodes(p_table TEXT)
                RETURNS BIGINT AS $$
                DECLARE
                    v_sql TEXT;
                    v_count BIGINT;
                BEGIN
                    IF p_table !~ '^cascade_analysis_[A-Za-z0-9_]+$' THEN
                        RAISE EXCEPTION 'Invalid cascade table name: %', p_table;
                    END IF;

                    v_sql := format('SELECT COUNT(*) FROM %I', p_table);
                    EXECUTE v_sql INTO v_count;
                    RETURN COALESCE(v_count, 0);
                END;
                $$ LANGUAGE plpgsql;

                CREATE OR REPLACE FUNCTION core.cascade_depth_histogram(p_table TEXT)
                RETURNS TABLE(depth INT, node_count BIGINT) AS $$
                DECLARE
                    v_sql TEXT;
                BEGIN
                    IF p_table !~ '^cascade_analysis_[A-Za-z0-9_]+$' THEN
                        RAISE EXCEPTION 'Invalid cascade table name: %', p_table;
                    END IF;

                    v_sql := format(
                        'SELECT depth, COUNT(*) AS node_count FROM %I GROUP BY depth ORDER BY depth',
                        p_table
                    );
                    RETURN QUERY EXECUTE v_sql;
                END;
                $$ LANGUAGE plpgsql;

                CREATE OR REPLACE FUNCTION core.cascade_find_integrity_violations(p_table TEXT, p_limit INTEGER DEFAULT 100)
                RETURNS TABLE(live_event_id ULID, archived_event_id ULID) AS $$
                DECLARE
                    v_sql TEXT;
                BEGIN
                    IF p_table !~ '^cascade_analysis_[A-Za-z0-9_]+$' THEN
                        RAISE EXCEPTION 'Invalid cascade table name: %', p_table;
                    END IF;

                    v_sql := format(
                        'WITH archived_set AS (
                            SELECT id FROM %I WHERE depth = 0
                        ),
                        violations AS (
                            SELECT 
                                e.id as live_event_id,
                                unnest(e.source_event_ids) as archived_event_id
                            FROM core.events e
                            WHERE e.source_event_ids && (SELECT array_agg(id) FROM archived_set)
                            AND e.id NOT IN (SELECT id FROM %I)
                        )
                        SELECT DISTINCT live_event_id, archived_event_id
                        FROM violations
                        LIMIT $1',
                        p_table,
                        p_table
                    );
                    RETURN QUERY EXECUTE v_sql USING p_limit;
                END;
                $$ LANGUAGE plpgsql;

                CREATE OR REPLACE FUNCTION core.cascade_find_integrity_violations_paginated(
                    p_table TEXT,
                    p_limit INTEGER DEFAULT 1000,
                    p_offset INTEGER DEFAULT 0
                )
                RETURNS TABLE(live_event_id ULID, archived_event_id ULID) AS $$
                DECLARE
                    v_sql TEXT;
                BEGIN
                    IF p_table !~ '^cascade_analysis_[A-Za-z0-9_]+$' THEN
                        RAISE EXCEPTION 'Invalid cascade table name: %', p_table;
                    END IF;

                    v_sql := format(
                        'WITH archived_set AS (
                            SELECT id FROM %I WHERE depth = 0
                        ),
                        violations AS (
                            SELECT
                                e.id as live_event_id,
                                unnest(e.source_event_ids) as archived_event_id
                            FROM core.events e
                            WHERE e.source_event_ids && (SELECT array_agg(id) FROM archived_set)
                            AND e.id NOT IN (SELECT id FROM %I)
                        )
                        SELECT DISTINCT live_event_id, archived_event_id
                        FROM violations
                        LIMIT $1 OFFSET $2',
                        p_table,
                        p_table
                    );
                    RETURN QUERY EXECUTE v_sql USING p_limit, p_offset;
                END;
                $$ LANGUAGE plpgsql;

                CREATE OR REPLACE FUNCTION core.cleanup_cascade_session(p_table TEXT)
                RETURNS VOID AS $$
                BEGIN
                    IF p_table IS NULL OR p_table = '' THEN
                        RETURN;
                    END IF;
                    IF p_table !~ '^cascade_analysis_[A-Za-z0-9_]+$' THEN
                        RAISE EXCEPTION 'Invalid cascade table name: %', p_table;
                    END IF;
                    EXECUTE format('DROP TABLE IF EXISTS %I', p_table);
                END;
                $$ LANGUAGE plpgsql;
                ",
            )
            .await?;

        manager
            .get_connection()
            .execute_unprepared(
                r"
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
                ",
            )
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

        // --- Phase 3: Apply Foreign Keys and Triggers ---
        // This is done after all tables exist to avoid dependency issues.

        // Archive and append-only triggers
        manager
            .get_connection()
            .execute_unprepared(ArchivedEvents::create_archive_trigger_sql())
            .await?;
        manager
            .get_connection()
            .execute_unprepared(Events::create_no_update_trigger_sql())
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
        for index_sql in Entities::create_trigram_indexes_sql() {
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
        for index in EventPayloadSchemas::create_indexes() {
            manager.create_index(index).await?;
        }
        for index in ProcessorManifests::create_indexes() {
            manager.create_index(index).await?;
        }
        for index_sql in ProcessorManifests::create_gin_indexes_sql() {
            manager
                .get_connection()
                .execute_unprepared(&index_sql)
                .await?;
        }
        for index in GitopsSchemaSources::create_indexes() {
            manager.create_index(index).await?;
        }

        Ok(())
    }

    /// Reverts the entire canonical schema.
    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let allow = env::var("SINEX_ALLOW_SCHEMA_DOWN")
            .unwrap_or_default()
            .to_lowercase();
        if !matches!(allow.as_str(), "1" | "true" | "yes" | "on") {
            return Err(DbErr::Custom(
                "Schema down migration is destructive; set SINEX_ALLOW_SCHEMA_DOWN=1 to proceed."
                    .to_string(),
            ));
        }

        // Drop everything in reverse dependency order.
        manager
            .get_connection()
            .execute_unprepared(
                r"
            DROP SCHEMA IF EXISTS core CASCADE;
            DROP SCHEMA IF EXISTS raw CASCADE;
            DROP SCHEMA IF EXISTS audit CASCADE;
            DROP SCHEMA IF EXISTS sinex_schemas CASCADE;
            DROP SCHEMA IF EXISTS metrics CASCADE;
            DROP FUNCTION IF EXISTS public.set_current_timestamp_updated_at();
            DROP FUNCTION IF EXISTS public.ulid_to_timestamptz(ULID);
            ",
            )
            .await?;
        Ok(())
    }
}

async fn ensure_required_extensions(conn: &SchemaManagerConnection<'_>) -> Result<(), DbErr> {
    let mut missing: Vec<String> = Vec::new();
    for extension in REQUIRED_EXTENSIONS {
        let mut target = *extension;
        let check_sql = format!(
            "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_available_extensions WHERE name = '{ext}') AS available",
            ext = extension.replace('\'', "''"),
        );
        let available = conn
            .query_one(Statement::from_string(DatabaseBackend::Postgres, check_sql))
            .await?
            .and_then(|row| row.try_get_by_index::<bool>(0).ok())
            .unwrap_or(false);

        let mut resolved_available = available;

        if !resolved_available && *extension == "ulid" {
            let fallback = "pgx_ulid";
            let fallback_check = format!(
                "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_available_extensions WHERE name = '{fallback}') AS available"
            );
            resolved_available = conn
                .query_one(Statement::from_string(
                    DatabaseBackend::Postgres,
                    fallback_check,
                ))
                .await?
                .and_then(|row| row.try_get_by_index::<bool>(0).ok())
                .unwrap_or(false);
            if resolved_available {
                target = fallback;
            }
        }

        if !resolved_available {
            let label = if *extension == "ulid" {
                "ulid (or pgx_ulid)".to_string()
            } else {
                String::from(*extension)
            };
            missing.push(label);
            continue;
        }

        let statement = format!(r#"CREATE EXTENSION IF NOT EXISTS "{target}";"#);
        conn.execute_unprepared(&statement).await?;
    }

    if !missing.is_empty() {
        return Err(DbErr::Custom(format!(
            "Required PostgreSQL extensions missing: {}",
            missing.join(", ")
        )));
    }

    Ok(())
}
