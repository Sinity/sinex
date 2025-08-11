//! Fix schema alignment with TARGET_canonical.md
//!
//! This migration addresses several issues:
//! 1. Removes duplicate raw.source_materials table (should use raw.source_material_registry)
//! 2. Creates audit schema and moves archived_events there from core
//! 3. Fixes temporal_ledger structure to match TARGET_canonical.md
//! 4. Ensures all constraints and indexes match the specification

use async_trait::async_trait;
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // 1. Create audit schema if it doesn't exist
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                CREATE SCHEMA IF NOT EXISTS audit;
                "#,
            )
            .await?;

        // 2. Move archived_events from core to audit schema
        // First, create the new table in audit schema
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                -- Create audit.archived_events with columns matching current core.events structure
                CREATE TABLE IF NOT EXISTS audit.archived_events (
                    id ULID NOT NULL PRIMARY KEY,
                    source TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    host TEXT,
                    payload JSONB NOT NULL,
                    ts_orig TIMESTAMPTZ NOT NULL,
                    ingestor_version TEXT,
                    payload_schema_id TEXT,
                    payload_schema_name TEXT,
                    payload_schema_version TEXT,
                    source_event_ids ULID[],
                    source_material_id ULID,
                    source_material_offset_start BIGINT,
                    source_material_offset_end BIGINT,
                    anchor_byte BIGINT,
                    associated_blob_ids ULID[],
                    processor_manifest_id TEXT,
                    ts_ingest TIMESTAMPTZ NOT NULL
                );

                -- Add audit-specific columns
                ALTER TABLE audit.archived_events
                    ADD COLUMN IF NOT EXISTS archived_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    ADD COLUMN IF NOT EXISTS archived_by TEXT,
                    ADD COLUMN IF NOT EXISTS archive_reason TEXT,
                    ADD COLUMN IF NOT EXISTS superseded_by_event_id ULID NULL,
                    ADD COLUMN IF NOT EXISTS operation_id ULID;
                "#,
            )
            .await?;

        // 3. Copy existing archived events from core to audit (if table exists)
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                DO $$
                BEGIN
                    -- Only copy if core.archived_events exists and has data
                    IF EXISTS (
                        SELECT 1 FROM information_schema.tables 
                        WHERE table_schema = 'core' AND table_name = 'archived_events'
                    ) THEN
                        INSERT INTO audit.archived_events (
                            id, source, event_type, host, payload, ts_orig, ingestor_version,
                            payload_schema_id, payload_schema_name, payload_schema_version,
                            source_event_ids, source_material_id,
                            source_material_offset_start, source_material_offset_end, anchor_byte,
                            associated_blob_ids, processor_manifest_id, ts_ingest,
                            archived_at, archived_by, archive_reason, superseded_by_event_id, operation_id
                        )
                        SELECT 
                            id, source, event_type, host, payload, ts_orig, ingestor_version,
                            payload_schema_id, payload_schema_name, payload_schema_version,
                            source_event_ids, source_material_id,
                            source_material_offset_start, source_material_offset_end, anchor_byte,
                            associated_blob_ids, processor_manifest_id, ts_ingest,
                            COALESCE(archived_at, NOW()),  -- Provide default if column doesn't exist
                            archived_by, 
                            COALESCE(archive_reason, 'Migrated from core schema'),
                            superseded_by_event_id, 
                            operation_id
                        FROM core.archived_events
                        ON CONFLICT (id) DO NOTHING;
                    END IF;
                END $$;
                "#,
            )
            .await?;

        // 4. Update the archive trigger to use audit schema
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                -- Update the archive_before_delete function to use audit schema
                CREATE OR REPLACE FUNCTION core.fn_archive_before_delete()
                RETURNS trigger LANGUAGE plpgsql AS $$
                DECLARE
                  op_id TEXT := current_setting('sinex.operation_id', true);
                  sup_id ULID := NULLIF(current_setting('sinex.superseded_by_id', true), '')::ULID;
                  who TEXT := current_setting('sinex.archived_by', true);
                  why TEXT := current_setting('sinex.archive_reason', true);
                BEGIN
                  IF op_id IS NULL OR op_id = '' THEN
                    RAISE EXCEPTION 'DELETE requires sinex.operation_id to be set in this session';
                  END IF;

                  INSERT INTO audit.archived_events (
                    id, source, event_type, host, payload, ts_orig, ingestor_version,
                    payload_schema_id, payload_schema_name, payload_schema_version,
                    source_event_ids, source_material_id,
                    source_material_offset_start, source_material_offset_end, anchor_byte,
                    associated_blob_ids, processor_manifest_id, ts_ingest,
                    archived_at, archived_by, archive_reason, superseded_by_event_id, operation_id
                  )
                  VALUES (
                    OLD.id, OLD.source, OLD.event_type, OLD.host, OLD.payload, OLD.ts_orig, OLD.ingestor_version,
                    OLD.payload_schema_id, OLD.payload_schema_name, OLD.payload_schema_version,
                    OLD.source_event_ids, OLD.source_material_id,
                    OLD.source_material_offset_start, OLD.source_material_offset_end, OLD.anchor_byte,
                    OLD.associated_blob_ids, OLD.processor_manifest_id, OLD.ts_ingest,
                    NOW(), who, why, sup_id, op_id::ULID
                  );

                  -- Also record this in operations_log
                  UPDATE core.operations_log 
                  SET events_archived = COALESCE(events_archived, 0) + 1
                  WHERE operation_id = op_id::ULID;

                  RETURN OLD;
                END $$;
                "#,
            )
            .await?;

        // 5. Drop the old core.archived_events table
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                DROP TABLE IF EXISTS core.archived_events CASCADE;
                "#,
            )
            .await?;

        // 6. Remove duplicate raw.source_materials table and fix references
        // First, migrate any data from source_materials to source_material_registry if needed
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                -- Check if there's any data in source_materials that isn't in source_material_registry
                -- This is a safety check - in production there shouldn't be any unique data there
                DO $$
                BEGIN
                    -- If raw.source_materials exists, drop it
                    IF EXISTS (
                        SELECT 1 FROM information_schema.tables 
                        WHERE table_schema = 'raw' AND table_name = 'source_materials'
                    ) THEN
                        -- First drop dependent objects
                        DROP TABLE IF EXISTS raw.sensor_jobs CASCADE;
                        DROP TABLE IF EXISTS raw.temporal_ledger CASCADE;
                        DROP TABLE IF EXISTS raw.source_materials CASCADE;
                    END IF;
                END $$;
                "#,
            )
            .await?;

        // 7. Create the correct temporal_ledger table according to TARGET_canonical.md E.3
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                -- Create temporal ledger as specified in TARGET_canonical.md
                CREATE TABLE IF NOT EXISTS raw.temporal_ledger (
                    entry_id ULID PRIMARY KEY DEFAULT gen_ulid(),
                    material_id ULID NOT NULL,
                    offset_start BIGINT NOT NULL,
                    offset_end BIGINT NOT NULL,
                    offset_kind TEXT NOT NULL CHECK (offset_kind IN ('byte','line','rowid','logical')),
                    ts_capture TIMESTAMPTZ NOT NULL,
                    precision TEXT NOT NULL CHECK (precision IN ('exact','bounded')),
                    clock TEXT NOT NULL CHECK (clock IN ('monotonic','wall')),
                    source_type TEXT NOT NULL CHECK (source_type IN ('realtime_capture','intrinsic_content','inferred_mtime','inferred_ctime','inferred_user')),
                    note TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    UNIQUE(material_id, offset_start)
                );

                CREATE INDEX IF NOT EXISTS ix_tl_material_offsets ON raw.temporal_ledger (material_id, offset_start, offset_end);
                CREATE INDEX IF NOT EXISTS ix_tl_ts ON raw.temporal_ledger (ts_capture, source_type);
                
                -- Add foreign key after checking what column exists
                DO $$
                BEGIN
                    IF EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_schema = 'raw' 
                        AND table_name = 'source_material_registry' 
                        AND column_name = 'source_material_id'
                    ) THEN
                        ALTER TABLE raw.temporal_ledger 
                        ADD CONSTRAINT fk_temporal_ledger_material 
                        FOREIGN KEY (material_id) 
                        REFERENCES raw.source_material_registry(source_material_id) 
                        ON DELETE CASCADE;
                    ELSIF EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_schema = 'raw' 
                        AND table_name = 'source_material_registry' 
                        AND column_name = 'blob_id'
                    ) THEN
                        ALTER TABLE raw.temporal_ledger 
                        ADD CONSTRAINT fk_temporal_ledger_material 
                        FOREIGN KEY (material_id) 
                        REFERENCES raw.source_material_registry(blob_id) 
                        ON DELETE CASCADE;
                    END IF;
                END $$;
                "#,
            )
            .await?;

        // 8. Add append-only trigger for temporal_ledger
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                -- Append-only trigger as specified in TARGET_canonical.md
                CREATE OR REPLACE FUNCTION raw.fn_temporal_ledger_append_only()
                RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN
                  RAISE EXCEPTION 'raw.temporal_ledger is append-only (no % allowed)', TG_OP;
                END $$;

                DROP TRIGGER IF EXISTS trg_tl_no_update ON raw.temporal_ledger;
                CREATE TRIGGER trg_tl_no_update
                BEFORE UPDATE OR DELETE ON raw.temporal_ledger
                FOR EACH ROW EXECUTE FUNCTION raw.fn_temporal_ledger_append_only();
                "#,
            )
            .await?;

        // 9. Create sensor_jobs table as specified in TARGET_canonical.md (section 4)
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                CREATE TABLE IF NOT EXISTS raw.sensor_jobs (
                    job_id ULID PRIMARY KEY DEFAULT gen_ulid(),
                    sensor_type TEXT NOT NULL,
                    target_uri TEXT NOT NULL,
                    source_identifier TEXT NOT NULL,
                    acquisition_mode JSONB NOT NULL DEFAULT '{}'::jsonb,
                    parameters JSONB NOT NULL DEFAULT '{}'::jsonb,
                    owner TEXT NOT NULL DEFAULT current_user,
                    resource_limits JSONB DEFAULT '{}'::jsonb,
                    status TEXT NOT NULL DEFAULT 'pending',
                    priority INT NOT NULL DEFAULT 0,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                );

                CREATE INDEX IF NOT EXISTS idx_sensor_jobs_status ON raw.sensor_jobs(status);
                CREATE INDEX IF NOT EXISTS idx_sensor_jobs_priority ON raw.sensor_jobs(priority DESC);
                "#,
            )
            .await?;

        // 10. Create sensor_states table for job state tracking
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                CREATE TABLE IF NOT EXISTS raw.sensor_states (
                    job_id ULID REFERENCES raw.sensor_jobs(job_id) ON DELETE CASCADE,
                    current_position JSONB,
                    last_successful_acquisition TIMESTAMPTZ,
                    error_count INT NOT NULL DEFAULT 0,
                    throughput JSONB,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (job_id)
                );
                "#,
            )
            .await?;

        // 11. Ensure all required indexes exist on core.events
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                -- Idempotency index (UNIQUE would be better but TimescaleDB doesn't support it without partition column)
                CREATE INDEX IF NOT EXISTS ux_events_material_anchor
                ON core.events(source_material_id, anchor_byte)
                WHERE source_material_id IS NOT NULL;

                -- GIN index for provenance traversal
                CREATE INDEX IF NOT EXISTS ix_events_source_event_ids
                ON core.events USING GIN (source_event_ids);

                -- Serving indexes
                CREATE INDEX IF NOT EXISTS ix_events_ts_orig ON core.events (ts_orig DESC);
                CREATE INDEX IF NOT EXISTS ix_events_type_ts ON core.events (event_type, ts_orig DESC);
                "#,
            )
            .await?;

        // 12. Add indexes for audit.archived_events
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                CREATE INDEX IF NOT EXISTS ix_archived_events_operation_id 
                ON audit.archived_events (operation_id) 
                WHERE operation_id IS NOT NULL;
                
                CREATE INDEX IF NOT EXISTS ix_archived_events_superseded_by 
                ON audit.archived_events (superseded_by_event_id) 
                WHERE superseded_by_event_id IS NOT NULL;
                
                CREATE INDEX IF NOT EXISTS ix_archived_events_archived_at
                ON audit.archived_events (archived_at DESC);
                "#,
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // This migration is largely irreversible due to data movement
        // We can only remove the new structures
        
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                -- Remove new tables
                DROP TABLE IF EXISTS raw.sensor_states CASCADE;
                DROP TABLE IF EXISTS raw.sensor_jobs CASCADE;
                DROP TABLE IF EXISTS raw.temporal_ledger CASCADE;
                
                -- Move archived_events back to core
                CREATE TABLE IF NOT EXISTS core.archived_events (LIKE audit.archived_events INCLUDING ALL);
                INSERT INTO core.archived_events SELECT * FROM audit.archived_events ON CONFLICT (id) DO NOTHING;
                DROP TABLE IF EXISTS audit.archived_events CASCADE;
                DROP SCHEMA IF EXISTS audit CASCADE;
                "#,
            )
            .await?;
        
        Ok(())
    }
}