//! Add the archive_before_delete trigger for application-immutable semantics
//!
//! This implements the critical trigger from TARGET_final.md E.2 that ensures
//! all DELETEs require an operation_id and archive the row before deletion.

use async_trait::async_trait;
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Create the archive_before_delete function exactly as specified in TARGET_final.md E.2
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                -- Require operation_id for any delete; move OLD row into archive with context
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
                    id, event_type, source, ts_orig, ts_ingest, host, payload,
                    source_material_id, offset_start, offset_end, anchor_byte,
                    source_event_ids, payload_schema_id,
                    archived_at, archived_by, archive_reason, superseded_by_event_id
                  )
                  VALUES (
                    OLD.id, OLD.event_type, OLD.source, OLD.ts_orig, OLD.ts_ingest, OLD.host, OLD.payload,
                    OLD.source_material_id, OLD.source_material_offset_start, OLD.source_material_offset_end, OLD.anchor_byte,
                    OLD.source_event_ids, OLD.payload_schema_id,
                    NOW(), who, why, sup_id
                  );

                  -- Also record this in operations_log
                  -- Optional: record archive activity in operations_log if schema supports it
                  -- UPDATE core.operations_log 
                  -- SET checkpoint = jsonb_set(COALESCE(checkpoint, '{}'), '{events_archived}', to_jsonb(COALESCE((checkpoint->>'events_archived')::int,0)+1), true)
                  -- WHERE id = op_id::uuid;

                  RETURN OLD;
                END $$;

                DROP TRIGGER IF EXISTS trg_events_archive_before_delete ON core.events;
                CREATE TRIGGER trg_events_archive_before_delete
                BEFORE DELETE ON core.events
                FOR EACH ROW EXECUTE FUNCTION core.fn_archive_before_delete();
                "#,
            )
            .await?;

        // Also add helper functions for setting archive context
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                -- Helper function to set all archive-related session variables
                CREATE OR REPLACE FUNCTION set_archive_context(
                    op_id TEXT,
                    archived_by TEXT DEFAULT NULL,
                    archive_reason TEXT DEFAULT NULL,
                    superseded_by_id TEXT DEFAULT NULL
                )
                RETURNS VOID AS $$
                BEGIN
                    PERFORM set_config('sinex.operation_id', op_id, false);
                    IF archived_by IS NOT NULL THEN
                        PERFORM set_config('sinex.archived_by', archived_by, false);
                    END IF;
                    IF archive_reason IS NOT NULL THEN
                        PERFORM set_config('sinex.archive_reason', archive_reason, false);
                    END IF;
                    IF superseded_by_id IS NOT NULL THEN
                        PERFORM set_config('sinex.superseded_by_id', superseded_by_id, false);
                    END IF;
                END;
                $$ LANGUAGE plpgsql;

                -- Helper to clear archive context after operation
                CREATE OR REPLACE FUNCTION clear_archive_context()
                RETURNS VOID AS $$
                BEGIN
                    PERFORM set_config('sinex.operation_id', '', false);
                    PERFORM set_config('sinex.archived_by', '', false);
                    PERFORM set_config('sinex.archive_reason', '', false);
                    PERFORM set_config('sinex.superseded_by_id', '', false);
                END;
                $$ LANGUAGE plpgsql;
                "#,
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                DROP TRIGGER IF EXISTS trg_events_archive_before_delete ON core.events;
                DROP FUNCTION IF EXISTS core.fn_archive_before_delete();
                DROP FUNCTION IF EXISTS set_archive_context(TEXT, TEXT, TEXT, TEXT);
                DROP FUNCTION IF EXISTS clear_archive_context();
                "#,
            )
            .await?;

        Ok(())
    }
}
