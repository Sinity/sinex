//! Migration to add audit trail support to core tables
//!
//! Adds soft delete capability and audit trail tracking to prevent data loss
//! and maintain immutable event sourcing principles.

use async_trait::async_trait;
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Add audit trail columns to core.events
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                -- Add audit trail columns to core.events
                ALTER TABLE core.events 
                ADD COLUMN deleted_at TIMESTAMPTZ,
                ADD COLUMN deleted_by TEXT,
                ADD COLUMN deletion_reason TEXT;

                -- Create index on deleted_at for performance
                CREATE INDEX idx_events_deleted_at ON core.events (deleted_at);
                CREATE INDEX idx_events_active ON core.events (id) WHERE deleted_at IS NULL;
                "#,
            )
            .await?;

        // Add audit trail columns to core.event_annotations
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                -- Add audit trail columns to core.event_annotations
                ALTER TABLE core.event_annotations 
                ADD COLUMN deleted_at TIMESTAMPTZ,
                ADD COLUMN deleted_by TEXT,
                ADD COLUMN deletion_reason TEXT;

                -- Create index on deleted_at for performance
                CREATE INDEX idx_event_annotations_deleted_at ON core.event_annotations (deleted_at);
                CREATE INDEX idx_event_annotations_active ON core.event_annotations (id) WHERE deleted_at IS NULL;
                "#,
            )
            .await?;

        // Add audit trail columns to core.processor_checkpoints
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                -- Add audit trail columns to core.processor_checkpoints
                ALTER TABLE core.processor_checkpoints 
                ADD COLUMN deleted_at TIMESTAMPTZ,
                ADD COLUMN deleted_by TEXT,
                ADD COLUMN deletion_reason TEXT;

                -- Create index on deleted_at for performance
                CREATE INDEX idx_processor_checkpoints_deleted_at ON core.processor_checkpoints (deleted_at);
                CREATE INDEX idx_processor_checkpoints_active ON core.processor_checkpoints (id) WHERE deleted_at IS NULL;
                "#,
            )
            .await?;

        // Create audit log table for tracking all deletion attempts
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                CREATE TABLE audit.deletion_log (
                    id ULID PRIMARY KEY DEFAULT gen_ulid(),
                    table_name TEXT NOT NULL,
                    record_id ULID NOT NULL,
                    original_data JSONB NOT NULL,
                    deleted_by TEXT NOT NULL,
                    deletion_reason TEXT,
                    operation_id TEXT,
                    deleted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    soft_delete BOOLEAN NOT NULL DEFAULT true
                );

                -- Create indexes for audit.deletion_log
                CREATE INDEX idx_deletion_log_table_name ON audit.deletion_log (table_name);
                CREATE INDEX idx_deletion_log_deleted_at ON audit.deletion_log (deleted_at DESC);
                CREATE INDEX idx_deletion_log_deleted_by ON audit.deletion_log (deleted_by);
                CREATE INDEX idx_deletion_log_operation_id ON audit.deletion_log (operation_id) WHERE operation_id IS NOT NULL;
                "#,
            )
            .await?;

        // Create trigger function to log deletion attempts
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                CREATE OR REPLACE FUNCTION audit.log_deletion_attempt()
                RETURNS TRIGGER AS $$
                DECLARE
                    current_op_id TEXT;
                    current_user_id TEXT;
                BEGIN
                    -- Get current operation_id and user from session
                    current_op_id := current_setting('sinex.operation_id', true);
                    current_user_id := current_setting('sinex.user_id', true);
                    
                    -- Default user if not set
                    IF current_user_id IS NULL OR current_user_id = '' THEN
                        current_user_id := session_user;
                    END IF;
                    
                    -- Log the deletion attempt
                    INSERT INTO audit.deletion_log (
                        table_name,
                        record_id,
                        original_data,
                        deleted_by,
                        deletion_reason,
                        operation_id,
                        soft_delete
                    ) VALUES (
                        TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME,
                        OLD.id,
                        to_jsonb(OLD),
                        current_user_id,
                        current_setting('sinex.deletion_reason', true),
                        current_op_id,
                        TG_OP = 'UPDATE'
                    );
                    
                    RETURN COALESCE(NEW, OLD);
                END;
                $$ LANGUAGE plpgsql;
                "#,
            )
            .await?;

        // Create function to enforce audit trail for core.events
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                CREATE OR REPLACE FUNCTION audit.enforce_events_immutability()
                RETURNS TRIGGER AS $$
                DECLARE
                    bypass_mode TEXT;
                BEGIN
                    -- Check if bypass mode is enabled (for test cleanup and admin operations)
                    bypass_mode := current_setting('sinex.bypass_audit', true);
                    
                    IF bypass_mode IS NULL OR bypass_mode != 'true' THEN
                        -- Prevent hard deletes on core.events in production
                        IF TG_OP = 'DELETE' THEN
                            RAISE EXCEPTION 'Hard deletes on core.events are not allowed. Use soft delete or set sinex.bypass_audit=true for administrative operations.';
                        END IF;
                    END IF;
                    
                    RETURN COALESCE(NEW, OLD);
                END;
                $$ LANGUAGE plpgsql;
                "#,
            )
            .await?;

        // Create triggers for audit logging
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                -- Audit trigger for core.events (logs soft and hard deletes)
                CREATE TRIGGER audit_events_deletion
                AFTER UPDATE OF deleted_at OR DELETE ON core.events
                FOR EACH ROW EXECUTE FUNCTION audit.log_deletion_attempt();

                -- Audit trigger for core.event_annotations
                CREATE TRIGGER audit_event_annotations_deletion
                AFTER UPDATE OF deleted_at OR DELETE ON core.event_annotations
                FOR EACH ROW EXECUTE FUNCTION audit.log_deletion_attempt();

                -- Audit trigger for core.processor_checkpoints
                CREATE TRIGGER audit_processor_checkpoints_deletion
                AFTER UPDATE OF deleted_at OR DELETE ON core.processor_checkpoints
                FOR EACH ROW EXECUTE FUNCTION audit.log_deletion_attempt();

                -- Enforcement trigger for core.events (prevents hard deletes)
                CREATE TRIGGER enforce_events_immutability
                BEFORE DELETE ON core.events
                FOR EACH ROW EXECUTE FUNCTION audit.enforce_events_immutability();
                "#,
            )
            .await?;

        // Create helper functions for soft delete operations
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                -- Function to set deletion context
                CREATE OR REPLACE FUNCTION set_deletion_context(user_id TEXT, reason TEXT)
                RETURNS VOID AS $$
                BEGIN
                    PERFORM set_config('sinex.user_id', user_id, false);
                    PERFORM set_config('sinex.deletion_reason', reason, false);
                END;
                $$ LANGUAGE plpgsql;

                -- Function to enable bypass mode for administrative operations
                CREATE OR REPLACE FUNCTION enable_audit_bypass()
                RETURNS VOID AS $$
                BEGIN
                    PERFORM set_config('sinex.bypass_audit', 'true', false);
                END;
                $$ LANGUAGE plpgsql;

                -- Function to disable bypass mode
                CREATE OR REPLACE FUNCTION disable_audit_bypass()
                RETURNS VOID AS $$
                BEGIN
                    PERFORM set_config('sinex.bypass_audit', 'false', false);
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
                -- Drop triggers
                DROP TRIGGER IF EXISTS audit_events_deletion ON core.events;
                DROP TRIGGER IF EXISTS audit_event_annotations_deletion ON core.event_annotations;
                DROP TRIGGER IF EXISTS audit_processor_checkpoints_deletion ON core.processor_checkpoints;
                DROP TRIGGER IF EXISTS enforce_events_immutability ON core.events;

                -- Drop functions
                DROP FUNCTION IF EXISTS audit.log_deletion_attempt();
                DROP FUNCTION IF EXISTS audit.enforce_events_immutability();
                DROP FUNCTION IF EXISTS set_deletion_context(TEXT, TEXT);
                DROP FUNCTION IF EXISTS enable_audit_bypass();
                DROP FUNCTION IF EXISTS disable_audit_bypass();

                -- Drop audit table
                DROP TABLE IF EXISTS audit.deletion_log;

                -- Remove audit columns from core.events
                ALTER TABLE core.events 
                DROP COLUMN IF EXISTS deleted_at,
                DROP COLUMN IF EXISTS deleted_by,
                DROP COLUMN IF EXISTS deletion_reason;

                -- Remove audit columns from core.event_annotations
                ALTER TABLE core.event_annotations 
                DROP COLUMN IF EXISTS deleted_at,
                DROP COLUMN IF EXISTS deleted_by,
                DROP COLUMN IF EXISTS deletion_reason;

                -- Remove audit columns from core.processor_checkpoints
                ALTER TABLE core.processor_checkpoints 
                DROP COLUMN IF EXISTS deleted_at,
                DROP COLUMN IF EXISTS deleted_by,
                DROP COLUMN IF EXISTS deletion_reason;

                -- Drop indexes (they will be automatically dropped with column removal)
                "#,
            )
            .await?;

        Ok(())
    }
}
