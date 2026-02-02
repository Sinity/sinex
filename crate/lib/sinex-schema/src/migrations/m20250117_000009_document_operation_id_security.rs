//! Document operation ID security model for archive trigger
//!
//! **Issue 63 (MEDIUM)**: Operation ID Can Be Forged
//!
//! The archive trigger (fn_archive_before_delete) checks for sinex.operation_id
//! to prevent accidental deletions, but any code with database access can set
//! this session variable and delete events. This is a known limitation of the
//! current design.
//!
//! ## Security Model
//!
//! The sinex.operation_id check is a **safety gate**, not a security boundary:
//! - Prevents accidental deletions from ad-hoc queries
//! - Requires explicit opt-in for replay operations
//! - Does NOT prevent malicious or compromised code from deleting events
//!
//! ## Future Improvements
//!
//! For cryptographic integrity, consider:
//! 1. Application-level signatures on operation metadata
//! 2. Audit log verification via external system
//! 3. Row-level security policies based on pg_authid
//! 4. Separate database role for replay operations
//!
//! This migration adds inline comments to the trigger function to document
//! the security model and prevent future confusion.

use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub(crate) struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                CREATE OR REPLACE FUNCTION core.fn_archive_before_delete()
                RETURNS trigger LANGUAGE plpgsql AS $$
                DECLARE
                  op_id TEXT := current_setting('sinex.operation_id', true);
                  sup_id ulid := NULLIF(current_setting('sinex.superseded_by_id', true), '');
                  who TEXT := current_setting('sinex.archived_by', true);
                  why TEXT := current_setting('sinex.archive_reason', true);
                BEGIN
                  -- SECURITY NOTE: This is a safety gate, not a security boundary.
                  -- Any database session can set sinex.operation_id via SET LOCAL.
                  -- The check prevents accidental deletions but does NOT prevent
                  -- malicious or compromised code from deleting events.
                  --
                  -- For stronger guarantees, implement:
                  -- 1. Application-level cryptographic signatures on operations
                  -- 2. Row-level security policies restricting DELETE to specific roles
                  -- 3. External audit log verification
                  --
                  -- TODO(security): Add cryptographic signature verification or RLS policy
                  IF op_id IS NULL OR op_id = '' THEN
                    RAISE EXCEPTION 'DELETE on core.events requires sinex.operation_id to be set in this session';
                  END IF;

                  -- Atomically copy the deleted row to the archive with additional context.
                  INSERT INTO audit.archived_events SELECT OLD.*, now(), who, why, sup_id;
                  RETURN OLD;
                END $$;

                -- No need to recreate the trigger; the function replacement is sufficient
                "#,
            )
            .await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Restore original function without security documentation
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                CREATE OR REPLACE FUNCTION core.fn_archive_before_delete()
                RETURNS trigger LANGUAGE plpgsql AS $$
                DECLARE
                  op_id TEXT := current_setting('sinex.operation_id', true);
                  sup_id ulid := NULLIF(current_setting('sinex.superseded_by_id', true), '');
                  who TEXT := current_setting('sinex.archived_by', true);
                  why TEXT := current_setting('sinex.archive_reason', true);
                BEGIN
                  -- This check is a critical safety gate. Normal application code cannot delete events.
                  -- Only audited operations (like replays) that set the session variable are allowed to.
                  IF op_id IS NULL OR op_id = '' THEN
                    RAISE EXCEPTION 'DELETE on core.events requires sinex.operation_id to be set in this session';
                  END IF;

                  -- Atomically copy the deleted row to the archive with additional context.
                  INSERT INTO audit.archived_events SELECT OLD.*, now(), who, why, sup_id;
                  RETURN OLD;
                END $$;
                "#,
            )
            .await?;
        Ok(())
    }
}
