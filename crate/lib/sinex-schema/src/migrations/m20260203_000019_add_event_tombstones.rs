//! Add event tombstones table and lifecycle management functions
//!
//! This migration implements the third tier of the "Principled Forgetting" data lifecycle:
//!
//! ```text
//! Live (core.events) ←→ Archive (audit.archived_events) → Tombstone (core.event_tombstones)
//!                           ↑                                          │
//!                           └──────────────────────────────────────────┘
//!                                      (one-way: data is gone)
//! ```
//!
//! ## Design Philosophy
//!
//! Sinex's manifesto promises "immutable event log" and "complete history", but the previous
//! 90-day TimescaleDB retention policy (m20250117_000008) silently hard-deleted data via
//! chunk drops, bypassing the archive trigger entirely. This was dishonest.
//!
//! The new model embraces "principled forgetting":
//! - **No silent deletion**: User explicitly controls lifecycle transitions
//! - **Three tiers with cascade boundaries**: Live → Archive → Tombstone
//! - **Tombstones preserve provenance structure**: Minimal skeleton (~100 bytes/event) that
//!   records that an event existed, enabling provenance chain analysis even after data is gone
//! - **Chain isolation**: Each tier contains COMPLETE provenance chains (no cross-tier references)
//!
//! ## Tombstone vs Archive
//!
//! | Aspect | Archive | Tombstone |
//! |--------|---------|-----------|
//! | Data | Full event preserved | Minimal skeleton only |
//! | Reversible | Yes (restore to live) | No (data is gone) |
//! | Storage | ~1KB/event | ~100 bytes/event |
//! | Purpose | Soft delete, audit trail | Provenance skeleton, permanent forget |
//!
//! ## Cascade Invariant
//!
//! When tombstoning archived events, the entire provenance chain must move together.
//! This reuses the same cascade analyzer pattern used for Live → Archive transitions.
//!
//! ## SQL Functions Added
//!
//! - `core.execute_cascade_tombstone(archived_ids, reason, operation_id)`: Move archived chain to tombstones
//! - `core.execute_cascade_restore(archived_ids, operation_id)`: Move archived chain back to live

use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub(crate) struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // 1. Create the tombstones table
        manager
            .get_connection()
            .execute_unprepared(CREATE_TOMBSTONES_TABLE)
            .await?;

        // 2. Create indexes for common query patterns
        manager
            .get_connection()
            .execute_unprepared(CREATE_TOMBSTONES_INDEXES)
            .await?;

        // 3. Create the cascade tombstone function
        manager
            .get_connection()
            .execute_unprepared(CREATE_CASCADE_TOMBSTONE_FUNCTION)
            .await?;

        // 4. Create the cascade restore function
        manager
            .get_connection()
            .execute_unprepared(CREATE_CASCADE_RESTORE_FUNCTION)
            .await?;

        // 5. Create helper functions for lifecycle status
        manager
            .get_connection()
            .execute_unprepared(CREATE_LIFECYCLE_STATUS_FUNCTION)
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared(
                r"
                DROP FUNCTION IF EXISTS core.lifecycle_tier_status();
                DROP FUNCTION IF EXISTS core.execute_cascade_restore(UUID[], TEXT);
                DROP FUNCTION IF EXISTS core.execute_cascade_tombstone(UUID[], TEXT, UUID);
                DROP TABLE IF EXISTS core.event_tombstones;
                ",
            )
            .await?;
        Ok(())
    }
}

/// Create the tombstones table with minimal skeleton structure.
///
/// Tombstones preserve:
/// - Event identity (id, source, event_type)
/// - Temporal context (ts_orig when event occurred, ts_purged when tombstoned)
/// - Audit trail (purge_reason, purge_operation_id)
///
/// Tombstones do NOT preserve:
/// - Payload (data is gone)
/// - Full provenance (source_material_id, source_event_ids gone)
/// - Host, ingestor_version, etc.
const CREATE_TOMBSTONES_TABLE: &str = r"
CREATE TABLE IF NOT EXISTS core.event_tombstones (
    -- Identity: which event was this?
    id UUID PRIMARY KEY,
    source TEXT NOT NULL,
    event_type TEXT NOT NULL,

    -- Temporal: when did the original event occur?
    ts_orig TIMESTAMPTZ NOT NULL,

    -- Audit: when and why was it tombstoned?
    ts_purged TIMESTAMPTZ NOT NULL DEFAULT now(),
    purge_reason TEXT,
    purge_operation_id UUID,

    -- Optional: track which archived event record this came from
    -- (for debugging/auditing the archive→tombstone transition)
    archived_at TIMESTAMPTZ
);

COMMENT ON TABLE core.event_tombstones IS
    'Minimal skeleton records for events that have been permanently purged. '
    'Preserves provenance chain structure (~100 bytes/event) while acknowledging data is gone. '
    'One-way: cannot be restored.';

COMMENT ON COLUMN core.event_tombstones.ts_orig IS
    'Original timestamp when the event occurred (from core.events.ts_orig)';

COMMENT ON COLUMN core.event_tombstones.ts_purged IS
    'Timestamp when this event was tombstoned (data permanently removed)';

COMMENT ON COLUMN core.event_tombstones.purge_operation_id IS
    'UUID of the operation that caused this tombstone (for audit correlation)';
";

/// Create indexes for common tombstone query patterns.
const CREATE_TOMBSTONES_INDEXES: &str = r"
-- Query tombstones by source (e.g., 'how many terminal events were tombstoned?')
CREATE INDEX IF NOT EXISTS ix_tombstones_source
    ON core.event_tombstones(source);

-- Query tombstones by original time range (e.g., 'what events from 2024 were tombstoned?')
CREATE INDEX IF NOT EXISTS ix_tombstones_ts_orig
    ON core.event_tombstones(ts_orig);

-- Query tombstones by purge time (e.g., 'what was tombstoned in the last cleanup?')
CREATE INDEX IF NOT EXISTS ix_tombstones_ts_purged
    ON core.event_tombstones(ts_purged);

-- Query tombstones by operation (e.g., 'what did operation X tombstone?')
CREATE INDEX IF NOT EXISTS ix_tombstones_purge_operation
    ON core.event_tombstones(purge_operation_id)
    WHERE purge_operation_id IS NOT NULL;
";

/// Function to execute cascade tombstone operation.
///
/// This function:
/// 1. Takes archived event IDs and tombstones them along with their entire cascade
/// 2. Preserves minimal skeleton in core.event_tombstones
/// 3. Deletes from audit.archived_events
///
/// IMPORTANT: Caller must have already run cascade analysis to identify the full set
/// of archived events to tombstone. This function does NOT perform cascade analysis -
/// it trusts the provided IDs represent a complete, valid cascade.
const CREATE_CASCADE_TOMBSTONE_FUNCTION: &str = r"
CREATE OR REPLACE FUNCTION core.execute_cascade_tombstone(
    p_archived_ids UUID[],
    p_reason TEXT,
    p_operation_id UUID
) RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
    v_count BIGINT;
BEGIN
    -- Validate inputs
    IF p_archived_ids IS NULL OR array_length(p_archived_ids, 1) IS NULL THEN
        RETURN 0;
    END IF;

    -- Insert tombstones from archived events
    -- We extract only the minimal skeleton: id, source, event_type, ts_orig
    INSERT INTO core.event_tombstones (
        id, source, event_type, ts_orig, ts_purged,
        purge_reason, purge_operation_id, archived_at
    )
    SELECT
        ae.id,
        ae.source,
        ae.event_type,
        ae.ts_orig,
        now(),
        p_reason,
        p_operation_id,
        ae.archived_at
    FROM audit.archived_events ae
    WHERE ae.id = ANY(p_archived_ids)
    ON CONFLICT (id) DO NOTHING;  -- Idempotent: already tombstoned is fine

    GET DIAGNOSTICS v_count = ROW_COUNT;

    -- Delete from archive (data is now gone)
    DELETE FROM audit.archived_events
    WHERE id = ANY(p_archived_ids);

    RETURN v_count;
END;
$$;

COMMENT ON FUNCTION core.execute_cascade_tombstone IS
    'Move archived events to tombstones (one-way operation). '
    'Caller must provide complete cascade set - this function does not analyze dependencies. '
    'Returns count of tombstones created.';
";

/// Function to execute cascade restore operation (Archive → Live).
///
/// This function:
/// 1. Takes archived event IDs and restores them to live (core.events)
/// 2. Deletes from audit.archived_events
///
/// IMPORTANT: Caller must have already run cascade analysis to identify the full set
/// of archived events to restore. This function does NOT perform cascade analysis.
const CREATE_CASCADE_RESTORE_FUNCTION: &str = r"
CREATE OR REPLACE FUNCTION core.execute_cascade_restore(
    p_archived_ids UUID[],
    p_operation_id TEXT
) RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
    v_count BIGINT;
BEGIN
    -- Validate inputs
    IF p_archived_ids IS NULL OR array_length(p_archived_ids, 1) IS NULL THEN
        RETURN 0;
    END IF;

    -- Set operation context for any triggers
    PERFORM pg_catalog.set_config('sinex.operation_id', p_operation_id, true);
    PERFORM pg_catalog.set_config('sinex.archive_reason', 'restored from archive', true);

    -- Insert back into live events.
    -- `ts_coided` is omitted intentionally because live `core.events.ts_coided`
    -- is generated from UUIDv7 `id` and cannot be assigned directly.
    INSERT INTO core.events (
        id, source, event_type, host, payload,
        ts_orig, ts_orig_subnano,
        source_material_id, anchor_byte, offset_start, offset_end, offset_kind,
        source_event_ids, associated_blob_ids,
        payload_schema_id, ingestor_version
    )
    SELECT
        ae.id, ae.source, ae.event_type, ae.host, ae.payload,
        ae.ts_orig, ae.ts_orig_subnano,
        ae.source_material_id, ae.anchor_byte, ae.offset_start, ae.offset_end, ae.offset_kind,
        ae.source_event_ids, ae.associated_blob_ids,
        ae.payload_schema_id, ae.ingestor_version
    FROM audit.archived_events ae
    WHERE ae.id = ANY(p_archived_ids)
    ON CONFLICT (id) DO NOTHING;  -- Idempotent: already live is fine

    GET DIAGNOSTICS v_count = ROW_COUNT;

    -- Delete from archive
    DELETE FROM audit.archived_events
    WHERE id = ANY(p_archived_ids);

    RETURN v_count;
END;
$$;

COMMENT ON FUNCTION core.execute_cascade_restore IS
    'Move archived events back to live (core.events). '
    'Caller must provide complete cascade set - this function does not analyze dependencies. '
    'Returns count of events restored.';
";

/// Function to get lifecycle tier status (for CLI status command).
const CREATE_LIFECYCLE_STATUS_FUNCTION: &str = r"
CREATE OR REPLACE FUNCTION core.lifecycle_tier_status()
RETURNS TABLE (
    tier TEXT,
    event_count BIGINT,
    oldest_ts TIMESTAMPTZ,
    newest_ts TIMESTAMPTZ,
    distinct_sources BIGINT
)
LANGUAGE sql
STABLE
AS $$
    -- Live tier
    SELECT
        'live'::TEXT as tier,
        COUNT(*) as event_count,
        MIN(ts_orig) as oldest_ts,
        MAX(ts_orig) as newest_ts,
        COUNT(DISTINCT source) as distinct_sources
    FROM core.events

    UNION ALL

    -- Archive tier
    SELECT
        'archive'::TEXT as tier,
        COUNT(*) as event_count,
        MIN(ts_orig) as oldest_ts,
        MAX(ts_orig) as newest_ts,
        COUNT(DISTINCT source) as distinct_sources
    FROM audit.archived_events

    UNION ALL

    -- Tombstone tier
    SELECT
        'tombstone'::TEXT as tier,
        COUNT(*) as event_count,
        MIN(ts_orig) as oldest_ts,
        MAX(ts_orig) as newest_ts,
        COUNT(DISTINCT source) as distinct_sources
    FROM core.event_tombstones;
$$;

COMMENT ON FUNCTION core.lifecycle_tier_status IS
    'Returns summary statistics for each data lifecycle tier (live, archive, tombstone).';
";
