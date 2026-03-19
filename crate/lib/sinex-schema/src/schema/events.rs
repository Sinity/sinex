//! The Canonical Database Schema for the `core.events` and `audit.archived_events` tables.
//!
//! This module provides the definitive, single source of truth for the event log's
//! structure, using `sea-query` to programmatically define all tables, columns,
//! indexes, and constraints. It is the physical implementation of the system's
//! core architectural invariants related to events and their provenance.

use crate::primitives::{Timestamp, Uuid};
use crate::schema::{EventPayloadSchemas, SourceMaterialRegistry, TableDef};
use sea_query::{
    Alias, ColumnDef, ColumnType, ConditionalStatement, Expr, ForeignKey, ForeignKeyAction, Iden,
    Index, IndexCreateStatement, IndexOrder, IntoIden, Table, TableCreateStatement,
};
use serde_json::Value as JsonValue;
use sqlx::FromRow;

// =============================================================================
// The `core.events` Table
// =============================================================================

/// **Table: `core.events`**
///
/// This is the single source of truth for all system knowledge. An immutable, append-only
/// log of both raw observations and synthesized conclusions, implemented as a
/// `TimescaleDB` hypertable for extreme performance and scalability.
///
/// ## Design Decision: Inline Synthetic Event Metadata
///
/// Synthesized events carry 6 nullable metadata columns directly on `core.events`:
///
/// - `temporal_policy` — which `SyntheticTemporalPolicy` governed `ts_orig`
/// - `semantics_version` — node logic version for deterministic replay
/// - `scope_key` — scope identifier for scope-reconciler replacement
/// - `equivalence_key` — output slot identifier for targeted replacement
/// - `created_by_operation_id` — FK to the replay/operation that spawned this event
/// - `node_model` — which derived node model produced this event
///
/// ### Rationale for inline columns (not a junction table):
/// 1. **Query efficiency**: Scope recomputation needs `WHERE scope_key = ? AND source = ?`
///    — inline columns enable efficient partial indexes without JOINs.
/// 2. **NULL compression**: Material events (99%+ of rows) leave all 6 columns NULL.
///    `PostgreSQL` stores NULL columns in a bitmap header with zero per-column overhead.
/// 3. **Operation tracking**: `created_by_operation_id` enables direct lookup of events
///    produced by a replay operation without timestamp-range inference.
/// 4. **Partial indexes**: Sparse indexes on `scope_key` and `created_by_operation_id`
///    (WHERE IS NOT NULL) cover only the synthesized rows, avoiding bloat.
#[derive(Iden, Copy, Clone)]
pub enum Events {
    Table,
    Id,
    Source,
    EventType,
    Host,
    Payload,
    TsOrig,
    TsOrigSubnano,
    TsCoided,
    TsPersisted,

    // External Provenance
    SourceMaterialId,
    AnchorByte,
    OffsetStart,
    OffsetEnd,
    OffsetKind,
    // Internal Provenance
    SourceEventIds,

    AssociatedBlobIds,

    // Metadata
    PayloadSchemaId,
    NodeRunId,

    // Synthetic event metadata (nullable — only set for derived/synthesized events)
    TemporalPolicy,
    SemanticsVersion,
    ScopeKey,
    EquivalenceKey,
    CreatedByOperationId,
    NodeModel,
}

impl TableDef for Events {
    fn table_name() -> &'static str {
        "events"
    }
    fn schema_name() -> &'static str {
        "core"
    }
    fn primary_key() -> &'static str {
        "id"
    }
}

/// The Rust struct representation of a row from `core.events`.
///
/// This is used by `sqlx::query_as!` for deserializing database results. Its
/// structure is a 1-to-1 mapping of the physical table layout. The conversion
/// to the logical `sinex_db::models::Event` domain model happens in the repository.
///
/// ## Serialization Support
///
/// When the `serde` feature is enabled, this struct supports JSON serialization
/// and deserialization, making it suitable for API responses and data interchange.
#[derive(Debug, FromRow, serde::Serialize, serde::Deserialize)]
pub struct EventRecord {
    pub id: Uuid,
    pub source: String,
    pub event_type: String,
    pub host: String,
    pub payload: JsonValue,
    pub ts_orig: Timestamp,
    pub ts_orig_subnano: Option<i32>,
    pub ts_coided: Timestamp,
    pub ts_persisted: Timestamp,

    // Provenance fields
    pub source_material_id: Option<Uuid>,
    pub anchor_byte: Option<i64>,
    pub offset_start: Option<i64>,
    pub offset_end: Option<i64>,
    pub offset_kind: Option<String>,
    pub source_event_ids: Option<Vec<Uuid>>,

    pub associated_blob_ids: Option<Vec<Uuid>>,

    // Metadata
    pub payload_schema_id: Option<Uuid>,
    pub node_run_id: Option<Uuid>,

    // Synthetic event metadata (nullable — only set for derived/synthesized events)
    pub temporal_policy: Option<String>,
    pub semantics_version: Option<String>,
    pub scope_key: Option<String>,
    pub equivalence_key: Option<String>,
    pub created_by_operation_id: Option<Uuid>,
    pub node_model: Option<String>,
}

impl Events {
    /// Generates the `CREATE TABLE` statement for `core.events`.
    #[must_use]
    pub fn create_table_statement() -> TableCreateStatement {
        Table::create()
            .table((Alias::new("core"), Events::Table))
            .if_not_exists()
            .col(ColumnDef::new(Events::Id).custom(Alias::new("UUID")).primary_key())
            .col(
                ColumnDef::new(Events::Source)
                    .text()
                    .not_null()
                    .check(Expr::cust("length(BTRIM(source, E' \\t\\n\\r\\v\\f')) > 0")),
            )
            .col(
                ColumnDef::new(Events::EventType)
                    .text()
                    .not_null()
                    .check(Expr::cust("length(BTRIM(event_type, E' \\t\\n\\r\\v\\f')) > 0")),
            )
            .col(ColumnDef::new(Events::Host).text().not_null())
            .col(ColumnDef::new(Events::Payload).json_binary().not_null())
            .col(ColumnDef::new(Events::TsOrig).timestamp_with_time_zone().not_null())
            .col(ColumnDef::new(Events::TsOrigSubnano).integer())
            .col(
                ColumnDef::new(Events::TsCoided)
                    .timestamp_with_time_zone()
                    .not_null()
                    .extra("GENERATED ALWAYS AS (uuid_extract_timestamp(id)) STORED"),
            )
            .col(
                ColumnDef::new(Events::TsPersisted)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(ColumnDef::new(Events::SourceMaterialId).custom(Alias::new("UUID")))
            .col(ColumnDef::new(Events::AnchorByte).big_integer())
            .col(ColumnDef::new(Events::OffsetStart).big_integer())
            .col(ColumnDef::new(Events::OffsetEnd).big_integer())
            .col(ColumnDef::new(Events::OffsetKind).text().check(Expr::cust("offset_kind IN ('byte', 'line', 'rowid', 'logical')")))
            .col(ColumnDef::new(Events::SourceEventIds).array(ColumnType::Custom(Alias::new("UUID").into_iden())))
            .col(ColumnDef::new(Events::AssociatedBlobIds).array(ColumnType::Custom(Alias::new("UUID").into_iden())))
            .col(ColumnDef::new(Events::PayloadSchemaId).custom(Alias::new("UUID")))
            .col(ColumnDef::new(Events::NodeRunId).custom(Alias::new("UUID")))
            // Synthetic event metadata (nullable — only populated for derived/synthesized events)
            .col(ColumnDef::new(Events::TemporalPolicy).text().check(
                Expr::cust("temporal_policy IS NULL OR temporal_policy IN ('inherit_parent', 'latest_input', 'window_boundary', 'declared_effective')")
            ))
            .col(ColumnDef::new(Events::SemanticsVersion).text())
            .col(ColumnDef::new(Events::ScopeKey).text())
            .col(ColumnDef::new(Events::EquivalenceKey).text())
            .col(ColumnDef::new(Events::CreatedByOperationId).custom(Alias::new("UUID")))
            .col(ColumnDef::new(Events::NodeModel).text().check(
                Expr::cust("node_model IS NULL OR node_model IN ('transducer', 'windowed', 'scope_reconciler')")
            ))
            // The Provenance XOR Invariant: an event MUST have exactly one type of provenance.
            .check(
                Expr::cust("(source_material_id IS NOT NULL AND source_event_ids IS NULL) OR (source_material_id IS NULL AND source_event_ids IS NOT NULL)")
            )
            .check(Expr::cust(
                "source_event_ids IS NULL OR cardinality(source_event_ids) > 0",
            ))
            .check(Expr::cust(
                "source_event_ids IS NULL OR array_position(source_event_ids, NULL) IS NULL",
            ))
            .check(Expr::cust(
                "source_material_id IS NOT NULL OR (anchor_byte IS NULL AND offset_start IS NULL AND offset_end IS NULL AND offset_kind IS NULL)",
            ))
            .check(Expr::cust(
                "source_material_id IS NULL OR anchor_byte IS NOT NULL",
            ))
            .check(Expr::cust("(offset_start IS NULL) = (offset_end IS NULL)"))
            .check(Expr::cust(
                "offset_kind IS NULL OR (offset_start IS NOT NULL AND offset_end IS NOT NULL)",
            ))
            .check(Expr::cust(
                "offset_start IS NULL OR offset_end IS NULL OR offset_end >= offset_start",
            ))
            .foreign_key(
                ForeignKey::create()
                    .from(Self::table_iden(), Events::SourceMaterialId)
                    .to(SourceMaterialRegistry::table_iden(), Alias::new("id"))
            )
            .foreign_key(
                ForeignKey::create()
                    .from(Self::table_iden(), Events::PayloadSchemaId)
                    .to(EventPayloadSchemas::table_iden(), Alias::new("id"))
                    .on_delete(ForeignKeyAction::SetNull)
            )
            .to_owned()
    }

    /// Generates the SQL statement to convert `core.events` into a `TimescaleDB` hypertable.
    ///
    /// ## `TimescaleDB` Configuration
    ///
    /// - **Time Dimension**: native `UUIDv7` `id` (`by_range('id')`)
    /// - **Chunk Interval**: 7 days
    /// - **Retention Policy**: 90 days
    ///
    /// These settings balance query performance, storage efficiency, and operational
    /// requirements.
    #[must_use]
    pub fn create_hypertable_sql() -> &'static str {
        "SELECT create_hypertable('core.events', by_range('id'), if_not_exists => TRUE);"
    }

    /// Generates all necessary indexes for `core.events`.
    ///
    /// ## Index Strategy
    ///
    /// - **Material lookups**: `ix_events_material_anchor` supports provenance scans
    /// - **Time-based queries**: `ix_events_ts_orig`, `ix_events_ts_coided`, and
    ///   `ix_events_ts_persisted` support filters aligned with semantic time,
    ///   UUID-derived ingest time, and persisted-at time
    /// - **Hot-path filters**: source/type composites on `ts_coided` and `ts_orig`
    ///   accelerate query/replay/archive selection paths
    /// - **Payload search**: GIN indexes (see `create_gin_indexes_sql()`) enable fast
    ///   JSON path queries, text search, and full-text search
    ///
    #[must_use]
    pub fn create_indexes() -> Vec<IndexCreateStatement> {
        vec![
            // Material provenance lookup accelerator.
            Index::create()
                .if_not_exists()
                .name("ix_events_material_anchor")
                .table(Self::table_iden())
                .col(Events::SourceMaterialId)
                .col(Events::AnchorByte)
                .cond_where(Expr::col(Events::SourceMaterialId).is_not_null())
                .to_owned(),
            // Performance Indexes for common query patterns.
            // Note: Cannot use unique indexes on hypertables without including the partition key (id)
            Index::create()
                .if_not_exists()
                .name("ix_events_ts_orig")
                .table(Self::table_iden())
                .col((Events::TsOrig, IndexOrder::Desc))
                .to_owned(),
            Index::create()
                .if_not_exists()
                .name("ix_events_ts_coided")
                .table(Self::table_iden())
                .col((Events::TsCoided, IndexOrder::Desc))
                .to_owned(),
            Index::create()
                .if_not_exists()
                .name("ix_events_source_ts_coided")
                .table(Self::table_iden())
                .col(Events::Source)
                .col((Events::TsCoided, IndexOrder::Desc))
                .to_owned(),
            Index::create()
                .if_not_exists()
                .name("ix_events_event_type_ts_coided")
                .table(Self::table_iden())
                .col(Events::EventType)
                .col((Events::TsCoided, IndexOrder::Desc))
                .to_owned(),
            Index::create()
                .if_not_exists()
                .name("ix_events_source_type_ts_coided")
                .table(Self::table_iden())
                .col(Events::Source)
                .col(Events::EventType)
                .col((Events::TsCoided, IndexOrder::Desc))
                .to_owned(),
            Index::create()
                .if_not_exists()
                .name("ix_events_ts_persisted")
                .table(Self::table_iden())
                .col((Events::TsPersisted, IndexOrder::Desc))
                .to_owned(),
            Index::create()
                .if_not_exists()
                .name("ix_events_source_ts_orig")
                .table(Self::table_iden())
                .col(Events::Source)
                .col((Events::TsOrig, IndexOrder::Desc))
                .to_owned(),
            // Scope recomputation: find all events for a given scope_key
            Index::create()
                .if_not_exists()
                .name("ix_events_scope_key")
                .table(Self::table_iden())
                .col(Events::Source)
                .col(Events::ScopeKey)
                .cond_where(Expr::col(Events::ScopeKey).is_not_null())
                .to_owned(),
            // Operation lineage: find events created by a specific operation
            Index::create()
                .if_not_exists()
                .name("ix_events_created_by_operation_id")
                .table(Self::table_iden())
                .col(Events::CreatedByOperationId)
                .cond_where(Expr::col(Events::CreatedByOperationId).is_not_null())
                .to_owned(),
            // Note: GIN indexes require raw SQL - see create_gin_indexes_sql()
        ]
    }

    /// Generates raw SQL for GIN indexes (PostgreSQL-specific feature)
    #[must_use]
    pub fn create_gin_indexes_sql() -> Vec<String> {
        vec![
            // GIN index for source_event_ids array
            format!(
                "CREATE INDEX IF NOT EXISTS ix_events_source_event_ids ON {}.{} USING GIN (source_event_ids) WHERE source_event_ids IS NOT NULL",
                Self::schema_name(),
                Self::table_name()
            ),
            // GIN index for JSONB payload with jsonb_path_ops for efficient path queries
            format!(
                "CREATE INDEX IF NOT EXISTS ix_events_payload_gin ON {}.{} USING GIN (payload jsonb_path_ops)",
                Self::schema_name(),
                Self::table_name()
            ),
        ]
    }

    /// Generates the trigger enforcing append-only semantics for `core.events`.
    #[must_use]
    pub fn create_no_update_trigger_sql() -> &'static str {
        r"
        CREATE OR REPLACE FUNCTION core.fn_events_no_update()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          RAISE EXCEPTION 'UPDATE on core.events is forbidden';
        END $$;

        DROP TRIGGER IF EXISTS trg_events_no_update ON core.events;
        CREATE TRIGGER trg_events_no_update
        BEFORE UPDATE ON core.events
        FOR EACH ROW EXECUTE FUNCTION core.fn_events_no_update();
        "
    }
}

// =============================================================================
// The `audit.archived_events` Table
// =============================================================================

/// **Table: `audit.archived_events`**
///
/// An immutable archive for events that have been superseded by a replay operation.
/// This table ensures no information is ever truly lost, preserving a complete
//  history of the system's evolving understanding.
#[derive(Iden, Copy, Clone)]
pub enum ArchivedEvents {
    Table,
    ArchivedAt,
    ArchivedBy,
    ArchiveReason,
    SupersededByEventId,
}

impl TableDef for ArchivedEvents {
    fn table_name() -> &'static str {
        "archived_events"
    }
    fn schema_name() -> &'static str {
        "audit"
    }
    fn primary_key() -> &'static str {
        "id"
    }
}

impl ArchivedEvents {
    /// Generates the `CREATE TABLE` statement using `PostgreSQL`'s `LIKE` to ensure
    /// an exact structural match with `core.events`, plus additional audit columns.
    #[must_use]
    pub fn create_table_sql() -> String {
        format!(
            r"CREATE TABLE IF NOT EXISTS audit.archived_events (
                LIKE core.events INCLUDING ALL,
                {archived_at} TIMESTAMPTZ NOT NULL DEFAULT now(),
                {archived_by} TEXT,
                {archive_reason} TEXT,
                {superseded_by} UUID NULL
            );
            DO $$
            BEGIN
                BEGIN
                    ALTER TABLE audit.archived_events
                        ALTER COLUMN ts_coided DROP EXPRESSION;
                EXCEPTION
                    WHEN others THEN
                        -- Expression already removed or column missing; ignore.
                        NULL;
                END;
            END $$;
            ",
            archived_at = ArchivedEvents::ArchivedAt.to_string(),
            archived_by = ArchivedEvents::ArchivedBy.to_string(),
            archive_reason = ArchivedEvents::ArchiveReason.to_string(),
            superseded_by = ArchivedEvents::SupersededByEventId.to_string()
        )
    }

    /// Generates indexes for the archived events table.
    #[must_use]
    pub fn create_indexes_sql() -> Vec<String> {
        vec![
            // Index for querying archives by original timestamp
            format!(
                "CREATE INDEX IF NOT EXISTS ix_archived_events_ts_orig ON {}.{}(ts_orig DESC)",
                Self::schema_name(),
                Self::table_name()
            ),
            // Source + time index for archive selection and pagination.
            format!(
                "CREATE INDEX IF NOT EXISTS ix_archived_events_source_ts_orig ON {}.{}(source, ts_orig DESC)",
                Self::schema_name(),
                Self::table_name()
            ),
            // Index for querying archives by archive time
            format!(
                "CREATE INDEX IF NOT EXISTS ix_archived_events_archived_at ON {}.{}(archived_at DESC)",
                Self::schema_name(),
                Self::table_name()
            ),
            // Fast lookup by replay replacement target.
            format!(
                "CREATE INDEX IF NOT EXISTS ix_archived_events_superseded_by_event_id ON {}.{}(superseded_by_event_id) WHERE superseded_by_event_id IS NOT NULL",
                Self::schema_name(),
                Self::table_name()
            ),
            // Cascade traversal from archive parents to live/archive descendants.
            format!(
                "CREATE INDEX IF NOT EXISTS ix_archived_events_source_event_ids ON {}.{} USING GIN (source_event_ids) WHERE source_event_ids IS NOT NULL",
                Self::schema_name(),
                Self::table_name()
            ),
        ]
    }

    /// Generates the trigger function that enforces the Archive-on-Delete invariant.
    ///
    /// ## Security Model
    ///
    /// The `sinex.operation_id` check is a **safety gate**, not a security boundary:
    /// - Prevents accidental deletions from ad-hoc queries
    /// - Requires explicit opt-in for replay operations
    /// - Does NOT prevent malicious or compromised code from deleting events
    ///
    /// Any database session can set `sinex.operation_id` via `SET LOCAL`, so this
    /// protection relies on application discipline rather than cryptographic or
    /// role-based enforcement.
    ///
    /// Security hardening beyond this safety gate (for example stricter DB role controls)
    /// remains an explicit follow-up concern.
    #[must_use]
    pub fn create_archive_trigger_sql() -> &'static str {
        r"
        CREATE OR REPLACE FUNCTION core.fn_archive_before_delete()
        RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE
          op_id TEXT := current_setting('sinex.operation_id', true);
          sup_id uuid := NULLIF(current_setting('sinex.superseded_by_id', true), '');
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

        DROP TRIGGER IF EXISTS trg_events_archive_before_delete ON core.events;
        CREATE TRIGGER trg_events_archive_before_delete
        BEFORE DELETE ON core.events
        FOR EACH ROW EXECUTE FUNCTION core.fn_archive_before_delete();
        "
    }
}

// =============================================================================
// The `core.event_tombstones` Table
// =============================================================================

/// **Table: `core.event_tombstones`**
///
/// Minimal skeleton records for events that have been permanently purged from the archive.
/// Unlike archived events (which preserve full data and can be restored), tombstones represent
/// events whose data is permanently gone. They preserve only structural metadata to maintain
/// the provenance graph skeleton.
///
/// ## Design Philosophy: "Principled Forgetting"
///
/// Tombstones acknowledge that some data will eventually be forgotten, but they preserve:
/// - **Event identity**: Which event existed (id, source, `event_type`)
/// - **Temporal context**: When the original event occurred (`ts_orig`)
/// - **Audit trail**: When and why it was tombstoned (`ts_purged`, `purge_reason`)
///
/// This enables queries like "how many terminal events from 2024 were eventually purged?"
/// without keeping the actual payloads forever.
///
/// ## Storage Efficiency
///
/// | Tier | Typical Size | Purpose |
/// |------|--------------|---------|
/// | Live | ~1-10KB/event | Full data, real-time queries |
/// | Archive | ~1-10KB/event | Full data preserved, can restore |
/// | Tombstone | ~100 bytes/event | Skeleton only, permanent |
///
/// ## Lifecycle Flow
///
/// ```text
/// Live ←→ Archive → Tombstone
///          ↑           │
///          └───────────┘  (one-way: data is gone)
/// ```
#[derive(Iden, Copy, Clone)]
pub enum EventTombstones {
    Table,
    Id,
    Source,
    EventType,
    TsOrig,
    TsPurged,
    PurgeReason,
    PurgeOperationId,
    ArchivedAt,
}

impl TableDef for EventTombstones {
    fn table_name() -> &'static str {
        "event_tombstones"
    }
    fn schema_name() -> &'static str {
        "core"
    }
    fn primary_key() -> &'static str {
        "id"
    }
}

/// The Rust struct representation of a row from `core.event_tombstones`.
///
/// Used for deserializing tombstone records for analytics and audit queries.
#[derive(Debug, FromRow, serde::Serialize, serde::Deserialize)]
pub struct EventTombstoneRecord {
    pub id: Uuid,
    pub source: String,
    pub event_type: String,
    pub ts_orig: Timestamp,
    pub ts_purged: Timestamp,
    pub purge_reason: Option<String>,
    pub purge_operation_id: Option<Uuid>,
    pub archived_at: Option<Timestamp>,
}

impl EventTombstones {
    /// Generates the `CREATE TABLE` statement for `core.event_tombstones`.
    ///
    /// Defined via raw SQL in the declarative apply engine for simplicity.
    /// This method is provided for programmatic access to the schema definition.
    #[must_use]
    pub fn create_table_statement() -> TableCreateStatement {
        Table::create()
            .table((Alias::new("core"), EventTombstones::Table))
            .if_not_exists()
            .col(
                ColumnDef::new(EventTombstones::Id)
                    .custom(Alias::new("UUID"))
                    .primary_key(),
            )
            .col(ColumnDef::new(EventTombstones::Source).text().not_null())
            .col(ColumnDef::new(EventTombstones::EventType).text().not_null())
            .col(
                ColumnDef::new(EventTombstones::TsOrig)
                    .timestamp_with_time_zone()
                    .not_null(),
            )
            .col(
                ColumnDef::new(EventTombstones::TsPurged)
                    .timestamp_with_time_zone()
                    .not_null()
                    .extra("DEFAULT now()"),
            )
            .col(ColumnDef::new(EventTombstones::PurgeReason).text())
            .col(ColumnDef::new(EventTombstones::PurgeOperationId).custom(Alias::new("UUID")))
            .col(ColumnDef::new(EventTombstones::ArchivedAt).timestamp_with_time_zone())
            .to_owned()
    }

    /// Generates indexes for the tombstones table.
    #[must_use]
    pub fn create_indexes_sql() -> Vec<String> {
        vec![
            format!(
                "CREATE INDEX IF NOT EXISTS ix_tombstones_source ON {}.{}(source)",
                Self::schema_name(),
                Self::table_name()
            ),
            format!(
                "CREATE INDEX IF NOT EXISTS ix_tombstones_ts_orig ON {}.{}(ts_orig)",
                Self::schema_name(),
                Self::table_name()
            ),
            format!(
                "CREATE INDEX IF NOT EXISTS ix_tombstones_ts_purged ON {}.{}(ts_purged)",
                Self::schema_name(),
                Self::table_name()
            ),
            format!(
                "CREATE INDEX IF NOT EXISTS ix_tombstones_purge_operation ON {}.{}(purge_operation_id) WHERE purge_operation_id IS NOT NULL",
                Self::schema_name(),
                Self::table_name()
            ),
        ]
    }
}

// =============================================================================
// The `audit.event_replacements` Table
// =============================================================================

/// **Table: `audit.event_replacements`**
///
/// A many-to-many relation tracking which events were replaced by which new events
/// during replay or scope recomputation operations.
///
/// Unlike `audit.archived_events.superseded_by_event_id` (which is a 1:1 optimization),
/// this table is the primary design center for replacement lineage. It supports:
///
/// - **1:1 replacement** (`superseded`): one old event directly replaced by one new
/// - **many:1 collapse** (`collapsed`): multiple old events collapsed into one new
/// - **1:many split** (`split`): one old event replaced by multiple new events
/// - **operation-level re-derivation** (`recomputed`): no confident equivalence match;
///   replacement is tracked at the operation level only
#[derive(Iden, Copy, Clone)]
pub enum EventReplacements {
    Table,
    Id,
    OldEventId,
    NewEventId,
    OperationId,
    RelationKind,
    ScopeKey,
    EquivalenceKey,
    ReplacedAt,
}

impl TableDef for EventReplacements {
    fn table_name() -> &'static str {
        "event_replacements"
    }
    fn schema_name() -> &'static str {
        "audit"
    }
    fn primary_key() -> &'static str {
        "id"
    }
}

/// The Rust struct representation of a row from `audit.event_replacements`.
#[derive(Debug, FromRow, serde::Serialize, serde::Deserialize)]
pub struct EventReplacementRecord {
    pub id: Uuid,
    pub old_event_id: Uuid,
    pub new_event_id: Uuid,
    pub operation_id: Uuid,
    pub relation_kind: String,
    pub scope_key: Option<String>,
    pub equivalence_key: Option<String>,
    pub replaced_at: Timestamp,
}

impl EventReplacements {
    /// Generates the `CREATE TABLE` statement for `audit.event_replacements`.
    #[must_use]
    pub fn create_table_statement() -> TableCreateStatement {
        Table::create()
            .table((Alias::new("audit"), EventReplacements::Table))
            .if_not_exists()
            .col(
                ColumnDef::new(EventReplacements::Id)
                    .custom(Alias::new("UUID"))
                    .primary_key()
                    .extra("DEFAULT gen_random_uuid()"),
            )
            .col(
                ColumnDef::new(EventReplacements::OldEventId)
                    .custom(Alias::new("UUID"))
                    .not_null(),
            )
            .col(
                ColumnDef::new(EventReplacements::NewEventId)
                    .custom(Alias::new("UUID"))
                    .not_null(),
            )
            .col(
                ColumnDef::new(EventReplacements::OperationId)
                    .custom(Alias::new("UUID"))
                    .not_null(),
            )
            .col(
                ColumnDef::new(EventReplacements::RelationKind)
                    .text()
                    .not_null()
                    .check(Expr::cust(
                        "relation_kind IN ('superseded', 'collapsed', 'split', 'recomputed')",
                    )),
            )
            .col(ColumnDef::new(EventReplacements::ScopeKey).text())
            .col(ColumnDef::new(EventReplacements::EquivalenceKey).text())
            .col(
                ColumnDef::new(EventReplacements::ReplacedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .to_owned()
    }

    /// Generates indexes for the event replacements table.
    #[must_use]
    pub fn create_indexes() -> Vec<IndexCreateStatement> {
        vec![
            // Look up all replacements for an archived event
            Index::create()
                .if_not_exists()
                .name("ix_event_replacements_old_event_id")
                .table(Self::table_iden())
                .col(EventReplacements::OldEventId)
                .to_owned(),
            // Look up what an event replaced
            Index::create()
                .if_not_exists()
                .name("ix_event_replacements_new_event_id")
                .table(Self::table_iden())
                .col(EventReplacements::NewEventId)
                .to_owned(),
            // Find all replacements for a given replay operation
            Index::create()
                .if_not_exists()
                .name("ix_event_replacements_operation_id")
                .table(Self::table_iden())
                .col(EventReplacements::OperationId)
                .to_owned(),
        ]
    }
}
