//! The Canonical Database Schema for the `core.events` and `audit.archived_events` tables.
//!
//! This module provides the definitive, single source of truth for the event log's
//! structure, using `sea-query` to programmatically define all tables, columns,
//! indexes, and constraints. It is the physical implementation of the system's
//! core architectural invariants related to events and their provenance.

use crate::schema::{EventPayloadSchemas, SourceMaterialRegistry, TableDef};
use crate::ulid::Ulid;
use chrono::{DateTime, Utc};
use sea_orm_migration::prelude::*;
use serde_json::Value as JsonValue;
use sqlx::FromRow;

// =============================================================================
// The `core.events` Table
// =============================================================================

/// **Table: `core.events`**
///
/// This is the single source of truth for all system knowledge. An immutable, append-only
/// log of both raw observations and synthesized conclusions, implemented as a
/// TimescaleDB hypertable for extreme performance and scalability.
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
    TsIngest,

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
    IngestorVersion,
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
/// This is used by `sqlx::query_as!` for deserializing database results. Its
/// structure is a 1-to-1 mapping of the physical table layout. The conversion
/// to the logical `sinex_db::models::Event` domain model happens in the repository.
///
/// ## Serialization Support
///
/// When the `serde` feature is enabled, this struct supports JSON serialization
/// and deserialization, making it suitable for API responses and data interchange.
#[derive(Debug, FromRow)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct EventRecord {
    pub id: Ulid,
    pub source: String,
    pub event_type: String,
    pub host: String,
    pub payload: JsonValue,
    pub ts_orig: DateTime<Utc>,
    pub ts_orig_subnano: Option<i16>,
    pub ts_ingest: DateTime<Utc>,

    // Provenance fields
    pub source_material_id: Option<Ulid>,
    pub anchor_byte: Option<i64>,
    pub offset_start: Option<i64>,
    pub offset_end: Option<i64>,
    pub offset_kind: Option<String>,
    pub source_event_ids: Option<Vec<Ulid>>,

    pub associated_blob_ids: Option<Vec<Ulid>>,

    // Metadata
    pub payload_schema_id: Option<Ulid>,
    pub ingestor_version: Option<String>,
}

impl Events {
    /// Generates the `CREATE TABLE` statement for `core.events`.
    pub fn create_table_statement() -> TableCreateStatement {
        Table::create()
            .table((Alias::new("core"), Events::Table))
            .if_not_exists()
            .col(ColumnDef::new(Events::Id).custom(Alias::new("ULID")).primary_key().extra("DEFAULT gen_ulid()"))
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
            .col(ColumnDef::new(Events::TsOrigSubnano).small_integer())
            .col(ColumnDef::new(Events::TsIngest).timestamp_with_time_zone().not_null().extra("GENERATED ALWAYS AS (id::timestamp) STORED"))
            .col(ColumnDef::new(Events::SourceMaterialId).custom(Alias::new("ULID")))
            .col(ColumnDef::new(Events::AnchorByte).big_integer())
            .col(ColumnDef::new(Events::OffsetStart).big_integer())
            .col(ColumnDef::new(Events::OffsetEnd).big_integer())
            .col(ColumnDef::new(Events::OffsetKind).text().check(Expr::cust("offset_kind IN ('byte', 'line', 'rowid', 'logical')")))
            .col(ColumnDef::new(Events::SourceEventIds).array(ColumnType::Custom(Alias::new("ULID").into_iden())))
            .col(ColumnDef::new(Events::AssociatedBlobIds).array(ColumnType::Custom(Alias::new("ULID").into_iden())))
            .col(ColumnDef::new(Events::PayloadSchemaId).custom(Alias::new("ULID")))
            .col(ColumnDef::new(Events::IngestorVersion).text())
            // The Provenance XOR Invariant: an event MUST have exactly one type of provenance.
            .check(
                Expr::cust("(source_material_id IS NOT NULL AND source_event_ids IS NULL) OR (source_material_id IS NULL AND source_event_ids IS NOT NULL)")
            )
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

    /// Generates the SQL statement to convert `core.events` into a TimescaleDB hypertable.
    pub fn create_hypertable_sql() -> &'static str {
        "SELECT create_hypertable('core.events', by_range('id', partition_func => 'public.ulid_to_timestamptz'::regproc), if_not_exists => TRUE);"
    }

    /// Generates all necessary indexes and unique constraints for `core.events`.
    pub fn create_indexes() -> Vec<IndexCreateStatement> {
        vec![
            // The Idempotency Invariant: a specific byte in a source material can only produce one event.
            // For hypertables, unique indexes must include the partition key (id)
            // Since id is unique already, adding it maintains the constraint
            Index::create()
                .unique()
                .name("ux_events_material_anchor_id")
                .table(Self::table_iden())
                .col(Events::SourceMaterialId)
                .col(Events::AnchorByte)
                .col(Events::Id)
                .cond_where(Expr::col(Events::SourceMaterialId).is_not_null())
                .to_owned(),
            // Performance Indexes for common query patterns.
            // Note: Cannot use unique indexes on hypertables without including the partition key (id)
            Index::create()
                .name("ix_events_ts_orig")
                .table(Self::table_iden())
                .col((Events::TsOrig, IndexOrder::Desc))
                .to_owned(),
            Index::create()
                .name("ix_events_source_type_ts")
                .table(Self::table_iden())
                .col(Events::Source)
                .col(Events::EventType)
                .col((Events::TsOrig, IndexOrder::Desc))
                .to_owned(),
            // Note: GIN indexes require raw SQL - see create_gin_indexes_sql()
        ]
    }

    /// Generates raw SQL for GIN indexes (PostgreSQL-specific feature)
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
    /// Generates the `CREATE TABLE` statement using PostgreSQL's `LIKE` to ensure
    /// an exact structural match with `core.events`, plus additional audit columns.
    pub fn create_table_sql() -> String {
        format!(
            r#"CREATE TABLE IF NOT EXISTS audit.archived_events (
                LIKE core.events INCLUDING ALL,
                {archived_at} TIMESTAMPTZ NOT NULL DEFAULT now(),
                {archived_by} TEXT,
                {archive_reason} TEXT,
                {superseded_by} ULID NULL
            );
            DO $$
            BEGIN
                BEGIN
                    ALTER TABLE audit.archived_events
                        ALTER COLUMN ts_ingest DROP EXPRESSION;
                EXCEPTION
                    WHEN others THEN
                        -- Expression already removed or column missing; ignore.
                        NULL;
                END;
            END $$;
            "#,
            archived_at = ArchivedEvents::ArchivedAt.to_string(),
            archived_by = ArchivedEvents::ArchivedBy.to_string(),
            archive_reason = ArchivedEvents::ArchiveReason.to_string(),
            superseded_by = ArchivedEvents::SupersededByEventId.to_string()
        )
    }

    /// Generates the trigger function that enforces the Archive-on-Delete invariant.
    pub fn create_archive_trigger_sql() -> &'static str {
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

        DROP TRIGGER IF EXISTS trg_events_archive_before_delete ON core.events;
        CREATE TRIGGER trg_events_archive_before_delete
        BEFORE DELETE ON core.events
        FOR EACH ROW EXECUTE FUNCTION core.fn_archive_before_delete();
        "#
    }
}
