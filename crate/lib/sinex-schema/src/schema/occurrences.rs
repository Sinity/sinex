//! The Canonical Database Schema for the Occurrence and Material Interpretation tables.
//!
//! This module defines `raw.occurrences` and `raw.material_interpretations`,
//! which together form the stable replay identity surface. Occurrence records
//! identify stable logical slots in source material; interpretation records
//! track which parser version produced which event for each occurrence.
//!
//! These tables are side tables that complement `core.events`, not replacements.
//! The existing `source_material_id`, `anchor_byte`, and `offset_*` columns
//! on `core.events` remain unchanged.

use crate::primitives::{Timestamp, Uuid};
use crate::schema::{Events, SourceMaterialRegistry, TableDef};
use sea_query::{
    Alias, ColumnDef, ConditionalStatement, Expr, ForeignKey, ForeignKeyAction, Iden, Index,
    IndexCreateStatement, Table, TableCreateStatement,
};
use serde_json::Value as JsonValue;
use sqlx::FromRow;

// =============================================================================
// The `raw.occurrences` Table
// =============================================================================

/// **Table: `raw.occurrences`**
///
/// Stable, source-unit-scoped logical occurrence slots. Each row identifies
/// a distinct real-world datum within a source material, anchored by a
/// material position (byte offset, SQLite row, git OID, etc.) or a
/// domain-specific natural key.
///
/// ## Occurrence Identity
///
/// An occurrence is uniquely identified by `(source_unit_id, source_material_id,
/// anchor_kind, anchor_data)`. The `id` column uses UUIDv5 derived from these
/// fields for deterministic idempotent registration via `ON CONFLICT DO NOTHING`.
///
/// ## Relationship to core.events
///
/// Occurrence records are NOT event provenance. `core.events` still uses its
/// XOR provenance model (`source_material_id` XOR `source_event_ids`).
/// Occurrences are a stable replay surface — when a parser re-interprets
/// an occurrence with a new version, a new event is created (new event ID),
/// but the occurrence record stays the same.
#[derive(Iden, Copy, Clone)]
pub enum Occurrences {
    Table,
    Id,
    SourceUnitId,
    SourceMaterialId,
    AnchorKind,
    AnchorData,
    NaturalKey,
    CreatedAt,
}

impl TableDef for Occurrences {
    fn table_name() -> &'static str {
        "occurrences"
    }
    fn schema_name() -> &'static str {
        "raw"
    }
    fn primary_key() -> &'static str {
        "id"
    }
}

/// The Rust struct representation of a row from `raw.occurrences`.
#[derive(Debug, FromRow, serde::Serialize, serde::Deserialize)]
pub struct OccurrenceRecord {
    pub id: Uuid,
    pub source_unit_id: String,
    pub source_material_id: Uuid,
    pub anchor_kind: String,
    pub anchor_data: JsonValue,
    pub natural_key: Option<String>,
    pub created_at: Timestamp,
}

impl Occurrences {
    /// Generates the `CREATE TABLE` statement for `raw.occurrences`.
    #[must_use]
    pub fn create_table_statement() -> TableCreateStatement {
        Table::create()
            .table(Self::table_iden())
            .if_not_exists()
            .col(
                ColumnDef::new(Occurrences::Id)
                    .custom(Alias::new("UUID"))
                    .not_null()
                    .primary_key(),
            )
            .col(
                ColumnDef::new(Occurrences::SourceUnitId)
                    .text()
                    .not_null()
                    .check(Expr::cust(
                        "length(BTRIM(source_unit_id, E' \\t\\n\\r\\v\\f')) > 0",
                    )),
            )
            .col(
                ColumnDef::new(Occurrences::SourceMaterialId)
                    .custom(Alias::new("UUID"))
                    .not_null(),
            )
            .col(
                ColumnDef::new(Occurrences::AnchorKind)
                    .text()
                    .not_null()
                    .check(Expr::cust(
                        "anchor_kind IN ('byte_offset', 'sqlite_row', 'line_number', \
                         'sequence_number', 'natural_key', 'cursor_token', 'git_oid', \
                         'stream_frame')",
                    )),
            )
            .col(
                ColumnDef::new(Occurrences::AnchorData)
                    .json_binary()
                    .not_null(),
            )
            .col(ColumnDef::new(Occurrences::NaturalKey).text())
            .col(
                ColumnDef::new(Occurrences::CreatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .foreign_key(
                ForeignKey::create()
                    .from(Self::table_iden(), Occurrences::SourceMaterialId)
                    .to(
                        SourceMaterialRegistry::table_iden(),
                        Alias::new("id"),
                    )
                    .on_delete(ForeignKeyAction::Restrict),
            )
            .to_owned()
    }

    /// Generates a UNIQUE constraint for idempotent occurrence registration.
    ///
    /// This is a raw SQL statement because sea-query does not expose
    /// `CREATE UNIQUE INDEX` with `COALESCE` expressions cleanly.
    #[must_use]
    pub fn create_unique_index_sql() -> String {
        format!(
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_occurrences_identity \
             ON {}.{}(source_unit_id, source_material_id, anchor_kind, \
             COALESCE(natural_key, anchor_data::text))",
            Self::schema_name(),
            Self::table_name()
        )
    }

    /// Generates indexes for `raw.occurrences`.
    #[must_use]
    pub fn create_indexes() -> Vec<IndexCreateStatement> {
        vec![
            // Foreign key accelerator
            Index::create()
                .if_not_exists()
                .name("ix_occurrences_source_material_id")
                .table(Self::table_iden())
                .col(Occurrences::SourceMaterialId)
                .to_owned(),
            // Source-unit-scoped lookups
            Index::create()
                .if_not_exists()
                .name("ix_occurrences_source_unit_id")
                .table(Self::table_iden())
                .col(Occurrences::SourceUnitId)
                .to_owned(),
            // Natural key lookups (for dedup by key across materials)
            Index::create()
                .if_not_exists()
                .name("ix_occurrences_natural_key")
                .table(Self::table_iden())
                .col(Occurrences::NaturalKey)
                .cond_where(Expr::col(Occurrences::NaturalKey).is_not_null())
                .to_owned(),
        ]
    }
}

// =============================================================================
// The `raw.material_interpretations` Table
// =============================================================================

/// **Table: `raw.material_interpretations`**
///
/// Records that a specific parser version interpreted a specific occurrence
/// and produced a specific event. This is the replay identity record — when
/// a parser version changes, replay can find old interpretations, compare
/// outputs, and decide whether to replace.
///
/// ## Interpretation Lifecycle
///
/// 1. Parser version X interprets occurrence Y → produces event Z
/// 2. A row is inserted with `is_current = true`
/// 3. When parser version X+N re-interprets Y → old row gets `is_current = false`,
///    new row with new event ID gets `is_current = true`
///
/// ## Why no FK to core.events
///
/// `core.events` is a TimescaleDB hypertable, which cannot be the target of
/// foreign keys from regular tables. The `event_id` column stores the
/// interpretation result UUID without DB-enforced referential integrity.
/// Application-level consistency checks compensate.
#[derive(Iden, Copy, Clone)]
pub enum MaterialInterpretations {
    Table,
    Id,
    OccurrenceId,
    ParserId,
    ParserVersion,
    SourceUnitId,
    EventId,
    InterpretedAt,
    IsCurrent,
}

impl TableDef for MaterialInterpretations {
    fn table_name() -> &'static str {
        "material_interpretations"
    }
    fn schema_name() -> &'static str {
        "raw"
    }
    fn primary_key() -> &'static str {
        "id"
    }
}

/// The Rust struct representation of a row from `raw.material_interpretations`.
#[derive(Debug, FromRow, serde::Serialize, serde::Deserialize)]
pub struct MaterialInterpretationRecord {
    pub id: Uuid,
    pub occurrence_id: Uuid,
    pub parser_id: String,
    pub parser_version: String,
    pub source_unit_id: String,
    pub event_id: Uuid,
    pub interpreted_at: Timestamp,
    pub is_current: bool,
}

impl MaterialInterpretations {
    /// Generates the `CREATE TABLE` statement for `raw.material_interpretations`.
    #[must_use]
    pub fn create_table_statement() -> TableCreateStatement {
        Table::create()
            .table(Self::table_iden())
            .if_not_exists()
            .col(
                ColumnDef::new(MaterialInterpretations::Id)
                    .custom(Alias::new("UUID"))
                    .not_null()
                    .primary_key(),
            )
            .col(
                ColumnDef::new(MaterialInterpretations::OccurrenceId)
                    .custom(Alias::new("UUID"))
                    .not_null(),
            )
            .col(
                ColumnDef::new(MaterialInterpretations::ParserId)
                    .text()
                    .not_null()
                    .check(Expr::cust(
                        "length(BTRIM(parser_id, E' \\t\\n\\r\\v\\f')) > 0",
                    )),
            )
            .col(
                ColumnDef::new(MaterialInterpretations::ParserVersion)
                    .text()
                    .not_null()
                    .check(Expr::cust(
                        "length(BTRIM(parser_version, E' \\t\\n\\r\\v\\f')) > 0",
                    )),
            )
            .col(
                ColumnDef::new(MaterialInterpretations::SourceUnitId)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(MaterialInterpretations::EventId)
                    .custom(Alias::new("UUID"))
                    .not_null(),
            )
            .col(
                ColumnDef::new(MaterialInterpretations::InterpretedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(
                ColumnDef::new(MaterialInterpretations::IsCurrent)
                    .boolean()
                    .not_null()
                    .default(true),
            )
            .foreign_key(
                ForeignKey::create()
                    .from(
                        Self::table_iden(),
                        MaterialInterpretations::OccurrenceId,
                    )
                    .to(Occurrences::table_iden(), Alias::new("id"))
                    .on_delete(ForeignKeyAction::Restrict),
            )
            .to_owned()
    }

    /// Generates a partial unique index: one current interpretation per
    /// (occurrence, parser) pair.
    #[must_use]
    pub fn create_partial_unique_index_sql() -> String {
        format!(
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_interpretations_current \
             ON {}.{}(occurrence_id, parser_id) WHERE is_current = true",
            Self::schema_name(),
            Self::table_name()
        )
    }

    /// Generates indexes for `raw.material_interpretations`.
    #[must_use]
    pub fn create_indexes() -> Vec<IndexCreateStatement> {
        vec![
            // Find all interpretations for an occurrence
            Index::create()
                .if_not_exists()
                .name("ix_interpretations_occurrence_id")
                .table(Self::table_iden())
                .col(MaterialInterpretations::OccurrenceId)
                .to_owned(),
            // Find all interpretations by a specific parser
            Index::create()
                .if_not_exists()
                .name("ix_interpretations_parser_id")
                .table(Self::table_iden())
                .col(MaterialInterpretations::ParserId)
                .to_owned(),
            // Find all interpretations for an event (reverse lookup)
            Index::create()
                .if_not_exists()
                .name("ix_interpretations_event_id")
                .table(Self::table_iden())
                .col(MaterialInterpretations::EventId)
                .to_owned(),
            // Source-unit + parser + version scoped lookups
            Index::create()
                .if_not_exists()
                .name("ix_interpretations_unit_parser_version")
                .table(Self::table_iden())
                .col(MaterialInterpretations::SourceUnitId)
                .col(MaterialInterpretations::ParserId)
                .col(MaterialInterpretations::ParserVersion)
                .to_owned(),
            // Current interpretations (most common query)
            Index::create()
                .if_not_exists()
                .name("ix_interpretations_is_current")
                .table(Self::table_iden())
                .col(MaterialInterpretations::IsCurrent)
                .cond_where(Expr::col(MaterialInterpretations::IsCurrent).eq(true))
                .to_owned(),
        ]
    }
}

// =============================================================================
// Add occurrence_id column to core.events
// =============================================================================

impl Events {
    /// SQL to add the `occurrence_id` column to `core.events` if it does not
    /// already exist. Only set for material-provenance events.
    #[must_use]
    pub fn add_occurrence_id_column_sql() -> String {
        "ALTER TABLE core.events ADD COLUMN IF NOT EXISTS occurrence_id UUID".to_string()
    }

    /// Index for occurrence_id on core.events (partial, only where set).
    #[must_use]
    pub fn create_occurrence_id_index() -> IndexCreateStatement {
        Index::create()
            .if_not_exists()
            .name("ix_events_occurrence_id")
            .table(Events::table_iden())
            .col(Events::OccurrenceId)
            .cond_where(Expr::col(Events::OccurrenceId).is_not_null())
            .to_owned()
    }
}

// =============================================================================
// Add OccurrenceId to the Events iden enum
// =============================================================================

/// Extend the Events iden enum with the new column.
///
/// Since `Events` is defined in `events.rs` and we can't add variants to an
/// existing enum from another module, the default value is provided by a
/// separate ColumnDef helper below so callers don't need to edit the Events
/// enum for the optional nullable FK column.
impl Events {
    /// Column name constant for the new `occurrence_id` column on `core.events`.
    #[must_use]
    pub fn occurrence_id_column_name() -> &'static str {
        "occurrence_id"
    }
}
