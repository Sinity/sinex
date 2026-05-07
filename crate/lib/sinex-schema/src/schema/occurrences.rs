//! Schema definitions for `raw.occurrences` and `raw.material_interpretations`.
//!
//! These tables make replay/dedup explicit by recording stable occurrence slots
//! and the interpretation history of each occurrence by specific parser versions.
//!
//! # Design
//!
//! - **`raw.occurrences`** — stable source-unit-scoped logical occurrence slots.
//!   Each row identifies "this real-world thing happened at this anchor in this
//!   source material." Occurrences are the replay surface: when a parser version
//!   changes, we re-interpret the same occurrences.
//!
//! - **`raw.material_interpretations`** — records that a specific parser version
//!   interpreted a specific occurrence and produced an event. The `is_current`
//!   flag tracks which interpretation is the live one. When replay produces a
//!   new interpretation, the old one is marked `is_current = false`.
//!
//! # Relationship to core.events
//!
//! These are side tables that make replay/dedup explicit, not a clean-break
//! replacement for the existing `core.events` XOR provenance columns. The
//! current provenance model stays unchanged. `material_interpretations.event_id`
//! points to the event row — events get inserted first, then the interpretation
//! record is written.

use crate::primitives::Uuid;
use crate::schema::{SourceMaterialRegistry, TableDef};
use sea_query::{
    Alias, ColumnDef, ConditionalStatement, Expr, ForeignKey, ForeignKeyAction, Iden, Index,
    IndexCreateStatement, IndexOrder, Table, TableCreateStatement,
};
use serde_json::Value as JsonValue;
use sqlx::FromRow;

// =============================================================================
// The `raw.occurrences` Table
// =============================================================================

/// **Table: `raw.occurrences`**
///
/// Stable logical occurrence slots scoped to a source unit, source material,
/// and anchor. Each row says: "at this location in this material, something
/// happened that a parser could interpret."
///
/// Unlike `core.events` (which are interpretations), occurrences are the stable
/// replay surface. When a parser version changes, we re-interpret the same
/// occurrences without re-scanning the source material.
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
    pub created_at: time::OffsetDateTime,
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
                    .primary_key()
                    .extra("DEFAULT uuidv7()"),
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
                         'sequence_number', 'natural_key', 'cursor_token', \
                         'git_oid', 'stream_frame')",
                    )),
            )
            .col(
                ColumnDef::new(Occurrences::AnchorData)
                    .json_binary()
                    .not_null()
                    .default(Expr::cust("'{}'::jsonb")),
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
                    .on_delete(ForeignKeyAction::Cascade),
            )
            .to_owned()
    }

    /// Generates indexes for `raw.occurrences`.
    #[must_use]
    pub fn create_indexes() -> Vec<IndexCreateStatement> {
        vec![
            // Unique constraint: one occurrence per (source_unit, material, anchor_kind, anchor_data)
            // This prevents duplicate occurrence records for the same real-world slot.
            Index::create()
                .if_not_exists()
                .name("uk_occurrences_material_anchor")
                .table(Self::table_iden())
                .col(Occurrences::SourceUnitId)
                .col(Occurrences::SourceMaterialId)
                .col(Occurrences::AnchorKind)
                // Anchor_data is jsonb and can't be in a btree unique index directly.
                // The application layer enforces the full uniqueness constraint.
                .unique()
                .to_owned(),
            // Look up occurrences by source material for replay planning.
            Index::create()
                .if_not_exists()
                .name("ix_occurrences_material")
                .table(Self::table_iden())
                .col(Occurrences::SourceMaterialId)
                .col((Occurrences::CreatedAt, IndexOrder::Desc))
                .to_owned(),
            // Look up occurrences by source unit for scope-based queries.
            Index::create()
                .if_not_exists()
                .name("ix_occurrences_source_unit")
                .table(Self::table_iden())
                .col(Occurrences::SourceUnitId)
                .col((Occurrences::CreatedAt, IndexOrder::Desc))
                .to_owned(),
            // Natural key lookup for domain-specific dedup.
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
/// and produced an event (the interpretation result). Tracks which interpretation
/// is current so replay can mark old interpretations as superseded.
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
    pub interpreted_at: time::OffsetDateTime,
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
                    .primary_key()
                    .extra("DEFAULT uuidv7()"),
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
                    .not_null()
                    .check(Expr::cust(
                        "length(BTRIM(source_unit_id, E' \\t\\n\\r\\v\\f')) > 0",
                    )),
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
                    .from(Self::table_iden(), MaterialInterpretations::OccurrenceId)
                    .to(Occurrences::table_iden(), Alias::new("id"))
                    .on_delete(ForeignKeyAction::Cascade),
            )
            // Note: event_id is NOT a FK to core.events because TimescaleDB
            // hypertables cannot be the target of FK references from other tables.
            // The application layer enforces referential integrity.
            .to_owned()
    }

    /// Generates indexes for `raw.material_interpretations`.
    #[must_use]
    pub fn create_indexes() -> Vec<IndexCreateStatement> {
        vec![
            // Find all interpretations for a given occurrence.
            Index::create()
                .if_not_exists()
                .name("ix_material_interpretations_occurrence")
                .table(Self::table_iden())
                .col(MaterialInterpretations::OccurrenceId)
                .col((MaterialInterpretations::InterpretedAt, IndexOrder::Desc))
                .to_owned(),
            // Find the current interpretation for an occurrence.
            Index::create()
                .if_not_exists()
                .name("ix_material_interpretations_current")
                .table(Self::table_iden())
                .col(MaterialInterpretations::OccurrenceId)
                .col(MaterialInterpretations::IsCurrent)
                .cond_where(Expr::col(MaterialInterpretations::IsCurrent).eq(true))
                .to_owned(),
            // Find all outputs for a parser/version combination.
            Index::create()
                .if_not_exists()
                .name("ix_material_interpretations_parser_version")
                .table(Self::table_iden())
                .col(MaterialInterpretations::ParserId)
                .col(MaterialInterpretations::ParserVersion)
                .col((MaterialInterpretations::InterpretedAt, IndexOrder::Desc))
                .to_owned(),
            // Navigate from an event back to its interpretation record.
            Index::create()
                .if_not_exists()
                .name("ix_material_interpretations_event")
                .table(Self::table_iden())
                .col(MaterialInterpretations::EventId)
                .to_owned(),
            // Source unit scope queries.
            Index::create()
                .if_not_exists()
                .name("ix_material_interpretations_source_unit")
                .table(Self::table_iden())
                .col(MaterialInterpretations::SourceUnitId)
                .col((MaterialInterpretations::InterpretedAt, IndexOrder::Desc))
                .to_owned(),
        ]
    }
}
