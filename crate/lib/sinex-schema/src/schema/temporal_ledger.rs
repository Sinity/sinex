//! The Canonical Database Schema for the Temporal Ledger.
//!
//! This module defines the `raw.temporal_ledger` table, a critical component
//! for the "Source Material is Ground Truth" and "Provenance Everywhere" principles.
//! It is a high-precision, immutable, append-only log that records *when* each
//! slice of data was physically acquired.

use crate::schema::{SourceMaterialRegistry, TableDef};
use crate::ulid::Ulid;
use sea_orm_migration::prelude::*;
use sqlx::FromRow;
use time::OffsetDateTime;

// =============================================================================
// The `raw.temporal_ledger` Table
// =============================================================================

/// **Table: `raw.temporal_ledger`**
///
/// An append-only ledger providing a high-precision, immutable record of when
/// each chunk of data was physically acquired. This table is the ground truth
/// for the *capture time* of all information. Ingestors **MUST** consult this
/// table to derive the `ts_orig` for the events they produce.
#[derive(Iden, Copy, Clone)]
pub enum TemporalLedger {
    Table,
    Id,
    SourceMaterialId,
    OffsetStart,
    OffsetEnd,
    OffsetKind,
    TsCapture,
    // Decomposed time quality fields for performance and integrity
    Precision,
    Clock,
    SourceType,
}

impl TableDef for TemporalLedger {
    fn table_name() -> &'static str {
        "temporal_ledger"
    }
    fn schema_name() -> &'static str {
        "raw"
    }
    fn primary_key() -> &'static str {
        "id"
    }
}

/// The Rust struct representation of a row from `raw.temporal_ledger`.
#[derive(Debug, FromRow)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct TemporalLedgerRecord {
    pub id: Ulid,
    pub source_material_id: Ulid,
    pub offset_start: i64,
    pub offset_end: i64,
    pub offset_kind: String,
    pub ts_capture: OffsetDateTime,
    pub precision: String,
    pub clock: String,
    pub source_type: String,
}

impl TemporalLedger {
    /// Generates the `CREATE TABLE` statement for `raw.temporal_ledger`.
    #[must_use]
    pub fn create_table_statement() -> TableCreateStatement {
        Table::create()
            .table(Self::table_iden())
            .if_not_exists()
            .col(ColumnDef::new(TemporalLedger::Id).custom(Alias::new("ULID")).primary_key().extra("DEFAULT gen_ulid()"))
            .col(ColumnDef::new(TemporalLedger::SourceMaterialId).custom(Alias::new("ULID")).not_null())
            .col(ColumnDef::new(TemporalLedger::OffsetStart).big_integer().not_null())
            .col(ColumnDef::new(TemporalLedger::OffsetEnd).big_integer().not_null())
            .col(ColumnDef::new(TemporalLedger::OffsetKind).text().not_null().check(Expr::cust("offset_kind IN ('byte', 'line', 'rowid', 'logical')")))
            .col(ColumnDef::new(TemporalLedger::TsCapture).timestamp_with_time_zone().not_null())
            .col(ColumnDef::new(TemporalLedger::Precision).text().not_null().check(Expr::cust("precision IN ('exact', 'bounded')")))
            .col(ColumnDef::new(TemporalLedger::Clock).text().not_null().check(Expr::cust("clock IN ('monotonic', 'wall')")))
            .col(ColumnDef::new(TemporalLedger::SourceType).text().not_null().check(Expr::cust("source_type IN ('realtime_capture', 'intrinsic_content', 'inferred_mtime', 'inferred_user')")))
            .foreign_key(
                ForeignKey::create()
                    .from(Self::table_iden(), TemporalLedger::SourceMaterialId)
                    .to(SourceMaterialRegistry::table_iden(), Alias::new("id"))
                    .on_delete(ForeignKeyAction::Cascade) // If the source material is deleted, its ledger entries are useless.
            )
            .check(Expr::col(TemporalLedger::OffsetEnd).gte(Expr::col(TemporalLedger::OffsetStart)))
            .to_owned()
    }

    /// Generates indexes for `raw.temporal_ledger`.
    #[must_use]
    pub fn create_indexes() -> Vec<IndexCreateStatement> {
        vec![
            // Unique constraint on material and offset
            Index::create()
                .name("uk_temporal_ledger_material_offset")
                .table(Self::table_iden())
                .col(TemporalLedger::SourceMaterialId)
                .col(TemporalLedger::OffsetStart)
                .unique()
                .to_owned(),
            // The primary query pattern for ingestors is to look up the capture time for a given byte range.
            Index::create()
                .name("ix_tl_material_offsets")
                .table(Self::table_iden())
                .col(TemporalLedger::SourceMaterialId)
                .col(TemporalLedger::OffsetStart)
                .col(TemporalLedger::OffsetEnd)
                .to_owned(),
            // Index to support time-based queries across all materials.
            Index::create()
                .name("ix_tl_ts_and_source_type")
                .table(Self::table_iden())
                .col(TemporalLedger::TsCapture)
                .col(TemporalLedger::SourceType)
                .to_owned(),
        ]
    }

    /// Generates the trigger that enforces the append-only nature of the temporal ledger.
    /// This is a critical invariant that guarantees the history of data capture cannot be altered.
    #[must_use]
    pub fn create_append_only_trigger_sql() -> &'static str {
        r"
        CREATE OR REPLACE FUNCTION raw.fn_temporal_ledger_append_only()
        RETURNS TRIGGER LANGUAGE plpgsql AS $$
        BEGIN
            -- Disallow any UPDATE or DELETE operations on this table.
            RAISE EXCEPTION 'Table raw.temporal_ledger is append-only (operation % is forbidden)', TG_OP;
        END $$;

        DROP TRIGGER IF EXISTS trg_tl_no_update_delete ON raw.temporal_ledger;
        CREATE TRIGGER trg_tl_no_update_delete
        BEFORE UPDATE OR DELETE ON raw.temporal_ledger
        FOR EACH ROW EXECUTE FUNCTION raw.fn_temporal_ledger_append_only();
        "
    }
}
