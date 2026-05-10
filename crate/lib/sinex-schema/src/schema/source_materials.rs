//! The Canonical Database Schema for the Source Material Registry.
//!
//! This module defines the `raw.source_material_registry` table, which is the
//! universal manifest for all external data artifacts captured by the system.
//! A record in this table is the "birth certificate" for any piece of information
//! entering Sinex and is the root of all external provenance chains.

use crate::primitives::{Timestamp, Uuid};
use crate::schema::{Blobs, TableDef};
use sea_query::{
    Alias, ColumnDef, ConditionalStatement, Expr, ForeignKey, ForeignKeyAction, Iden, Index,
    IndexCreateStatement, IndexOrder, Table, TableCreateStatement,
};
use serde_json::Value as JsonValue;
use sqlx::FromRow;

// =============================================================================
// The `raw.source_material_registry` Table
// =============================================================================

/// **Table: `raw.source_material_registry`**
///
/// This table is the manifest for all captured external data artifacts. It is
/// managed by capture pipelines using the "Stage-as-you-go" pattern. An entry is
/// created with `status = 'sensing'` before the data is
/// fully captured, providing a stable `id` that ingestors can immediately use
/// for event provenance. The record is then updated to a terminal status
/// (`completed`, `cancelled`, `recovered_partial`, or `failed`) upon finalization.
#[derive(Iden, Copy, Clone)]
pub enum SourceMaterialRegistry {
    Table,
    Id,
    MaterialKind,
    SourceIdentifier,
    Status,
    TimingInfoType,
    Metadata,
    StagedAt,
    StartTime,
    EndTime,
    StagedBy,
    StagedOnHost,
    OptionalBlobId,
    TotalBytes,
    /// Operator-declared coverage contract (#1174).
    ///
    /// JSONB column carrying a [`DeclaredCoverageContract`][dc] payload with
    /// the discriminator field `kind` constrained by a named CHECK to one of
    /// `Continuous`, `PeriodicDump`, `OpportunisticImport`, `FiniteOneShot`,
    /// `EphemeralStream`, or `Unknown`. Legacy rows default to `Unknown` so
    /// continuity reports can flag "configuration gap" rather than "data gap".
    ///
    /// [dc]: sinex_primitives::sources::continuity::DeclaredCoverageContract
    CoverageContract,
    /// Operator-declared privacy classification (#1174).
    ///
    /// `TEXT NOT NULL DEFAULT 'unknown'`. Constrained by a named CHECK to
    /// one of `public`, `personal`, `secret`, `redacted`, `unknown`. Seam
    /// classification only treats `personal` / `secret` / `redacted` as
    /// private — never `unknown` — to keep heuristic and declared signals
    /// distinct.
    ///
    /// Mirrored by [`PrivacyClass`][pc] in primitives.
    ///
    /// [pc]: sinex_primitives::sources::continuity::PrivacyClass
    PrivacyClass,
}

/// **Table: `raw.source_material_links`**
///
/// Directional evidence links between source materials. These links are not
/// event provenance and deliberately do not weaken the `core.events`
/// material/synthesis XOR invariant. They record auxiliary evidence such as
/// "this row-stream material is backed by that `SQLite` snapshot material".
#[derive(Iden, Copy, Clone)]
pub enum SourceMaterialLinks {
    Table,
    Id,
    FromMaterialId,
    ToMaterialId,
    RelationType,
    Metadata,
    CreatedAt,
}

impl TableDef for SourceMaterialRegistry {
    fn table_name() -> &'static str {
        "source_material_registry"
    }
    fn schema_name() -> &'static str {
        "raw"
    }
    fn primary_key() -> &'static str {
        "id"
    }
}

/// The Rust struct representation of a row from `raw.source_material_registry`.
#[derive(Debug, FromRow, serde::Serialize, serde::Deserialize)]
pub struct SourceMaterialRecord {
    pub id: Uuid,
    pub material_kind: String,
    /// Logical source identifier, optionally carrying a material_id for
    /// disambiguation.  Canonical parser/formatter:
    /// `sinex_primitives::domain::SourceIdentifier`.
    pub source_identifier: String,
    pub status: String,
    pub timing_info_type: String,
    pub metadata: JsonValue,
    pub staged_at: Timestamp,
    pub start_time: Option<Timestamp>,
    pub end_time: Option<Timestamp>,
    pub staged_by: Option<String>,
    pub staged_on_host: Option<String>,
    pub optional_blob_id: Option<Uuid>,
    /// Total size of the source material in bytes, set during finalization.
    /// NULL until finalization completes. Used for `anchor_byte` plausibility checks.
    pub total_bytes: Option<i64>,
    /// Operator-declared coverage contract (#1174). Defaults to a
    /// `{"kind":"Unknown"}` payload for legacy rows so continuity reports
    /// can flag "configuration gap" rather than "data gap".
    #[serde(default = "default_unknown_coverage_contract")]
    pub coverage_contract: JsonValue,
    /// Operator-declared privacy classification (#1174). Defaults to
    /// `"unknown"` for legacy rows; downstream seam classification only
    /// treats `"personal"` / `"secret"` / `"redacted"` as private.
    #[serde(default = "default_unknown_privacy_class")]
    pub privacy_class: String,
}

fn default_unknown_coverage_contract() -> JsonValue {
    serde_json::json!({ "kind": "Unknown", "declared_at": null })
}

fn default_unknown_privacy_class() -> String {
    "unknown".to_string()
}

/// The Rust struct representation of a row from `raw.source_material_links`.
#[derive(Debug, FromRow, serde::Serialize, serde::Deserialize)]
pub struct SourceMaterialLinkRecord {
    pub id: Uuid,
    pub from_material_id: Uuid,
    pub to_material_id: Uuid,
    pub relation_type: String,
    pub metadata: JsonValue,
    pub created_at: Timestamp,
}

impl SourceMaterialRegistry {
    /// Generates the `CREATE TABLE` statement for `raw.source_material_registry`.
    #[must_use]
    pub fn create_table_statement() -> TableCreateStatement {
        Table::create()
            .table(Self::table_iden())
            .if_not_exists()
            .col(
                ColumnDef::new(SourceMaterialRegistry::Id)
                    .custom(Alias::new("UUID"))
                    .primary_key()
                    .extra("DEFAULT uuidv7()"),
            )
            .col(
                ColumnDef::new(SourceMaterialRegistry::MaterialKind)
                    .text()
                    .not_null()
                    .check(Expr::cust("material_kind IN ('annex', 'git', 'local_cas')")),
            )
            .col(
                ColumnDef::new(SourceMaterialRegistry::SourceIdentifier)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(SourceMaterialRegistry::Status)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(SourceMaterialRegistry::TimingInfoType)
                    .text()
                    .not_null()
                    .check(Expr::cust(
                        "timing_info_type IN ('realtime', 'intrinsic', 'inferred', 'declared', 'atemporal', 'staged_at')",
                    )),
            )
            .col(
                ColumnDef::new(SourceMaterialRegistry::Metadata)
                    .json_binary()
                    .not_null()
                    .default(Expr::cust("'{}'::jsonb")),
            )
            .col(
                ColumnDef::new(SourceMaterialRegistry::StagedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(ColumnDef::new(SourceMaterialRegistry::StartTime).timestamp_with_time_zone())
            .col(ColumnDef::new(SourceMaterialRegistry::EndTime).timestamp_with_time_zone())
            .col(ColumnDef::new(SourceMaterialRegistry::StagedBy).text())
            .col(ColumnDef::new(SourceMaterialRegistry::StagedOnHost).text())
            .col(ColumnDef::new(SourceMaterialRegistry::OptionalBlobId).custom(Alias::new("UUID")))
            .col(
                ColumnDef::new(SourceMaterialRegistry::TotalBytes)
                    .big_integer()
                    .check(Expr::cust("total_bytes IS NULL OR total_bytes >= 0")),
            )
            // Coverage contract (#1174): JSONB carrying the operator-declared
            // shape. Default is `{"kind":"Unknown","declared_at":null}` so
            // legacy rows pre-date operator intent and continuity reports can
            // flag the absence as a configuration gap.
            //
            // The `kind` discriminator is constrained by the named CHECK
            // `source_material_registry_coverage_contract_kind_check` listed
            // in the convergence registry (`crate/lib/sinex-schema/src/converge.rs`).
            // Named CHECKs are reconciled by the convergence engine; inline
            // CHECKs in CREATE TABLE are not.
            .col(
                ColumnDef::new(SourceMaterialRegistry::CoverageContract)
                    .json_binary()
                    .not_null()
                    .default(Expr::cust(
                        "'{\"kind\":\"Unknown\",\"declared_at\":null}'::jsonb",
                    )),
            )
            // Privacy class (#1174): operator-declared classification.
            // Default `'unknown'` for legacy rows; the convergence registry
            // carries a named CHECK constraining the value to the canonical set.
            .col(
                ColumnDef::new(SourceMaterialRegistry::PrivacyClass)
                    .text()
                    .not_null()
                    .default("unknown"),
            )
            .foreign_key(
                ForeignKey::create()
                    .from(Self::table_iden(), SourceMaterialRegistry::OptionalBlobId)
                    .to(Blobs::table_iden(), Alias::new("id")) // `Blobs::Iden` is fine
                    .on_delete(ForeignKeyAction::SetNull),
            )
            .to_owned()
    }

    /// Generates indexes for `raw.source_material_registry`.
    #[must_use]
    pub fn create_indexes() -> Vec<IndexCreateStatement> {
        vec![
            // Unique constraint on source identifier
            Index::create()
                .if_not_exists()
                .name("uk_sm_registry_source_identifier")
                .table(Self::table_iden())
                .col(SourceMaterialRegistry::SourceIdentifier)
                .unique()
                .to_owned(),
            // Index to efficiently query materials by their source and time.
            Index::create()
                .if_not_exists()
                .name("ix_sm_registry_identifier_staged")
                .table(Self::table_iden())
                .col(SourceMaterialRegistry::SourceIdentifier)
                .col((SourceMaterialRegistry::StagedAt, IndexOrder::Desc))
                .to_owned(),
            // Ingestd seeds recently staged materials on startup. Keep
            // `staged_at` leading so restarts do not scan the registry as it
            // grows.
            Index::create()
                .if_not_exists()
                .name("ix_sm_registry_staged_at")
                .table(Self::table_iden())
                .col((SourceMaterialRegistry::StagedAt, IndexOrder::Desc))
                .to_owned(),
            // Partial index to quickly find materials that have been finalized and have associated blob content.
            Index::create()
                .if_not_exists()
                .name("ix_sm_registry_blob_id")
                .table(Self::table_iden())
                .col(SourceMaterialRegistry::OptionalBlobId)
                .cond_where(Expr::col(SourceMaterialRegistry::OptionalBlobId).is_not_null())
                .to_owned(),
        ]
    }

    /// Generates the trigger enforcing source-material byte-size finalization.
    ///
    /// Event insertion rejects out-of-bounds anchors when `total_bytes` is already
    /// known. This companion trigger handles the reverse order: events may be
    /// admitted while a material is still being captured (`total_bytes IS NULL`),
    /// but finalization cannot later set a byte size that makes those existing
    /// material events impossible.
    #[must_use]
    pub fn create_event_bounds_trigger_sql() -> &'static str {
        r"
        CREATE OR REPLACE FUNCTION raw.fn_source_material_validate_event_bounds()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF NEW.total_bytes IS NULL THEN
                RETURN NEW;
            END IF;

            IF EXISTS (
                SELECT 1
                FROM core.events e
                WHERE e.source_material_id = NEW.id
                  AND (
                    e.anchor_byte > NEW.total_bytes
                    OR (
                        e.offset_kind = 'byte'
                        AND e.offset_start IS NOT NULL
                        AND e.offset_end IS NOT NULL
                        AND (e.offset_start > NEW.total_bytes OR e.offset_end > NEW.total_bytes)
                    )
                  )
                LIMIT 1
            ) THEN
                RAISE EXCEPTION
                    'source material total_bytes would invalidate existing event anchors (source_material_id=%, total_bytes=%)',
                    NEW.id, NEW.total_bytes
                    USING ERRCODE = 'check_violation';
            END IF;

            RETURN NEW;
        END $$;

        DROP TRIGGER IF EXISTS trg_source_material_validate_event_bounds ON raw.source_material_registry;
        CREATE TRIGGER trg_source_material_validate_event_bounds
        BEFORE INSERT OR UPDATE OF total_bytes ON raw.source_material_registry
        FOR EACH ROW EXECUTE FUNCTION raw.fn_source_material_validate_event_bounds();
        "
    }
}

impl TableDef for SourceMaterialLinks {
    fn table_name() -> &'static str {
        "source_material_links"
    }
    fn schema_name() -> &'static str {
        "raw"
    }
    fn primary_key() -> &'static str {
        "id"
    }
}

impl SourceMaterialLinks {
    /// Generates the `CREATE TABLE` statement for `raw.source_material_links`.
    #[must_use]
    pub fn create_table_statement() -> TableCreateStatement {
        Table::create()
            .table(Self::table_iden())
            .if_not_exists()
            .col(
                ColumnDef::new(SourceMaterialLinks::Id)
                    .custom(Alias::new("UUID"))
                    .primary_key()
                    .extra("DEFAULT uuidv7()"),
            )
            .col(
                ColumnDef::new(SourceMaterialLinks::FromMaterialId)
                    .custom(Alias::new("UUID"))
                    .not_null(),
            )
            .col(
                ColumnDef::new(SourceMaterialLinks::ToMaterialId)
                    .custom(Alias::new("UUID"))
                    .not_null(),
            )
            .col(
                ColumnDef::new(SourceMaterialLinks::RelationType)
                    .text()
                    .not_null()
                    .check(Expr::cust("relation_type ~ '^[a-z][a-z0-9_.-]*$'")),
            )
            .col(
                ColumnDef::new(SourceMaterialLinks::Metadata)
                    .json_binary()
                    .not_null()
                    .default(Expr::cust("'{}'::jsonb")),
            )
            .col(
                ColumnDef::new(SourceMaterialLinks::CreatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .foreign_key(
                ForeignKey::create()
                    .from(Self::table_iden(), SourceMaterialLinks::FromMaterialId)
                    .to(
                        SourceMaterialRegistry::table_iden(),
                        SourceMaterialRegistry::Id,
                    )
                    .on_delete(ForeignKeyAction::Cascade),
            )
            .foreign_key(
                ForeignKey::create()
                    .from(Self::table_iden(), SourceMaterialLinks::ToMaterialId)
                    .to(
                        SourceMaterialRegistry::table_iden(),
                        SourceMaterialRegistry::Id,
                    )
                    .on_delete(ForeignKeyAction::Cascade),
            )
            .check(Expr::cust("from_material_id <> to_material_id"))
            .to_owned()
    }

    /// Generates indexes for `raw.source_material_links`.
    #[must_use]
    pub fn create_indexes() -> Vec<IndexCreateStatement> {
        vec![
            Index::create()
                .if_not_exists()
                .name("uk_source_material_links_edge")
                .table(Self::table_iden())
                .col(SourceMaterialLinks::FromMaterialId)
                .col(SourceMaterialLinks::ToMaterialId)
                .col(SourceMaterialLinks::RelationType)
                .unique()
                .to_owned(),
            Index::create()
                .if_not_exists()
                .name("ix_source_material_links_from")
                .table(Self::table_iden())
                .col(SourceMaterialLinks::FromMaterialId)
                .to_owned(),
            Index::create()
                .if_not_exists()
                .name("ix_source_material_links_to")
                .table(Self::table_iden())
                .col(SourceMaterialLinks::ToMaterialId)
                .to_owned(),
            Index::create()
                .if_not_exists()
                .name("ix_source_material_links_relation_created")
                .table(Self::table_iden())
                .col(SourceMaterialLinks::RelationType)
                .col((SourceMaterialLinks::CreatedAt, IndexOrder::Desc))
                .to_owned(),
        ]
    }
}
