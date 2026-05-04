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
                    .check(Expr::cust("material_kind IN ('annex', 'git')")),
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
                        "timing_info_type IN ('realtime', 'intrinsic', 'inferred')",
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
