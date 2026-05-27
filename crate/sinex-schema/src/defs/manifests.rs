//! The canonical database schema for runtime process/source manifests.
//!
//! `core.manifests` records the immutable identity of every runtime entity:
//! nodes (ingestd, gateway), sources (weechat-parser, atuin-history), and
//! automata (canonicalizer, health-aggregator).
//!
//! Each manifest declares what the entity IS — its type, version, parent,
//! and the event types it consumes and emits. This is the runtime counterpart
//! to the compile-time `register_source_unit!` inventory.

use crate::primitives::Timestamp;
use crate::TableDef;
use sea_query::{
    ColumnDef, Expr, ForeignKey, ForeignKeyAction, Iden, Index, IndexCreateStatement, Table,
    TableCreateStatement,
};
use serde_json::Value as JsonValue;
use sqlx::FromRow;

#[derive(Iden, Copy, Clone)]
pub enum Manifests {
    Table,
    Id,
    Name,
    ManifestType,
    Version,
    CommitHash,
    ParentManifestId,
    EmittedEventTypes,
    ConsumedEventTypes,
    Description,
    CreatedAt,
}

impl TableDef for Manifests {
    fn table_name() -> &'static str {
        "manifests"
    }
    fn schema_name() -> &'static str {
        "core"
    }
    fn primary_key() -> &'static str {
        "id"
    }
}

#[derive(Debug, FromRow, serde::Serialize, serde::Deserialize)]
pub struct ManifestRecord {
    pub id: i32,
    pub name: String,
    pub manifest_type: String,
    pub version: String,
    pub commit_hash: Option<String>,
    pub parent_manifest_id: Option<i32>,
    pub emitted_event_types: Option<JsonValue>,
    pub consumed_event_types: Option<JsonValue>,
    pub description: Option<String>,
    pub created_at: Timestamp,
}

impl Manifests {
    #[must_use]
    pub fn create_table_statement() -> TableCreateStatement {
        Table::create()
            .table(Self::table_iden())
            .if_not_exists()
            .col(
                ColumnDef::new(Manifests::Id)
                    .integer()
                    .auto_increment()
                    .primary_key(),
            )
            .col(ColumnDef::new(Manifests::Name).text().not_null())
            // The CHECK constraint on manifest_type is converged by the
            // schema-apply engine from the `NodeType` enum's
            // `#[derive(DbCheck)]` spec (issue #1236). Do NOT add an inline
            // `.check(...)` here — it would survive only on first table
            // creation and prevent the apply engine from owning the rename.
            .col(ColumnDef::new(Manifests::ManifestType).text().not_null())
            .col(ColumnDef::new(Manifests::Version).text().not_null())
            .col(ColumnDef::new(Manifests::CommitHash).text())
            .col(ColumnDef::new(Manifests::ParentManifestId).integer())
            .col(ColumnDef::new(Manifests::EmittedEventTypes).json_binary())
            .col(ColumnDef::new(Manifests::ConsumedEventTypes).json_binary())
            .col(ColumnDef::new(Manifests::Description).text())
            .col(
                ColumnDef::new(Manifests::CreatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .foreign_key(
                ForeignKey::create()
                    .name("fk_manifests_parent")
                    .from(Self::table_iden(), Manifests::ParentManifestId)
                    .to(Self::table_iden(), Manifests::Id)
                    .on_delete(ForeignKeyAction::SetNull),
            )
            .to_owned()
    }

    #[must_use]
    pub fn create_indexes() -> Vec<IndexCreateStatement> {
        vec![
            Index::create()
                .if_not_exists()
                .name("uk_manifest_identity")
                .table(Self::table_iden())
                .col(Manifests::Name)
                .col(Manifests::Version)
                .unique()
                .to_owned(),
            Index::create()
                .if_not_exists()
                .name("ix_manifests_type")
                .table(Self::table_iden())
                .col(Manifests::ManifestType)
                .to_owned(),
            Index::create()
                .if_not_exists()
                .name("ix_manifests_parent")
                .table(Self::table_iden())
                .col(Manifests::ParentManifestId)
                .to_owned(),
        ]
    }
}
