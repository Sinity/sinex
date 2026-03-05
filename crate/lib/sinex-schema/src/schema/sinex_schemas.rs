//! The Canonical Database Schema for System Contracts and Manifests.
//!
//! This module defines the tables within the `sinex_schemas` and `core` namespaces
//! that are responsible for managing the system's "meta-layer". This includes:
//! - Data contracts for event payloads (`event_payload_schemas`).
//! - Manifests for the nodes that interpret data (`node_manifests`).
//! - Sources for discovering schemas via `GitOps` (`gitops_schema_sources` - aspirational, see docs).
//! - Caching for validation results (`validation_cache`).

use crate::primitives::{Timestamp, Uuid};
use crate::schema::{Events, TableDef};
use sea_orm_migration::prelude::*;
use serde_json::Value as JsonValue;
use sqlx::FromRow;

// =============================================================================
// I. EVENT PAYLOAD SCHEMAS
// =============================================================================

/// **Table: `sinex_schemas.event_payload_schemas`**
///
/// The central registry for all event payload JSON schemas. This table acts as the
/// data contract registry for the entire system. It is managed by the `sinex-schema`
/// tool (which synchronizes from Rust code) and is read by `ingestd` at runtime
/// to perform validation on all incoming events.
#[derive(Iden, Copy, Clone)]
pub enum EventPayloadSchemas {
    Table,
    Id,
    Source,
    EventType,
    SchemaVersion,
    SchemaContent,
    ContentHash,
    IsActive,
    UpdatedAt,
}

impl TableDef for EventPayloadSchemas {
    fn table_name() -> &'static str {
        "event_payload_schemas"
    }
    fn schema_name() -> &'static str {
        "sinex_schemas"
    }
    fn primary_key() -> &'static str {
        "id"
    }
}

#[derive(Debug, FromRow)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct EventPayloadSchemaRecord {
    pub id: Uuid,
    pub source: String,
    pub event_type: String,
    pub schema_version: String,
    pub schema_content: JsonValue,
    pub content_hash: String,
    pub is_active: bool,
    pub updated_at: Timestamp,
}

impl EventPayloadSchemas {
    /// Generates the `CREATE TABLE` statement for `sinex_schemas.event_payload_schemas`.
    #[must_use]
    pub fn create_table_statement() -> TableCreateStatement {
        Table::create()
            .table(Self::table_iden())
            .if_not_exists()
            .col(
                ColumnDef::new(EventPayloadSchemas::Id)
                    .custom(Alias::new("UUID"))
                    .primary_key()
                    .extra("DEFAULT uuidv7()"),
            )
            .col(
                ColumnDef::new(EventPayloadSchemas::Source)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(EventPayloadSchemas::EventType)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(EventPayloadSchemas::SchemaVersion)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(EventPayloadSchemas::SchemaContent)
                    .json_binary()
                    .not_null(),
            )
            .col(
                ColumnDef::new(EventPayloadSchemas::ContentHash)
                    .text()
                    .not_null()
                    .unique_key(),
            )
            .col(
                ColumnDef::new(EventPayloadSchemas::IsActive)
                    .boolean()
                    .not_null()
                    .default(true),
            )
            .col(
                ColumnDef::new(EventPayloadSchemas::UpdatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .to_owned()
    }

    #[must_use]
    pub fn create_indexes() -> Vec<IndexCreateStatement> {
        vec![
            Index::create()
                .if_not_exists()
                .name("uk_schema_identity")
                .table(Self::table_iden())
                .col(EventPayloadSchemas::Source)
                .col(EventPayloadSchemas::EventType)
                .col(EventPayloadSchemas::SchemaVersion)
                .unique()
                .to_owned(),
        ]
    }

    /// Creates a trigger to update the `updated_at` column
    #[must_use]
    pub fn create_updated_at_trigger_sql() -> String {
        format!(
            r"
            DROP TRIGGER IF EXISTS trg_event_payload_schemas_updated_at ON {}.{};
            CREATE TRIGGER trg_event_payload_schemas_updated_at
            BEFORE UPDATE ON {}.{}
            FOR EACH ROW EXECUTE FUNCTION public.set_current_timestamp_updated_at();
            ",
            Self::schema_name(),
            Self::table_name(),
            Self::schema_name(),
            Self::table_name()
        )
    }
}

// =============================================================================
// II. PROCESSOR MANIFESTS
// =============================================================================

/// **Table: `core.node_manifests`**
///
/// A registry for the *immutable definition* of all nodes (ingestors, automata,
/// agents). This table provides a durable record of each node's version and,
/// crucially, its deterministic data processing rules (like `anchor_rule_version`).
/// It allows the system to link an event back to the exact version of the code that
/// produced it, which is critical for auditable and reproducible replays, especially
/// for detecting "anchor churn".
#[derive(Iden, Copy, Clone)]
pub enum NodeManifests {
    Table,
    Id,
    NodeName,
    NodeType,
    Version,
    Description,
    // Key field for reproducible replays
    AnchorRuleVersion,
    ConfigSchema,
    ConsumesEventTypes,
    CreatedAt,
    /// Runtime status: 'active', 'inactive', etc.
    Status,
    /// Timestamp of the most recent heartbeat
    LastHeartbeatAt,
}

impl TableDef for NodeManifests {
    fn table_name() -> &'static str {
        "node_manifests"
    }
    fn schema_name() -> &'static str {
        "core"
    }
    fn primary_key() -> &'static str {
        "id"
    }
}

#[derive(Debug, FromRow)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct NodeManifestRecord {
    pub id: i32,
    pub node_name: String,
    pub node_type: String,
    pub version: String,
    pub description: Option<String>,
    pub anchor_rule_version: i32,
    pub config_schema: Option<JsonValue>,
    pub consumes_event_types: Option<JsonValue>,
    pub created_at: Timestamp,
    pub status: String,
    pub last_heartbeat_at: Option<Timestamp>,
}

impl NodeManifests {
    #[must_use]
    pub fn create_table_statement() -> TableCreateStatement {
        Table::create()
            .table(Self::table_iden())
            .if_not_exists()
            .col(
                ColumnDef::new(NodeManifests::Id)
                    .integer()
                    .auto_increment()
                    .primary_key(),
            )
            .col(ColumnDef::new(NodeManifests::NodeName).text().not_null())
            .col(
                ColumnDef::new(NodeManifests::NodeType)
                    .text()
                    .not_null()
                    .check(Expr::cust(
                        "node_type IN ('ingestor', 'automaton', 'agent', 'system')",
                    )),
            )
            .col(ColumnDef::new(NodeManifests::Version).text().not_null())
            .col(ColumnDef::new(NodeManifests::Description).text())
            // This version number tracks changes to an ingestor's deterministic slicing and anchoring logic.
            // The replay planner uses this to detect "anchor churn".
            .col(
                ColumnDef::new(NodeManifests::AnchorRuleVersion)
                    .integer()
                    .not_null()
                    .default(1),
            )
            .col(ColumnDef::new(NodeManifests::ConfigSchema).json_binary())
            .col(ColumnDef::new(NodeManifests::ConsumesEventTypes).json_binary())
            .col(
                ColumnDef::new(NodeManifests::CreatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .to_owned()
    }

    #[must_use]
    pub fn create_indexes() -> Vec<IndexCreateStatement> {
        vec![
            Index::create()
                .if_not_exists()
                .name("uk_node_version")
                .table(Self::table_iden())
                .col(NodeManifests::NodeName)
                .col(NodeManifests::Version)
                .unique()
                .to_owned(),
        ]
    }

    #[must_use]
    pub fn create_gin_indexes_sql() -> Vec<String> {
        vec![format!(
            "CREATE INDEX IF NOT EXISTS ix_node_manifests_consumes_event_types \
             ON {}.{} USING GIN ({})",
            Self::schema_name(),
            Self::table_name(),
            NodeManifests::ConsumesEventTypes.to_string()
        )]
    }
}

// =============================================================================
// III. SCHEMA DISCOVERY & VALIDATION CACHING
// =============================================================================

/// **Table: `sinex_schemas.gitops_schema_sources`**
///
/// Defines Git repositories as sources of truth for event schemas. A background
/// job in `ingestd` or a dedicated service can poll these sources, discover new
/// or updated schema files (e.g., `.json` files), and automatically register
/// them in the `event_payload_schemas` table. This enables a fully automated,
/// CI/CD-driven workflow for managing data contracts.
///
/// **STATUS:** Aspirational (table defined, no sync implementation).
/// See `crate/lib/sinex-schema/docs/gitops-schema-sources-status.md` for roadmap.
#[derive(Iden, Copy, Clone)]
pub enum GitopsSchemaSources {
    Table,
    Id,
    RepositoryUrl,
    Branch,
    PathPattern,
    SyncEnabled,
    LastSyncAt,
    LastSyncCommit,
    SyncFrequencyMinutes,
    UpdatedAt,
}

impl TableDef for GitopsSchemaSources {
    fn table_name() -> &'static str {
        "gitops_schema_sources"
    }
    fn schema_name() -> &'static str {
        "sinex_schemas"
    }
    fn primary_key() -> &'static str {
        "id"
    }
}

impl GitopsSchemaSources {
    #[must_use]
    pub fn create_table_statement() -> TableCreateStatement {
        Table::create()
            .table(Self::table_iden())
            .if_not_exists()
            .col(
                ColumnDef::new(GitopsSchemaSources::Id)
                    .custom(Alias::new("UUID"))
                    .primary_key()
                    .extra("DEFAULT uuidv7()"),
            )
            .col(
                ColumnDef::new(GitopsSchemaSources::RepositoryUrl)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(GitopsSchemaSources::Branch)
                    .text()
                    .not_null()
                    .default("'main'"),
            )
            .col(
                ColumnDef::new(GitopsSchemaSources::PathPattern)
                    .text()
                    .not_null()
                    .default("'schemas/**/*.json'"),
            )
            .col(
                ColumnDef::new(GitopsSchemaSources::SyncEnabled)
                    .boolean()
                    .not_null()
                    .default(true),
            )
            .col(ColumnDef::new(GitopsSchemaSources::LastSyncAt).timestamp_with_time_zone())
            .col(ColumnDef::new(GitopsSchemaSources::LastSyncCommit).text())
            .col(
                ColumnDef::new(GitopsSchemaSources::SyncFrequencyMinutes)
                    .integer()
                    .not_null()
                    .default(60),
            )
            .col(
                ColumnDef::new(GitopsSchemaSources::UpdatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .to_owned()
    }

    #[must_use]
    pub fn create_indexes() -> Vec<IndexCreateStatement> {
        vec![
            Index::create()
                .if_not_exists()
                .name("uk_gitops_source")
                .table(Self::table_iden())
                .col(GitopsSchemaSources::RepositoryUrl)
                .col(GitopsSchemaSources::Branch)
                .col(GitopsSchemaSources::PathPattern)
                .unique()
                .to_owned(),
            Index::create()
                .if_not_exists()
                .name("ix_gitops_sources_for_sync")
                .table(Self::table_iden())
                .col(GitopsSchemaSources::LastSyncAt)
                .cond_where(Expr::col(GitopsSchemaSources::SyncEnabled).eq(true))
                .to_owned(),
        ]
    }

    /// Creates a trigger to update the `updated_at` column
    #[must_use]
    pub fn create_updated_at_trigger_sql() -> String {
        format!(
            r"
            DROP TRIGGER IF EXISTS trg_gitops_schema_sources_updated_at ON {}.{};
            CREATE TRIGGER trg_gitops_schema_sources_updated_at
            BEFORE UPDATE ON {}.{}
            FOR EACH ROW EXECUTE FUNCTION public.set_current_timestamp_updated_at();
            ",
            Self::schema_name(),
            Self::table_name(),
            Self::schema_name(),
            Self::table_name()
        )
    }
}

/// **Table: `sinex_schemas.validation_cache`**
///
/// A performance optimization table. The `is_payload_valid` `PostgreSQL` function
/// can be computationally expensive as it involves parsing and comparing large JSON
/// objects. This table caches the validation result for a given `(event_id, schema_id)`
/// pair to avoid re-computation.
#[derive(Iden, Copy, Clone)]
pub enum ValidationCache {
    Table,
    EventId,
    SchemaId,
    IsValid,
    ValidationErrors,
    ValidatedAt,
}

impl TableDef for ValidationCache {
    fn table_name() -> &'static str {
        "validation_cache"
    }
    fn schema_name() -> &'static str {
        "sinex_schemas"
    }
    fn primary_key() -> &'static str {
        "(event_id, schema_id)"
    }
}

impl ValidationCache {
    #[must_use]
    pub fn create_table_statement() -> TableCreateStatement {
        Table::create()
            .table(Self::table_iden())
            .if_not_exists()
            .col(
                ColumnDef::new(ValidationCache::EventId)
                    .custom(Alias::new("UUID"))
                    .not_null(),
            )
            .col(
                ColumnDef::new(ValidationCache::SchemaId)
                    .custom(Alias::new("UUID"))
                    .not_null(),
            )
            .col(
                ColumnDef::new(ValidationCache::IsValid)
                    .boolean()
                    .not_null(),
            )
            .col(ColumnDef::new(ValidationCache::ValidationErrors).json_binary())
            .col(
                ColumnDef::new(ValidationCache::ValidatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .primary_key(
                Index::create()
                    .col(ValidationCache::EventId)
                    .col(ValidationCache::SchemaId),
            )
            .foreign_key(
                ForeignKey::create()
                    .from(Self::table_iden(), ValidationCache::EventId)
                    .to(Events::table_iden(), Alias::new("id"))
                    .on_delete(ForeignKeyAction::Cascade),
            )
            .foreign_key(
                ForeignKey::create()
                    .from(Self::table_iden(), ValidationCache::SchemaId)
                    .to(EventPayloadSchemas::table_iden(), EventPayloadSchemas::Id)
                    .on_delete(ForeignKeyAction::Cascade),
            )
            .to_owned()
    }
}
