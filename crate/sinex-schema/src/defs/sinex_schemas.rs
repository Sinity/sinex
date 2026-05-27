//! The Canonical Database Schema for System Contracts and Manifests.
//!
//! This module defines the tables within the `sinex_schemas` and `core` namespaces
//! that are responsible for managing the system's "meta-layer". This includes:
//! - Data contracts for event payloads (`event_payload_schemas`).
//! - Manifests for the nodes that interpret data (`source_manifests`).
//! - Sources for discovering schemas via `GitOps` (`gitops_schema_sources` - aspirational, see docs).
//! - Caching for validation results (`validation_cache`).

use crate::primitives::{Timestamp, Uuid};
use crate::TableDef;
use sea_query::{
    Alias, ColumnDef, Expr, ForeignKey, ForeignKeyAction, Iden, Index, IndexCreateStatement, Table,
    TableCreateStatement,
};
use serde_json::Value as JsonValue;
use sqlx::FromRow;

// =============================================================================
// I. EVENT PAYLOAD SCHEMAS
// =============================================================================

/// **Table: `sinex_schemas.event_payload_schemas`**
///
/// The central registry for all event payload JSON schemas. This table acts as the
/// data contract registry for the entire system. It is managed by the schema
/// toolchain that synchronizes Rust definitions into Postgres and is read by
/// `ingestd` at runtime to perform validation on all incoming events.
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
    /// Retention horizon for events bound to this schema (#1172).
    ///
    /// `BIGINT NULL` — `NULL` means "never expire" (current default for all
    /// existing schemas). When non-null, the gateway-side TTL enforcer
    /// (Phase 6 follow-up) will archive events older than `retention_seconds`
    /// since `ts_orig`. This phase only lands the column; no archival logic
    /// is wired yet.
    RetentionSeconds,
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

#[derive(Debug, FromRow, serde::Serialize, serde::Deserialize)]
pub struct EventPayloadSchemaRecord {
    pub id: Uuid,
    pub source: String,
    pub event_type: String,
    pub schema_version: String,
    pub schema_content: JsonValue,
    pub content_hash: String,
    pub is_active: bool,
    pub updated_at: Timestamp,
    /// Retention horizon in seconds (#1172). `None` means "never expire".
    /// Phase 1 lands the column; Phase 6 wires gateway-side TTL enforcement.
    #[serde(default)]
    pub retention_seconds: Option<i64>,
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
            // Retention horizon (#1172). NULL = never expire (current default
            // for every schema). Phase 6 wires gateway-side enforcement; this
            // phase only lands the column.
            .col(ColumnDef::new(EventPayloadSchemas::RetentionSeconds).big_integer())
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
// II. RUNS (Execution Tracking)
// =============================================================================

/// **Table: `core.runs`**
///
/// Each row represents a single execution period of a process. A run is
/// created on startup, updated with heartbeats, and marked ended on shutdown.
#[derive(Iden, Copy, Clone)]
pub enum Runs {
    Table,
    Id,
    ManifestId,
    ServiceName,
    InstanceId,
    Host,
    StartedAt,
    EndedAt,
    Status,
    LastHeartbeatAt,
    EffectiveConfigHash,
    EffectiveConfig,
}

impl TableDef for Runs {
    fn table_name() -> &'static str {
        "runs"
    }
    fn schema_name() -> &'static str {
        "core"
    }
    fn primary_key() -> &'static str {
        "id"
    }
}

#[derive(Debug, FromRow, serde::Serialize, serde::Deserialize)]
pub struct RunRecord {
    pub id: Uuid,
    pub manifest_id: Option<i32>,
    pub service_name: String,
    pub instance_id: String,
    pub host: String,
    pub started_at: Timestamp,
    pub ended_at: Option<Timestamp>,
    pub status: String,
    pub last_heartbeat_at: Option<Timestamp>,
    pub effective_config_hash: Option<String>,
    pub effective_config: Option<JsonValue>,
}

impl Runs {
    #[must_use]
    pub fn create_table_statement() -> TableCreateStatement {
        Table::create()
            .table(Self::table_iden())
            .if_not_exists()
            .col(
                ColumnDef::new(Runs::Id)
                    .custom(Alias::new("UUID"))
                    .primary_key()
                    .extra("DEFAULT uuidv7()"),
            )
            .col(ColumnDef::new(Runs::ManifestId).integer())
            .col(ColumnDef::new(Runs::ServiceName).text().not_null())
            .col(ColumnDef::new(Runs::InstanceId).text().not_null())
            .col(ColumnDef::new(Runs::Host).text().not_null())
            .col(
                ColumnDef::new(Runs::StartedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(ColumnDef::new(Runs::EndedAt).timestamp_with_time_zone())
            .col(
                ColumnDef::new(Runs::Status)
                    .text()
                    .not_null()
                    .default("running"),
            )
            .col(ColumnDef::new(Runs::LastHeartbeatAt).timestamp_with_time_zone())
            .col(ColumnDef::new(Runs::EffectiveConfigHash).text())
            .col(ColumnDef::new(Runs::EffectiveConfig).json_binary())
            .foreign_key(
                ForeignKey::create()
                    .name("fk_runs_manifest")
                    .from(Self::table_iden(), Runs::ManifestId)
                    .to(crate::Manifests::table_iden(), Alias::new("id"))
                    .on_delete(ForeignKeyAction::SetNull),
            )
            .to_owned()
    }

    #[must_use]
    pub fn create_indexes() -> Vec<IndexCreateStatement> {
        vec![
            Index::create()
                .if_not_exists()
                .name("ix_runs_service_status")
                .table(Self::table_iden())
                .col(Runs::ServiceName)
                .col(Runs::Status)
                .to_owned(),
            Index::create()
                .if_not_exists()
                .name("ix_runs_heartbeat")
                .table(Self::table_iden())
                .col(Runs::LastHeartbeatAt)
                .to_owned(),
        ]
    }
}

// =============================================================================
// III. BINARY SCHEMA VERSION (Startup Compatibility Gate)
// =============================================================================

/// **Table: `sinex_schemas.binary_schema_version`**
///
/// Single-row table checked at gateway and ingestd startup. If the row is
/// missing it is inserted with the current expected version; if the version
/// mismatches the service refuses to start.
#[derive(Iden, Copy, Clone)]
pub enum BinarySchemaVersion {
    Table,
    Id,
    Version,
}

impl TableDef for BinarySchemaVersion {
    fn table_name() -> &'static str {
        "binary_schema_version"
    }
    fn schema_name() -> &'static str {
        "sinex_schemas"
    }
    fn primary_key() -> &'static str {
        "id"
    }
}

#[derive(Debug, FromRow, serde::Serialize, serde::Deserialize)]
pub struct BinarySchemaVersionRecord {
    pub id: i32,
    pub version: String,
}

impl BinarySchemaVersion {
    #[must_use]
    pub fn create_table_statement() -> TableCreateStatement {
        Table::create()
            .table(Self::table_iden())
            .if_not_exists()
            .col(
                ColumnDef::new(BinarySchemaVersion::Id)
                    .integer()
                    .primary_key(),
            )
            .col(
                ColumnDef::new(BinarySchemaVersion::Version)
                    .text()
                    .not_null(),
            )
            .to_owned()
    }
}
