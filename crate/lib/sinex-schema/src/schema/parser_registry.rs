//! Schema for `raw.parser_registry` and `raw.parser_jobs`.
//!
//! These tables support the staged-source parser substrate: parser identity
//! registration and per-material parse job lifecycle management.

use crate::primitives::Uuid;
use crate::schema::TableDef;
use sea_query::{
    Alias, ColumnDef, Expr, ForeignKey, ForeignKeyAction, Iden, Index, IndexCreateStatement,
    IndexOrder, Table, TableCreateStatement,
};
use serde_json::Value as JsonValue;
use sqlx::FromRow;

// =============================================================================
// `raw.parser_registry`
// =============================================================================

/// **Table: `raw.parser_registry`**
///
/// Registers parser implementations with the system. Each `(parser_id, parser_version)`
/// pair declares its input shape, source unit, event types, and privacy contexts.
/// This is a lightweight registry — parsers are discovered at process startup and
/// must be registered before jobs can be created for them.
#[derive(Iden, Copy, Clone)]
pub enum ParserRegistry {
    Table,
    ParserId,
    ParserVersion,
    InputShapeKind,
    SourceUnitId,
    DeclaredEventTypes,
    PrivacyContexts,
    ProofObligations,
    Manifest,
    RegisteredAt,
}

impl TableDef for ParserRegistry {
    fn table_name() -> &'static str {
        "parser_registry"
    }
    fn schema_name() -> &'static str {
        "raw"
    }
    fn primary_key() -> &'static str {
        "parser_id"
    }
}

/// The Rust struct representation of a row from `raw.parser_registry`.
#[derive(Debug, FromRow, serde::Serialize, serde::Deserialize)]
pub struct ParserRegistryRecord {
    pub parser_id: String,
    pub parser_version: String,
    pub input_shape_kind: String,
    pub source_unit_id: String,
    pub declared_event_types: JsonValue,
    pub privacy_contexts: JsonValue,
    pub proof_obligations: Vec<String>,
    pub manifest: JsonValue,
    pub registered_at: crate::primitives::Timestamp,
}

impl ParserRegistry {
    /// Generates the `CREATE TABLE` statement for `raw.parser_registry`.
    #[must_use]
    pub fn create_table_statement() -> TableCreateStatement {
        Table::create()
            .table(Self::table_iden())
            .if_not_exists()
            .col(
                ColumnDef::new(ParserRegistry::ParserId)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(ParserRegistry::ParserVersion)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(ParserRegistry::InputShapeKind)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(ParserRegistry::SourceUnitId)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(ParserRegistry::DeclaredEventTypes)
                    .json_binary()
                    .not_null()
                    .default(Expr::cust("'[]'::jsonb")),
            )
            .col(
                ColumnDef::new(ParserRegistry::PrivacyContexts)
                    .json_binary()
                    .not_null()
                    .default(Expr::cust("'[]'::jsonb")),
            )
            .col(
                ColumnDef::new(ParserRegistry::ProofObligations)
                    .array(sea_query::ColumnType::Text)
                    .not_null()
                    .default(Expr::cust("'{}'::text[]")),
            )
            .col(
                ColumnDef::new(ParserRegistry::Manifest)
                    .json_binary()
                    .not_null()
                    .default(Expr::cust("'{}'::jsonb")),
            )
            .col(
                ColumnDef::new(ParserRegistry::RegisteredAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .primary_key(
                sea_query::Index::create()
                    .col(ParserRegistry::ParserId)
                    .col(ParserRegistry::ParserVersion),
            )
            .to_owned()
    }

    /// Generates indexes for `raw.parser_registry`.
    #[must_use]
    pub fn create_indexes() -> Vec<IndexCreateStatement> {
        vec![
            // Look up all parsers for a source unit.
            Index::create()
                .if_not_exists()
                .name("ix_parser_registry_source_unit")
                .table(Self::table_iden())
                .col(ParserRegistry::SourceUnitId)
                .to_owned(),
            // Look up parsers by input shape (for scheduling).
            Index::create()
                .if_not_exists()
                .name("ix_parser_registry_shape")
                .table(Self::table_iden())
                .col(ParserRegistry::InputShapeKind)
                .col(ParserRegistry::RegisteredAt)
                .to_owned(),
        ]
    }
}

// =============================================================================
// `raw.parser_jobs`
// =============================================================================

/// **Table: `raw.parser_jobs`**
///
/// Tracks the lifecycle of a parse job: one material parsed by one parser
/// version. Supports leasing (workers claim jobs with `FOR UPDATE SKIP LOCKED`),
/// retry, and structured error recording.
#[derive(Iden, Copy, Clone)]
pub enum ParserJobs {
    Table,
    Id,
    SourceMaterialId,
    SourceBindingId,
    SourceUnitId,
    ParserId,
    ParserVersion,
    InputShapeKind,
    Status,
    Cursor,
    HighWatermark,
    Attempts,
    MaxAttempts,
    LeaseOwner,
    LeaseExpiresAt,
    OperationId,
    TimingPolicy,
    ErrorClass,
    ErrorSummary,
    QueuedAt,
    StartedAt,
    CompletedAt,
    UpdatedAt,
}

impl TableDef for ParserJobs {
    fn table_name() -> &'static str {
        "parser_jobs"
    }
    fn schema_name() -> &'static str {
        "raw"
    }
    fn primary_key() -> &'static str {
        "id"
    }
}

/// The Rust struct representation of a row from `raw.parser_jobs`.
#[derive(Debug, FromRow, serde::Serialize, serde::Deserialize)]
pub struct ParserJobRecord {
    pub id: Uuid,
    pub source_material_id: Uuid,
    pub source_binding_id: Option<Uuid>,
    pub source_unit_id: String,
    pub parser_id: String,
    pub parser_version: String,
    pub input_shape_kind: String,
    pub status: String,
    pub cursor: Option<JsonValue>,
    pub high_watermark: Option<JsonValue>,
    pub attempts: i32,
    pub max_attempts: i32,
    pub lease_owner: Option<String>,
    pub lease_expires_at: Option<crate::primitives::Timestamp>,
    pub operation_id: Option<Uuid>,
    pub timing_policy: JsonValue,
    pub error_class: Option<String>,
    pub error_summary: Option<String>,
    pub queued_at: crate::primitives::Timestamp,
    pub started_at: Option<crate::primitives::Timestamp>,
    pub completed_at: Option<crate::primitives::Timestamp>,
    pub updated_at: crate::primitives::Timestamp,
}

impl ParserJobs {
    /// Generates the `CREATE TABLE` statement for `raw.parser_jobs`.
    #[must_use]
    pub fn create_table_statement() -> TableCreateStatement {
        Table::create()
            .table(Self::table_iden())
            .if_not_exists()
            .col(
                ColumnDef::new(ParserJobs::Id)
                    .custom(Alias::new("UUID"))
                    .primary_key()
                    .extra("DEFAULT uuidv7()"),
            )
            .col(
                ColumnDef::new(ParserJobs::SourceMaterialId)
                    .custom(Alias::new("UUID"))
                    .not_null(),
            )
            .col(
                ColumnDef::new(ParserJobs::SourceBindingId)
                    .custom(Alias::new("UUID")),
            )
            .col(
                ColumnDef::new(ParserJobs::SourceUnitId)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(ParserJobs::ParserId)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(ParserJobs::ParserVersion)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(ParserJobs::InputShapeKind)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(ParserJobs::Status)
                    .text()
                    .not_null()
                    .check(Expr::cust(
                        "status IN ('queued','leased','running','waiting_material','completed','failed_retryable','failed_permanent','cancelled')",
                    )),
            )
            .col(ColumnDef::new(ParserJobs::Cursor).json_binary())
            .col(ColumnDef::new(ParserJobs::HighWatermark).json_binary())
            .col(
                ColumnDef::new(ParserJobs::Attempts)
                    .integer()
                    .not_null()
                    .default(0),
            )
            .col(
                ColumnDef::new(ParserJobs::MaxAttempts)
                    .integer()
                    .not_null()
                    .default(3),
            )
            .col(ColumnDef::new(ParserJobs::LeaseOwner).text())
            .col(
                ColumnDef::new(ParserJobs::LeaseExpiresAt)
                    .timestamp_with_time_zone(),
            )
            .col(
                ColumnDef::new(ParserJobs::OperationId)
                    .custom(Alias::new("UUID")),
            )
            .col(
                ColumnDef::new(ParserJobs::TimingPolicy)
                    .json_binary()
                    .not_null()
                    .default(Expr::cust("'{}'::jsonb")),
            )
            .col(ColumnDef::new(ParserJobs::ErrorClass).text())
            .col(ColumnDef::new(ParserJobs::ErrorSummary).text())
            .col(
                ColumnDef::new(ParserJobs::QueuedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(ColumnDef::new(ParserJobs::StartedAt).timestamp_with_time_zone())
            .col(
                ColumnDef::new(ParserJobs::CompletedAt)
                    .timestamp_with_time_zone(),
            )
            .col(
                ColumnDef::new(ParserJobs::UpdatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .foreign_key(
                ForeignKey::create()
                    .from(Self::table_iden(), ParserJobs::SourceMaterialId)
                    .to(
                        super::source_materials::SourceMaterialRegistry::table_iden(),
                        super::source_materials::SourceMaterialRegistry::Id,
                    )
                    .on_delete(ForeignKeyAction::Restrict),
            )
            .to_owned()
    }

    /// Generates indexes for `raw.parser_jobs`.
    #[must_use]
    pub fn create_indexes() -> Vec<IndexCreateStatement> {
        vec![
            // Unique constraint for idempotent job creation.
            Index::create()
                .if_not_exists()
                .name("uk_parser_jobs_material_parser_source")
                .table(Self::table_iden())
                .col(ParserJobs::SourceMaterialId)
                .col(ParserJobs::ParserId)
                .col(ParserJobs::ParserVersion)
                .col(ParserJobs::SourceUnitId)
                .unique()
                .to_owned(),
            // Worker lease acquisition: find queued jobs by status.
            Index::create()
                .if_not_exists()
                .name("ix_parser_jobs_status_queued")
                .table(Self::table_iden())
                .col(ParserJobs::Status)
                .col((ParserJobs::QueuedAt, IndexOrder::Desc))
                .to_owned(),
            // Worker lease expiry sweep.
            Index::create()
                .if_not_exists()
                .name("ix_parser_jobs_lease_expiry")
                .table(Self::table_iden())
                .col(ParserJobs::LeaseExpiresAt)
                .col(ParserJobs::Status)
                .to_owned(),
            // Look up jobs by operation (for replay/audit).
            Index::create()
                .if_not_exists()
                .name("ix_parser_jobs_operation")
                .table(Self::table_iden())
                .col(ParserJobs::OperationId)
                .to_owned(),
            // Status transitions.
            Index::create()
                .if_not_exists()
                .name("ix_parser_jobs_source_unit_status")
                .table(Self::table_iden())
                .col(ParserJobs::SourceUnitId)
                .col(ParserJobs::Status)
                .to_owned(),
        ]
    }
}
