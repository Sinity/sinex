//! Acquisition job catalog schema.
//!
//! Acquisition jobs are durable declarations of source-material acquisition work.
//! They let the system track "stage this source, watch that directory, snapshot
//! this DB" as first-class DB entities, bridging source bindings (#1061) and
//! source-material registry evidence (#1066).
//!
//! This module defines:
//! - `raw.acquisition_jobs` — the acquisition job catalog

use crate::primitives::{Timestamp, Uuid};
use crate::schema::TableDef;
use sea_query::{
    Alias, ColumnDef, Expr, ForeignKey, ForeignKeyAction, Iden, Index, IndexCreateStatement,
    IndexOrder, Table, TableCreateStatement,
};
use serde_json::Value as JsonValue;
use sqlx::FromRow;

// =============================================================================
// raw.acquisition_jobs
// =============================================================================

/// **Table: `raw.acquisition_jobs`**
///
/// Durable declarations of source-material acquisition work. Each row records
/// what to acquire, how to acquire it, and the current state of that acquisition.
#[derive(Iden, Copy, Clone)]
pub enum AcquisitionJobs {
    Table,
    Id,
    SourceBindingId,
    SourceIdentifier,
    AcquisitionMode,
    InputShape,
    ParserBindingId,
    MaterialFormatHint,
    TimingPolicy,
    RawMaterialPolicy,
    CursorState,
    Status,
    Attempts,
    LastError,
    StartedAt,
    CompletedAt,
    MaterialId,
    MaterialStagedAt,
    CreatedAt,
    UpdatedAt,
}

impl TableDef for AcquisitionJobs {
    fn table_name() -> &'static str {
        "acquisition_jobs"
    }
    fn schema_name() -> &'static str {
        "raw"
    }
    fn primary_key() -> &'static str {
        "id"
    }
}

/// Rust struct for a `raw.acquisition_jobs` row.
#[derive(Debug, FromRow, serde::Serialize, serde::Deserialize)]
pub struct AcquisitionJobRecord {
    pub id: Uuid,
    pub source_binding_id: Uuid,
    pub source_identifier: String,
    pub acquisition_mode: String,
    pub input_shape: String,
    pub parser_binding_id: Option<Uuid>,
    pub material_format_hint: Option<String>,
    pub timing_policy: JsonValue,
    pub raw_material_policy: JsonValue,
    pub cursor_state: JsonValue,
    pub status: String,
    pub attempts: i32,
    pub last_error: Option<String>,
    pub started_at: Option<Timestamp>,
    pub completed_at: Option<Timestamp>,
    pub material_id: Option<Uuid>,
    pub material_staged_at: Option<Timestamp>,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

// ── Canonical constant modules for column values ──────────────────────────

/// Canonical acquisition mode values.
pub mod acquisition_mode {
    pub const ONE_SHOT: &str = "one_shot";
    pub const PERIODIC_SCAN: &str = "periodic_scan";
    pub const WATCH_DIRECTORY: &str = "watch_directory";
    pub const APPEND_TAIL: &str = "append_tail";
    pub const SQLITE_SNAPSHOT: &str = "sqlite_snapshot";
    pub const REPOSITORY_SNAPSHOT: &str = "repository_snapshot";

    pub const ALL: &[&str] = &[
        ONE_SHOT,
        PERIODIC_SCAN,
        WATCH_DIRECTORY,
        APPEND_TAIL,
        SQLITE_SNAPSHOT,
        REPOSITORY_SNAPSHOT,
    ];
}

/// Canonical input shape values.
pub mod input_shape {
    pub const STATIC_FILE: &str = "static_file";
    pub const DIRECTORY: &str = "directory";
    pub const GROWING_SQLITE: &str = "growing_sqlite";
    pub const APPEND_ONLY_FILE: &str = "append_only_file";
    pub const API_POLL: &str = "api_poll";
    pub const FILE_DROP: &str = "file_drop";

    pub const ALL: &[&str] = &[
        STATIC_FILE,
        DIRECTORY,
        GROWING_SQLITE,
        APPEND_ONLY_FILE,
        API_POLL,
        FILE_DROP,
    ];
}

/// Canonical acquisition job status values.
pub mod acquisition_job_status {
    pub const PENDING: &str = "pending";
    pub const RUNNING: &str = "running";
    pub const COMPLETED: &str = "completed";
    pub const FAILED: &str = "failed";
    pub const CANCELLED: &str = "cancelled";
    pub const DRAINED: &str = "drained";

    pub const ALL: &[&str] = &[PENDING, RUNNING, COMPLETED, FAILED, CANCELLED, DRAINED];
}

// ── SQL CHECK constraint helpers ──────────────────────────────────────────

fn acquisition_mode_check() -> String {
    let quoted: Vec<String> = acquisition_mode::ALL
        .iter()
        .map(|v| format!("'{v}'"))
        .collect();
    format!("acquisition_mode IN ({})", quoted.join(", "))
}

fn input_shape_check() -> String {
    let quoted: Vec<String> = input_shape::ALL.iter().map(|v| format!("'{v}'")).collect();
    format!("input_shape IN ({})", quoted.join(", "))
}

fn acquisition_job_status_check() -> String {
    let quoted: Vec<String> = acquisition_job_status::ALL
        .iter()
        .map(|v| format!("'{v}'"))
        .collect();
    format!("status IN ({})", quoted.join(", "))
}

impl AcquisitionJobs {
    #[must_use]
    pub fn create_table_statement() -> TableCreateStatement {
        Table::create()
            .table(Self::table_iden())
            .if_not_exists()
            .col(
                ColumnDef::new(AcquisitionJobs::Id)
                    .custom(Alias::new("UUID"))
                    .primary_key()
                    .extra("DEFAULT uuidv7()"),
            )
            .col(
                ColumnDef::new(AcquisitionJobs::SourceBindingId)
                    .custom(Alias::new("UUID"))
                    .not_null(),
            )
            .col(
                ColumnDef::new(AcquisitionJobs::SourceIdentifier)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(AcquisitionJobs::AcquisitionMode)
                    .text()
                    .not_null()
                    .check(Expr::cust(acquisition_mode_check())),
            )
            .col(
                ColumnDef::new(AcquisitionJobs::InputShape)
                    .text()
                    .not_null()
                    .check(Expr::cust(input_shape_check())),
            )
            .col(
                ColumnDef::new(AcquisitionJobs::ParserBindingId)
                    .custom(Alias::new("UUID")),
            )
            .col(
                ColumnDef::new(AcquisitionJobs::MaterialFormatHint)
                    .text(),
            )
            .col(
                ColumnDef::new(AcquisitionJobs::TimingPolicy)
                    .json_binary()
                    .not_null()
                    .default(Expr::cust("'{}'::jsonb")),
            )
            .col(
                ColumnDef::new(AcquisitionJobs::RawMaterialPolicy)
                    .json_binary()
                    .not_null()
                    .default(Expr::cust("'{}'::jsonb")),
            )
            .col(
                ColumnDef::new(AcquisitionJobs::CursorState)
                    .json_binary()
                    .not_null()
                    .default(Expr::cust("'{}'::jsonb")),
            )
            .col(
                ColumnDef::new(AcquisitionJobs::Status)
                    .text()
                    .not_null()
                    .default(acquisition_job_status::PENDING)
                    .check(Expr::cust(acquisition_job_status_check())),
            )
            .col(
                ColumnDef::new(AcquisitionJobs::Attempts)
                    .integer()
                    .not_null()
                    .default(0),
            )
            .col(
                ColumnDef::new(AcquisitionJobs::LastError)
                    .text(),
            )
            .col(
                ColumnDef::new(AcquisitionJobs::StartedAt)
                    .timestamp_with_time_zone(),
            )
            .col(
                ColumnDef::new(AcquisitionJobs::CompletedAt)
                    .timestamp_with_time_zone(),
            )
            .col(
                ColumnDef::new(AcquisitionJobs::MaterialId)
                    .custom(Alias::new("UUID")),
            )
            .col(
                ColumnDef::new(AcquisitionJobs::MaterialStagedAt)
                    .timestamp_with_time_zone(),
            )
            .col(
                ColumnDef::new(AcquisitionJobs::CreatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(
                ColumnDef::new(AcquisitionJobs::UpdatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .foreign_key(
                ForeignKey::create()
                    .name("fk_acquisition_jobs_source_binding")
                    .from(AcquisitionJobs::Table, AcquisitionJobs::SourceBindingId)
                    .to(
                        (Alias::new("raw"), Alias::new("source_bindings")),
                        Alias::new("id"),
                    )
                    .on_delete(ForeignKeyAction::NoAction)
                    .on_update(ForeignKeyAction::NoAction),
            )
            .to_owned()
    }

    #[must_use]
    pub fn create_indexes() -> Vec<IndexCreateStatement> {
        vec![
            Index::create()
                .if_not_exists()
                .name("ix_acquisition_jobs_binding_id")
                .table(Self::table_iden())
                .col(AcquisitionJobs::SourceBindingId)
                .to_owned(),
            Index::create()
                .if_not_exists()
                .name("ix_acquisition_jobs_status")
                .table(Self::table_iden())
                .col(AcquisitionJobs::Status)
                .to_owned(),
            Index::create()
                .if_not_exists()
                .name("ix_acquisition_jobs_mode")
                .table(Self::table_iden())
                .col(AcquisitionJobs::AcquisitionMode)
                .to_owned(),
            Index::create()
                .if_not_exists()
                .name("ix_acquisition_jobs_created")
                .table(Self::table_iden())
                .col((AcquisitionJobs::CreatedAt, IndexOrder::Desc))
                .to_owned(),
            Index::create()
                .if_not_exists()
                .name("ix_acquisition_jobs_material_id")
                .table(Self::table_iden())
                .col(AcquisitionJobs::MaterialId)
                .to_owned(),
        ]
    }
}
