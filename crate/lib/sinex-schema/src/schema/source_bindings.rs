//! Source binding catalog schema.
//!
//! Source bindings are durable declarations of source-material acquisition intent.
//! They answer: what source, how to locate it, what policy applies, and when to acquire.
//!
//! This module defines:
//! - `raw.source_bindings` — the binding catalog
//! - `raw.source_binding_resolution_log` — audit trail for resolution attempts

use crate::primitives::{Timestamp, Uuid};
use crate::schema::TableDef;
use sea_query::{
    Alias, ColumnDef, Expr, ForeignKey, ForeignKeyAction, Iden, Index, IndexCreateStatement,
    IndexOrder, Table, TableCreateStatement,
};
use serde_json::Value as JsonValue;
use sqlx::FromRow;

// =============================================================================
// raw.source_bindings
// =============================================================================

/// **Table: `raw.source_bindings`**
///
/// Durable declarations of source-material acquisition intent. A binding is not
/// source material and not a parser job — it says "here is where source material
/// comes from and what to do with it."
#[derive(Iden, Copy, Clone)]
pub enum SourceBindings {
    Table,
    Id,
    Name,
    SourceFamily,
    BindingMode,
    ResolverPreset,
    Locator,
    InputShapeKind,
    MaterialFormatHint,
    ParserId,
    SourceUnitId,
    PrivacyPolicyId,
    RawMaterialPolicy,
    WatchPolicy,
    HostScope,
    UserScope,
    Enabled,
    Status,
    LastResolved,
    LastError,
    CreatedAt,
    UpdatedAt,
}

/// **Table: `raw.source_binding_resolution_log`**
///
/// Audit trail recording every resolution attempt for a binding: what candidates
/// were found, which was selected, and whether resolution succeeded or failed.
#[derive(Iden, Copy, Clone)]
pub enum SourceBindingResolutionLog {
    Table,
    Id,
    SourceBindingId,
    ResolvedAt,
    CandidateCount,
    SelectedLocator,
    Evidence,
    Status,
    ErrorSummary,
}

impl TableDef for SourceBindings {
    fn table_name() -> &'static str {
        "source_bindings"
    }
    fn schema_name() -> &'static str {
        "raw"
    }
    fn primary_key() -> &'static str {
        "id"
    }
}

impl TableDef for SourceBindingResolutionLog {
    fn table_name() -> &'static str {
        "source_binding_resolution_log"
    }
    fn schema_name() -> &'static str {
        "raw"
    }
    fn primary_key() -> &'static str {
        "id"
    }
}

/// Rust struct for a `raw.source_bindings` row.
#[derive(Debug, FromRow, serde::Serialize, serde::Deserialize)]
pub struct SourceBindingRecord {
    pub id: Uuid,
    pub name: String,
    pub source_family: String,
    pub binding_mode: String,
    pub resolver_preset: Option<String>,
    pub locator: JsonValue,
    pub input_shape_kind: String,
    pub material_format_hint: Option<String>,
    pub parser_id: Option<String>,
    pub source_unit_id: Option<String>,
    pub privacy_policy_id: String,
    pub raw_material_policy: JsonValue,
    pub watch_policy: JsonValue,
    pub host_scope: Option<String>,
    pub user_scope: Option<String>,
    pub enabled: bool,
    pub status: String,
    pub last_resolved: Option<JsonValue>,
    pub last_error: Option<String>,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

/// Rust struct for a `raw.source_binding_resolution_log` row.
#[derive(Debug, FromRow, serde::Serialize, serde::Deserialize)]
pub struct SourceBindingResolutionLogRecord {
    pub id: Uuid,
    pub source_binding_id: Uuid,
    pub resolved_at: Timestamp,
    pub candidate_count: i32,
    pub selected_locator: Option<JsonValue>,
    pub evidence: JsonValue,
    pub status: String,
    pub error_summary: Option<String>,
}

// ── Canonical constant modules for column values ──────────────────────────

/// Canonical source binding status values.
pub mod binding_status {
    pub const ENABLED: &str = "enabled";
    pub const DISABLED: &str = "disabled";
    pub const MISSING: &str = "missing";
    pub const PERMISSION_DENIED: &str = "permission_denied";
    pub const POLICY_BLOCKED: &str = "policy_blocked";
    pub const ERROR: &str = "error";

    pub const ALL: &[&str] = &[
        ENABLED,
        DISABLED,
        MISSING,
        PERMISSION_DENIED,
        POLICY_BLOCKED,
        ERROR,
    ];
}

/// Canonical source binding mode values.
pub mod binding_mode {
    pub const STAGE_ONLY: &str = "stage_only";
    pub const STAGE_THEN_PARSE: &str = "stage_then_parse";
    pub const LIVE_CAPTURE: &str = "live_capture";
    pub const EXTERNAL_PRODUCER: &str = "external_producer";

    pub const ALL: &[&str] = &[STAGE_ONLY, STAGE_THEN_PARSE, LIVE_CAPTURE, EXTERNAL_PRODUCER];
}

/// Canonical material capture class values for privacy policy.
pub mod material_capture_class {
    /// Raw bytes may be stored as plaintext.
    pub const ALLOWED_PLAINTEXT: &str = "allowed_plaintext";
    /// Only metadata is stored; raw bytes are discarded.
    pub const METADATA_ONLY: &str = "metadata_only";
    /// Raw bytes are encrypted before persistence.
    pub const ENCRYPTED_MATERIAL: &str = "encrypted_material";
    /// Raw bytes are copied to a restricted store but not parsed until approved.
    pub const LOCAL_QUARANTINE: &str = "local_quarantine";
    /// Material is not accepted at all.
    pub const SUPPRESSED: &str = "suppressed";
    /// Material requires explicit one-shot operator confirmation before staging.
    pub const EXPLICIT_IMPORT: &str = "explicit_import";

    pub const ALL: &[&str] = &[
        ALLOWED_PLAINTEXT,
        METADATA_ONLY,
        ENCRYPTED_MATERIAL,
        LOCAL_QUARANTINE,
        SUPPRESSED,
        EXPLICIT_IMPORT,
    ];
}

/// Canonical input shape kinds.
pub mod input_shape_kind {
    pub const FILE: &str = "file";
    pub const DIRECTORY: &str = "directory";
    pub const FILE_DROP: &str = "file_drop";
    pub const APPEND_ONLY: &str = "append_only";
    pub const SQLITE_DB: &str = "sqlite_db";
    pub const STREAM: &str = "stream";

    pub const ALL: &[&str] = &[FILE, DIRECTORY, FILE_DROP, APPEND_ONLY, SQLITE_DB, STREAM];
}

// ── SQL CHECK constraint helpers ──────────────────────────────────────────

fn check_in(values: &[&str]) -> String {
    let quoted: Vec<String> = values.iter().map(|v| format!("'{v}'")).collect();
    format!("IN ({})", quoted.join(", "))
}

fn source_binding_status_check() -> String {
    let quoted: Vec<String> = binding_status::ALL.iter().map(|v| format!("'{v}'")).collect();
    format!("status IN ({})", quoted.join(", "))
}

fn source_binding_mode_check() -> String {
    let quoted: Vec<String> = binding_mode::ALL.iter().map(|v| format!("'{v}'")).collect();
    format!("binding_mode IN ({})", quoted.join(", "))
}

fn material_capture_class_check() -> String {
    let quoted: Vec<String> = material_capture_class::ALL.iter().map(|v| format!("'{v}'")).collect();
    format!("IN ({})", quoted.join(", "))
}

fn input_shape_kind_check() -> String {
    let quoted: Vec<String> = input_shape_kind::ALL.iter().map(|v| format!("'{v}'")).collect();
    format!("input_shape_kind IN ({})", quoted.join(", "))
}

impl SourceBindings {
    #[must_use]
    pub fn create_table_statement() -> TableCreateStatement {
        Table::create()
            .table(Self::table_iden())
            .if_not_exists()
            .col(
                ColumnDef::new(SourceBindings::Id)
                    .custom(Alias::new("UUID"))
                    .primary_key()
                    .extra("DEFAULT uuidv7()"),
            )
            .col(
                ColumnDef::new(SourceBindings::Name)
                    .text()
                    .not_null()
                    .unique_key(),
            )
            .col(
                ColumnDef::new(SourceBindings::SourceFamily)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(SourceBindings::BindingMode)
                    .text()
                    .not_null()
                    .check(Expr::cust(source_binding_mode_check())),
            )
            .col(ColumnDef::new(SourceBindings::ResolverPreset).text())
            .col(
                ColumnDef::new(SourceBindings::Locator)
                    .json_binary()
                    .not_null()
                    .default(Expr::cust("'{}'::jsonb")),
            )
            .col(
                ColumnDef::new(SourceBindings::InputShapeKind)
                    .text()
                    .not_null()
                    .check(Expr::cust(input_shape_kind_check())),
            )
            .col(ColumnDef::new(SourceBindings::MaterialFormatHint).text())
            .col(ColumnDef::new(SourceBindings::ParserId).text())
            .col(ColumnDef::new(SourceBindings::SourceUnitId).text())
            .col(
                ColumnDef::new(SourceBindings::PrivacyPolicyId)
                    .text()
                    .not_null()
                    .default("allowed_plaintext"),
            )
            .col(
                ColumnDef::new(SourceBindings::RawMaterialPolicy)
                    .json_binary()
                    .not_null()
                    .default(Expr::cust("'{}'::jsonb")),
            )
            .col(
                ColumnDef::new(SourceBindings::WatchPolicy)
                    .json_binary()
                    .not_null()
                    .default(Expr::cust("'{}'::jsonb")),
            )
            .col(ColumnDef::new(SourceBindings::HostScope).text())
            .col(ColumnDef::new(SourceBindings::UserScope).text())
            .col(
                ColumnDef::new(SourceBindings::Enabled)
                    .boolean()
                    .not_null()
                    .default(true),
            )
            .col(
                ColumnDef::new(SourceBindings::Status)
                    .text()
                    .not_null()
                    .default(binding_status::ENABLED)
                    .check(Expr::cust(source_binding_status_check())),
            )
            .col(ColumnDef::new(SourceBindings::LastResolved).json_binary())
            .col(ColumnDef::new(SourceBindings::LastError).text())
            .col(
                ColumnDef::new(SourceBindings::CreatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(
                ColumnDef::new(SourceBindings::UpdatedAt)
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
                .name("ix_source_bindings_family")
                .table(Self::table_iden())
                .col(SourceBindings::SourceFamily)
                .to_owned(),
            Index::create()
                .if_not_exists()
                .name("ix_source_bindings_mode")
                .table(Self::table_iden())
                .col(SourceBindings::BindingMode)
                .to_owned(),
            Index::create()
                .if_not_exists()
                .name("ix_source_bindings_status")
                .table(Self::table_iden())
                .col(SourceBindings::Status)
                .to_owned(),
            Index::create()
                .if_not_exists()
                .name("ix_source_bindings_created")
                .table(Self::table_iden())
                .col((SourceBindings::CreatedAt, IndexOrder::Desc))
                .to_owned(),
        ]
    }
}

impl SourceBindingResolutionLog {
    #[must_use]
    pub fn create_table_statement() -> TableCreateStatement {
        Table::create()
            .table(Self::table_iden())
            .if_not_exists()
            .col(
                ColumnDef::new(SourceBindingResolutionLog::Id)
                    .custom(Alias::new("UUID"))
                    .primary_key()
                    .extra("DEFAULT uuidv7()"),
            )
            .col(
                ColumnDef::new(SourceBindingResolutionLog::SourceBindingId)
                    .custom(Alias::new("UUID"))
                    .not_null(),
            )
            .col(
                ColumnDef::new(SourceBindingResolutionLog::ResolvedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(
                ColumnDef::new(SourceBindingResolutionLog::CandidateCount)
                    .integer()
                    .not_null()
                    .default(0),
            )
            .col(
                ColumnDef::new(SourceBindingResolutionLog::SelectedLocator)
                    .json_binary(),
            )
            .col(
                ColumnDef::new(SourceBindingResolutionLog::Evidence)
                    .json_binary()
                    .not_null()
                    .default(Expr::cust("'{}'::jsonb")),
            )
            .col(
                ColumnDef::new(SourceBindingResolutionLog::Status)
                    .text()
                    .not_null()
                    .check(Expr::cust(source_binding_status_check())),
            )
            .col(
                ColumnDef::new(SourceBindingResolutionLog::ErrorSummary)
                    .text(),
            )
            .foreign_key(
                ForeignKey::create()
                    .from(
                        Self::table_iden(),
                        SourceBindingResolutionLog::SourceBindingId,
                    )
                    .to(SourceBindings::table_iden(), SourceBindings::Id)
                    .on_delete(ForeignKeyAction::Cascade),
            )
            .to_owned()
    }

    #[must_use]
    pub fn create_indexes() -> Vec<IndexCreateStatement> {
        vec![
            Index::create()
                .if_not_exists()
                .name("ix_source_binding_resolution_binding_id")
                .table(Self::table_iden())
                .col(SourceBindingResolutionLog::SourceBindingId)
                .to_owned(),
            Index::create()
                .if_not_exists()
                .name("ix_source_binding_resolution_resolved_at")
                .table(Self::table_iden())
                .col((
                    SourceBindingResolutionLog::ResolvedAt,
                    IndexOrder::Desc,
                ))
                .to_owned(),
        ]
    }
}
