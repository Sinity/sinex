//! The canonical database schema for runtime operations and coordination.
//!
//! This module defines the tables that manage the state and lifecycle of the
//! system's runtime modules. It includes schemas for:
//! - Auditing high-level system operations (`operations_log`).
//! - Coordinating leadership and instance discovery (`node_instances`, etc.).

use crate::TableDef;
use sea_query::{
    Alias, ColumnDef, Expr, Iden, Index, IndexCreateStatement, Table, TableCreateStatement,
};

// =============================================================================
// I. OPERATIONAL STATE
// =============================================================================

/// **Table: `core.operations_log`**
///
/// The audit trail of high-level, intentional system operations (e.g., replays,
/// archival, restore, and tombstone jobs). This provides "intent provenance" -
/// the *why* behind data changes.
#[derive(Iden, Copy, Clone)]
pub enum OperationsLog {
    Table,
    Id,
    OperationType,
    Operator,
    Scope,
    ScopeWindow,
    ResultStatus,
    ResultMessage,
    PreviewSummary,
    DurationMs,
}

/// **Table: `core.email_provider_state`**
///
/// Durable current-state projection for operator-facing mailbox provider
/// health, cursors, and sync debt. `operations_log` remains the audit trail;
/// this table keeps the latest provider outcome per account/mailbox scope.
#[derive(Iden, Copy, Clone)]
pub enum EmailProviderState {
    Table,
    Id,
    SourceId,
    ModeId,
    Provider,
    AccountBindingRef,
    MailboxScope,
    OperationId,
    ResultStatus,
    AuthState,
    NetworkState,
    SyncState,
    RateLimitState,
    RuntimeStateRef,
    CoverageRef,
    DebtRef,
    FailureClass,
    RequiredAction,
    RetryAfterSecs,
    ReconnectState,
    CursorKind,
    CursorValue,
    ContinuityState,
    ProviderRuntime,
    ProviderCursor,
    ProviderFailure,
    ObservedAt,
    UpdatedAt,
}

/// **Table: `core.email_mailbox_projection`**
///
/// Durable metadata projection for mailbox message/thread/attachment events.
/// It intentionally stores only operator-safe metadata and materialization
/// debt, not message body text or attachment bytes.
#[derive(Iden, Copy, Clone)]
pub enum EmailMailboxProjection {
    Table,
    Id,
    SourceId,
    ModeId,
    MessageKey,
    MessageId,
    ThreadKey,
    ThreadRootMessageId,
    Direction,
    Folder,
    MailboxFormat,
    SourceFile,
    RawMaterialId,
    MboxByteStart,
    MboxByteEnd,
    Subject,
    FromAddresses,
    ToAddresses,
    BodyBytes,
    AttachmentCount,
    AttachmentObservedCount,
    AttachmentPolicyRefs,
    LastMessageEventId,
    LastThreadEventId,
    LastAttachmentEventId,
    LastObservedAt,
    UpdatedAt,
}

impl TableDef for EmailMailboxProjection {
    fn table_name() -> &'static str {
        "email_mailbox_projection"
    }
    fn schema_name() -> &'static str {
        "core"
    }
    fn primary_key() -> &'static str {
        "id"
    }
}

impl EmailMailboxProjection {
    #[must_use]
    pub fn create_table_statement() -> TableCreateStatement {
        Table::create()
            .table(Self::table_iden())
            .if_not_exists()
            .col(
                ColumnDef::new(EmailMailboxProjection::Id)
                    .custom(Alias::new("UUID"))
                    .primary_key()
                    .extra("DEFAULT uuidv7()"),
            )
            .col(
                ColumnDef::new(EmailMailboxProjection::SourceId)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(EmailMailboxProjection::ModeId)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(EmailMailboxProjection::MessageKey)
                    .text()
                    .not_null(),
            )
            .col(ColumnDef::new(EmailMailboxProjection::MessageId).text())
            .col(ColumnDef::new(EmailMailboxProjection::ThreadKey).text())
            .col(ColumnDef::new(EmailMailboxProjection::ThreadRootMessageId).text())
            .col(ColumnDef::new(EmailMailboxProjection::Direction).text())
            .col(ColumnDef::new(EmailMailboxProjection::Folder).text())
            .col(ColumnDef::new(EmailMailboxProjection::MailboxFormat).text())
            .col(ColumnDef::new(EmailMailboxProjection::SourceFile).text())
            .col(ColumnDef::new(EmailMailboxProjection::RawMaterialId).text())
            .col(ColumnDef::new(EmailMailboxProjection::MboxByteStart).big_integer())
            .col(ColumnDef::new(EmailMailboxProjection::MboxByteEnd).big_integer())
            .col(ColumnDef::new(EmailMailboxProjection::Subject).text())
            .col(
                ColumnDef::new(EmailMailboxProjection::FromAddresses)
                    .json_binary()
                    .not_null()
                    .default(Expr::cust("'[]'::jsonb")),
            )
            .col(
                ColumnDef::new(EmailMailboxProjection::ToAddresses)
                    .json_binary()
                    .not_null()
                    .default(Expr::cust("'[]'::jsonb")),
            )
            .col(
                ColumnDef::new(EmailMailboxProjection::BodyBytes)
                    .big_integer()
                    .not_null()
                    .default(0),
            )
            .col(
                ColumnDef::new(EmailMailboxProjection::AttachmentCount)
                    .integer()
                    .not_null()
                    .default(0),
            )
            .col(
                ColumnDef::new(EmailMailboxProjection::AttachmentObservedCount)
                    .integer()
                    .not_null()
                    .default(0),
            )
            .col(
                ColumnDef::new(EmailMailboxProjection::AttachmentPolicyRefs)
                    .json_binary()
                    .not_null()
                    .default(Expr::cust("'[]'::jsonb")),
            )
            .col(
                ColumnDef::new(EmailMailboxProjection::LastMessageEventId)
                    .custom(Alias::new("UUID")),
            )
            .col(
                ColumnDef::new(EmailMailboxProjection::LastThreadEventId)
                    .custom(Alias::new("UUID")),
            )
            .col(
                ColumnDef::new(EmailMailboxProjection::LastAttachmentEventId)
                    .custom(Alias::new("UUID")),
            )
            .col(
                ColumnDef::new(EmailMailboxProjection::LastObservedAt)
                    .custom(Alias::new("TIMESTAMPTZ"))
                    .not_null()
                    .default(Expr::cust("NOW()")),
            )
            .col(
                ColumnDef::new(EmailMailboxProjection::UpdatedAt)
                    .custom(Alias::new("TIMESTAMPTZ"))
                    .not_null()
                    .default(Expr::cust("NOW()")),
            )
            .to_owned()
    }

    #[must_use]
    pub fn create_indexes() -> Vec<IndexCreateStatement> {
        vec![
            Index::create()
                .if_not_exists()
                .name("ux_email_mailbox_projection_current_message")
                .table(Self::table_iden())
                .col(EmailMailboxProjection::SourceId)
                .col(EmailMailboxProjection::ModeId)
                .col(EmailMailboxProjection::MessageKey)
                .unique()
                .to_owned(),
            Index::create()
                .if_not_exists()
                .name("ix_email_mailbox_projection_source_mode")
                .table(Self::table_iden())
                .col(EmailMailboxProjection::SourceId)
                .col(EmailMailboxProjection::ModeId)
                .to_owned(),
            Index::create()
                .if_not_exists()
                .name("ix_email_mailbox_projection_thread")
                .table(Self::table_iden())
                .col(EmailMailboxProjection::ThreadKey)
                .to_owned(),
        ]
    }

    #[must_use]
    pub fn create_updated_at_trigger_sql() -> String {
        "DROP TRIGGER IF EXISTS trg_email_mailbox_projection_updated_at ON core.email_mailbox_projection;
CREATE TRIGGER trg_email_mailbox_projection_updated_at
    BEFORE UPDATE ON core.email_mailbox_projection
    FOR EACH ROW
    EXECUTE FUNCTION public.set_current_timestamp_updated_at();"
            .to_string()
    }
}

impl TableDef for EmailProviderState {
    fn table_name() -> &'static str {
        "email_provider_state"
    }
    fn schema_name() -> &'static str {
        "core"
    }
    fn primary_key() -> &'static str {
        "id"
    }
}

impl EmailProviderState {
    #[must_use]
    pub fn create_table_statement() -> TableCreateStatement {
        Table::create()
            .table(Self::table_iden())
            .if_not_exists()
            .col(
                ColumnDef::new(EmailProviderState::Id)
                    .custom(Alias::new("UUID"))
                    .primary_key()
                    .extra("DEFAULT uuidv7()"),
            )
            .col(
                ColumnDef::new(EmailProviderState::SourceId)
                    .text()
                    .not_null(),
            )
            .col(ColumnDef::new(EmailProviderState::ModeId).text().not_null())
            .col(
                ColumnDef::new(EmailProviderState::Provider)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(EmailProviderState::AccountBindingRef)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(EmailProviderState::MailboxScope)
                    .text()
                    .not_null()
                    .default("default"),
            )
            .col(
                ColumnDef::new(EmailProviderState::OperationId)
                    .custom(Alias::new("UUID"))
                    .not_null(),
            )
            .col(
                ColumnDef::new(EmailProviderState::ResultStatus)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(EmailProviderState::AuthState)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(EmailProviderState::NetworkState)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(EmailProviderState::SyncState)
                    .text()
                    .not_null(),
            )
            .col(ColumnDef::new(EmailProviderState::RateLimitState).text())
            .col(
                ColumnDef::new(EmailProviderState::RuntimeStateRef)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(EmailProviderState::CoverageRef)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(EmailProviderState::DebtRef)
                    .text()
                    .not_null(),
            )
            .col(ColumnDef::new(EmailProviderState::FailureClass).text())
            .col(ColumnDef::new(EmailProviderState::RequiredAction).text())
            .col(ColumnDef::new(EmailProviderState::RetryAfterSecs).integer())
            .col(ColumnDef::new(EmailProviderState::ReconnectState).text())
            .col(ColumnDef::new(EmailProviderState::CursorKind).text())
            .col(ColumnDef::new(EmailProviderState::CursorValue).text())
            .col(ColumnDef::new(EmailProviderState::ContinuityState).text())
            .col(
                ColumnDef::new(EmailProviderState::ProviderRuntime)
                    .json_binary()
                    .not_null()
                    .default(Expr::cust("'{}'::jsonb")),
            )
            .col(ColumnDef::new(EmailProviderState::ProviderCursor).json_binary())
            .col(ColumnDef::new(EmailProviderState::ProviderFailure).json_binary())
            .col(
                ColumnDef::new(EmailProviderState::ObservedAt)
                    .custom(Alias::new("TIMESTAMPTZ"))
                    .not_null()
                    .default(Expr::cust("NOW()")),
            )
            .col(
                ColumnDef::new(EmailProviderState::UpdatedAt)
                    .custom(Alias::new("TIMESTAMPTZ"))
                    .not_null()
                    .default(Expr::cust("NOW()")),
            )
            .to_owned()
    }

    #[must_use]
    pub fn create_indexes() -> Vec<IndexCreateStatement> {
        vec![
            Index::create()
                .if_not_exists()
                .name("ux_email_provider_state_current_scope")
                .table(Self::table_iden())
                .col(EmailProviderState::SourceId)
                .col(EmailProviderState::ModeId)
                .col(EmailProviderState::Provider)
                .col(EmailProviderState::AccountBindingRef)
                .col(EmailProviderState::MailboxScope)
                .unique()
                .to_owned(),
            Index::create()
                .if_not_exists()
                .name("ix_email_provider_state_source_mode")
                .table(Self::table_iden())
                .col(EmailProviderState::SourceId)
                .col(EmailProviderState::ModeId)
                .to_owned(),
            Index::create()
                .if_not_exists()
                .name("ix_email_provider_state_operation")
                .table(Self::table_iden())
                .col(EmailProviderState::OperationId)
                .to_owned(),
        ]
    }

    #[must_use]
    pub fn create_updated_at_trigger_sql() -> String {
        "DROP TRIGGER IF EXISTS trg_email_provider_state_updated_at ON core.email_provider_state;
CREATE TRIGGER trg_email_provider_state_updated_at
    BEFORE UPDATE ON core.email_provider_state
    FOR EACH ROW
    EXECUTE FUNCTION public.set_current_timestamp_updated_at();"
            .to_string()
    }
}

impl TableDef for OperationsLog {
    fn table_name() -> &'static str {
        "operations_log"
    }
    fn schema_name() -> &'static str {
        "core"
    }
    fn primary_key() -> &'static str {
        "id"
    }
}

impl OperationsLog {
    #[must_use]
    pub fn create_table_statement() -> TableCreateStatement {
        Table::create()
            .table(Self::table_iden())
            .if_not_exists()
            .col(
                ColumnDef::new(OperationsLog::Id)
                    .custom(Alias::new("UUID"))
                    .primary_key()
                    .extra("DEFAULT uuidv7()"),
            )
            .col(
                ColumnDef::new(OperationsLog::OperationType)
                    .text()
                    .not_null()
                    .check(Expr::cust("operation_type ~ '^[a-z][a-z0-9_.-]*$'")),
            )
            .col(ColumnDef::new(OperationsLog::Operator).text().not_null())
            .col(ColumnDef::new(OperationsLog::Scope).json_binary()) // Parameters of the operation
            .col(ColumnDef::new(OperationsLog::ScopeWindow).custom(Alias::new("tstzrange")))
            // The CHECK constraint on result_status is converged by the
            // schema-apply engine from the `OperationStatus` enum's
            // `#[derive(DbCheck)]` spec (issue #1236). Do NOT add an inline
            // `.check(...)` here — it would survive only on first table
            // creation and prevent the apply engine from owning the rename.
            .col(
                ColumnDef::new(OperationsLog::ResultStatus)
                    .text()
                    .not_null(),
            )
            .col(ColumnDef::new(OperationsLog::ResultMessage).text())
            .col(ColumnDef::new(OperationsLog::PreviewSummary).json_binary()) // Output of replay planner
            .col(ColumnDef::new(OperationsLog::DurationMs).integer())
            .to_owned()
    }

    /// Generates indexes for `core.operations_log`.
    #[must_use]
    pub fn create_indexes() -> Vec<IndexCreateStatement> {
        vec![
            // B-tree index on (operator, id) for queries filtering by operator and
            // ordering by recency.
            Index::create()
                .if_not_exists()
                .name("ix_operations_log_operator_id")
                .table(Self::table_iden())
                .col(OperationsLog::Operator)
                .col(OperationsLog::Id)
                .to_owned(),
            // B-tree index on (result_status, id) for queries filtering by status
            // and ordering by recency.
            Index::create()
                .if_not_exists()
                .name("ix_operations_log_status_id")
                .table(Self::table_iden())
                .col(OperationsLog::ResultStatus)
                .col(OperationsLog::Id)
                .to_owned(),
            // B-tree index on (operation_type, result_status) for queries by type+status.
            Index::create()
                .if_not_exists()
                .name("ix_operations_log_type_status")
                .table(Self::table_iden())
                .col(OperationsLog::OperationType)
                .col(OperationsLog::ResultStatus)
                .to_owned(),
        ]
    }

    /// Generates raw SQL for GIN indexes (PostgreSQL-specific feature).
    #[must_use]
    pub fn create_gin_indexes_sql() -> Vec<String> {
        vec![format!(
            "CREATE INDEX IF NOT EXISTS ix_operations_log_scope_gin \
             ON {}.{} USING GIN (scope)",
            Self::schema_name(),
            Self::table_name()
        )]
    }
}

// =============================================================================
// II. NODE COORDINATION
// -- These tables provide the backend for distributed leadership election and handoff.
// =============================================================================
