//! The canonical database schema for node operations and coordination.
//!
//! This module defines the tables that manage the state and lifecycle of the
//! system's distributed agents (nodes). It includes schemas for:
//! - Auditing high-level system operations (`operations_log`).
//! - Coordinating leadership and instance discovery (`node_instances`, etc.).

use crate::schema::TableDef;
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
                    .check(Expr::cust(
                        "operation_type ~ '^[a-z][a-z0-9_.-]*$'",
                    )),
            )
            .col(ColumnDef::new(OperationsLog::Operator).text().not_null())
            .col(ColumnDef::new(OperationsLog::Scope).json_binary()) // Parameters of the operation
            .col(ColumnDef::new(OperationsLog::ScopeWindow).custom(Alias::new("tstzrange")))
            .col(
                ColumnDef::new(OperationsLog::ResultStatus)
                    .text()
                    .not_null()
                    .check(Expr::cust(
                        "result_status IN ('success', 'failure', 'partial', 'running', 'cancelled')",
                    )),
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
