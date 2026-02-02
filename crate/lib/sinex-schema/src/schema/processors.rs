//! The Canonical Database Schema for Processor State and Operations.
//!
//! This module defines the tables that manage the state and lifecycle of the
//! system's distributed agents (nodes). It includes schemas for:
//! - Auditing high-level system operations (`operations_log`).
//! - Coordinating leadership and instance discovery (`node_instances`, etc.).

use crate::schema::TableDef;
use sea_orm_migration::prelude::*;

// =============================================================================
// I. OPERATIONAL STATE
// =============================================================================

/// **Table: `core.operations_log`**
///
/// The audit trail of high-level, intentional system operations (e.g., replays,
/// archival jobs). This provides "intent provenance" - the *why* behind data changes.
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
                    .custom(Alias::new("ULID"))
                    .primary_key()
                    .extra("DEFAULT gen_ulid()"),
            )
            .col(
                ColumnDef::new(OperationsLog::OperationType)
                    .text()
                    .not_null(),
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
}

// =============================================================================
// II. NODE COORDINATION
// -- These tables provide the backend for distributed leadership election and handoff.
// =============================================================================
