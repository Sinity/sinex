//! The Canonical Database Schema for Processor State and Operations.
//!
//! This module defines the tables that manage the state and lifecycle of the
//! system's distributed agents (satellites). It includes schemas for:
//! - Tracking processor progress (`processor_checkpoints`).
//! - Auditing high-level system operations (`operations_log`).
//! - Coordinating leadership and instance discovery (`satellite_instances`, etc.).

use crate::schema::{Events, TableDef};
use crate::ulid::Ulid;
use chrono::{DateTime, Utc};
use sea_orm_migration::prelude::*;
use serde_json::Value as JsonValue;
use sqlx::FromRow;

// =============================================================================
// I. OPERATIONAL STATE
// =============================================================================

/// **Table: `core.processor_checkpoints`**
///
/// Stores the progress of all stateful processors. This is the single source of
/// truth for where each satellite should resume processing after a restart,
/// ensuring at-least-once processing semantics.
#[derive(Iden, Copy, Clone)]
pub enum ProcessorCheckpoints {
    Table,
    Id,
    ProcessorName,
    ConsumerGroup,
    ConsumerName,
    LastProcessedId,
    ProcessedCount,
    CheckpointData,
    CheckpointVersion,
    CreatedAt,
    LastActivity,
    UpdatedAt,
}

impl TableDef for ProcessorCheckpoints {
    fn table_name() -> &'static str {
        "processor_checkpoints"
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
pub struct CheckpointRecord {
    pub id: Ulid,
    pub processor_name: String,
    pub consumer_group: String,
    pub consumer_name: String,
    pub last_processed_id: Option<Ulid>,
    pub processed_count: i64,
    pub checkpoint_data: Option<JsonValue>,
    pub checkpoint_version: i32,
    pub created_at: DateTime<Utc>,
    pub last_activity: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl ProcessorCheckpoints {
    pub fn create_table_statement() -> TableCreateStatement {
        Table::create()
            .table(Self::table_iden())
            .if_not_exists()
            .col(
                ColumnDef::new(ProcessorCheckpoints::Id)
                    .custom(Alias::new("ULID"))
                    .primary_key()
                    .extra("DEFAULT gen_ulid()"),
            )
            .col(
                ColumnDef::new(ProcessorCheckpoints::ProcessorName)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(ProcessorCheckpoints::ConsumerGroup)
                    .text()
                    .not_null()
                    .default("'default'"),
            )
            .col(
                ColumnDef::new(ProcessorCheckpoints::ConsumerName)
                    .text()
                    .not_null()
                    .default("'default'"),
            )
            .col(ColumnDef::new(ProcessorCheckpoints::LastProcessedId).custom(Alias::new("ULID")))
            .col(
                ColumnDef::new(ProcessorCheckpoints::ProcessedCount)
                    .big_integer()
                    .not_null()
                    .default(0),
            )
            .col(ColumnDef::new(ProcessorCheckpoints::CheckpointData).json_binary())
            .col(
                ColumnDef::new(ProcessorCheckpoints::CheckpointVersion)
                    .integer()
                    .not_null()
                    .default(1),
            )
            .col(
                ColumnDef::new(ProcessorCheckpoints::CreatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(
                ColumnDef::new(ProcessorCheckpoints::LastActivity)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(
                ColumnDef::new(ProcessorCheckpoints::UpdatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .foreign_key(
                ForeignKey::create()
                    .from(Self::table_iden(), ProcessorCheckpoints::LastProcessedId)
                    .to(Events::table_iden(), Alias::new("id"))
                    .on_delete(ForeignKeyAction::SetNull),
            )
            .to_owned()
    }

    pub fn create_indexes() -> Vec<IndexCreateStatement> {
        vec![Index::create()
            .name("uk_processor_consumer")
            .table(Self::table_iden())
            .col(ProcessorCheckpoints::ProcessorName)
            .col(ProcessorCheckpoints::ConsumerGroup)
            .col(ProcessorCheckpoints::ConsumerName)
            .unique()
            .to_owned()]
    }

    /// Creates a trigger to update the updated_at column
    pub fn create_updated_at_trigger_sql() -> String {
        format!(
            r#"
            DROP TRIGGER IF EXISTS trg_processor_checkpoints_updated_at ON {}.{};
            CREATE TRIGGER trg_processor_checkpoints_updated_at
            BEFORE UPDATE ON {}.{}
            FOR EACH ROW EXECUTE FUNCTION public.set_current_timestamp_updated_at();
            "#,
            Self::schema_name(),
            Self::table_name(),
            Self::schema_name(),
            Self::table_name()
        )
    }
}

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
                        "result_status IN ('success', 'failure', 'partial', 'running')",
                    )),
            )
            .col(ColumnDef::new(OperationsLog::ResultMessage).text())
            .col(ColumnDef::new(OperationsLog::PreviewSummary).json_binary()) // Output of replay planner
            .col(ColumnDef::new(OperationsLog::DurationMs).integer())
            .to_owned()
    }
}

// =============================================================================
// II. SATELLITE COORDINATION
// -- These tables provide the backend for distributed leadership election and handoff.
// =============================================================================

/// **Table: `core.satellite_instances`**
///
/// A registry of all active satellite instances, enabling service discovery
/// and version-aware leadership election. This is the source of truth for
/// what processors are currently running in the constellation.
#[derive(Iden, Copy, Clone)]
pub enum SatelliteInstances {
    Table,
    Id,
    ServiceName,
    InstanceId,
    Version,
    StartTime,
    LastHeartbeat,
    HostName,
    Metadata,
}

impl TableDef for SatelliteInstances {
    fn table_name() -> &'static str {
        "satellite_instances"
    }
    fn schema_name() -> &'static str {
        "core"
    }
    fn primary_key() -> &'static str {
        "id"
    }
}

impl SatelliteInstances {
    pub fn create_table_statement() -> TableCreateStatement {
        Table::create()
            .table(Self::table_iden())
            .if_not_exists()
            .col(
                ColumnDef::new(SatelliteInstances::Id)
                    .custom(Alias::new("ULID"))
                    .primary_key()
                    .extra("DEFAULT gen_ulid()"),
            )
            .col(
                ColumnDef::new(SatelliteInstances::ServiceName)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(SatelliteInstances::InstanceId)
                    .text()
                    .not_null()
                    .unique_key(),
            )
            .col(
                ColumnDef::new(SatelliteInstances::Version)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(SatelliteInstances::StartTime)
                    .timestamp_with_time_zone()
                    .not_null(),
            )
            .col(
                ColumnDef::new(SatelliteInstances::LastHeartbeat)
                    .timestamp_with_time_zone()
                    .not_null(),
            )
            .col(
                ColumnDef::new(SatelliteInstances::HostName)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(SatelliteInstances::Metadata)
                    .json_binary()
                    .not_null()
                    .default(Expr::cust("'{}'::jsonb")),
            )
            .to_owned()
    }

    pub fn create_indexes_sql() -> &'static str {
        r#"
        CREATE INDEX IF NOT EXISTS idx_satellite_instances_service_version
            ON core.satellite_instances(service_name, version DESC, start_time ASC);
        "#
    }
}

/// **Table: `core.service_leadership`**
///
/// Tracks the current leader for each service, enforced by PostgreSQL advisory locks.
/// The `instance_id` here is a foreign key to `satellite_instances`, providing a
/// direct link to the full metadata of the current leader.
#[derive(Iden, Copy, Clone)]
pub enum ServiceLeadership {
    Table,
    ServiceName,
    InstanceId,
    AcquiredAt,
    LastHeartbeat,
    Version,
}

impl TableDef for ServiceLeadership {
    fn table_name() -> &'static str {
        "service_leadership"
    }
    fn schema_name() -> &'static str {
        "core"
    }
    fn primary_key() -> &'static str {
        "service_name"
    }
}

impl ServiceLeadership {
    pub fn create_table_statement() -> TableCreateStatement {
        Table::create()
            .table(Self::table_iden())
            .if_not_exists()
            .col(
                ColumnDef::new(ServiceLeadership::ServiceName)
                    .text()
                    .primary_key(),
            )
            .col(
                ColumnDef::new(ServiceLeadership::InstanceId)
                    .text()
                    .not_null()
                    .unique_key(),
            )
            .col(
                ColumnDef::new(ServiceLeadership::AcquiredAt)
                    .timestamp_with_time_zone()
                    .not_null(),
            )
            .col(
                ColumnDef::new(ServiceLeadership::LastHeartbeat)
                    .timestamp_with_time_zone()
                    .not_null(),
            )
            .col(ColumnDef::new(ServiceLeadership::Version).text().not_null())
            .foreign_key(
                ForeignKey::create()
                    .from(Self::table_iden(), ServiceLeadership::InstanceId)
                    .to(
                        SatelliteInstances::table_iden(),
                        SatelliteInstances::InstanceId,
                    )
                    .on_delete(ForeignKeyAction::Cascade),
            )
            .to_owned()
    }

    pub fn create_indexes_sql() -> &'static str {
        r#"
        CREATE INDEX IF NOT EXISTS idx_service_leadership_heartbeat
            ON core.service_leadership(last_heartbeat);
        "#
    }
}

/// **Table: `core.satellite_signals`**
///
/// Transient signalling bus used for graceful leadership handoff and failure
/// coordination between satellite instances. Each record represents a signal
/// addressed to a specific instance (or broadcast via `ALL`).
#[derive(Iden, Copy, Clone)]
pub enum SatelliteSignals {
    Table,
    Id,
    TargetInstance,
    SignalType,
    Message,
    Payload,
    CreatedAt,
    ProcessedAt,
    ProcessedBy,
}

impl TableDef for SatelliteSignals {
    fn table_name() -> &'static str {
        "satellite_signals"
    }
    fn schema_name() -> &'static str {
        "core"
    }
    fn primary_key() -> &'static str {
        "id"
    }
}

impl SatelliteSignals {
    pub fn create_table_statement() -> TableCreateStatement {
        Table::create()
            .table(Self::table_iden())
            .if_not_exists()
            .col(
                ColumnDef::new(SatelliteSignals::Id)
                    .integer()
                    .not_null()
                    .primary_key()
                    .extra("GENERATED ALWAYS AS IDENTITY"),
            )
            .col(
                ColumnDef::new(SatelliteSignals::TargetInstance)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(SatelliteSignals::SignalType)
                    .text()
                    .not_null(),
            )
            .col(ColumnDef::new(SatelliteSignals::Message).text())
            .col(
                ColumnDef::new(SatelliteSignals::Payload)
                    .json_binary()
                    .default(Expr::cust("'{}'::jsonb")),
            )
            .col(
                ColumnDef::new(SatelliteSignals::CreatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::cust("NOW()")),
            )
            .col(ColumnDef::new(SatelliteSignals::ProcessedAt).timestamp_with_time_zone())
            .col(ColumnDef::new(SatelliteSignals::ProcessedBy).uuid())
            .to_owned()
    }

    pub fn create_indexes_sql() -> &'static str {
        r#"
        CREATE INDEX IF NOT EXISTS idx_satellite_signals_target_unprocessed
            ON core.satellite_signals(target_instance, created_at)
            WHERE processed_at IS NULL;
        "#
    }
}
