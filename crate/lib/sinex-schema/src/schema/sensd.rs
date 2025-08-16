//! The Canonical Database Schema for the `sensd` Acquisition Daemon.
//!
//! This module defines the control plane for `sensd`. It follows a standard
//! controller/operator pattern with two main tables:
//! - `raw.sensor_jobs`: The "Spec". This is the declarative configuration that
//!   defines what `sensd` *should* be doing.
//! - `raw.sensor_states`: The "Status". This is the operational state that
//!   reflects what `sensd` *is currently* doing and what its progress is.
//!
//! The `sensd` daemon's primary responsibility is to reconcile the state of its
//! running sensors to match the desired state defined in `sensor_jobs`.

use crate::schema::TableDef;
use crate::ulid::Ulid;
use chrono::{DateTime, Utc};
use sea_orm_migration::prelude::*;
use serde_json::Value as JsonValue;
use sqlx::FromRow;

// =============================================================================
// The `raw.sensor_jobs` Table (The "Spec")
// =============================================================================

/// **Table: `raw.sensor_jobs`**
///
/// This table contains the declarative configuration for all sensor jobs. Users or
/// other services create or update rows here to instruct `sensd` on what data to
/// acquire.
#[derive(Iden, Copy, Clone)]
pub enum SensorJobs {
    Table,
    Id,
    SensorType,
    TargetUri,
    Config,
    Status,
    Priority,
    UpdatedAt,
}

impl TableDef for SensorJobs {
    fn table_name() -> &'static str {
        "sensor_jobs"
    }
    fn schema_name() -> &'static str {
        "raw"
    }
    fn primary_key() -> &'static str {
        "id"
    }
}

/// The Rust struct representation of a row from `raw.sensor_jobs`.
#[derive(Debug, FromRow)]
pub struct SensorJobRecord {
    pub id: Ulid,
    pub sensor_type: String,
    pub target_uri: String,
    pub config: JsonValue,
    pub status: String,
    pub priority: i32,
    pub updated_at: DateTime<Utc>,
}

impl SensorJobs {
    /// Generates the `CREATE TABLE` statement for `raw.sensor_jobs`.
    pub fn create_table_statement() -> TableCreateStatement {
        Table::create()
            .table(Self::table_iden())
            .if_not_exists()
            .col(
                ColumnDef::new(SensorJobs::Id)
                    .custom(Alias::new("ULID"))
                    .primary_key()
                    .extra("DEFAULT gen_ulid()"),
            )
            .col(ColumnDef::new(SensorJobs::SensorType).text().not_null())
            .col(ColumnDef::new(SensorJobs::TargetUri).text().not_null())
            .col(
                ColumnDef::new(SensorJobs::Config)
                    .json_binary()
                    .not_null()
                    .default(Expr::cust("'{}'::jsonb")),
            )
            .col(
                ColumnDef::new(SensorJobs::Status)
                    .text()
                    .not_null()
                    .default("'active'")
                    .check(Expr::cust("status IN ('active', 'paused', 'retired')")),
            )
            .col(
                ColumnDef::new(SensorJobs::Priority)
                    .integer()
                    .not_null()
                    .default(100),
            )
            .col(
                ColumnDef::new(SensorJobs::UpdatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .to_owned()
    }

    /// Generates indexes for `raw.sensor_jobs`.
    pub fn create_indexes() -> Vec<IndexCreateStatement> {
        vec![
            // Unique constraint: A sensor can only have one job per target.
            Index::create()
                .name("uk_sensor_jobs_type_target")
                .table(Self::table_iden())
                .col(SensorJobs::SensorType)
                .col(SensorJobs::TargetUri)
                .unique()
                .to_owned(),
            // Index to help `sensd` efficiently find active jobs to run.
            Index::create()
                .name("ix_sensor_jobs_active_by_priority")
                .table(Self::table_iden())
                .col((SensorJobs::Priority, IndexOrder::Desc))
                .cond_where(Expr::col(SensorJobs::Status).eq("active"))
                .to_owned(),
        ]
    }

    /// Creates a trigger to update the updated_at column
    pub fn create_updated_at_trigger_sql() -> String {
        format!(
            r#"
            DROP TRIGGER IF EXISTS trg_sensor_jobs_updated_at ON {}.{};
            CREATE TRIGGER trg_sensor_jobs_updated_at
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

// =============================================================================
// The `raw.sensor_states` Table (The "Status")
// =============================================================================

/// **Table: `raw.sensor_states`**
///
/// This table stores the operational state and checkpoint for each sensor job.
/// It is a 1-to-1 extension of `sensor_jobs` and is exclusively managed by the
/// `sensd` daemon itself. This allows `sensd` to be stateless and resumable.
#[derive(Iden, Copy, Clone)]
pub enum SensorStates {
    Table,
    JobId,
    CurrentPosition,
    LastSuccessfulAcquisition,
    ErrorCount,
    Throughput,
    UpdatedAt,
}

impl TableDef for SensorStates {
    fn table_name() -> &'static str {
        "sensor_states"
    }
    fn schema_name() -> &'static str {
        "raw"
    }
    fn primary_key() -> &'static str {
        "job_id"
    }
}

/// The Rust struct representation of a row from `raw.sensor_states`.
#[derive(Debug, FromRow)]
pub struct SensorStateRecord {
    pub job_id: Ulid,
    pub current_position: JsonValue,
    pub last_successful_acquisition: Option<DateTime<Utc>>,
    pub error_count: i32,
    pub throughput: JsonValue,
    pub updated_at: DateTime<Utc>,
}

impl SensorStates {
    /// Generates the `CREATE TABLE` statement for `raw.sensor_states`.
    pub fn create_table_statement() -> TableCreateStatement {
        Table::create()
            .table(Self::table_iden())
            .if_not_exists()
            // The job_id is both the primary key and the foreign key to sensor_jobs.
            .col(
                ColumnDef::new(SensorStates::JobId)
                    .custom(Alias::new("ULID"))
                    .primary_key(),
            )
            .col(
                ColumnDef::new(SensorStates::CurrentPosition)
                    .json_binary()
                    .not_null()
                    .default(Expr::cust("'{}'::jsonb")),
            )
            .col(ColumnDef::new(SensorStates::LastSuccessfulAcquisition).timestamp_with_time_zone())
            .col(
                ColumnDef::new(SensorStates::ErrorCount)
                    .integer()
                    .not_null()
                    .default(0)
                    .check(Expr::cust("error_count >= 0")),
            )
            .col(
                ColumnDef::new(SensorStates::Throughput)
                    .json_binary()
                    .not_null()
                    .default(Expr::cust("'{}'::jsonb")),
            )
            .col(
                ColumnDef::new(SensorStates::UpdatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .foreign_key(
                ForeignKey::create()
                    .from(Self::table_iden(), SensorStates::JobId)
                    .to(SensorJobs::table_iden(), SensorJobs::Id)
                    .on_delete(ForeignKeyAction::Cascade), // If a job is deleted, its state is also deleted.
            )
            .to_owned()
    }

    /// Generates indexes for `raw.sensor_states`.
    pub fn create_indexes() -> Vec<IndexCreateStatement> {
        vec![
            // Index to find sensors that may be stale or failing.
            Index::create()
                .name("ix_sensor_states_last_acquisition")
                .table(Self::table_iden())
                .col((SensorStates::LastSuccessfulAcquisition, IndexOrder::Desc))
                // Note: nulls_first() may not be supported in sea-query
                .to_owned(),
        ]
    }

    /// Creates a trigger to update the updated_at column
    pub fn create_updated_at_trigger_sql() -> String {
        format!(
            r#"
            DROP TRIGGER IF EXISTS trg_sensor_states_updated_at ON {}.{};
            CREATE TRIGGER trg_sensor_states_updated_at
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
