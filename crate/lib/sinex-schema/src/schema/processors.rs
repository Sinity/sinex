//! Schema definitions for processors and coordination tables

use crate::schema::TableDef;
use sea_orm_migration::prelude::*;

#[derive(Iden, Copy, Clone)]
pub enum ProcessorCheckpoints {
    Table,
    Id,
    ProcessorName,
    ConsumerGroup,
    ConsumerName,
    LastProcessedEventId,
    LastProcessedAt,
    ProcessedCount,
    FailedAttempts,
    LastError,
    Checkpoint,
    CheckpointData,
    CheckpointVersion,
    State,
    CreatedAt,
    UpdatedAt,
}

#[derive(Iden, Copy, Clone)]
pub enum ProcessorManifests {
    Table,
    Id,
    ProcessorName,
    ProcessorType,
    Version,
    Description,
    Capabilities,
    ConfigSchema,
    RuntimeRequirements,
    DeploymentStatus,
    CreatedAt,
    UpdatedAt,
}

#[derive(Iden, Copy, Clone)]
pub enum OperationsLog {
    Table,
    Id,
    OperationType,
    State,
    Actor,
    Scope,
    Context,
    Outcome,
    PreviewSummary,
    ApprovedBy,
    StartedAt,
    CompletedAt,
    FinishedAt,
    ErrorDetails,
    Checkpoint,
    CreatedAt,
}

impl ProcessorCheckpoints {
    pub fn create_table() -> String {
        Table::create()
            .table((Alias::new("core"), ProcessorCheckpoints::Table))
            .if_not_exists()
            .col(
                ColumnDef::new(ProcessorCheckpoints::Id)
                    .uuid()
                    .not_null()
                    .primary_key(),
            )
            .col(
                ColumnDef::new(ProcessorCheckpoints::ProcessorName)
                    .text()
                    .not_null(),
            )
            .col(ColumnDef::new(ProcessorCheckpoints::ConsumerGroup).text())
            .col(ColumnDef::new(ProcessorCheckpoints::ConsumerName).text())
            .col(ColumnDef::new(ProcessorCheckpoints::LastProcessedEventId).custom("ulid"))
            .col(ColumnDef::new(ProcessorCheckpoints::LastProcessedAt).timestamp_with_time_zone())
            .col(
                ColumnDef::new(ProcessorCheckpoints::ProcessedCount)
                    .big_integer()
                    .not_null()
                    .default(0),
            )
            .col(
                ColumnDef::new(ProcessorCheckpoints::FailedAttempts)
                    .integer()
                    .not_null()
                    .default(0),
            )
            .col(ColumnDef::new(ProcessorCheckpoints::LastError).text())
            .col(ColumnDef::new(ProcessorCheckpoints::Checkpoint).json_binary())
            .col(ColumnDef::new(ProcessorCheckpoints::CheckpointData).json_binary())
            .col(
                ColumnDef::new(ProcessorCheckpoints::CheckpointVersion)
                    .integer()
                    .not_null()
                    .default(1),
            )
            .col(ColumnDef::new(ProcessorCheckpoints::State).json_binary())
            .col(
                ColumnDef::new(ProcessorCheckpoints::CreatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default("NOW()"),
            )
            .col(
                ColumnDef::new(ProcessorCheckpoints::UpdatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default("NOW()"),
            )
            .to_string(PostgresQueryBuilder)
    }

    pub fn create_indexes() -> Vec<String> {
        vec![]
    }

    pub fn create_constraints() -> Vec<String> {
        vec![]
    }
}

impl ProcessorManifests {
    pub fn create_table() -> String {
        Table::create()
            .table((Alias::new("core"), ProcessorManifests::Table))
            .if_not_exists()
            .col(
                ColumnDef::new(ProcessorManifests::Id)
                    .uuid()
                    .not_null()
                    .primary_key(),
            )
            .col(
                ColumnDef::new(ProcessorManifests::ProcessorName)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(ProcessorManifests::ProcessorType)
                    .text()
                    .default("generic"),
            )
            .col(
                ColumnDef::new(ProcessorManifests::Version)
                    .text()
                    .not_null()
                    .default("0.0.0"),
            )
            .col(ColumnDef::new(ProcessorManifests::Description).text())
            .col(
                ColumnDef::new(ProcessorManifests::Capabilities)
                    .json_binary()
                    .default("{}"),
            )
            .col(ColumnDef::new(ProcessorManifests::ConfigSchema).json_binary())
            .col(ColumnDef::new(ProcessorManifests::RuntimeRequirements).json_binary())
            .col(
                ColumnDef::new(ProcessorManifests::DeploymentStatus)
                    .text()
                    .not_null()
                    .default("inactive"),
            )
            .col(
                ColumnDef::new(ProcessorManifests::CreatedAt)
                    .timestamp_with_time_zone()
                    .default("NOW()"),
            )
            .col(
                ColumnDef::new(ProcessorManifests::UpdatedAt)
                    .timestamp_with_time_zone()
                    .default("NOW()"),
            )
            .to_string(PostgresQueryBuilder)
    }

    pub fn create_indexes() -> Vec<String> {
        vec![]
    }
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

impl OperationsLog {
    pub fn create_table() -> String {
        Table::create()
            .table((Alias::new("core"), OperationsLog::Table))
            .if_not_exists()
            .col(
                ColumnDef::new(OperationsLog::Id)
                    .uuid()
                    .not_null()
                    .primary_key(),
            )
            .col(
                ColumnDef::new(OperationsLog::OperationType)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(OperationsLog::State)
                    .text()
                    .not_null()
                    .default("pending"),
            )
            .col(
                ColumnDef::new(OperationsLog::Actor)
                    .text()
                    .not_null()
                    .default("system"),
            )
            .col(
                ColumnDef::new(OperationsLog::Scope)
                    .text()
                    .not_null()
                    .default("unknown"),
            )
            .col(ColumnDef::new(OperationsLog::Context).json_binary())
            .col(ColumnDef::new(OperationsLog::Outcome).text())
            .col(ColumnDef::new(OperationsLog::PreviewSummary).text())
            .col(ColumnDef::new(OperationsLog::ApprovedBy).text())
            .col(
                ColumnDef::new(OperationsLog::StartedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default("NOW()"),
            )
            .col(ColumnDef::new(OperationsLog::CompletedAt).timestamp_with_time_zone())
            .col(ColumnDef::new(OperationsLog::FinishedAt).timestamp_with_time_zone())
            .col(ColumnDef::new(OperationsLog::ErrorDetails).json_binary())
            .col(ColumnDef::new(OperationsLog::Checkpoint).json_binary())
            .col(
                ColumnDef::new(OperationsLog::CreatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default("NOW()"),
            )
            .to_string(PostgresQueryBuilder)
    }

    pub fn create_indexes() -> Vec<String> {
        vec![]
    }
}
