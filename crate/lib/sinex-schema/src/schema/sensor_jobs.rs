//! Schema definitions for sensor jobs table

use sea_orm_migration::prelude::*;

#[derive(Iden)]
pub enum SensorJobs {
    Table,
    #[iden = "job_id"]
    JobId,
    #[iden = "sensor_type"]
    SensorType,
    #[iden = "target_uri"]
    TargetUri,
    #[iden = "source_identifier"]
    SourceIdentifier,
    #[iden = "acquisition_mode"]
    AcquisitionMode,
    #[iden = "parameters"]
    Parameters,
    #[iden = "owner"]
    Owner,
    #[iden = "resource_limits"]
    ResourceLimits,
    #[iden = "status"]
    Status,
    #[iden = "priority"]
    Priority,
    #[iden = "created_at"]
    CreatedAt,
    #[iden = "updated_at"]
    UpdatedAt,
    #[iden = "started_at"]
    StartedAt,
    #[iden = "completed_at"]
    CompletedAt,
    #[iden = "error_message"]
    ErrorMessage,
    #[iden = "material_id"]
    MaterialId,
}

impl SensorJobs {
    pub fn create_table_statement() -> TableCreateStatement {
        Table::create()
            .table((Alias::new("raw"), SensorJobs::Table))
            .if_not_exists()
            // Primary key
            .col(
                ColumnDef::new(SensorJobs::JobId)
                    .custom(Alias::new("ULID"))
                    .not_null()
                    .primary_key(),
            )
            // Core job definition
            .col(ColumnDef::new(SensorJobs::SensorType).text().not_null())
            .col(ColumnDef::new(SensorJobs::TargetUri).text().not_null())
            .col(
                ColumnDef::new(SensorJobs::SourceIdentifier)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(SensorJobs::AcquisitionMode)
                    .json_binary()
                    .not_null(),
            )
            .col(
                ColumnDef::new(SensorJobs::Parameters)
                    .json_binary()
                    .not_null()
                    .default("'{}'"),
            )
            // Ownership and limits
            .col(ColumnDef::new(SensorJobs::Owner).text())
            .col(ColumnDef::new(SensorJobs::ResourceLimits).json_binary())
            // Status and priority
            .col(
                ColumnDef::new(SensorJobs::Status)
                    .text()
                    .not_null()
                    .default("'pending'"),
            )
            .col(
                ColumnDef::new(SensorJobs::Priority)
                    .integer()
                    .not_null()
                    .default(1000),
            )
            // Timestamps
            .col(
                ColumnDef::new(SensorJobs::CreatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(
                ColumnDef::new(SensorJobs::UpdatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(ColumnDef::new(SensorJobs::StartedAt).timestamp_with_time_zone())
            .col(ColumnDef::new(SensorJobs::CompletedAt).timestamp_with_time_zone())
            // Error handling
            .col(ColumnDef::new(SensorJobs::ErrorMessage).text())
            // Result tracking
            .col(ColumnDef::new(SensorJobs::MaterialId).custom(Alias::new("ULID")))
            .to_owned()
    }

    pub fn create_check_constraints() -> Vec<String> {
        vec![
            format!(
                r#"ALTER TABLE raw.{} ADD CONSTRAINT chk_sensor_jobs_status 
                   CHECK (status IN ('pending', 'running', 'completed', 'failed', 'cancelled'))"#,
                SensorJobs::Table.to_string()
            ),
            format!(
                r#"ALTER TABLE raw.{} ADD CONSTRAINT chk_sensor_jobs_priority 
                   CHECK (priority >= 0 AND priority <= 9999)"#,
                SensorJobs::Table.to_string()
            ),
        ]
    }

    pub fn create_indexes() -> Vec<String> {
        vec![
            format!(
                r#"CREATE INDEX IF NOT EXISTS ix_sensor_jobs_status_priority 
                   ON raw.{} (status, priority DESC)"#,
                SensorJobs::Table.to_string()
            ),
            format!(
                r#"CREATE INDEX IF NOT EXISTS ix_sensor_jobs_sensor_type 
                   ON raw.{} (sensor_type)"#,
                SensorJobs::Table.to_string()
            ),
            format!(
                r#"CREATE INDEX IF NOT EXISTS ix_sensor_jobs_created_at 
                   ON raw.{} (created_at)"#,
                SensorJobs::Table.to_string()
            ),
            format!(
                r#"CREATE INDEX IF NOT EXISTS ix_sensor_jobs_material_id 
                   ON raw.{} (material_id) WHERE material_id IS NOT NULL"#,
                SensorJobs::Table.to_string()
            ),
        ]
    }

    pub fn create_updated_at_trigger() -> String {
        format!(
            r#"CREATE TRIGGER trg_sensor_jobs_updated_at
               BEFORE UPDATE ON raw.{}
               FOR EACH ROW EXECUTE FUNCTION update_updated_at_column()"#,
            SensorJobs::Table.to_string()
        )
    }
}
