//! Schema definitions for sensor states table
//!
//! Based on TARGET_canonical.md line 250:
//! raw.sensor_states tracks job state including last positions and metrics

use sea_orm_migration::prelude::*;

#[derive(Iden)]
pub enum SensorStates {
    Table,
    #[iden = "job_id"]
    JobId,
    #[iden = "current_position"]
    CurrentPosition,
    #[iden = "last_successful_acquisition"]
    LastSuccessfulAcquisition,
    #[iden = "error_count"]
    ErrorCount,
    #[iden = "throughput"]
    Throughput,
    #[iden = "updated_at"]
    UpdatedAt,
}

impl SensorStates {
    pub fn create_table_statement() -> TableCreateStatement {
        Table::create()
            .table((Alias::new("raw"), SensorStates::Table))
            .if_not_exists()
            // Foreign key to sensor_jobs
            .col(
                ColumnDef::new(SensorStates::JobId)
                    .custom(Alias::new("ULID"))
                    .not_null()
                    .primary_key(),
            )
            // State tracking
            .col(
                ColumnDef::new(SensorStates::CurrentPosition)
                    .json_binary()
                    .not_null()
                    .default("{}"),
            )
            .col(ColumnDef::new(SensorStates::LastSuccessfulAcquisition).timestamp_with_time_zone())
            .col(
                ColumnDef::new(SensorStates::ErrorCount)
                    .integer()
                    .not_null()
                    .default(0),
            )
            // Metrics
            .col(
                ColumnDef::new(SensorStates::Throughput)
                    .json_binary()
                    .not_null()
                    .default("{}"),
            )
            // Timestamp
            .col(
                ColumnDef::new(SensorStates::UpdatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .to_owned()
    }

    pub fn create_foreign_key_constraints() -> Vec<String> {
        vec![format!(
            r#"ALTER TABLE raw.{} 
               ADD CONSTRAINT fk_sensor_states_job_id 
               FOREIGN KEY (job_id) 
               REFERENCES raw.sensor_jobs(job_id) 
               ON DELETE CASCADE"#,
            SensorStates::Table.to_string()
        )]
    }

    pub fn create_check_constraints() -> Vec<String> {
        vec![format!(
            r#"ALTER TABLE raw.{} 
               ADD CONSTRAINT chk_sensor_states_error_count 
               CHECK (error_count >= 0)"#,
            SensorStates::Table.to_string()
        )]
    }

    pub fn create_indexes() -> Vec<String> {
        vec![
            format!(
                r#"CREATE INDEX IF NOT EXISTS ix_sensor_states_updated_at 
                   ON raw.{} (updated_at DESC)"#,
                SensorStates::Table.to_string()
            ),
            format!(
                r#"CREATE INDEX IF NOT EXISTS ix_sensor_states_last_acquisition 
                   ON raw.{} (last_successful_acquisition) 
                   WHERE last_successful_acquisition IS NOT NULL"#,
                SensorStates::Table.to_string()
            ),
        ]
    }

    pub fn create_updated_at_trigger() -> String {
        format!(
            r#"
            CREATE OR REPLACE FUNCTION raw.fn_sensor_states_updated_at()
            RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                NEW.updated_at = CURRENT_TIMESTAMP;
                RETURN NEW;
            END $$;

            DROP TRIGGER IF EXISTS trg_sensor_states_updated_at ON raw.{};
            CREATE TRIGGER trg_sensor_states_updated_at
            BEFORE UPDATE ON raw.{}
            FOR EACH ROW EXECUTE FUNCTION raw.fn_sensor_states_updated_at();
            "#,
            SensorStates::Table.to_string(),
            SensorStates::Table.to_string()
        )
    }
}

/// Example current_position structures for different sensor types
pub mod position_examples {
    use serde_json::json;

    /// append_stream position tracking
    pub fn append_stream_position() -> serde_json::Value {
        json!({
            "bytes_read": 0i64,
            "lines_processed": 0i64,
            "last_offset": 0i64,
            "last_timestamp": null,
        })
    }

    /// tree_watch position tracking
    pub fn tree_watch_position() -> serde_json::Value {
        json!({
            "last_scan_path": null,
            "files_processed": 0i64,
            "directories_visited": 0i64,
            "last_mtime": null,
        })
    }

    /// batched_pull position tracking (API pagination)
    pub fn batched_pull_position() -> serde_json::Value {
        json!({
            "cursor": null,
            "page": 0i64,
            "etag": null,
            "last_id": null,
        })
    }

    /// db_snapshot position tracking
    pub fn db_snapshot_position() -> serde_json::Value {
        json!({
            "last_rowid": 0i64,
            "last_timestamp": null,
            "snapshot_hash": null,
            "rows_processed": 0i64,
        })
    }
}

/// Example throughput metrics structure
pub mod throughput_examples {
    use serde_json::json;

    pub fn default_throughput() -> serde_json::Value {
        json!({
            "bytes_per_second": 0.0,
            "events_per_second": 0.0,
            "last_minute_bytes": 0i64,
            "last_minute_events": 0i64,
            "total_bytes": 0i64,
            "total_events": 0i64,
            "avg_latency_ms": 0.0,
        })
    }
}
