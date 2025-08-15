//! Schema definitions and database migrations for the Sinex event-driven data capture system.
//!
//! This crate contains:
//! - Database schema migrations using SeaORM migration framework
//! - Schema definitions and Record types for database tables
//! - Core type definitions (IDs, ULIDs) used across the system
//!
//! Migrations are organized chronologically and handle the evolution of the database schema
//! for the core Sinex data substrate (PostgreSQL + TimescaleDB).
//!
//! ## Migration Strategy
//!
//! - All migrations are atomic and reversible where possible
//! - ULID-based primary keys for distributed-safe time-ordered IDs
//! - JSON Schema validation via pg_jsonschema for event payloads
//! - TimescaleDB hypertables for time-series optimization
//! - Comprehensive indexing for query performance

pub use sea_orm_migration::prelude::*;

// Core type definitions
pub mod ulid;
pub mod ulid_conversions;

// Database constants and utilities
pub mod constants;
pub mod migration_helpers;

// Schema definitions
pub mod schema;

// Migration modules
mod m20240101_000001_initial_schema;
mod m20240102_000002_add_validation_functions;
mod m20240103_000003_create_analytics_views;
mod m20240104_000004_create_helper_functions;
mod m20240105_000005_create_test_helper_functions;
mod m20240106_000006_create_coordination_tables;
mod m20240109_000009_add_payload_validation_function;
mod m20240110_000010_add_event_payload_check_constraint;
mod m20250810_000001_create_outbox_table;
mod m20250810_000006_add_archive_trigger;
mod m20250810_132050_drop_obsolete_artifact_tables;
mod m20250811_000002_add_path_validation_functions;
mod m20250811_000003_fix_idempotency_index;
mod m20250811_000004_add_sensd_tables;
mod m20250812_000001_add_sensor_states_table;
mod m20250812_140035_add_missing_annotation_metadata;
mod m20250812_213648_add_missing_state_columns;
mod m20250813_000001_fix_processor_checkpoints;
mod m20250813_000002_fix_blobs_table;
mod m20250813_000003_fix_event_payload_schemas;
mod m20250813_000004_add_missing_columns;
mod m20250813_000005_add_more_missing_columns;
mod m20250813_000006_add_more_schema_columns;
mod m20250813_000007_final_missing_columns;
mod m20250813_000008_fix_processor_manifests;
mod m20250813_100000_fix_operations_log;
mod m20250813_110000_add_approved_by_columns;
mod m20250813_120000_add_operations_log_columns;
mod m20250813_130000_add_processor_manifest_schemas;
mod m20250813_140000_add_operations_created_at;
mod m20250813_150000_fix_operations_scope_type;
mod m20250813_160000_add_processor_config_schema;
mod m20250813_170000_add_runtime_requirements;
mod m20250813_180000_fix_preview_summary_type;
mod m20250814_000001_add_schema_unique_constraint;
mod m20250814_000002_add_missing_table_columns;

pub struct Migrator;

#[async_trait::async_trait]
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![
            Box::new(m20240101_000001_initial_schema::Migration),
            Box::new(m20240102_000002_add_validation_functions::Migration),
            Box::new(m20240103_000003_create_analytics_views::Migration),
            Box::new(m20240104_000004_create_helper_functions::Migration),
            Box::new(m20240105_000005_create_test_helper_functions::Migration),
            Box::new(m20240106_000006_create_coordination_tables::Migration),
            Box::new(m20240109_000009_add_payload_validation_function::Migration),
            Box::new(m20240110_000010_add_event_payload_check_constraint::Migration),
            Box::new(m20250810_000001_create_outbox_table::Migration),
            Box::new(m20250810_000006_add_archive_trigger::Migration),
            Box::new(m20250810_132050_drop_obsolete_artifact_tables::Migration),
            Box::new(m20250811_000002_add_path_validation_functions::Migration),
            Box::new(m20250811_000003_fix_idempotency_index::Migration),
            Box::new(m20250811_000004_add_sensd_tables::Migration),
            Box::new(m20250812_000001_add_sensor_states_table::Migration),
            Box::new(m20250812_140035_add_missing_annotation_metadata::Migration),
            Box::new(m20250813_000001_fix_processor_checkpoints::Migration),
            Box::new(m20250813_000002_fix_blobs_table::Migration),
            Box::new(m20250813_000003_fix_event_payload_schemas::Migration),
            Box::new(m20250813_000004_add_missing_columns::Migration),
            Box::new(m20250813_000005_add_more_missing_columns::Migration),
            Box::new(m20250813_000006_add_more_schema_columns::Migration),
            Box::new(m20250813_000007_final_missing_columns::Migration),
            Box::new(m20250813_000008_fix_processor_manifests::Migration),
            Box::new(m20250813_100000_fix_operations_log::Migration),
            Box::new(m20250813_110000_add_approved_by_columns::Migration),
            Box::new(m20250813_120000_add_operations_log_columns::Migration),
            Box::new(m20250813_130000_add_processor_manifest_schemas::Migration),
            Box::new(m20250813_140000_add_operations_created_at::Migration),
            Box::new(m20250813_150000_fix_operations_scope_type::Migration),
            Box::new(m20250813_160000_add_processor_config_schema::Migration),
            Box::new(m20250813_170000_add_runtime_requirements::Migration),
            Box::new(m20250813_180000_fix_preview_summary_type::Migration),
            Box::new(m20250812_213648_add_missing_state_columns::Migration),
            Box::new(m20250814_000001_add_schema_unique_constraint::Migration),
            Box::new(m20250814_000002_add_missing_table_columns::Migration),
        ]
    }
}
