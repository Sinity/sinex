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

/// Macro to create migration vector with less boilerplate
macro_rules! migrations {
    ($($migration:ident),* $(,)?) => {
        vec![
            $(Box::new($migration::Migration) as Box<dyn MigrationTrait>,)*
        ]
    };
}

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

pub struct Migrator;

#[async_trait::async_trait]
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        migrations![
            m20240101_000001_initial_schema,
            m20240102_000002_add_validation_functions,
            m20240103_000003_create_analytics_views,
            m20240104_000004_create_helper_functions,
            m20240105_000005_create_test_helper_functions,
            m20240106_000006_create_coordination_tables,
            m20240109_000009_add_payload_validation_function,
            m20240110_000010_add_event_payload_check_constraint,
            m20250810_000001_create_outbox_table,
            m20250810_000006_add_archive_trigger,
            m20250810_132050_drop_obsolete_artifact_tables,
            m20250811_000002_add_path_validation_functions,
            m20250811_000003_fix_idempotency_index,
            m20250811_000004_add_sensd_tables,
        ]
    }
}
