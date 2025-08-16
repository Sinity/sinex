//! # Sinex Database Migrations
//!
//! This crate contains the complete schema definition and the evolutionary history
//! of the Sinex database. It uses the `sea-orm-migration` framework with `sea-query`
//! to define the schema in a type-safe, programmatic way.
//!
//! ## Architecture
//!
//! - **`src/schema/`:** Contains the canonical, state-of-the-art definition of
//!   every table in the database as of the latest version. This is the single
//!   source of truth for the schema.
//! - **`src/migrations/`:** Contains a single "squashed" initial migration that
//!   creates the entire canonical schema from scratch. All future schema changes
//!   will be new, timestamped migration files that apply incremental `ALTER`
//!   statements.
//! - **`src/main.rs`:** Provides a CLI for managing migrations.

pub use sea_orm_migration::prelude::*;

// Core type definitions
pub mod ulid;
pub mod ulid_conversions;

// The single source of truth for all schema definitions.
pub mod schema;

// The directory containing all migration files.

mod migrations;

/// The canonical Migrator for the Sinex database.
///
/// This struct is the entry point for the `sea-orm-migration` tool. It defines
/// the complete, ordered list of all migrations that constitute the history
/// of the database schema.
pub struct Migrator;

#[async_trait::async_trait]
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![Box::new(
            migrations::m20241028_000001_create_canonical_schema::Migration,
        )]
    }
}
