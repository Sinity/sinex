#![doc = include_str!("../docs/README.md")]
#![doc = include_str!("../docs/overview.md")]
#![doc = include_str!("../docs/schema_design.md")]
#![doc = include_str!("../../../../docs/current/architecture/Core_Architecture.md")]
#![doc = include_str!("../docs/ulid.md")]

//! Workspace schema definitions and migrations.

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
