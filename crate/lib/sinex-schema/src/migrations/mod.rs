//! The central module for all database migrations.
//!
//! This file serves as the entry point for the `migrations` directory. Its purpose
//! is to declare all individual migration files as public sub-modules of the
//! `migrations` module. This makes them accessible to the `Migrator` struct
//! defined in the crate's `lib.rs`.
//!
//! ### The Migration Process
//!
//! 1.  **Declaration (Here):** Every new migration file created in this directory
//!     must have a corresponding `pub mod <filename>;` line added here.
//!
//! 2.  **Registration (`lib.rs`):** The `Migrator` struct in `lib.rs` collects these
//!     declared modules and adds them to the execution sequence.
//!
//! This two-step process ensures that the Rust compiler is aware of all migration
//! modules and that they are correctly ordered for execution by the migration tool.
//!
//! ### Note on the "Squashed" Migration
//!
//! As per the canonical architectural refactoring, all previous, incremental migration
//! files have been consolidated into a single, comprehensive initial schema migration.
//! This simplifies the setup of new databases and provides a clean baseline. All
//! future schema changes will be new, timestamped migration files added to this
//! module.

// Declare the single, "squashed" initial schema migration as a public module.
// This makes `migrations::m20241028_000001_create_canonical_schema` accessible
// from `lib.rs` where it is registered with the Migrator.
pub mod m20241028_000001_create_canonical_schema;

// To add a new migration in the future, a developer would:
// 1. Create a new file, e.g., `src/migrations/m<timestamp>_add_new_feature.rs`.
// 2. Add a new line here: `pub mod m<timestamp>_add_new_feature;`.
// 3. Add the new migration to the `vec!` in `lib.rs`.
