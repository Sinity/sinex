#![doc = include_str!("../../doc/migrations.md")]

// Declare the single, "squashed" initial schema migration as a public module.
// This makes `migrations::m20241028_000001_create_canonical_schema` accessible
// from `lib.rs` where it is registered with the Migrator.
pub mod m20241028_000001_create_canonical_schema;

// To add a new migration in the future, a developer would:
// 1. Create a new file, e.g., `src/migrations/m<timestamp>_add_new_feature.rs`.
// 2. Add a new line here: `pub mod m<timestamp>_add_new_feature;`.
// 3. Add the new migration to the `vec!` in `lib.rs`.
