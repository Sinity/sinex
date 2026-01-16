#![doc = include_str!("../../docs/migrations.md")]

// Declare the single, "squashed" initial schema migration as a public module.
// This makes `migrations::m20241028_000001_create_canonical_schema` accessible
// from `lib.rs` where it is registered with the Migrator.
pub mod m20241028_000001_create_canonical_schema;
pub mod m20250115_000002_add_entity_trgm_indexes;
pub mod m20250115_000003_add_events_payload_trgm_index;
pub mod m20250115_000004_add_events_payload_fts_index;
pub mod m20250115_000005_drop_legacy_coordination;

// To add a new migration in the future, a developer would:
// 1. Create a new file, e.g., `src/migrations/m<timestamp>_add_new_feature.rs`.
// 2. Add a new line here: `pub mod m<timestamp>_add_new_feature;`.
// 3. Add the new migration to the `vec!` in `lib.rs`.
