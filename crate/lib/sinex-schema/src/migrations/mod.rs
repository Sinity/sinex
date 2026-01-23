#![doc = include_str!("../../docs/migrations.md")]

// Declare the single, "squashed" initial schema migration as a public module.
// This makes `migrations::m20241028_000001_create_canonical_schema` accessible
// from `lib.rs` where it is registered with the Migrator.
pub mod m20241028_000001_create_canonical_schema;
pub mod m20250115_000002_add_entity_trgm_indexes;
pub mod m20250115_000003_add_events_payload_trgm_index;
pub mod m20250115_000004_add_events_payload_fts_index;
pub mod m20250115_000005_drop_legacy_coordination;
pub mod m20250117_000006_add_ts_ingest_index;
pub mod m20250117_000007_configure_chunk_interval;
pub mod m20250117_000008_add_retention_policy;
pub mod m20250117_000009_document_operation_id_security;
pub mod m20250117_000010_rename_processor_type_to_node_type;
pub mod m20250117_000011_add_self_observation_aggregates;
pub mod m20250118_000012_add_cancelled_status;
pub mod m20250121_000013_fix_partitioning;
pub mod m20260121_000014_add_jsonb_merge_function;
pub mod m20260121_000015_drop_payload_expensive_indexes;
pub mod m20260122_000016_add_embeddings;
pub mod m20260122_000017_add_user_state_aggregates;

// To add a new migration in the future, a developer would:
// 1. Create a new file, e.g., `src/migrations/m<timestamp>_add_new_feature.rs`.
// 2. Add a new line here: `pub mod m<timestamp>_add_new_feature;`.
// 3. Add the new migration to the `vec!` in `lib.rs`.
