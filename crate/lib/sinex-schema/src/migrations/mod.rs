#![doc = include_str!("../../docs/migrations.md")]

// Declare the single, "squashed" initial schema migration as a public module.
// This makes `migrations::m20241028_000001_create_canonical_schema` accessible
// from `lib.rs` where it is registered with the Migrator.
pub(crate) mod m20241028_000001_create_canonical_schema;
pub(crate) mod m20250115_000002_add_entity_trgm_indexes;
pub(crate) mod m20250115_000003_add_events_payload_trgm_index;
pub(crate) mod m20250115_000004_add_events_payload_fts_index;
pub(crate) mod m20250115_000005_drop_legacy_coordination;
pub(crate) mod m20250117_000006_add_ts_ingest_index;
pub(crate) mod m20250117_000007_configure_chunk_interval;
pub(crate) mod m20250117_000008_add_retention_policy;
pub(crate) mod m20250117_000009_document_operation_id_security;
pub(crate) mod m20250117_000010_rename_processor_type_to_node_type;
pub(crate) mod m20250117_000011_add_self_observation_aggregates;
pub(crate) mod m20250118_000012_add_cancelled_status;
pub(crate) mod m20250121_000013_fix_partitioning;
pub(crate) mod m20260121_000014_add_jsonb_merge_function;
pub(crate) mod m20260121_000015_drop_payload_expensive_indexes;
pub(crate) mod m20260122_000016_add_embeddings;
pub(crate) mod m20260122_000017_add_user_state_aggregates;
pub(crate) mod m20260203_000018_dynamic_embedding_dimensions;
pub(crate) mod m20260203_000019_add_event_tombstones;
pub(crate) mod m20260213_000020_role_separation;
pub(crate) mod m20260213_000021_add_processor_status_tracking;
pub(crate) mod m20260214_000022_grant_gitops_to_ingestd;
pub(crate) mod m20260221_000023_rename_ingestor_version_to_node_version;
pub(crate) mod m20260221_000024_rename_processor_manifests_to_node_manifests;
pub(crate) mod m20260228_000025_add_provenance_gin_index;
pub(crate) mod m20260306_000026_rename_ts_ingest_to_ts_coided;

// To add a new migration in the future, a developer would:
// 1. Create a new file, e.g., `src/migrations/m<timestamp>_add_new_feature.rs`.
// 2. Add a new line here: `pub mod m<timestamp>_add_new_feature;`.
// 3. Add the new migration to the `vec!` in `lib.rs`.
