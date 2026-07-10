//! Aggregated test entrypoint for tests/event_engine/*.rs (sinex-v7od).
//!
//! Consolidates what used to be N separate `[[test]]` binaries (each
//! relinking the full sinexd lib) into one. cargo-nextest still runs every
//! test function in its own process regardless of binary layout, so this
//! loses no test isolation. Files that shared tests/event_engine/support.rs
//! now reach it via an explicit `#[path = "support.rs"]` override -- `#[path]`
//! resolves relative to the directory that would hold this file's own
//! bare-mod children (`tests/event_engine/`), not relative to a `<stem>/`
//! subdirectory the way a plain `mod support;` would once nested one level
//! deeper under `mod event_engine { ... }`.

mod event_engine {
    mod admission_test;
    mod config_security_test;
    mod dlq_schema_validation_test;
    mod effective_config_test;
    mod envelope_admission_test;
    mod events_consumer_integration_test;
    mod ingest_service_test;
    mod jetstream_consumer_test;
    mod jetstream_dlq_test;
    mod jetstream_e2e_integration_test;
    mod jetstream_idempotency_property_test;
    mod jetstream_stream_name_test;
    mod jetstream_stress_test;
    mod material_assembler_concurrency_test;
    mod material_assembler_test;
    mod material_ready_set_inline_test;
    mod migration_lock_test;
    mod pipeline_integration_test;
    mod pipeline_resilience_test;
    mod privacy_policy_test;
    mod schema_sync_test;
    mod telemetry_persistence_test;
    mod tls_integration_test;
    mod validator_schema_order_test;
}
