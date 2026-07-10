//! Aggregated test entrypoint for tests/api/*.rs (sinex-v7od).
//!
//! Consolidates what used to be N separate `[[test]]` binaries (each
//! relinking the full sinexd lib) into one. cargo-nextest still runs every
//! test function in its own process regardless of binary layout, so this
//! loses no test isolation. The 7 files that shared tests/api/common/mod.rs
//! now reach it via an explicit `#[path = "common/mod.rs"]` override --
//! `#[path]` resolves relative to the directory that would hold this file's
//! own bare-mod children (`tests/api/`), not relative to a `<stem>/`
//! subdirectory the way a plain `mod common;` would once nested one level
//! deeper under `mod api { ... }`.

mod api {
    mod audit_handlers_test;
    mod auth_boundary_test;
    mod auth_test;
    mod automata_handlers_test;
    mod blob_event_forwarding_test;
    mod blob_route_security_test;
    mod cascade_analyzer_cycle_test;
    mod cascade_analyzer_test;
    mod cascade_depth_truncation_test;
    mod client_test;
    mod config_test;
    mod content_service_test;
    mod coordination_handlers_test;
    mod curation_handlers_test;
    mod distributed_rate_limit_test;
    mod dlq_handlers_test;
    mod documents_handlers_test;
    mod gateway_secret_management_test;
    mod handlers_test;
    mod health_handlers_test;
    mod instructions_handlers_test;
    mod lifecycle_handlers_test;
    mod llm_handlers_test;
    mod native_messaging_auth_test;
    mod ops_handlers_test;
    mod pkm_handlers_test;
    mod rate_limit_test;
    mod replay_auth_test;
    mod replay_control_resilience_test;
    mod replay_control_serialization_test;
    mod replay_determinism_test;
    mod replay_failure_test;
    mod replay_idempotency_test;
    mod replay_lifecycle_test;
    mod replay_rpc_live_test;
    mod replay_state_machine_test;
    mod rpc_auth_test;
    mod runtime_handlers_test;
    mod runtime_registry_handlers_test;
    mod semantic_handlers_test;
    mod service_container_test;
    mod shadow_handlers_test;
    mod sources_handlers_test;
    mod sse_real_confirmations_test;
    mod sse_stream_test;
    mod tasks_handlers_test;
    mod telemetry_handlers_test;
    mod tls_handshake_test;
    mod token_rotation_test;
    mod transport_security_test;
}
