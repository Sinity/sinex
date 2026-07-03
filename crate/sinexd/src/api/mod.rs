//! JSON-RPC / SSE / native-messaging operator surface.
//!
//! Hosts the RPC server, handler dispatch, native-messaging protocol for
//! browser extensions, server-sent-events fanout, auth, rate limiting, and
//! replay control. Reads from the database via the `sinex_db` repository
//! layer; control surfaces invoke `crate::event_engine::*` for lifecycle and
//! replay coordination.

pub mod auth;
pub mod cascade_analyzer;
pub mod client;
pub mod config;
pub mod content_service;
pub mod distributed_rate_limit;
pub mod gateway_metrics;
pub mod handlers;
#[cfg(any(feature = "test-support", test))]
pub mod handlers_test_support;
pub mod lifecycle_ttl;
pub mod native_messaging;
pub mod prelude;
pub mod rate_limit;
pub mod replay_control;
pub mod rpc_registry;
pub mod rpc_server;
#[cfg(any(feature = "test-support", test))]
pub mod rpc_server_test_support;
pub mod schema_registry;
pub mod service_container;
pub mod sse_bus;
pub mod sse_handler;

pub use cascade_analyzer::{
    CascadeAnalysis, CascadeAnalyzerConfig, CircularDependency, IntegrityViolation, Severity,
    StreamingCascadeAnalyzer, ViolationType,
};
pub use service_container::ServiceContainer;
pub use sinex_db::replay::state_machine::{
    ReplayCheckpoint, ReplayOperation, ReplayScope, ReplayState, ReplayStateMachine,
};
