#![doc = include_str!("../docs/README.md")]
#![doc = include_str!("../docs/overview.md")]
#![doc = include_str!("../docs/interaction_and_query.md")]

//! Gateway service orchestrating RPC, replay, and stream handling.

// Expose modules for testing and external use
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
pub mod native_messaging;
pub mod prelude;
pub mod rate_limit;
pub mod replay_control;
pub mod rpc_registry;
pub mod rpc_server;
#[cfg(any(feature = "test-support", test))]
pub mod rpc_server_test_support;
pub mod service_container;
pub mod sse_bus;
pub mod sse_handler;

// Re-export commonly used types
pub use cascade_analyzer::{
    CascadeAnalysis, CascadeAnalyzerConfig, CircularDependency, IntegrityViolation, Severity,
    StreamingCascadeAnalyzer, ViolationType,
};
pub use service_container::ServiceContainer;
pub use sinex_db::replay::state_machine::{
    ReplayCheckpoint, ReplayOperation, ReplayScope, ReplayState, ReplayStateMachine,
};
