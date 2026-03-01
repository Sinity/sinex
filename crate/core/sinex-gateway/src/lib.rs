#![allow(async_fn_in_trait)]
#![doc = include_str!("../docs/README.md")]
#![doc = include_str!("../docs/overview.md")]
#![doc = include_str!("../../../../docs/current/architecture/UserInteraction_And_Query_Architecture.md")]
#![doc = include_str!("../../../../docs/current/architecture/SystemOperations_And_Integrity_Architecture.md")]

//! Gateway service orchestrating RPC, replay, and stream handling.

// Expose modules for testing and external use
pub mod auth;
pub mod cascade_analyzer;
pub mod client;
pub mod distributed_rate_limit;
pub mod gateway_metrics;
pub mod handlers;
#[cfg(any(feature = "test-support", test))]
pub mod handlers_test_support;
pub mod native_messaging;
pub mod prelude;
pub mod rate_limit;
pub mod replay_control;
pub mod replay_state_machine;
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
pub use replay_state_machine::{
    ReplayCheckpoint, ReplayOperation, ReplayScope, ReplayState, ReplayStateMachine,
};
pub use service_container::ServiceContainer;
