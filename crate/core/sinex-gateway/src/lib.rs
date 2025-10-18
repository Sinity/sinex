#![doc = include_str!("../doc/README.md")]
#![doc = include_str!("../doc/overview.md")]
#![doc = include_str!("../../../../docs/architecture/UserInteraction_And_Query_Architecture.md")]
#![doc = include_str!("../../../../docs/architecture/SystemOperations_And_Integrity_Architecture.md")]

//! Gateway service orchestrating RPC, replay, and stream handling.

// Expose modules for testing and external use
pub mod cascade_analyzer;
pub mod handlers;
pub mod prelude;
pub mod replay_state_machine;
pub mod rpc_server;
pub mod service_container;

// Re-export commonly used types
pub use cascade_analyzer::{
    CascadeAnalysis, CascadeAnalyzerConfig, CircularDependency, IntegrityViolation, Severity,
    StreamingCascadeAnalyzer, ViolationType,
};
pub use replay_state_machine::{
    ReplayCheckpoint, ReplayOperation, ReplayScope, ReplayState, ReplayStateMachine,
};
pub use service_container::ServiceContainer;
