//! Sinex Gateway Library
//!
//! Provides the service container, replay system, and related functionality for the Sinex Gateway

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
