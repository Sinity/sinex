//! Prelude module for convenient imports
//!
//! This module re-exports the most commonly used types and traits from the
//! sinexd crate for more ergonomic imports:
//!
//! ```rust
//! use sinexd::api::prelude::*;
//!
//! // Instead of:
//! // use sinexd::api::service_container::ServiceContainer;
//! // use sinex_db::replay::state_machine::{ReplayState, ReplayStateMachine};
//! // use sinexd::api::cascade_analyzer::{CascadeAnalysis, StreamingCascadeAnalyzer};
//! ```

// Service container
pub use crate::api::ServiceContainer;

// Replay system
pub use crate::api::{
    ReplayCheckpoint, ReplayOperation, ReplayScope, ReplayState, ReplayStateMachine,
};

// Cascade analysis
pub use crate::api::{
    CascadeAnalysis, CascadeAnalyzerConfig, CircularDependency, IntegrityViolation, Severity,
    StreamingCascadeAnalyzer, ViolationType,
};
