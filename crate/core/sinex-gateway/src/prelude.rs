//! Prelude module for convenient imports
//!
//! This module re-exports the most commonly used types and traits from the
//! sinex-gateway crate for more ergonomic imports:
//!
//! ```rust
//! use sinex_gateway::prelude::*;
//!
//! // Instead of:
//! // use sinex_gateway::service_container::ServiceContainer;
//! // use sinex_db::replay::state_machine::{ReplayState, ReplayStateMachine};
//! // use sinex_gateway::cascade_analyzer::{CascadeAnalysis, StreamingCascadeAnalyzer};
//! ```

// Service container
pub use crate::ServiceContainer;

// Replay system
pub use crate::{ReplayCheckpoint, ReplayOperation, ReplayScope, ReplayState, ReplayStateMachine};

// Cascade analysis
pub use crate::{
    CascadeAnalysis, CascadeAnalyzerConfig, CircularDependency, IntegrityViolation, Severity,
    StreamingCascadeAnalyzer, ViolationType,
};
