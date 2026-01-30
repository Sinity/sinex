//! Replay system for event re-processing
//!
//! This module provides functionality for replaying events through the system,
//! including cascade analysis, state management, and invariant enforcement.

pub mod config;
pub mod dry_run;
pub mod invariants;
pub mod logging;
pub mod state_machine;

// Re-export commonly used types
pub use config::ReplayConfig;
pub use dry_run::{execute_dry_run, DryRunExecutor, DryRunOperation, DryRunResult};
pub use invariants::{InvariantViolation, ViolationSeverity, ViolationType};
pub use state_machine::{
    ReplayCheckpoint, ReplayOperation, ReplayScope, ReplayState, ReplayStateMachine,
};
