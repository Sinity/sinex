//! Development orchestration modules (merged from sx tool).
//!
//! This module provides hot reload, file watching, production tethering,
//! and LLM-based node generation capabilities.

pub mod generate;
pub mod orchestrator;
pub mod state;
pub mod tether;
pub mod watcher;

// Re-exports for convenience
pub use generate::{GenerateArgs, GeneratorConfig, NodeGenerator, NodeSpec};
pub use orchestrator::{DevOrchestrator, RunArgs};
pub use state::{CheckoutState, LockInfo};
pub use tether::{TetherClient, TetherConfig, TetherSession};
pub use watcher::{FileWatcher, WatchEvent};
