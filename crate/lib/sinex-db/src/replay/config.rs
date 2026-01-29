//! Configuration for replay operations
//!
//! This module provides configuration for replay operations.

use serde::{Deserialize, Serialize};

/// Configuration for the replay system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayConfig {
    /// Whether to run in dry-run mode (no actual changes)
    pub dry_run: bool,
    /// Whether to log all operations that would be performed in dry-run
    pub dry_run_verbose: bool,
    /// Number of events to process in each batch
    pub batch_size: usize,
    /// Number of parallel workers for processing
    pub parallel_workers: usize,
}

impl Default for ReplayConfig {
    fn default() -> Self {
        Self {
            dry_run: false,
            dry_run_verbose: false,
            batch_size: 500,
            parallel_workers: 4,
        }
    }
}
