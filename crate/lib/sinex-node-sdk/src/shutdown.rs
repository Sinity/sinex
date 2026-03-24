//! Shutdown-related runtime configuration helpers.
//!
//! The active node runtimes handle their own signal wiring and checkpoint
//! persistence directly. This module keeps only the shared checkpoint-path and
//! shutdown-configuration surface that the runtimes still use.

use std::path::PathBuf;

/// Default checkpoint file path for a node.
#[must_use]
pub fn default_checkpoint_path(node_name: &str) -> PathBuf {
    let runtime_dir = std::env::var("SINEX_RUNTIME_DIR")
        .or_else(|_| std::env::var("SINEX_WORK_DIR"))
        .ok()
        .map(PathBuf::from)
        .or_else(|| dirs::cache_dir().map(|dir| dir.join("sinex")))
        .unwrap_or_else(|| PathBuf::from("/tmp/sinex"));
    runtime_dir.join(format!("{node_name}.checkpoint.json"))
}

/// Configuration for shutdown behavior.
#[derive(Debug, Clone)]
pub struct ShutdownConfig {
    /// Whether to save state to file on shutdown
    pub save_state_on_shutdown: bool,
    /// Whether to restore state from file on startup
    pub restore_state_on_startup: bool,
    /// Custom checkpoint file path (None = use default)
    pub checkpoint_path: Option<PathBuf>,
    /// Grace period before forced shutdown (seconds)
    pub grace_period_secs: u64,
}

impl Default for ShutdownConfig {
    fn default() -> Self {
        Self {
            save_state_on_shutdown: true,
            restore_state_on_startup: true,
            checkpoint_path: None,
            grace_period_secs: 30,
        }
    }
}

impl ShutdownConfig {
    /// Get the checkpoint path, using default if not specified.
    #[must_use]
    pub fn checkpoint_path(&self, node_name: &str) -> PathBuf {
        self.checkpoint_path
            .clone()
            .unwrap_or_else(|| default_checkpoint_path(node_name))
    }
}
