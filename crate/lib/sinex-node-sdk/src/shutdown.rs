//! Shutdown-related runtime configuration helpers.
//!
//! The active node runtimes handle their own signal wiring and checkpoint
//! persistence directly. This module keeps only the shared checkpoint-path and
//! shutdown-configuration surface that the runtimes still use.

use sinex_primitives::env as shared_env;
use std::path::PathBuf;
use tracing::warn;

/// Default checkpoint file path for a node.
#[must_use]
pub fn default_checkpoint_path(node_name: &str) -> PathBuf {
    let runtime_dir = env_nonempty_string_optional("SINEX_RUNTIME_DIR", "shutdown checkpoint path")
        .or_else(|| env_nonempty_string_optional("SINEX_WORK_DIR", "shutdown checkpoint path"))
        .map(PathBuf::from)
        .or_else(|| dirs::cache_dir().map(|dir| dir.join("sinex")))
        .unwrap_or_else(|| PathBuf::from("/tmp/sinex"));
    runtime_dir.join(format!("{node_name}.checkpoint.json"))
}

fn env_nonempty_string_optional(var: &str, context: &str) -> Option<String> {
    shared_env::var_optional(var, context).and_then(|raw| {
        if raw.trim().is_empty() {
            warn!(
                variable = var,
                context, "Environment override is blank; ignoring value"
            );
            None
        } else {
            Some(raw)
        }
    })
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

/// Wait until shutdown is explicitly requested or the sender disappears.
///
/// Returns `true` once the shutdown flag is observed as set, including when the
/// flag was already set before the wait begins. Returns `false` if the sender is
/// dropped before any explicit shutdown request is observed.
#[cfg(feature = "messaging")]
pub async fn wait_for_shutdown_signal(
    shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
) -> bool {
    loop {
        if *shutdown_rx.borrow() {
            return true;
        }

        if shutdown_rx.changed().await.is_err() {
            return false;
        }
    }
}
