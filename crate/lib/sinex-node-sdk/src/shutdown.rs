//! Shutdown-related runtime configuration helpers.
//!
//! The active node runtimes handle their own signal wiring and checkpoint
//! persistence directly. This module keeps only the shared checkpoint-path and
//! shutdown-configuration surface that the runtimes still use.

use sinex_primitives::env as shared_env;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tracing::warn;

/// Wait until shutdown is signaled via an `Arc<AtomicBool>` + `Arc<Notify>` pair.
///
/// Uses the `Notify` for reactive waking (no polling). Returns once the flag
/// has been observed as set, even if it was already set before the first check.
pub async fn wait_for_shutdown_signal_bool(
    shutdown_flag: &Arc<AtomicBool>,
    shutdown_notify: &Arc<tokio::sync::Notify>,
) {
    loop {
        let notified = shutdown_notify.notified();
        if shutdown_flag.load(Ordering::Acquire) {
            return;
        }
        notified.await;
    }
}

/// Wait for OS shutdown signals (`SIGTERM`, `SIGINT` on Unix; `Ctrl+C` on all
/// platforms).
///
/// Returns the name of the signal that triggered shutdown.
#[cfg(unix)]
pub async fn wait_for_os_shutdown_signal() -> std::io::Result<&'static str> {
    use tokio::signal::unix::{SignalKind, signal};

    let mut sigterm = signal(SignalKind::terminate())?;
    let mut sigint = signal(SignalKind::interrupt())?;

    tokio::select! {
        _ = sigterm.recv() => Ok("SIGTERM"),
        _ = sigint.recv() => Ok("SIGINT"),
    }
}

/// Wait for OS shutdown signals (platform fallback: `Ctrl+C` only).
#[cfg(not(unix))]
pub async fn wait_for_os_shutdown_signal() -> std::io::Result<&'static str> {
    tokio::signal::ctrl_c().await?;
    Ok("Ctrl+C")
}

/// Validate that a node name is safe to use as a filename component.
///
/// Rejects empty names, names containing path separators or `..`, and any
/// character outside `[a-zA-Z0-9_-]`.  This prevents a maliciously-crafted
/// `node_name` from escaping the runtime directory via path traversal when
/// the name is joined into a checkpoint file path.
///
/// # Errors
///
/// Returns an error string if the name fails validation.
pub fn validate_node_name(node_name: &str) -> Result<(), String> {
    if node_name.is_empty() {
        return Err("node name must not be empty".to_string());
    }
    if !node_name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return Err(format!(
            "node name {node_name:?} contains disallowed characters; \
             only [a-zA-Z0-9_-] are permitted"
        ));
    }
    Ok(())
}

/// Sanitize a node name for use as a filename component.
///
/// Replaces any character that is not alphanumeric, `-`, or `_` with `_`.
/// This is the lenient counterpart to [`validate_node_name`]; callers that
/// reach this from runtime contexts where validation has already happened
/// can use it as a defense-in-depth fallback.
fn sanitize_node_name_for_filename(name: &str) -> String {
    if name.is_empty() {
        return "_".to_string();
    }
    name.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

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
