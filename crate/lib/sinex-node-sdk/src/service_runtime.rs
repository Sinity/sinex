//! Shared bootstrap utilities for service-level binaries (gateway, ingestd).
//!
//! These helpers cover the common startup concerns that aren't specific to
//! the node lifecycle managed by `node_entrypoint!`. Unlike `IngestorNode` or
//! `DerivedNodeAdapter`, this module is intentionally *not* lifecycle-aware —
//! it provides pure setup functions that each binary calls once at the start
//! of `main`.
//!
//! The duplication this consolidates was flagged by the issue-drift detector
//! (#694) and the audit-cycle synthesis: `load_env_filter` was copy-pasted
//! identically into both gateway and ingestd, and the tracing-init shape
//! drifted between them in small ways (try_init vs init, target/thread-id
//! flags) without any of the differences being deliberate.
//!
//! # Conventions
//!
//! - [`install_tracing`] **must** be called once per process before any
//!   `tracing` macros are used.
//! - `human_panic::setup_panic!()` is a proc-macro and cannot be wrapped in a
//!   function; call it directly at the top of each `main`.
//! - [`spawn_shutdown_task`] returns a `watch::Receiver<bool>` that flips to
//!   `true` on SIGINT/SIGTERM. Pass it to long-running async code.
//!
//! # Example
//!
//! ```ignore
//! use sinex_node_sdk::service_runtime::{self, TracingFormat};
//!
//! #[tokio::main]
//! async fn main() -> color_eyre::eyre::Result<()> {
//!     human_panic::setup_panic!();
//!     color_eyre::install()?;
//!
//!     service_runtime::install_tracing(TracingFormat::Text, "my_service=info")?;
//!     let shutdown_rx = service_runtime::spawn_shutdown_task("my-service");
//!
//!     run_service(shutdown_rx).await?;
//!     Ok(())
//! }
//! ```

use color_eyre::eyre::{Result, eyre};
use sinex_primitives::strict_env_filter_source;
use tokio::sync::watch;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

/// Output format for tracing (log) messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TracingFormat {
    /// Human-readable text output (default for interactive use).
    Text,
    /// Structured JSON output for machine parsing (e.g. journald / log
    /// aggregators).
    Json,
}

/// Build a [`tracing_subscriber::EnvFilter`] from `RUST_LOG`, falling back to
/// `default_filter` when the variable is absent.
///
/// Returns an error if `RUST_LOG` contains non-UTF-8 bytes or an invalid
/// directive — the error message always names the env variable so operators
/// know what to fix.
pub fn load_env_filter(default_filter: &str) -> Result<EnvFilter> {
    let raw = strict_env_filter_source(default_filter)?;
    EnvFilter::try_new(&raw).map_err(|error| {
        eyre!(
            "Invalid {} directive `{raw}`: {error}",
            EnvFilter::DEFAULT_ENV
        )
    })
}

/// Install a global `tracing` subscriber with the given format and default
/// filter.
///
/// Reads `RUST_LOG` for filter directives; falls back to `default_filter` if
/// unset. Both formats include target name and thread IDs in output.
///
/// Returns an error if `RUST_LOG` is malformed or if a subscriber is already
/// installed in the current process.
pub fn install_tracing(format: TracingFormat, default_filter: &str) -> Result<()> {
    let env_filter = load_env_filter(default_filter)?;

    let result = match format {
        TracingFormat::Json => tracing_subscriber::fmt()
            .json()
            .with_writer(std::io::stderr)
            .with_env_filter(env_filter)
            .with_target(true)
            .with_thread_ids(true)
            .try_init(),
        TracingFormat::Text => tracing_subscriber::fmt()
            .with_writer(std::io::stderr)
            .with_env_filter(env_filter)
            .with_target(true)
            .with_thread_ids(true)
            .try_init(),
    };

    result.map_err(|error| eyre!("Failed to initialize tracing subscriber: {error}"))
}

/// Spawn a task that waits for SIGINT/SIGTERM and flips a `watch` channel.
///
/// Returns the `Receiver` half. Long-running async code should listen for
/// `*receiver.borrow() == true` (or `receiver.changed().await`) to drain
/// gracefully.
///
/// `service_name` appears in the shutdown log line so multi-service hosts can
/// tell which service handled the signal.
#[must_use]
pub fn spawn_shutdown_task(service_name: &'static str) -> watch::Receiver<bool> {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    tokio::spawn(async move {
        match crate::wait_for_os_shutdown_signal().await {
            Ok(signal_name) => {
                info!(
                    service = service_name,
                    signal = signal_name,
                    "Received shutdown signal, initiating graceful shutdown"
                );
            }
            Err(error) => {
                error!(
                    service = service_name,
                    error = %error,
                    "Failed to listen for shutdown signal"
                );
            }
        }

        if shutdown_tx.send(true).is_err() {
            warn!(
                service = service_name,
                "shutdown receiver was already dropped before signal delivery"
            );
        }
    });

    shutdown_rx
}

#[cfg(test)]
mod tests {
    use super::*;
    use xtask::sandbox::EnvGuard;
    use xtask::sandbox::prelude::*;

    #[sinex_serial_test]
    async fn load_env_filter_defaults_when_rust_log_is_missing() -> TestResult<()> {
        let mut env = EnvGuard::new();
        env.clear("RUST_LOG");

        load_env_filter("test_service=info")?;
        Ok(())
    }

    #[sinex_serial_test]
    async fn load_env_filter_rejects_invalid_directive() -> TestResult<()> {
        let mut env = EnvGuard::new();
        env.set("RUST_LOG", "test_service=wat");

        let error = load_env_filter("test_service=info")
            .expect_err("invalid directives must fail honestly");
        let message = error.to_string();
        assert!(message.contains("RUST_LOG"));
        assert!(message.contains("test_service=wat"));
        Ok(())
    }
}
