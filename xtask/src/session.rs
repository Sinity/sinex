//! Generic watch/follow loop for live streaming of changing state.
//!
//! `WatchLoop` provides a reusable polling abstraction for `--watch` and
//! `--follow` modes. It handles Ctrl+C shutdown, configurable interval, and
//! an optional tick limit, so each command only needs to supply the per-tick
//! logic.

use std::time::Duration;

use color_eyre::eyre::{Result, WrapErr};

/// Action returned by a `WatchLoop` tick function.
pub enum WatchAction {
    /// Continue polling after the next interval.
    Continue,
    /// Stop the loop cleanly (job completed, terminal state reached, etc.).
    Stop,
}

/// A reusable polling loop for watch/follow modes.
///
/// Calls `tick_fn` on every interval. Stops when:
/// - `tick_fn` returns [`WatchAction::Stop`]
/// - Ctrl+C is received
pub struct WatchLoop {
    interval: Duration,
}

impl WatchLoop {
    /// Create a new `WatchLoop` with the given interval.
    #[must_use]
    pub fn new(interval: Duration) -> Self {
        Self { interval }
    }

    /// Convenience constructor: interval in whole seconds.
    #[must_use]
    pub fn with_interval_secs(secs: u64) -> Self {
        Self::new(Duration::from_secs(secs))
    }

    /// Run `tick_fn` repeatedly.
    ///
    /// `tick_fn(first_tick)` receives `true` on the very first call so callers
    /// can skip setup work on subsequent ticks. Returns `Ok(())` on clean stop
    /// (Ctrl+C or `WatchAction::Stop`).
    pub async fn run<F, Fut>(&self, tick_fn: F) -> Result<()>
    where
        F: FnMut(bool) -> Fut,
        Fut: std::future::Future<Output = Result<WatchAction>>,
    {
        self.run_with_shutdown_signal(tokio::signal::ctrl_c(), tick_fn)
            .await
    }

    async fn run_with_shutdown_signal<S, F, Fut>(
        &self,
        shutdown_signal: S,
        mut tick_fn: F,
    ) -> Result<()>
    where
        S: std::future::Future<Output = std::io::Result<()>>,
        F: FnMut(bool) -> Fut,
        Fut: std::future::Future<Output = Result<WatchAction>>,
    {
        tokio::pin!(shutdown_signal);
        let mut first = true;
        loop {
            match tick_fn(first).await? {
                WatchAction::Stop => break,
                WatchAction::Continue => {}
            }
            first = false;
            tokio::select! {
                () = tokio::time::sleep(self.interval) => {}
                result = &mut shutdown_signal => {
                    result.wrap_err("failed to wait for Ctrl+C in watch loop")?;
                    break;
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "session_test.rs"]
mod tests;
