//! Generic watch/follow loop for live streaming of changing state.
//!
//! `WatchLoop` provides a reusable polling abstraction for `--watch` and
//! `--follow` modes. It handles Ctrl+C shutdown, configurable interval, and
//! an optional tick limit, so each command only needs to supply the per-tick
//! logic.

use std::time::Duration;

use color_eyre::eyre::Result;

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
    pub fn new(interval: Duration) -> Self {
        Self { interval }
    }

    /// Convenience constructor: interval in whole seconds.
    pub fn with_interval_secs(secs: u64) -> Self {
        Self::new(Duration::from_secs(secs))
    }

    /// Run `tick_fn` repeatedly.
    ///
    /// `tick_fn(first_tick)` receives `true` on the very first call so callers
    /// can skip setup work on subsequent ticks. Returns `Ok(())` on clean stop
    /// (Ctrl+C or `WatchAction::Stop`).
    pub async fn run<F, Fut>(&self, mut tick_fn: F) -> Result<()>
    where
        F: FnMut(bool) -> Fut,
        Fut: std::future::Future<Output = Result<WatchAction>>,
    {
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
        tokio::spawn(async move {
            tokio::signal::ctrl_c().await.ok();
            let _ = shutdown_tx.send(true);
        });

        let mut first = true;
        loop {
            if *shutdown_rx.borrow() {
                break;
            }
            match tick_fn(first).await? {
                WatchAction::Stop => break,
                WatchAction::Continue => {}
            }
            first = false;
            tokio::select! {
                () = tokio::time::sleep(self.interval) => {}
                _ = shutdown_rx.changed() => break,
            }
        }
        Ok(())
    }
}
