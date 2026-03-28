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
        self.run_with_shutdown_signal(tokio::signal::ctrl_c(), move |first| tick_fn(first))
            .await
    }

    async fn run_with_shutdown_signal<S, F, Fut>(&self, shutdown_signal: S, mut tick_fn: F) -> Result<()>
    where
        S: std::future::Future<Output = std::io::Result<()>>,
        F: FnMut(bool) -> Fut,
        Fut: std::future::Future<Output = Result<WatchAction>>,
    {
        let shutdown_signal = shutdown_signal;
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
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use super::{WatchAction, WatchLoop};
    use xtask::sandbox::sinex_test;

    #[sinex_test]
    async fn test_watch_loop_stops_on_shutdown_signal() -> ::xtask::sandbox::TestResult<()> {
        let ticks = Arc::new(AtomicUsize::new(0));
        let loop_ = WatchLoop::new(Duration::from_millis(1));

        loop_
            .run_with_shutdown_signal(std::future::ready(Ok(())), {
                let ticks = ticks.clone();
                move |_| {
                    let ticks = ticks.clone();
                    async move {
                        ticks.fetch_add(1, Ordering::SeqCst);
                        Ok(WatchAction::Continue)
                    }
                }
            })
            .await?;

        assert_eq!(ticks.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[sinex_test]
    async fn test_watch_loop_surfaces_shutdown_listener_failure()
    -> ::xtask::sandbox::TestResult<()> {
        let loop_ = WatchLoop::new(Duration::from_millis(1));

        let error = loop_
            .run_with_shutdown_signal(
                std::future::ready(Err(std::io::Error::other("ctrl-c unavailable"))),
                |_| async { Ok(WatchAction::Continue) },
            )
            .await
            .expect_err("shutdown listener failure should surface");

        let message = format!("{error:#}");
        assert!(message.contains("failed to wait for Ctrl+C in watch loop"));
        assert!(message.contains("ctrl-c unavailable"));
        Ok(())
    }
}
