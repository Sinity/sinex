//! Lightweight chaos utilities for test scenarios.
//!
//! These helpers allow injecting latency/failure into async operations and
//! simulating temporary partitions without forcing production code to change.

use std::time::Duration;

use color_eyre::eyre::{eyre, Result};
use rand::Rng;
use tokio::time::sleep;

/// Chaos injection settings.
#[derive(Clone, Copy, Debug)]
pub struct ChaosConfig {
    pub latency: Duration,
    pub failure_rate: f64,
}

impl ChaosConfig {
    pub fn new(latency: Duration, failure_rate: f64) -> Self {
        Self {
            latency,
            failure_rate: failure_rate.clamp(0.0, 1.0),
        }
    }

    /// Apply latency then randomly fail based on `failure_rate`.
    pub async fn inject<T, F, Fut>(&self, op: F) -> Result<T>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        if !self.latency.is_zero() {
            sleep(self.latency).await;
        }
        if self.failure_rate > 0.0 {
            let mut rng = rand::thread_rng();
            if rng.gen_bool(self.failure_rate) {
                return Err(eyre!("chaos: induced failure"));
            }
        }
        op().await
    }

    /// Simulate a transient partition by sleeping for the given duration.
    pub async fn partition(&self, duration: Duration) {
        let delay = if duration.is_zero() {
            self.latency
        } else {
            duration
        };
        if !delay.is_zero() {
            sleep(delay).await;
        }
    }
}
