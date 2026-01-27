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

/// High-level chaos helper used by integration/chaos suites.
#[derive(Clone, Debug)]
pub struct ChaosInjestor {
    config: ChaosConfig,
}

impl ChaosInjestor {
    pub fn new(latency: Duration, failure_rate: f64) -> Self {
        Self {
            config: ChaosConfig::new(latency, failure_rate),
        }
    }

    /// Execute an async operation with optional simulated failures/latency.
    pub async fn with_simulated_failures<F, Fut, T>(&self, op: F) -> Result<T>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        self.config.inject(op).await
    }

    /// Simulate a temporary network partition.
    pub async fn simulate_network_partition(&self) -> Result<()> {
        self.config.partition(Duration::ZERO).await;
        Ok(())
    }

    /// Simulate a database crash for callers that expect a failure.
    pub async fn simulate_database_crash(&self) -> Result<()> {
        Err(eyre!("simulated database crash"))
    }
}
