use std::time::Duration;

use color_eyre::eyre::{eyre, Result};
use rand::Rng;
use tokio::time::sleep;

/// Lightweight chaos simulator used by integration tests.
#[derive(Debug, Clone, Copy)]
pub struct ChaosInjestor {
    latency: Duration,
    failure_rate: f64,
}

impl ChaosInjestor {
    /// Create a new chaos helper with the provided latency + failure knobs.
    pub fn new(latency: Duration, failure_rate: f64) -> Self {
        Self {
            latency,
            failure_rate: failure_rate.clamp(0.0, 1.0),
        }
    }

    /// Run the provided future while optionally injecting latency/failures.
    pub async fn with_simulated_failures<Fut, T>(&self, operation: Fut) -> Result<T>
    where
        Fut: std::future::Future<Output = Result<T>>,
    {
        self.inject_latency().await;
        self.maybe_fail("chaos failure before operation")?;
        let result = operation.await;
        match result {
            Ok(value) => {
                self.maybe_fail("chaos failure after operation")?;
                Ok(value)
            }
            Err(err) => Err(err),
        }
    }

    /// Simulate a temporary network partition.
    pub async fn simulate_network_partition(&self) -> Result<()> {
        self.inject_latency().await;
        self.maybe_fail("simulated network partition")
    }

    /// Simulate an abrupt database crash.
    pub async fn simulate_database_crash(&self) -> Result<()> {
        Err(eyre!(
            "simulated database crash (chaos failure rate = {:.2})",
            self.failure_rate
        ))
    }

    async fn inject_latency(&self) {
        if !self.latency.is_zero() {
            sleep(self.latency).await;
        }
    }

    fn maybe_fail(&self, message: &str) -> Result<()> {
        if self.failure_rate > 0.0 {
            let mut rng = rand::thread_rng();
            if rng.gen_bool(self.failure_rate) {
                return Err(eyre!("{}", message));
            }
        }
        Ok(())
    }
}

impl Default for ChaosInjestor {
    fn default() -> Self {
        Self::new(Duration::from_millis(0), 0.0)
    }
}
