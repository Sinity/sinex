//! Linux PSI (Pressure Stall Information) monitor.
//!
//! Reads `/proc/pressure/io` and `/proc/pressure/memory` to detect
//! resource contention so callers can apply bounded backoff before
//! heavy IO or memory-intensive operations (e.g. large CAS writes).

use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, warn};

/// Default thresholds: back off when IO pressure averages above 50%
/// or memory pressure averages above 50% over the 60-second window.
const DEFAULT_IO_THRESHOLD: f64 = 50.0;
const DEFAULT_MEMORY_THRESHOLD: f64 = 50.0;
const DEFAULT_CHECK_INTERVAL: Duration = Duration::from_secs(2);

#[cfg(target_os = "linux")]
#[derive(Debug, Clone)]
pub struct PressureMonitor {
    io_threshold: f64,
    memory_threshold: f64,
    check_interval: Duration,
}

#[cfg(not(target_os = "linux"))]
#[derive(Debug, Clone, Default)]
pub struct PressureMonitor;

#[cfg(target_os = "linux")]
impl PressureMonitor {
    /// Create a new pressure monitor with the given thresholds.
    ///
    /// `io_threshold` and `memory_threshold` are percentages (0.0-100.0).
    /// When the `some` avg10 value from `/proc/pressure/{io,memory}` exceeds
    /// the threshold, `should_backoff` returns `true`.
    #[must_use]
    pub fn new(io_threshold: f64, memory_threshold: f64) -> Self {
        Self {
            io_threshold,
            memory_threshold,
            check_interval: DEFAULT_CHECK_INTERVAL,
        }
    }

    /// Create a monitor with default thresholds (50% for both IO and memory).
    #[must_use]
    pub fn default_thresholds() -> Self {
        Self {
            io_threshold: DEFAULT_IO_THRESHOLD,
            memory_threshold: DEFAULT_MEMORY_THRESHOLD,
            check_interval: DEFAULT_CHECK_INTERVAL,
        }
    }

    /// Parse the `some avg10` value from a PSI line such as:
    /// `some avg10=12.34 avg60=8.90 avg300=5.67 total=12345678`
    fn parse_psi_avg10(line: &str) -> Option<f64> {
        let some_segment = line
            .split_whitespace()
            .find(|token| token.starts_with("some"))?;
        // `some` segment format: `some avg10=X.XX avg60=Y.YY avg300=Z.ZZ total=N`
        let avg10_segment = some_segment
            .split_whitespace()
            .find(|token| token.starts_with("avg10="))?;
        avg10_segment
            .strip_prefix("avg10=")?
            .parse::<f64>()
            .ok()
    }

    /// Read `/proc/pressure/io` and return `true` if the `some avg10`
    /// value exceeds the configured IO threshold.
    pub fn io_pressure_high(&self) -> bool {
        match Self::read_proc_pressure("io") {
            Ok(avg10) => avg10 > self.io_threshold,
            Err(error) => {
                warn!(
                    error = %error,
                    "Failed to read /proc/pressure/io; treating as no pressure"
                );
                false
            }
        }
    }

    /// Read `/proc/pressure/memory` and return `true` if the `some avg10`
    /// value exceeds the configured memory threshold.
    pub fn memory_pressure_high(&self) -> bool {
        match Self::read_proc_pressure("memory") {
            Ok(avg10) => avg10 > self.memory_threshold,
            Err(error) => {
                warn!(
                    error = %error,
                    "Failed to read /proc/pressure/memory; treating as no pressure"
                );
                false
            }
        }
    }

    /// Return `true` if either IO or memory pressure is above threshold.
    #[must_use]
    pub fn should_backoff(&self) -> bool {
        self.io_pressure_high() || self.memory_pressure_high()
    }

    /// Wait until pressure drops below threshold for both IO and memory.
    ///
    /// Polls at `check_interval` intervals. Returns immediately if pressure
    /// is already below both thresholds.
    pub async fn wait_until_safe(&self) {
        while self.should_backoff() {
            debug!(
                io_threshold = self.io_threshold,
                memory_threshold = self.memory_threshold,
                "PSI pressure above threshold; backing off"
            );
            sleep(self.check_interval).await;
        }
    }

    fn read_proc_pressure(resource: &str) -> Result<f64, std::io::Error> {
        let path = format!("/proc/pressure/{resource}");
        let content = std::fs::read_to_string(&path)?;
        Self::parse_psi_avg10(&content)
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("unable to parse avg10 from {path}: {content}"),
                )
            })
    }
}

#[cfg(not(target_os = "linux"))]
impl PressureMonitor {
    #[must_use]
    pub fn new(_io_threshold: f64, _memory_threshold: f64) -> Self {
        Self
    }

    #[must_use]
    pub fn default_thresholds() -> Self {
        Self
    }

    #[must_use]
    pub fn io_pressure_high(&self) -> bool {
        false
    }

    #[must_use]
    pub fn memory_pressure_high(&self) -> bool {
        false
    }

    #[must_use]
    pub fn should_backoff(&self) -> bool {
        false
    }

    pub async fn wait_until_safe(&self) {
        // no-op on non-Linux
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xtask::sandbox::sinex_test;

    #[sinex_test]
    async fn parse_psi_avg10_extracts_correct_value() -> xtask::sandbox::TestResult<()> {
        let line = "some avg10=12.34 avg60=8.90 avg300=5.67 total=12345678";
        let value = PressureMonitor::parse_psi_avg10(line);
        assert_eq!(value, Some(12.34));
        Ok(())
    }

    #[sinex_test]
    async fn parse_psi_avg10_returns_none_for_malformed_line()
    -> xtask::sandbox::TestResult<()> {
        assert!(PressureMonitor::parse_psi_avg10("garbage").is_none());
        assert!(PressureMonitor::parse_psi_avg10("").is_none());
        Ok(())
    }

    #[sinex_test]
    async fn pressure_monitor_below_threshold_does_not_backoff()
    -> xtask::sandbox::TestResult<()> {
        let monitor = PressureMonitor::new(99.0, 99.0);
        // On non-Linux or systems with actual low pressure, this should be false.
        assert!(!monitor.should_backoff());
        Ok(())
    }

    #[sinex_test]
    async fn non_linux_stub_always_returns_false() -> xtask::sandbox::TestResult<()> {
        let monitor = PressureMonitor::default_thresholds();
        assert!(!monitor.should_backoff());
        assert!(!monitor.io_pressure_high());
        assert!(!monitor.memory_pressure_high());
        Ok(())
    }
}
