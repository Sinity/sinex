//! System resource checks for job scheduling and command preflight.
//!
//! Provides memory and CPU load checks to warn users before heavy operations.

use anyhow::Result;

/// Minimum recommended memory in GB for various operations.
pub mod thresholds {
    /// Minimum for `cargo xtask check` (fmt + cargo check)
    pub const CARGO_CHECK_GB: u64 = 2;
    /// Minimum for `cargo xtask test`
    pub const CARGO_TEST_GB: u64 = 6;
    /// Minimum for `cargo xtask ci-preflight` or full workspace builds
    pub const FULL_CI_GB: u64 = 8;
}

/// Current system resource status.
#[derive(Debug, Clone)]
pub struct ResourceStatus {
    /// Available memory in GB
    pub memory_available_gb: f64,
    /// Total system memory in GB
    pub memory_total_gb: f64,
    /// 1-minute load average
    pub load_1min: f64,
    /// Number of CPU cores
    pub cpu_count: usize,
}

impl ResourceStatus {
    /// Capture current system resource status.
    pub fn capture() -> Result<Self> {
        let (available_kb, total_kb) = memory_info();
        let load = load_1min();
        let cpu_count = num_cpus::get();

        Ok(Self {
            memory_available_gb: available_kb as f64 / 1024.0 / 1024.0,
            memory_total_gb: total_kb as f64 / 1024.0 / 1024.0,
            load_1min: load,
            cpu_count,
        })
    }

    /// Check if enough memory is available for an operation.
    #[must_use]
    pub fn has_memory_for(&self, required_gb: u64) -> bool {
        self.memory_available_gb >= required_gb as f64
    }

    /// Check if system load is acceptable (not overloaded).
    #[must_use]
    pub fn load_acceptable(&self) -> bool {
        // Consider overloaded if load > 90% of CPU count
        self.load_1min < (self.cpu_count as f64 * 0.9)
    }

    /// Get a warning message if resources are constrained.
    ///
    /// Returns `Some(warning)` if memory is low or load is high.
    #[must_use]
    pub fn warning(&self, required_gb: u64) -> Option<String> {
        let mut warnings = Vec::new();

        if !self.has_memory_for(required_gb) {
            warnings.push(format!(
                "Low memory: {:.1}GB available, {}GB recommended",
                self.memory_available_gb, required_gb
            ));
        }

        if !self.load_acceptable() {
            warnings.push(format!(
                "High system load: {:.1} (1min avg) on {} CPUs",
                self.load_1min, self.cpu_count
            ));
        }

        if warnings.is_empty() {
            None
        } else {
            Some(warnings.join(". "))
        }
    }

    /// Get a summary line suitable for preflight display.
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "Memory: {:.1}/{:.1}GB free, Load: {:.2} ({} CPUs)",
            self.memory_available_gb, self.memory_total_gb, self.load_1min, self.cpu_count
        )
    }
}

/// Read memory information from /proc/meminfo.
/// Returns (`available_kb`, `total_kb`).
fn memory_info() -> (u64, u64) {
    let content = match std::fs::read_to_string("/proc/meminfo") {
        Ok(c) => c,
        Err(_) => return (0, 0),
    };

    let mut available = 0u64;
    let mut total = 0u64;

    for line in content.lines() {
        if line.starts_with("MemAvailable:") {
            if let Some(size_str) = line.split_whitespace().nth(1) {
                if let Ok(size) = size_str.parse::<u64>() {
                    available = size;
                }
            }
        } else if line.starts_with("MemTotal:") {
            if let Some(size_str) = line.split_whitespace().nth(1) {
                if let Ok(size) = size_str.parse::<u64>() {
                    total = size;
                }
            }
        }
    }

    (available, total)
}

/// Read 1-minute load average from /proc/loadavg.
fn load_1min() -> f64 {
    std::fs::read_to_string("/proc/loadavg")
        .ok()
        .and_then(|s| s.split_whitespace().next().and_then(|v| v.parse().ok()))
        .unwrap_or(0.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resource_capture() {
        // Should not panic, even if /proc doesn't exist (non-Linux)
        let status = ResourceStatus::capture().unwrap();
        // On Linux, these should be > 0
        if cfg!(target_os = "linux") {
            assert!(status.cpu_count > 0);
        }
    }

    #[test]
    fn test_warning_low_memory() {
        let status = ResourceStatus {
            memory_available_gb: 3.0,
            memory_total_gb: 32.0,
            load_1min: 1.0,
            cpu_count: 8,
        };
        let warning = status.warning(8);
        assert!(warning.is_some());
        assert!(warning.unwrap().contains("Low memory"));
    }

    #[test]
    fn test_warning_high_load() {
        let status = ResourceStatus {
            memory_available_gb: 16.0,
            memory_total_gb: 32.0,
            load_1min: 15.0,
            cpu_count: 8,
        };
        let warning = status.warning(8);
        assert!(warning.is_some());
        assert!(warning.unwrap().contains("High system load"));
    }

    #[test]
    fn test_no_warning_when_ok() {
        let status = ResourceStatus {
            memory_available_gb: 16.0,
            memory_total_gb: 32.0,
            load_1min: 2.0,
            cpu_count: 8,
        };
        assert!(status.warning(8).is_none());
    }
}
