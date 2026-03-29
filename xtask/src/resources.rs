//! System resource checks for job scheduling and command preflight.
//!
//! Provides memory and CPU load checks to warn users before heavy operations.

use color_eyre::eyre::{Result, eyre};

/// Minimum recommended memory in GB for various operations.
pub mod thresholds {
    /// Minimum for `xtask check` (fmt + cargo check)
    pub const CARGO_CHECK_GB: u64 = 2;
    /// Minimum for `xtask test`
    pub const CARGO_TEST_GB: u64 = 6;
    /// Minimum for `xtask ci-preflight` or full workspace builds
    pub const FULL_CI_GB: u64 = 8;
}

/// Current system resource status.
#[derive(Debug, Clone)]
pub struct ResourceStatus {
    /// Available memory in GB, or `None` when the platform does not expose it.
    pub memory_available_gb: Option<f64>,
    /// Total system memory in GB, or `None` when the platform does not expose it.
    pub memory_total_gb: Option<f64>,
    /// 1-minute load average, or `None` when the platform does not expose it.
    pub load_1min: Option<f64>,
    /// Number of CPU cores
    pub cpu_count: usize,
}

impl ResourceStatus {
    /// Capture current system resource status.
    pub fn capture() -> Result<Self> {
        let memory_gb = memory_info()?.map(|(available_kb, total_kb)| {
            (
                available_kb as f64 / 1024.0 / 1024.0,
                total_kb as f64 / 1024.0 / 1024.0,
            )
        });
        let load = load_1min()?;
        let cpu_count = num_cpus::get();

        Ok(Self {
            memory_available_gb: memory_gb.map(|(available, _)| available),
            memory_total_gb: memory_gb.map(|(_, total)| total),
            load_1min: load,
            cpu_count,
        })
    }

    /// Check if enough memory is available for an operation.
    #[must_use]
    pub fn has_memory_for(&self, required_gb: u64) -> bool {
        self.memory_available_gb
            .is_none_or(|available| available >= required_gb as f64)
    }

    /// Check if system load is acceptable (not overloaded).
    #[must_use]
    pub fn load_acceptable(&self) -> bool {
        // Consider overloaded if load > 90% of CPU count
        self.load_1min
            .is_none_or(|load_1min| load_1min < (self.cpu_count as f64 * 0.9))
    }

    /// Get a warning message if resources are constrained.
    ///
    /// Returns `Some(warning)` if memory is low or load is high.
    #[must_use]
    pub fn warning(&self, required_gb: u64) -> Option<String> {
        let mut warnings = Vec::new();

        if let Some(available_gb) = self.memory_available_gb
            && available_gb < required_gb as f64
        {
            warnings.push(format!(
                "Low memory: {:.1}GB available, {}GB recommended",
                available_gb, required_gb
            ));
        }

        if let Some(load_1min) = self.load_1min
            && !self.load_acceptable()
        {
            warnings.push(format!(
                "High system load: {:.1} (1min avg) on {} CPUs",
                load_1min, self.cpu_count
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
        let memory = match (self.memory_available_gb, self.memory_total_gb) {
            (Some(available), Some(total)) => format!("{available:.1}/{total:.1}GB free"),
            _ => "unavailable".to_string(),
        };
        let load = self
            .load_1min
            .map(|load_1min| format!("{load_1min:.2}"))
            .unwrap_or_else(|| "unavailable".to_string());
        format!("Memory: {memory}, Load: {load} ({} CPUs)", self.cpu_count)
    }
}

/// Read memory information from /proc/meminfo.
///
/// Returns `Ok(Some((available_kb, total_kb)))`, or `Ok(None)` if /proc/meminfo is
/// absent (non-Linux). Read and parse failures are returned explicitly so callers
/// can surface honest diagnostics instead of fabricating resource values.
fn memory_info() -> Result<Option<(u64, u64)>> {
    let content = match std::fs::read_to_string("/proc/meminfo") {
        Ok(content) => content,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(eyre!(error).wrap_err("failed to read /proc/meminfo"));
        }
    };

    parse_memory_info(&content).map(Some)
}

fn parse_memory_info(content: &str) -> Result<(u64, u64)> {
    let mut available = None;
    let mut total = None;

    fn parse_meminfo_kb(content: &str, line: &str, field: &str) -> Result<u64> {
        let size_str = line
            .split_whitespace()
            .nth(1)
            .ok_or_else(|| eyre!("/proc/meminfo entry {field} was missing its numeric value"))?;

        size_str.parse::<u64>().map_err(|error| {
            eyre!(error).wrap_err(format!(
                "failed to parse /proc/meminfo entry {field}: {content}"
            ))
        })
    }

    for line in content.lines() {
        if line.starts_with("MemAvailable:") {
            available = Some(parse_meminfo_kb(content, line, "MemAvailable")?);
        } else if line.starts_with("MemTotal:") {
            total = Some(parse_meminfo_kb(content, line, "MemTotal")?);
        }
    }

    let available = available.ok_or_else(|| eyre!("/proc/meminfo is missing MemAvailable"))?;
    let total = total.ok_or_else(|| eyre!("/proc/meminfo is missing MemTotal"))?;

    Ok((available, total))
}

/// Read 1-minute load average from /proc/loadavg.
fn load_1min() -> Result<Option<f64>> {
    let content = match std::fs::read_to_string("/proc/loadavg") {
        Ok(content) => content,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(eyre!(error).wrap_err("failed to read /proc/loadavg"));
        }
    };

    parse_load_1min(&content).map(Some)
}

fn parse_load_1min(content: &str) -> Result<f64> {
    let raw = content
        .split_whitespace()
        .next()
        .ok_or_else(|| eyre!("/proc/loadavg did not contain a 1-minute load value"))?;

    raw.parse::<f64>()
        .map_err(|error| eyre!(error).wrap_err("failed to parse 1-minute load average"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::sinex_test;

    #[sinex_test]
    async fn test_resource_capture() -> TestResult<()> {
        // Should not panic, even if /proc doesn't exist (non-Linux)
        let status = ResourceStatus::capture()?;
        // On Linux, these should be > 0
        if cfg!(target_os = "linux") {
            assert!(status.cpu_count > 0);
        }
        Ok(())
    }

    #[sinex_test]
    async fn test_warning_low_memory() -> TestResult<()> {
        let status = ResourceStatus {
            memory_available_gb: Some(3.0),
            memory_total_gb: Some(32.0),
            load_1min: Some(1.0),
            cpu_count: 8,
        };
        let warning = status.warning(8);
        assert!(warning.is_some());
        assert!(warning.unwrap().contains("Low memory"));
        Ok(())
    }

    #[sinex_test]
    async fn test_warning_high_load() -> TestResult<()> {
        let status = ResourceStatus {
            memory_available_gb: Some(16.0),
            memory_total_gb: Some(32.0),
            load_1min: Some(15.0),
            cpu_count: 8,
        };
        let warning = status.warning(8);
        assert!(warning.is_some());
        assert!(warning.unwrap().contains("High system load"));
        Ok(())
    }

    #[sinex_test]
    async fn test_no_warning_when_ok() -> TestResult<()> {
        let status = ResourceStatus {
            memory_available_gb: Some(16.0),
            memory_total_gb: Some(32.0),
            load_1min: Some(2.0),
            cpu_count: 8,
        };
        assert!(status.warning(8).is_none());
        Ok(())
    }

    #[sinex_test]
    async fn test_summary_reports_missing_memory_honestly() -> TestResult<()> {
        let status = ResourceStatus {
            memory_available_gb: None,
            memory_total_gb: None,
            load_1min: Some(2.0),
            cpu_count: 8,
        };

        assert_eq!(status.summary(), "Memory: unavailable, Load: 2.00 (8 CPUs)");
        assert!(status.warning(8).is_none());
        assert!(status.has_memory_for(8));
        Ok(())
    }

    #[sinex_test]
    async fn test_summary_reports_missing_load_honestly() -> TestResult<()> {
        let status = ResourceStatus {
            memory_available_gb: Some(16.0),
            memory_total_gb: Some(32.0),
            load_1min: None,
            cpu_count: 8,
        };

        assert_eq!(status.summary(), "Memory: 16.0/32.0GB free, Load: unavailable (8 CPUs)");
        assert!(status.warning(8).is_none());
        assert!(status.load_acceptable());
        Ok(())
    }

    #[sinex_test]
    async fn test_parse_load_1min_rejects_invalid_first_field() -> TestResult<()> {
        let error = parse_load_1min("not-a-number 0.00 0.00 1/1 1").unwrap_err();
        let rendered = format!("{error:#}");
        assert!(rendered.contains("failed to parse 1-minute load average"));
        Ok(())
    }

    #[sinex_test]
    async fn test_parse_load_1min_rejects_missing_first_field() -> TestResult<()> {
        let error = parse_load_1min("").unwrap_err();
        let rendered = format!("{error:#}");
        assert!(rendered.contains("/proc/loadavg did not contain a 1-minute load value"));
        Ok(())
    }

    #[sinex_test]
    async fn test_parse_memory_info_rejects_missing_memavailable() -> TestResult<()> {
        let error = parse_memory_info("MemTotal: 1024 kB\n").unwrap_err();
        let rendered = format!("{error:#}");
        assert!(rendered.contains("/proc/meminfo is missing MemAvailable"));
        Ok(())
    }

    #[sinex_test]
    async fn test_parse_memory_info_rejects_invalid_memtotal() -> TestResult<()> {
        let error = parse_memory_info("MemAvailable: 512 kB\nMemTotal: no\n").unwrap_err();
        let rendered = format!("{error:#}");
        assert!(rendered.contains("failed to parse /proc/meminfo entry MemTotal"));
        Ok(())
    }
}
