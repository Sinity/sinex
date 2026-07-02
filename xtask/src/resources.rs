//! System resource checks for job scheduling and command preflight.
//!
//! Provides memory, CPU load, and PSI checks before heavy operations.

use color_eyre::eyre::{Result, eyre};

use crate::process::PressureSnapshot;

/// Minimum recommended memory in GB for various operations.
pub mod thresholds {
    /// Minimum for `xtask check` (fmt + cargo check)
    pub const CARGO_CHECK_GB: u64 = 2;
    /// Minimum for `xtask test`
    pub const CARGO_TEST_GB: u64 = 6;
    /// Minimum for `xtask ci-preflight` or full workspace builds
    pub const FULL_CI_GB: u64 = 8;

    /// Warn before broad checks/tests when the host is already visibly stalled
    /// on IO. This threshold is intentionally low: `io.full` means every
    /// runnable non-idle task was waiting on IO, so even single-digit values
    /// can make an interactive workstation feel sticky.
    pub const PSI_IO_FULL_WARN: f64 = 3.0;
    /// Refuse broad checks/tests unless explicitly overridden.
    pub const PSI_IO_FULL_REFUSE: f64 = 10.0;
    /// Warn when memory stalls are present before broad work starts.
    pub const PSI_MEMORY_FULL_WARN: f64 = 5.0;
    /// Refuse broad checks/tests unless explicitly overridden.
    pub const PSI_MEMORY_FULL_REFUSE: f64 = 10.0;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PressureLevel {
    Clear,
    Elevated,
    Severe,
}

#[derive(Debug, Clone)]
pub struct PressureRecommendation {
    pub level: PressureLevel,
    pub cpu_some_avg10: Option<f64>,
    pub io_some_avg10: Option<f64>,
    pub io_full_avg10: Option<f64>,
    pub memory_some_avg10: Option<f64>,
    pub memory_full_avg10: Option<f64>,
    pub shm_used_mb: Option<f64>,
    pub shm_free_mb: Option<f64>,
}

impl PressureRecommendation {
    #[must_use]
    pub fn capture() -> Self {
        let cpu = crate::process::read_pressure_snapshot("cpu");
        let io = crate::process::read_pressure_snapshot("io");
        let memory = crate::process::read_pressure_snapshot("memory");
        let shm = crate::process::shm_usage_mb();
        Self::from_snapshots(cpu, io, memory, shm)
    }

    #[must_use]
    pub fn from_snapshots(
        cpu: PressureSnapshot,
        io: PressureSnapshot,
        memory: PressureSnapshot,
        shm: Option<(f64, f64)>,
    ) -> Self {
        let io_full = io.full_avg10.unwrap_or(0.0);
        let memory_full = memory.full_avg10.unwrap_or(0.0);
        let level = if io_full >= thresholds::PSI_IO_FULL_REFUSE
            || memory_full >= thresholds::PSI_MEMORY_FULL_REFUSE
        {
            PressureLevel::Severe
        } else if io_full >= thresholds::PSI_IO_FULL_WARN
            || memory_full >= thresholds::PSI_MEMORY_FULL_WARN
        {
            PressureLevel::Elevated
        } else {
            PressureLevel::Clear
        };

        Self {
            level,
            cpu_some_avg10: cpu.some_avg10,
            io_some_avg10: io.some_avg10,
            io_full_avg10: io.full_avg10,
            memory_some_avg10: memory.some_avg10,
            memory_full_avg10: memory.full_avg10,
            shm_used_mb: shm.map(|(used, _)| used),
            shm_free_mb: shm.map(|(_, free)| free),
        }
    }

    #[must_use]
    pub fn warning(&self, workload: &str) -> Option<String> {
        if self.level == PressureLevel::Clear {
            return None;
        }
        Some(format!(
            "Host pressure before {workload}: {}. Broad work is demoted, but starting now may still add latency.",
            self.summary()
        ))
    }

    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "io.full avg10 {}, memory.full avg10 {}, cpu.some avg10 {}",
            format_optional_percent(self.io_full_avg10),
            format_optional_percent(self.memory_full_avg10),
            format_optional_percent(self.cpu_some_avg10)
        )
    }

    #[must_use]
    pub fn recommendation(&self) -> &'static str {
        match self.level {
            PressureLevel::Clear => {
                "Pressure is low enough for normal scoped work. Broad work can start if it is actually needed."
            }
            PressureLevel::Elevated => {
                "Prefer scoped checks/tests now. Broad work is allowed but should stay backgrounded and low-priority; use `xtask analytics pressure --top-io` if the machine feels stuck."
            }
            PressureLevel::Severe => {
                "Broad work is allowed, but current pressure is severe. Use `xtask analytics pressure --top-io` to attribute IO if the machine feels stuck."
            }
        }
    }
}

fn format_optional_percent(value: Option<f64>) -> String {
    value.map_or_else(|| "unavailable".to_string(), |value| format!("{value:.2}%"))
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
                "Low memory: {available_gb:.1}GB available, {required_gb}GB recommended",
            ));
        }

        if let Some(load_1min) = self.load_1min
            && !self.load_acceptable()
        {
            warnings.push(format!(
                "High system load: {load_1min:.1} (1min avg) on {} CPUs",
                self.cpu_count
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
        let load = self.load_1min.map_or_else(
            || "unavailable".to_string(),
            |load_1min| format!("{load_1min:.2}"),
        );
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
#[path = "resources_test.rs"]
mod tests;
