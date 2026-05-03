//! Process execution helpers for xtask commands.
//!
//! Provides a fluent builder API for spawning external processes with:
//! - Consistent error handling and context
//! - Automatic output capture and formatting
//! - Special handling for common tools (git, cargo, postgres)
//!
//! # Examples
//!
//! ```no_run
//! use xtask::process::{ProcessBuilder, ProcessOutput};
//!
//! // Simple command execution
//! let output = ProcessBuilder::new("ls")
//!     .args(&["-la", "/tmp"])
//!     .run()?;
//!
//! // Git command with automatic context
//! let output = ProcessBuilder::git()
//!     .args(&["status", "--short"])
//!     .run()?;
//!
//! // Cargo command
//! let output = ProcessBuilder::cargo()
//!     .args(&["build", "--release"])
//!     .run()?;
//! # Ok::<(), color_eyre::eyre::Report>(())
//! ```

use color_eyre::eyre::{Result, WrapErr, bail, eyre};
#[cfg(target_os = "linux")]
use parking_lot::Mutex;
#[cfg(target_os = "linux")]
use serde::{Deserialize, Serialize};
#[cfg(target_os = "linux")]
use std::collections::{HashMap, VecDeque};
#[cfg(target_os = "linux")]
use std::path::Path;
use std::path::PathBuf;
use std::process::{Command, Stdio};
#[cfg(target_os = "linux")]
use std::sync::{
    Arc, LazyLock,
    atomic::{AtomicBool, Ordering},
};
#[cfg(target_os = "linux")]
use std::thread;
#[cfg(target_os = "linux")]
use std::time::{Duration, Instant};

#[cfg(target_os = "linux")]
pub fn configure_managed_child_std(command: &mut Command) {
    use std::os::unix::process::CommandExt;

    // SAFETY: `prctl(PR_SET_PDEATHSIG, SIGKILL)` and `setpgid(0, 0)` are
    // configured in the child between fork and exec. This gives xtask two
    // guarantees:
    // - foreground helpers die if the parent agent/session disappears
    // - the spawned command becomes its own process-group leader, so xtask can
    //   terminate the entire descendant tree rather than only the immediate PID
    unsafe {
        command.pre_exec(|| {
            if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            if libc::setpgid(0, 0) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
}

#[cfg(not(target_os = "linux"))]
pub fn configure_managed_child_std(_command: &mut Command) {}

#[cfg(target_os = "linux")]
pub fn configure_managed_child_tokio(command: &mut tokio::process::Command) {
    // SAFETY: `prctl(PR_SET_PDEATHSIG, SIGKILL)` and `setpgid(0, 0)` are
    // configured in the child between fork and exec so xtask can later reap
    // the entire descendant tree by process group.
    unsafe {
        command.pre_exec(|| {
            if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            if libc::setpgid(0, 0) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
}

#[cfg(not(target_os = "linux"))]
pub fn configure_managed_child_tokio(_command: &mut tokio::process::Command) {}

#[cfg(target_os = "linux")]
pub fn configure_background_job_child_tokio(command: &mut tokio::process::Command) {
    // SAFETY: background xtask jobs should survive the short-lived launcher
    // process, so they intentionally do NOT inherit PR_SET_PDEATHSIG. They do
    // still become their own process-group leader so the coordinator/watchdog
    // can terminate the entire job tree coherently.
    unsafe {
        command.pre_exec(|| {
            if libc::setpgid(0, 0) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
}

#[cfg(not(target_os = "linux"))]
pub fn configure_background_job_child_tokio(_command: &mut tokio::process::Command) {}

#[cfg(target_os = "linux")]
pub fn configure_persistent_service_child_std(command: &mut Command) {
    use std::os::unix::process::CommandExt;

    // SAFETY: long-lived service daemons launched by xtask should not inherit
    // the helper wrapper's lifecycle. They still need their own process group
    // so xtask helpers can be terminated without taking the daemon down with
    // them.
    unsafe {
        command.pre_exec(|| {
            if libc::setpgid(0, 0) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
}

#[cfg(not(target_os = "linux"))]
pub fn configure_persistent_service_child_std(_command: &mut Command) {}

#[cfg(target_os = "linux")]
pub fn arm_current_process_parent_death_signal() -> Result<()> {
    let original_parent = unsafe { libc::getppid() };
    let rc = unsafe { libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) };
    if rc != 0 {
        return Err(std::io::Error::last_os_error())
            .wrap_err("failed to arm xtask parent-death signal");
    }
    if unsafe { libc::getppid() } != original_parent {
        unsafe {
            libc::raise(libc::SIGKILL);
        }
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub fn arm_current_process_parent_death_signal() -> Result<()> {
    Ok(())
}

#[cfg(target_os = "linux")]
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProcessTreeMetrics {
    pub cpu_usage_avg: Option<f64>,
    pub memory_usage_max_mb: Option<f64>,
    pub root_cpu_usage_avg: Option<f64>,
    pub root_memory_usage_max_mb: Option<f64>,
    pub process_count_max: Option<u32>,
    pub sample_count: u32,
}

#[cfg(not(target_os = "linux"))]
#[derive(Debug, Clone, Default)]
pub struct ProcessTreeMetrics {
    pub cpu_usage_avg: Option<f64>,
    pub memory_usage_max_mb: Option<f64>,
    pub root_cpu_usage_avg: Option<f64>,
    pub root_memory_usage_max_mb: Option<f64>,
    pub process_count_max: Option<u32>,
    pub sample_count: u32,
}

#[cfg(target_os = "linux")]
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SharedBuildMetrics {
    pub shared_nix_daemon_cpu_usage_avg: Option<f64>,
    pub shared_nix_daemon_memory_usage_max_mb: Option<f64>,
    pub shared_nix_build_slice_cpu_usage_avg: Option<f64>,
    pub shared_nix_build_slice_memory_usage_max_mb: Option<f64>,
    pub shared_background_slice_cpu_usage_avg: Option<f64>,
    pub shared_background_slice_memory_usage_max_mb: Option<f64>,
}

#[cfg(target_os = "linux")]
impl SharedBuildMetrics {
    #[must_use]
    pub fn has_samples(&self) -> bool {
        self.shared_nix_daemon_cpu_usage_avg.is_some()
            || self.shared_nix_daemon_memory_usage_max_mb.is_some()
            || self.shared_nix_build_slice_cpu_usage_avg.is_some()
            || self.shared_nix_build_slice_memory_usage_max_mb.is_some()
            || self.shared_background_slice_cpu_usage_avg.is_some()
            || self.shared_background_slice_memory_usage_max_mb.is_some()
    }
}

#[cfg(not(target_os = "linux"))]
#[derive(Debug, Clone, Default)]
pub struct SharedBuildMetrics {
    pub shared_nix_daemon_cpu_usage_avg: Option<f64>,
    pub shared_nix_daemon_memory_usage_max_mb: Option<f64>,
    pub shared_nix_build_slice_cpu_usage_avg: Option<f64>,
    pub shared_nix_build_slice_memory_usage_max_mb: Option<f64>,
    pub shared_background_slice_cpu_usage_avg: Option<f64>,
    pub shared_background_slice_memory_usage_max_mb: Option<f64>,
}

#[cfg(not(target_os = "linux"))]
impl SharedBuildMetrics {
    #[must_use]
    pub fn has_samples(&self) -> bool {
        self.shared_nix_daemon_cpu_usage_avg.is_some()
            || self.shared_nix_daemon_memory_usage_max_mb.is_some()
            || self.shared_nix_build_slice_cpu_usage_avg.is_some()
            || self.shared_nix_build_slice_memory_usage_max_mb.is_some()
            || self.shared_background_slice_cpu_usage_avg.is_some()
            || self.shared_background_slice_memory_usage_max_mb.is_some()
    }
}

#[cfg(target_os = "linux")]
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct InvocationResourceMetrics {
    pub process_tree: ProcessTreeMetrics,
    pub shared_build: SharedBuildMetrics,
}

#[cfg(not(target_os = "linux"))]
#[derive(Debug, Clone, Default)]
pub struct InvocationResourceMetrics {
    pub process_tree: ProcessTreeMetrics,
    pub shared_build: SharedBuildMetrics,
}

#[cfg(target_os = "linux")]
#[derive(Debug, Clone)]
struct ProcSample {
    ppid: u32,
    start_ticks: u64,
    total_cpu_ticks: u64,
    rss_pages: u64,
}

#[cfg(target_os = "linux")]
#[derive(Debug, Clone)]
struct TrackedProcessGroup {
    pid: u32,
    start_ticks: u64,
    label: String,
}

#[cfg(target_os = "linux")]
#[derive(Debug, Clone)]
struct ProcessTimeoutHandle {
    pid: u32,
    start_ticks: u64,
    label: String,
}

#[cfg(target_os = "linux")]
#[derive(Debug, Clone)]
struct TrackedProcess {
    pid: u32,
    start_ticks: u64,
}

#[cfg(target_os = "linux")]
static REGISTERED_PROCESS_GROUPS: LazyLock<Mutex<Vec<TrackedProcessGroup>>> =
    LazyLock::new(|| Mutex::new(Vec::new()));

#[cfg(target_os = "linux")]
const INVOCATION_RESOURCE_SAMPLE_INTERVAL_MS: u64 = 100;
#[cfg(target_os = "linux")]
const DEFAULT_CGROUP_ROOT: &str = "/sys/fs/cgroup";
#[cfg(target_os = "linux")]
const NIX_DAEMON_CGROUP_CANDIDATES: &[&str] = &[
    "system.slice/nix-daemon.service",
    "nix.slice/nix-build.slice/nix-daemon.service",
    "nix-daemon.service",
];
#[cfg(target_os = "linux")]
const NIX_BUILD_SLICE_CANDIDATES: &[&str] = &[
    "system.slice/nix-build.slice",
    "nix.slice/nix-build.slice",
    "nix-build.slice",
];
#[cfg(target_os = "linux")]
const BACKGROUND_SLICE_CANDIDATES: &[&str] = &["background.slice"];
#[cfg(target_os = "linux")]
const NIX_DAEMON_CGROUP_DISCOVERY_NAMES: &[&str] = &["nix-daemon.service"];
#[cfg(target_os = "linux")]
const NIX_BUILD_SLICE_DISCOVERY_NAMES: &[&str] = &["nix-build.slice"];
#[cfg(target_os = "linux")]
const BACKGROUND_SLICE_DISCOVERY_NAMES: &[&str] = &["background.slice"];

#[cfg(target_os = "linux")]
fn sysconf_positive(name: libc::c_int, fallback: u64) -> u64 {
    let raw = unsafe { libc::sysconf(name) };
    if raw > 0 { raw as u64 } else { fallback }
}

#[cfg(target_os = "linux")]
fn parse_proc_stat(stat: &str) -> Option<ProcSample> {
    let close = stat.rfind(") ")?;
    let after = stat.get(close + 2..)?;
    let parts: Vec<&str> = after.split_whitespace().collect();
    if parts.len() <= 21 {
        return None;
    }

    let ppid = parts.get(1)?.parse().ok()?;
    let utime: u64 = parts.get(11)?.parse().ok()?;
    let stime: u64 = parts.get(12)?.parse().ok()?;
    let start_ticks = parts.get(19)?.parse().ok()?;
    let rss_pages: i64 = parts.get(21)?.parse().ok()?;

    Some(ProcSample {
        ppid,
        start_ticks,
        total_cpu_ticks: utime.saturating_add(stime),
        rss_pages: rss_pages.max(0) as u64,
    })
}

#[cfg(target_os = "linux")]
#[derive(Debug, Clone, Copy)]
struct CgroupSample {
    cpu_usage_usec: u64,
    memory_bytes: u64,
}

#[cfg(target_os = "linux")]
fn default_cgroup_root() -> PathBuf {
    PathBuf::from(DEFAULT_CGROUP_ROOT)
}

#[cfg(target_os = "linux")]
fn parse_cpu_usage_usec(cpu_stat: &str) -> Option<u64> {
    cpu_stat.lines().find_map(|line| {
        let mut parts = line.split_whitespace();
        match (parts.next(), parts.next()) {
            (Some("usage_usec"), Some(value)) => value.parse::<u64>().ok(),
            _ => None,
        }
    })
}

#[cfg(target_os = "linux")]
fn read_cgroup_sample_from_dir(path: &Path) -> Option<CgroupSample> {
    let cpu_usage_usec =
        parse_cpu_usage_usec(&std::fs::read_to_string(path.join("cpu.stat")).ok()?)?;
    let memory_bytes = std::fs::read_to_string(path.join("memory.current"))
        .ok()?
        .trim()
        .parse::<u64>()
        .ok()?;
    Some(CgroupSample {
        cpu_usage_usec,
        memory_bytes,
    })
}

#[cfg(target_os = "linux")]
fn discover_cgroup_dir_by_basename(
    cgroup_root: &Path,
    basenames: &[&str],
    max_depth: usize,
) -> Option<PathBuf> {
    let mut queue = VecDeque::from([(cgroup_root.to_path_buf(), 0_usize)]);

    while let Some((dir, depth)) = queue.pop_front() {
        if depth >= max_depth {
            continue;
        }

        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };

        for entry in entries.flatten() {
            let Ok(file_type) = entry.file_type() else {
                continue;
            };
            if !file_type.is_dir() {
                continue;
            }

            let path = entry.path();
            let Some(name) = path.file_name().and_then(std::ffi::OsStr::to_str) else {
                continue;
            };
            if basenames.iter().any(|candidate| candidate == &name)
                && read_cgroup_sample_from_dir(&path).is_some()
            {
                return Some(path);
            }

            queue.push_back((path, depth.saturating_add(1)));
        }
    }

    None
}

#[cfg(target_os = "linux")]
fn resolve_cgroup_dir(
    cgroup_root: &Path,
    candidates: &[&str],
    basenames: &[&str],
) -> Option<PathBuf> {
    candidates
        .iter()
        .map(|relative| cgroup_root.join(relative))
        .find(|path| read_cgroup_sample_from_dir(path).is_some())
        .or_else(|| discover_cgroup_dir_by_basename(cgroup_root, basenames, 8))
}

#[cfg(target_os = "linux")]
fn read_proc_sample(pid: u32) -> Option<ProcSample> {
    let path = format!("/proc/{pid}/stat");
    let stat = std::fs::read_to_string(path).ok()?;
    parse_proc_stat(&stat)
}

#[cfg(target_os = "linux")]
fn read_process_table() -> HashMap<u32, ProcSample> {
    let mut table = HashMap::new();
    let Ok(entries) = std::fs::read_dir("/proc") else {
        return table;
    };

    for entry in entries.flatten() {
        let Ok(name) = entry.file_name().into_string() else {
            continue;
        };
        let Ok(pid) = name.parse::<u32>() else {
            continue;
        };
        if let Some(sample) = read_proc_sample(pid) {
            table.insert(pid, sample);
        }
    }

    table
}

#[cfg(target_os = "linux")]
fn collect_process_tree_stats(
    root_pid: u32,
    table: &HashMap<u32, ProcSample>,
) -> Option<(u64, u64, u32)> {
    if !table.contains_key(&root_pid) {
        return None;
    }

    let mut children: HashMap<u32, Vec<u32>> = HashMap::new();
    for (&pid, sample) in table {
        children.entry(sample.ppid).or_default().push(pid);
    }

    let mut queue = VecDeque::from([root_pid]);
    let mut total_cpu_ticks = 0_u64;
    let mut total_rss_pages = 0_u64;
    let mut process_count = 0_u32;

    while let Some(pid) = queue.pop_front() {
        let Some(sample) = table.get(&pid) else {
            continue;
        };
        total_cpu_ticks = total_cpu_ticks.saturating_add(sample.total_cpu_ticks);
        total_rss_pages = total_rss_pages.saturating_add(sample.rss_pages);
        process_count = process_count.saturating_add(1);

        if let Some(descendants) = children.get(&pid) {
            queue.extend(descendants.iter().copied());
        }
    }

    Some((total_cpu_ticks, total_rss_pages, process_count))
}

#[cfg(target_os = "linux")]
fn collect_descendant_processes(
    root_pid: u32,
    table: &HashMap<u32, ProcSample>,
) -> Vec<TrackedProcess> {
    if !table.contains_key(&root_pid) {
        return Vec::new();
    }

    let mut children: HashMap<u32, Vec<u32>> = HashMap::new();
    for (&pid, sample) in table {
        children.entry(sample.ppid).or_default().push(pid);
    }

    let mut queue = VecDeque::from([root_pid]);
    let mut descendants = Vec::new();

    while let Some(pid) = queue.pop_front() {
        if let Some(child_pids) = children.get(&pid) {
            for &child_pid in child_pids {
                queue.push_back(child_pid);
                if let Some(sample) = table.get(&child_pid) {
                    descendants.push(TrackedProcess {
                        pid: child_pid,
                        start_ticks: sample.start_ticks,
                    });
                }
            }
        }
    }

    descendants
}

#[cfg(target_os = "linux")]
fn process_group_matches(tracked: &TrackedProcessGroup) -> bool {
    if let Some(sample) = read_proc_sample(tracked.pid) {
        return sample.start_ticks == tracked.start_ticks;
    }

    let group_exists = unsafe { libc::kill(-(tracked.pid as i32), 0) };
    if group_exists == 0 {
        return true;
    }

    let error = std::io::Error::last_os_error();
    matches!(error.raw_os_error(), Some(libc::EPERM))
}

#[cfg(target_os = "linux")]
fn process_matches(tracked: &TrackedProcess) -> bool {
    read_proc_sample(tracked.pid).is_some_and(|sample| sample.start_ticks == tracked.start_ticks)
}

#[cfg(target_os = "linux")]
fn register_process_group(pid: u32, label: &str) {
    let Some(sample) = read_proc_sample(pid) else {
        return;
    };
    let mut groups = REGISTERED_PROCESS_GROUPS.lock();
    if groups
        .iter()
        .any(|existing| existing.pid == pid && existing.start_ticks == sample.start_ticks)
    {
        return;
    }
    groups.push(TrackedProcessGroup {
        pid,
        start_ticks: sample.start_ticks,
        label: label.to_string(),
    });
}

#[cfg(not(target_os = "linux"))]
fn register_process_group(_pid: u32, _label: &str) {}

#[cfg(target_os = "linux")]
pub fn register_process_group_leader_pid(pid: u32, label: &str) {
    register_process_group(pid, label);
}

#[cfg(not(target_os = "linux"))]
pub fn register_process_group_leader_pid(_pid: u32, _label: &str) {}

#[cfg(target_os = "linux")]
pub fn register_std_child_process_group(child: &std::process::Child, label: &str) {
    register_process_group(child.id(), label);
}

#[cfg(not(target_os = "linux"))]
pub fn register_std_child_process_group(_child: &std::process::Child, _label: &str) {}

pub fn spawn_managed_std_child(
    command: &mut Command,
    label: &str,
) -> std::io::Result<std::process::Child> {
    configure_managed_child_std(command);
    let child = command.spawn()?;
    register_std_child_process_group(&child, label);
    Ok(child)
}

pub fn run_managed_foreground_std_command(
    command: &mut Command,
    label: &str,
) -> std::io::Result<std::process::ExitStatus> {
    let mut child = spawn_managed_std_child(command, label)?;
    child.wait()
}

#[cfg(target_os = "linux")]
pub fn register_tokio_child_process_group(child: &tokio::process::Child, label: &str) {
    if let Some(pid) = child.id() {
        register_process_group(pid, label);
    }
}

#[cfg(not(target_os = "linux"))]
pub fn register_tokio_child_process_group(_child: &tokio::process::Child, _label: &str) {}

#[cfg(unix)]
#[must_use]
pub fn status_indicates_clean_interactive_shutdown(status: &std::process::ExitStatus) -> bool {
    use std::os::unix::process::ExitStatusExt;

    status.success() || matches!(status.signal(), Some(libc::SIGINT | libc::SIGTERM))
}

#[cfg(not(unix))]
#[must_use]
pub fn status_indicates_clean_interactive_shutdown(status: &std::process::ExitStatus) -> bool {
    status.success()
}

#[cfg(target_os = "linux")]
fn send_signal_to_group(pid: u32, signal: libc::c_int) -> std::io::Result<()> {
    let rc = unsafe { libc::kill(-(pid as i32), signal) };
    if rc == 0 {
        return Ok(());
    }
    let group_error = std::io::Error::last_os_error();
    let fallback = unsafe { libc::kill(pid as i32, signal) };
    if fallback == 0 {
        return Ok(());
    }
    let process_error = std::io::Error::last_os_error();
    Err(std::io::Error::new(
        process_error.kind(),
        format!("group signal failed ({group_error}); process signal failed ({process_error})"),
    ))
}

#[cfg(target_os = "linux")]
fn send_signal_to_process(pid: u32, signal: libc::c_int) -> std::io::Result<()> {
    let rc = unsafe { libc::kill(pid as i32, signal) };
    if rc == 0 {
        return Ok(());
    }

    let error = std::io::Error::last_os_error();
    if matches!(error.raw_os_error(), Some(libc::ESRCH)) {
        return Ok(());
    }
    Err(error)
}

#[cfg(target_os = "linux")]
fn terminate_process_group_impl(group: &TrackedProcessGroup, reason: &str) -> Result<bool> {
    if !process_group_matches(group) {
        return Ok(false);
    }

    send_signal_to_group(group.pid, libc::SIGTERM).with_context(|| {
        format!(
            "failed to send SIGTERM to managed process group '{}' (pid {}) while {reason}",
            group.label, group.pid
        )
    })?;

    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        if !process_group_matches(group) {
            return Ok(true);
        }
        thread::sleep(Duration::from_millis(100));
    }

    if process_group_matches(group) {
        send_signal_to_group(group.pid, libc::SIGKILL).with_context(|| {
            format!(
                "failed to send SIGKILL to managed process group '{}' (pid {}) while {reason}",
                group.label, group.pid
            )
        })?;
    }

    Ok(true)
}

#[cfg(target_os = "linux")]
fn terminate_process_group_by_pid(pid: u32, label: &str, reason: &str) -> Result<bool> {
    let Some(sample) = read_proc_sample(pid) else {
        return Ok(false);
    };
    terminate_process_group_impl(
        &TrackedProcessGroup {
            pid,
            start_ticks: sample.start_ticks,
            label: label.to_string(),
        },
        reason,
    )
}

#[cfg(target_os = "linux")]
pub fn terminate_process_group_by_leader_pid(pid: u32, label: &str, reason: &str) -> Result<bool> {
    terminate_process_group_by_pid(pid, label, reason)
}

#[cfg(not(target_os = "linux"))]
pub fn terminate_process_group_by_leader_pid(
    _pid: u32,
    _label: &str,
    _reason: &str,
) -> Result<bool> {
    Ok(false)
}

#[cfg(target_os = "linux")]
fn process_timeout_handle(pid: u32, label: &str) -> Option<ProcessTimeoutHandle> {
    let sample = read_proc_sample(pid)?;
    Some(ProcessTimeoutHandle {
        pid,
        start_ticks: sample.start_ticks,
        label: label.to_string(),
    })
}

#[cfg(target_os = "linux")]
fn process_timeout_handle_matches(handle: &ProcessTimeoutHandle) -> bool {
    read_proc_sample(handle.pid).is_some_and(|sample| sample.start_ticks == handle.start_ticks)
}

#[cfg(target_os = "linux")]
fn terminate_process_timeout_handle(handle: &ProcessTimeoutHandle, reason: &str) -> Result<bool> {
    terminate_process_group_impl(
        &TrackedProcessGroup {
            pid: handle.pid,
            start_ticks: handle.start_ticks,
            label: handle.label.clone(),
        },
        reason,
    )
}

pub struct ProcessTimeoutGuard {
    #[cfg(target_os = "linux")]
    cancel_tx: Option<std::sync::mpsc::Sender<()>>,
    #[cfg(target_os = "linux")]
    timed_out: Arc<AtomicBool>,
}

impl ProcessTimeoutGuard {
    #[must_use]
    pub fn inactive() -> Self {
        Self {
            #[cfg(target_os = "linux")]
            cancel_tx: None,
            #[cfg(target_os = "linux")]
            timed_out: Arc::new(AtomicBool::new(false)),
        }
    }

    #[must_use]
    pub fn start_for_process_group_leader(
        pid: u32,
        label: impl Into<String>,
        timeout: Duration,
        reason: impl Into<String>,
    ) -> Self {
        #[cfg(target_os = "linux")]
        {
            let label = label.into();
            let reason = reason.into();
            let handle = process_timeout_handle(pid, &label);
            let timed_out = Arc::new(AtomicBool::new(false));
            let timed_out_clone = Arc::clone(&timed_out);
            let (cancel_tx, cancel_rx) = std::sync::mpsc::channel::<()>();
            thread::spawn(move || {
                if cancel_rx.recv_timeout(timeout).is_ok() {
                    return;
                }
                timed_out_clone.store(true, Ordering::Relaxed);
                if let Some(handle) = handle {
                    if !process_timeout_handle_matches(&handle) {
                        return;
                    }
                    if let Err(error) = terminate_process_timeout_handle(&handle, &reason) {
                        eprintln!(
                            "⚠️  Failed to terminate timed-out process group '{}' (pid {}): {error:#}",
                            handle.label, handle.pid
                        );
                    }
                }
            });

            Self {
                cancel_tx: Some(cancel_tx),
                timed_out,
            }
        }

        #[cfg(not(target_os = "linux"))]
        {
            let _ = pid;
            let _ = label;
            let _ = timeout;
            let _ = reason;
            Self::inactive()
        }
    }

    pub fn finish(&mut self) -> bool {
        #[cfg(target_os = "linux")]
        {
            if let Some(cancel_tx) = self.cancel_tx.take() {
                let _ = cancel_tx.send(());
            }
            self.timed_out.load(Ordering::Relaxed)
        }

        #[cfg(not(target_os = "linux"))]
        {
            false
        }
    }
}

impl Drop for ProcessTimeoutGuard {
    fn drop(&mut self) {
        let _ = self.finish();
    }
}

#[cfg(target_os = "linux")]
pub fn terminate_registered_process_groups(reason: &str) -> Result<usize> {
    let groups = {
        let mut groups = REGISTERED_PROCESS_GROUPS.lock();
        let snapshot = groups.clone();
        groups.clear();
        snapshot
    };

    let mut terminated = 0_usize;
    for group in groups {
        if terminate_process_group_impl(&group, reason)? {
            terminated += 1;
        }
    }
    Ok(terminated)
}

#[cfg(not(target_os = "linux"))]
pub fn terminate_registered_process_groups(_reason: &str) -> Result<usize> {
    Ok(0)
}

#[cfg(target_os = "linux")]
pub fn terminate_current_process_descendants(reason: &str) -> Result<usize> {
    let root_pid = std::process::id();
    let table = read_process_table();
    let descendants = collect_descendant_processes(root_pid, &table);
    if descendants.is_empty() {
        return Ok(0);
    }

    for descendant in &descendants {
        send_signal_to_process(descendant.pid, libc::SIGTERM).with_context(|| {
            format!(
                "failed to send SIGTERM to descendant process {} while {reason}",
                descendant.pid
            )
        })?;
    }

    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        if descendants
            .iter()
            .all(|descendant| !process_matches(descendant))
        {
            return Ok(descendants.len());
        }
        thread::sleep(Duration::from_millis(100));
    }

    for descendant in &descendants {
        if process_matches(descendant) {
            send_signal_to_process(descendant.pid, libc::SIGKILL).with_context(|| {
                format!(
                    "failed to send SIGKILL to descendant process {} while {reason}",
                    descendant.pid
                )
            })?;
        }
    }

    Ok(descendants.len())
}

#[cfg(not(target_os = "linux"))]
pub fn terminate_current_process_descendants(_reason: &str) -> Result<usize> {
    Ok(0)
}

#[cfg(target_os = "linux")]
pub fn prune_registered_process_groups() -> usize {
    let mut groups = REGISTERED_PROCESS_GROUPS.lock();
    let before = groups.len();
    groups.retain(process_group_matches);
    before.saturating_sub(groups.len())
}

#[cfg(not(target_os = "linux"))]
pub fn prune_registered_process_groups() -> usize {
    0
}

#[cfg(target_os = "linux")]
pub fn terminate_std_child_process_group(
    child: &mut std::process::Child,
    label: &str,
    reason: &str,
) -> Result<bool> {
    let pid = child.id();
    terminate_process_group_by_pid(pid, label, reason)
}

#[cfg(not(target_os = "linux"))]
pub fn terminate_std_child_process_group(
    _child: &mut std::process::Child,
    _label: &str,
    _reason: &str,
) -> Result<bool> {
    Ok(false)
}

#[cfg(target_os = "linux")]
pub fn terminate_tokio_child_process_group(
    child: &mut tokio::process::Child,
    label: &str,
    reason: &str,
) -> Result<bool> {
    let Some(pid) = child.id() else {
        return Ok(false);
    };
    terminate_process_group_by_pid(pid, label, reason)
}

#[cfg(not(target_os = "linux"))]
pub fn terminate_tokio_child_process_group(
    _child: &mut tokio::process::Child,
    _label: &str,
    _reason: &str,
) -> Result<bool> {
    Ok(false)
}

#[cfg(target_os = "linux")]
#[derive(Default)]
struct ResourceAccumulator {
    last_tree_cpu_ticks: Option<u64>,
    tree_cpu_sum_pct: f64,
    tree_cpu_sample_count: u32,
    max_tree_rss_bytes: u64,
    last_root_cpu_ticks: Option<u64>,
    root_cpu_sum_pct: f64,
    root_cpu_sample_count: u32,
    max_root_rss_bytes: u64,
    last_sample_at: Option<Instant>,
    max_process_count: u32,
    sample_count: u32,
}

#[cfg(target_os = "linux")]
impl ResourceAccumulator {
    fn record_cpu_sample(
        last_ticks: &mut Option<u64>,
        cpu_sum_pct: &mut f64,
        cpu_sample_count: &mut u32,
        total_cpu_ticks: u64,
        now: Instant,
        last_sample_at: Option<Instant>,
        clock_ticks_per_sec: u64,
        cpu_count: usize,
    ) {
        if let (Some(previous_ticks), Some(previous_time)) = (*last_ticks, last_sample_at) {
            let elapsed = now.saturating_duration_since(previous_time).as_secs_f64();
            if elapsed > 0.0 && clock_ticks_per_sec > 0 && cpu_count > 0 {
                let delta_ticks = total_cpu_ticks.saturating_sub(previous_ticks) as f64;
                let cpu_seconds = delta_ticks / clock_ticks_per_sec as f64;
                let cpu_pct = cpu_seconds / elapsed / cpu_count as f64 * 100.0;
                *cpu_sum_pct += cpu_pct.max(0.0);
                *cpu_sample_count = cpu_sample_count.saturating_add(1);
            }
        }

        *last_ticks = Some(total_cpu_ticks);
    }

    fn record(
        &mut self,
        tree_cpu_ticks: u64,
        tree_rss_pages: u64,
        root_cpu_ticks: u64,
        root_rss_pages: u64,
        process_count: u32,
        page_size: u64,
        clock_ticks_per_sec: u64,
        cpu_count: usize,
    ) {
        let now = Instant::now();
        self.sample_count = self.sample_count.saturating_add(1);
        self.max_process_count = self.max_process_count.max(process_count);
        self.max_tree_rss_bytes = self
            .max_tree_rss_bytes
            .max(tree_rss_pages.saturating_mul(page_size));
        self.max_root_rss_bytes = self
            .max_root_rss_bytes
            .max(root_rss_pages.saturating_mul(page_size));

        Self::record_cpu_sample(
            &mut self.last_tree_cpu_ticks,
            &mut self.tree_cpu_sum_pct,
            &mut self.tree_cpu_sample_count,
            tree_cpu_ticks,
            now,
            self.last_sample_at,
            clock_ticks_per_sec,
            cpu_count,
        );
        Self::record_cpu_sample(
            &mut self.last_root_cpu_ticks,
            &mut self.root_cpu_sum_pct,
            &mut self.root_cpu_sample_count,
            root_cpu_ticks,
            now,
            self.last_sample_at,
            clock_ticks_per_sec,
            cpu_count,
        );
        self.last_sample_at = Some(now);
    }

    fn finish(&self) -> ProcessTreeMetrics {
        ProcessTreeMetrics {
            cpu_usage_avg: (self.tree_cpu_sample_count > 0)
                .then_some(self.tree_cpu_sum_pct / f64::from(self.tree_cpu_sample_count)),
            memory_usage_max_mb: (self.sample_count > 0)
                .then_some(self.max_tree_rss_bytes as f64 / 1024.0 / 1024.0),
            root_cpu_usage_avg: (self.root_cpu_sample_count > 0)
                .then_some(self.root_cpu_sum_pct / f64::from(self.root_cpu_sample_count)),
            root_memory_usage_max_mb: (self.sample_count > 0)
                .then_some(self.max_root_rss_bytes as f64 / 1024.0 / 1024.0),
            process_count_max: (self.sample_count > 0).then_some(self.max_process_count),
            sample_count: self.sample_count,
        }
    }
}

#[cfg(target_os = "linux")]
#[derive(Default)]
struct CgroupResourceAccumulator {
    last_cpu_usage_usec: Option<u64>,
    cpu_sum_pct: f64,
    cpu_sample_count: u32,
    max_memory_bytes: u64,
    sample_count: u32,
    last_sample_at: Option<Instant>,
}

#[cfg(target_os = "linux")]
impl CgroupResourceAccumulator {
    fn record(&mut self, sample: CgroupSample, cpu_count: usize) {
        let now = Instant::now();
        self.sample_count = self.sample_count.saturating_add(1);
        self.max_memory_bytes = self.max_memory_bytes.max(sample.memory_bytes);

        if let (Some(previous_usage_usec), Some(previous_time)) =
            (self.last_cpu_usage_usec, self.last_sample_at)
        {
            let elapsed = now.saturating_duration_since(previous_time).as_secs_f64();
            if elapsed > 0.0 && cpu_count > 0 {
                let delta_usage_usec =
                    sample.cpu_usage_usec.saturating_sub(previous_usage_usec) as f64;
                let cpu_seconds = delta_usage_usec / 1_000_000.0;
                let cpu_pct = cpu_seconds / elapsed / cpu_count as f64 * 100.0;
                self.cpu_sum_pct += cpu_pct.max(0.0);
                self.cpu_sample_count = self.cpu_sample_count.saturating_add(1);
            }
        }

        self.last_cpu_usage_usec = Some(sample.cpu_usage_usec);
        self.last_sample_at = Some(now);
    }

    fn finish_cpu_avg(&self) -> Option<f64> {
        (self.cpu_sample_count > 0).then_some(self.cpu_sum_pct / f64::from(self.cpu_sample_count))
    }

    fn finish_memory_max_mb(&self) -> Option<f64> {
        (self.sample_count > 0).then_some(self.max_memory_bytes as f64 / 1024.0 / 1024.0)
    }
}

#[cfg(target_os = "linux")]
#[derive(Default)]
struct SharedBuildAccumulator {
    nix_daemon: CgroupResourceAccumulator,
    nix_build_slice: CgroupResourceAccumulator,
    background_slice: CgroupResourceAccumulator,
}

#[cfg(target_os = "linux")]
impl SharedBuildAccumulator {
    fn record(&mut self, targets: &ResolvedSharedCgroupTargets, cpu_count: usize) {
        if let Some(sample) = targets
            .nix_daemon
            .as_ref()
            .and_then(|path| read_cgroup_sample_from_dir(path))
        {
            self.nix_daemon.record(sample, cpu_count);
        }
        if let Some(sample) = targets
            .nix_build_slice
            .as_ref()
            .and_then(|path| read_cgroup_sample_from_dir(path))
        {
            self.nix_build_slice.record(sample, cpu_count);
        }
        if let Some(sample) = targets
            .background_slice
            .as_ref()
            .and_then(|path| read_cgroup_sample_from_dir(path))
        {
            self.background_slice.record(sample, cpu_count);
        }
    }

    fn finish(&self) -> SharedBuildMetrics {
        SharedBuildMetrics {
            shared_nix_daemon_cpu_usage_avg: self.nix_daemon.finish_cpu_avg(),
            shared_nix_daemon_memory_usage_max_mb: self.nix_daemon.finish_memory_max_mb(),
            shared_nix_build_slice_cpu_usage_avg: self.nix_build_slice.finish_cpu_avg(),
            shared_nix_build_slice_memory_usage_max_mb: self.nix_build_slice.finish_memory_max_mb(),
            shared_background_slice_cpu_usage_avg: self.background_slice.finish_cpu_avg(),
            shared_background_slice_memory_usage_max_mb: self
                .background_slice
                .finish_memory_max_mb(),
        }
    }
}

#[cfg(target_os = "linux")]
#[derive(Debug, Clone, Default)]
struct ResolvedSharedCgroupTargets {
    nix_daemon: Option<PathBuf>,
    nix_build_slice: Option<PathBuf>,
    background_slice: Option<PathBuf>,
}

#[cfg(target_os = "linux")]
fn resolve_shared_cgroup_targets(cgroup_root: &Path) -> ResolvedSharedCgroupTargets {
    ResolvedSharedCgroupTargets {
        nix_daemon: resolve_cgroup_dir(
            cgroup_root,
            NIX_DAEMON_CGROUP_CANDIDATES,
            NIX_DAEMON_CGROUP_DISCOVERY_NAMES,
        ),
        nix_build_slice: resolve_cgroup_dir(
            cgroup_root,
            NIX_BUILD_SLICE_CANDIDATES,
            NIX_BUILD_SLICE_DISCOVERY_NAMES,
        ),
        background_slice: resolve_cgroup_dir(
            cgroup_root,
            BACKGROUND_SLICE_CANDIDATES,
            BACKGROUND_SLICE_DISCOVERY_NAMES,
        ),
    }
}

#[cfg(target_os = "linux")]
fn sample_current_process_tree(
    root_pid: u32,
    metrics: &Mutex<ResourceAccumulator>,
    page_size: u64,
    clock_ticks_per_sec: u64,
    cpu_count: usize,
) {
    let table = read_process_table();
    let Some(root_sample) = table.get(&root_pid) else {
        return;
    };
    if let Some((total_cpu_ticks, total_rss_pages, process_count)) =
        collect_process_tree_stats(root_pid, &table)
    {
        metrics.lock().record(
            total_cpu_ticks,
            total_rss_pages,
            root_sample.total_cpu_ticks,
            root_sample.rss_pages,
            process_count,
            page_size,
            clock_ticks_per_sec,
            cpu_count,
        );
    }
}

#[cfg(target_os = "linux")]
fn sample_shared_build_cgroups(
    targets: &ResolvedSharedCgroupTargets,
    metrics: &Mutex<SharedBuildAccumulator>,
    cpu_count: usize,
) {
    metrics.lock().record(targets, cpu_count);
}

pub struct InvocationResourceMonitor {
    #[cfg(target_os = "linux")]
    running: Arc<AtomicBool>,
    #[cfg(target_os = "linux")]
    metrics: Arc<Mutex<ResourceAccumulator>>,
    #[cfg(target_os = "linux")]
    shared_build_metrics: Arc<Mutex<SharedBuildAccumulator>>,
    #[cfg(target_os = "linux")]
    root_pid: u32,
    #[cfg(target_os = "linux")]
    resolved_shared_cgroup_targets: ResolvedSharedCgroupTargets,
    #[cfg(target_os = "linux")]
    page_size: u64,
    #[cfg(target_os = "linux")]
    clock_ticks_per_sec: u64,
    #[cfg(target_os = "linux")]
    cpu_count: usize,
    #[cfg(target_os = "linux")]
    handle: Option<thread::JoinHandle<()>>,
}

impl InvocationResourceMonitor {
    #[must_use]
    pub fn start_for_current_process() -> Self {
        #[cfg(target_os = "linux")]
        {
            let root_pid = std::process::id();
            let metrics = Arc::new(Mutex::new(ResourceAccumulator::default()));
            let shared_build_metrics = Arc::new(Mutex::new(SharedBuildAccumulator::default()));
            let running = Arc::new(AtomicBool::new(true));
            let cgroup_root = default_cgroup_root();
            let resolved_shared_cgroup_targets = resolve_shared_cgroup_targets(&cgroup_root);
            let page_size = sysconf_positive(libc::_SC_PAGESIZE, 4096);
            let clock_ticks_per_sec = sysconf_positive(libc::_SC_CLK_TCK, 100);
            let cpu_count =
                std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get);

            let metrics_clone = metrics.clone();
            let shared_build_metrics_clone = shared_build_metrics.clone();
            let running_clone = running.clone();
            let resolved_shared_cgroup_targets_clone = resolved_shared_cgroup_targets.clone();

            let handle = thread::spawn(move || {
                while running_clone.load(Ordering::Relaxed) {
                    sample_current_process_tree(
                        root_pid,
                        metrics_clone.as_ref(),
                        page_size,
                        clock_ticks_per_sec,
                        cpu_count,
                    );
                    sample_shared_build_cgroups(
                        &resolved_shared_cgroup_targets_clone,
                        shared_build_metrics_clone.as_ref(),
                        cpu_count,
                    );
                    thread::sleep(Duration::from_millis(
                        INVOCATION_RESOURCE_SAMPLE_INTERVAL_MS,
                    ));
                }
            });

            Self {
                running,
                metrics,
                shared_build_metrics,
                root_pid,
                resolved_shared_cgroup_targets,
                page_size,
                clock_ticks_per_sec,
                cpu_count,
                handle: Some(handle),
            }
        }

        #[cfg(not(target_os = "linux"))]
        {
            Self {}
        }
    }

    pub fn stop(&mut self) -> InvocationResourceMetrics {
        #[cfg(target_os = "linux")]
        {
            sample_current_process_tree(
                self.root_pid,
                self.metrics.as_ref(),
                self.page_size,
                self.clock_ticks_per_sec,
                self.cpu_count,
            );
            sample_shared_build_cgroups(
                &self.resolved_shared_cgroup_targets,
                self.shared_build_metrics.as_ref(),
                self.cpu_count,
            );
            self.running.store(false, Ordering::Relaxed);
            if let Some(handle) = self.handle.take() {
                let _ = handle.join();
            }
            return InvocationResourceMetrics {
                process_tree: self.metrics.lock().finish(),
                shared_build: self.shared_build_metrics.lock().finish(),
            };
        }

        #[cfg(not(target_os = "linux"))]
        {
            InvocationResourceMetrics::default()
        }
    }
}

#[cfg(target_os = "linux")]
#[must_use]
pub fn probe_process_tree_metrics(
    root_pid: u32,
    sample_window: Duration,
) -> Option<ProcessTreeMetrics> {
    let metrics = Mutex::new(ResourceAccumulator::default());
    let page_size = sysconf_positive(libc::_SC_PAGESIZE, 4096);
    let clock_ticks_per_sec = sysconf_positive(libc::_SC_CLK_TCK, 100);
    let cpu_count = std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get);

    sample_current_process_tree(
        root_pid,
        &metrics,
        page_size,
        clock_ticks_per_sec,
        cpu_count,
    );

    if !sample_window.is_zero() {
        thread::sleep(sample_window);
        sample_current_process_tree(
            root_pid,
            &metrics,
            page_size,
            clock_ticks_per_sec,
            cpu_count,
        );
    }

    let snapshot = metrics.lock().finish();
    (snapshot.sample_count > 0).then_some(snapshot)
}

#[cfg(not(target_os = "linux"))]
#[must_use]
pub fn probe_process_tree_metrics(
    _root_pid: u32,
    _sample_window: Duration,
) -> Option<ProcessTreeMetrics> {
    None
}

#[cfg(target_os = "linux")]
fn probe_shared_build_metrics_at_root(
    cgroup_root: &Path,
    sample_window: Duration,
) -> Option<SharedBuildMetrics> {
    let metrics = Mutex::new(SharedBuildAccumulator::default());
    let cpu_count = std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get);
    let targets = resolve_shared_cgroup_targets(cgroup_root);

    sample_shared_build_cgroups(&targets, &metrics, cpu_count);
    if !sample_window.is_zero() {
        thread::sleep(sample_window);
        sample_shared_build_cgroups(&targets, &metrics, cpu_count);
    }

    let snapshot = metrics.lock().finish();
    snapshot.has_samples().then_some(snapshot)
}

#[cfg(target_os = "linux")]
#[must_use]
pub fn probe_shared_build_metrics(sample_window: Duration) -> Option<SharedBuildMetrics> {
    probe_shared_build_metrics_at_root(&default_cgroup_root(), sample_window)
}

#[cfg(not(target_os = "linux"))]
#[must_use]
pub fn probe_shared_build_metrics(_sample_window: Duration) -> Option<SharedBuildMetrics> {
    None
}

/// Canonical low-level cargo command constructor.
///
/// Use this when a call site needs stdio/process control that `ProcessBuilder`
/// does not currently expose. Keeping cargo construction here gives xtask one
/// seam for future policy changes.
#[must_use]
pub fn cargo_command() -> Command {
    let mut cmd = Command::new("cargo");
    configure_managed_child_std(&mut cmd);
    cmd
}

/// Canonical async cargo command constructor.
///
/// Prefer `ProcessBuilder::cargo()` for simple invocations; use this helper when
/// the caller needs direct access to `tokio::process::Command`.
#[must_use]
pub fn cargo_tokio_command() -> tokio::process::Command {
    let mut cmd = tokio::process::Command::new("cargo");
    configure_managed_child_tokio(&mut cmd);
    cmd
}

const DEFAULT_HELPER_PROCESS_TIMEOUT_SECS: u64 = 300;
const DEFAULT_HEAVY_HELPER_PROCESS_TIMEOUT_SECS: u64 = 1800;

pub(crate) fn helper_process_timeout_for_program(program: &str) -> Duration {
    let (env_var, default) = match program {
        "cargo" | "nix" | "xtask" | "cargo-mutants" | "cargo-fuzz" => (
            "SINEX_HEAVY_PROCESS_TIMEOUT",
            DEFAULT_HEAVY_HELPER_PROCESS_TIMEOUT_SECS,
        ),
        _ => ("SINEX_PROCESS_TIMEOUT", DEFAULT_HELPER_PROCESS_TIMEOUT_SECS),
    };

    Duration::from_secs(crate::parse_positive_u64_env_or_default(
        env_var,
        default,
        "managed helper process timeout",
    ))
}

/// Output from a process execution.
#[derive(Debug)]
pub struct ProcessOutput {
    /// Standard output as UTF-8 string
    pub stdout: String,
    /// Standard error as UTF-8 string
    pub stderr: String,
    /// Exit status code
    pub exit_code: i32,
}

impl ProcessOutput {
    /// Check if the process succeeded (exit code 0).
    #[must_use]
    pub fn success(&self) -> bool {
        self.exit_code == 0
    }

    /// Get combined output (stdout + stderr).
    #[must_use]
    pub fn combined(&self) -> String {
        format!("{}{}", self.stdout, self.stderr)
    }
}

/// Builder for executing external processes with consistent error handling.
pub struct ProcessBuilder {
    program: String,
    args: Vec<String>,
    env_vars: Vec<(String, String)>,
    working_dir: Option<PathBuf>,
    description: Option<String>,
    capture_output: bool,
    timeout: Option<Duration>,
}

impl ProcessBuilder {
    /// Create a new process builder for the given program.
    pub fn new(program: impl AsRef<str>) -> Self {
        Self {
            program: program.as_ref().to_string(),
            args: Vec::new(),
            env_vars: Vec::new(),
            working_dir: None,
            description: None,
            capture_output: true,
            timeout: Some(helper_process_timeout_for_program(program.as_ref())),
        }
    }

    /// Create a git command builder with automatic context.
    #[must_use]
    pub fn git() -> Self {
        Self::new("git").with_description("git command")
    }

    /// Create a cargo command builder with automatic context.
    #[must_use]
    pub fn cargo() -> Self {
        Self::new("cargo").with_description("cargo command")
    }

    /// Create a psql (`PostgreSQL`) command builder.
    #[must_use]
    pub fn psql() -> Self {
        Self::new("psql").with_description("PostgreSQL command")
    }

    /// Create a nix command builder.
    #[must_use]
    pub fn nix() -> Self {
        Self::new("nix").with_description("nix command")
    }

    /// Set command arguments.
    pub fn args<I, S>(mut self, args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        for arg in args {
            self.args.push(arg.as_ref().to_string());
        }
        self
    }

    /// Set a single argument.
    pub fn arg(mut self, arg: impl AsRef<str>) -> Self {
        self.args.push(arg.as_ref().to_string());
        self
    }

    /// Set an environment variable.
    pub fn env(mut self, key: impl AsRef<str>, val: impl AsRef<str>) -> Self {
        self.env_vars
            .push((key.as_ref().to_string(), val.as_ref().to_string()));
        self
    }

    /// Set multiple environment variables.
    pub fn envs<I, K, V>(mut self, envs: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: AsRef<str>,
    {
        for (key, val) in envs {
            self.env_vars
                .push((key.as_ref().to_string(), val.as_ref().to_string()));
        }
        self
    }

    /// Set the working directory.
    pub fn current_dir(mut self, dir: impl AsRef<std::path::Path>) -> Self {
        self.working_dir = Some(dir.as_ref().to_path_buf());
        self
    }

    /// Set a description for error messages.
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Disable output capture (inherit stdio from parent).
    #[must_use]
    pub fn inherit_output(mut self) -> Self {
        self.capture_output = false;
        self
    }

    /// Set an explicit wall-clock timeout for this helper command.
    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Disable the helper timeout entirely.
    #[must_use]
    pub fn without_timeout(mut self) -> Self {
        self.timeout = None;
        self
    }

    fn command_display(&self) -> String {
        if self.args.is_empty() {
            self.program.clone()
        } else {
            format!("{} {}", self.program, self.args.join(" "))
        }
    }

    fn context_message(&self) -> String {
        self.description
            .clone()
            .unwrap_or_else(|| format!("running {}", self.command_display()))
    }

    fn timeout_message(&self, timeout: Duration) -> String {
        format!(
            "{} timed out after {:.0}s; set {} to adjust the default or call \
             ProcessBuilder::with_timeout()/without_timeout() for this site",
            self.context_message(),
            timeout.as_secs_f64(),
            match self.program.as_str() {
                "cargo" | "nix" | "xtask" | "cargo-mutants" | "cargo-fuzz" => {
                    "SINEX_HEAVY_PROCESS_TIMEOUT"
                }
                _ => "SINEX_PROCESS_TIMEOUT",
            }
        )
    }

    fn build_std_command(&self) -> Command {
        let mut cmd = Command::new(&self.program);
        cmd.args(&self.args).stdin(Stdio::null());
        configure_managed_child_std(&mut cmd);

        if let Some(ref dir) = self.working_dir {
            cmd.current_dir(dir);
        }

        for (key, val) in &self.env_vars {
            cmd.env(key, val);
        }

        cmd
    }

    /// Execute the command and return the output.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The command fails to spawn
    /// - The command exits with non-zero status
    /// - Output cannot be decoded as UTF-8
    pub fn run(self) -> Result<ProcessOutput> {
        let context_msg = self.context_message();
        let timeout = self.timeout;
        let mut cmd = self.build_std_command();

        if self.capture_output {
            cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
            let child = cmd
                .spawn()
                .with_context(|| format!("failed to spawn: {context_msg}"))?;
            register_std_child_process_group(&child, &self.program);
            let mut timeout_guard = timeout.map(|timeout| {
                ProcessTimeoutGuard::start_for_process_group_leader(
                    child.id(),
                    self.program.clone(),
                    timeout,
                    format!("{context_msg} timed out"),
                )
            });
            let output = child
                .wait_with_output()
                .with_context(|| format!("failed to wait for: {context_msg}"))?;
            if timeout_guard
                .as_mut()
                .is_some_and(ProcessTimeoutGuard::finish)
            {
                bail!(
                    "{}",
                    self.timeout_message(timeout.expect("timeout present"))
                );
            }

            let exit_code = output.status.code().unwrap_or(-1);
            let stdout = String::from_utf8_lossy(&output.stdout).to_string();
            let stderr = String::from_utf8_lossy(&output.stderr).to_string();

            if !output.status.success() {
                bail!(
                    "{} failed with exit code {}\nstderr: {}",
                    context_msg,
                    exit_code,
                    stderr.trim()
                );
            }

            Ok(ProcessOutput {
                stdout,
                stderr,
                exit_code,
            })
        } else {
            cmd.stdout(Stdio::inherit()).stderr(Stdio::inherit());

            let mut child = cmd
                .spawn()
                .with_context(|| format!("failed to spawn: {context_msg}"))?;
            register_std_child_process_group(&child, &self.program);
            let mut timeout_guard = timeout.map(|timeout| {
                ProcessTimeoutGuard::start_for_process_group_leader(
                    child.id(),
                    self.program.clone(),
                    timeout,
                    format!("{context_msg} timed out"),
                )
            });
            let status = child
                .wait()
                .with_context(|| format!("failed to wait for: {context_msg}"))?;
            if timeout_guard
                .as_mut()
                .is_some_and(ProcessTimeoutGuard::finish)
            {
                bail!(
                    "{}",
                    self.timeout_message(timeout.expect("timeout present"))
                );
            }

            let exit_code = status.code().unwrap_or(-1);

            if !status.success() {
                bail!("{context_msg} failed with exit code {exit_code}");
            }

            Ok(ProcessOutput {
                stdout: String::new(),
                stderr: String::new(),
                exit_code,
            })
        }
    }

    /// Execute the command and return only if it succeeds (discarding output).
    pub fn run_ok(self) -> Result<()> {
        self.run().map(|_| ())
    }

    /// Execute the command and return the output, even if it exits non-zero.
    ///
    /// Unlike `run()`, this does NOT fail on non-zero exit codes.
    /// Useful when you need to parse stdout from a command that may fail
    /// (e.g., parsing compiler diagnostics from a failed build).
    pub fn run_capture(self) -> Result<ProcessOutput> {
        let context_msg = self.context_message();
        let timeout = self.timeout;
        let mut cmd = self.build_std_command();
        cmd.stdout(Stdio::piped()).stderr(Stdio::piped());

        let child = cmd
            .spawn()
            .with_context(|| format!("failed to spawn: {context_msg}"))?;
        register_std_child_process_group(&child, &self.program);
        let mut timeout_guard = timeout.map(|timeout| {
            ProcessTimeoutGuard::start_for_process_group_leader(
                child.id(),
                self.program.clone(),
                timeout,
                format!("{context_msg} timed out"),
            )
        });
        let output = child
            .wait_with_output()
            .with_context(|| format!("failed to wait for: {context_msg}"))?;
        if timeout_guard
            .as_mut()
            .is_some_and(ProcessTimeoutGuard::finish)
        {
            bail!(
                "{}",
                self.timeout_message(timeout.expect("timeout present"))
            );
        }

        Ok(ProcessOutput {
            stdout: String::from_utf8_lossy(&output.stdout).to_string(),
            stderr: String::from_utf8_lossy(&output.stderr).to_string(),
            exit_code: output.status.code().unwrap_or(-1),
        })
    }

    /// Execute the command and return stdout as a trimmed string.
    pub fn run_stdout(self) -> Result<String> {
        self.run().map(|output| output.stdout.trim().to_string())
    }

    /// Execute the command and check if it succeeds, returning a boolean.
    ///
    /// Unlike `run()`, this doesn't fail on non-zero exit - it returns false instead.
    pub fn run_success(self) -> Result<bool> {
        let timeout = self.timeout;
        let mut cmd = self.build_std_command();
        cmd.stdout(Stdio::null()).stderr(Stdio::null());

        let child = cmd.spawn().context("failed to spawn command")?;
        register_std_child_process_group(&child, &self.program);
        let mut timeout_guard = timeout.map(|timeout| {
            ProcessTimeoutGuard::start_for_process_group_leader(
                child.id(),
                self.program.clone(),
                timeout,
                format!("{} timed out", self.context_message()),
            )
        });
        let output = child
            .wait_with_output()
            .context("failed to wait for command")?;
        if timeout_guard
            .as_mut()
            .is_some_and(ProcessTimeoutGuard::finish)
        {
            bail!(
                "{}",
                self.timeout_message(timeout.expect("timeout present"))
            );
        }

        Ok(output.status.success())
    }

    /// Spawn the command and return the `std::process::Child` handle.
    ///
    /// This is useful for long-running processes or when you need manual control
    /// over the process lifecycle (e.g., background jobs).
    pub fn spawn(self) -> Result<std::process::Child> {
        let mut cmd = Command::new(&self.program);
        cmd.args(&self.args);
        configure_managed_child_std(&mut cmd);

        if let Some(ref dir) = self.working_dir {
            cmd.current_dir(dir);
        }

        for (key, val) in &self.env_vars {
            cmd.env(key, val);
        }

        // Configure stdio based on capture setting
        if self.capture_output {
            cmd.stdout(Stdio::piped());
            cmd.stderr(Stdio::piped());
        } else {
            cmd.stdout(Stdio::inherit());
            cmd.stderr(Stdio::inherit());
        }

        let child = cmd
            .spawn()
            .with_context(|| format!("failed to spawn: {}", self.program))?;
        register_process_group(child.id(), &self.program);
        Ok(child)
    }

    /// Spawn the command using tokio and return the `tokio::process::Child` handle.
    ///
    /// This is the async version of `spawn()`.
    pub fn spawn_tokio(self) -> Result<tokio::process::Child> {
        let mut cmd = tokio::process::Command::new(&self.program);
        cmd.args(&self.args);
        configure_managed_child_tokio(&mut cmd);

        if let Some(ref dir) = self.working_dir {
            cmd.current_dir(dir);
        }

        for (key, val) in &self.env_vars {
            cmd.env(key, val);
        }

        if self.capture_output {
            cmd.stdout(Stdio::piped());
            cmd.stderr(Stdio::piped());
        } else {
            cmd.stdout(Stdio::inherit());
            cmd.stderr(Stdio::inherit());
        }
        cmd.kill_on_drop(true);

        let child = cmd
            .spawn()
            .with_context(|| format!("failed to spawn async: {}", self.program))?;
        if let Some(pid) = child.id() {
            register_process_group(pid, &self.program);
        }
        Ok(child)
    }

    /// Execute the command and return an async stream of stdout lines.
    ///
    /// This is the non-blocking equivalent of `spawn_with_streaming`.
    pub fn spawn_tokio_streaming(
        self,
    ) -> Result<(
        tokio::process::Child,
        tokio::io::Lines<tokio::io::BufReader<tokio::process::ChildStdout>>,
    )> {
        use tokio::io::AsyncBufReadExt;

        let mut cmd = tokio::process::Command::new(&self.program);
        cmd.args(&self.args);
        configure_managed_child_tokio(&mut cmd);

        if let Some(ref dir) = self.working_dir {
            cmd.current_dir(dir);
        }

        for (key, val) in &self.env_vars {
            cmd.env(key, val);
        }

        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());
        cmd.kill_on_drop(true);

        let mut child = cmd
            .spawn()
            .with_context(|| format!("failed to spawn async streaming: {}", self.program))?;
        if let Some(pid) = child.id() {
            register_process_group(pid, &self.program);
        }

        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| eyre!("failed to capture async stdout"))?;

        let reader = tokio::io::BufReader::new(stdout).lines();

        Ok((child, reader))
    }

    /// Execute the command asynchronously and return its exit status.
    pub async fn run_tokio_status(self) -> Result<std::process::ExitStatus> {
        let context_message = self.context_message();
        let timeout = self.timeout;
        let capture_output = self.capture_output;
        let program = self.program.clone();
        let timeout_message = timeout.map(|timeout| self.timeout_message(timeout));
        let mut child = self.spawn_tokio()?;
        let timeout_guard = if let (Some(timeout), Some(pid)) = (timeout, child.id()) {
            Some(ProcessTimeoutGuard::start_for_process_group_leader(
                pid,
                &program,
                timeout,
                format!("{program} timeout"),
            ))
        } else {
            None
        };

        let status = if capture_output {
            child
                .wait_with_output()
                .await
                .with_context(|| context_message.clone())?
                .status
        } else {
            child
                .wait()
                .await
                .with_context(|| context_message.clone())?
        };

        if let Some(mut guard) = timeout_guard
            && guard.finish()
        {
            return Err(eyre!(
                timeout_message.unwrap_or_else(|| format!("{program} timed out"))
            ));
        }

        Ok(status)
    }

    /// Execute the command asynchronously and require a zero exit status.
    pub async fn run_tokio_ok(self) -> Result<()> {
        let context_message = self.context_message();
        let status = self.run_tokio_status().await?;
        if status.success() {
            Ok(())
        } else {
            bail!("{context_message} failed with exit status {status}");
        }
    }

    /// Execute the command and return a streaming iterator over stdout lines.
    ///
    /// This is useful for TUI applications that need to process output in real-time
    /// (e.g., tests) while still capturing it.
    ///
    /// Note: This version is synchronous and blocks the caller when reading from the reader.
    /// For async contexts, use `spawn_tokio_streaming`.
    pub fn spawn_with_streaming(self) -> Result<(std::process::Child, impl std::io::BufRead)> {
        let mut cmd = Command::new(&self.program);
        cmd.args(&self.args);
        configure_managed_child_std(&mut cmd);

        if let Some(ref dir) = self.working_dir {
            cmd.current_dir(dir);
        }

        for (key, val) in &self.env_vars {
            cmd.env(key, val);
        }

        // We strictly pipe stdout for streaming
        cmd.stdout(Stdio::piped());
        // Stderr is piped too to avoid pollution, caller can read it from child if needed
        cmd.stderr(Stdio::piped());

        let mut child = cmd
            .spawn()
            .with_context(|| format!("failed to spawn for streaming: {}", self.program))?;
        register_process_group(child.id(), &self.program);

        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| eyre!("failed to capture stdout"))?;

        Ok((child, std::io::BufReader::new(stdout)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::sinex_test;
    use tempfile::tempdir;

    #[sinex_test]
    async fn test_process_builder_basic() -> TestResult<()> {
        let output = ProcessBuilder::new("echo").arg("hello").run()?;

        assert!(output.success());
        assert_eq!(output.stdout.trim(), "hello");
        Ok(())
    }

    #[sinex_test]
    async fn test_process_builder_git() -> TestResult<()> {
        let output = ProcessBuilder::git().args(["--version"]).run()?;

        assert!(output.success());
        assert!(output.stdout.contains("git version"));
        Ok(())
    }

    #[sinex_test]
    async fn test_process_builder_failure() -> TestResult<()> {
        let result = ProcessBuilder::new("false").run();
        assert!(result.is_err());
        Ok(())
    }

    #[sinex_test]
    async fn test_process_builder_run_success() -> TestResult<()> {
        let success = ProcessBuilder::new("true").run_success()?;
        assert!(success);

        let failure = ProcessBuilder::new("false").run_success()?;
        assert!(!failure);
        Ok(())
    }

    #[sinex_test]
    async fn test_process_builder_stdout() -> TestResult<()> {
        let output = ProcessBuilder::new("echo")
            .arg("test output")
            .run_stdout()?;

        assert_eq!(output, "test output");
        Ok(())
    }

    #[sinex_test]
    async fn test_process_builder_multiple_args() -> TestResult<()> {
        let output = ProcessBuilder::new("echo")
            .args(["one", "two", "three"])
            .run()?;

        assert!(output.success());
        assert_eq!(output.stdout.trim(), "one two three");
        Ok(())
    }

    #[sinex_test]
    async fn test_process_builder_cargo_helper() -> TestResult<()> {
        let output = ProcessBuilder::cargo().args(["--version"]).run()?;

        assert!(output.success());
        assert!(output.stdout.contains("cargo"));
        Ok(())
    }

    #[sinex_test]
    async fn test_process_builder_with_description() -> TestResult<()> {
        let result = ProcessBuilder::new("nonexistent_command_xyz")
            .with_description("test command")
            .run();

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("test command"));
        Ok(())
    }

    #[sinex_test]
    async fn test_process_builder_env() -> TestResult<()> {
        let output = ProcessBuilder::new("sh")
            .args(["-c", "echo $TEST_VAR"])
            .env("TEST_VAR", "test_value")
            .run()?;

        assert!(output.success());
        assert_eq!(output.stdout.trim(), "test_value");
        Ok(())
    }

    #[sinex_test]
    async fn test_process_builder_current_dir() -> TestResult<()> {
        let output = ProcessBuilder::new("pwd").current_dir("/tmp").run()?;

        assert!(output.success());
        assert!(output.stdout.contains("/tmp"));
        Ok(())
    }

    #[sinex_test]
    async fn test_process_output_combined() -> TestResult<()> {
        let output = ProcessBuilder::new("sh")
            .args(["-c", "echo stdout; echo stderr >&2"])
            .run()?;

        let combined = output.combined();
        assert!(combined.contains("stdout"));
        assert!(combined.contains("stderr"));
        Ok(())
    }

    #[sinex_test]
    async fn test_process_builder_run_ok() -> TestResult<()> {
        ProcessBuilder::new("true").run_ok()?;

        let result = ProcessBuilder::new("false").run_ok();
        assert!(result.is_err());
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[sinex_test]
    async fn test_parse_proc_stat_handles_spacey_command_names() -> TestResult<()> {
        let sample =
            "1234 (cargo check) S 4321 1234 1234 0 -1 4194304 0 0 0 0 11 7 0 0 20 0 4 0 55 4096 33";
        let parsed = parse_proc_stat(sample).expect("sample stat line should parse");
        assert_eq!(parsed.ppid, 4321);
        assert_eq!(parsed.total_cpu_ticks, 18);
        assert_eq!(parsed.start_ticks, 55);
        assert_eq!(parsed.rss_pages, 33);
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[sinex_test]
    async fn test_collect_process_tree_stats_walks_descendants() -> TestResult<()> {
        let table = HashMap::from([
            (
                10_u32,
                ProcSample {
                    ppid: 1,
                    start_ticks: 100,
                    total_cpu_ticks: 5,
                    rss_pages: 2,
                },
            ),
            (
                11_u32,
                ProcSample {
                    ppid: 10,
                    start_ticks: 101,
                    total_cpu_ticks: 7,
                    rss_pages: 3,
                },
            ),
            (
                12_u32,
                ProcSample {
                    ppid: 11,
                    start_ticks: 102,
                    total_cpu_ticks: 9,
                    rss_pages: 4,
                },
            ),
            (
                99_u32,
                ProcSample {
                    ppid: 1,
                    start_ticks: 200,
                    total_cpu_ticks: 100,
                    rss_pages: 100,
                },
            ),
        ]);

        let (cpu_ticks, rss_pages, process_count) =
            collect_process_tree_stats(10, &table).expect("root should be present");
        assert_eq!(cpu_ticks, 21);
        assert_eq!(rss_pages, 9);
        assert_eq!(process_count, 3);
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[sinex_test(timeout = 30)]
    async fn test_terminate_registered_process_groups_handles_exited_group_leader() -> TestResult<()>
    {
        let _ = terminate_registered_process_groups("test setup cleanup")?;
        let dir = tempdir()?;
        let pid_file = dir.path().join("sleep.pid");
        let script = format!("sleep 30 & echo $! > {} ; exit 0", pid_file.display());

        let success = ProcessBuilder::new("sh")
            .args(["-c", &script])
            .run_success()?;
        assert!(success);

        let sleep_pid: i32 = std::fs::read_to_string(&pid_file)?.trim().parse()?;
        assert_eq!(unsafe { libc::kill(sleep_pid, 0) }, 0);

        let terminated = terminate_registered_process_groups("test cleanup")?;
        assert!(terminated >= 1);

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while std::time::Instant::now() < deadline {
            if unsafe { libc::kill(sleep_pid, 0) } != 0 {
                return Ok(());
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }

        Err(color_eyre::eyre::eyre!(
            "background sleep process {sleep_pid} survived registered process-group cleanup"
        ))
    }

    #[cfg(target_os = "linux")]
    #[sinex_test(timeout = 30)]
    async fn test_process_builder_timeout_kills_descendants() -> TestResult<()> {
        let dir = tempdir()?;
        let pid_file = dir.path().join("sleep.pid");
        let script = format!("sleep 30 & echo $! > {} ; sleep 30", pid_file.display());

        let result = ProcessBuilder::new("sh")
            .args(["-c", &script])
            .with_description("timeout descendant cleanup")
            .with_timeout(Duration::from_millis(250))
            .run_capture();

        let error = result.expect_err("timed command should fail");
        assert!(
            error.to_string().contains("timed out after"),
            "timeout error should mention deadline: {error:#}"
        );

        let sleep_pid: i32 = std::fs::read_to_string(&pid_file)?.trim().parse()?;
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if unsafe { libc::kill(sleep_pid, 0) } != 0 {
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(100));
        }

        Err(color_eyre::eyre::eyre!(
            "timed-out ProcessBuilder left descendant process {sleep_pid} alive"
        ))
    }

    #[cfg(target_os = "linux")]
    #[sinex_test(timeout = 30)]
    async fn test_process_builder_run_tokio_status_timeout_kills_descendants() -> TestResult<()> {
        let dir = tempdir()?;
        let pid_file = dir.path().join("sleep.pid");
        let script = format!("sleep 30 & echo $! > {} ; sleep 30", pid_file.display());

        let result = ProcessBuilder::new("sh")
            .args(["-c", &script])
            .with_description("async timeout descendant cleanup")
            .with_timeout(Duration::from_millis(250))
            .run_tokio_status()
            .await;

        let error = result.expect_err("timed async command should fail");
        assert!(
            error.to_string().contains("timed out after"),
            "timeout error should mention deadline: {error:#}"
        );

        let sleep_pid: i32 = std::fs::read_to_string(&pid_file)?.trim().parse()?;
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if unsafe { libc::kill(sleep_pid, 0) } != 0 {
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(100));
        }

        Err(color_eyre::eyre::eyre!(
            "timed-out async ProcessBuilder left descendant process {sleep_pid} alive"
        ))
    }

    #[cfg(target_os = "linux")]
    #[sinex_test(timeout = 30)]
    async fn test_probe_process_tree_metrics_reports_live_descendants() -> TestResult<()> {
        let mut child = ProcessBuilder::new("sh")
            .args(["-c", "sleep 30 & wait"])
            .spawn()?;
        let pid = child.id();

        let snapshot = probe_process_tree_metrics(pid, Duration::from_millis(120))
            .expect("live process tree snapshot should exist");
        assert!(
            snapshot.sample_count >= 1,
            "live probe should record at least one sample"
        );
        assert!(
            snapshot.process_count_max.unwrap_or_default() >= 2,
            "live probe should see the shell plus its background child"
        );

        terminate_std_child_process_group(&mut child, "probe-process-tree", "test cleanup")?;
        let _ = child.wait();
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[sinex_test(timeout = 30)]
    async fn test_resolve_shared_cgroup_targets_prefers_system_slice() -> TestResult<()> {
        let dir = tempdir()?;
        let system_nix_daemon_dir = dir.path().join("system.slice/nix-daemon.service");
        let legacy_nix_daemon_dir = dir
            .path()
            .join("nix.slice/nix-build.slice/nix-daemon.service");
        let system_nix_build_dir = dir.path().join("system.slice/nix-build.slice");
        let legacy_nix_build_dir = dir.path().join("nix.slice/nix-build.slice");
        for path in [
            &system_nix_daemon_dir,
            &legacy_nix_daemon_dir,
            &system_nix_build_dir,
            &legacy_nix_build_dir,
        ] {
            std::fs::create_dir_all(path)?;
            std::fs::write(path.join("cpu.stat"), "usage_usec 1000\n")?;
            std::fs::write(path.join("memory.current"), "67108864\n")?;
        }

        let targets = resolve_shared_cgroup_targets(dir.path());

        assert_eq!(targets.nix_daemon, Some(system_nix_daemon_dir));
        assert_eq!(targets.nix_build_slice, Some(system_nix_build_dir));
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[sinex_test(timeout = 30)]
    async fn test_probe_shared_build_metrics_reports_cgroup_activity() -> TestResult<()> {
        let dir = tempdir()?;
        let nix_daemon_dir = dir
            .path()
            .join("nix.slice/nix-build.slice/nix-daemon.service");
        let nix_build_dir = dir.path().join("nix.slice/nix-build.slice");
        let background_dir = dir.path().join("background.slice");
        std::fs::create_dir_all(&nix_daemon_dir)?;
        std::fs::create_dir_all(&nix_build_dir)?;
        std::fs::create_dir_all(&background_dir)?;

        std::fs::write(nix_daemon_dir.join("cpu.stat"), "usage_usec 1000\n")?;
        std::fs::write(nix_daemon_dir.join("memory.current"), "67108864\n")?;
        std::fs::write(nix_build_dir.join("cpu.stat"), "usage_usec 2000\n")?;
        std::fs::write(nix_build_dir.join("memory.current"), "536870912\n")?;
        std::fs::write(background_dir.join("cpu.stat"), "usage_usec 500\n")?;
        std::fs::write(background_dir.join("memory.current"), "33554432\n")?;

        let nix_daemon_dir_for_thread = nix_daemon_dir.clone();
        let nix_build_dir_for_thread = nix_build_dir.clone();
        let background_dir_for_thread = background_dir.clone();
        let update_handle = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(40));
            let _ = std::fs::write(
                nix_daemon_dir_for_thread.join("cpu.stat"),
                "usage_usec 21000\n",
            );
            let _ = std::fs::write(
                nix_daemon_dir_for_thread.join("memory.current"),
                "134217728\n",
            );
            let _ = std::fs::write(
                nix_build_dir_for_thread.join("cpu.stat"),
                "usage_usec 42000\n",
            );
            let _ = std::fs::write(
                nix_build_dir_for_thread.join("memory.current"),
                "1073741824\n",
            );
            let _ = std::fs::write(
                background_dir_for_thread.join("cpu.stat"),
                "usage_usec 10500\n",
            );
            let _ = std::fs::write(
                background_dir_for_thread.join("memory.current"),
                "268435456\n",
            );
        });

        let snapshot = probe_shared_build_metrics_at_root(dir.path(), Duration::from_millis(120))
            .expect("shared build metrics should exist for synthetic cgroups");
        update_handle
            .join()
            .expect("cgroup update thread should join");

        assert!(
            snapshot.shared_nix_daemon_cpu_usage_avg.is_some(),
            "shared nix-daemon CPU should be sampled"
        );
        assert_eq!(
            snapshot
                .shared_nix_daemon_memory_usage_max_mb
                .map(f64::round),
            Some(128.0)
        );
        assert!(
            snapshot.shared_nix_build_slice_cpu_usage_avg.is_some(),
            "shared nix-build CPU should be sampled"
        );
        assert_eq!(
            snapshot
                .shared_nix_build_slice_memory_usage_max_mb
                .map(f64::round),
            Some(1024.0)
        );
        assert!(
            snapshot.shared_background_slice_cpu_usage_avg.is_some(),
            "shared background-slice CPU should be sampled"
        );
        assert_eq!(
            snapshot
                .shared_background_slice_memory_usage_max_mb
                .map(f64::round),
            Some(256.0)
        );
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[sinex_test(timeout = 30)]
    async fn test_probe_shared_build_metrics_discovers_nested_cgroup_layouts() -> TestResult<()> {
        let dir = tempdir()?;
        let nix_daemon_dir = dir
            .path()
            .join("custom.slice/worker.scope/nix-daemon.service");
        let nix_build_dir = dir
            .path()
            .join("custom.slice/worker.scope/builds/nix-build.slice");
        std::fs::create_dir_all(&nix_daemon_dir)?;
        std::fs::create_dir_all(&nix_build_dir)?;

        std::fs::write(nix_daemon_dir.join("cpu.stat"), "usage_usec 1000\n")?;
        std::fs::write(nix_daemon_dir.join("memory.current"), "67108864\n")?;
        std::fs::write(nix_build_dir.join("cpu.stat"), "usage_usec 2000\n")?;
        std::fs::write(nix_build_dir.join("memory.current"), "536870912\n")?;

        let nix_daemon_dir_for_thread = nix_daemon_dir.clone();
        let nix_build_dir_for_thread = nix_build_dir.clone();
        let update_handle = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(40));
            let _ = std::fs::write(
                nix_daemon_dir_for_thread.join("cpu.stat"),
                "usage_usec 21000\n",
            );
            let _ = std::fs::write(
                nix_daemon_dir_for_thread.join("memory.current"),
                "134217728\n",
            );
            let _ = std::fs::write(
                nix_build_dir_for_thread.join("cpu.stat"),
                "usage_usec 42000\n",
            );
            let _ = std::fs::write(
                nix_build_dir_for_thread.join("memory.current"),
                "1073741824\n",
            );
        });

        let snapshot = probe_shared_build_metrics_at_root(dir.path(), Duration::from_millis(120))
            .expect("shared build metrics should discover nested cgroup layouts");
        update_handle
            .join()
            .expect("cgroup update thread should join");

        assert!(
            snapshot.shared_nix_daemon_cpu_usage_avg.is_some(),
            "shared nix-daemon CPU should be discovered from basename search"
        );
        assert_eq!(
            snapshot
                .shared_nix_daemon_memory_usage_max_mb
                .map(f64::round),
            Some(128.0)
        );
        assert!(
            snapshot.shared_nix_build_slice_cpu_usage_avg.is_some(),
            "shared nix-build CPU should be discovered from basename search"
        );
        assert_eq!(
            snapshot
                .shared_nix_build_slice_memory_usage_max_mb
                .map(f64::round),
            Some(1024.0)
        );
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[sinex_test]
    async fn test_configure_persistent_service_child_std_creates_dedicated_process_group()
    -> TestResult<()> {
        let mut command = std::process::Command::new("sleep");
        command.arg("30");
        configure_persistent_service_child_std(&mut command);

        let mut child = command.spawn()?;
        let pid = child.id() as i32;
        let process_group = nix::unistd::getpgid(Some(nix::unistd::Pid::from_raw(pid)))?;
        assert_eq!(process_group.as_raw(), pid);

        terminate_std_child_process_group(&mut child, "persistent-service-child", "test cleanup")?;
        let _ = child.wait();
        Ok(())
    }

    #[test]
    fn test_status_indicates_clean_interactive_shutdown_accepts_success_and_interrupts() {
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;

            assert!(status_indicates_clean_interactive_shutdown(
                &std::process::ExitStatus::from_raw(0)
            ));
            assert!(status_indicates_clean_interactive_shutdown(
                &std::process::ExitStatus::from_raw(libc::SIGINT)
            ));
            assert!(status_indicates_clean_interactive_shutdown(
                &std::process::ExitStatus::from_raw(libc::SIGTERM)
            ));
            assert!(!status_indicates_clean_interactive_shutdown(
                &std::process::ExitStatus::from_raw(1 << 8)
            ));
        }

        #[cfg(not(unix))]
        {
            let _ = status_indicates_clean_interactive_shutdown;
        }
    }
}
