use std::time::Duration;

#[cfg(not(test))]
const ZOMBIE_REAPER_SIGTERM_GRACE: Duration = Duration::from_secs(2);
#[cfg(test)]
const ZOMBIE_REAPER_SIGTERM_GRACE: Duration = Duration::from_millis(25);

#[derive(Debug, Clone)]
pub(super) struct StaleInvocationCandidate {
    pub(super) invocation_id: i64,
    pub(super) background_job_id: Option<i64>,
    pub(super) command: String,
    pub(super) pid: Option<i64>,
    /// Seconds since started_at, computed in SQL via julianday() arithmetic.
    /// `None` if started_at couldn't be parsed.
    pub(super) age_secs: Option<f64>,
}

fn background_watchdog_timeout_secs(command: &str) -> f64 {
    if command == "test" { 3600.0 } else { 1800.0 }
}

pub(super) fn background_watchdog_escape_threshold_secs(command: &str) -> f64 {
    background_watchdog_timeout_secs(command) * 2.0
}

/// Best-effort zombie reaper: SIGTERM, 2s grace, SIGKILL if still alive.
///
/// Used by the open-time sweep to clean up watchdog escapees. Returns Ok(())
/// on success or if the PID is already dead; returns Err only on system error
/// (rare: invalid PID, EPERM despite being alive).
pub(super) fn try_reap_zombie_pid(pid: i64) {
    if !(1..=i64::from(i32::MAX)).contains(&pid) {
        return;
    }
    let nix_pid = nix::unistd::Pid::from_raw(pid as i32);

    let _ = nix::sys::signal::kill(nix_pid, nix::sys::signal::Signal::SIGTERM);
    std::thread::sleep(ZOMBIE_REAPER_SIGTERM_GRACE);

    if nix::sys::signal::kill(nix_pid, None).is_ok() {
        let _ = nix::sys::signal::kill(nix_pid, nix::sys::signal::Signal::SIGKILL);
    }
}

pub(super) fn history_process_is_alive(pid: i64) -> bool {
    if !(1..=i64::from(i32::MAX)).contains(&pid) {
        return false;
    }

    let pid = nix::unistd::Pid::from_raw(pid as i32);
    matches!(
        nix::sys::signal::killpg(pid, None),
        Ok(()) | Err(nix::errno::Errno::EPERM)
    ) || matches!(
        nix::sys::signal::kill(pid, None),
        Ok(()) | Err(nix::errno::Errno::EPERM)
    )
}

/// Check if a process with the given PID is still running.
pub(super) fn is_process_running(pid: u32) -> bool {
    #[cfg(unix)]
    {
        // On Unix, sending signal 0 checks if process exists.
        unsafe { libc::kill(pid as i32, 0) == 0 }
    }
    #[cfg(not(unix))]
    {
        true
    }
}
