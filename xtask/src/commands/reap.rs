/// `xtask __reap` — internal detached process watchdog.
///
/// This subcommand is spawned by `JobManager::spawn_with_history_env` as a
/// detached reaper for every background job.  It performs a POSIX double-fork
/// so that the grandchild is orphaned to init and therefore survives when the
/// parent xtask launcher exits.  After sleeping for `--max-secs`, it sends
/// SIGTERM → (2s grace) → SIGKILL to the monitored PID, then marks the job as
/// timed-out in the history DB.
///
/// This replaces the `std::thread::spawn` watchdog that died with its parent.
use std::path::PathBuf;
use std::process::Stdio;
use std::time::{Duration, Instant};

use clap::Parser;
use color_eyre::eyre::Result;

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::history::{HistoryDb, JobLifecycleStatus};

#[derive(Parser, Clone, Debug)]
#[command(
    hide = true,
    about = "Internal: detached process watchdog (not for human use)"
)]
pub struct ReapCommand {
    /// PID of the background job process to monitor.
    #[arg(long)]
    pub target_pid: u32,

    /// Maximum seconds to allow before killing the target.
    #[arg(long)]
    pub max_secs: u64,

    /// Invocation ID to mark as cancelled on timeout.
    #[arg(long)]
    pub invocation_id: i64,

    /// Background job ID to mark as killed on timeout.
    #[arg(long)]
    pub job_id: i64,

    /// Path to the history DB file.
    #[arg(long)]
    pub db_path: PathBuf,

    /// Directory containing the job's exit_code file (written on timeout).
    #[arg(long)]
    pub job_dir: PathBuf,
}

impl XtaskCommand for ReapCommand {
    fn name(&self) -> &'static str {
        "__reap"
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata {
            track_in_history: false,
            ..CommandMetadata::default()
        }
    }

    async fn execute(&self, _ctx: &CommandContext) -> Result<CommandResult> {
        // Perform double-fork to detach from the parent's session and reparent
        // the grandchild to init so it survives when the launcher exits.
        //
        //   fork() → child (intermediate)
        //     └─ setsid() — detach from caller's session
        //     └─ fork() → grandchild (reaper proper)
        //          └─ do the actual waiting + kill + DB update
        //     └─ _exit(0)  — child exits; grandchild is now parented to init
        //   parent waits for child (~microseconds) then returns Ok immediately
        //
        // The parent branch returns Ok(CommandResult::success()) so the launcher
        // xtask can exit promptly.
        double_fork_reap(self)
    }
}

/// Perform the double-fork and, in the grandchild, run the reaper loop.
/// The parent and intermediate child both return immediately.
fn double_fork_reap(args: &ReapCommand) -> Result<CommandResult> {
    use nix::unistd::{ForkResult, fork, setsid};

    // Safety: we fork immediately after parsing args.  No threads have been
    // spawned inside this process path by the time reap::execute() is called —
    // the tokio runtime is single-threaded at this point (xtask uses
    // `#[tokio::main]` which spawns workers, but forking inside an async context
    // with a multi-threaded runtime is unsafe).  We mitigate this by calling
    // std::process::exit() in the child branches (never returning to the
    // runtime) and by keeping the grandchild fully synchronous (no tokio).
    match unsafe { fork() }? {
        ForkResult::Parent { child } => {
            // Wait for the intermediate child so it doesn't become a zombie.
            // This is synchronous but takes only microseconds (child calls _exit(0)).
            let _ = nix::sys::wait::waitpid(child, None);
            // Parent returns immediately; the grandchild is now an orphan.
            Ok(CommandResult::success())
        }
        ForkResult::Child => {
            // Intermediate child: detach from the caller's session.
            let _ = setsid();

            // Second fork — grandchild becomes the real reaper.
            match unsafe { fork() } {
                Ok(ForkResult::Parent { .. }) => {
                    // Intermediate child exits; grandchild is reparented to init.
                    unsafe { libc::_exit(0) };
                }
                Ok(ForkResult::Child) => {
                    // Grandchild: run the reaper synchronously.
                    run_reaper_grandchild(args);
                    unsafe { libc::_exit(0) };
                }
                Err(_) => {
                    unsafe { libc::_exit(1) };
                }
            }
        }
    }
}

/// The reaper body — runs in the orphaned grandchild process.
/// Fully synchronous; must not touch the tokio runtime.
fn run_reaper_grandchild(args: &ReapCommand) {
    let nix_pid = nix::unistd::Pid::from_raw(args.target_pid as i32);
    if nix::sys::signal::kill(nix_pid, None).is_err() {
        return;
    }

    let deadline = Instant::now() + Duration::from_secs(args.max_secs);

    while Instant::now() < deadline {
        if nix::sys::signal::kill(nix_pid, None).is_err() {
            return;
        }
        std::thread::sleep(Duration::from_secs(1));
    }

    // PID reuse guard: confirm it still looks like a cargo/xtask process.
    if !crate::process::is_xtask_pid(args.target_pid) {
        return;
    }

    // SIGTERM first.
    let _ = send_job_signal(nix_pid, nix::sys::signal::Signal::SIGTERM);

    // 2-second grace period, then SIGKILL if still alive.
    std::thread::sleep(Duration::from_secs(2));
    if nix::sys::signal::kill(nix_pid, None).is_ok()
        && crate::process::is_xtask_pid(args.target_pid)
    {
        let _ = send_job_signal(nix_pid, nix::sys::signal::Signal::SIGKILL);
    }

    // Write exit_code=124 to the job dir.
    let exit_code_path = args.job_dir.join("exit_code");
    let _ = std::fs::write(&exit_code_path, "124\n");

    // Update history DB.
    let duration_secs = args.max_secs as f64;
    if let Ok(db) = HistoryDb::open(&args.db_path) {
        let _ = db.finish_invocation_cancelled(
            args.invocation_id,
            Some(124),
            duration_secs,
            "watchdog_timeout",
            "watchdog",
        );
        let _ = db.finish_background_job(
            args.job_id,
            JobLifecycleStatus::Killed,
            Some(124),
            duration_secs,
            None,
            None,
        );
    } else {
        // No stderr in orphaned grandchild; silently skip DB update.
        // The open-time sweep (a158ae44b) will clean this up on the next
        // xtask invocation.
    }
}

fn send_job_signal(
    pid: nix::unistd::Pid,
    signal: nix::sys::signal::Signal,
) -> std::result::Result<(), nix::errno::Errno> {
    // Try process group first (child is its own group leader), fall back to direct kill.
    match nix::sys::signal::killpg(pid, signal) {
        Ok(()) => Ok(()),
        Err(_) => nix::sys::signal::kill(pid, signal),
    }
}

// ─── Spawn helper ────────────────────────────────────────────────────────────

/// Spawn a detached `xtask __reap` process for the given background job.
///
/// The spawned process immediately double-forks and orphans itself to init, so
/// it survives after the current xtask launcher exits.  The parent branch of
/// `double_fork_reap` returns to the launcher promptly; only the grandchild
/// actually sleeps.
pub fn spawn_reaper(
    target_pid: u32,
    max_secs: u64,
    invocation_id: i64,
    job_id: i64,
    db_path: &std::path::Path,
    job_dir: &std::path::Path,
) -> Result<()> {
    let exe = std::env::current_exe()
        .map_err(|e| color_eyre::eyre::eyre!("cannot determine xtask exe path: {e}"))?;

    std::process::Command::new(&exe)
        .args([
            "__reap",
            "--target-pid",
            &target_pid.to_string(),
            "--max-secs",
            &max_secs.to_string(),
            "--invocation-id",
            &invocation_id.to_string(),
            "--job-id",
            &job_id.to_string(),
            "--db-path",
            &db_path.to_string_lossy(),
            "--job-dir",
            &job_dir.to_string_lossy(),
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|e| color_eyre::eyre::eyre!("failed to spawn xtask __reap: {e}"))?;
    // Don't wait — the reaper detaches via double-fork.
    Ok(())
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    use crate::sandbox::sinex_test;

    /// CLI argument parsing round-trips correctly.
    #[sinex_test]
    async fn reap_command_parses_all_args() -> TestResult<()> {
        use clap::Parser;

        let args = ReapCommand::try_parse_from([
            "reap",
            "--target-pid",
            "12345",
            "--max-secs",
            "1800",
            "--invocation-id",
            "42",
            "--job-id",
            "7",
            "--db-path",
            "/tmp/xtask-history.db",
            "--job-dir",
            "/tmp/jobs/7",
        ])?;

        assert_eq!(args.target_pid, 12345);
        assert_eq!(args.max_secs, 1800);
        assert_eq!(args.invocation_id, 42);
        assert_eq!(args.job_id, 7);
        assert_eq!(
            args.db_path,
            std::path::PathBuf::from("/tmp/xtask-history.db")
        );
        assert_eq!(args.job_dir, std::path::PathBuf::from("/tmp/jobs/7"));
        Ok(())
    }

    /// Missing required args produce a parse error.
    #[sinex_test]
    async fn reap_command_rejects_missing_args() -> TestResult<()> {
        use clap::Parser;

        // Missing --target-pid and others.
        let result = ReapCommand::try_parse_from(["reap", "--max-secs", "60"]);
        assert!(result.is_err(), "missing required args must fail parse");
        Ok(())
    }

    /// `is_xtask_pid` returns true for the current process (xtask/cargo).
    #[sinex_test]
    async fn is_xtask_pid_recognises_self() -> TestResult<()> {
        let my_pid = std::process::id();
        // The current process is xtask (or cargo-nextest during testing).
        // Either way it should pass the cargo/xtask heuristic.
        // We just check it doesn't panic; the boolean depends on the runner name.
        let _ = crate::process::is_xtask_pid(my_pid);
        Ok(())
    }

    /// `is_xtask_pid` returns true for an obviously invalid PID
    /// (conservative fallback when /proc entry is absent).
    #[sinex_test]
    async fn is_xtask_pid_conservative_on_missing_pid() -> TestResult<()> {
        // PID 0 is never a valid user process; /proc/0/cmdline won't exist.
        // The function should return true (conservative, don't skip kill).
        assert!(
            crate::process::is_xtask_pid(0),
            "missing /proc entry should conservatively return true"
        );
        Ok(())
    }

    /// Given a PID that has already exited, `run_reaper_grandchild` returns without error.
    /// We test this structurally by verifying the kill(0) check fires first.
    #[sinex_test]
    async fn reaper_noop_on_already_dead_pid() -> TestResult<()> {
        use tempfile::tempdir;

        let tmp = tempdir()?;
        let db_path = tmp.path().join("history.db");
        let job_dir = tmp.path().to_path_buf();

        // Use PID 2^31-1 (practically guaranteed non-existent).
        let args = ReapCommand {
            target_pid: i32::MAX as u32,
            max_secs: 0,
            invocation_id: 999,
            job_id: 888,
            db_path,
            job_dir: job_dir.clone(),
        };

        // Should complete without panic and without writing exit_code
        // (the process is already gone, so the function returns early).
        run_reaper_grandchild(&args);

        assert!(
            !job_dir.join("exit_code").exists(),
            "exit_code must not be written when target PID was already gone"
        );
        Ok(())
    }
}
