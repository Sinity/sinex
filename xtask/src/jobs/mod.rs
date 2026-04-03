//! Background job execution and tracking.
//!
//! Jobs are tracked in `HistoryDb` (`SQLite`) with log files in `$SINEX_STATE_DIR/jobs/<id>/`:
//! - `stdout.log` - Captured stdout
//! - `stderr.log` - Captured stderr
//!
//! `HistoryDb` is the single source of truth. `JobManager` is a thin wrapper for spawning.

use color_eyre::eyre::{Result, WrapErr, bail, eyre};
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Mutex;
use std::time::Duration;
use time::OffsetDateTime;
use tokio::process;

use crate::config::config;
use crate::history::{BackgroundJob, HistoryDb, InvocationStatus, JobLifecycleStatus};

/// A handle to a background job (backed by `HistoryDb`).
pub struct Job {
    /// Background job ID (`background_jobs.id`) — the process handle used for directories/coordinator.
    pub id: i64,
    /// Invocation ID (`invocations.id`) — the durable execution record (for stage/diagnostic queries).
    pub invocation_id: Option<i64>,
    /// Command that was run
    pub command: String,
    /// Arguments
    pub args: Vec<String>,
    /// When the job started
    pub started_at: OffsetDateTime,
    /// Process ID, or `None` if the job never exposed one.
    pub pid: Option<u32>,
    /// Process lifecycle status
    pub job_status: JobLifecycleStatus,
    /// Path to stdout log
    pub stdout_path: PathBuf,
    /// Path to stderr log
    pub stderr_path: PathBuf,
    /// Exit code (if completed)
    pub exit_code: Option<i32>,
}

impl Job {
    fn require_spawned_pid(pid: Option<u32>, command: &str, args: &[String]) -> Result<u32> {
        pid.ok_or_else(|| eyre!("spawned background job for {command} {args:?} did not expose a PID"))
    }

    fn read_archived_stream(&self, stream_name: &str) -> Result<String> {
        let cfg = config();
        let history_db_path = cfg.history_db_path();
        let db = HistoryDb::open(&history_db_path).with_context(|| {
            format!(
                "failed to open history DB at {} while reading {stream_name} for job {}",
                history_db_path.display(),
                self.id
            )
        })?;
        let (stdout, stderr) = db.get_job_logs(self.id).with_context(|| {
            format!("failed to load archived {stream_name} from history DB for job {}", self.id)
        })?;
        let archived = match stream_name {
            "stdout" => stdout,
            "stderr" => stderr,
            _ => bail!("unsupported archived job stream requested: {stream_name}"),
        };

        archived.ok_or_else(|| {
            eyre!(
                "no archived {stream_name} content recorded for terminal job {} ({})",
                self.id,
                self.job_status.as_str()
            )
        })
    }

    /// Create Job from `HistoryDb` `BackgroundJob`.
    fn from_background_job(bg: BackgroundJob, jobs_dir: &Path) -> Self {
        let stdout_path = bg.stdout_path.map_or_else(
            || jobs_dir.join(bg.id.to_string()).join("stdout.log"),
            PathBuf::from,
        );
        let stderr_path = bg.stderr_path.map_or_else(
            || jobs_dir.join(bg.id.to_string()).join("stderr.log"),
            PathBuf::from,
        );

        Self {
            id: bg.id,
            invocation_id: bg.invocation_id,
            command: bg.command,
            args: bg.args,
            started_at: bg.started_at,
            pid: bg.pid,
            job_status: bg.job_status,
            stdout_path,
            stderr_path,
            exit_code: bg.exit_code,
        }
    }

    /// Check if the job has finished.
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        self.job_status.is_terminal()
    }

    /// Read the last N lines of stdout.
    pub fn tail_stdout(&self, lines: usize) -> Result<String> {
        let content = self.read_stdout()?;
        let all_lines: Vec<&str> = content.lines().collect();
        let start = all_lines.len().saturating_sub(lines);
        Ok(all_lines[start..].join("\n"))
    }

    /// Read the last N lines of stderr.
    pub fn tail_stderr(&self, lines: usize) -> Result<String> {
        let content = self.read_stderr()?;
        let all_lines: Vec<&str> = content.lines().collect();
        let start = all_lines.len().saturating_sub(lines);
        Ok(all_lines[start..].join("\n"))
    }

    /// Read all stdout.
    ///
    /// For completed jobs, reads from DB. For running jobs, reads from file.
    pub fn read_stdout(&self) -> Result<String> {
        // Try file first (for running jobs)
        if self.stdout_path.exists() {
            return fs::read_to_string(&self.stdout_path).context("failed to read stdout");
        }
        if !self.is_terminal() {
            bail!(
                "stdout log file is missing for non-terminal job {} ({}) at {}",
                self.id,
                self.job_status.as_str(),
                self.stdout_path.display()
            );
        }
        self.read_archived_stream("stdout")
    }

    /// Read all stderr.
    ///
    /// For completed jobs, reads from DB. For running jobs, reads from file.
    pub fn read_stderr(&self) -> Result<String> {
        // Try file first (for running jobs)
        if self.stderr_path.exists() {
            return fs::read_to_string(&self.stderr_path).context("failed to read stderr");
        }
        if !self.is_terminal() {
            bail!(
                "stderr log file is missing for non-terminal job {} ({}) at {}",
                self.id,
                self.job_status.as_str(),
                self.stderr_path.display()
            );
        }
        self.read_archived_stream("stderr")
    }

    /// Check if the job process is still running.
    #[must_use]
    pub fn is_alive(&self) -> bool {
        matches!(self.job_status, JobLifecycleStatus::Running)
            && self
                .pid
                .is_some_and(|pid| Path::new(&format!("/proc/{pid}")).exists())
    }
}

/// Manager for background jobs.
///
/// This is a thin wrapper that handles process spawning and log file creation.
/// All metadata is stored in `HistoryDb`.
pub struct JobManager {
    jobs_dir: PathBuf,
    db: Mutex<HistoryDb>,
}

impl JobManager {
    fn terminal_status_from_exit_code_file(job_dir: &Path) -> Result<(InvocationStatus, Option<i32>)> {
        let exit_code_path = job_dir.join("exit_code");
        match fs::read_to_string(&exit_code_path) {
            Ok(content) => {
                let code = content.trim().parse::<i32>().with_context(|| {
                    format!(
                        "failed to parse stale background job exit code from {}",
                        exit_code_path.display()
                    )
                })?;
                if code == 0 {
                    Ok((InvocationStatus::Success, Some(0)))
                } else if code == 124 {
                    Ok((InvocationStatus::Cancelled, Some(124)))
                } else {
                    Ok((InvocationStatus::Failed, Some(code)))
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                Ok((InvocationStatus::Failed, None))
            }
            Err(error) => Err(error).with_context(|| {
                format!(
                    "failed to read stale background job exit code from {}",
                    exit_code_path.display()
                )
            }),
        }
    }

    fn finish_stale_running_job(&self, db: &HistoryDb, job: &BackgroundJob) -> Result<()> {
        let job_dir = self.jobs_dir.join(job.id.to_string());
        let (inv_status, exit_code) = Self::terminal_status_from_exit_code_file(&job_dir)?;
        let job_status = if exit_code.is_some() {
            JobLifecycleStatus::from_invocation_status(inv_status)
        } else {
            JobLifecycleStatus::Orphaned
        };

        let stdout_path = job_dir.join("stdout.log");
        let stderr_path = job_dir.join("stderr.log");
        if let Some(invocation_id) = job.invocation_id {
            db.finish_invocation(invocation_id, inv_status, exit_code, 0.0)
                .with_context(|| {
                    format!(
                        "failed to finish stale invocation {} while reaping background job {}",
                        invocation_id, job.id
                    )
                })?;
        }

        db.finish_background_job(
            job.id,
            job_status,
            exit_code,
            0.0,
            stdout_path.exists().then_some(stdout_path.as_path()),
            stderr_path.exists().then_some(stderr_path.as_path()),
        )
        .with_context(|| format!("failed to finish stale background job {}", job.id))?;

        Ok(())
    }

    /// Create a new job manager.
    pub fn new(jobs_dir: PathBuf) -> Result<Self> {
        fs::create_dir_all(&jobs_dir).context("failed to create jobs directory")?;
        let cfg = config();
        cfg.ensure_jobs_dir()?;
        let db = HistoryDb::open(&cfg.history_db_path())?;
        Ok(Self {
            jobs_dir,
            db: Mutex::new(db),
        })
    }

    /// Get the path to a job's directory.
    fn job_dir(&self, id: i64) -> PathBuf {
        self.jobs_dir.join(id.to_string())
    }

    /// Spawn an xtask command in background.
    pub fn spawn_xtask(
        &self,
        subcommand: &str,
        args: &[String],
        format: crate::output::OutputFormat,
    ) -> Result<Job> {
        let mut full_args = vec![
            "--fg".to_string(), // Force foreground since we're in a job
            "--format".to_string(),
            format.as_cli_str().to_string(),
            subcommand.to_string(),
        ];
        full_args.extend(args.iter().cloned());
        self.spawn_with_history("xtask", &full_args, subcommand, args)
    }

    /// Spawn a cargo command as a background job.
    pub fn spawn_cargo(&self, args: &[String]) -> Result<Job> {
        self.spawn("cargo", args)
    }

    /// Start a new background job.
    pub fn spawn(&self, command: &str, args: &[String]) -> Result<Job> {
        self.spawn_with_history_env(command, args, command, args, &[])
    }

    /// Start a new background job with explicit environment overrides.
    pub fn spawn_with_env(
        &self,
        command: &str,
        args: &[String],
        env_vars: &[(String, String)],
    ) -> Result<Job> {
        self.spawn_with_history_env(command, args, command, args, env_vars)
    }

    /// Start a new background job with explicit history metadata.
    fn spawn_with_history(
        &self,
        command: &str,
        args: &[String],
        history_command: &str,
        history_args: &[String],
    ) -> Result<Job> {
        self.spawn_with_history_env(command, args, history_command, history_args, &[])
    }

    fn spawn_with_history_env(
        &self,
        command: &str,
        args: &[String],
        history_command: &str,
        history_args: &[String],
        env_vars: &[(String, String)],
    ) -> Result<Job> {
        // Register with HistoryDb first to get both IDs.
        // invocation_id: the durable execution record (claimed by the child process).
        // job_id: the process handle used for directory naming and coordinator tracking.
        let (invocation_id, job_id) = {
            let db = self.db.lock().map_err(|_| eyre!("db lock poisoned"))?;
            db.start_background_job(
                history_command,
                history_args,
                None,
                Path::new(""),
                Path::new(""),
            )?
        };

        // Create job directory using job_id (not invocation_id).
        let job_dir = self.job_dir(job_id);
        fs::create_dir_all(&job_dir)?;

        let stdout_path = job_dir.join("stdout.log");
        let stderr_path = job_dir.join("stderr.log");

        // Create output files
        let stdout_file = File::create(&stdout_path)?;
        let stderr_file = File::create(&stderr_path)?;

        // Spawn the process using tokio
        // CARGO_NO_SLICE=1 bypasses the systemd-run wrapper (scripts/cargo) which would
        // otherwise run cargo commands in a systemd scope, making process control (kill, etc.)
        // unreliable. Background jobs need direct process control.
        // XTASK_JOB_DIR tells the child --fg process to write exit_code on completion.
        // XTASK_BG_INVOCATION_ID: for the child to claim the invocation row.
        // XTASK_BG_JOB_ID: for the coordinator to track the job handle.
        let mut cmd = process::Command::new(command);
        cmd.args(args)
            .env("CARGO_NO_SLICE", "1")
            .env("XTASK_JOB_DIR", &job_dir)
            .env("XTASK_BG_INVOCATION_ID", invocation_id.to_string())
            .env("XTASK_BG_JOB_ID", job_id.to_string())
            .stdout(Stdio::from(stdout_file))
            .stderr(Stdio::from(stderr_file));
        for (key, value) in env_vars {
            cmd.env(key, value);
        }

        // Make the child its own process group leader so the coordinator can
        // kill the entire group (cargo + rustc/nextest children) via kill(-pid).
        // SAFETY: setpgid is async-signal-safe per POSIX.
        unsafe {
            cmd.pre_exec(|| {
                libc::setpgid(0, 0);
                Ok(())
            });
        }

        let child = cmd
            .spawn()
            .with_context(|| format!("failed to spawn: {command} {args:?}"))?;

        let pid = Job::require_spawned_pid(child.id(), command, args)?;

        // Update background_jobs with PID and log paths (using job_id).
        {
            let db = self.db.lock().map_err(|_| eyre!("db lock poisoned"))?;
            db.update_job_pid(job_id, pid)?;
            db.update_job_paths(job_id, &stdout_path, &stderr_path)?;
        }

        // Spawn watchdog: kills the process after a max duration.
        // Uses a detached thread (not tokio) so it survives if the parent exits.
        // Max duration: check/build = 30 min, test = 60 min, others = 30 min.
        let max_duration = {
            let subcommand = if command == "xtask" {
                args.first().map_or("", String::as_str)
            } else {
                command
            };
            match subcommand {
                "test" => Duration::from_hours(1), // 60 minutes
                _ => Duration::from_mins(30),      // 30 minutes
            }
        };

        let watchdog_job_dir = job_dir.clone();
        let watchdog_db_path = config().history_db_path();
        std::thread::spawn(move || {
            std::thread::sleep(max_duration);

            // Check if process is still alive via kill(0)
            let nix_pid = nix::unistd::Pid::from_raw(pid as i32);

            let still_alive = nix::sys::signal::kill(nix_pid, None).is_ok();
            if !still_alive {
                return;
            }

            // Send SIGTERM to the entire process group (child is its own group leader)
            let _ = nix::sys::signal::killpg(nix_pid, nix::sys::signal::Signal::SIGTERM);

            // Grace period, then SIGKILL if still alive — but first verify the PID still belongs
            // to a cargo/xtask process to guard against PID reuse (R4 fix).
            std::thread::sleep(Duration::from_secs(2));
            if nix::sys::signal::kill(nix_pid, None).is_ok() && pid_is_expected_process(pid) {
                let _ = nix::sys::signal::killpg(nix_pid, nix::sys::signal::Signal::SIGKILL);
            }

            // Write exit_code=124 (standard timeout exit code) for the job reader
            let exit_code_path = watchdog_job_dir.join("exit_code");
            let _ = std::fs::write(&exit_code_path, "124\n");

            // Update history DB: finish the invocation and the job handle.
            match HistoryDb::open(&watchdog_db_path) {
                Ok(db) => {
                    if let Err(error) = db.finish_invocation(
                        invocation_id,
                        InvocationStatus::Cancelled,
                        Some(124),
                        max_duration.as_secs_f64(),
                    ) {
                        eprintln!(
                            "Warning: failed to mark timed-out invocation {invocation_id} as cancelled in history DB: {error}"
                        );
                    }
                    if let Err(error) = db.finish_background_job(
                        job_id,
                        JobLifecycleStatus::Killed,
                        Some(124),
                        max_duration.as_secs_f64(),
                        None,
                        None,
                    ) {
                        eprintln!(
                            "Warning: failed to mark timed-out background job {job_id} as killed in history DB: {error}"
                        );
                    }
                }
                Err(error) => {
                    eprintln!(
                        "Warning: failed to open history DB at {} while recording timed-out background job {job_id}: {error}",
                        watchdog_db_path.display()
                    );
                }
            }
        });

        Ok(Job {
            id: job_id,
            invocation_id: Some(invocation_id),
            command: history_command.to_string(),
            args: history_args.to_vec(),
            started_at: OffsetDateTime::now_utc(),
            pid: Some(pid),
            job_status: JobLifecycleStatus::Running,
            stdout_path,
            stderr_path,
            exit_code: None, // Job is just starting
        })
    }

    /// Get a job by ID (direct SQL lookup, O(1)).
    ///
    /// If the job is "running" but its PID is dead, automatically reaps it.
    pub fn get(&self, id: i64) -> Result<Option<Job>> {
        let db = self.db.lock().map_err(|_| eyre!("db lock poisoned"))?;
        let Some(bg) = db.get_background_job_by_id(id)? else {
            return Ok(None);
        };

        // Reap stale running entries:
        // - pid == None means we never captured a live process id
        // - Some(pid) but process no longer exists
        if matches!(bg.job_status, JobLifecycleStatus::Running) {
            let Some(pid) = bg.pid else {
                self.finish_stale_running_job(&db, &bg)?;
                let updated = db.get_background_job_by_id(id)?;
                return Ok(updated.map(|b| Job::from_background_job(b, &self.jobs_dir)));
            };

            let pid = nix::unistd::Pid::from_raw(pid as i32);
            if nix::sys::signal::kill(pid, None).is_err() {
                self.finish_stale_running_job(&db, &bg)?;
                let updated = db.get_background_job_by_id(id)?;
                return Ok(updated.map(|b| Job::from_background_job(b, &self.jobs_dir)));
            }
        }

        Ok(Some(Job::from_background_job(bg, &self.jobs_dir)))
    }

    /// List all jobs.
    pub fn list(&self) -> Result<Vec<Job>> {
        let db = self.db.lock().map_err(|_| eyre!("db lock poisoned"))?;
        let jobs = db.get_recent_background_jobs(1000)?;
        Ok(jobs
            .into_iter()
            .map(|bg| Job::from_background_job(bg, &self.jobs_dir))
            .collect())
    }

    /// List recent jobs (up to limit), reaping zombies first and pruning old ones.
    pub fn list_recent(&self, limit: usize) -> Result<Vec<Job>> {
        self.reap_zombies()?;
        self.prune(7)
            .context("failed to prune completed background jobs")?;
        let db = self.db.lock().map_err(|_| eyre!("db lock poisoned"))?;
        let jobs = db.get_recent_background_jobs(limit)?;
        Ok(jobs
            .into_iter()
            .map(|bg| Job::from_background_job(bg, &self.jobs_dir))
            .collect())
    }

    /// Reap zombie jobs: mark "running" jobs whose PIDs no longer exist as failed.
    ///
    /// This handles the case where the xtask process (or systemd scope) died
    /// without updating the DB. Called automatically by list/get operations.
    pub fn reap_zombies(&self) -> Result<usize> {
        let db = self.db.lock().map_err(|_| eyre!("db lock poisoned"))?;
        let active = db.get_active_background_jobs()?;
        let mut reaped = 0;

        for job in active {
            let Some(pid) = job.pid else {
                self.finish_stale_running_job(&db, &job)?;
                reaped += 1;
                continue;
            };

            let pid = nix::unistd::Pid::from_raw(pid as i32);
            // Signal 0 checks if process exists without sending a signal
            if nix::sys::signal::kill(pid, None).is_err() {
                self.finish_stale_running_job(&db, &job)?;
                reaped += 1;
            }
        }

        Ok(reaped)
    }

    /// List only active (running) jobs, reaping zombies first and pruning old ones.
    pub fn list_active(&self) -> Result<Vec<Job>> {
        self.reap_zombies()?;
        self.prune(7)
            .context("failed to prune completed background jobs")?;
        let db = self.db.lock().map_err(|_| eyre!("db lock poisoned"))?;
        let jobs = db.get_active_background_jobs()?;
        Ok(jobs
            .into_iter()
            .map(|bg| Job::from_background_job(bg, &self.jobs_dir))
            .collect())
    }

    /// Cancel a running job.
    ///
    /// Sends SIGTERM to the process group, marks the job as Cancelled in the DB,
    /// then spawns a background thread to SIGKILL after a 5s grace period (X10 fix).
    pub fn cancel(&self, id: i64) -> Result<bool> {
        let Some(job) = self.get(id)? else {
            return Ok(false);
        };

        if matches!(job.job_status, JobLifecycleStatus::Running) {
            if let Some(job_pid) = job.pid {
                let pid = nix::unistd::Pid::from_raw(job_pid as i32);
                send_job_signal(pid, nix::sys::signal::Signal::SIGTERM);

                // Grace period then SIGKILL if still alive (X10 fix)
                std::thread::spawn(move || {
                    std::thread::sleep(std::time::Duration::from_secs(5));
                    if job_process_is_alive(pid) {
                        send_job_signal(pid, nix::sys::signal::Signal::SIGKILL);
                    }
                });
            }

            // Update both the job handle and the invocation record.
            let db = self.db.lock().map_err(|_| eyre!("db lock poisoned"))?;
            if let Some(inv_id) = job.invocation_id {
                db.finish_invocation(inv_id, InvocationStatus::Cancelled, None, 0.0)
                    .with_context(|| {
                        format!("failed to finish cancelled invocation {inv_id} in history DB")
                    })?;
            }
            db.finish_background_job(id, JobLifecycleStatus::Killed, None, 0.0, None, None)?;

            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Wait for a job to complete.
    pub async fn wait(&self, id: i64, timeout: Option<Duration>) -> Result<Job> {
        let start = std::time::Instant::now();

        loop {
            let job = self.get(id)?.ok_or_else(|| eyre!("job {id} not found"))?;

            if job.is_terminal() {
                return Ok(job);
            }

            if let Some(timeout) = timeout
                && start.elapsed() > timeout
            {
                bail!("timeout waiting for job {id}");
            }

            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    /// Clean up old completed jobs.
    pub fn prune(&self, older_than_days: u32) -> Result<usize> {
        // Prune from HistoryDb
        let count = {
            let db = self.db.lock().map_err(|_| eyre!("db lock poisoned"))?;
            db.prune_background_jobs(older_than_days)?
        };

        // Collect valid job IDs (single DB query, lock released before fs ops)
        let valid_ids = {
            let db = self.db.lock().map_err(|_| eyre!("db lock poisoned"))?;
            db.get_all_background_job_ids()?
        };

        // Clean orphan directories (no DB lock held)
        let entries = fs::read_dir(&self.jobs_dir)
            .with_context(|| format!("failed to read jobs directory {}", self.jobs_dir.display()))?;
        for entry in entries {
            let entry = entry.with_context(|| {
                format!(
                    "failed to enumerate a background job entry in {}",
                    self.jobs_dir.display()
                )
            })?;
            if let Ok(id) = entry.file_name().to_string_lossy().parse::<i64>()
                && !valid_ids.contains(&id)
            {
                fs::remove_dir_all(entry.path()).with_context(|| {
                    format!(
                        "failed to remove orphaned background job directory {}",
                        entry.path().display()
                    )
                })?;
            }
        }

        Ok(count)
    }
}

/// Verify that the process at `pid` is still the cargo/xtask job we spawned,
/// not an unrelated process that reused the PID (R4 fix).
///
/// Reads `/proc/{pid}/cmdline` and checks for "cargo" or "xtask". If /proc
/// is unavailable or the read fails, we conservatively assume it is our process
/// (same as before this check existed) to avoid skipping necessary kills.
fn pid_is_expected_process(pid: u32) -> bool {
    let cmdline_path = format!("/proc/{pid}/cmdline");
    match std::fs::read(&cmdline_path) {
        Ok(bytes) => {
            // /proc/PID/cmdline is NUL-delimited; check if any component looks like cargo/xtask
            let cmdline = String::from_utf8_lossy(&bytes);
            cmdline.contains("cargo") || cmdline.contains("xtask")
        }
        // If /proc is unavailable (non-Linux, restricted), assume it's ours
        Err(_) => true,
    }
}

fn job_process_is_alive(pid: nix::unistd::Pid) -> bool {
    matches!(
        nix::sys::signal::killpg(pid, None),
        Ok(()) | Err(nix::errno::Errno::EPERM)
    ) || matches!(
        nix::sys::signal::kill(pid, None),
        Ok(()) | Err(nix::errno::Errno::EPERM)
    )
}

fn send_job_signal(pid: nix::unistd::Pid, signal: nix::sys::signal::Signal) {
    match nix::sys::signal::killpg(pid, signal) {
        Ok(()) => {}
        Err(nix::errno::Errno::ESRCH)
        | Err(nix::errno::Errno::EPERM)
        | Err(nix::errno::Errno::EINVAL) => {
            let _ = nix::sys::signal::kill(pid, signal);
        }
        Err(_) => {
            let _ = nix::sys::signal::kill(pid, signal);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::history::HistoryDb;
    use tempfile::tempdir;
    use xtask::sandbox::sinex_test;

    use xtask::sandbox::EnvGuard;

    #[sinex_test]
    async fn test_job_tail_stdout() -> TestResult<()> {
        let dir = tempdir()?;
        let stdout_path = dir.path().join("stdout.log");
        fs::write(&stdout_path, "line1\nline2\nline3\nline4\nline5")?;

        let job = Job {
            id: 1,
            invocation_id: None,
            command: "test".into(),
            args: vec![],
            started_at: OffsetDateTime::now_utc(),
            pid: Some(1234),
            job_status: JobLifecycleStatus::Running,
            stdout_path: stdout_path.clone(),
            stderr_path: dir.path().join("stderr.log"),
            exit_code: None,
        };

        let result = job.tail_stdout(3)?;
        assert_eq!(result, "line3\nline4\nline5");
        Ok(())
    }

    #[sinex_test]
    async fn test_require_spawned_pid_rejects_missing_pid() -> TestResult<()> {
        let error = Job::require_spawned_pid(None, "xtask", &["check".to_string()])
            .expect_err("missing PID must surface");
        let rendered = error.to_string();
        assert!(rendered.contains("did not expose a PID"));
        assert!(rendered.contains("xtask"));
        Ok(())
    }

    #[sinex_test]
    async fn test_terminal_status_from_exit_code_file() -> TestResult<()> {
        let dir = tempdir()?;
        fs::write(dir.path().join("exit_code"), "124\n")?;
        let (status, code) = JobManager::terminal_status_from_exit_code_file(dir.path())?;
        assert!(matches!(status, InvocationStatus::Cancelled));
        assert_eq!(code, Some(124));

        fs::write(dir.path().join("exit_code"), "0\n")?;
        let (status, code) = JobManager::terminal_status_from_exit_code_file(dir.path())?;
        assert!(matches!(status, InvocationStatus::Success));
        assert_eq!(code, Some(0));
        // Verify conversion to JobLifecycleStatus
        let job_status = JobLifecycleStatus::from_invocation_status(status);
        assert!(matches!(job_status, JobLifecycleStatus::Completed));

        fs::write(dir.path().join("exit_code"), "1\n")?;
        let (status, code) = JobManager::terminal_status_from_exit_code_file(dir.path())?;
        assert!(matches!(status, InvocationStatus::Failed));
        assert_eq!(code, Some(1));
        assert!(matches!(
            JobLifecycleStatus::from_invocation_status(status),
            JobLifecycleStatus::Failed
        ));

        fs::write(dir.path().join("exit_code"), "not-a-number\n")?;
        let error = JobManager::terminal_status_from_exit_code_file(dir.path())
            .expect_err("malformed stale exit code should surface");
        assert!(error
            .to_string()
            .contains("failed to parse stale background job exit code"));
        Ok(())
    }

    #[sinex_test]
    async fn test_job_read_stdout_errors_when_history_db_is_unavailable() -> TestResult<()> {
        let dir = tempdir()?;
        let blocking_parent = dir.path().join("not-a-directory");
        fs::write(&blocking_parent, "occupied")?;
        let unreadable_db_path = blocking_parent.join("xtask-history.db");
        let mut _history_db_guard = EnvGuard::new();
        _history_db_guard.set("XTASK_HISTORY_DB", &unreadable_db_path);

        let job = Job {
            id: 42,
            invocation_id: Some(7),
            command: "check".into(),
            args: vec![],
            started_at: OffsetDateTime::now_utc(),
            pid: None,
            job_status: JobLifecycleStatus::Completed,
            stdout_path: dir.path().join("missing-stdout.log"),
            stderr_path: dir.path().join("missing-stderr.log"),
            exit_code: Some(0),
        };

        let error = job
            .read_stdout()
            .expect_err("missing archived logs should surface DB access failures");
        assert!(error.to_string().contains("failed to open history DB"));
        Ok(())
    }

    #[sinex_test]
    async fn test_job_read_stdout_errors_when_running_log_file_is_missing() -> TestResult<()> {
        let dir = tempdir()?;
        let job = Job {
            id: 42,
            invocation_id: Some(7),
            command: "check".into(),
            args: vec![],
            started_at: OffsetDateTime::now_utc(),
            pid: Some(1234),
            job_status: JobLifecycleStatus::Running,
            stdout_path: dir.path().join("missing-stdout.log"),
            stderr_path: dir.path().join("missing-stderr.log"),
            exit_code: None,
        };

        let error = job
            .read_stdout()
            .expect_err("missing live stdout log should surface");
        let message = format!("{error:#}");
        assert!(message.contains("stdout log file is missing for non-terminal job 42"));
        assert!(message.contains("running"));
        Ok(())
    }

    #[sinex_test]
    async fn test_job_read_stdout_errors_when_terminal_archive_is_missing() -> TestResult<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("xtask-history.db");
        let mut _history_db_guard = EnvGuard::new();
        _history_db_guard.set("XTASK_HISTORY_DB", &db_path);
        let db = HistoryDb::open(&db_path)?;
        let stdout_path = dir.path().join("stdout.log");
        let stderr_path = dir.path().join("stderr.log");
        let (_invocation_id, job_id) =
            db.start_background_job("check", &[], Some(11111), &stdout_path, &stderr_path)?;
        db.finish_background_job(
            job_id,
            JobLifecycleStatus::Completed,
            Some(0),
            0.1,
            None,
            None,
        )?;

        let job = Job {
            id: job_id,
            invocation_id: Some(1),
            command: "check".into(),
            args: vec![],
            started_at: OffsetDateTime::now_utc(),
            pid: None,
            job_status: JobLifecycleStatus::Completed,
            stdout_path,
            stderr_path,
            exit_code: Some(0),
        };

        let error = job
            .read_stdout()
            .expect_err("missing archived stdout should surface");
        let message = format!("{error:#}");
        assert!(message.contains("no archived stdout content recorded for terminal job"));
        assert!(message.contains("completed"));
        Ok(())
    }

    #[sinex_test]
    async fn test_prune_surfaces_jobs_dir_read_failures() -> TestResult<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("xtask-history.db");
        let db = HistoryDb::open(&db_path)?;
        let jobs_dir = dir.path().join("jobs");
        fs::create_dir_all(&jobs_dir)?;
        let manager = JobManager {
            jobs_dir: jobs_dir.clone(),
            db: std::sync::Mutex::new(db),
        };

        fs::remove_dir_all(&jobs_dir)?;
        fs::write(&jobs_dir, "occupied")?;

        let error = manager
            .prune(7)
            .expect_err("jobs-dir read failures should surface");
        let message = format!("{error:#}");
        assert!(message.contains("failed to read jobs directory"));
        assert!(message.contains(jobs_dir.display().to_string().as_str()));
        Ok(())
    }

    #[sinex_test]
    async fn test_prune_surfaces_orphan_directory_removal_failures() -> TestResult<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("xtask-history.db");
        let db = HistoryDb::open(&db_path)?;
        let jobs_dir = dir.path().join("jobs");
        fs::create_dir_all(&jobs_dir)?;
        let manager = JobManager {
            jobs_dir: jobs_dir.clone(),
            db: std::sync::Mutex::new(db),
        };
        let orphan_path = jobs_dir.join("123");
        fs::write(&orphan_path, "occupied")?;

        let error = manager
            .prune(7)
            .expect_err("orphan removal failures should surface");
        let message = format!("{error:#}");
        assert!(message.contains("failed to remove orphaned background job directory"));
        assert!(message.contains(orphan_path.display().to_string().as_str()));
        Ok(())
    }

    #[sinex_test]
    async fn test_list_recent_surfaces_prune_failures() -> TestResult<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("xtask-history.db");
        let db = HistoryDb::open(&db_path)?;
        let jobs_dir = dir.path().join("jobs");
        fs::create_dir_all(&jobs_dir)?;
        let manager = JobManager {
            jobs_dir: jobs_dir.clone(),
            db: std::sync::Mutex::new(db),
        };

        fs::remove_dir_all(&jobs_dir)?;
        fs::write(&jobs_dir, "occupied")?;

        let error = match manager.list_recent(10) {
            Ok(_) => panic!("list_recent should surface prune failures"),
            Err(error) => error,
        };
        let message = format!("{error:#}");
        assert!(message.contains("failed to prune completed background jobs"));
        assert!(message.contains("failed to read jobs directory"));
        Ok(())
    }

    #[sinex_test]
    async fn test_list_active_surfaces_prune_failures() -> TestResult<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("xtask-history.db");
        let db = HistoryDb::open(&db_path)?;
        let jobs_dir = dir.path().join("jobs");
        fs::create_dir_all(&jobs_dir)?;
        let manager = JobManager {
            jobs_dir: jobs_dir.clone(),
            db: std::sync::Mutex::new(db),
        };

        fs::remove_dir_all(&jobs_dir)?;
        fs::write(&jobs_dir, "occupied")?;

        let error = match manager.list_active() {
            Ok(_) => panic!("list_active should surface prune failures"),
            Err(error) => error,
        };
        let message = format!("{error:#}");
        assert!(message.contains("failed to prune completed background jobs"));
        assert!(message.contains("failed to read jobs directory"));
        Ok(())
    }

    #[sinex_test]
    async fn test_get_reaps_stale_running_job_and_finishes_invocation() -> TestResult<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("xtask-history.db");
        let db = HistoryDb::open(&db_path)?;
        let jobs_dir = dir.path().join("jobs");
        fs::create_dir_all(&jobs_dir)?;
        let manager = JobManager {
            jobs_dir: jobs_dir.clone(),
            db: std::sync::Mutex::new(db),
        };

        let stdout_path = jobs_dir.join("42").join("stdout.log");
        let stderr_path = jobs_dir.join("42").join("stderr.log");
        if let Some(parent) = stdout_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let (invocation_id, job_id) =
            manager
                .db
                .lock()
                .map_err(|_| eyre!("db lock poisoned"))?
                .start_background_job("check", &[], None, &stdout_path, &stderr_path)?;
        fs::create_dir_all(jobs_dir.join(job_id.to_string()))?;
        drop(fs::File::create(jobs_dir.join(job_id.to_string()).join("stdout.log"))?);
        drop(fs::File::create(jobs_dir.join(job_id.to_string()).join("stderr.log"))?);

        let job = manager
            .get(job_id)?
            .ok_or_else(|| eyre!("reaped job should still be queryable"))?;
        assert!(matches!(job.job_status, JobLifecycleStatus::Orphaned));

        let db = manager.db.lock().map_err(|_| eyre!("db lock poisoned"))?;
        let invocation = db
            .get_invocation_full(invocation_id)?
            .ok_or_else(|| eyre!("missing invocation after reaping stale job"))?;
        assert_eq!(invocation.invocation.status, InvocationStatus::Failed);
        assert!(invocation.invocation.finished_at.is_some());
        Ok(())
    }

    #[sinex_test]
    async fn test_cancel_finishes_linked_invocation() -> TestResult<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("xtask-history.db");
        let db = HistoryDb::open(&db_path)?;
        let jobs_dir = dir.path().join("jobs");
        fs::create_dir_all(&jobs_dir)?;
        let manager = JobManager {
            jobs_dir: jobs_dir.clone(),
            db: std::sync::Mutex::new(db),
        };

        let mut child = std::process::Command::new("sleep")
            .arg("60")
            .spawn()
            .map_err(|error| eyre!("failed to spawn sleep process for cancellation test: {error}"))?;
        let stdout_path = jobs_dir.join("stdout.log");
        let stderr_path = jobs_dir.join("stderr.log");
        let (invocation_id, job_id) =
            manager
                .db
                .lock()
                .map_err(|_| eyre!("db lock poisoned"))?
                .start_background_job("check", &[], Some(child.id()), &stdout_path, &stderr_path)?;

        assert!(manager.cancel(job_id)?);

        let mut child_exited = false;
        for _ in 0..40 {
            if child.try_wait()?.is_some() {
                child_exited = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(
            child_exited,
            "cancel should still terminate legacy single-process jobs without a dedicated process group"
        );

        let db = manager.db.lock().map_err(|_| eyre!("db lock poisoned"))?;
        let invocation = db
            .get_invocation_full(invocation_id)?
            .ok_or_else(|| eyre!("missing invocation after cancellation"))?;
        assert_eq!(invocation.invocation.status, InvocationStatus::Cancelled);
        assert!(invocation.invocation.finished_at.is_some());

        let job = db
            .get_background_job_by_id(job_id)?
            .ok_or_else(|| eyre!("missing background job after cancellation"))?;
        assert!(matches!(job.job_status, JobLifecycleStatus::Killed));
        Ok(())
    }

    #[sinex_test]
    async fn test_get_surfaces_malformed_exit_code_for_stale_job() -> TestResult<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("xtask-history.db");
        let db = HistoryDb::open(&db_path)?;
        let jobs_dir = dir.path().join("jobs");
        fs::create_dir_all(&jobs_dir)?;
        let manager = JobManager {
            jobs_dir: jobs_dir.clone(),
            db: std::sync::Mutex::new(db),
        };

        let stdout_path = jobs_dir.join("77").join("stdout.log");
        let stderr_path = jobs_dir.join("77").join("stderr.log");
        if let Some(parent) = stdout_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let (_invocation_id, job_id) =
            manager
                .db
                .lock()
                .map_err(|_| eyre!("db lock poisoned"))?
                .start_background_job("check", &[], None, &stdout_path, &stderr_path)?;
        let job_dir = jobs_dir.join(job_id.to_string());
        fs::create_dir_all(&job_dir)?;
        fs::write(job_dir.join("exit_code"), "bogus\n")?;

        let error = match manager.get(job_id) {
            Ok(_) => panic!("malformed stale exit code should surface during reaping"),
            Err(error) => error,
        };
        let message = format!("{error:#}");
        assert!(message.contains("failed to parse stale background job exit code"));
        assert!(message.contains("exit_code"));
        Ok(())
    }
}
