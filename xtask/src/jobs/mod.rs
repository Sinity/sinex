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
use crate::process::{configure_background_job_child_tokio, terminate_process_tree_by_root_pid};

#[cfg(not(test))]
const CANCEL_SIGTERM_GRACE: Duration = Duration::from_secs(5);
#[cfg(test)]
const CANCEL_SIGTERM_GRACE: Duration = Duration::from_millis(100);
const CANCEL_POLL_INTERVAL: Duration = Duration::from_millis(25);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BackgroundWatchdog {
    Default,
    Disabled,
}

fn default_watchdog_secs(command: &str, args: &[String]) -> u64 {
    let subcommand = if command == "xtask" {
        args.first().map_or("", String::as_str)
    } else {
        command
    };
    match subcommand {
        "test" => 3600, // 60 minutes
        _ => 1800,      // 30 minutes
    }
}

/// A handle to a background job (backed by `HistoryDb`).
#[derive(Clone)]
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
        pid.ok_or_else(|| {
            eyre!("spawned background job for {command} {args:?} did not expose a PID")
        })
    }

    fn read_archived_stream(&self, stream_name: &str) -> Result<String> {
        let cfg = config();
        let history_db_path = cfg.history_db_path();
        let db = HistoryDb::open_query(&history_db_path).with_context(|| {
            format!(
                "failed to open history DB at {} while reading {stream_name} for job {}",
                history_db_path.display(),
                self.id
            )
        })?;
        let (stdout, stderr) = db.get_job_logs(self.id).with_context(|| {
            format!(
                "failed to load archived {stream_name} from history DB for job {}",
                self.id
            )
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

fn background_job_is_live(job: &BackgroundJob) -> bool {
    let Some(pid) = job.pid else {
        return false;
    };
    nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid as i32), None).is_ok()
}

pub(crate) fn snapshot_recent_and_active_from_history_db(
    db: &HistoryDb,
    jobs_dir: &Path,
    limit: usize,
) -> Result<(Vec<Job>, Vec<Job>)> {
    let active_started_at = std::time::Instant::now();
    let active = db
        .get_active_background_jobs()?
        .into_iter()
        .filter(background_job_is_live)
        .map(|bg| synthesize_job_for_query(bg, jobs_dir))
        .filter(|job| job.as_ref().is_ok_and(|job| !job.is_terminal()))
        .collect::<Result<Vec<_>>>()?;
    if std::env::var("SINEX_STATUS_PROFILE").is_ok() {
        eprintln!(
            "[status-profile] jobs.get_active_background_jobs: {:.3}s",
            active_started_at.elapsed().as_secs_f64()
        );
    }

    let recent_started_at = std::time::Instant::now();
    let recent = db
        .get_recent_background_jobs(limit)?
        .into_iter()
        .map(|bg| synthesize_job_for_query(bg, jobs_dir))
        .collect::<Result<Vec<_>>>()?;
    if std::env::var("SINEX_STATUS_PROFILE").is_ok() {
        eprintln!(
            "[status-profile] jobs.get_recent_background_jobs: {:.3}s",
            recent_started_at.elapsed().as_secs_f64()
        );
    }
    Ok((active, recent))
}

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

fn synthesize_job_for_query(bg: BackgroundJob, jobs_dir: &Path) -> Result<Job> {
    let mut job = Job::from_background_job(bg, jobs_dir);
    if matches!(job.job_status, JobLifecycleStatus::Running) {
        let job_dir = jobs_dir.join(job.id.to_string());
        let (invocation_status, exit_code) = terminal_status_from_exit_code_file(&job_dir)?;
        job.job_status = if exit_code.is_some() {
            JobLifecycleStatus::from_invocation_status(invocation_status)
        } else if !job.is_alive() {
            JobLifecycleStatus::Orphaned
        } else {
            return Ok(job);
        };
        job.exit_code = exit_code;
    }
    Ok(job)
}

/// Read-only job catalog for observational commands.
///
/// This opens the history DB in query mode so status/list/output surfaces remain
/// responsive while a writer is actively recording live progress.
pub struct JobQueryManager {
    jobs_dir: PathBuf,
    db: HistoryDb,
}

impl JobQueryManager {
    pub fn new(jobs_dir: PathBuf) -> Result<Self> {
        fs::create_dir_all(&jobs_dir).context("failed to create jobs directory")?;
        let cfg = config();
        cfg.ensure_jobs_dir()?;
        let db = HistoryDb::open_query(&cfg.history_db_path())?;
        Ok(Self { jobs_dir, db })
    }

    pub fn get(&self, id: i64) -> Result<Option<Job>> {
        let Some(bg) = self.db.get_background_job_by_id(id)? else {
            return Ok(None);
        };
        Ok(Some(synthesize_job_for_query(bg, &self.jobs_dir)?))
    }

    pub fn list_recent(&self, limit: usize) -> Result<Vec<Job>> {
        self.db
            .get_recent_background_jobs(limit)?
            .into_iter()
            .map(|bg| synthesize_job_for_query(bg, &self.jobs_dir))
            .collect()
    }

    pub fn list_active(&self) -> Result<Vec<Job>> {
        self.db
            .get_active_background_jobs()?
            .into_iter()
            .filter(background_job_is_live)
            .map(|bg| synthesize_job_for_query(bg, &self.jobs_dir))
            .filter(|job| job.as_ref().is_ok_and(|job| !job.is_terminal()))
            .collect()
    }

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
    fn finish_stale_running_job(&self, db: &HistoryDb, job: &BackgroundJob) -> Result<()> {
        let job_dir = self.jobs_dir.join(job.id.to_string());
        let (inv_status, exit_code) = terminal_status_from_exit_code_file(&job_dir)?;
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
        self.spawn_with_history_env(
            command,
            args,
            command,
            args,
            &[],
            BackgroundWatchdog::Default,
        )
    }

    /// Start a new background job with explicit environment overrides.
    pub fn spawn_with_env(
        &self,
        command: &str,
        args: &[String],
        env_vars: &[(String, String)],
    ) -> Result<Job> {
        self.spawn_with_history_env(
            command,
            args,
            command,
            args,
            env_vars,
            BackgroundWatchdog::Default,
        )
    }

    /// Start a long-lived runtime job without a detached timeout watchdog.
    ///
    /// Use this for explicit runtime lifecycle commands such as `xtask run --bg`.
    /// Build, check, and test jobs should keep the default watchdog so abandoned
    /// finite work cannot accumulate forever.
    pub fn spawn_with_env_without_watchdog(
        &self,
        command: &str,
        args: &[String],
        env_vars: &[(String, String)],
    ) -> Result<Job> {
        self.spawn_with_history_env(
            command,
            args,
            command,
            args,
            env_vars,
            BackgroundWatchdog::Disabled,
        )
    }

    /// Start a new background job with explicit history metadata.
    fn spawn_with_history(
        &self,
        command: &str,
        args: &[String],
        history_command: &str,
        history_args: &[String],
    ) -> Result<Job> {
        self.spawn_with_history_env(
            command,
            args,
            history_command,
            history_args,
            &[],
            BackgroundWatchdog::Default,
        )
    }

    fn spawn_with_history_env(
        &self,
        command: &str,
        args: &[String],
        history_command: &str,
        history_args: &[String],
        env_vars: &[(String, String)],
        watchdog: BackgroundWatchdog,
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

        // Spawn the process using tokio.
        // XTASK_JOB_DIR tells the child --fg process to write exit_code on completion.
        // XTASK_BG_INVOCATION_ID: for the child to claim the invocation row.
        // XTASK_BG_JOB_ID: for the coordinator to track the job handle.
        let mut cmd = process::Command::new(command);
        cmd.args(args)
            .env("XTASK_JOB_DIR", &job_dir)
            .env("XTASK_BG_INVOCATION_ID", invocation_id.to_string())
            .env("XTASK_BG_JOB_ID", job_id.to_string())
            .stdout(Stdio::from(stdout_file))
            .stderr(Stdio::from(stderr_file));
        for (key, value) in env_vars {
            cmd.env(key, value);
        }

        configure_background_job_child_tokio(&mut cmd);

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

        if watchdog == BackgroundWatchdog::Default {
            // Spawn a detached reaper via `xtask __reap` (double-fork orphan).
            // The reaper survives when the launcher xtask exits — unlike
            // std::thread, which dies with its parent process.
            let watchdog_db_path = config().history_db_path();
            if let Err(error) = crate::commands::reap::spawn_reaper(
                pid,
                default_watchdog_secs(command, args),
                invocation_id,
                job_id,
                &watchdog_db_path,
                &job_dir,
            ) {
                eprintln!(
                    "Warning: failed to spawn detached reaper for background job {job_id} (pid {pid}): {error}"
                );
            }
        }

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

    /// Read active and recent jobs with a single maintenance pass.
    pub fn snapshot_recent_and_active(&self, limit: usize) -> Result<(Vec<Job>, Vec<Job>)> {
        self.reap_zombies()?;
        self.prune(7)
            .context("failed to prune completed background jobs")?;
        let db = self.db.lock().map_err(|_| eyre!("db lock poisoned"))?;
        let active = db
            .get_active_background_jobs()?
            .into_iter()
            .map(|bg| Job::from_background_job(bg, &self.jobs_dir))
            .collect();
        let recent = db
            .get_recent_background_jobs(limit)?
            .into_iter()
            .map(|bg| Job::from_background_job(bg, &self.jobs_dir))
            .collect();
        Ok((active, recent))
    }

    /// Read active and recent jobs without full directory-prune maintenance.
    ///
    /// This is for latency-sensitive status surfaces where pruning old completed
    /// job directories is unnecessary noise. We still reap obviously stale
    /// "running" rows so status does not claim phantom background jobs.
    pub fn snapshot_recent_and_active_fast(&self, limit: usize) -> Result<(Vec<Job>, Vec<Job>)> {
        let mut active_jobs = {
            let db = self.db.lock().map_err(|_| eyre!("db lock poisoned"))?;
            db.get_active_background_jobs()?
        };

        let needs_reap = active_jobs.iter().any(|job| {
            let Some(pid) = job.pid else {
                return true;
            };
            nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid as i32), None).is_err()
        });

        if needs_reap {
            self.reap_zombies()?;
            let db = self.db.lock().map_err(|_| eyre!("db lock poisoned"))?;
            active_jobs = db.get_active_background_jobs()?;
        }

        let recent_jobs = {
            let db = self.db.lock().map_err(|_| eyre!("db lock poisoned"))?;
            db.get_recent_background_jobs(limit)?
        };

        let active = active_jobs
            .into_iter()
            .map(|bg| Job::from_background_job(bg, &self.jobs_dir))
            .collect();
        let recent = recent_jobs
            .into_iter()
            .map(|bg| Job::from_background_job(bg, &self.jobs_dir))
            .collect();
        Ok((active, recent))
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
    /// Sends SIGTERM to the process group, escalates to SIGKILL after a bounded
    /// grace period if needed, then marks the job as cancelled in the DB.
    pub fn cancel(&self, id: i64) -> Result<bool> {
        let Some(job) = self.get(id)? else {
            return Ok(false);
        };

        if matches!(job.job_status, JobLifecycleStatus::Running) {
            if let Some(job_pid) = job.pid {
                let _ = terminate_process_tree_by_root_pid(
                    job_pid,
                    &format!("cancelling background job {id}"),
                )?;
                let pid = nix::unistd::Pid::from_raw(job_pid as i32);
                match terminate_job_process(pid)? {
                    SignalDelivery::Delivered => {}
                    SignalDelivery::Missing => {
                        self.reap_zombies()?;
                        return Ok(false);
                    }
                }
            }

            // Update both the job handle and the invocation record.
            let db = self.db.lock().map_err(|_| eyre!("db lock poisoned"))?;
            if let Some(inv_id) = job.invocation_id {
                db.finish_invocation_cancelled(inv_id, None, 0.0, "user_cancel", "user")
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
        let entries = fs::read_dir(&self.jobs_dir).with_context(|| {
            format!("failed to read jobs directory {}", self.jobs_dir.display())
        })?;
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

fn terminate_job_process(pid: nix::unistd::Pid) -> Result<SignalDelivery> {
    match send_job_signal(pid, nix::sys::signal::Signal::SIGTERM)? {
        SignalDelivery::Delivered => {}
        SignalDelivery::Missing => return Ok(SignalDelivery::Missing),
    }

    let deadline = std::time::Instant::now() + CANCEL_SIGTERM_GRACE;
    while std::time::Instant::now() < deadline {
        if !job_process_is_alive(pid) {
            return Ok(SignalDelivery::Delivered);
        }
        std::thread::sleep(CANCEL_POLL_INTERVAL);
    }

    if job_process_is_alive(pid) {
        send_job_signal(pid, nix::sys::signal::Signal::SIGKILL)?;
    }

    Ok(SignalDelivery::Delivered)
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SignalDelivery {
    Delivered,
    Missing,
}

fn send_job_signal(
    pid: nix::unistd::Pid,
    signal: nix::sys::signal::Signal,
) -> Result<SignalDelivery> {
    match nix::sys::signal::killpg(pid, signal) {
        Ok(()) => Ok(SignalDelivery::Delivered),
        Err(nix::errno::Errno::ESRCH | nix::errno::Errno::EPERM | nix::errno::Errno::EINVAL) => {
            match nix::sys::signal::kill(pid, signal) {
                Ok(()) => Ok(SignalDelivery::Delivered),
                Err(nix::errno::Errno::ESRCH) => Ok(SignalDelivery::Missing),
                Err(error) => Err(eyre!("failed to send {signal:?} to job pid {pid}: {error}")),
            }
        }
        Err(error) => match nix::sys::signal::kill(pid, signal) {
            Ok(()) => Ok(SignalDelivery::Delivered),
            Err(nix::errno::Errno::ESRCH) => Ok(SignalDelivery::Missing),
            Err(fallback_error) => Err(eyre!(
                "failed to send {signal:?} to job pid {pid}: process-group error {error}; process error {fallback_error}"
            )),
        },
    }
}

#[cfg(test)]
#[path = "../jobs_test.rs"]
mod tests;
