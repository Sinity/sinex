//! Background job execution and tracking.
//!
//! Jobs are tracked in `HistoryDb` (`SQLite`) with log files in `$SINEX_STATE_DIR/jobs/<id>/`:
//! - `stdout.log` - Captured stdout
//! - `stderr.log` - Captured stderr
//!
//! `HistoryDb` is the single source of truth. `JobManager` is a thin wrapper for spawning.

use anyhow::{bail, Context, Result};
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Mutex;
use std::time::Duration;
use time::OffsetDateTime;
use tokio::process;

use crate::config::config;
use crate::history::{BackgroundJob, HistoryDb, InvocationStatus};

/// A handle to a background job (backed by `HistoryDb`).
pub struct Job {
    /// `HistoryDb` invocation ID
    pub id: i64,
    /// Command that was run
    pub command: String,
    /// Arguments
    pub args: Vec<String>,
    /// When the job started
    pub started_at: OffsetDateTime,
    /// Process ID (if running)
    pub pid: u32,
    /// Current status
    pub status: InvocationStatus,
    /// Path to stdout log
    pub stdout_path: PathBuf,
    /// Path to stderr log
    pub stderr_path: PathBuf,
}

impl Job {
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
            command: bg.command,
            args: bg.args,
            started_at: bg.started_at,
            pid: bg.pid,
            status: bg.status,
            stdout_path,
            stderr_path,
        }
    }

    /// Check if the job has finished.
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        !matches!(self.status, InvocationStatus::Running)
    }

    /// Read the last N lines of stdout.
    pub fn tail_stdout(&self, lines: usize) -> Result<String> {
        let content = self.read_stdout()?;
        let all_lines: Vec<&str> = content.lines().collect();
        let start = all_lines.len().saturating_sub(lines);
        Ok(all_lines[start..].join("\n"))
    }

    /// Read the last N lines of stderr.
    #[allow(dead_code)]
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
        // Fall back to DB (for completed jobs)
        let cfg = config();
        if let Ok(db) = HistoryDb::open(&cfg.history_db_path()) {
            if let Ok((Some(content), _)) = db.get_job_logs(self.id) {
                return Ok(content);
            }
        }
        Ok(String::new())
    }

    /// Read all stderr.
    ///
    /// For completed jobs, reads from DB. For running jobs, reads from file.
    pub fn read_stderr(&self) -> Result<String> {
        // Try file first (for running jobs)
        if self.stderr_path.exists() {
            return fs::read_to_string(&self.stderr_path).context("failed to read stderr");
        }
        // Fall back to DB (for completed jobs)
        let cfg = config();
        if let Ok(db) = HistoryDb::open(&cfg.history_db_path()) {
            if let Ok((_, Some(content))) = db.get_job_logs(self.id) {
                return Ok(content);
            }
        }
        Ok(String::new())
    }

    /// Check if the job process is still running.
    #[must_use]
    pub fn is_alive(&self) -> bool {
        if matches!(self.status, InvocationStatus::Running) && self.pid > 0 {
            Path::new(&format!("/proc/{}", self.pid)).exists()
        } else {
            false
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
    /// Create a new job manager.
    pub fn new(jobs_dir: PathBuf) -> Result<Self> {
        fs::create_dir_all(&jobs_dir).context("failed to create jobs directory")?;
        let cfg = config();
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
    pub async fn spawn_xtask(&self, subcommand: &str, args: &[String]) -> Result<Job> {
        let mut full_args = vec![
            "xtask".to_string(),
            "--fg".to_string(), // Force foreground since we're in a job
            subcommand.to_string(),
        ];
        full_args.extend(args.iter().cloned());
        self.spawn("cargo", &full_args).await
    }

    /// Spawn a cargo command as a background job.
    pub async fn spawn_cargo(&self, args: &[String]) -> Result<Job> {
        self.spawn("cargo", args).await
    }

    /// Start a new background job.
    pub async fn spawn(&self, command: &str, args: &[String]) -> Result<Job> {
        // Register with HistoryDb first to get the ID
        let history_id = {
            let db = self
                .db
                .lock()
                .map_err(|_| anyhow::anyhow!("db lock poisoned"))?;
            db.start_background_job(command, args, 0, Path::new(""), Path::new(""))?
        };

        // Create job directory using HistoryDb ID
        let job_dir = self.job_dir(history_id);
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
        let mut cmd = process::Command::new(command);
        cmd.args(args)
            .env("CARGO_NO_SLICE", "1")
            .stdout(Stdio::from(stdout_file))
            .stderr(Stdio::from(stderr_file));

        let child = cmd
            .spawn()
            .with_context(|| format!("failed to spawn: {command} {args:?}"))?;

        let pid = child.id().unwrap_or(0);

        // Update HistoryDb with PID and log paths
        {
            let db = self
                .db
                .lock()
                .map_err(|_| anyhow::anyhow!("db lock poisoned"))?;
            db.update_job_pid(history_id, pid)?;
            db.update_job_paths(history_id, &stdout_path, &stderr_path)?;
        }

        // Spawn async task to wait for completion and move logs to DB
        let db_path = config().history_db_path();
        let stdout_path_clone = stdout_path.clone();
        let stderr_path_clone = stderr_path.clone();

        tokio::spawn(async move {
            wait_for_child_async(
                child,
                history_id,
                db_path,
                stdout_path_clone,
                stderr_path_clone,
            )
            .await;
        });

        Ok(Job {
            id: history_id,
            command: command.to_string(),
            args: args.to_vec(),
            started_at: OffsetDateTime::now_utc(),
            pid,
            status: InvocationStatus::Running,
            stdout_path,
            stderr_path,
        })
    }

    /// Get a job by ID (direct SQL lookup, O(1)).
    pub fn get(&self, id: i64) -> Result<Option<Job>> {
        let db = self
            .db
            .lock()
            .map_err(|_| anyhow::anyhow!("db lock poisoned"))?;
        let bg = db.get_background_job_by_id(id)?;
        Ok(bg.map(|b| Job::from_background_job(b, &self.jobs_dir)))
    }

    /// List all jobs.
    pub fn list(&self) -> Result<Vec<Job>> {
        let db = self
            .db
            .lock()
            .map_err(|_| anyhow::anyhow!("db lock poisoned"))?;
        let jobs = db.get_recent_background_jobs(1000)?;
        Ok(jobs
            .into_iter()
            .map(|bg| Job::from_background_job(bg, &self.jobs_dir))
            .collect())
    }

    /// List recent jobs (up to limit).
    pub fn list_recent(&self, limit: usize) -> Result<Vec<Job>> {
        let db = self
            .db
            .lock()
            .map_err(|_| anyhow::anyhow!("db lock poisoned"))?;
        let jobs = db.get_recent_background_jobs(limit)?;
        Ok(jobs
            .into_iter()
            .map(|bg| Job::from_background_job(bg, &self.jobs_dir))
            .collect())
    }

    /// List only active (running) jobs.
    pub fn list_active(&self) -> Result<Vec<Job>> {
        let db = self
            .db
            .lock()
            .map_err(|_| anyhow::anyhow!("db lock poisoned"))?;
        let jobs = db.get_active_background_jobs()?;
        Ok(jobs
            .into_iter()
            .map(|bg| Job::from_background_job(bg, &self.jobs_dir))
            .collect())
    }

    /// Cancel a running job.
    ///
    /// Sends SIGTERM to the process and updates the job status to Cancelled.
    /// If the process is in a systemd scope (old jobs), this may fail silently
    /// but the status will still be updated.
    pub fn cancel(&self, id: i64) -> Result<bool> {
        let job = match self.get(id)? {
            Some(j) => j,
            None => return Ok(false),
        };

        if matches!(job.status, InvocationStatus::Running) && job.pid > 0 {
            // Send SIGTERM - ignore errors since process may be in a systemd scope
            // or may have already exited
            let pid = nix::unistd::Pid::from_raw(job.pid as i32);
            match nix::sys::signal::kill(pid, nix::sys::signal::Signal::SIGTERM) {
                Ok(()) => {
                    // Successfully sent signal
                }
                Err(nix::errno::Errno::ESRCH) => {
                    // Process doesn't exist - it already exited
                }
                Err(nix::errno::Errno::EPERM) => {
                    // Permission denied - process may be in a different scope
                    // Try killing the process group instead
                    let _ = nix::sys::signal::killpg(pid, nix::sys::signal::Signal::SIGTERM);
                }
                Err(_) => {
                    // Other error - ignore, we'll mark as cancelled anyway
                }
            }

            // Update status in HistoryDb regardless of signal result
            let db = self
                .db
                .lock()
                .map_err(|_| anyhow::anyhow!("db lock poisoned"))?;
            db.finish_invocation(id, InvocationStatus::Cancelled, None, 0.0)?;

            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Wait for a job to complete.
    pub async fn wait(&self, id: i64, timeout: Option<Duration>) -> Result<Job> {
        let start = std::time::Instant::now();

        loop {
            let job = self
                .get(id)?
                .ok_or_else(|| anyhow::anyhow!("job {id} not found"))?;

            if job.is_terminal() {
                return Ok(job);
            }

            if let Some(timeout) = timeout {
                if start.elapsed() > timeout {
                    bail!("timeout waiting for job {id}");
                }
            }

            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    /// Clean up old completed jobs.
    pub fn prune(&self, older_than_days: u32) -> Result<usize> {
        // Prune from HistoryDb
        let count = {
            let db = self
                .db
                .lock()
                .map_err(|_| anyhow::anyhow!("db lock poisoned"))?;
            db.prune_old_jobs(older_than_days)?
        };

        // Collect valid job IDs (single DB query, lock released before fs ops)
        let valid_ids = {
            let db = self
                .db
                .lock()
                .map_err(|_| anyhow::anyhow!("db lock poisoned"))?;
            db.get_all_background_job_ids()?
        };

        // Clean orphan directories (no DB lock held)
        if let Ok(entries) = fs::read_dir(&self.jobs_dir) {
            for entry in entries.filter_map(std::result::Result::ok) {
                if let Ok(id) = entry.file_name().to_string_lossy().parse::<i64>() {
                    if !valid_ids.contains(&id) {
                        let _ = fs::remove_dir_all(entry.path());
                    }
                }
            }
        }

        Ok(count)
    }
}

/// Wait for a child process, update `HistoryDb`, and move logs to DB.
///
/// Enforces a 4-hour hard ceiling to prevent zombie jobs from accumulating
/// when spawned processes hang indefinitely.
async fn wait_for_child_async(
    mut child: tokio::process::Child,
    history_id: i64,
    db_path: PathBuf,
    stdout_path: PathBuf,
    stderr_path: PathBuf,
) {
    let start = std::time::Instant::now();
    let timeout = Duration::from_secs(4 * 3600); // 4-hour hard ceiling

    let result = match tokio::time::timeout(timeout, child.wait()).await {
        Ok(result) => result,
        Err(_) => {
            // Timed out — kill the child process
            let _ = child.kill().await;
            Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "job timed out after 4 hours",
            ))
        }
    };
    let duration = start.elapsed().as_secs_f64();

    if let Ok(db) = HistoryDb::open(&db_path) {
        let (status, exit_code) = match result {
            Ok(exit) if exit.success() => (InvocationStatus::Success, Some(0)),
            Ok(exit) => (InvocationStatus::Failed, exit.code()),
            Err(_) => (InvocationStatus::Failed, None),
        };
        // Store logs in DB and delete files
        let _ = db.finish_background_job(
            history_id,
            status,
            exit_code,
            duration,
            Some(&stdout_path),
            Some(&stderr_path),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_job_tail_stdout() {
        let dir = tempdir().unwrap();
        let stdout_path = dir.path().join("stdout.log");
        fs::write(&stdout_path, "line1\nline2\nline3\nline4\nline5").unwrap();

        let job = Job {
            id: 1,
            command: "test".into(),
            args: vec![],
            started_at: OffsetDateTime::now_utc(),
            pid: 0,
            status: InvocationStatus::Running,
            stdout_path: stdout_path.clone(),
            stderr_path: dir.path().join("stderr.log"),
        };

        let result = job.tail_stdout(3).unwrap();
        assert_eq!(result, "line3\nline4\nline5");
    }
}
