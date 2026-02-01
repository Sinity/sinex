//! Background job execution and tracking.
//!
//! Jobs are stored in `$SINEX_STATE_DIR/jobs/<id>/` with:
//! - `meta.json` - Job metadata (command, args, pid, started_at, status)
//! - `stdout.log` - Captured stdout
//! - `stderr.log` - Captured stderr
//! - `result.json` - Final result (when complete)

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use time::OffsetDateTime;

/// Counter for generating unique job IDs within a session.
#[allow(dead_code)]
static JOB_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Status of a background job.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "status", rename_all = "lowercase")]
pub enum JobStatus {
    Running { pid: u32 },
    Completed { exit_code: i32, duration_secs: f64 },
    Failed { exit_code: i32, error: String },
    Cancelled,
}

impl JobStatus {
    pub fn is_terminal(&self) -> bool {
        !matches!(self, JobStatus::Running { .. })
    }
}

/// Metadata for a background job.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobMeta {
    pub id: u64,
    pub command: String,
    pub args: Vec<String>,
    pub started_at: OffsetDateTime,
    pub finished_at: Option<OffsetDateTime>,
    pub status: JobStatus,
    pub cwd: String,
}

/// A handle to a job's directory.
pub struct Job {
    pub meta: JobMeta,
    pub dir: PathBuf,
}

impl Job {
    /// Path to the stdout log file.
    pub fn stdout_path(&self) -> PathBuf {
        self.dir.join("stdout.log")
    }

    /// Path to the stderr log file.
    pub fn stderr_path(&self) -> PathBuf {
        self.dir.join("stderr.log")
    }

    /// Path to the meta file.
    pub fn meta_path(&self) -> PathBuf {
        self.dir.join("meta.json")
    }

    /// Read the last N lines of stdout.
    pub fn tail_stdout(&self, lines: usize) -> Result<String> {
        tail_file(&self.stdout_path(), lines)
    }

    /// Read the last N lines of stderr.
    #[allow(dead_code)]
    pub fn tail_stderr(&self, lines: usize) -> Result<String> {
        tail_file(&self.stderr_path(), lines)
    }

    /// Read all stdout.
    pub fn read_stdout(&self) -> Result<String> {
        fs::read_to_string(self.stdout_path()).context("failed to read stdout")
    }

    /// Read all stderr.
    pub fn read_stderr(&self) -> Result<String> {
        fs::read_to_string(self.stderr_path()).context("failed to read stderr")
    }

    /// Update the job metadata.
    pub fn update_meta(&self) -> Result<()> {
        let json = serde_json::to_string_pretty(&self.meta)?;
        fs::write(self.meta_path(), json)?;
        Ok(())
    }

    /// Reload the job metadata from disk.
    #[allow(dead_code)]
    pub fn reload(&mut self) -> Result<()> {
        let json = fs::read_to_string(self.meta_path())?;
        self.meta = serde_json::from_str(&json)?;
        Ok(())
    }

    /// Check if the job process is still running.
    #[allow(dead_code)]
    pub fn is_alive(&self) -> bool {
        if let JobStatus::Running { pid } = self.meta.status {
            // Check if process exists
            Path::new(&format!("/proc/{}", pid)).exists()
        } else {
            false
        }
    }
}

/// Manager for background jobs.
pub struct JobManager {
    jobs_dir: PathBuf,
}

impl JobManager {
    /// Create a new job manager with the given jobs directory.
    pub fn new(jobs_dir: PathBuf) -> Result<Self> {
        fs::create_dir_all(&jobs_dir).context("failed to create jobs directory")?;
        Ok(Self { jobs_dir })
    }

    /// Get the path to a job's directory.
    fn job_dir(&self, id: u64) -> PathBuf {
        self.jobs_dir.join(id.to_string())
    }

    /// Spawn an xtask command in background.
    ///
    /// Re-invokes cargo xtask with the provided subcommand and args,
    /// but with --fg to ensure it runs in foreground (within the job).
    pub fn spawn_xtask(&self, subcommand: &str, args: &[String]) -> Result<Job> {
        let mut full_args = vec![
            "xtask".to_string(),
            "--fg".to_string(), // Force foreground since we're in a job
            subcommand.to_string(),
        ];
        full_args.extend(args.iter().cloned());
        self.spawn("cargo", &full_args)
    }

    /// Spawn a cargo command as a background job.
    ///
    /// This is a convenience wrapper that sets up the working directory
    /// and handles cargo-specific environment.
    pub fn spawn_cargo(&self, args: &[String]) -> Result<Job> {
        self.spawn("cargo", args)
    }

    /// Start a new background job.
    pub fn spawn(&self, command: &str, args: &[String]) -> Result<Job> {
        // Generate unique job ID
        let id = generate_job_id();
        let job_dir = self.job_dir(id);
        fs::create_dir_all(&job_dir)?;

        // Create output files
        let stdout_file = File::create(job_dir.join("stdout.log"))?;
        let stderr_file = File::create(job_dir.join("stderr.log"))?;

        // Spawn the process
        let child = Command::new(command)
            .args(args)
            .stdout(Stdio::from(stdout_file))
            .stderr(Stdio::from(stderr_file))
            .spawn()
            .with_context(|| format!("failed to spawn: {} {:?}", command, args))?;

        let meta = JobMeta {
            id,
            command: command.to_string(),
            args: args.to_vec(),
            started_at: OffsetDateTime::now_utc(),
            finished_at: None,
            status: JobStatus::Running { pid: child.id() },
            cwd: std::env::current_dir()
                .map(|p| p.display().to_string())
                .unwrap_or_default(),
        };

        let job = Job { meta, dir: job_dir };
        job.update_meta()?;

        // Spawn a background thread to wait for the process and update status
        let job_dir_clone = job.dir.clone();
        std::thread::spawn(move || {
            let _ = wait_for_child(child, &job_dir_clone);
        });

        Ok(job)
    }

    /// Get a job by ID.
    pub fn get(&self, id: u64) -> Result<Option<Job>> {
        let job_dir = self.job_dir(id);
        if !job_dir.exists() {
            return Ok(None);
        }

        let meta_path = job_dir.join("meta.json");
        let json = fs::read_to_string(&meta_path).context("failed to read job meta")?;
        let meta: JobMeta = serde_json::from_str(&json)?;

        Ok(Some(Job { meta, dir: job_dir }))
    }

    /// List all jobs.
    pub fn list(&self) -> Result<Vec<Job>> {
        let mut jobs = Vec::new();

        if !self.jobs_dir.exists() {
            return Ok(jobs);
        }

        for entry in fs::read_dir(&self.jobs_dir)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                if let Ok(id) = entry.file_name().to_string_lossy().parse::<u64>() {
                    if let Ok(Some(job)) = self.get(id) {
                        jobs.push(job);
                    }
                }
            }
        }

        // Sort by started_at descending
        jobs.sort_by_key(|j| std::cmp::Reverse(j.meta.started_at));

        Ok(jobs)
    }

    /// List recent jobs (up to limit).
    pub fn list_recent(&self, limit: usize) -> Result<Vec<Job>> {
        let jobs = self.list()?;
        Ok(jobs.into_iter().take(limit).collect())
    }

    /// List only active (running) jobs.
    pub fn list_active(&self) -> Result<Vec<Job>> {
        let jobs = self.list()?;
        Ok(jobs
            .into_iter()
            .filter(|j| matches!(j.meta.status, JobStatus::Running { .. }))
            .collect())
    }

    /// Cancel a running job.
    pub fn cancel(&self, id: u64) -> Result<bool> {
        let job = match self.get(id)? {
            Some(j) => j,
            None => return Ok(false),
        };

        if let JobStatus::Running { pid } = job.meta.status {
            // Send SIGTERM
            unsafe {
                libc::kill(pid as i32, libc::SIGTERM);
            }

            // Update status
            let mut job = job;
            job.meta.status = JobStatus::Cancelled;
            job.meta.finished_at = Some(OffsetDateTime::now_utc());
            job.update_meta()?;

            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Wait for a job to complete.
    pub fn wait(&self, id: u64, timeout: Option<Duration>) -> Result<Job> {
        let start = std::time::Instant::now();

        loop {
            let job = self
                .get(id)?
                .ok_or_else(|| anyhow::anyhow!("job {} not found", id))?;

            if job.meta.status.is_terminal() {
                return Ok(job);
            }

            if let Some(timeout) = timeout {
                if start.elapsed() > timeout {
                    bail!("timeout waiting for job {}", id);
                }
            }

            std::thread::sleep(Duration::from_millis(500));
        }
    }

    /// Clean up old completed jobs.
    pub fn prune(&self, older_than_days: u32) -> Result<usize> {
        let cutoff = OffsetDateTime::now_utc() - time::Duration::days(older_than_days as i64);
        let mut removed = 0;

        for job in self.list()? {
            if job.meta.status.is_terminal() {
                if let Some(finished) = job.meta.finished_at {
                    if finished < cutoff {
                        fs::remove_dir_all(&job.dir)?;
                        removed += 1;
                    }
                }
            }
        }

        Ok(removed)
    }
}

/// Generate a unique job ID based on timestamp + counter.
#[allow(dead_code)]
fn generate_job_id() -> u64 {
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let counter = JOB_COUNTER.fetch_add(1, Ordering::SeqCst);

    // Combine timestamp (lower 48 bits) + counter (upper 16 bits)
    (timestamp & 0xFFFFFFFFFFFF) | ((counter & 0xFFFF) << 48)
}

/// Wait for a child process and update job status.
#[allow(dead_code)]
fn wait_for_child(mut child: Child, job_dir: &Path) -> Result<()> {
    let start = std::time::Instant::now();
    let result = child.wait()?;
    let duration = start.elapsed().as_secs_f64();

    // Read current meta
    let meta_path = job_dir.join("meta.json");
    let json = fs::read_to_string(&meta_path)?;
    let mut meta: JobMeta = serde_json::from_str(&json)?;

    // Update status
    meta.finished_at = Some(OffsetDateTime::now_utc());
    meta.status = if result.success() {
        JobStatus::Completed {
            exit_code: 0,
            duration_secs: duration,
        }
    } else {
        let code = result.code().unwrap_or(-1);
        JobStatus::Failed {
            exit_code: code,
            error: format!("exit code {}", code),
        }
    };

    // Write updated meta
    let json = serde_json::to_string_pretty(&meta)?;
    fs::write(&meta_path, json)?;

    Ok(())
}

/// Read the last N lines from a file.
fn tail_file(path: &Path, lines: usize) -> Result<String> {
    let file = File::open(path).context("failed to open file")?;
    let reader = BufReader::new(file);

    let all_lines: Vec<_> = reader.lines().map_while(Result::ok).collect();
    let start = all_lines.len().saturating_sub(lines);

    Ok(all_lines[start..].join("\n"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_job_manager_basic() {
        let dir = tempdir().unwrap();
        let manager = JobManager::new(dir.path().join("jobs")).unwrap();

        // Spawn a simple job
        let job = manager.spawn("echo", &["hello".to_string()]).unwrap();

        assert!(job.meta.id > 0);

        // Wait for it to complete
        let job = manager
            .wait(job.meta.id, Some(Duration::from_secs(5)))
            .unwrap();
        assert!(job.meta.status.is_terminal());

        // Check stdout
        let stdout = job.read_stdout().unwrap();
        assert!(stdout.contains("hello"));
    }

    #[test]
    fn test_job_list() {
        let dir = tempdir().unwrap();
        let manager = JobManager::new(dir.path().join("jobs")).unwrap();

        // Spawn a few jobs
        manager.spawn("true", &[]).unwrap();
        manager.spawn("true", &[]).unwrap();

        std::thread::sleep(Duration::from_millis(100));

        let jobs = manager.list().unwrap();
        assert_eq!(jobs.len(), 2);
    }
}
