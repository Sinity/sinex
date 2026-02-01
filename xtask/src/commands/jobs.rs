//! Background job management commands
//!
//! Jobs are tracked in the history database (SQLite) for unified tracking with
//! regular invocations. Log files are stored in the filesystem.

use anyhow::Result;
use std::fs;
use std::time::Duration;
use tabled::{builder::Builder, settings::Style};

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::config::config;
use crate::history::{HistoryDb, InvocationStatus};
use crate::jobs::{JobManager, JobStatus};

/// Jobs command configuration
#[derive(Debug, Clone, clap::Args)]
pub struct JobsCommand {
    #[command(subcommand)]
    pub subcommand: JobsSubcommand,
}

/// Jobs subcommands
#[derive(Debug, Clone, clap::Subcommand)]
pub enum JobsSubcommand {
    /// List recent jobs
    List {
        #[arg(long, default_value = "20")]
        limit: usize,
    },
    /// Show only running/active jobs
    Active,
    /// Show status of a specific job
    Status {
        #[arg(value_name = "JOB_ID")]
        id: i64,
        #[arg(short, long)]
        follow: bool,
    },
    /// Show full output of a job
    Output {
        #[arg(value_name = "JOB_ID")]
        id: i64,
        #[arg(long)]
        stderr: bool,
    },
    /// Wait for a job to complete
    Wait {
        #[arg(value_name = "JOB_ID")]
        id: i64,
        #[arg(short, long, default_value = "0")]
        timeout: u64,
    },
    /// Cancel a running job
    Cancel {
        #[arg(value_name = "JOB_ID")]
        id: i64,
    },
    /// Remove completed jobs older than N days
    Prune {
        #[arg(long, default_value = "7")]
        older_than: u32,
    },
}

impl XtaskCommand for JobsCommand {
    fn name(&self) -> &str {
        "jobs"
    }

    fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        let cfg = config();

        // Try to use history database first, fall back to JobManager
        let history_db = HistoryDb::open(&cfg.history_db_path()).ok();
        let job_manager = JobManager::new(cfg.jobs_dir())?;

        match &self.subcommand {
            JobsSubcommand::List { limit } => {
                execute_list(history_db.as_ref(), &job_manager, *limit, ctx)
            }
            JobsSubcommand::Active => execute_active(history_db.as_ref(), &job_manager, ctx),
            JobsSubcommand::Status { id, follow } => {
                execute_status(history_db.as_ref(), &job_manager, *id, *follow, ctx)
            }
            JobsSubcommand::Output { id, stderr } => {
                execute_output(history_db.as_ref(), &job_manager, *id, *stderr, ctx)
            }
            JobsSubcommand::Wait { id, timeout } => {
                execute_wait(history_db.as_ref(), &job_manager, *id, *timeout, ctx)
            }
            JobsSubcommand::Cancel { id } => {
                execute_cancel(history_db.as_ref(), &job_manager, *id, ctx)
            }
            JobsSubcommand::Prune { older_than } => {
                execute_prune(history_db.as_ref(), &job_manager, *older_than, ctx)
            }
        }
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::utility()
    }
}

fn execute_list(
    history_db: Option<&HistoryDb>,
    job_manager: &JobManager,
    limit: usize,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    // Try history database first
    if let Some(db) = history_db {
        if let Ok(jobs) = db.get_recent_background_jobs(limit) {
            if ctx.is_human() {
                if jobs.is_empty() {
                    println!("No jobs found in history database.");
                } else {
                    let mut builder = Builder::new();
                    builder.push_record(["ID", "COMMAND", "STATUS", "PID", "STARTED"]);
                    for job in &jobs {
                        let status_str = match job.status {
                            InvocationStatus::Running => "running",
                            InvocationStatus::Success => "completed",
                            InvocationStatus::Failed => "failed",
                            InvocationStatus::Cancelled => "cancelled",
                        };
                        builder.push_record([
                            job.id.to_string(),
                            truncate_str(&job.command, 16),
                            status_str.to_string(),
                            job.pid.to_string(),
                            format_time(&job.started_at),
                        ]);
                    }
                    let mut table = builder.build();
                    table.with(Style::rounded());
                    println!("{table}");
                }
            } else {
                let json = serde_json::to_string_pretty(&jobs)?;
                println!("{json}");
            }

            return Ok(CommandResult::success()
                .with_message(format!("Listed {} jobs (from history DB)", jobs.len()))
                .with_duration(ctx.elapsed()));
        }
    }

    // Fall back to JobManager
    let jobs = job_manager.list_recent(limit)?;

    if ctx.is_human() {
        if jobs.is_empty() {
            println!("No jobs found.");
        } else {
            let mut builder = Builder::new();
            builder.push_record(["ID", "COMMAND", "STATUS", "DURATION", "STARTED"]);
            for job in &jobs {
                let status_str = match &job.meta.status {
                    JobStatus::Running { .. } => "running",
                    JobStatus::Completed { .. } => "completed",
                    JobStatus::Failed { .. } => "failed",
                    JobStatus::Cancelled => "cancelled",
                };
                let duration = match &job.meta.status {
                    JobStatus::Completed { duration_secs, .. } => {
                        format!("{:.1}s", duration_secs)
                    }
                    _ => "-".into(),
                };
                builder.push_record([
                    job.meta.id.to_string(),
                    job.meta.command.clone(),
                    status_str.to_string(),
                    duration,
                    format_time(&job.meta.started_at),
                ]);
            }
            let mut table = builder.build();
            table.with(Style::rounded());
            println!("{table}");
        }
    } else {
        let json = serde_json::to_string_pretty(&jobs.iter().map(|j| &j.meta).collect::<Vec<_>>())?;
        println!("{json}");
    }

    Ok(CommandResult::success()
        .with_message(format!("Listed {} jobs", jobs.len()))
        .with_duration(ctx.elapsed()))
}

fn execute_active(
    history_db: Option<&HistoryDb>,
    job_manager: &JobManager,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    // Try history database first
    if let Some(db) = history_db {
        if let Ok(active) = db.get_active_background_jobs() {
            if ctx.is_human() {
                if active.is_empty() {
                    println!("No active jobs.");
                } else {
                    let mut builder = Builder::new();
                    builder.push_record(["ID", "COMMAND", "PID", "STARTED"]);
                    for job in &active {
                        builder.push_record([
                            job.id.to_string(),
                            truncate_str(&job.command, 16),
                            job.pid.to_string(),
                            format_time(&job.started_at),
                        ]);
                    }
                    let mut table = builder.build();
                    table.with(Style::rounded());
                    println!("{table}");
                }
            } else {
                let json = serde_json::to_string_pretty(&active)?;
                println!("{json}");
            }

            return Ok(CommandResult::success()
                .with_message(format!("{} active jobs", active.len()))
                .with_duration(ctx.elapsed()));
        }
    }

    // Fall back to JobManager
    let jobs = job_manager.list_recent(100)?;
    let active: Vec<_> = jobs
        .iter()
        .filter(|j| matches!(j.meta.status, JobStatus::Running { .. }))
        .collect();

    if ctx.is_human() {
        if active.is_empty() {
            println!("No active jobs.");
        } else {
            let mut builder = Builder::new();
            builder.push_record(["ID", "COMMAND", "RUNNING", "STARTED"]);
            for job in &active {
                let running_time = if matches!(job.meta.status, JobStatus::Running { .. }) {
                    let elapsed = time::OffsetDateTime::now_utc() - job.meta.started_at;
                    format!("{:.0}s", elapsed.whole_seconds())
                } else {
                    "-".into()
                };
                builder.push_record([
                    job.meta.id.to_string(),
                    job.meta.command.clone(),
                    running_time,
                    format_time(&job.meta.started_at),
                ]);
            }
            let mut table = builder.build();
            table.with(Style::rounded());
            println!("{table}");
        }
    } else {
        let json =
            serde_json::to_string_pretty(&active.iter().map(|j| &j.meta).collect::<Vec<_>>())?;
        println!("{json}");
    }

    Ok(CommandResult::success()
        .with_message(format!("{} active jobs", active.len()))
        .with_duration(ctx.elapsed()))
}

fn execute_status(
    _history_db: Option<&HistoryDb>,
    job_manager: &JobManager,
    id: i64,
    follow: bool,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    // For status with follow, we need the filesystem-based Job
    let job = job_manager
        .get(id as u64)?
        .ok_or_else(|| anyhow::anyhow!("job {} not found", id))?;

    if follow {
        // Follow mode: tail output until job completes
        let mut last_pos = 0u64;
        loop {
            // Print new output
            if let Ok(stdout) = job.read_stdout() {
                if stdout.len() as u64 > last_pos {
                    print!("{}", &stdout[last_pos as usize..]);
                    last_pos = stdout.len() as u64;
                }
            }

            // Reload and check status
            let job = job_manager.get(id as u64)?.unwrap();
            if job.meta.status.is_terminal() {
                break;
            }

            std::thread::sleep(Duration::from_millis(500));
        }

        Ok(CommandResult::success()
            .with_message(format!("Job {} completed", id))
            .with_duration(ctx.elapsed()))
    } else {
        if ctx.is_human() {
            println!("Job {}", id);
            println!(
                "  Command:  {} {}",
                job.meta.command,
                job.meta.args.join(" ")
            );
            println!("  Status:   {:?}", job.meta.status);
            println!("  Started:  {}", job.meta.started_at);
            if let Some(finished) = job.meta.finished_at {
                println!("  Finished: {}", finished);
            }
            // Show last few lines of output
            if let Ok(tail) = job.tail_stdout(5) {
                if !tail.is_empty() {
                    println!("\n  Last output:\n{}", tail);
                }
            }
        } else {
            let json = serde_json::to_string_pretty(&job.meta)?;
            println!("{json}");
        }

        Ok(CommandResult::success()
            .with_message(format!("Job {} status", id))
            .with_duration(ctx.elapsed()))
    }
}

fn execute_output(
    history_db: Option<&HistoryDb>,
    job_manager: &JobManager,
    id: i64,
    stderr: bool,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    // Try to get log path from history database
    if let Some(db) = history_db {
        if let Ok(jobs) = db.get_recent_background_jobs(100) {
            if let Some(job) = jobs.iter().find(|j| j.id == id) {
                let path = if stderr {
                    job.stderr_path.as_ref()
                } else {
                    job.stdout_path.as_ref()
                };

                if let Some(path) = path {
                    if let Ok(content) = fs::read_to_string(path) {
                        println!("{content}");
                        return Ok(CommandResult::success()
                            .with_message(format!(
                                "Job {} {} output",
                                id,
                                if stderr { "stderr" } else { "stdout" }
                            ))
                            .with_duration(ctx.elapsed()));
                    }
                }
            }
        }
    }

    // Fall back to JobManager
    let job = job_manager
        .get(id as u64)?
        .ok_or_else(|| anyhow::anyhow!("job {} not found", id))?;

    let output = if stderr {
        job.read_stderr()?
    } else {
        job.read_stdout()?
    };

    println!("{output}");

    Ok(CommandResult::success()
        .with_message(format!(
            "Job {} {} output",
            id,
            if stderr { "stderr" } else { "stdout" }
        ))
        .with_duration(ctx.elapsed()))
}

fn execute_wait(
    _history_db: Option<&HistoryDb>,
    job_manager: &JobManager,
    id: i64,
    timeout_secs: u64,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let timeout = if timeout_secs > 0 {
        Some(Duration::from_secs(timeout_secs))
    } else {
        None
    };

    let job = job_manager.wait(id as u64, timeout)?;

    if ctx.is_human() {
        println!("Job {} completed: {:?}", id, job.meta.status);
    } else {
        let json = serde_json::to_string_pretty(&job.meta)?;
        println!("{json}");
    }

    Ok(CommandResult::success()
        .with_message(format!("Job {} wait completed", id))
        .with_duration(ctx.elapsed()))
}

fn execute_cancel(
    history_db: Option<&HistoryDb>,
    job_manager: &JobManager,
    id: i64,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    // Try to get PID from history database
    if let Some(db) = history_db {
        if let Ok(jobs) = db.get_active_background_jobs() {
            if let Some(job) = jobs.iter().find(|j| j.id == id) {
                // Send SIGTERM
                unsafe {
                    libc::kill(job.pid as i32, libc::SIGTERM);
                }

                // Update status in database
                let _ = db.finish_invocation(id, InvocationStatus::Cancelled, None, 0.0);

                if ctx.is_human() {
                    println!("Job {} cancelled (via history DB)", id);
                }
                return Ok(CommandResult::success()
                    .with_message(format!("Job {} cancelled", id))
                    .with_duration(ctx.elapsed()));
            }
        }
    }

    // Fall back to JobManager
    if job_manager.cancel(id as u64)? {
        if ctx.is_human() {
            println!("Job {} cancelled", id);
        }
        Ok(CommandResult::success()
            .with_message(format!("Job {} cancelled", id))
            .with_duration(ctx.elapsed()))
    } else {
        if ctx.is_human() {
            println!("Job {} not found or not running", id);
        }
        Ok(CommandResult::failure(crate::output::StructuredError {
            code: "JOB_NOT_FOUND".to_string(),
            message: format!("Job {} not found or not running", id),
            location: None,
            suggestion: Some("Use 'cargo xtask jobs list' to see available jobs".to_string()),
        }))
    }
}

fn execute_prune(
    history_db: Option<&HistoryDb>,
    job_manager: &JobManager,
    older_than: u32,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let mut count = 0;

    // Prune from history database
    if let Some(db) = history_db {
        // TODO: Add prune_background_jobs method to HistoryDb
        // For now, just prune from the general invocations table
        if let Ok(pruned) = db.prune(older_than) {
            count += pruned;
        }
    }

    // Also prune from JobManager filesystem
    let fs_pruned = job_manager.prune(older_than)?;
    count += fs_pruned;

    if ctx.is_human() {
        println!("Pruned {} jobs older than {} days", count, older_than);
    }

    Ok(CommandResult::success()
        .with_message(format!("Pruned {} jobs", count))
        .with_detail(format!("older than {} days", older_than))
        .with_duration(ctx.elapsed()))
}

/// Format a time for display
fn format_time(time: &time::OffsetDateTime) -> String {
    time.format(&time::format_description::parse("[year]-[month]-[day] [hour]:[minute]").unwrap())
        .unwrap_or_else(|_| "-".into())
}

/// Truncate a string to max length
fn truncate_str(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}...", &s[..max_len - 3])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_command_name() {
        let cmd = JobsCommand {
            subcommand: JobsSubcommand::List { limit: 10 },
        };
        assert_eq!(cmd.name(), "jobs");
    }

    #[test]
    fn test_command_metadata() {
        let cmd = JobsCommand {
            subcommand: JobsSubcommand::Prune { older_than: 7 },
        };
        let metadata = cmd.metadata();
        assert!(!metadata.modifies_state);
        assert!(!metadata.track_in_history); // Utility commands don't track history
    }

    #[test]
    fn test_clone() {
        let cmd = JobsCommand {
            subcommand: JobsSubcommand::Cancel { id: 123 },
        };
        let cloned = cmd.subcommand.clone();
        if let JobsSubcommand::Cancel { id } = cloned {
            assert_eq!(id, 123);
        } else {
            panic!("Expected Cancel subcommand");
        }
    }

    #[test]
    fn test_subcommand_variants() {
        // Test that all subcommand variants can be constructed
        let _ = JobsSubcommand::List { limit: 5 };
        let _ = JobsSubcommand::Status {
            id: 1,
            follow: false,
        };
        let _ = JobsSubcommand::Output {
            id: 2,
            stderr: true,
        };
        let _ = JobsSubcommand::Wait { id: 3, timeout: 30 };
        let _ = JobsSubcommand::Cancel { id: 4 };
        let _ = JobsSubcommand::Prune { older_than: 14 };
    }

    #[test]
    fn test_metadata_category() {
        let cmd = JobsCommand {
            subcommand: JobsSubcommand::List { limit: 10 },
        };
        let metadata = cmd.metadata();
        assert_eq!(metadata.category, Some("utility".to_string()));
        assert!(metadata.timeout.is_none()); // Utility commands have no timeout
    }

    #[test]
    fn test_truncate_str() {
        assert_eq!(truncate_str("short", 10), "short");
        assert_eq!(truncate_str("verylongstring", 10), "verylon...");
        assert_eq!(truncate_str("exactly10!", 10), "exactly10!");
    }
}
