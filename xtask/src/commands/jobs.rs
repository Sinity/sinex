//! Background job management commands
//!
//! Jobs are tracked in the history database (`SQLite`). Log files are stored in the filesystem.
//! `JobManager` is a thin wrapper - `HistoryDb` is the single source of truth.

use anyhow::Result;
use std::time::Duration;
use tabled::{builder::Builder, settings::Style};

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::config::config;
use crate::history::InvocationStatus;
use crate::jobs::JobManager;

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

#[async_trait::async_trait]
impl XtaskCommand for JobsCommand {
    fn name(&self) -> &'static str {
        "jobs"
    }

    async fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        let cfg = config();
        let job_manager = JobManager::new(cfg.jobs_dir())?;

        match &self.subcommand {
            JobsSubcommand::List { limit } => execute_list(&job_manager, *limit, ctx).await,
            JobsSubcommand::Active => execute_active(&job_manager, ctx).await,
            JobsSubcommand::Status { id, follow } => {
                execute_status(&job_manager, *id, *follow, ctx).await
            }
            JobsSubcommand::Output { id, stderr } => {
                execute_output(&job_manager, *id, *stderr, ctx).await
            }
            JobsSubcommand::Wait { id, timeout } => {
                execute_wait(&job_manager, *id, *timeout, ctx).await
            }
            JobsSubcommand::Cancel { id } => execute_cancel(&job_manager, *id, ctx).await,
            JobsSubcommand::Prune { older_than } => {
                execute_prune(&job_manager, *older_than, ctx).await
            }
        }
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::utility()
    }
}

async fn execute_list(
    job_manager: &JobManager,
    limit: usize,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let jobs = job_manager.list_recent(limit)?;

    if ctx.is_human() {
        if jobs.is_empty() {
            println!("No jobs found.");
        } else {
            let mut builder = Builder::new();
            builder.push_record(["ID", "COMMAND", "STATUS", "PID", "STARTED"]);
            for job in &jobs {
                let status_str = status_to_str(job.status);
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
    }

    let mut result = CommandResult::success()
        .with_message(format!("Listed {} jobs", jobs.len()))
        .with_duration(ctx.elapsed());

    if !ctx.is_human() {
        result = result.with_data(serde_json::json!({
            "filter": "recent",
            "jobs": jobs.iter().map(|j| serde_json::json!({
                "id": j.id,
                "command": j.command,
                "args": j.args,
                "status": status_to_str(j.status),
                "pid": j.pid,
                "started_at": j.started_at.to_string(),
                "exit_code": j.exit_code,
            })).collect::<Vec<_>>()
        }));
    }

    Ok(result)
}

async fn execute_active(job_manager: &JobManager, ctx: &CommandContext) -> Result<CommandResult> {
    let active = job_manager.list_active()?;

    if ctx.is_human() {
        if active.is_empty() {
            println!("No active jobs.");
        } else {
            let mut builder = Builder::new();
            builder.push_record(["ID", "COMMAND", "PID", "RUNNING", "STARTED"]);
            for job in &active {
                let elapsed = time::OffsetDateTime::now_utc() - job.started_at;
                let running_time = format!("{:.0}s", elapsed.whole_seconds());
                builder.push_record([
                    job.id.to_string(),
                    truncate_str(&job.command, 16),
                    job.pid.to_string(),
                    running_time,
                    format_time(&job.started_at),
                ]);
            }
            let mut table = builder.build();
            table.with(Style::rounded());
            println!("{table}");
        }
    }

    let mut result = CommandResult::success()
        .with_message(format!("{} active jobs", active.len()))
        .with_duration(ctx.elapsed());

    if !ctx.is_human() {
        result = result.with_data(serde_json::json!({
            "filter": "active",
            "jobs": active.iter().map(|j| serde_json::json!({
                "id": j.id,
                "command": j.command,
                "args": j.args,
                "status": status_to_str(j.status),
                "pid": j.pid,
                "started_at": j.started_at.to_string(),
                "exit_code": j.exit_code,
            })).collect::<Vec<_>>()
        }));
    }

    Ok(result)
}

async fn execute_status(
    job_manager: &JobManager,
    id: i64,
    follow: bool,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let job = job_manager
        .get(id)?
        .ok_or_else(|| anyhow::anyhow!("job {id} not found"))?;

    if follow {
        // Follow mode: seek-based tailing (O(delta) per poll, not O(n))
        use std::io::{Read, Seek, SeekFrom};

        let mut last_pos = 0u64;
        loop {
            // Read only new content since last position
            if let Ok(mut file) = std::fs::File::open(&job.stdout_path) {
                let _ = file.seek(SeekFrom::Start(last_pos));
                let mut buf = String::new();
                if let Ok(n) = file.read_to_string(&mut buf) {
                    if n > 0 {
                        print!("{buf}");
                        last_pos += n as u64;
                    }
                }
            } else if job.is_terminal() {
                // File gone (archived to DB) — read remainder from DB
                if let Ok(stdout) = job.read_stdout() {
                    if stdout.len() as u64 > last_pos {
                        print!("{}", &stdout[last_pos as usize..]);
                    }
                }
                break;
            }

            // Reload and check status
            let updated = job_manager.get(id)?;
            match updated {
                Some(j) if j.is_terminal() => {
                    // One more read to catch final output before file is archived
                    if let Ok(mut file) = std::fs::File::open(&job.stdout_path) {
                        let _ = file.seek(SeekFrom::Start(last_pos));
                        let mut buf = String::new();
                        if let Ok(n) = file.read_to_string(&mut buf) {
                            if n > 0 {
                                print!("{buf}");
                            }
                        }
                    }
                    break;
                }
                None => break,
                _ => {}
            }

            tokio::time::sleep(Duration::from_millis(250)).await;
        }

        Ok(CommandResult::success()
            .with_message(format!("Job {id} completed"))
            .with_duration(ctx.elapsed()))
    } else {
        if ctx.is_human() {
            println!("Job {id}");
            println!("  Command:  {} {}", job.command, job.args.join(" "));
            println!("  Status:   {}", status_to_str(job.status));
            println!("  PID:      {}", job.pid);
            println!("  Started:  {}", job.started_at);
            // Show last few lines of output
            if let Ok(tail) = job.tail_stdout(5) {
                if !tail.is_empty() {
                    println!("\n  Last output:\n{tail}");
                }
            }
        }

        let mut result = CommandResult::success()
            .with_message(format!("Job {id} status"))
            .with_duration(ctx.elapsed());

        if !ctx.is_human() {
            result = result.with_data(serde_json::json!({
                "id": job.id,
                "command": job.command,
                "args": job.args,
                "status": status_to_str(job.status),
                "pid": job.pid,
                "started_at": job.started_at.to_string(),
                "exit_code": job.exit_code,
            }));
        }

        Ok(result)
    }
}

async fn execute_output(
    job_manager: &JobManager,
    id: i64,
    stderr: bool,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let job = job_manager
        .get(id)?
        .ok_or_else(|| anyhow::anyhow!("job {id} not found"))?;

    let output = if stderr {
        job.read_stderr()?
    } else {
        job.read_stdout()?
    };

    let stream_name = if stderr { "stderr" } else { "stdout" };

    if ctx.is_human() {
        println!("{output}");
    }

    let mut result = CommandResult::success()
        .with_message(format!("Job {id} {stream_name} output"))
        .with_duration(ctx.elapsed());

    if !ctx.is_human() {
        result = result.with_data(serde_json::json!({
            "id": id,
            "stream": stream_name,
            "content": output,
        }));
    }

    Ok(result)
}

async fn execute_wait(
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

    let job = job_manager.wait(id, timeout).await?;

    if ctx.is_human() {
        println!("Job {} completed: {}", id, status_to_str(job.status));
    }

    let mut result = CommandResult::success()
        .with_message(format!("Job {id} wait completed"))
        .with_duration(ctx.elapsed());

    if !ctx.is_human() {
        result = result.with_data(serde_json::json!({
            "id": job.id,
            "status": status_to_str(job.status),
            "exit_code": job.exit_code,
        }));
    }

    Ok(result)
}

async fn execute_cancel(
    job_manager: &JobManager,
    id: i64,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    if job_manager.cancel(id)? {
        if ctx.is_human() {
            println!("Job {id} cancelled");
        }
        Ok(CommandResult::success()
            .with_message(format!("Job {id} cancelled"))
            .with_duration(ctx.elapsed()))
    } else {
        if ctx.is_human() {
            println!("Job {id} not found or not running");
        }
        Ok(CommandResult::failure(crate::output::StructuredError {
            code: "JOB_NOT_FOUND".to_string(),
            message: format!("Job {id} not found or not running"),
            location: Some("jobs::cancel".to_string()),
            suggestion: Some("List active jobs: cargo xtask jobs active".to_string()),
        }))
    }
}

async fn execute_prune(
    job_manager: &JobManager,
    older_than: u32,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let count = job_manager.prune(older_than)?;

    if ctx.is_human() {
        println!("Pruned {count} jobs older than {older_than} days");
    }

    Ok(CommandResult::success()
        .with_message(format!("Pruned {count} jobs"))
        .with_detail(format!("older than {older_than} days"))
        .with_duration(ctx.elapsed()))
}

/// Convert `InvocationStatus` to display string.
fn status_to_str(status: InvocationStatus) -> &'static str {
    match status {
        InvocationStatus::Running => "running",
        InvocationStatus::Success => "completed",
        InvocationStatus::Failed => "failed",
        InvocationStatus::Cancelled => "cancelled",
    }
}

/// Format a time for display
fn format_time(time: &time::OffsetDateTime) -> String {
    use std::sync::LazyLock as Lazy;
    static TIME_FORMAT: Lazy<Vec<time::format_description::BorrowedFormatItem<'static>>> =
        Lazy::new(|| {
            time::format_description::parse("[year]-[month]-[day] [hour]:[minute]")
                .expect("static format string is valid")
        });
    time.format(&*TIME_FORMAT).unwrap_or_else(|_| "-".into())
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
        assert!(!metadata.track_in_history);
    }

    #[test]
    fn test_truncate_str() {
        assert_eq!(truncate_str("short", 10), "short");
        assert_eq!(truncate_str("verylongstring", 10), "verylon...");
        assert_eq!(truncate_str("exactly10!", 10), "exactly10!");
    }

    #[test]
    fn test_status_to_str() {
        assert_eq!(status_to_str(InvocationStatus::Running), "running");
        assert_eq!(status_to_str(InvocationStatus::Success), "completed");
        assert_eq!(status_to_str(InvocationStatus::Failed), "failed");
        assert_eq!(status_to_str(InvocationStatus::Cancelled), "cancelled");
    }
}
