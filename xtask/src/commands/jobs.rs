//! Background job management commands
//!
//! Jobs are tracked in the history database (`SQLite`). Log files are stored in the filesystem.
//! `JobManager` is a thin wrapper - `HistoryDb` is the single source of truth.

use color_eyre::eyre::{Result, eyre};
use std::time::Duration;
use tabled::{builder::Builder, settings::Style};

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::config::config;
use crate::history::{InvocationProgress, JobLifecycleStatus, TestProgress};
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
        /// Show only running/active jobs
        #[arg(long)]
        active: bool,
    },
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
    fn name(&self) -> &'static str {
        "jobs"
    }

    async fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        let cfg = config();
        let job_manager = JobManager::new(cfg.jobs_dir())?;

        match &self.subcommand {
            JobsSubcommand::List { limit, active } => {
                if *active {
                    execute_active(&job_manager, ctx)
                } else {
                    execute_list(&job_manager, *limit, ctx)
                }
            }
            JobsSubcommand::Status { id, follow } => {
                execute_status(&job_manager, *id, *follow, ctx).await
            }
            JobsSubcommand::Output { id, stderr } => {
                execute_output(&job_manager, *id, *stderr, ctx)
            }
            JobsSubcommand::Wait { id, timeout } => {
                execute_wait(&job_manager, *id, *timeout, ctx).await
            }
            JobsSubcommand::Cancel { id } => execute_cancel(&job_manager, *id, ctx),
            JobsSubcommand::Prune { older_than } => execute_prune(&job_manager, *older_than, ctx),
        }
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::utility()
    }
}

fn execute_list(
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
            builder.push_record(["ID", "COMMAND", "STATUS", "PROGRESS", "PID", "STARTED"]);
            for job in &jobs {
                let status_str = status_to_str(job.job_status);
                builder.push_record([
                    job.id.to_string(),
                    truncate_str(&job.command, 16),
                    status_str.to_string(),
                    progress_brief(job.test_progress.as_ref()),
                    job.pid.to_string(),
                    super::format_display_time(&job.started_at),
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
                "invocation_id": j.invocation_id,
                "command": j.command,
                "args": j.args,
                "status": status_to_str(j.job_status),
                "pid": j.pid,
                "started_at": j.started_at.to_string(),
                "exit_code": j.exit_code,
                "progress": j.test_progress.as_ref().map(progress_to_json),
            })).collect::<Vec<_>>()
        }));
    }

    Ok(result)
}

fn execute_active(job_manager: &JobManager, ctx: &CommandContext) -> Result<CommandResult> {
    let active = job_manager.list_active()?;

    if ctx.is_human() {
        if active.is_empty() {
            println!("No active jobs.");
        } else {
            let mut builder = Builder::new();
            builder.push_record(["ID", "COMMAND", "PROGRESS", "PID", "RUNNING", "STARTED"]);
            for job in &active {
                let elapsed = time::OffsetDateTime::now_utc() - job.started_at;
                let running_time = format!("{:.0}s", elapsed.whole_seconds());
                builder.push_record([
                    job.id.to_string(),
                    truncate_str(&job.command, 16),
                    progress_brief(job.test_progress.as_ref()),
                    job.pid.to_string(),
                    running_time,
                    super::format_display_time(&job.started_at),
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
                "invocation_id": j.invocation_id,
                "command": j.command,
                "args": j.args,
                "status": status_to_str(j.job_status),
                "pid": j.pid,
                "started_at": j.started_at.to_string(),
                "exit_code": j.exit_code,
                "progress": j.test_progress.as_ref().map(progress_to_json),
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
        .ok_or_else(|| eyre!("job {id} not found"))?;

    if follow {
        // Follow mode: seek-based tailing (O(delta) per poll, not O(n))
        use std::io::{Read, Seek, SeekFrom};

        let mut last_pos = 0u64;
        loop {
            // Read only new content since last position
            if let Ok(mut file) = std::fs::File::open(&job.stdout_path) {
                let _ = file.seek(SeekFrom::Start(last_pos));
                let mut buf = String::new();
                if let Ok(n) = file.read_to_string(&mut buf)
                    && n > 0
                {
                    print!("{buf}");
                    last_pos += n as u64;
                }
            } else if job.is_terminal() {
                // File gone (archived to DB) — read remainder from DB
                if let Ok(stdout) = job.read_stdout()
                    && stdout.len() as u64 > last_pos
                {
                    print!("{}", &stdout[last_pos as usize..]);
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
                        if let Ok(n) = file.read_to_string(&mut buf)
                            && n > 0
                        {
                            print!("{buf}");
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
            println!("  Status:   {}", status_to_str(job.job_status));
            println!("  PID:      {}", job.pid);
            println!("  Started:  {}", job.started_at);
            if let Some(progress) = job.test_progress.as_ref() {
                println!("  Progress: {}", progress_brief(Some(progress)));
                if let Some(last) = &progress.last_test_name
                    && !last.is_empty()
                {
                    println!("  Last test: {last}");
                }
                if let Some(updated_at) = &progress.updated_at {
                    println!("  Updated:  {updated_at}");
                }
            }
            // Show last few lines of output
            if let Ok(tail) = job.tail_stdout(5)
                && !tail.is_empty()
            {
                println!("\n  Last output:\n{tail}");
            }
        }

        let mut result = CommandResult::success()
            .with_message(format!("Job {id} status"))
            .with_duration(ctx.elapsed());

        if !ctx.is_human() {
            // Stage/diagnostic queries target the invocation record, not the job handle.
            let live_stage = job
                .invocation_id
                .and_then(|iid| ctx.with_history_db(|db| db.get_live_stage(iid)).flatten());
            let stages: Vec<serde_json::Value> = job
                .invocation_id
                .and_then(|iid| ctx.with_history_db(|db| db.get_stage_timings_for_invocation(iid)))
                .unwrap_or_default()
                .iter()
                .map(|s| {
                    serde_json::json!({
                        "name": s.stage_name,
                        "duration_secs": s.duration_secs,
                        "success": s.success,
                    })
                })
                .collect();
            let inv_progress: Option<InvocationProgress> = job
                .invocation_id
                .and_then(|iid| ctx.with_history_db(|db| db.get_progress(iid)).flatten());
            result = result.with_data(serde_json::json!({
                "id": job.id,
                "invocation_id": job.invocation_id,
                "command": job.command,
                "args": job.args,
                "status": status_to_str(job.job_status),
                "phase": live_stage,
                "stages": stages,
                "pid": job.pid,
                "started_at": job.started_at.to_string(),
                "exit_code": job.exit_code,
                "progress": job.test_progress.as_ref().map(progress_to_json),
                "inv_progress": inv_progress.as_ref().map(|p| serde_json::json!({
                    "phase": p.phase,
                    "step": p.step,
                    "pct_done": p.pct_done,
                    "items_done": p.items_done,
                    "items_total": p.items_total,
                    "updated_at": p.updated_at,
                })),
            }));
        }

        Ok(result)
    }
}

fn execute_output(
    job_manager: &JobManager,
    id: i64,
    stderr: bool,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let job = job_manager
        .get(id)?
        .ok_or_else(|| eyre!("job {id} not found"))?;

    let output = if stderr {
        job.read_stderr()?
    } else {
        job.read_stdout()?
    };

    let stream_name = if stderr { "stderr" } else { "stdout" };

    if ctx.is_human() {
        println!("{output}");
        if job.is_terminal() {
            let elapsed = time::OffsetDateTime::now_utc() - job.started_at;
            let exit_str = job
                .exit_code
                .map_or_else(|| "?".to_string(), |c| c.to_string());
            eprintln!(
                "─── Job completed in {:.0}s (exit: {}) ───",
                elapsed.whole_seconds(),
                exit_str
            );
        }
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
        println!("Job {} completed: {}", id, status_to_str(job.job_status));
    }

    let job_failed = matches!(
        job.job_status,
        JobLifecycleStatus::Orphaned | JobLifecycleStatus::Killed
    ) || job.exit_code.is_some_and(|c| c != 0);

    let mut result = if job_failed {
        CommandResult::partial()
            .with_message(format!("Job {id} completed: {}", status_to_str(job.job_status)))
    } else {
        CommandResult::success().with_message(format!("Job {id} wait completed"))
    }
    .with_duration(ctx.elapsed());

    if !ctx.is_human() {
        result = result.with_data(serde_json::json!({
            "id": job.id,
            "invocation_id": job.invocation_id,
            "status": status_to_str(job.job_status),
            "exit_code": job.exit_code,
            "progress": job.test_progress.as_ref().map(progress_to_json),
        }));
    }

    Ok(result)
}

fn execute_cancel(
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
            suggestion: Some("List active jobs: xtask jobs list --active".to_string()),
        }))
    }
}

fn execute_prune(
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

/// Convert `JobLifecycleStatus` to display string.
fn status_to_str(status: JobLifecycleStatus) -> &'static str {
    match status {
        JobLifecycleStatus::Running => "running",
        JobLifecycleStatus::Completed => "completed",
        JobLifecycleStatus::Orphaned => "orphaned",
        JobLifecycleStatus::Killed => "killed",
    }
}

/// Truncate a string to max length
fn truncate_str(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}...", &s[..max_len - 3])
    }
}

fn progress_brief(progress: Option<&TestProgress>) -> String {
    let Some(progress) = progress else {
        return "-".to_string();
    };
    let completed = progress.completed;
    let failed = progress.failed;
    if let Some(total) = progress.total
        && total > 0
    {
        let pct = (completed as f64 / total as f64) * 100.0;
        return format!("{completed}/{total} ({pct:.1}%, fail {failed})");
    }
    format!("{completed} done (fail {failed})")
}

fn progress_to_json(progress: &TestProgress) -> serde_json::Value {
    let percent = progress.total.and_then(|total| {
        if total > 0 {
            Some((progress.completed as f64 / total as f64) * 100.0)
        } else {
            None
        }
    });
    serde_json::json!({
        "total": progress.total,
        "passed": progress.passed,
        "failed": progress.failed,
        "ignored": progress.ignored,
        "completed": progress.completed,
        "percent": percent,
        "last_test_name": progress.last_test_name,
        "updated_at": progress.updated_at,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::sinex_test;

    #[sinex_test]
    async fn test_command_name() -> ::xtask::sandbox::TestResult<()> {
        let cmd = JobsCommand {
            subcommand: JobsSubcommand::List {
                limit: 10,
                active: false,
            },
        };
        assert_eq!(cmd.name(), "jobs");
        Ok(())
    }

    #[sinex_test]
    async fn test_command_metadata() -> ::xtask::sandbox::TestResult<()> {
        let cmd = JobsCommand {
            subcommand: JobsSubcommand::Prune { older_than: 7 },
        };
        let metadata = cmd.metadata();
        assert!(!metadata.modifies_state);
        assert!(!metadata.track_in_history);
        Ok(())
    }

    #[sinex_test]
    async fn test_truncate_str() -> ::xtask::sandbox::TestResult<()> {
        assert_eq!(truncate_str("short", 10), "short");
        assert_eq!(truncate_str("verylongstring", 10), "verylon...");
        assert_eq!(truncate_str("exactly10!", 10), "exactly10!");
        Ok(())
    }

    #[sinex_test]
    async fn test_status_to_str() -> ::xtask::sandbox::TestResult<()> {
        assert_eq!(status_to_str(JobLifecycleStatus::Running), "running");
        assert_eq!(status_to_str(JobLifecycleStatus::Completed), "completed");
        assert_eq!(status_to_str(JobLifecycleStatus::Orphaned), "orphaned");
        assert_eq!(status_to_str(JobLifecycleStatus::Killed), "killed");
        Ok(())
    }
}
