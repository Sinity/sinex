//! Background job management commands
//!
//! Jobs are tracked in the history database (`SQLite`). Log files are stored in the filesystem.
//! `JobManager` is a thin wrapper - `HistoryDb` is the single source of truth.

use color_eyre::eyre::{Result, WrapErr, eyre};
use std::path::Path;
use std::time::Duration;
use tabled::{builder::Builder, settings::Style};

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::config::config;
use crate::history::{InvocationProgress, JobLifecycleStatus, StageTiming};
use crate::jobs::{JobManager, JobQueryManager};

/// Inspect and manage background xtask jobs.
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
        match &self.subcommand {
            JobsSubcommand::List { limit, active } => {
                let job_query = JobQueryManager::new(cfg.jobs_dir())?;
                if *active {
                    execute_active(&job_query, ctx)
                } else {
                    execute_list(&job_query, *limit, ctx)
                }
            }
            JobsSubcommand::Status { id, follow } => {
                let job_query = JobQueryManager::new(cfg.jobs_dir())?;
                execute_status(&job_query, *id, *follow, ctx).await
            }
            JobsSubcommand::Output { id, stderr } => {
                let job_query = JobQueryManager::new(cfg.jobs_dir())?;
                execute_output(&job_query, *id, *stderr, ctx)
            }
            JobsSubcommand::Wait { id, timeout } => {
                let job_query = JobQueryManager::new(cfg.jobs_dir())?;
                execute_wait(&job_query, *id, *timeout, ctx).await
            }
            JobsSubcommand::Cancel { id } => {
                let job_manager = JobManager::new(cfg.jobs_dir())?;
                execute_cancel(&job_manager, *id, ctx)
            }
            JobsSubcommand::Prune { older_than } => {
                let job_manager = JobManager::new(cfg.jobs_dir())?;
                execute_prune(&job_manager, *older_than, ctx)
            }
        }
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::utility()
    }
}

fn format_job_pid(pid: Option<u32>) -> String {
    pid.map(|pid| pid.to_string())
        .unwrap_or_else(|| "<unavailable>".to_string())
}

fn execute_list(
    job_query: &JobQueryManager,
    limit: usize,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let jobs = job_query.list_recent(limit)?;
    let mut progress_issues = Vec::new();

    if ctx.is_human() {
        if jobs.is_empty() {
            println!("No jobs found.");
        } else {
            let mut builder = Builder::new();
            builder.push_record(["ID", "COMMAND", "STATUS", "PROGRESS", "PID", "STARTED"]);
            for job in &jobs {
                let status_str = status_to_str(job.job_status);
                let progress = load_invocation_progress(ctx, job.invocation_id);
                if let Some(issue) = &progress.issue {
                    progress_issues.push(format!("job {}: {issue}", job.id));
                }
                builder.push_record([
                    job.id.to_string(),
                    truncate_str(&job.command, 16),
                    status_str.to_string(),
                    progress_brief(progress.progress.as_ref()),
                    format_job_pid(job.pid),
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
    for issue in &progress_issues {
        result = result.with_warning(issue.clone());
    }

    if !ctx.is_human() {
        result = result.with_data(serde_json::json!({
            "filter": "recent",
            "jobs": jobs.iter().map(|j| {
                let progress = load_invocation_progress(ctx, j.invocation_id);
                serde_json::json!({
                    "id": j.id,
                    "invocation_id": j.invocation_id,
                    "command": j.command,
                    "args": j.args,
                    "status": status_to_str(j.job_status),
                    "pid": j.pid,
                    "started_at": j.started_at.to_string(),
                    "exit_code": j.exit_code,
                    "progress": progress.progress.as_ref().map(progress_to_json),
                    "progress_issue": progress.issue,
                })
            }).collect::<Vec<_>>()
        }));
    }

    Ok(result)
}

fn execute_active(job_query: &JobQueryManager, ctx: &CommandContext) -> Result<CommandResult> {
    let active = job_query.list_active()?;
    let mut progress_issues = Vec::new();

    if ctx.is_human() {
        if active.is_empty() {
            println!("No active jobs.");
        } else {
            let mut builder = Builder::new();
            builder.push_record(["ID", "COMMAND", "PROGRESS", "PID", "RUNNING", "STARTED"]);
            for job in &active {
                let elapsed = time::OffsetDateTime::now_utc() - job.started_at;
                let running_time = format!("{:.0}s", elapsed.whole_seconds());
                let progress = load_invocation_progress(ctx, job.invocation_id);
                if let Some(issue) = &progress.issue {
                    progress_issues.push(format!("job {}: {issue}", job.id));
                }
                builder.push_record([
                    job.id.to_string(),
                    truncate_str(&job.command, 16),
                    progress_brief(progress.progress.as_ref()),
                    format_job_pid(job.pid),
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
    for issue in &progress_issues {
        result = result.with_warning(issue.clone());
    }

    if !ctx.is_human() {
        result = result.with_data(serde_json::json!({
            "filter": "active",
            "jobs": active.iter().map(|j| {
                let progress = load_invocation_progress(ctx, j.invocation_id);
                serde_json::json!({
                    "id": j.id,
                    "invocation_id": j.invocation_id,
                    "command": j.command,
                    "args": j.args,
                    "status": status_to_str(j.job_status),
                    "pid": j.pid,
                    "started_at": j.started_at.to_string(),
                    "exit_code": j.exit_code,
                    "progress": progress.progress.as_ref().map(progress_to_json),
                    "progress_issue": progress.issue,
                })
            }).collect::<Vec<_>>()
        }));
    }

    Ok(result)
}

async fn execute_status(
    job_query: &JobQueryManager,
    id: i64,
    follow: bool,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let job = job_query
        .get(id)?
        .ok_or_else(|| eyre!("job {id} not found"))?;

    if follow {
        let mut last_pos = 0u64;
        loop {
            // Read only new content since last position
            if let Some((buf, new_pos)) = read_stdout_delta_from_file(&job.stdout_path, last_pos)? {
                if !buf.is_empty() {
                    print!("{buf}");
                    last_pos = new_pos;
                }
            } else if job.is_terminal() {
                // File gone (archived to DB) — read remainder from DB
                let stdout = job.read_stdout()?;
                if stdout.len() as u64 > last_pos {
                    print!("{}", &stdout[last_pos as usize..]);
                }
                break;
            }

            // Reload and check status
            let updated = job_query.get(id)?;
            match updated {
                Some(j) if j.is_terminal() => {
                    // One more read to catch final output before file is archived
                    if let Some((buf, _new_pos)) =
                        read_stdout_delta_from_file(&job.stdout_path, last_pos)?
                    {
                        if !buf.is_empty() {
                            print!("{buf}");
                        }
                    } else {
                        let stdout = job.read_stdout()?;
                        if stdout.len() as u64 > last_pos {
                            print!("{}", &stdout[last_pos as usize..]);
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
        let stdout_tail = job.tail_stdout(5);

        if ctx.is_human() {
            println!("Job {id}");
            println!("  Command:  {} {}", job.command, job.args.join(" "));
            println!("  Status:   {}", status_to_str(job.job_status));
            println!("  PID:      {}", format_job_pid(job.pid));
            println!("  Started:  {}", job.started_at);
            let progress = load_invocation_progress(ctx, job.invocation_id);
            if let Some(ref p) = progress.progress {
                println!("  Progress: {}", progress_brief(Some(p)));
                if let Some(step) = &p.step
                    && !step.is_empty()
                {
                    println!("  Last step: {step}");
                }
                println!("  Updated:  {}", &p.updated_at);
            } else if let Some(issue) = &progress.issue {
                println!("  Progress: <unavailable>");
                println!("  Progress read failed: {issue}");
            }
            // Show last few lines of output
            match &stdout_tail {
                Ok(tail) if !tail.is_empty() => {
                    println!("\n  Last output:\n{tail}");
                }
                Ok(_) => {}
                Err(error) => {
                    println!("\n  Last output: <unavailable>");
                    println!("  Output read failed: {error:#}");
                }
            }
        }

        let mut result = CommandResult::success()
            .with_message(format!("Job {id} status"))
            .with_duration(ctx.elapsed());
        if let Err(error) = &stdout_tail {
            result = result.with_warning(format!(
                "failed to read recent stdout for job {id}: {error:#}"
            ));
        }
        let progress = load_invocation_progress(ctx, job.invocation_id);
        if let Some(issue) = &progress.issue {
            result = result.with_warning(issue.clone());
        }

        if !ctx.is_human() {
            // Stage/diagnostic queries target the invocation record, not the job handle.
            let stages = load_stage_timings(ctx, job.invocation_id);
            if let Some(issue) = &stages.issue {
                result = result.with_warning(issue.clone());
            }
            let stages_json: Vec<serde_json::Value> = stages
                .stages
                .iter()
                .map(|s| {
                    serde_json::json!({
                        "name": s.stage_name,
                        "duration_secs": s.duration_secs,
                        "success": s.success,
                    })
                })
                .collect();
            // Phase is available via progress.phase — not emitted separately at top level.
            result = result.with_data(serde_json::json!({
                "id": job.id,
                "invocation_id": job.invocation_id,
                "command": job.command,
                "args": job.args,
                "status": status_to_str(job.job_status),
                "stages": stages_json,
                "stages_issue": stages.issue,
                "pid": job.pid,
                "started_at": job.started_at.to_string(),
                "exit_code": job.exit_code,
                "progress": progress.progress.as_ref().map(progress_to_json),
                "progress_issue": progress.issue,
            }));
        }

        Ok(result)
    }
}

fn read_stdout_delta_from_file(path: &Path, last_pos: u64) -> Result<Option<(String, u64)>> {
    use std::io::{Read, Seek, SeekFrom};

    let mut file = match std::fs::File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("failed to open job stdout log {}", path.display()));
        }
    };

    file.seek(SeekFrom::Start(last_pos))
        .with_context(|| format!("failed to seek job stdout log {}", path.display()))?;

    let mut buf = String::new();
    let bytes_read = file
        .read_to_string(&mut buf)
        .with_context(|| format!("failed to read job stdout log {}", path.display()))?;

    Ok(Some((buf, last_pos + bytes_read as u64)))
}

fn execute_output(
    job_query: &JobQueryManager,
    id: i64,
    stderr: bool,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let job = job_query
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
    job_query: &JobQueryManager,
    id: i64,
    timeout_secs: u64,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let timeout = if timeout_secs > 0 {
        Some(Duration::from_secs(timeout_secs))
    } else {
        None
    };

    let job = job_query.wait(id, timeout).await?;

    if ctx.is_human() {
        println!("Job {} completed: {}", id, status_to_str(job.job_status));
    }

    let job_failed = matches!(
        job.job_status,
        JobLifecycleStatus::Failed | JobLifecycleStatus::Orphaned | JobLifecycleStatus::Killed
    ) || job.exit_code.is_some_and(|c| c != 0);

    let mut result = if job_failed {
        CommandResult::partial().with_message(format!(
            "Job {id} completed: {}",
            status_to_str(job.job_status)
        ))
    } else {
        CommandResult::success().with_message(format!("Job {id} wait completed"))
    }
    .with_duration(ctx.elapsed());

    let progress = load_invocation_progress(ctx, job.invocation_id);
    if let Some(issue) = &progress.issue {
        result = result.with_warning(issue.clone());
    }

    if !ctx.is_human() {
        result = result.with_data(serde_json::json!({
            "id": job.id,
            "invocation_id": job.invocation_id,
            "status": status_to_str(job.job_status),
            "exit_code": job.exit_code,
            "progress": progress.progress.as_ref().map(progress_to_json),
            "progress_issue": progress.issue,
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
        JobLifecycleStatus::Failed => "failed",
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

fn progress_brief(progress: Option<&InvocationProgress>) -> String {
    let Some(progress) = progress else {
        return "-".to_string();
    };
    if let Some(pct) = progress.pct_done {
        if let (Some(done), Some(total)) = (progress.items_done, progress.items_total) {
            return format!("{done}/{total} ({pct:.1}%)");
        }
        return format!("{pct:.1}%");
    }
    if let Some(phase) = &progress.phase {
        return phase.clone();
    }
    "-".to_string()
}

fn progress_to_json(progress: &InvocationProgress) -> serde_json::Value {
    serde_json::json!({
        "phase": progress.phase,
        "step": progress.step,
        "pct_done": progress.pct_done,
        "items_done": progress.items_done,
        "items_total": progress.items_total,
        "updated_at": progress.updated_at,
        "mode": progress.mode,
        "unit_kind": progress.unit_kind,
        "rate_per_sec": progress.rate_per_sec,
        "eta_confidence": progress.eta_confidence,
        "terminal_summary": progress.terminal_summary,
    })
}

#[derive(Debug)]
struct ProgressProbe {
    progress: Option<InvocationProgress>,
    issue: Option<String>,
}

#[derive(Debug)]
struct StageTimingsProbe {
    stages: Vec<StageTiming>,
    issue: Option<String>,
}

fn load_invocation_progress(ctx: &CommandContext, invocation_id: Option<i64>) -> ProgressProbe {
    match invocation_id {
        Some(iid) => progress_probe_from_result(
            iid,
            ctx.try_with_history_db_query(|db| db.get_progress(iid)),
        ),
        None => ProgressProbe {
            progress: None,
            issue: None,
        },
    }
}

fn progress_probe_from_result(
    invocation_id: i64,
    result: Option<Result<Option<InvocationProgress>>>,
) -> ProgressProbe {
    match result {
        Some(Ok(progress)) => ProgressProbe {
            progress,
            issue: None,
        },
        Some(Err(error)) => ProgressProbe {
            progress: None,
            issue: Some(format!(
                "failed to load progress for invocation {invocation_id}: {error:#}"
            )),
        },
        None => ProgressProbe {
            progress: None,
            issue: Some(format!(
                "history DB unavailable while loading progress for invocation {invocation_id}"
            )),
        },
    }
}

fn load_stage_timings(ctx: &CommandContext, invocation_id: Option<i64>) -> StageTimingsProbe {
    match invocation_id {
        Some(iid) => stage_timings_probe_from_result(
            iid,
            ctx.try_with_history_db_query(|db| db.get_stage_timings_for_invocation(iid)),
        ),
        None => StageTimingsProbe {
            stages: Vec::new(),
            issue: None,
        },
    }
}

fn stage_timings_probe_from_result(
    invocation_id: i64,
    result: Option<Result<Vec<StageTiming>>>,
) -> StageTimingsProbe {
    match result {
        Some(Ok(stages)) => StageTimingsProbe {
            stages,
            issue: None,
        },
        Some(Err(error)) => StageTimingsProbe {
            stages: Vec::new(),
            issue: Some(format!(
                "failed to load stage timings for invocation {invocation_id}: {error:#}"
            )),
        },
        None => StageTimingsProbe {
            stages: Vec::new(),
            issue: Some(format!(
                "history DB unavailable while loading stage timings for invocation {invocation_id}"
            )),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::sinex_test;
    use tempfile::tempdir;

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
        assert_eq!(status_to_str(JobLifecycleStatus::Failed), "failed");
        assert_eq!(status_to_str(JobLifecycleStatus::Orphaned), "orphaned");
        assert_eq!(status_to_str(JobLifecycleStatus::Killed), "killed");
        Ok(())
    }

    #[sinex_test]
    async fn test_read_stdout_delta_from_file_reports_io_failures()
    -> ::xtask::sandbox::TestResult<()> {
        let dir = tempdir()?;
        let error = read_stdout_delta_from_file(dir.path(), 0).unwrap_err();
        let message = format!("{error:#}");
        assert!(message.contains(dir.path().to_string_lossy().as_ref()));
        assert!(
            message.contains("failed to open job stdout log")
                || message.contains("failed to seek job stdout log")
                || message.contains("failed to read job stdout log")
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_read_stdout_delta_from_file_reads_new_bytes_only()
    -> ::xtask::sandbox::TestResult<()> {
        let dir = tempdir()?;
        let path = dir.path().join("stdout.log");
        std::fs::write(&path, "first\nsecond\n")?;

        let first = read_stdout_delta_from_file(&path, 0)?;
        assert_eq!(first, Some(("first\nsecond\n".to_string(), 13)));

        let second = read_stdout_delta_from_file(&path, 13)?;
        assert_eq!(second, Some((String::new(), 13)));
        Ok(())
    }

    #[sinex_test]
    async fn test_progress_probe_from_result_reports_history_errors()
    -> ::xtask::sandbox::TestResult<()> {
        let probe = progress_probe_from_result(42, Some(Err(eyre!("boom"))));
        assert!(probe.progress.is_none());
        assert!(probe.issue.unwrap_or_default().contains("boom"));
        Ok(())
    }

    #[sinex_test]
    async fn test_progress_probe_from_result_reports_unavailable_history()
    -> ::xtask::sandbox::TestResult<()> {
        let probe = progress_probe_from_result(42, None);
        assert!(probe.progress.is_none());
        assert!(
            probe
                .issue
                .unwrap_or_default()
                .contains("history DB unavailable")
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_stage_timings_probe_from_result_reports_history_errors()
    -> ::xtask::sandbox::TestResult<()> {
        let probe = stage_timings_probe_from_result(42, Some(Err(eyre!("stages boom"))));
        assert!(probe.stages.is_empty());
        assert!(probe.issue.unwrap_or_default().contains("stages boom"));
        Ok(())
    }
}
