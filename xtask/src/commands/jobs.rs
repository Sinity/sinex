//! Background job management commands

use anyhow::Result;
use std::time::Duration;

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::config::config;
use crate::jobs::{JobManager, JobStatus};

/// Jobs command configuration
pub struct JobsCommand {
    pub subcommand: JobsSubcommand,
}

/// Jobs subcommands
#[derive(Debug, Clone)]
pub enum JobsSubcommand {
    /// List recent jobs
    List { limit: usize },
    /// Show status of a specific job
    Status { id: u64, follow: bool },
    /// Show full output of a job
    Output { id: u64, stderr: bool },
    /// Wait for a job to complete
    Wait { id: u64, timeout: u64 },
    /// Cancel a running job
    Cancel { id: u64 },
    /// Remove completed jobs older than N days
    Prune { older_than: u32 },
}

impl XtaskCommand for JobsCommand {
    fn name(&self) -> &str {
        "jobs"
    }

    fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        let cfg = config();
        let manager = JobManager::new(cfg.jobs_dir())?;

        match &self.subcommand {
            JobsSubcommand::List { limit } => execute_list(&manager, *limit, ctx),
            JobsSubcommand::Status { id, follow } => execute_status(&manager, *id, *follow, ctx),
            JobsSubcommand::Output { id, stderr } => execute_output(&manager, *id, *stderr, ctx),
            JobsSubcommand::Wait { id, timeout } => execute_wait(&manager, *id, *timeout, ctx),
            JobsSubcommand::Cancel { id } => execute_cancel(&manager, *id, ctx),
            JobsSubcommand::Prune { older_than } => execute_prune(&manager, *older_than, ctx),
        }
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::utility() // Job management is a utility command
    }
}

fn execute_list(manager: &JobManager, limit: usize, ctx: &CommandContext) -> Result<CommandResult> {
    let jobs = manager.list_recent(limit)?;

    if ctx.is_human() {
        if jobs.is_empty() {
            println!("No jobs found.");
        } else {
            println!(
                "{:<16} {:<12} {:<10} {:>8}  {}",
                "ID", "COMMAND", "STATUS", "DURATION", "STARTED"
            );
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
                println!(
                    "{:<16} {:<12} {:<10} {:>8}  {}",
                    job.meta.id,
                    job.meta.command,
                    status_str,
                    duration,
                    job.meta.started_at.format("%Y-%m-%d %H:%M")
                );
            }
        }
    } else {
        let json = serde_json::to_string_pretty(&jobs.iter().map(|j| &j.meta).collect::<Vec<_>>())?;
        println!("{json}");
    }

    Ok(CommandResult::success()
        .with_message(format!("Listed {} jobs", jobs.len()))
        .with_duration(ctx.elapsed()))
}

fn execute_status(
    manager: &JobManager,
    id: u64,
    follow: bool,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let job = manager
        .get(id)?
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
            let job = manager.get(id)?.unwrap();
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
    manager: &JobManager,
    id: u64,
    stderr: bool,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let job = manager
        .get(id)?
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
    manager: &JobManager,
    id: u64,
    timeout_secs: u64,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let timeout = if timeout_secs > 0 {
        Some(Duration::from_secs(timeout_secs))
    } else {
        None
    };

    let job = manager.wait(id, timeout)?;

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

fn execute_cancel(manager: &JobManager, id: u64, ctx: &CommandContext) -> Result<CommandResult> {
    if manager.cancel(id)? {
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
    manager: &JobManager,
    older_than: u32,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let count = manager.prune(older_than)?;

    if ctx.is_human() {
        println!("Pruned {} jobs older than {} days", count, older_than);
    }

    Ok(CommandResult::success()
        .with_message(format!("Pruned {} jobs", count))
        .with_detail(format!("older than {} days", older_than))
        .with_duration(ctx.elapsed()))
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
        assert_eq!(metadata.modifies_state, false);
        assert_eq!(metadata.track_in_history, false); // Utility commands don't track history
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
}
