//! History command - query build/test execution history

use anyhow::Result;
use tabled::{builder::Builder, settings::Style};

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::config::config;
use crate::history::HistoryDb;

/// History command variants
#[derive(Debug, Clone, clap::Subcommand)]
pub enum HistorySubcommand {
    /// List recent invocations
    List {
        #[arg(long, default_value = "20")]
        limit: usize,
        #[arg(long)]
        command: Option<String>,
    },
    /// Show the last invocation for a command
    Last {
        #[arg(long)]
        command: String,
    },
    /// Show statistics for a command
    Stats {
        #[arg(long)]
        command: String,
        #[arg(long, default_value = "30")]
        days: u32,
    },
    /// Prune old history entries
    Prune {
        #[arg(long, default_value = "90")]
        older_than: u32,
    },
    /// Export history as JSON
    Export {
        #[arg(long)]
        limit: usize,
    },
    /// Query test result history
    Tests {
        #[command(subcommand)]
        tests_cmd: HistoryTestsSubcommand,
    },
    /// Query build diagnostics (warnings/errors)
    Diagnostics {
        /// Maximum number of diagnostics to show
        #[arg(long, default_value = "20")]
        limit: usize,
        /// Filter by level (error, warning)
        #[arg(long)]
        level: Option<String>,
        /// Filter by file path pattern
        #[arg(long)]
        file: Option<String>,
    },
}

/// History tests subcommand variants
#[derive(Debug, Clone, clap::Subcommand)]
pub enum HistoryTestsSubcommand {
    Slowest {
        #[arg(long, default_value = "10")]
        limit: usize,
    },
    Flaky {
        #[arg(long, default_value = "10")]
        limit: usize,
    },
    GettingSlower {
        #[arg(long, default_value = "20.0")]
        threshold_pct: f64,
        #[arg(long, default_value = "10")]
        window: usize,
        #[arg(long, default_value = "10")]
        limit: usize,
    },
    Trends {
        #[arg(long)]
        pattern: Option<String>,
        #[arg(long)]
        package: Option<String>,
        #[arg(long, default_value = "30")]
        runs: usize,
    },
    Eta,
}

/// History management command
#[derive(Debug, Clone, clap::Args)]
pub struct HistoryCommand {
    #[command(subcommand)]
    pub subcommand: HistorySubcommand,
}

#[async_trait::async_trait]
impl XtaskCommand for HistoryCommand {
    fn name(&self) -> &'static str {
        "history"
    }

    async fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        let db = open_history_db()?;

        match &self.subcommand {
            HistorySubcommand::List { limit, command } => {
                execute_list(&db, *limit, command.as_deref(), ctx)
            }
            HistorySubcommand::Last { command } => execute_last(&db, command, ctx),
            HistorySubcommand::Stats { command, days } => execute_stats(&db, command, *days, ctx),
            HistorySubcommand::Prune { older_than } => execute_prune(&db, *older_than, ctx),
            HistorySubcommand::Export { limit } => execute_export(&db, *limit, ctx),
            HistorySubcommand::Tests { tests_cmd } => execute_tests(tests_cmd, &db, ctx),
            HistorySubcommand::Diagnostics { limit, level, file } => {
                execute_diagnostics(&db, *limit, level.as_deref(), file.as_deref(), ctx)
            }
        }
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::diagnostics()
    }
}

/// Open the history database
fn open_history_db() -> Result<HistoryDb> {
    let cfg = config();
    HistoryDb::open(&cfg.history_db_path())
}

fn execute_list(
    db: &HistoryDb,
    limit: usize,
    command: Option<&str>,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let invocations = db.get_recent(limit, command)?;

    if ctx.is_human() {
        if invocations.is_empty() {
            println!("No history entries found.");
        } else {
            println!(
                "{:<6} {:<12} {:<10} {:<10} {:>8}  STARTED",
                "ID", "COMMAND", "PROFILE", "STATUS", "DURATION"
            );
            for inv in &invocations {
                let profile = inv.profile.as_deref().unwrap_or("-");
                let duration = inv
                    .duration_secs
                    .map_or_else(|| "-".into(), |d| format!("{d:.1}s"));
                let status = format!("{:?}", inv.status).to_lowercase();
                println!(
                    "{:<6} {:<12} {:<10} {:<10} {:>8}  {}",
                    inv.id,
                    inv.command,
                    profile,
                    status,
                    duration,
                    inv.started_at
                        .format(
                            &time::format_description::parse(
                                "[year]-[month]-[day] [hour]:[minute]"
                            )
                            .unwrap()
                        )
                        .unwrap_or_else(|_| "-".into())
                );
            }
        }
    } else {
        let json = serde_json::to_string_pretty(&invocations)?;
        println!("{json}");
    }

    Ok(CommandResult::success()
        .with_message(format!("Found {} history entries", invocations.len()))
        .with_duration(ctx.elapsed()))
}

fn execute_last(db: &HistoryDb, command: &str, ctx: &CommandContext) -> Result<CommandResult> {
    let inv = db.get_last(command)?;

    if ctx.is_human() {
        match &inv {
            Some(inv) => {
                println!("Last {command} invocation:");
                println!("  ID:       {}", inv.id);
                println!("  Status:   {:?}", inv.status);
                println!("  Started:  {}", inv.started_at);
                if let Some(d) = inv.duration_secs {
                    println!("  Duration: {d:.2}s");
                }
                if let Some(c) = &inv.git_commit {
                    println!(
                        "  Commit:   {}{}",
                        c,
                        if inv.git_dirty { " (dirty)" } else { "" }
                    );
                }
            }
            None => println!("No history for command: {command}"),
        }
    } else {
        let json = serde_json::to_string_pretty(&inv)?;
        println!("{json}");
    }

    let message = if inv.is_some() {
        format!("Last invocation for '{command}'")
    } else {
        format!("No history for command '{command}'")
    };

    Ok(CommandResult::success()
        .with_message(message)
        .with_duration(ctx.elapsed()))
}

fn execute_stats(
    db: &HistoryDb,
    command: &str,
    days: u32,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let stats = db.get_stats(command, days)?;

    if ctx.is_human() {
        println!("Statistics for '{command}' (last {days} days):");
        println!("  Total:     {}", stats.total);
        println!("  Successes: {}", stats.successes);
        println!("  Failures:  {}", stats.failures);
        if let Some(avg) = stats.avg_duration_secs {
            println!("  Avg time:  {avg:.2}s");
        }
        if stats.total > 0 {
            let rate = (stats.successes as f64 / stats.total as f64) * 100.0;
            println!("  Success:   {rate:.1}%");
        }
    } else {
        let json = serde_json::to_string_pretty(&stats)?;
        println!("{json}");
    }

    Ok(CommandResult::success()
        .with_message(format!("Statistics for '{command}' over {days} days"))
        .with_duration(ctx.elapsed()))
}

fn execute_prune(db: &HistoryDb, older_than: u32, ctx: &CommandContext) -> Result<CommandResult> {
    let count = db.prune(older_than)?;

    if ctx.is_human() {
        println!("Pruned {count} entries older than {older_than} days");
    } else {
        println!(r#"{{"pruned": {count}, "older_than_days": {older_than}}}"#);
    }

    Ok(CommandResult::success()
        .with_message(format!("Pruned {count} old entries"))
        .with_duration(ctx.elapsed()))
}

fn execute_export(db: &HistoryDb, limit: usize, ctx: &CommandContext) -> Result<CommandResult> {
    let invocations = db.get_recent(limit, None)?;
    let json = serde_json::to_string_pretty(&invocations)?;
    println!("{json}");

    Ok(CommandResult::success()
        .with_message(format!("Exported {} entries", invocations.len()))
        .with_duration(ctx.elapsed()))
}

fn execute_tests(
    tests_cmd: &HistoryTestsSubcommand,
    db: &HistoryDb,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    match tests_cmd {
        HistoryTestsSubcommand::Slowest { limit } => execute_tests_slowest(db, *limit, ctx),
        HistoryTestsSubcommand::Flaky { limit } => execute_tests_flaky(db, *limit, ctx),
        HistoryTestsSubcommand::GettingSlower {
            threshold_pct,
            window,
            limit,
        } => execute_tests_getting_slower(db, *threshold_pct, *window, *limit, ctx),
        HistoryTestsSubcommand::Trends {
            pattern,
            package,
            runs,
        } => execute_tests_trends(db, pattern.as_deref(), package.as_deref(), *runs, ctx),
        HistoryTestsSubcommand::Eta => execute_tests_eta(db, ctx),
    }
}

fn execute_tests_slowest(
    db: &HistoryDb,
    limit: usize,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let tests = db.get_slowest_tests(limit)?;

    if ctx.is_human() {
        if tests.is_empty() {
            println!("No test timing data found.");
        } else {
            println!(
                "{:<50} {:<20} {:>10} {:>6}",
                "TEST", "PACKAGE", "AVG (s)", "RUNS"
            );
            for (name, package, avg, runs) in &tests {
                let display_name = if name.len() > 48 {
                    format!("...{}", &name[name.len() - 45..])
                } else {
                    name.clone()
                };
                println!("{display_name:<50} {package:<20} {avg:>10.3} {runs:>6}");
            }
        }
    } else {
        let json = serde_json::to_string_pretty(&tests)?;
        println!("{json}");
    }

    Ok(CommandResult::success()
        .with_message(format!("Found {} slowest tests", tests.len()))
        .with_duration(ctx.elapsed()))
}

fn execute_tests_flaky(
    db: &HistoryDb,
    limit: usize,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let tests = db.get_flaky_tests(limit)?;

    if ctx.is_human() {
        if tests.is_empty() {
            println!("No flaky tests found.");
        } else {
            let mut builder = Builder::new();
            builder.push_record(["TEST", "PACKAGE", "INVOCATION"]);
            for (name, package, inv_id) in &tests {
                let display_name = if name.len() > 48 {
                    format!("...{}", &name[name.len() - 45..])
                } else {
                    name.clone()
                };
                builder.push_record([display_name, package.clone(), inv_id.to_string()]);
            }
            let mut table = builder.build();
            table.with(Style::rounded());
            println!("{table}");
        }
    } else {
        let json = serde_json::to_string_pretty(&tests)?;
        println!("{json}");
    }

    Ok(CommandResult::success()
        .with_message(format!("Found {} flaky tests", tests.len()))
        .with_duration(ctx.elapsed()))
}

fn execute_tests_getting_slower(
    db: &HistoryDb,
    threshold_pct: f64,
    window: usize,
    limit: usize,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let tests = db.get_tests_getting_slower(window, threshold_pct, limit)?;

    if ctx.is_human() {
        if tests.is_empty() {
            println!("No tests found slowing >{threshold_pct}% over {window} runs.");
        } else {
            let mut builder = Builder::new();
            builder.push_record(["TEST", "PACKAGE", "OLD (s)", "NEW (s)", "CHANGE"]);
            for test in &tests {
                let display_name = if test.test_name.len() > 43 {
                    format!("...{}", &test.test_name[test.test_name.len() - 40..])
                } else {
                    test.test_name.clone()
                };
                builder.push_record([
                    display_name,
                    test.package.clone(),
                    format!("{:.3}", test.older_avg_secs),
                    format!("{:.3}", test.recent_avg_secs),
                    format!("{:+.1}%", test.pct_change),
                ]);
            }
            let mut table = builder.build();
            table.with(Style::rounded());
            println!("{table}");
        }
    } else {
        let json = serde_json::to_string_pretty(&tests)?;
        println!("{json}");
    }

    Ok(CommandResult::success()
        .with_message(format!("Found {} tests getting slower", tests.len()))
        .with_duration(ctx.elapsed()))
}

fn execute_tests_trends(
    db: &HistoryDb,
    pattern: Option<&str>,
    package: Option<&str>,
    runs: usize,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let tests = db.get_test_trends(pattern, package, runs)?;

    if ctx.is_human() {
        if tests.is_empty() {
            println!("No matching tests found.");
        } else {
            for test in &tests {
                println!(
                    "{}::{} (avg: {:.3}s)",
                    test.package, test.test_name, test.avg_duration_secs
                );
                for (i, duration) in test.durations.iter().enumerate() {
                    let timestamp = test.timestamps.get(i).map_or("-", |s| s.as_str());
                    println!("  {timestamp}: {duration:.3}s");
                }
                println!();
            }
        }
    } else {
        let json = serde_json::to_string_pretty(&tests)?;
        println!("{json}");
    }

    Ok(CommandResult::success()
        .with_message(format!("Found {} test trends", tests.len()))
        .with_duration(ctx.elapsed()))
}

fn execute_diagnostics(
    db: &HistoryDb,
    limit: usize,
    level: Option<&str>,
    file_pattern: Option<&str>,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let diagnostics = db.get_recent_diagnostics_filtered(limit, level, file_pattern)?;

    if ctx.is_human() {
        if diagnostics.is_empty() {
            println!("No diagnostics found.");
        } else {
            let mut builder = Builder::new();
            builder.push_record(["LEVEL", "CODE", "FILE", "MESSAGE"]);
            for diag in &diagnostics {
                let code = diag.code.as_deref().unwrap_or("-");
                let file_loc = match (&diag.file_path, diag.line) {
                    (Some(path), Some(line)) => {
                        let short_path = if path.len() > 45 {
                            format!("...{}", &path[path.len() - 42..])
                        } else {
                            path.clone()
                        };
                        format!("{short_path}:{line}")
                    }
                    (Some(path), None) => {
                        if path.len() > 48 {
                            format!("...{}", &path[path.len() - 45..])
                        } else {
                            path.clone()
                        }
                    }
                    _ => "-".to_string(),
                };
                let message = if diag.message.len() > 60 {
                    format!("{}...", &diag.message[..57])
                } else {
                    diag.message.clone()
                };
                builder.push_record([diag.level.clone(), code.to_string(), file_loc, message]);
            }
            let mut table = builder.build();
            table.with(Style::rounded());
            println!("{table}");
        }
    } else {
        let json = serde_json::to_string_pretty(&diagnostics)?;
        println!("{json}");
    }

    Ok(CommandResult::success()
        .with_message(format!("Found {} diagnostics", diagnostics.len()))
        .with_duration(ctx.elapsed()))
}

fn execute_tests_eta(db: &HistoryDb, ctx: &CommandContext) -> Result<CommandResult> {
    let estimate = db.estimate_runtime()?;

    if ctx.is_human() {
        if estimate.test_count == 0 {
            println!("No test history available for estimation.");
        } else {
            println!(
                "Estimated runtime: {:.0}s ({} tests, {} confidence)",
                estimate.estimated_secs, estimate.test_count, estimate.confidence
            );
            if !estimate.breakdown.is_empty() && estimate.breakdown.len() <= 10 {
                println!("\nBreakdown by package:");
                for (pkg, secs) in &estimate.breakdown {
                    println!("  {pkg:<30} {secs:>6.1}s");
                }
            }
        }
    } else {
        let json = serde_json::to_string_pretty(&estimate)?;
        println!("{json}");
    }

    Ok(CommandResult::success()
        .with_message(format!(
            "Estimated runtime: {:.0}s",
            estimate.estimated_secs
        ))
        .with_duration(ctx.elapsed()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_history_command_metadata() {
        let cmd = HistoryCommand {
            subcommand: HistorySubcommand::List {
                limit: 10,
                command: None,
            },
        };

        let metadata = cmd.metadata();
        assert_eq!(metadata.category, Some("diagnostics".to_string()));
        assert!(metadata.timeout.is_some());
        assert!(!metadata.modifies_state); // History commands are read-only
    }

    #[test]
    fn test_history_command_name() {
        let cmd = HistoryCommand {
            subcommand: HistorySubcommand::Stats {
                command: "test".to_string(),
                days: 7,
            },
        };

        assert_eq!(cmd.name(), "history");
    }
}
