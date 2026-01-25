//! History command - query build/test execution history

use anyhow::Result;

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::config::config;
use crate::history::HistoryDb;

/// History command variants
#[derive(Debug, Clone)]
pub enum HistorySubcommand {
    List {
        limit: usize,
        command: Option<String>,
    },
    Last {
        command: String,
    },
    Stats {
        command: String,
        days: u32,
    },
    Prune {
        older_than: u32,
    },
    Export {
        limit: usize,
    },
    Tests {
        tests_cmd: HistoryTestsSubcommand,
    },
}

/// History tests subcommand variants
#[derive(Debug, Clone)]
pub enum HistoryTestsSubcommand {
    Slowest {
        limit: usize,
    },
    Flaky {
        limit: usize,
    },
    GettingSlower {
        threshold_pct: f64,
        window: usize,
        limit: usize,
    },
    Trends {
        pattern: Option<String>,
        package: Option<String>,
        runs: usize,
    },
    Eta,
}

/// History management command
pub struct HistoryCommand {
    pub subcommand: HistorySubcommand,
}

impl XtaskCommand for HistoryCommand {
    fn name(&self) -> &str {
        "history"
    }

    fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
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
                "{:<6} {:<12} {:<10} {:<10} {:>8}  {}",
                "ID", "COMMAND", "PROFILE", "STATUS", "DURATION", "STARTED"
            );
            for inv in &invocations {
                let profile = inv.profile.as_deref().unwrap_or("-");
                let duration = inv
                    .duration_secs
                    .map(|d| format!("{:.1}s", d))
                    .unwrap_or_else(|| "-".into());
                let status = format!("{:?}", inv.status).to_lowercase();
                println!(
                    "{:<6} {:<12} {:<10} {:<10} {:>8}  {}",
                    inv.id,
                    inv.command,
                    profile,
                    status,
                    duration,
                    inv.started_at.format("%Y-%m-%d %H:%M")
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
                println!("Last {} invocation:", command);
                println!("  ID:       {}", inv.id);
                println!("  Status:   {:?}", inv.status);
                println!("  Started:  {}", inv.started_at);
                if let Some(d) = inv.duration_secs {
                    println!("  Duration: {:.2}s", d);
                }
                if let Some(c) = &inv.git_commit {
                    println!(
                        "  Commit:   {}{}",
                        c,
                        if inv.git_dirty { " (dirty)" } else { "" }
                    );
                }
            }
            None => println!("No history for command: {}", command),
        }
    } else {
        let json = serde_json::to_string_pretty(&inv)?;
        println!("{json}");
    }

    let message = if inv.is_some() {
        format!("Last invocation for '{}'", command)
    } else {
        format!("No history for command '{}'", command)
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
        println!("Statistics for '{}' (last {} days):", command, days);
        println!("  Total:     {}", stats.total);
        println!("  Successes: {}", stats.successes);
        println!("  Failures:  {}", stats.failures);
        if let Some(avg) = stats.avg_duration_secs {
            println!("  Avg time:  {:.2}s", avg);
        }
        if stats.total > 0 {
            let rate = (stats.successes as f64 / stats.total as f64) * 100.0;
            println!("  Success:   {:.1}%", rate);
        }
    } else {
        let json = serde_json::to_string_pretty(&stats)?;
        println!("{json}");
    }

    Ok(CommandResult::success()
        .with_message(format!("Statistics for '{}' over {} days", command, days))
        .with_duration(ctx.elapsed()))
}

fn execute_prune(db: &HistoryDb, older_than: u32, ctx: &CommandContext) -> Result<CommandResult> {
    let count = db.prune(older_than)?;

    if ctx.is_human() {
        println!("Pruned {} entries older than {} days", count, older_than);
    } else {
        println!(
            r#"{{"pruned": {}, "older_than_days": {}}}"#,
            count, older_than
        );
    }

    Ok(CommandResult::success()
        .with_message(format!("Pruned {} old entries", count))
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
                println!(
                    "{:<50} {:<20} {:>10.3} {:>6}",
                    display_name, package, avg, runs
                );
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
            println!("{:<50} {:<20} {:>10}", "TEST", "PACKAGE", "INVOCATION");
            for (name, package, inv_id) in &tests {
                let display_name = if name.len() > 48 {
                    format!("...{}", &name[name.len() - 45..])
                } else {
                    name.clone()
                };
                println!("{:<50} {:<20} {:>10}", display_name, package, inv_id);
            }
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
            println!(
                "No tests found slowing >{}% over {} runs.",
                threshold_pct, window
            );
        } else {
            println!(
                "{:<45} {:<15} {:>10} {:>10} {:>8}",
                "TEST", "PACKAGE", "OLD (s)", "NEW (s)", "CHANGE"
            );
            for test in &tests {
                let display_name = if test.test_name.len() > 43 {
                    format!("...{}", &test.test_name[test.test_name.len() - 40..])
                } else {
                    test.test_name.clone()
                };
                println!(
                    "{:<45} {:<15} {:>10.3} {:>10.3} {:>+7.1}%",
                    display_name,
                    test.package,
                    test.older_avg_secs,
                    test.recent_avg_secs,
                    test.pct_change
                );
            }
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
                    let timestamp = test.timestamps.get(i).map(|s| s.as_str()).unwrap_or("-");
                    println!("  {}: {:.3}s", timestamp, duration);
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
                    println!("  {:<30} {:>6.1}s", pkg, secs);
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
    use crate::output::OutputWriter;

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
