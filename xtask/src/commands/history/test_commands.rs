use color_eyre::eyre::Result;
use console::style;
use tabled::{builder::Builder, settings::Style};

use crate::command::{CommandContext, CommandResult};
use crate::history::HistoryDb;

/// History tests subcommand variants
#[derive(Debug, Clone, clap::Subcommand)]
pub enum HistoryTestsSubcommand {
    Slowest {
        #[arg(long, default_value = "10")]
        limit: usize,
        /// Test run selector: `latest`, `previous`, `latest-success`, `latest-failure`,
        /// invocation ID, `inv:<id>`, or `job:<id>`
        #[arg(long)]
        invocation: Option<String>,
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
    /// Show failing tests from the most recent test run
    Failures {
        #[arg(long, default_value = "20")]
        limit: usize,
        /// Show captured failure output (can be verbose)
        #[arg(long)]
        output: bool,
        /// Test run selector: `latest`, `previous`, `latest-success`, `latest-failure`,
        /// invocation ID, `inv:<id>`, or `job:<id>`
        #[arg(long, default_value = "latest")]
        invocation: String,
    },
    /// Comprehensive analysis of the most recent test run
    ///
    /// Shows duration distribution, probable timeouts, and per-package failure summaries.
    Analyze {
        /// Test run selector: `latest`, `previous`, `latest-success`, `latest-failure`,
        /// invocation ID, `inv:<id>`, or `job:<id>`
        #[arg(long, default_value = "latest")]
        invocation: String,
    },
    /// Show captured output for a test (pass or fail)
    Output {
        /// Test name pattern to search for
        pattern: String,
        /// Test run selector: `latest`, `previous`, `latest-success`, `latest-failure`,
        /// invocation ID, `inv:<id>`, or `job:<id>`
        #[arg(long, default_value = "latest")]
        invocation: String,
    },
    Eta,
    /// Full-text search across stored test output (G7)
    Grep {
        /// Text to search for in captured test output
        text: String,
        #[arg(long, default_value = "20")]
        limit: usize,
        /// Test run selector: `latest`, `previous`, `latest-success`, `latest-failure`,
        /// invocation ID, `inv:<id>`, or `job:<id>`
        #[arg(long, default_value = "latest")]
        invocation: String,
    },
    /// Per-package pass rate, test count, avg duration, and flaky count (G7)
    ByPackage {
        /// Test run selector: `latest`, `previous`, `latest-success`, `latest-failure`,
        /// invocation ID, `inv:<id>`, or `job:<id>`
        #[arg(long, default_value = "latest")]
        invocation: String,
    },
    /// P95 duration per test over recent runs (G7)
    DurationP95 {
        #[arg(long, default_value = "20")]
        limit: usize,
    },
    /// Tests newly failing in the last N runs that previously passed (G7)
    Regression {
        /// Number of recent invocations to search for regressions
        #[arg(long, default_value = "5")]
        runs: usize,
    },
}

pub(super) fn execute_tests(
    tests_cmd: &HistoryTestsSubcommand,
    db: &HistoryDb,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    match tests_cmd {
        HistoryTestsSubcommand::Slowest { limit, invocation } => {
            execute_tests_slowest(db, invocation.as_deref(), *limit, ctx)
        }
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
        HistoryTestsSubcommand::Failures {
            limit,
            output,
            invocation,
        } => execute_tests_failures(db, invocation, *limit, *output, ctx),
        HistoryTestsSubcommand::Analyze { invocation } => {
            execute_tests_analyze(db, invocation, ctx)
        }
        HistoryTestsSubcommand::Output {
            pattern,
            invocation,
        } => execute_tests_output(db, invocation, pattern, ctx),
        HistoryTestsSubcommand::Eta => execute_tests_eta(db, ctx),
        HistoryTestsSubcommand::Grep {
            text,
            limit,
            invocation,
        } => execute_tests_grep(db, invocation, text, *limit, ctx),
        HistoryTestsSubcommand::ByPackage { invocation } => {
            execute_tests_by_package(db, invocation, ctx)
        }
        HistoryTestsSubcommand::DurationP95 { limit } => {
            execute_tests_duration_p95(db, *limit, ctx)
        }
        HistoryTestsSubcommand::Regression { runs } => execute_tests_regression(db, *runs, ctx),
    }
}

fn resolve_selected_test_run(
    db: &HistoryDb,
    invocation: &str,
) -> Result<Option<crate::history::ResolvedTestRun>> {
    db.resolve_test_run(Some(invocation))
}

fn describe_test_run(run: &crate::history::ResolvedTestRun) -> String {
    match run.job_id {
        Some(job_id) => format!("invocation #{} (job #{job_id})", run.invocation_id),
        None => format!("invocation #{}", run.invocation_id),
    }
}

pub(super) fn execute_tests_slowest(
    db: &HistoryDb,
    invocation: Option<&str>,
    limit: usize,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    if let Some(invocation) = invocation {
        let Some(test_run) = resolve_selected_test_run(db, invocation)? else {
            if ctx.is_human() {
                println!("No test run data found.");
            }
            return Ok(CommandResult::success()
                .with_message("No test run data")
                .with_duration(ctx.elapsed()));
        };
        let tests = db.get_slowest_tests_for_invocation(test_run.invocation_id, limit)?;

        if ctx.is_human() {
            if tests.is_empty() {
                println!(
                    "No test timing rows found for {}.",
                    describe_test_run(&test_run)
                );
            } else {
                println!(
                    "{}, started {}",
                    describe_test_run(&test_run),
                    test_run.started_at
                );
                println!(
                    "{:<50} {:<20} {:<10} {:>10}",
                    "TEST", "PACKAGE", "STATUS", "DURATION"
                );
                for test in &tests {
                    let display_name = if test.test_name.len() > 48 {
                        format!("...{}", &test.test_name[test.test_name.len() - 45..])
                    } else {
                        test.test_name.clone()
                    };
                    println!(
                        "{display_name:<50} {:<20} {:<10} {:>10.3}",
                        test.package, test.status, test.duration_secs
                    );
                }
            }
        } else {
            ctx.print_json(&tests)?;
        }

        return Ok(CommandResult::success()
            .with_message(format!(
                "Found {} slowest tests for {}",
                tests.len(),
                describe_test_run(&test_run)
            ))
            .with_data(serde_json::to_value(&tests)?)
            .with_duration(ctx.elapsed()));
    }

    let tests = db.get_slowest_tests(limit)?;

    if ctx.is_human() {
        if tests.is_empty() {
            println!("No test timing data found.");
        } else {
            println!(
                "{:<50} {:<20} {:>10} {:>6}",
                "TEST", "PACKAGE", "AVG (s)", "RUNS"
            );
            for test in &tests {
                let display_name = if test.test_name.len() > 48 {
                    format!("...{}", &test.test_name[test.test_name.len() - 45..])
                } else {
                    test.test_name.clone()
                };
                println!(
                    "{display_name:<50} {:<20} {:>10.3} {:>6}",
                    test.package, test.avg_duration_secs, test.passing_runs
                );
            }
        }
    } else {
        ctx.print_json(&tests)?;
    }

    Ok(CommandResult::success()
        .with_message(format!("Found {} slowest tests", tests.len()))
        .with_data(serde_json::to_value(&tests)?)
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
        ctx.print_json(&tests)?;
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
        ctx.print_json(&tests)?;
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
        ctx.print_json(&tests)?;
    }

    Ok(CommandResult::success()
        .with_message(format!("Found {} test trends", tests.len()))
        .with_duration(ctx.elapsed()))
}

fn execute_tests_failures(
    db: &HistoryDb,
    invocation: &str,
    limit: usize,
    show_output: bool,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let Some(test_run) = resolve_selected_test_run(db, invocation)? else {
        if ctx.is_human() {
            println!("No test run data found.");
        }
        return Ok(CommandResult::success()
            .with_message("No test run data")
            .with_duration(ctx.elapsed()));
    };
    let tests = db.get_failing_tests_with_output(test_run.invocation_id, limit)?;

    if ctx.is_human() {
        if tests.is_empty() {
            println!("No failing tests in {}.", describe_test_run(&test_run));
        } else {
            println!("{}", describe_test_run(&test_run));
            let mut builder = Builder::new();
            let has_failure_msgs = tests.iter().any(|t| t.failure_message.is_some());
            if has_failure_msgs {
                builder.push_record(["TEST", "PACKAGE", "DURATION", "FAILURE"]);
            } else {
                builder.push_record(["TEST", "PACKAGE", "DURATION"]);
            }
            for test in &tests {
                let display_name = if test.test_name.len() > 48 {
                    format!("...{}", &test.test_name[test.test_name.len() - 45..])
                } else {
                    test.test_name.clone()
                };
                if has_failure_msgs {
                    let msg = test
                        .failure_message
                        .as_deref()
                        .unwrap_or("-")
                        .chars()
                        .take(60)
                        .collect::<String>();
                    builder.push_record([
                        display_name,
                        test.package.clone(),
                        format!("{:.3}s", test.duration_secs),
                        msg,
                    ]);
                } else {
                    builder.push_record([
                        display_name,
                        test.package.clone(),
                        format!("{:.3}s", test.duration_secs),
                    ]);
                }
            }
            let mut table = builder.build();
            table.with(Style::rounded());
            println!("{table}");

            if show_output {
                println!();
                for test in &tests {
                    println!("── {} ({}) ──", test.test_name, test.package);
                    if let Some(output) = &test.output {
                        println!("{output}");
                    }
                    if let Some(nats_ctx) = &test.nats_context {
                        // Pretty-print NATS context if it's valid JSON, else raw
                        let rendered = serde_json::from_str::<serde_json::Value>(nats_ctx)
                            .ok()
                            .and_then(|v| serde_json::to_string_pretty(&v).ok())
                            .unwrap_or_else(|| nats_ctx.clone());
                        println!("  NATS consumer context:");
                        for line in rendered.lines() {
                            println!("    {line}");
                        }
                    }
                    println!();
                }
            }
        }
    } else {
        ctx.print_json(&tests)?;
    }

    Ok(CommandResult::success()
        .with_message(format!(
            "Found {} failing tests in {}",
            tests.len(),
            describe_test_run(&test_run)
        ))
        .with_duration(ctx.elapsed()))
}

pub(super) fn execute_tests_analyze(
    db: &HistoryDb,
    invocation: &str,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let Some(test_run) = resolve_selected_test_run(db, invocation)? else {
        if ctx.is_human() {
            println!("No test run data found.");
        }
        return Ok(CommandResult::success()
            .with_message("No test run data")
            .with_duration(ctx.elapsed()));
    };
    let analysis = db.analyze_test_run(test_run.invocation_id)?;

    match analysis {
        None => {
            if ctx.is_human() {
                println!(
                    "No test result rows found for {}.",
                    describe_test_run(&test_run)
                );
            }
            Ok(CommandResult::success()
                .with_message(format!(
                    "No test result rows for {}",
                    describe_test_run(&test_run)
                ))
                .with_duration(ctx.elapsed()))
        }
        Some(analysis) => {
            let infra_probe =
                infra_timing_probe_from_result(db.get_infra_timing_summary(test_run.invocation_id));
            if ctx.is_human() {
                println!("{}", style("━━━ Test Suite Analysis ━━━").bold());
                println!(
                    "{}, started {}",
                    describe_test_run(&test_run),
                    analysis.started_at
                );
                println!(
                    "  {} passed, {} failed, {} ignored",
                    style(analysis.total_passed).green(),
                    if analysis.total_failed > 0 {
                        style(analysis.total_failed).red().to_string()
                    } else {
                        style(analysis.total_failed).to_string()
                    },
                    analysis.total_ignored
                );
                println!("  Total duration: {:.1}s", analysis.total_duration_secs);

                // Duration distribution
                println!("\n{}", style("Duration Distribution:").bold());
                for bucket in &analysis.duration_buckets {
                    if bucket.count > 0 {
                        let bar = "█".repeat(std::cmp::min(bucket.count, 50));
                        println!("  {:>8} │ {:>4} │ {}", bucket.label, bucket.count, bar);
                    }
                }

                if !analysis.slowest_tests.is_empty() {
                    println!("\n{}", style("Slowest Tests:").bold());
                    let mut builder = Builder::new();
                    builder.push_record(["TEST", "PACKAGE", "STATUS", "DURATION"]);
                    for test in &analysis.slowest_tests {
                        let display_name = if test.test_name.len() > 48 {
                            format!("...{}", &test.test_name[test.test_name.len() - 45..])
                        } else {
                            test.test_name.clone()
                        };
                        builder.push_record([
                            display_name,
                            test.package.clone(),
                            test.status.clone(),
                            format!("{:.3}s", test.duration_secs),
                        ]);
                    }
                    let mut table = builder.build();
                    table.with(Style::rounded());
                    println!("{table}");
                }

                // Probable timeouts
                if !analysis.probable_timeouts.is_empty() {
                    println!("\n{}", style("⚠ Probable Timeouts:").yellow().bold());
                    for t in &analysis.probable_timeouts {
                        println!(
                            "  {}::{} ({:.1}s, {})",
                            t.package, t.test_name, t.duration_secs, t.status
                        );
                    }
                }

                // Per-package failure summary
                if !analysis.failure_summary.is_empty() {
                    println!("\n{}", style("Failures by Package:").red().bold());
                    let mut builder = Builder::new();
                    builder.push_record(["PACKAGE", "FAILED", "PASSED", "RATE", "TESTS"]);
                    for pkg in &analysis.failure_summary {
                        let tests_display = if pkg.failed_tests.len() <= 3 {
                            pkg.failed_tests.join(", ")
                        } else {
                            let first_three = &pkg.failed_tests[..3];
                            format!(
                                "{}, +{} more",
                                first_three.join(", "),
                                pkg.failed_tests.len() - 3
                            )
                        };
                        builder.push_record([
                            pkg.package.clone(),
                            pkg.failed_count.to_string(),
                            pkg.passed_count.to_string(),
                            format!("{:.1}%", pkg.failure_rate_pct),
                            tests_display,
                        ]);
                    }
                    let mut table = builder.build();
                    table.with(Style::rounded());
                    println!("{table}");
                }

                // Infrastructure timing (from sandbox slog metadata)
                if let Some(infra) = infra_probe.value.as_ref() {
                    println!("\n{}", style("Infrastructure Timing:").cyan().bold());
                    println!(
                        "  Slot acquisition: avg {:.0}ms, max {}ms ({} tests with data)",
                        infra.avg_slot_wait_ms, infra.max_slot_wait_ms, infra.tests_with_metadata,
                    );
                    if infra.dirty_slot_count > 0 {
                        println!(
                            "  Dirty slot cleanup: avg {:.0}ms ({} of {} slots were dirty)",
                            infra.avg_cleanup_ms, infra.dirty_slot_count, infra.tests_with_metadata,
                        );
                    }
                    if infra.slot_usage.len() > 1 {
                        let top_slots: Vec<String> = infra
                            .slot_usage
                            .iter()
                            .take(5)
                            .map(|(name, count)| format!("{name}:{count}"))
                            .collect();
                        println!(
                            "  Slot distribution: {} slots used (top: {})",
                            infra.slot_usage.len(),
                            top_slots.join(", ")
                        );
                    }
                } else if let Some(issue) = infra_probe.issue.as_ref() {
                    println!("\n{}", style("Infrastructure Timing:").cyan().bold());
                    println!("  {}", style(issue).yellow());
                }
            } else {
                ctx.print_json(&analysis)?;
            }

            let mut result = CommandResult::success()
                .with_message(format!(
                    "Analysis for {}: {} passed, {} failed",
                    describe_test_run(&test_run),
                    analysis.total_passed,
                    analysis.total_failed
                ))
                .with_data(serde_json::to_value(&analysis)?)
                .with_duration(ctx.elapsed());
            if let Some(issue) = infra_probe.issue {
                result = result.with_warning(issue);
            }
            Ok(result)
        }
    }
}

fn execute_tests_output(
    db: &HistoryDb,
    invocation: &str,
    pattern: &str,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let Some(test_run) = resolve_selected_test_run(db, invocation)? else {
        if ctx.is_human() {
            println!("No test run data found.");
        }
        return Ok(CommandResult::success()
            .with_message("No test run data")
            .with_duration(ctx.elapsed()));
    };
    let entries = db.get_test_output(test_run.invocation_id, pattern)?;

    if ctx.is_human() {
        if entries.is_empty() {
            println!(
                "No tests matching '{pattern}' found in {}.",
                describe_test_run(&test_run)
            );
        } else {
            println!("{}", describe_test_run(&test_run));
            for entry in &entries {
                println!(
                    "── {} ({}, {}, {:.3}s) ──",
                    entry.test_name, entry.package, entry.status, entry.duration_secs
                );
                if let Some(output) = &entry.output {
                    println!("{output}");
                } else {
                    println!("  (no captured output)");
                }
                println!();
            }
        }
    } else {
        ctx.print_json(&entries)?;
    }

    Ok(CommandResult::success()
        .with_message(format!(
            "Found {} matching tests in {}",
            entries.len(),
            describe_test_run(&test_run)
        ))
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
        ctx.print_json(&estimate)?;
    }

    Ok(CommandResult::success()
        .with_message(format!(
            "Estimated runtime: {:.0}s",
            estimate.estimated_secs
        ))
        .with_duration(ctx.elapsed()))
}

// ─── G7: Test Analytics Extensions ──────────────────────────────────────────

/// Search stored test output for text (G7 --grep).
fn execute_tests_grep(
    db: &HistoryDb,
    invocation: &str,
    text: &str,
    limit: usize,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let Some(test_run) = resolve_selected_test_run(db, invocation)? else {
        if ctx.is_human() {
            println!("No test run data found.");
        }
        return Ok(CommandResult::success()
            .with_message("No test run data")
            .with_duration(ctx.elapsed()));
    };
    let results = db.search_test_output(test_run.invocation_id, text, limit)?;

    if ctx.is_human() {
        if results.is_empty() {
            println!(
                "No test output matching '{text}' found in {}.",
                describe_test_run(&test_run)
            );
        } else {
            println!("{}", describe_test_run(&test_run));
            let mut builder = Builder::new();
            builder.push_record(["TEST", "PACKAGE", "STATUS", "DURATION"]);
            for entry in &results {
                let display_name = if entry.test_name.len() > 48 {
                    format!("...{}", &entry.test_name[entry.test_name.len() - 45..])
                } else {
                    entry.test_name.clone()
                };
                builder.push_record([
                    display_name,
                    entry.package.clone(),
                    entry.status.clone(),
                    format!("{:.3}s", entry.duration_secs),
                ]);
            }
            let mut table = builder.build();
            table.with(Style::rounded());
            println!("{table}");
            println!();
            for entry in &results {
                if let Some(output) = &entry.output {
                    // Highlight matching text in output (simple prefix/suffix)
                    let excerpt: String = output
                        .lines()
                        .filter(|l| l.to_lowercase().contains(&text.to_lowercase()))
                        .take(3)
                        .collect::<Vec<_>>()
                        .join("\n");
                    if !excerpt.is_empty() {
                        println!("  {} → {}", style(&entry.test_name).dim(), excerpt);
                    }
                }
            }
        }
    } else {
        ctx.print_json(&results)?;
    }

    Ok(CommandResult::success()
        .with_message(format!(
            "Found {} matching tests in {}",
            results.len(),
            describe_test_run(&test_run)
        ))
        .with_duration(ctx.elapsed()))
}

/// Per-package test stats (G7 --by-package).
fn execute_tests_by_package(
    db: &HistoryDb,
    invocation: &str,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let Some(test_run) = resolve_selected_test_run(db, invocation)? else {
        if ctx.is_human() {
            println!("No test run data found.");
        }
        return Ok(CommandResult::success()
            .with_message("No test run data")
            .with_duration(ctx.elapsed()));
    };
    let stats = db.get_tests_by_package(test_run.invocation_id)?;

    if ctx.is_human() {
        if stats.is_empty() {
            println!(
                "No per-package test data found in {}.",
                describe_test_run(&test_run)
            );
        } else {
            println!("{}", describe_test_run(&test_run));
            let mut builder = Builder::new();
            builder.push_record(["PACKAGE", "TOTAL", "PASSED", "FAILED", "AVG (s)", "FLAKY"]);
            for s in &stats {
                let pass_rate = if s.total > 0 {
                    format!("{:.1}%", (s.passed as f64 / s.total as f64) * 100.0)
                } else {
                    "-".into()
                };
                builder.push_record([
                    s.package.clone(),
                    s.total.to_string(),
                    format!("{} ({})", s.passed, pass_rate),
                    s.failed.to_string(),
                    format!("{:.3}", s.avg_duration_secs),
                    s.flaky_count.to_string(),
                ]);
            }
            let mut table = builder.build();
            table.with(Style::rounded());
            println!("{table}");
        }
    } else {
        ctx.print_json(&stats)?;
    }

    Ok(CommandResult::success()
        .with_message(format!(
            "Stats for {} packages in {}",
            stats.len(),
            describe_test_run(&test_run)
        ))
        .with_duration(ctx.elapsed()))
}

/// P95 duration per test (G7 --duration-p95).
fn execute_tests_duration_p95(
    db: &HistoryDb,
    limit: usize,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let results = db.get_test_duration_p95(limit)?;

    if ctx.is_human() {
        if results.is_empty() {
            println!("No test duration data found.");
        } else {
            println!("P95 test durations (slowest {limit}):");
            let mut builder = Builder::new();
            builder.push_record(["TEST", "PACKAGE", "P95 (s)"]);
            for (name, pkg, p95) in &results {
                let display_name = if name.len() > 48 {
                    format!("...{}", &name[name.len() - 45..])
                } else {
                    name.clone()
                };
                builder.push_record([display_name, pkg.clone(), format!("{p95:.3}")]);
            }
            let mut table = builder.build();
            table.with(Style::rounded());
            println!("{table}");
        }
    } else {
        ctx.print_json(
            &results
                .iter()
                .map(|(n, p, d)| serde_json::json!({"test_name": n, "package": p, "p95_secs": d}))
                .collect::<Vec<_>>(),
        )?;
    }

    Ok(CommandResult::success()
        .with_message(format!("{} tests with P95 data", results.len()))
        .with_duration(ctx.elapsed()))
}

/// Tests newly failing in recent runs that previously passed (G7 --regression).
fn execute_tests_regression(
    db: &HistoryDb,
    runs: usize,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let regressions = db.get_tests_regressing(runs)?;

    if ctx.is_human() {
        if regressions.is_empty() {
            println!("No test regressions found in the last {runs} runs.");
        } else {
            println!(
                "{} test{} newly failing in the last {runs} runs:",
                style(regressions.len()).red().bold(),
                if regressions.len() == 1 { "" } else { "s" }
            );
            let mut builder = Builder::new();
            builder.push_record(["TEST", "PACKAGE", "DURATION"]);
            for r in &regressions {
                let display_name = if r.test_name.len() > 48 {
                    format!("...{}", &r.test_name[r.test_name.len() - 45..])
                } else {
                    r.test_name.clone()
                };
                builder.push_record([
                    display_name,
                    r.package.clone(),
                    format!("{:.3}s", r.duration_secs),
                ]);
            }
            let mut table = builder.build();
            table.with(Style::rounded());
            println!("{table}");
        }
    } else {
        ctx.print_json(&regressions)?;
    }

    Ok(CommandResult::success()
        .with_message(format!("{} regressions found", regressions.len()))
        .with_duration(ctx.elapsed()))
}

pub(super) struct OptionalProbe<T> {
    pub(super) value: Option<T>,
    pub(super) issue: Option<String>,
}

pub(super) fn infra_timing_probe_from_result<T>(result: Result<Option<T>>) -> OptionalProbe<T> {
    match result {
        Ok(value) => OptionalProbe { value, issue: None },
        Err(error) => OptionalProbe {
            value: None,
            issue: Some(format!(
                "failed to read infrastructure timing summary: {error:#}"
            )),
        },
    }
}
