use super::*;

pub(super) fn execute_stages(
    db: &HistoryDb,
    command: Option<&str>,
    invocation: Option<i64>,
    slowest: usize,
    trend: Option<&str>,
    window: usize,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    if let Some(stage_name) = trend {
        // Trend view: per-invocation timing for one stage
        let points = db.get_stage_trend(stage_name, command, window)?;
        if ctx.is_human() {
            if points.is_empty() {
                println!("No timing data for stage '{stage_name}'.");
            } else {
                println!(
                    "Stage '{}' trend (last {} invocations):",
                    style(stage_name).bold(),
                    points.len()
                );
                for pt in &points {
                    let status_icon = if pt.success { "✓" } else { "✗" };
                    println!(
                        "  [{}] {} {:.3}s  {}",
                        status_icon,
                        super::format_display_time_str(&pt.started_at),
                        pt.duration_secs,
                        style(format!("(inv {})", pt.invocation_id)).dim()
                    );
                }
            }
        } else {
            ctx.print_json(&points)?;
        }
        return Ok(CommandResult::success()
            .with_message(format!(
                "{} data points for stage '{stage_name}'",
                points.len()
            ))
            .with_duration(ctx.elapsed()));
    }

    if let Some(inv_id) = invocation {
        // Per-invocation timings
        let timings = db.get_stage_timings_for_invocation(inv_id)?;
        if ctx.is_human() {
            if timings.is_empty() {
                println!("No stage timings for invocation {inv_id}.");
            } else {
                println!("Stage timings for invocation {inv_id}:");
                let mut builder = Builder::new();
                builder.push_record(["STAGE", "STARTED", "DURATION", "STATUS"]);
                for t in &timings {
                    let status = if t.success { "ok" } else { "fail" };
                    builder.push_record([
                        t.stage_name.clone(),
                        super::format_display_time_str(&t.started_at),
                        format!("{:.3}s", t.duration_secs),
                        status.to_string(),
                    ]);
                }
                let mut table = builder.build();
                table.with(Style::rounded());
                println!("{table}");
            }
        } else {
            ctx.print_json(&timings)?;
        }
        return Ok(CommandResult::success()
            .with_message(format!("{} stage timings for inv {inv_id}", timings.len()))
            .with_duration(ctx.elapsed()));
    }

    // Default: slowest N stages by avg duration
    let stats = db.get_slowest_stages(command, slowest)?;
    if ctx.is_human() {
        if stats.is_empty() {
            println!("No stage timing data found.");
            if command.is_some() {
                println!(
                    "  {}",
                    style("(Try without --command to see all stages)").dim()
                );
            }
        } else {
            let cmd_note = command
                .map(|c| format!(" (command: {c})"))
                .unwrap_or_default();
            println!("Slowest stages{cmd_note} (avg):");
            let mut builder = Builder::new();
            builder.push_record(["STAGE", "AVG (s)", "TAIL (s)", "RUNS"]);
            for s in &stats {
                builder.push_record([
                    s.stage_name.clone(),
                    format!("{:.3}", s.avg_duration_secs),
                    format!("{:.3}", s.max_duration_secs),
                    s.run_count.to_string(),
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
        .with_message(format!("{} stages", stats.len()))
        .with_duration(ctx.elapsed()))
}

// ─── G3: Fix Session Analytics ──────────────────────────────────────────────

/// Show fix session history (G3).
pub(super) fn execute_fix_sessions(
    db: &HistoryDb,
    sessions: usize,
    effectiveness: bool,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let fix_sessions = db.get_fix_sessions(sessions)?;

    if ctx.is_human() {
        if fix_sessions.is_empty() {
            println!("No fix session history found.");
            println!(
                "  {}",
                style("(Fix sessions are recorded when you run `xtask fix`)").dim()
            );
        } else if effectiveness {
            println!(
                "Fix effectiveness ({} session{}):",
                fix_sessions.len(),
                if fix_sessions.len() == 1 { "" } else { "s" }
            );
            let mut builder = Builder::new();
            builder.push_record([
                "STARTED",
                "DURATION",
                "PRE-ERRORS",
                "PRE-WARNINGS",
                "PRE-FIXABLE",
            ]);
            for s in &fix_sessions {
                let duration = s
                    .duration_secs
                    .map_or_else(|| "-".into(), |d| format!("{d:.1}s"));
                builder.push_record([
                    super::format_display_time_str(&s.started_at),
                    duration,
                    s.pre_fix_errors
                        .map_or_else(|| "-".into(), |v| v.to_string()),
                    s.pre_fix_warnings
                        .map_or_else(|| "-".into(), |v| v.to_string()),
                    s.pre_fix_fixable
                        .map_or_else(|| "-".into(), |v| v.to_string()),
                ]);
            }
            let mut table = builder.build();
            table.with(Style::rounded());
            println!("{table}");
        } else {
            println!("Fix sessions (last {}):", fix_sessions.len());
            for s in &fix_sessions {
                let duration = s
                    .duration_secs
                    .map_or_else(|| "running".into(), |d| format!("{d:.1}s"));
                let pre = if let (Some(e), Some(w)) = (s.pre_fix_errors, s.pre_fix_warnings) {
                    format!(" [pre-fix: {e}E {w}W]")
                } else {
                    String::new()
                };
                println!(
                    "  {} — {}{}",
                    super::format_display_time_str(&s.started_at),
                    duration,
                    style(pre).dim()
                );
            }
        }
    } else {
        ctx.print_json(&fix_sessions)?;
    }

    Ok(CommandResult::success()
        .with_message(format!("{} fix sessions", fix_sessions.len()))
        .with_duration(ctx.elapsed()))
}

// ─── I: Semantic Query Intelligence execute functions ─────────────────────────

/// I3: Diagnostic lifecycle view.
pub(super) fn execute_diagnostics_lifecycle(
    db: &HistoryDb,
    package: Option<&str>,
    code: Option<&str>,
    level: Option<&str>,
    lifecycle_status: Option<&str>,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let entries = db.get_diagnostic_lifecycle(package, code, level, lifecycle_status, 200)?;

    if ctx.is_human() {
        if entries.is_empty() {
            println!("No diagnostic lifecycle data found.");
            println!(
                "  {}",
                style("(Run `xtask check` to populate diagnostic history)").dim()
            );
        } else {
            let mut builder = Builder::new();
            builder.push_record([
                "STATUS",
                "PACKAGE",
                "LEVEL",
                "CODE",
                "OCCURRENCES",
                "MESSAGE",
            ]);
            for e in &entries {
                let status = match e.status {
                    LifecycleStatus::New => style("new".to_string()).green().to_string(),
                    LifecycleStatus::Chronic => style("chronic".to_string()).red().to_string(),
                    LifecycleStatus::Recurring => {
                        style("recurring".to_string()).yellow().to_string()
                    }
                    LifecycleStatus::Resolved => style("resolved".to_string()).dim().to_string(),
                };
                let pkg = e.package.as_deref().unwrap_or("-");
                let code = e.code.as_deref().unwrap_or("-");
                let msg = truncate_message(&e.message, 55);
                builder.push_record([
                    status,
                    pkg.to_string(),
                    e.level.clone(),
                    code.to_string(),
                    e.occurrence_count.to_string(),
                    msg,
                ]);
            }
            let mut table = builder.build();
            table.with(Style::rounded());
            println!("{table}");
        }
    } else {
        ctx.print_json(&entries)?;
    }

    Ok(CommandResult::success()
        .with_message(format!("Lifecycle: {} diagnostics", entries.len()))
        .with_duration(ctx.elapsed()))
}

/// I1: Named views dispatch.
pub(super) fn execute_view(
    db: &HistoryDb,
    name: Option<&str>,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    struct ViewDef {
        name: &'static str,
        description: &'static str,
    }
    let views = [
        ViewDef {
            name: "fixable-now",
            description: "Auto-fixable diagnostics in current workspace state",
        },
        ViewDef {
            name: "drift-guard-bypasses",
            description: "Recent pre-push drift-guard bypasses (security/hygiene audit trail)",
        },
        ViewDef {
            name: "impact-audit",
            description: "Recent impact-plan audit runs (skip-accuracy / false-negative evidence)",
        },
        ViewDef {
            name: "traces",
            description: "Most recent internal trace events",
        },
        ViewDef {
            name: "chronic-diagnostics",
            description: "Diagnostics present in 3+ recent invocations",
        },
        ViewDef {
            name: "new-diagnostics",
            description: "Diagnostics appearing for the first time",
        },
        ViewDef {
            name: "resolved-last-run",
            description: "Diagnostics that disappeared in the most recent run",
        },
        ViewDef {
            name: "flaky-tests",
            description: "Tests that have failed and passed across recent runs",
        },
        ViewDef {
            name: "slow-stages",
            description: "Slowest pipeline stages by average duration",
        },
        ViewDef {
            name: "hot-packages",
            description: "Packages with the most current diagnostics",
        },
        ViewDef {
            name: "fix-history",
            description: "Recent fix sessions with before/after counts",
        },
        ViewDef {
            name: "recent-regressions",
            description: "New errors correlated with test failures (last 7d)",
        },
        ViewDef {
            name: "workspace-timeline",
            description: "Chronological view of recent invocations",
        },
        ViewDef {
            name: "build-bottlenecks",
            description: "Pipeline stages contributing most to build time",
        },
    ];

    let Some(name) = name else {
        if ctx.is_human() {
            println!("Available views (use: xtask history view <name>):\n");
            for v in &views {
                println!("  {:30} {}", style(v.name).bold(), v.description);
            }
        } else {
            ctx.print_json(
                &views
                    .iter()
                    .map(|v| serde_json::json!({"name": v.name, "description": v.description}))
                    .collect::<Vec<_>>(),
            )?;
        }
        return Ok(CommandResult::success()
            .with_message(format!("{} views available", views.len()))
            .with_duration(ctx.elapsed()));
    };

    match name {
        "fixable-now" => {
            let diags = DiagnosticQuery::new().fixable().current().run(db)?;
            if ctx.is_human() {
                if diags.is_empty() {
                    println!("No fixable diagnostics found.");
                } else {
                    println!("Fixable diagnostics ({}):", diags.len());
                    render_diagnostics_table(&diags, DiagnosticsDisplayMode::Fixable);
                }
            } else {
                ctx.print_json(&diags)?;
            }
            Ok(CommandResult::success()
                .with_message(format!("{} fixable diagnostics", diags.len()))
                .with_duration(ctx.elapsed()))
        }
        "drift-guard-bypasses" => {
            let rows = db.get_drift_guard_bypasses(20)?;
            if ctx.is_human() {
                if rows.is_empty() {
                    println!("No drift-guard bypasses recorded.");
                } else {
                    println!("Drift-guard bypasses ({}):", rows.len());
                    let mut builder = Builder::new();
                    builder.push_record(["RECORDED", "BRANCH", "HEAD", "PUSH_OK"]);
                    for r in &rows {
                        builder.push_record([
                            r.recorded_at.clone(),
                            r.git_branch.clone().unwrap_or_else(|| "-".to_string()),
                            r.head_sha
                                .as_deref()
                                .map_or_else(|| "-".to_string(), |s| truncate_message(s, 12)),
                            r.push_succeeded
                                .map_or_else(|| "-".to_string(), |b| b.to_string()),
                        ]);
                    }
                    let mut table = builder.build();
                    table.with(Style::rounded());
                    println!("{table}");
                }
            } else {
                ctx.print_json(&rows)?;
            }
            Ok(CommandResult::success()
                .with_message(format!("{} drift-guard bypasses", rows.len()))
                .with_duration(ctx.elapsed()))
        }
        "impact-audit" => {
            let rows = db.get_impact_audit_runs(20)?;
            if ctx.is_human() {
                if rows.is_empty() {
                    println!("No impact-plan audit runs recorded.");
                } else {
                    println!("Impact-plan audit runs ({}):", rows.len());
                    let mut builder = Builder::new();
                    builder.push_record(["CREATED", "STATUS", "SAMPLE", "FALSE_NEG"]);
                    for r in &rows {
                        builder.push_record([
                            r.created_at.clone(),
                            r.status.clone(),
                            r.sample_size.to_string(),
                            r.false_negative_count.to_string(),
                        ]);
                    }
                    let mut table = builder.build();
                    table.with(Style::rounded());
                    println!("{table}");
                }
            } else {
                ctx.print_json(&rows)?;
            }
            Ok(CommandResult::success()
                .with_message(format!("{} impact audit runs", rows.len()))
                .with_duration(ctx.elapsed()))
        }
        "traces" => {
            let rows = db.get_recent_trace_events(50)?;
            if ctx.is_human() {
                if rows.is_empty() {
                    println!("No trace events recorded.");
                } else {
                    println!("Recent trace events ({}):", rows.len());
                    let mut builder = Builder::new();
                    builder.push_record(["TS", "LEVEL", "TARGET", "MESSAGE"]);
                    for r in &rows {
                        builder.push_record([
                            r.ts.clone(),
                            r.level.clone(),
                            truncate_message(&r.target, 28),
                            truncate_message(&r.message, 60),
                        ]);
                    }
                    let mut table = builder.build();
                    table.with(Style::rounded());
                    println!("{table}");
                }
            } else {
                ctx.print_json(&rows)?;
            }
            Ok(CommandResult::success()
                .with_message(format!("{} trace events", rows.len()))
                .with_duration(ctx.elapsed()))
        }
        "chronic-diagnostics" | "new-diagnostics" | "resolved-last-run" => {
            let status = match name {
                "chronic-diagnostics" => "chronic",
                "new-diagnostics" => "new",
                _ => "resolved",
            };
            execute_diagnostics_lifecycle(db, None, None, None, Some(status), ctx)
        }
        "flaky-tests" => {
            let tests = db.get_flaky_tests(20)?;
            if ctx.is_human() {
                if tests.is_empty() {
                    println!("No flaky tests found.");
                } else {
                    let mut builder = Builder::new();
                    builder.push_record(["TEST", "PACKAGE", "INVOCATION"]);
                    for (name, pkg, inv) in &tests {
                        builder.push_record([
                            truncate_message(name, 48),
                            pkg.clone(),
                            inv.to_string(),
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
                .with_message(format!("{} flaky tests", tests.len()))
                .with_duration(ctx.elapsed()))
        }
        "slow-stages" | "build-bottlenecks" => {
            let stages = db.get_slowest_stages(None, 15)?;
            if ctx.is_human() {
                if stages.is_empty() {
                    println!("No stage timing data found.");
                } else {
                    println!("Slowest pipeline stages:");
                    let mut builder = Builder::new();
                    builder.push_record(["STAGE", "AVG (s)", "TAIL (s)", "RUNS"]);
                    for s in &stages {
                        builder.push_record([
                            s.stage_name.clone(),
                            format!("{:.2}", s.avg_duration_secs),
                            format!("{:.2}", s.max_duration_secs),
                            s.run_count.to_string(),
                        ]);
                    }
                    let mut table = builder.build();
                    table.with(Style::rounded());
                    println!("{table}");
                }
            } else {
                ctx.print_json(&stages)?;
            }
            Ok(CommandResult::success()
                .with_message(format!("{} stages", stages.len()))
                .with_duration(ctx.elapsed()))
        }
        "hot-packages" => {
            let analysis = HistoryAnalysis::new(db);
            let health = analysis.all_packages_health()?;
            if ctx.is_human() {
                if health.is_empty() {
                    println!("No package diagnostic data found.");
                } else {
                    let mut builder = Builder::new();
                    builder.push_record(["PACKAGE", "DIAGNOSTICS", "FIXABLE", "TEST RATE"]);
                    for h in health.iter().take(20) {
                        let test_rate = h
                            .test_pass_rate
                            .map_or_else(|| "-".into(), |r| format!("{:.0}%", r * 100.0));
                        builder.push_record([
                            h.package.clone(),
                            h.diagnostic_count.to_string(),
                            h.fixable_count.to_string(),
                            test_rate,
                        ]);
                    }
                    let mut table = builder.build();
                    table.with(Style::rounded());
                    println!("{table}");
                }
            } else {
                ctx.print_json(&health)?;
            }
            Ok(CommandResult::success()
                .with_message(format!("{} packages", health.len()))
                .with_duration(ctx.elapsed()))
        }
        "fix-history" => execute_fix_sessions(db, 10, true, ctx),
        "recent-regressions" => {
            let since = time::OffsetDateTime::now_utc() - time::Duration::days(7);
            let analysis = HistoryAnalysis::new(db);
            let regressions = analysis.regression_scan(since)?;
            if ctx.is_human() {
                if regressions.is_empty() {
                    println!("No recent regressions found (last 7 days).");
                } else {
                    println!("Recent regressions ({}):", regressions.len());
                    let mut builder = Builder::new();
                    builder.push_record([
                        "INVOCATION",
                        "PACKAGE",
                        "LEVEL",
                        "TEST FAILURES",
                        "MESSAGE",
                    ]);
                    for r in &regressions {
                        let pkg = r.package.as_deref().unwrap_or("-");
                        builder.push_record([
                            r.invocation_id.to_string(),
                            pkg.to_string(),
                            r.level.clone(),
                            r.test_failures.to_string(),
                            truncate_message(&r.message, 50),
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
                .with_message(format!("{} regressions", regressions.len()))
                .with_duration(ctx.elapsed()))
        }
        "workspace-timeline" => execute_timeline(db, None, 7, 20, false, ctx),
        _ => {
            let names: Vec<&str> = views.iter().map(|v| v.name).collect();
            Err(color_eyre::eyre::eyre!(
                "Unknown view '{name}'. Available: {}",
                names.join(", ")
            ))
        }
    }
}
