use super::*;

pub(super) fn parse_duration_secs(s: &str) -> Option<i64> {
    let s = s.trim();
    if let Some(n) = s.strip_suffix('s') {
        n.parse::<i64>().ok()
    } else if let Some(n) = s.strip_suffix('m') {
        n.parse::<i64>().ok().map(|n| n * 60)
    } else if let Some(n) = s.strip_suffix('h') {
        n.parse::<i64>().ok().map(|n| n * 3600)
    } else if let Some(n) = s.strip_suffix('d') {
        n.parse::<i64>().ok().map(|n| n * 86400)
    } else {
        None
    }
}

pub(super) fn format_history_cutoff_timestamp(
    cutoff: time::OffsetDateTime,
    context: &'static str,
) -> Result<String> {
    cutoff
        .format(&time::format_description::well_known::Rfc3339)
        .wrap_err_with(|| format!("failed to format {context} as RFC3339"))
}

#[derive(Clone, Copy)]
pub(super) struct ListFlags {
    pub(super) with_diagnostics: bool,
    pub(super) with_stages: bool,
    pub(super) with_tests: bool,
    pub(super) include_zombies: bool,
}

#[allow(clippy::too_many_arguments)]
pub(super) fn execute_list(
    db: &HistoryDb,
    limit: usize,
    offset: usize,
    command: Option<&str>,
    after_invocation: Option<&str>,
    before_invocation: Option<&str>,
    since: Option<&str>,
    sort_by: &str,
    flags: ListFlags,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let ListFlags {
        with_diagnostics,
        with_stages,
        with_tests,
        include_zombies,
    } = flags;
    let mut warnings = Vec::new();

    // Parse --since into an RFC3339 cutoff timestamp
    let since_ts = since
        .and_then(parse_duration_secs)
        .map(|secs| {
            format_history_cutoff_timestamp(
                time::OffsetDateTime::now_utc() - time::Duration::seconds(secs),
                "history --since cutoff",
            )
        })
        .transpose()?;

    let after_id = after_invocation
        .map(|value| {
            db.resolve_invocation_id(value, command)?.ok_or_else(|| {
                color_eyre::eyre::eyre!(
                    "--after-invocation '{}' did not match any recorded invocation",
                    value
                )
            })
        })
        .transpose()?;
    let before_id = before_invocation
        .map(|value| {
            db.resolve_invocation_id(value, command)?.ok_or_else(|| {
                color_eyre::eyre::eyre!(
                    "--before-invocation '{}' did not match any recorded invocation",
                    value
                )
            })
        })
        .transpose()?;

    let mut query = InvocationQuery::new().limit(limit).offset(offset);
    if let Some(command) = command {
        query = query.command(command);
    }
    if let Some(after_id) = after_id {
        query = query.after_invocation(after_id);
    }
    if let Some(before_id) = before_id {
        query = query.before_invocation(before_id);
    }
    if let Some(since_ts) = since_ts {
        query = query.since_rfc3339(since_ts);
    }
    query = match sort_by {
        "duration" => query.sort_duration(),
        "status" => query.sort_status(),
        _ => query.sort_started(),
    };
    if include_zombies {
        query = query.include_zombies();
    }

    let invocations = query.run(db)?;

    if ctx.is_human() {
        if invocations.is_empty() {
            println!("No history entries found.");
        } else {
            let enriched = with_diagnostics || with_stages || with_tests;
            if enriched {
                println!(
                    "{:<6} {:<12} {:<10} {:>8}  STARTED             ENRICHMENT",
                    "ID", "COMMAND", "STATUS", "DURATION"
                );
            } else {
                println!(
                    "{:<6} {:<12} {:<10} {:<10} {:>8}  STARTED",
                    "ID", "COMMAND", "PROFILE", "STATUS", "DURATION"
                );
            }
            for inv in &invocations {
                let duration = inv
                    .duration_secs
                    .map_or_else(|| "-".into(), |d| format!("{d:.1}s"));
                let status = format!("{:?}", inv.status).to_lowercase();

                if enriched {
                    let mut parts = Vec::new();
                    if with_diagnostics {
                        let probe = diagnostic_summary_probe_from_result(
                            inv.id,
                            db.get_diagnostic_counts_for_invocation(inv.id),
                        );
                        if let Some(issue) = probe.issue {
                            warnings.push(issue);
                        }
                        parts.push(probe.fragment);
                    }
                    if with_stages {
                        let probe = stage_summary_probe_from_result(
                            inv.id,
                            db.get_stage_timings_for_invocation(inv.id),
                        );
                        if let Some(issue) = probe.issue {
                            warnings.push(issue);
                        }
                        parts.push(probe.fragment);
                    }
                    if with_tests {
                        let probe = test_summary_probe_from_result(
                            inv.id,
                            db.get_test_counts_for_invocation(inv.id),
                        );
                        if let Some(issue) = probe.issue {
                            warnings.push(issue);
                        }
                        parts.push(probe.fragment);
                    }
                    println!(
                        "{:<6} {:<12} {:<10} {:>8}  {}  {}",
                        inv.id,
                        inv.command,
                        status,
                        duration,
                        super::format_display_time(&inv.started_at),
                        parts.join(" "),
                    );
                } else {
                    let profile = inv.profile.as_deref().unwrap_or("-");
                    println!(
                        "{:<6} {:<12} {:<10} {:<10} {:>8}  {}",
                        inv.id,
                        inv.command,
                        profile,
                        status,
                        duration,
                        super::format_display_time(&inv.started_at)
                    );
                }
            }
        }
    } else {
        ctx.print_json(&invocations)?;
    }

    let mut result = CommandResult::success()
        .with_message(format!("Found {} history entries", invocations.len()))
        .with_duration(ctx.elapsed());
    for warning in warnings {
        result = result.with_warning(warning);
    }
    Ok(result)
}

pub(super) fn execute_last(
    db: &HistoryDb,
    command: &str,
    ctx: &CommandContext,
) -> Result<CommandResult> {
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
        ctx.print_json(&inv)?;
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

pub(super) fn execute_stats(
    db: &HistoryDb,
    command: &str,
    days: u32,
    package: Option<&str>,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let stats = db.get_stats(command, days)?;

    if ctx.is_human() {
        let pkg_note = package
            .map(|p| format!(" (package: {p})"))
            .unwrap_or_default();
        println!("Statistics for '{command}'{pkg_note} (last {days} days):");
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
        ctx.print_json(&stats)?;
    }

    Ok(CommandResult::success()
        .with_message(format!("Statistics for '{command}' over {days} days"))
        .with_duration(ctx.elapsed()))
}

pub(super) fn execute_stats_all_packages(
    db: &HistoryDb,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let analysis = HistoryAnalysis::new(db);
    let health = analysis.all_packages_health()?;

    if ctx.is_human() {
        if health.is_empty() {
            println!("No package diagnostic data found.");
        } else {
            let mut builder = Builder::new();
            builder.push_record([
                "PACKAGE",
                "DIAGNOSTICS",
                "FIXABLE",
                "TEST RATE",
                "AVG BUILD",
            ]);
            for h in &health {
                let test_rate = h
                    .test_pass_rate
                    .map_or_else(|| "-".into(), |r| format!("{:.0}%", r * 100.0));
                let avg_build = h
                    .avg_build_time_secs
                    .map_or_else(|| "-".into(), |s| format!("{s:.1}s"));
                builder.push_record([
                    h.package.clone(),
                    h.diagnostic_count.to_string(),
                    h.fixable_count.to_string(),
                    test_rate,
                    avg_build,
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
        .with_message(format!("Health for {} packages", health.len()))
        .with_duration(ctx.elapsed()))
}

pub(super) fn execute_stats_all_commands(
    db: &HistoryDb,
    days: u32,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    // Collect unique commands from history, then get stats for each
    let invocations = db.get_recent(500, None)?;
    let mut commands: Vec<String> = invocations
        .iter()
        .map(|i| i.command.clone())
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();
    commands.sort();

    let mut all_stats = Vec::new();
    for cmd in &commands {
        let stats = db.get_stats(cmd, days)?;
        all_stats.push((cmd.clone(), stats));
    }

    if ctx.is_human() {
        if all_stats.is_empty() {
            println!("No history found.");
        } else {
            let mut builder = Builder::new();
            builder.push_record([
                "COMMAND",
                "TOTAL",
                "SUCCESS",
                "FAILED",
                "SUCCESS %",
                "AVG TIME",
            ]);
            for (cmd, s) in &all_stats {
                let rate = if s.total > 0 {
                    format!("{:.1}%", (s.successes as f64 / s.total as f64) * 100.0)
                } else {
                    "-".into()
                };
                let avg = s
                    .avg_duration_secs
                    .map_or_else(|| "-".into(), |d| format!("{d:.1}s"));
                builder.push_record([
                    cmd.clone(),
                    s.total.to_string(),
                    s.successes.to_string(),
                    s.failures.to_string(),
                    rate,
                    avg,
                ]);
            }
            let mut table = builder.build();
            table.with(Style::rounded());
            println!("{table}");
        }
    } else {
        ctx.print_json(
            &all_stats
                .iter()
                .map(|(cmd, s)| serde_json::json!({"command": cmd, "stats": s}))
                .collect::<Vec<_>>(),
        )?;
    }

    Ok(CommandResult::success()
        .with_message(format!("Stats for {} commands", all_stats.len()))
        .with_duration(ctx.elapsed()))
}

pub(super) fn parse_history_time(value: &str, field: &'static str) -> Result<time::OffsetDateTime> {
    time::OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339)
        .wrap_err_with(|| format!("failed to parse history {field}: {value}"))
}

pub(super) fn json_i64(
    row: &serde_json::Map<String, serde_json::Value>,
    field: &'static str,
) -> Result<i64> {
    row.get(field)
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| color_eyre::eyre::eyre!("history cost row missing integer field {field}"))
}

pub(super) fn json_string(
    row: &serde_json::Map<String, serde_json::Value>,
    field: &'static str,
) -> Result<String> {
    row.get(field)
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned)
        .ok_or_else(|| color_eyre::eyre::eyre!("history cost row missing string field {field}"))
}

pub(super) fn json_optional_string(
    row: &serde_json::Map<String, serde_json::Value>,
    field: &'static str,
) -> Option<String> {
    row.get(field)
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned)
}

pub(super) fn json_optional_f64(
    row: &serde_json::Map<String, serde_json::Value>,
    field: &'static str,
) -> Option<f64> {
    row.get(field).and_then(serde_json::Value::as_f64)
}

pub(super) fn json_optional_i64(
    row: &serde_json::Map<String, serde_json::Value>,
    field: &'static str,
) -> Option<i64> {
    row.get(field).and_then(serde_json::Value::as_i64)
}

pub(super) fn sql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

pub(super) fn secs_to_hours(secs: f64) -> f64 {
    let hours = secs / 3600.0;
    if hours.abs() < 0.000_001 { 0.0 } else { hours }
}

pub(super) fn execute_export(
    db: &HistoryDb,
    limit: usize,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let invocations = db.get_recent(limit, None)?;
    ctx.print_json(&invocations)?;

    Ok(CommandResult::success()
        .with_message(format!("Exported {} entries", invocations.len()))
        .with_duration(ctx.elapsed()))
}

/// Output format for diagnostics.
#[derive(Debug, Clone, Default, clap::ValueEnum)]
pub enum DiagnosticsFormat {
    /// Human-readable table (default)
    #[default]
    Table,
    /// GCC-compatible format: file:line:col: level: message [code]
    ///
    /// Consumed by VS Code problem matchers, Vim :make, Emacs compile-mode.
    Gcc,
}
