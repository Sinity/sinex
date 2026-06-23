use super::*;

pub(super) enum DiagnosticsDisplayMode {
    /// Default: shows PACKAGE and SOURCE columns
    Current,
    /// --all: raw accumulated, no package/source
    All,
    /// --invocation: single invocation, no source
    Invocation,
    /// --fixable: shows FIX column
    Fixable,
}

#[derive(Clone, Copy)]
pub(super) struct DiagnosticFilter<'a> {
    level: Option<&'a str>,
    file: Option<&'a str>,
    command: Option<&'a str>,
    package: Option<&'a str>,
    code: Option<&'a str>,
    fixable: bool,
}

impl<'a> DiagnosticFilter<'a> {
    pub(super) const fn new(
        level: Option<&'a str>,
        file: Option<&'a str>,
        command: Option<&'a str>,
        package: Option<&'a str>,
        code: Option<&'a str>,
        fixable: bool,
    ) -> Self {
        Self {
            level,
            file,
            command,
            package,
            code,
            fixable,
        }
    }
}

pub(super) fn apply_diagnostic_filters(
    diagnostics: &mut Vec<crate::history::StoredDiagnostic>,
    filter: DiagnosticFilter<'_>,
) {
    diagnostics.retain(|diagnostic| {
        if let Some(level) = filter.level
            && diagnostic.level != level
        {
            return false;
        }

        if let Some(pattern) = filter.file
            && !diagnostic
                .file_path
                .as_ref()
                .is_some_and(|path| path.contains(pattern))
        {
            return false;
        }

        if let Some(command) = filter.command
            && diagnostic.source_command.as_deref() != Some(command)
        {
            return false;
        }

        if let Some(package) = filter.package
            && diagnostic.package.as_deref() != Some(package)
        {
            return false;
        }

        if let Some(code) = filter.code
            && diagnostic.code.as_deref() != Some(code)
        {
            return false;
        }

        if filter.fixable && diagnostic.fix_applicability.as_deref() != Some("MachineApplicable") {
            return false;
        }

        true
    });
}

fn retain_existing_file_diagnostics(diagnostics: &mut Vec<crate::history::StoredDiagnostic>) {
    let workspace_root = crate::config::workspace_root();
    diagnostics.retain(|diagnostic| diagnostic.points_to_existing_file(&workspace_root));
}

/// Format a file path + line for display (truncates long paths).
fn format_file_loc(path: &Option<String>, line: Option<u32>) -> String {
    match (path, line) {
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
    }
}

/// Format a source_time string to short "HH:MM" display.
fn format_source_short(command: &Option<String>, time: &Option<String>) -> String {
    let cmd = command.as_deref().unwrap_or("-");
    let time_short = time
        .as_ref()
        .and_then(|t| {
            // Parse ISO timestamp and extract HH:MM
            t.get(11..16)
        })
        .unwrap_or("-");
    format!("{cmd} @ {time_short}")
}

fn format_source_with_authority(diagnostic: &crate::history::StoredDiagnostic) -> String {
    let source = format_source_short(&diagnostic.source_command, &diagnostic.source_time);
    if diagnostic.authority == "proof" {
        source
    } else {
        format!("{source}/{}", diagnostic.authority)
    }
}

pub(super) fn diagnostic_source_command_counts(
    diagnostics: &[crate::history::StoredDiagnostic],
) -> Vec<(String, usize)> {
    let mut counts = BTreeMap::<String, usize>::new();
    for diagnostic in diagnostics {
        let command = diagnostic
            .source_command
            .as_deref()
            .unwrap_or("unknown")
            .to_string();
        *counts.entry(command).or_default() += 1;
    }

    let mut counts: Vec<_> = counts.into_iter().collect();
    counts.sort_by(|(left_command, left_count), (right_command, right_count)| {
        right_count
            .cmp(left_count)
            .then_with(|| left_command.cmp(right_command))
    });
    counts
}

pub(super) fn format_diagnostic_source_command_counts(
    diagnostics: &[crate::history::StoredDiagnostic],
) -> String {
    diagnostic_source_command_counts(diagnostics)
        .into_iter()
        .map(|(command, count)| format!("{command}: {count}"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Render diagnostics table with mode-specific columns.
#[allow(
    clippy::needless_pass_by_value,
    reason = "DiagnosticsDisplayMode is Copy"
)]
pub(super) fn render_diagnostics_table(
    diagnostics: &[crate::history::StoredDiagnostic],
    mode: DiagnosticsDisplayMode,
) {
    let mut builder = Builder::new();

    match mode {
        DiagnosticsDisplayMode::Current => {
            builder.push_record(["LEVEL", "PACKAGE", "CODE", "FILE", "MESSAGE", "SOURCE"]);
            for diag in diagnostics {
                let code = diag.code.as_deref().unwrap_or("-");
                let file_loc = format_file_loc(&diag.file_path, diag.line);
                let package = diag.package.as_deref().unwrap_or("-");
                let message = truncate_message(&diag.message, 50);
                let source = format_source_with_authority(diag);
                builder.push_record([
                    diag.level.clone(),
                    package.to_string(),
                    code.to_string(),
                    file_loc,
                    message,
                    source,
                ]);
            }
        }
        DiagnosticsDisplayMode::All | DiagnosticsDisplayMode::Invocation => {
            builder.push_record(["LEVEL", "PACKAGE", "CODE", "FILE", "MESSAGE", "SOURCE"]);
            for diag in diagnostics {
                let code = diag.code.as_deref().unwrap_or("-");
                let file_loc = format_file_loc(&diag.file_path, diag.line);
                let package = diag.package.as_deref().unwrap_or("-");
                let message = truncate_message(&diag.message, 55);
                let source = format_source_with_authority(diag);
                builder.push_record([
                    diag.level.clone(),
                    package.to_string(),
                    code.to_string(),
                    file_loc,
                    message,
                    source,
                ]);
            }
        }
        DiagnosticsDisplayMode::Fixable => {
            builder.push_record(["FILE", "CODE", "FIX", "MESSAGE"]);
            for diag in diagnostics {
                let code = diag.code.as_deref().unwrap_or("-");
                let file_loc = format_file_loc(&diag.file_path, diag.line);
                let fix = diag
                    .fix_replacement
                    .as_deref()
                    .map_or_else(|| "-".to_string(), |r| truncate_message(r, 40));
                let message = truncate_message(&diag.message, 45);
                builder.push_record([file_loc, code.to_string(), fix, message]);
            }
        }
    }

    let mut table = builder.build();
    table.with(Style::rounded());
    println!("{table}");
}

/// Render diagnostics in GCC-compatible format: `file:line:col: level: message [code]`
fn render_diagnostics_gcc(diagnostics: &[crate::history::StoredDiagnostic]) {
    for diag in diagnostics {
        let file = diag.file_path.as_deref().unwrap_or("<unknown>");
        let line = diag.line.unwrap_or(1);
        let col = diag.col.unwrap_or(1);
        let level = &diag.level;
        let msg = &diag.message;
        if let Some(code) = &diag.code {
            println!("{file}:{line}:{col}: {level}: {msg} [{code}]");
        } else {
            println!("{file}:{line}:{col}: {level}: {msg}");
        }
    }
}

pub(super) fn truncate_message(msg: &str, max_len: usize) -> String {
    if msg.len() > max_len {
        format!("{}...", &msg[..max_len.saturating_sub(3)])
    } else {
        msg.to_string()
    }
}

/// Default mode: package-scoped current diagnostics.
pub(super) fn execute_diagnostics_current(
    db: &HistoryDb,
    level: Option<&str>,
    file: Option<&str>,
    command: Option<&str>,
    package: Option<&str>,
    fixable: bool,
    code: Option<&str>,
    format: &DiagnosticsFormat,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let mut diagnostics = db.get_current_diagnostics(level, file, package, command, fixable)?;
    retain_existing_file_diagnostics(&mut diagnostics);
    apply_diagnostic_filters(
        &mut diagnostics,
        DiagnosticFilter::new(level, file, command, package, code, fixable),
    );

    if matches!(format, DiagnosticsFormat::Gcc) {
        render_diagnostics_gcc(&diagnostics);
    } else if ctx.is_human() {
        if diagnostics.is_empty() {
            println!("No current diagnostics.");
            println!(
                "  {}",
                style("(Run `xtask check` to populate diagnostic data)").dim()
            );
        } else {
            let mode = if fixable {
                DiagnosticsDisplayMode::Fixable
            } else {
                DiagnosticsDisplayMode::Current
            };
            println!(
                "Current diagnostics ({} total):",
                style(diagnostics.len()).bold()
            );
            render_diagnostics_table(&diagnostics, mode);
            if command.is_none() && diagnostic_source_command_counts(&diagnostics).len() > 1 {
                println!(
                    "Sources: {}",
                    format_diagnostic_source_command_counts(&diagnostics)
                );
                println!(
                    "  {}",
                    style(
                        "(Use `xtask history diagnostics --command check` to isolate the normal check surface.)"
                    )
                    .dim()
                );
            }
        }
    } else {
        ctx.print_json(&diagnostics)?;
    }

    Ok(CommandResult::success()
        .with_message(format!("Found {} current diagnostics", diagnostics.len()))
        .with_duration(ctx.elapsed()))
}

/// --all mode: raw accumulated diagnostics.
pub(super) fn execute_diagnostics_all(
    db: &HistoryDb,
    limit: usize,
    level: Option<&str>,
    file: Option<&str>,
    command: Option<&str>,
    package: Option<&str>,
    fixable: bool,
    code: Option<&str>,
    format: &DiagnosticsFormat,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let mut diagnostics = db.get_recent_diagnostics_all(limit, level, file, command, package)?;
    apply_diagnostic_filters(
        &mut diagnostics,
        DiagnosticFilter::new(level, file, command, package, code, fixable),
    );

    if matches!(format, DiagnosticsFormat::Gcc) {
        render_diagnostics_gcc(&diagnostics);
    } else if ctx.is_human() {
        if diagnostics.is_empty() {
            println!("No diagnostics found.");
        } else {
            println!(
                "All diagnostics (limit {}, {} shown):",
                limit,
                diagnostics.len()
            );
            render_diagnostics_table(&diagnostics, DiagnosticsDisplayMode::All);
        }
    } else {
        ctx.print_json(&diagnostics)?;
    }

    Ok(CommandResult::success()
        .with_message(format!("Found {} diagnostics", diagnostics.len()))
        .with_duration(ctx.elapsed()))
}

/// --invocation mode: diagnostics from a specific invocation.
pub(super) fn execute_diagnostics_invocation(
    db: &HistoryDb,
    invocation: &str,
    command: Option<&str>,
    level_filter: Option<&str>,
    file_filter: Option<&str>,
    package_filter: Option<&str>,
    fixable_only: bool,
    code_filter: Option<&str>,
    format: &DiagnosticsFormat,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let mut diagnostics = db.get_diagnostics_for_invocation(invocation, command)?;
    apply_diagnostic_filters(
        &mut diagnostics,
        DiagnosticFilter::new(
            level_filter,
            file_filter,
            command,
            package_filter,
            code_filter,
            fixable_only,
        ),
    );

    if matches!(format, DiagnosticsFormat::Gcc) {
        render_diagnostics_gcc(&diagnostics);
    } else if ctx.is_human() {
        let scope = if invocation == "latest" {
            format!("latest {}", command.unwrap_or("any"))
        } else {
            format!("invocation #{invocation}")
        };
        if diagnostics.is_empty() {
            println!("No diagnostics found for {scope}.");
        } else {
            println!("Diagnostics from {scope} ({} total):", diagnostics.len());
            render_diagnostics_table(&diagnostics, DiagnosticsDisplayMode::Invocation);
        }
    } else {
        ctx.print_json(&diagnostics)?;
    }

    Ok(CommandResult::success()
        .with_message(format!(
            "Found {} diagnostics from invocation",
            diagnostics.len()
        ))
        .with_duration(ctx.elapsed()))
}

/// --trend mode: show diagnostic count trend over recent invocations.
pub(super) fn execute_diagnostics_trend(
    db: &HistoryDb,
    window: usize,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let points = db.get_diagnostic_trend(window)?;

    if ctx.is_human() {
        if points.is_empty() {
            println!("No check/build invocations found for trend analysis.");
            println!(
                "  {}",
                style("(Run `xtask check` a few times to build trend data)").dim()
            );
        } else {
            // Compute trend direction
            let (trend_label, trend_dir) = compute_trend_direction(&points);

            println!(
                "Diagnostic trend ({} invocations, {}):",
                style(points.len()).bold(),
                trend_label,
            );
            println!();

            // Header
            println!(
                "  {:>5}  {:>6}  {:>7}  {:>5}  {:>6}  {:>6}  TIME",
                "ID", "CMD", "STATUS", "ERRS", "WARNS", "TOTAL"
            );
            println!("  {}", "─".repeat(60));

            for pt in &points {
                let time_short = pt.started_at.get(11..16).unwrap_or("??:??");
                let date_short = pt.started_at.get(5..10).unwrap_or("??-??");
                let status_label = match pt.status {
                    InvocationStatus::Success => "success",
                    InvocationStatus::Failed => "failed",
                    InvocationStatus::Running => "running",
                    InvocationStatus::Cancelled => "cancelled",
                };
                let status_styled = if matches!(pt.status, InvocationStatus::Success) {
                    style(status_label).green()
                } else {
                    style(status_label).red()
                };
                let errors_styled = if pt.errors > 0 {
                    style(pt.errors.to_string()).red().bold()
                } else {
                    style("0".to_string()).dim()
                };
                let warns_styled = if pt.warnings > 0 {
                    style(pt.warnings.to_string()).yellow()
                } else {
                    style("0".to_string()).dim()
                };

                println!(
                    "  {:>5}  {:>6}  {:>7}  {:>5}  {:>6}  {:>6}  {} {}",
                    pt.invocation_id,
                    pt.command,
                    status_styled,
                    errors_styled,
                    warns_styled,
                    pt.total,
                    date_short,
                    time_short,
                );
            }

            println!();

            // Summary
            if let Some(latest) = points.last() {
                let trend_symbol = match trend_dir {
                    TrendDirection::Improving => style("↓ improving").green(),
                    TrendDirection::Worsening => style("↑ worsening").red(),
                    TrendDirection::Stable => style("→ stable").dim(),
                    TrendDirection::Insufficient => style("? insufficient data").dim(),
                };
                println!(
                    "  Latest: {} errors, {} warnings | Trend: {}",
                    latest.errors, latest.warnings, trend_symbol
                );
            }
        }
    } else {
        // JSON output
        let json_output = serde_json::json!({
            "points": points,
            "count": points.len(),
            "trend": compute_trend_direction(&points).0,
        });
        ctx.print_json(&json_output)?;
    }

    Ok(CommandResult::success()
        .with_message(format!("Showed trend for {} invocations", points.len()))
        .with_duration(ctx.elapsed()))
}

enum TrendDirection {
    Improving,
    Worsening,
    Stable,
    Insufficient,
}

/// Compute trend direction by comparing older half vs recent half of invocations.
fn compute_trend_direction(
    points: &[crate::history::DiagnosticTrendPoint],
) -> (String, TrendDirection) {
    if points.len() < 4 {
        return (
            "insufficient data".to_string(),
            TrendDirection::Insufficient,
        );
    }

    let mid = points.len() / 2;
    let older = &points[..mid];
    let recent = &points[mid..];

    let older_avg = older.iter().map(|p| p.total).sum::<usize>() as f64 / older.len() as f64;
    let recent_avg = recent.iter().map(|p| p.total).sum::<usize>() as f64 / recent.len() as f64;

    if older_avg == 0.0 && recent_avg == 0.0 {
        return ("stable (clean)".to_string(), TrendDirection::Stable);
    }

    let pct_change = if older_avg > 0.0 {
        ((recent_avg - older_avg) / older_avg) * 100.0
    } else {
        100.0 // went from 0 to something
    };

    if pct_change > 15.0 {
        (
            format!("worsening (+{pct_change:.0}%)"),
            TrendDirection::Worsening,
        )
    } else if pct_change < -15.0 {
        (
            format!("improving ({pct_change:.0}%)"),
            TrendDirection::Improving,
        )
    } else {
        ("stable".to_string(), TrendDirection::Stable)
    }
}

// ─── G1: Diagnostic Delta ────────────────────────────────────────────────────

/// Show new/resolved/persistent diagnostics between two invocations (G1).
pub(super) fn execute_diagnostics_delta(
    db: &HistoryDb,
    delta_from: Option<i64>,
    delta_to: Option<i64>,
    level: Option<&str>,
    file: Option<&str>,
    command: Option<&str>,
    package: Option<&str>,
    fixable: bool,
    code: Option<&str>,
    format: &DiagnosticsFormat,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    // Resolve to/from invocation IDs
    let to_id: i64 = if let Some(id) = delta_to {
        id
    } else if let Some(cmd) = command {
        db.get_last(cmd)?
            .map(|inv| inv.id)
            .ok_or_else(|| color_eyre::eyre::eyre!("No recent {cmd} invocation found"))?
    } else {
        let check_last = db.get_last("check")?.map(|inv| inv.id);
        resolve_default_diagnostics_delta_target(
            check_last,
            db.get_last("build").map(|inv| inv.map(|inv| inv.id)),
        )?
    };

    let from_id: i64 = if let Some(id) = delta_from {
        id
    } else {
        // Find the invocation before `to_id` for the same command
        let inv = db.get_recent(50, None)?;
        inv.into_iter()
            .find(|i| {
                i.id < to_id
                    && command.is_none_or(|cmd| i.command == cmd)
                    && matches!(
                        i.status,
                        InvocationStatus::Success | InvocationStatus::Failed
                    )
            })
            .map(|i| i.id)
            .ok_or_else(|| {
                color_eyre::eyre::eyre!("No previous invocation found to compare against")
            })?
    };

    let mut delta = db.get_diagnostic_delta(from_id, to_id)?;
    let filter = DiagnosticFilter::new(level, file, command, package, code, fixable);
    apply_diagnostic_filters(&mut delta.new, filter);
    apply_diagnostic_filters(&mut delta.resolved, filter);
    apply_diagnostic_filters(&mut delta.persistent, filter);

    if matches!(format, DiagnosticsFormat::Gcc) {
        // GCC mode: prefix new/resolved
        for d in &delta.new {
            if let (Some(path), Some(line)) = (&d.file_path, d.line) {
                println!(
                    "{}:{}:{}:NEW {} {}",
                    path,
                    line,
                    d.col.unwrap_or(0),
                    d.level,
                    d.message
                );
            }
        }
        for d in &delta.resolved {
            if let (Some(path), Some(line)) = (&d.file_path, d.line) {
                println!(
                    "{}:{}:{}:RESOLVED {} {}",
                    path,
                    line,
                    d.col.unwrap_or(0),
                    d.level,
                    d.message
                );
            }
        }
    } else if ctx.is_human() {
        println!(
            "Diagnostic delta: invocation {} → {} ({} new, {} resolved, {} persistent)",
            from_id,
            to_id,
            style(delta.new.len()).green().bold(),
            style(delta.resolved.len()).red().bold(),
            delta.persistent.len(),
        );

        if !delta.new.is_empty() {
            println!("\n{}", style("NEW (appeared):").green().bold());
            render_diagnostics_table(&delta.new, DiagnosticsDisplayMode::Current);
        }
        if !delta.resolved.is_empty() {
            println!("\n{}", style("RESOLVED (fixed):").cyan().bold());
            render_diagnostics_table(&delta.resolved, DiagnosticsDisplayMode::Current);
        }
        if delta.new.is_empty() && delta.resolved.is_empty() {
            println!("\n{}", style("No changes — diagnostics are stable.").dim());
        }
    } else {
        ctx.print_json(&delta)?;
    }

    Ok(CommandResult::success()
        .with_message(format!(
            "Delta: {} new, {} resolved, {} persistent",
            delta.new.len(),
            delta.resolved.len(),
            delta.persistent.len()
        ))
        .with_duration(ctx.elapsed()))
}

pub(super) fn resolve_default_diagnostics_delta_target(
    check_last: Option<i64>,
    build_last: Result<Option<i64>>,
) -> Result<i64> {
    if let Some(id) = check_last {
        return Ok(id);
    }

    if let Some(id) = build_last.wrap_err(
        "failed to read most recent build invocation while resolving diagnostics delta target",
    )? {
        return Ok(id);
    }

    Err(color_eyre::eyre::eyre!(
        "No recent check/build invocation found"
    ))
}

/// Group current diagnostics by error code (G1 --by-code).
pub(super) fn execute_diagnostics_by_code(
    db: &HistoryDb,
    level: Option<&str>,
    file: Option<&str>,
    command: Option<&str>,
    package: Option<&str>,
    fixable: bool,
    code: Option<&str>,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let mut diagnostics = db.get_current_diagnostics(level, file, package, command, fixable)?;
    retain_existing_file_diagnostics(&mut diagnostics);
    apply_diagnostic_filters(
        &mut diagnostics,
        DiagnosticFilter::new(level, file, command, package, code, fixable),
    );

    // Group by code
    let mut by_code: std::collections::BTreeMap<String, Vec<&crate::history::StoredDiagnostic>> =
        std::collections::BTreeMap::new();
    for d in &diagnostics {
        let key = d.code.clone().unwrap_or_else(|| "(no code)".into());
        by_code.entry(key).or_default().push(d);
    }

    if ctx.is_human() {
        if by_code.is_empty() {
            println!("No current diagnostics.");
        } else {
            for (code, diags) in &by_code {
                println!(
                    "{} — {} occurrence{}",
                    style(code).yellow().bold(),
                    diags.len(),
                    if diags.len() == 1 { "" } else { "s" }
                );
                for d in diags.iter().take(3) {
                    let loc = d
                        .file_path
                        .as_deref()
                        .map(|p| {
                            if let Some(line) = d.line {
                                format!(" @ {p}:{line}")
                            } else {
                                format!(" @ {p}")
                            }
                        })
                        .unwrap_or_default();
                    println!(
                        "  {} {}{}",
                        style(&d.level).dim(),
                        d.message,
                        style(loc).dim()
                    );
                }
                if diags.len() > 3 {
                    println!("  {} …and {} more", style("").dim(), diags.len() - 3);
                }
            }
        }
    } else {
        let grouped: Vec<serde_json::Value> = by_code
            .iter()
            .map(|(code, diags)| {
                serde_json::json!({
                    "code": code,
                    "count": diags.len(),
                    "diagnostics": diags,
                })
            })
            .collect();
        ctx.print_json(&grouped)?;
    }

    Ok(CommandResult::success()
        .with_message(format!("{} unique codes", by_code.len()))
        .with_duration(ctx.elapsed()))
}
