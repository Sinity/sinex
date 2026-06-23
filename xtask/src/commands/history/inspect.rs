use super::*;

pub(super) fn execute_query(
    db: &HistoryDb,
    sql: &str,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let rows = db.run_readonly_query(sql).wrap_err("query failed")?;

    if ctx.is_human() {
        if rows.is_empty() {
            println!("(no rows)");
        } else {
            // Extract column names from first row
            let cols: Vec<&str> = rows[0].keys().map(String::as_str).collect();
            let mut builder = Builder::new();
            builder.push_record(cols.clone());
            for row in &rows {
                let record: Vec<String> = cols
                    .iter()
                    .map(|c| {
                        row.get(*c)
                            .map(|v| match v {
                                serde_json::Value::Null => "-".to_string(),
                                serde_json::Value::String(s) => s.clone(),
                                other => other.to_string(),
                            })
                            .unwrap_or_default()
                    })
                    .collect();
                builder.push_record(record);
            }
            let mut table = builder.build();
            table.with(Style::rounded());
            println!("{table}");
        }
    } else {
        ctx.print_json(&rows)?;
    }

    Ok(CommandResult::success()
        .with_message(format!("{} rows", rows.len()))
        .with_duration(ctx.elapsed()))
}

/// I2: Open an interactive SQLite shell on the history database.
pub(super) fn execute_shell(_db: &HistoryDb, ctx: &CommandContext) -> Result<CommandResult> {
    let db_path = ctx.history_db_path();
    if !db_path.exists() {
        return Err(color_eyre::eyre::eyre!(
            "History database not found at {}. Run a command first.",
            db_path.display()
        ));
    }

    // Check sqlite3 is available
    ensure_sqlite3_available(std::process::Command::new("which").arg("sqlite3").output())?;

    if ctx.is_human() {
        println!("Opening history database: {}", db_path.display());
        println!("Type .tables to list tables, .schema <table> for schema, .quit to exit.");
    }

    let status = std::process::Command::new("sqlite3")
        .arg(db_path)
        .status()
        .wrap_err("failed to launch sqlite3")?;

    let exit_code = status.code().unwrap_or(-1);
    Ok(CommandResult::success()
        .with_message(format!("sqlite3 exited with code {exit_code}"))
        .with_duration(ctx.elapsed()))
}

pub(super) fn ensure_sqlite3_available(probe: std::io::Result<std::process::Output>) -> Result<()> {
    match probe {
        Ok(output) if output.status.success() => Ok(()),
        Ok(output) => {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let detail = stderr.trim();
            let suffix = if detail.is_empty() {
                String::new()
            } else {
                format!(" ({detail})")
            };
            Err(color_eyre::eyre::eyre!(
                "sqlite3 is not available on PATH{suffix}. Provide it via the devshell or system configuration"
            ))
        }
        Err(error) => Err(color_eyre::eyre::eyre!(
            "failed to probe sqlite3 availability: {error}"
        )),
    }
}

/// I2: Dump annotated schema CREATE TABLE statements.
pub(super) fn execute_schema(db: &HistoryDb, ctx: &CommandContext) -> Result<CommandResult> {
    let tables = db.get_schema_dump()?;

    if ctx.is_human() {
        if tables.is_empty() {
            println!("No tables found.");
        } else {
            for (name, sql) in &tables {
                println!("-- Table: {name}");
                println!("{sql};\n");
            }
        }
    } else {
        let json: Vec<_> = tables
            .iter()
            .map(|(n, s)| serde_json::json!({"name": n, "sql": s}))
            .collect();
        ctx.print_json(&json)?;
    }

    Ok(CommandResult::success()
        .with_message(format!("{} tables", tables.len()))
        .with_duration(ctx.elapsed()))
}

/// I4: Cross-invocation timeline.
pub(super) fn execute_timeline(
    db: &HistoryDb,
    command: Option<&str>,
    days: u32,
    limit: usize,
    include_zombies: bool,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let entries = db.get_invocation_timeline_with_zombies(command, days, limit, include_zombies)?;

    if ctx.is_human() {
        if entries.is_empty() {
            println!("No invocation history found for the last {days} days.");
        } else {
            render_timeline_table(&entries);
        }
    } else {
        ctx.print_json(&entries)?;
    }

    Ok(CommandResult::success()
        .with_message(format!("{} timeline entries ({}d)", entries.len(), days))
        .with_duration(ctx.elapsed()))
}

fn render_timeline_table(entries: &[InvocationTimelineEntry]) {
    let mut builder = Builder::new();
    builder.push_record([
        "ID", "COMMAND", "STATUS", "STARTED", "DURATION", "STAGES", "ERRORS", "WARNS", "ΔDIAG",
    ]);
    for e in entries {
        let status = match e.status {
            InvocationStatus::Success => style("success".to_string()).green().to_string(),
            InvocationStatus::Failed => style("failed".to_string()).red().to_string(),
            InvocationStatus::Cancelled => style("cancelled".to_string()).dim().to_string(),
            InvocationStatus::Running => style("running".to_string()).yellow().to_string(),
        };
        let duration = e
            .duration_secs
            .map_or_else(|| "-".into(), |d| format!("{d:.1}s"));
        let delta = match e.diagnostic_delta.cmp(&0) {
            std::cmp::Ordering::Equal => "—".to_string(),
            std::cmp::Ordering::Greater => {
                style(format!("+{}", e.diagnostic_delta)).red().to_string()
            }
            std::cmp::Ordering::Less => {
                style(format!("{}", e.diagnostic_delta)).green().to_string()
            }
        };
        builder.push_record([
            e.id.to_string(),
            e.command.clone(),
            status,
            e.started_at[..16].to_string(), // trim to YYYY-MM-DDTHH:MM
            duration,
            e.stage_count.to_string(),
            e.error_count.to_string(),
            e.warning_count.to_string(),
            delta,
        ]);
    }
    let mut table = builder.build();
    table.with(Style::rounded());
    println!("{table}");
}

/// I5: Compare two invocations.
pub(super) fn execute_diff(
    db: &HistoryDb,
    from: Option<i64>,
    to: Option<i64>,
    command: Option<&str>,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let to_id = match to {
        Some(id) => id,
        None => db
            .resolve_invocation_id("latest", command)?
            .ok_or_else(|| color_eyre::eyre::eyre!("No completed invocations found"))?,
    };
    let from_id = match from {
        Some(id) => id,
        None => db
            .get_previous_invocation_id(to_id, command)?
            .ok_or_else(|| {
                color_eyre::eyre::eyre!(
                    "No previous invocation found to diff against. Use --from <id>."
                )
            })?,
    };

    let from_full = db
        .get_invocation_full(from_id)?
        .ok_or_else(|| color_eyre::eyre::eyre!("Invocation #{from_id} not found"))?;
    let to_full = db
        .get_invocation_full(to_id)?
        .ok_or_else(|| color_eyre::eyre::eyre!("Invocation #{to_id} not found"))?;
    let delta = db.get_diagnostic_delta(from_id, to_id)?;

    let from_dur = from_full.invocation.duration_secs.unwrap_or(0.0);
    let to_dur = to_full.invocation.duration_secs.unwrap_or(0.0);

    if ctx.is_human() {
        println!(
            "Diff: #{from_id} ({}) → #{to_id} ({})",
            from_full.invocation.command, to_full.invocation.command
        );
        println!();
        let dur_delta = to_dur - from_dur;
        let dur_style = if dur_delta > 1.0 {
            style(format!("{dur_delta:+.1}s")).red().to_string()
        } else if dur_delta < -1.0 {
            style(format!("{dur_delta:+.1}s")).green().to_string()
        } else {
            format!("{dur_delta:+.1}s")
        };
        println!("  Duration: {from_dur:.1}s → {to_dur:.1}s ({dur_style})");
        println!(
            "  Stages:   {} → {}",
            from_full.stages.len(),
            to_full.stages.len()
        );
        println!(
            "  Diagnostics: {} → {} (new: {}, resolved: {}, persistent: {})",
            from_full.diagnostics.len(),
            to_full.diagnostics.len(),
            style(delta.new.len()).red(),
            style(delta.resolved.len()).green(),
            delta.persistent.len(),
        );

        if !delta.new.is_empty() {
            println!("\n  New diagnostics (+{}):", delta.new.len());
            render_diagnostics_table(&delta.new, DiagnosticsDisplayMode::All);
        }
        if !delta.resolved.is_empty() {
            println!("\n  Resolved diagnostics (-{}):", delta.resolved.len());
            render_diagnostics_table(&delta.resolved, DiagnosticsDisplayMode::All);
        }
    } else {
        let json = serde_json::json!({
            "from": { "id": from_id, "duration_secs": from_dur },
            "to": { "id": to_id, "duration_secs": to_dur },
            "duration_delta_secs": to_dur - from_dur,
            "stage_delta": to_full.stages.len() as i64 - from_full.stages.len() as i64,
            "new_diagnostics": delta.new,
            "resolved_diagnostics": delta.resolved,
            "persistent_diagnostics": delta.persistent,
        });
        ctx.print_json(&json)?;
    }

    Ok(CommandResult::success()
        .with_message(format!(
            "Diff #{from_id}→#{to_id}: +{} -{}",
            delta.new.len(),
            delta.resolved.len()
        ))
        .with_duration(ctx.elapsed()))
}

/// I6: Working session grouping.
pub(super) fn execute_sessions(
    db: &HistoryDb,
    limit: usize,
    gap_minutes: u32,
    include_zombies: bool,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let sessions = db.get_working_sessions_with_zombies(limit, gap_minutes, include_zombies)?;

    if ctx.is_human() {
        if sessions.is_empty() {
            println!("No working sessions found.");
        } else {
            println!(
                "Working sessions (gap > {gap_minutes}min, showing {}):",
                sessions.len()
            );
            let mut builder = Builder::new();
            builder.push_record([
                "#",
                "STARTED",
                "INVOCATIONS",
                "DURATION",
                "SUCCESS",
                "COMMANDS",
            ]);
            for s in &sessions {
                let duration = format!("{:.0}s", s.total_duration_secs);
                let rate = if s.invocation_count > 0 {
                    format!("{}/{} ok", s.success_count, s.invocation_count)
                } else {
                    "-".into()
                };
                let cmds = s.commands.join(", ");
                builder.push_record([
                    s.session_index.to_string(),
                    super::format_display_time_str(&s.first_started),
                    s.invocation_count.to_string(),
                    duration,
                    rate,
                    cmds,
                ]);
            }
            let mut table = builder.build();
            table.with(Style::rounded());
            println!("{table}");
        }
    } else {
        ctx.print_json(&sessions)?;
    }

    Ok(CommandResult::success()
        .with_message(format!("{} sessions", sessions.len()))
        .with_duration(ctx.elapsed()))
}

/// I7: Full single-invocation details.
pub(super) fn execute_invocation(
    db: &HistoryDb,
    id: &str,
    full: bool,
    command: Option<&str>,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let inv_id = db
        .resolve_invocation_id(id, command)?
        .ok_or_else(|| color_eyre::eyre::eyre!("No invocation found for '{id}'"))?;

    let inv_full = db
        .get_invocation_full(inv_id)?
        .ok_or_else(|| color_eyre::eyre::eyre!("Invocation #{inv_id} not found"))?;
    let resource_usage = match db.get_resource_usage_for_invocation(inv_id)? {
        Some(usage) => Some(usage),
        None if matches!(inv_full.invocation.status, InvocationStatus::Running) => db
            .get_running_job_pid_for_invocation(inv_id)?
            .and_then(|pid| live_resource_usage_for_invocation(&inv_full.invocation, pid)),
        None => None,
    };

    if ctx.is_human() {
        let inv = &inv_full.invocation;
        let status_str = match inv.status {
            InvocationStatus::Success => style("success").green().to_string(),
            InvocationStatus::Failed => style("failed").red().to_string(),
            InvocationStatus::Cancelled => style("cancelled").dim().to_string(),
            InvocationStatus::Running => style("running").yellow().to_string(),
        };
        println!("Invocation #{}", inv.id);
        println!("  Command:  {}", inv.command);
        println!("  Status:   {status_str}");
        println!(
            "  Started:  {}",
            super::format_display_time(&inv.started_at)
        );
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
        println!(
            "  Diagnostics: {}E {}W",
            inv_full.error_count, inv_full.warning_count
        );
        println!("  Stages:   {}", inv_full.stages.len());
        if let Some(resources) = &resource_usage {
            println!("  Resources: {}", format_resource_usage(resources));
        }

        if full {
            if !inv_full.stages.is_empty() {
                println!("\n  Stage timings:");
                let mut builder = Builder::new();
                builder.push_record(["STAGE", "DURATION", "OK"]);
                for s in &inv_full.stages {
                    builder.push_record([
                        s.stage_name.clone(),
                        format!("{:.2}s", s.duration_secs),
                        if s.success {
                            "✓".to_string()
                        } else {
                            "✗".to_string()
                        },
                    ]);
                }
                let mut table = builder.build();
                table.with(Style::rounded());
                println!("{table}");
            }

            if !inv_full.diagnostics.is_empty() {
                println!("\n  Diagnostics ({}):", inv_full.diagnostics.len());
                render_diagnostics_table(&inv_full.diagnostics, DiagnosticsDisplayMode::Invocation);
            }
        }
    } else if full {
        ctx.print_json(&serde_json::json!({
            "invocation": inv_full.invocation,
            "stages": inv_full.stages,
            "diagnostics": inv_full.diagnostics,
            "error_count": inv_full.error_count,
            "warning_count": inv_full.warning_count,
            "resource_usage": resource_usage,
        }))?;
    } else {
        ctx.print_json(&inv_full.invocation)?;
    }

    Ok(CommandResult::success()
        .with_message(format!("Invocation #{inv_id}"))
        .with_duration(ctx.elapsed()))
}

fn live_resource_usage_for_invocation(
    invocation: &crate::history::Invocation,
    pid: u32,
) -> Option<ResourceUsage> {
    let process_metrics =
        crate::process::probe_process_tree_metrics(pid, Duration::from_millis(120));
    let shared_build_metrics =
        crate::process::probe_shared_build_metrics(Duration::from_millis(120));

    match (process_metrics, shared_build_metrics) {
        (Some(metrics), shared_build_metrics) => Some(ResourceUsage {
            command: invocation.command.clone(),
            status: invocation.status.as_str().to_string(),
            started_at: invocation.started_at.to_string(),
            duration_secs: Some(
                (time::OffsetDateTime::now_utc() - invocation.started_at).as_seconds_f64(),
            ),
            process_cpu_usage_avg: metrics.cpu_usage_avg,
            process_memory_usage_max_mb: metrics.memory_usage_max_mb,
            root_process_cpu_usage_avg: metrics.root_cpu_usage_avg,
            root_process_memory_usage_max_mb: metrics.root_memory_usage_max_mb,
            shared_nix_daemon_cpu_usage_avg: shared_build_metrics
                .as_ref()
                .and_then(|metrics| metrics.shared_nix_daemon_cpu_usage_avg),
            shared_nix_daemon_memory_usage_max_mb: shared_build_metrics
                .as_ref()
                .and_then(|metrics| metrics.shared_nix_daemon_memory_usage_max_mb),
            shared_nix_build_slice_cpu_usage_avg: shared_build_metrics
                .as_ref()
                .and_then(|metrics| metrics.shared_nix_build_slice_cpu_usage_avg),
            shared_nix_build_slice_memory_usage_max_mb: shared_build_metrics
                .as_ref()
                .and_then(|metrics| metrics.shared_nix_build_slice_memory_usage_max_mb),
            shared_background_slice_cpu_usage_avg: shared_build_metrics
                .as_ref()
                .and_then(|metrics| metrics.shared_background_slice_cpu_usage_avg),
            shared_background_slice_memory_usage_max_mb: shared_build_metrics
                .as_ref()
                .and_then(|metrics| metrics.shared_background_slice_memory_usage_max_mb),
            process_count_max: metrics.process_count_max,
            sample_count: Some(metrics.sample_count),
            host_cpu_usage_avg: None,
            host_memory_usage_max_mb: None,
            host_cpu_pressure_some_avg10_max: None,
            host_io_pressure_some_avg10_max: None,
            host_io_pressure_full_avg10_max: None,
            host_memory_pressure_some_avg10_max: None,
            host_memory_pressure_full_avg10_max: None,
            host_block_read_mib_delta: None,
            host_block_write_mib_delta: None,
            host_block_read_iops_avg: None,
            host_block_write_iops_avg: None,
            host_block_busiest_device: None,
            host_block_busiest_device_total_mib_delta: None,
            host_block_busiest_device_read_iops_avg: None,
            host_block_busiest_device_write_iops_avg: None,
            host_block_busiest_device_weighted_io_ms_per_s: None,
            shm_free_min_mb: None,
            shm_used_max_mb: None,
        }),
        (None, Some(shared_build_metrics)) => Some(ResourceUsage {
            command: invocation.command.clone(),
            status: invocation.status.as_str().to_string(),
            started_at: invocation.started_at.to_string(),
            duration_secs: Some(
                (time::OffsetDateTime::now_utc() - invocation.started_at).as_seconds_f64(),
            ),
            process_cpu_usage_avg: None,
            process_memory_usage_max_mb: None,
            root_process_cpu_usage_avg: None,
            root_process_memory_usage_max_mb: None,
            shared_nix_daemon_cpu_usage_avg: shared_build_metrics.shared_nix_daemon_cpu_usage_avg,
            shared_nix_daemon_memory_usage_max_mb: shared_build_metrics
                .shared_nix_daemon_memory_usage_max_mb,
            shared_nix_build_slice_cpu_usage_avg: shared_build_metrics
                .shared_nix_build_slice_cpu_usage_avg,
            shared_nix_build_slice_memory_usage_max_mb: shared_build_metrics
                .shared_nix_build_slice_memory_usage_max_mb,
            shared_background_slice_cpu_usage_avg: shared_build_metrics
                .shared_background_slice_cpu_usage_avg,
            shared_background_slice_memory_usage_max_mb: shared_build_metrics
                .shared_background_slice_memory_usage_max_mb,
            process_count_max: None,
            sample_count: None,
            host_cpu_usage_avg: None,
            host_memory_usage_max_mb: None,
            host_cpu_pressure_some_avg10_max: None,
            host_io_pressure_some_avg10_max: None,
            host_io_pressure_full_avg10_max: None,
            host_memory_pressure_some_avg10_max: None,
            host_memory_pressure_full_avg10_max: None,
            host_block_read_mib_delta: None,
            host_block_write_mib_delta: None,
            host_block_read_iops_avg: None,
            host_block_write_iops_avg: None,
            host_block_busiest_device: None,
            host_block_busiest_device_total_mib_delta: None,
            host_block_busiest_device_read_iops_avg: None,
            host_block_busiest_device_write_iops_avg: None,
            host_block_busiest_device_weighted_io_ms_per_s: None,
            shm_free_min_mb: None,
            shm_used_max_mb: None,
        }),
        (None, None) => None,
    }
}

fn format_resource_usage(usage: &ResourceUsage) -> String {
    let cpu = if let Some(value) = usage.process_cpu_usage_avg {
        format!("{value:.1}% tree cpu")
    } else if let Some(value) = usage.host_cpu_usage_avg {
        format!("{value:.1}% host cpu (legacy)")
    } else {
        "cpu n/a".to_string()
    };
    let memory = if let Some(value) = usage.process_memory_usage_max_mb {
        format!("{value:.0} MB tree mem")
    } else if let Some(value) = usage.host_memory_usage_max_mb {
        format!("{value:.0} MB host mem (legacy)")
    } else {
        "mem n/a".to_string()
    };
    let process_count = usage.process_count_max.map_or_else(
        || "proc n/a".to_string(),
        |count| format!("max {count} proc"),
    );
    let root_cpu = usage.root_process_cpu_usage_avg.map_or_else(
        || "xtask cpu n/a".to_string(),
        |value| format!("{value:.1}% xtask cpu"),
    );
    let root_mem = usage.root_process_memory_usage_max_mb.map_or_else(
        || "xtask mem n/a".to_string(),
        |value| format!("{value:.0} MB xtask mem"),
    );
    let samples = usage.sample_count.map_or_else(
        || "samples n/a".to_string(),
        |count| format!("{count} samples"),
    );
    let mut parts = vec![cpu, memory];
    if let Some(cpu) = usage.shared_nix_daemon_cpu_usage_avg {
        parts.push(format!("{cpu:.1}% nix-daemon shared cpu"));
    }
    if let Some(memory) = usage.shared_nix_daemon_memory_usage_max_mb {
        parts.push(format!("{memory:.0} MB nix-daemon shared mem"));
    }
    if let Some(cpu) = usage.shared_nix_build_slice_cpu_usage_avg {
        parts.push(format!("{cpu:.1}% nix-build shared cpu"));
    }
    if let Some(memory) = usage.shared_nix_build_slice_memory_usage_max_mb {
        parts.push(format!("{memory:.0} MB nix-build shared mem"));
    }
    if let Some(cpu) = usage.shared_background_slice_cpu_usage_avg {
        parts.push(format!("{cpu:.1}% background shared cpu"));
    }
    if let Some(memory) = usage.shared_background_slice_memory_usage_max_mb {
        parts.push(format!("{memory:.0} MB background shared mem"));
    }
    parts.extend([process_count, root_cpu, root_mem, samples]);
    parts.join(", ")
}

pub(super) fn execute_seed(
    db: &HistoryDb,
    days: u32,
    invocations: u32,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    use crate::history::seed::{SeedOptions, seed_history};

    let opts = SeedOptions { days, invocations };

    if ctx.is_human() {
        println!(
            "Seeding history database with {invocations} synthetic invocations over {days} days…"
        );
    }

    seed_history(db, &opts)?;

    let db_path = ctx.history_db_path();
    if ctx.is_human() {
        println!("  ✓ Done. Database: {}", db_path.display());
        println!("  The database is now marked synthetic.");
        println!("  History commands will warn until real runs replace this data.");
        println!("  To clear: xtask reset --yes --history");
    }

    Ok(CommandResult::success()
        .with_message(format!("Seeded {invocations} invocations over {days} days"))
        .with_duration(ctx.elapsed())
        .with_data(serde_json::json!({
            "days": days,
            "invocations": invocations,
            "db_path": db_path.display().to_string(),
            "synthetic": true,
        })))
}

/// Show live/final progress for an invocation.
pub(super) fn execute_progress(
    db: &HistoryDb,
    invocation: Option<&str>,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let selector = invocation.unwrap_or("current");
    let inv_id = db
        .resolve_invocation_id(selector, None)?
        .ok_or_else(|| color_eyre::eyre::eyre!("No invocation found for selector '{selector}'"))?;

    let progress = db.get_progress(inv_id)?;

    if ctx.is_human() {
        match &progress {
            Some(p) => {
                println!("Progress for invocation #{inv_id}:");
                println!("  Phase:   {}", p.phase.as_deref().unwrap_or("(unknown)"));
                if let Some(step) = &p.step {
                    println!("  Step:    {step}");
                }
                if let Some(pct) = p.pct_done {
                    println!("  Done:    {pct:.1}%");
                }
                if let (Some(done), Some(total)) = (p.items_done, p.items_total) {
                    println!("  Items:   {done}/{total}");
                } else if let Some(done) = p.items_done {
                    println!("  Items:   {done} done");
                }
                println!("  Updated: {}", p.updated_at);
            }
            None => {
                println!("No progress data for invocation #{inv_id}.");
            }
        }
    } else {
        ctx.print_json(&progress)?;
    }

    Ok(CommandResult::success()
        .with_message(format!("Progress for invocation #{inv_id}"))
        .with_duration(ctx.elapsed()))
}

/// Show ETA estimates for a command based on recorded phase timings.
pub(super) fn execute_eta(
    db: &HistoryDb,
    command: &str,
    phase: Option<&str>,
    window: usize,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    if let Some(phase_name) = phase {
        // Single phase estimate
        let estimate = db.get_eta_estimate(command, phase_name, window)?;
        if ctx.is_human() {
            match estimate {
                Some(secs) => {
                    println!(
                        "ETA for '{command}' phase '{phase_name}': {secs:.1}s  (median of recent samples)"
                    );
                }
                None => {
                    println!(
                        "No ETA for '{command}' phase '{phase_name}' — fewer than 3 samples recorded."
                    );
                }
            }
        } else {
            let json = serde_json::json!({
                "command": command,
                "phase": phase_name,
                "median_secs": estimate,
                "window": window,
            });
            ctx.print_json(&json)?;
        }
        Ok(CommandResult::success()
            .with_message(format!(
                "ETA for '{command}' phase '{phase_name}': {}",
                estimate.map_or_else(|| "n/a".into(), |s| format!("{s:.1}s"))
            ))
            .with_duration(ctx.elapsed()))
    } else {
        // All phases for command
        let phases = db.get_eta_phases(command)?;
        if ctx.is_human() {
            if phases.is_empty() {
                println!("No ETA samples recorded for command '{command}'.");
                println!(
                    "  {}",
                    style("(ETA data is recorded as commands complete stages)").dim()
                );
            } else {
                println!("ETA estimates for '{command}':");
                let mut builder = Builder::new();
                builder.push_record(["PHASE", "MEDIAN (s)", "SAMPLES"]);
                for (phase_name, median, count) in &phases {
                    let median_str = median.map_or_else(|| "n/a".into(), |s| format!("{s:.1}"));
                    let count_str = if *count < 3 {
                        format!("{count} (need 3+)")
                    } else {
                        count.to_string()
                    };
                    builder.push_record([phase_name.clone(), median_str, count_str]);
                }
                let mut table = builder.build();
                table.with(Style::rounded());
                println!("{table}");
            }
        } else {
            let json: Vec<serde_json::Value> = phases
                .iter()
                .map(|(phase_name, median, count)| {
                    serde_json::json!({
                        "phase": phase_name,
                        "median_secs": median,
                        "sample_count": count,
                    })
                })
                .collect();
            ctx.print_json(&serde_json::json!({
                "command": command,
                "phases": json,
            }))?;
        }
        Ok(CommandResult::success()
            .with_message(format!("ETA phases for '{command}': {}", phases.len()))
            .with_duration(ctx.elapsed()))
    }
}

pub(super) fn execute_exercise_history(
    db: &HistoryDb,
    limit: usize,
    verbose: bool,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let rows = db.get_exercise_runs(limit)?;
    let mut warnings = Vec::new();

    if ctx.is_json() {
        let mut json_runs = Vec::with_capacity(rows.len());
        for row in &rows {
            let mut run = serde_json::json!({
                "run_id": row.run_id,
                "invocation_id": row.invocation_id,
                "tier": row.tier,
                "total": row.total,
                "passed": row.passed,
                "failed": row.failed,
                "skipped": row.skipped,
                "duration_secs": row.duration_secs,
                "recorded_at": row.recorded_at,
                "invocation_status": row.invocation_status,
                "git_commit": row.git_commit,
            });
            if verbose {
                let results_probe = exercise_results_probe_from_result(
                    row.run_id,
                    db.get_exercise_results_for_run(row.run_id),
                );
                if let Some(issue) = &results_probe.issue {
                    warnings.push(issue.clone());
                    run["results_issue"] = serde_json::Value::String(issue.clone());
                }
                let results = results_probe
                    .results
                    .into_iter()
                    .map(|r| {
                        serde_json::json!({
                            "exercise_id": r.exercise_id,
                            "tier": r.exercise_tier,
                            "passed": r.passed,
                            "duration_secs": r.duration_secs,
                            "error": r.error,
                            "step_count": r.step_count,
                        })
                    })
                    .collect::<Vec<_>>();
                run["results"] = serde_json::Value::Array(results);
            }
            json_runs.push(run);
        }
        ctx.print_json(&serde_json::json!({ "runs": json_runs }))?;
    } else {
        if rows.is_empty() {
            println!("No exercise runs recorded yet. Run `xtask exercise` first.");
            return Ok(CommandResult::success()
                .with_message("no exercise runs found")
                .with_duration(ctx.elapsed()));
        }

        let mut builder = Builder::new();
        builder.push_record(["WHEN", "TIER", "PASS", "FAIL", "SKIP", "DUR", "STATUS"]);

        let mut prev_passed_all = true;
        for (i, row) in rows.iter().enumerate() {
            let tier_str = row.tier.as_deref().unwrap_or("mixed");
            let regression = i > 0 && row.failed > 0 && prev_passed_all;
            let status = if row.failed == 0 {
                style("✓ green").green().to_string()
            } else if regression {
                style("↓ regressed").red().bold().to_string()
            } else {
                style("✗ failing").red().to_string()
            };
            let when: String = row.recorded_at.chars().take(16).collect();
            builder.push_record([
                when,
                tier_str.to_string(),
                row.passed.to_string(),
                row.failed.to_string(),
                row.skipped.to_string(),
                format!("{:.1}s", row.duration_secs),
                status,
            ]);
            prev_passed_all = row.total == row.passed;
        }
        let mut table = builder.build();
        table.with(Style::rounded());
        println!("{table}");

        if verbose {
            for row in &rows {
                if row.failed > 0 {
                    println!("\nFailed exercises in run {}:", row.recorded_at);
                    let results_probe = exercise_results_probe_from_result(
                        row.run_id,
                        db.get_exercise_results_for_run(row.run_id),
                    );
                    if let Some(issue) = &results_probe.issue {
                        warnings.push(issue.clone());
                        println!("  {}", style(issue).yellow());
                        continue;
                    }
                    for r in results_probe.results.into_iter().filter(|r| !r.passed) {
                        let err_str = r.error.as_deref().unwrap_or("(no error)");
                        println!("  {} {}: {err_str}", style("✗").red(), r.exercise_id);
                    }
                }
            }
        }
    }

    let mut result = CommandResult::success()
        .with_message(format!("{} exercise run(s) shown", rows.len()))
        .with_duration(ctx.elapsed());
    for warning in warnings {
        result = result.with_warning(warning);
    }
    Ok(result)
}
