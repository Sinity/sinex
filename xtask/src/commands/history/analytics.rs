use color_eyre::eyre::Result;
use console::style;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use tabled::{builder::Builder, settings::Style};

use crate::command::{CommandContext, CommandResult};
use crate::history::HistoryDb;

use super::{
    format_history_cutoff_timestamp, json_i64, json_optional_f64, json_optional_i64,
    json_optional_string, json_string, parse_history_time, secs_to_hours, sql_string_literal,
};

#[derive(Debug, Clone, Serialize)]
struct DayComparisonReport {
    day: String,
    against: String,
    commands: Vec<String>,
    include_failures: bool,
    rows: Vec<CommandDayComparison>,
    slowest: Vec<SlowInvocationSummary>,
    evidence_limits: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct CommandDayComparison {
    command: String,
    baseline: DayCommandSummary,
    current: DayCommandSummary,
    avg_duration_delta_secs: Option<f64>,
    avg_duration_ratio: Option<f64>,
    median_duration_delta_secs: Option<f64>,
    median_duration_ratio: Option<f64>,
    max_duration_delta_secs: Option<f64>,
    io_full_avg_delta: Option<f64>,
    memory_full_avg_delta: Option<f64>,
}

#[derive(Debug, Clone, Default, Serialize)]
struct DayCommandSummary {
    invocation_count: usize,
    avg_duration_secs: Option<f64>,
    median_duration_secs: Option<f64>,
    min_duration_secs: Option<f64>,
    max_duration_secs: Option<f64>,
    avg_io_full: Option<f64>,
    max_io_full: Option<f64>,
    avg_memory_full: Option<f64>,
    max_memory_full: Option<f64>,
    avg_process_memory_mb: Option<f64>,
    failed_count: usize,
}

#[derive(Debug, Clone, Serialize)]
struct SlowInvocationSummary {
    id: i64,
    command: String,
    status: String,
    exit_code: Option<i64>,
    started_at: String,
    duration_secs: f64,
    io_full: Option<f64>,
    memory_full: Option<f64>,
    process_memory_mb: Option<f64>,
    args_json: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct HistoryExplainReport {
    day: String,
    against: String,
    commands: Vec<String>,
    include_failures: bool,
    command_deltas: Vec<CommandDayComparison>,
    slowest_invocations: Vec<SlowInvocationSummary>,
    stage_totals: Vec<ExplainStageSummary>,
    test_overhead: Vec<ExplainTestOverheadRow>,
    interpretation: Vec<String>,
    evidence_limits: Vec<String>,
    machine_followups: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct ExplainStageSummary {
    command: String,
    stage_name: String,
    invocation_count: usize,
    total_duration_secs: f64,
    avg_duration_secs: Option<f64>,
    max_duration_secs: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
struct ExplainTestOverheadRow {
    invocation_id: i64,
    started_at: String,
    status: String,
    duration_secs: f64,
    test_body_duration_secs: f64,
    non_test_overhead_secs: f64,
    test_body_ratio: f64,
    io_full: Option<f64>,
    memory_full: Option<f64>,
    args_json: Option<String>,
}

#[derive(Debug, Clone)]
struct CompareInvocationRow {
    id: i64,
    command: String,
    status: String,
    exit_code: Option<i64>,
    started_at: String,
    duration_secs: f64,
    io_full: Option<f64>,
    memory_full: Option<f64>,
    process_memory_mb: Option<f64>,
    args_json: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct ResourceWindowReport {
    window: String,
    commands: Vec<String>,
    include_background: bool,
    success_only: bool,
    invocation_count: usize,
    rows: Vec<ResourceCommandSummary>,
    top_devices: Vec<ResourceDeviceSummary>,
    slowest: Vec<ResourceInvocationSummary>,
    evidence_limits: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize)]
struct ResourceCommandSummary {
    command: String,
    invocation_count: usize,
    failed_count: usize,
    cancelled_count: usize,
    background_count: usize,
    total_duration_hours: f64,
    avg_duration_secs: Option<f64>,
    max_duration_secs: Option<f64>,
    avg_io_full: Option<f64>,
    max_io_full: Option<f64>,
    high_io_full_count: usize,
    avg_memory_full: Option<f64>,
    max_memory_full: Option<f64>,
    avg_process_count_max: Option<f64>,
    max_process_count_max: Option<i64>,
    host_block_read_mib: f64,
    host_block_write_mib: f64,
    avg_host_block_read_iops: Option<f64>,
    avg_host_block_write_iops: Option<f64>,
}

#[derive(Debug, Clone, Default, Serialize)]
struct ResourceDeviceSummary {
    device: String,
    invocation_count: usize,
    total_mib: f64,
    avg_read_iops: Option<f64>,
    avg_write_iops: Option<f64>,
    max_weighted_io_ms_per_s: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
struct ResourceInvocationSummary {
    id: i64,
    command: String,
    status: String,
    started_at: String,
    duration_secs: f64,
    io_full: Option<f64>,
    memory_full: Option<f64>,
    process_count_max: Option<i64>,
    host_block_read_mib: Option<f64>,
    host_block_write_mib: Option<f64>,
    host_block_busiest_device: Option<String>,
    host_block_busiest_device_total_mib: Option<f64>,
    args_json: Option<String>,
}

#[derive(Debug, Clone)]
struct ResourceInvocationRow {
    id: i64,
    command: String,
    status: String,
    started_at: String,
    duration_secs: f64,
    is_background: bool,
    io_full: Option<f64>,
    memory_full: Option<f64>,
    process_count_max: Option<i64>,
    host_block_read_mib: Option<f64>,
    host_block_write_mib: Option<f64>,
    host_block_read_iops: Option<f64>,
    host_block_write_iops: Option<f64>,
    host_block_busiest_device: Option<String>,
    host_block_busiest_device_total_mib: Option<f64>,
    host_block_busiest_device_read_iops: Option<f64>,
    host_block_busiest_device_write_iops: Option<f64>,
    host_block_busiest_device_weighted_io_ms_per_s: Option<f64>,
    args_json: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct InvocationOverlapReport {
    target: OverlapInvocation,
    shared_resources: SharedResourceSummary,
    overlapping_invocations: Vec<OverlapInvocation>,
    overlapping_background_jobs: Vec<OverlapBackgroundJob>,
    evidence_limits: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct SharedResourceSummary {
    process_cpu_avg: Option<f64>,
    process_memory_max_mb: Option<f64>,
    root_process_cpu_avg: Option<f64>,
    root_process_memory_max_mb: Option<f64>,
    shared_nix_daemon_cpu_avg: Option<f64>,
    shared_nix_daemon_memory_max_mb: Option<f64>,
    shared_nix_build_slice_cpu_avg: Option<f64>,
    shared_nix_build_slice_memory_max_mb: Option<f64>,
    shared_background_slice_cpu_avg: Option<f64>,
    shared_background_slice_memory_max_mb: Option<f64>,
    process_count_max: Option<i64>,
    resource_sample_count: Option<i64>,
    host_cpu_pressure_some_avg10_max: Option<f64>,
    host_io_pressure_some_avg10_max: Option<f64>,
    host_io_pressure_full_avg10_max: Option<f64>,
    host_memory_pressure_some_avg10_max: Option<f64>,
    host_memory_pressure_full_avg10_max: Option<f64>,
    host_block_read_mib_delta: Option<f64>,
    host_block_write_mib_delta: Option<f64>,
    host_block_read_iops_avg: Option<f64>,
    host_block_write_iops_avg: Option<f64>,
    host_block_busiest_device: Option<String>,
    host_block_busiest_device_total_mib_delta: Option<f64>,
    host_block_busiest_device_read_iops_avg: Option<f64>,
    host_block_busiest_device_write_iops_avg: Option<f64>,
    host_block_busiest_device_weighted_io_ms_per_s: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
struct OverlapInvocation {
    id: i64,
    command: String,
    status: String,
    started_at: String,
    finished_at: Option<String>,
    duration_secs: Option<f64>,
    overlap_secs: Option<f64>,
    overlap_pct_of_target: Option<f64>,
    is_background: bool,
    args_json: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct OverlapBackgroundJob {
    id: i64,
    invocation_id: Option<i64>,
    command: String,
    job_status: String,
    pid: Option<i64>,
    started_at: String,
    finished_at: Option<String>,
    overlap_secs: Option<f64>,
    overlap_pct_of_target: Option<f64>,
    args_json: Option<String>,
}

pub(super) fn execute_overlap(
    db: &HistoryDb,
    invocation_selector: &str,
    command: Option<&str>,
    limit: usize,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let invocation_id = db
        .resolve_invocation_id(invocation_selector, command)?
        .ok_or_else(|| {
            color_eyre::eyre::eyre!("No invocation matched selector {invocation_selector:?}")
        })?;
    let target = load_overlap_target(db, invocation_id)?;
    let target_start = parse_history_time(&target.started_at, "started_at")?;
    let target_end = match target.finished_at.as_deref() {
        Some(finished_at) => parse_history_time(finished_at, "finished_at")?,
        None => time::OffsetDateTime::now_utc(),
    };
    let target_duration_secs = target
        .duration_secs
        .unwrap_or_else(|| (target_end - target_start).as_seconds_f64().max(0.0));

    let mut overlapping_invocations =
        load_overlapping_invocations(db, &target, target_start, target_end, target_duration_secs)?;
    overlapping_invocations.truncate(limit);
    let mut overlapping_background_jobs =
        load_overlapping_background_jobs(db, target_start, target_end, target_duration_secs)?;
    overlapping_background_jobs.truncate(limit);

    let report = InvocationOverlapReport {
        shared_resources: load_shared_resource_summary(db, invocation_id)?,
        target,
        overlapping_invocations,
        overlapping_background_jobs,
        evidence_limits: vec![
            "History overlap is limited to xtask invocations and background jobs recorded in this checkout history database.".to_string(),
            "Shared nix/build/background slice columns are CPU and memory summaries, not per-process I/O byte attribution.".to_string(),
            "Host pressure fields are PSI stall percentages observed during the invocation; they do not identify a causal process or I/O pattern by themselves.".to_string(),
            "Host block fields are aggregate whole-device deltas sampled while this xtask invocation ran; they quantify device load shape but do not partition ownership across unrelated services.".to_string(),
        ],
    };

    let mut result = CommandResult::success()
        .with_message(format!(
            "Explained overlap for invocation #{}",
            report.target.id
        ))
        .with_duration(ctx.elapsed());
    if ctx.is_human() {
        print_overlap_report(&report);
    } else {
        result = result.with_data(serde_json::to_value(&report)?);
    }
    Ok(result)
}

pub(super) fn execute_compare_days(
    db: &HistoryDb,
    day: Option<&str>,
    against: Option<&str>,
    commands: &[String],
    limit: usize,
    include_failures: bool,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let today = time::OffsetDateTime::now_utc().date();
    let day = resolve_history_day(day, today, "--day")?;
    let against = resolve_history_day(against, today - time::Duration::days(1), "--against")?;

    let commands = if commands.is_empty() {
        vec![
            "check".to_string(),
            "test".to_string(),
            "build".to_string(),
            "fix".to_string(),
        ]
    } else {
        commands.to_vec()
    };
    let command_list = commands
        .iter()
        .map(|command| sql_string_literal(command))
        .collect::<Vec<_>>()
        .join(", ");
    let status_filter = if include_failures {
        "status IN ('success', 'failed')"
    } else {
        "status = 'success'"
    };
    let rows_sql = format!(
        r"
        SELECT id, command, status, exit_code, started_at, duration_secs,
               host_io_pressure_full_avg10_max AS io_full,
               host_memory_pressure_full_avg10_max AS memory_full,
               process_memory_usage_max_mb AS process_memory_mb,
               args_json
        FROM invocations
        WHERE command IN ({command_list})
          AND date(started_at) IN ({}, {})
          AND duration_secs IS NOT NULL
          AND {status_filter}
        ORDER BY started_at ASC
        ",
        sql_string_literal(&against),
        sql_string_literal(&day)
    );
    let mut rows = db
        .run_readonly_query(&rows_sql)?
        .into_iter()
        .map(|row| compare_row_from_json(&row))
        .collect::<Result<Vec<_>>>()?;

    let mut comparisons = Vec::new();
    for command in &commands {
        let baseline_rows = rows
            .iter()
            .filter(|row| row.command == *command && row.started_at.starts_with(&against))
            .collect::<Vec<_>>();
        let current_rows = rows
            .iter()
            .filter(|row| row.command == *command && row.started_at.starts_with(&day))
            .collect::<Vec<_>>();
        let baseline = summarize_compare_rows(&baseline_rows);
        let current = summarize_compare_rows(&current_rows);
        comparisons.push(CommandDayComparison {
            command: command.clone(),
            avg_duration_delta_secs: option_delta(
                current.avg_duration_secs,
                baseline.avg_duration_secs,
            ),
            avg_duration_ratio: option_ratio(current.avg_duration_secs, baseline.avg_duration_secs),
            median_duration_delta_secs: option_delta(
                current.median_duration_secs,
                baseline.median_duration_secs,
            ),
            median_duration_ratio: option_ratio(
                current.median_duration_secs,
                baseline.median_duration_secs,
            ),
            max_duration_delta_secs: option_delta(
                current.max_duration_secs,
                baseline.max_duration_secs,
            ),
            io_full_avg_delta: option_delta(current.avg_io_full, baseline.avg_io_full),
            memory_full_avg_delta: option_delta(current.avg_memory_full, baseline.avg_memory_full),
            baseline,
            current,
        });
    }

    rows.sort_by(|left, right| {
        right
            .duration_secs
            .partial_cmp(&left.duration_secs)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let slowest = rows
        .iter()
        .filter(|row| row.started_at.starts_with(&day))
        .take(limit)
        .map(|row| SlowInvocationSummary {
            id: row.id,
            command: row.command.clone(),
            status: row.status.clone(),
            exit_code: row.exit_code,
            started_at: row.started_at.clone(),
            duration_secs: row.duration_secs,
            io_full: row.io_full,
            memory_full: row.memory_full,
            process_memory_mb: row.process_memory_mb,
            args_json: row.args_json.clone(),
        })
        .collect::<Vec<_>>();

    let report = DayComparisonReport {
        day,
        against,
        commands,
        include_failures,
        rows: comparisons,
        slowest,
        evidence_limits: vec![
            "Compare-days averages and medians exclude cancelled stale/zombie rows, but tail durations still require end-marker scrutiny.".to_string(),
            "MAX and slowest rows are outlier triage hints, not proof of actual runtime; missing or late finish registration can inflate them.".to_string(),
            "Use median deltas and recorded stage totals as the primary day-over-day signal.".to_string(),
        ],
    };

    let mut result = CommandResult::success()
        .with_message(format!(
            "Compared {} against {}",
            report.day, report.against
        ))
        .with_duration(ctx.elapsed());

    if ctx.is_human() {
        print_compare_days_report(&report);
    } else {
        result = result.with_data(serde_json::to_value(report)?);
    }

    Ok(result)
}

pub(super) fn execute_explain(
    db: &HistoryDb,
    day: Option<&str>,
    against: Option<&str>,
    commands: &[String],
    limit: usize,
    include_failures: bool,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let today = time::OffsetDateTime::now_utc().date();
    let day = resolve_history_day(day, today, "--day")?;
    let against = resolve_history_day(against, today - time::Duration::days(1), "--against")?;
    let commands = if commands.is_empty() {
        vec!["check".to_string(), "test".to_string(), "build".to_string()]
    } else {
        commands.to_vec()
    };
    let command_list = commands
        .iter()
        .map(|command| sql_string_literal(command))
        .collect::<Vec<_>>()
        .join(", ");
    let status_filter = if include_failures {
        "status IN ('success', 'failed')"
    } else {
        "status = 'success'"
    };
    let rows_sql = format!(
        r"
        SELECT id, command, status, exit_code, started_at, duration_secs,
               host_io_pressure_full_avg10_max AS io_full,
               host_memory_pressure_full_avg10_max AS memory_full,
               process_memory_usage_max_mb AS process_memory_mb,
               args_json
        FROM invocations
        WHERE command IN ({command_list})
          AND date(started_at) IN ({}, {})
          AND duration_secs IS NOT NULL
          AND {status_filter}
        ORDER BY started_at ASC
        ",
        sql_string_literal(&against),
        sql_string_literal(&day)
    );
    let mut rows = db
        .run_readonly_query(&rows_sql)?
        .into_iter()
        .map(|row| compare_row_from_json(&row))
        .collect::<Result<Vec<_>>>()?;

    let mut command_deltas = Vec::new();
    for command in &commands {
        let baseline_rows = rows
            .iter()
            .filter(|row| row.command == *command && row.started_at.starts_with(&against))
            .collect::<Vec<_>>();
        let current_rows = rows
            .iter()
            .filter(|row| row.command == *command && row.started_at.starts_with(&day))
            .collect::<Vec<_>>();
        let baseline = summarize_compare_rows(&baseline_rows);
        let current = summarize_compare_rows(&current_rows);
        command_deltas.push(CommandDayComparison {
            command: command.clone(),
            avg_duration_delta_secs: option_delta(
                current.avg_duration_secs,
                baseline.avg_duration_secs,
            ),
            avg_duration_ratio: option_ratio(current.avg_duration_secs, baseline.avg_duration_secs),
            median_duration_delta_secs: option_delta(
                current.median_duration_secs,
                baseline.median_duration_secs,
            ),
            median_duration_ratio: option_ratio(
                current.median_duration_secs,
                baseline.median_duration_secs,
            ),
            max_duration_delta_secs: option_delta(
                current.max_duration_secs,
                baseline.max_duration_secs,
            ),
            io_full_avg_delta: option_delta(current.avg_io_full, baseline.avg_io_full),
            memory_full_avg_delta: option_delta(current.avg_memory_full, baseline.avg_memory_full),
            baseline,
            current,
        });
    }

    rows.sort_by(|left, right| {
        right
            .duration_secs
            .partial_cmp(&left.duration_secs)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let slowest_invocations = rows
        .iter()
        .filter(|row| row.started_at.starts_with(&day))
        .take(limit)
        .map(|row| SlowInvocationSummary {
            id: row.id,
            command: row.command.clone(),
            status: row.status.clone(),
            exit_code: row.exit_code,
            started_at: row.started_at.clone(),
            duration_secs: row.duration_secs,
            io_full: row.io_full,
            memory_full: row.memory_full,
            process_memory_mb: row.process_memory_mb,
            args_json: row.args_json.clone(),
        })
        .collect::<Vec<_>>();
    let inv_status_filter = if include_failures {
        "inv.status IN ('success', 'failed')"
    } else {
        "inv.status = 'success'"
    };
    let test_status_filter = if include_failures {
        "i.status IN ('success', 'failed')"
    } else {
        "i.status = 'success'"
    };
    let stage_totals = load_explain_stage_totals(db, &day, &command_list, inv_status_filter)?;
    let test_overhead = load_explain_test_overhead(db, &day, test_status_filter, limit)?;
    let interpretation =
        build_explain_interpretation(&command_deltas, &test_overhead, &stage_totals);
    let machine_followups = slowest_invocations
        .iter()
        .take(3)
        .map(|row| {
            format!(
                "Lynchpin: python -m lynchpin.analysis.machine.service_io --xtask-invocation {} --limit 8 --min-total-mib 0.1",
                row.id
            )
        })
        .collect::<Vec<_>>();
    let report = HistoryExplainReport {
        day,
        against,
        commands,
        include_failures,
        command_deltas,
        slowest_invocations,
        stage_totals,
        test_overhead,
        interpretation,
        evidence_limits: vec![
            "xtask history can prove invocation duration, stage timing, test body duration, runner/setup overhead, xtask-sampled PSI maxima, and aggregate host block-device counters when recorded.".to_string(),
            "Tail durations are outlier triage hints, not proof of actual runtime; missing or late finish registration can inflate MAX/slowest rows.".to_string(),
            "xtask history cannot name external service/process ownership for I/O stalls; use Lynchpin machine telemetry for cgroup/process/block-device attribution.".to_string(),
            "A runner/setup-dominated test run means test bodies were not the wallclock cost center; it does not by itself distinguish cargo compile, linker, nextest startup, DB fixture setup, or kernel I/O wait.".to_string(),
        ],
        machine_followups,
    };

    let mut result = CommandResult::success()
        .with_message(format!("Explained build/test runtime for {}", report.day))
        .with_duration(ctx.elapsed());
    if ctx.is_human() {
        print_explain_report(&report);
    } else {
        result = result.with_data(serde_json::to_value(&report)?);
    }
    Ok(result)
}

fn load_explain_stage_totals(
    db: &HistoryDb,
    day: &str,
    command_list: &str,
    status_filter: &str,
) -> Result<Vec<ExplainStageSummary>> {
    let sql = format!(
        r"
        SELECT inv.command AS command,
               st.stage_name AS stage_name,
               COUNT(DISTINCT inv.id) AS invocation_count,
               COALESCE(SUM(st.duration_secs), 0.0) AS total_duration_secs,
               AVG(st.duration_secs) AS avg_duration_secs,
               MAX(st.duration_secs) AS max_duration_secs
        FROM stage_timings st
        JOIN invocations inv ON inv.id = st.invocation_id
        WHERE date(inv.started_at) = {}
          AND inv.command IN ({command_list})
          AND inv.duration_secs IS NOT NULL
          AND {status_filter}
        GROUP BY inv.command, st.stage_name
        ORDER BY total_duration_secs DESC, inv.command, st.stage_name
        ",
        sql_string_literal(day)
    );
    db.run_readonly_query(&sql)?
        .into_iter()
        .map(|row| {
            Ok(ExplainStageSummary {
                command: json_string(&row, "command")?,
                stage_name: json_string(&row, "stage_name")?,
                invocation_count: json_i64(&row, "invocation_count")? as usize,
                total_duration_secs: json_optional_f64(&row, "total_duration_secs")
                    .unwrap_or_default(),
                avg_duration_secs: json_optional_f64(&row, "avg_duration_secs"),
                max_duration_secs: json_optional_f64(&row, "max_duration_secs"),
            })
        })
        .collect()
}

fn load_explain_test_overhead(
    db: &HistoryDb,
    day: &str,
    status_filter: &str,
    limit: usize,
) -> Result<Vec<ExplainTestOverheadRow>> {
    let sql = format!(
        r"
        SELECT i.id AS invocation_id,
               i.started_at AS started_at,
               i.status AS status,
               i.duration_secs AS duration_secs,
               COALESCE(SUM(t.duration_secs), 0.0) AS test_body_duration_secs,
               i.host_io_pressure_full_avg10_max AS io_full,
               i.host_memory_pressure_full_avg10_max AS memory_full,
               i.args_json AS args_json
        FROM invocations i
        LEFT JOIN test_results t ON t.invocation_id = i.id
        WHERE i.command = 'test'
          AND date(i.started_at) = {}
          AND i.duration_secs IS NOT NULL
          AND {status_filter}
        GROUP BY i.id
        ORDER BY i.duration_secs DESC
        ",
        sql_string_literal(day)
    );
    let mut rows = db
        .run_readonly_query(&sql)?
        .into_iter()
        .map(|row| {
            let duration_secs = json_optional_f64(&row, "duration_secs").unwrap_or_default();
            let test_body_duration_secs =
                json_optional_f64(&row, "test_body_duration_secs").unwrap_or_default();
            let non_test_overhead_secs = (duration_secs - test_body_duration_secs).max(0.0);
            let test_body_ratio = if duration_secs > 0.0 {
                (test_body_duration_secs / duration_secs).clamp(0.0, 1.0)
            } else {
                0.0
            };
            Ok(ExplainTestOverheadRow {
                invocation_id: json_i64(&row, "invocation_id")?,
                started_at: json_string(&row, "started_at")?,
                status: json_string(&row, "status")?,
                duration_secs,
                test_body_duration_secs,
                non_test_overhead_secs,
                test_body_ratio,
                io_full: json_optional_f64(&row, "io_full"),
                memory_full: json_optional_f64(&row, "memory_full"),
                args_json: json_optional_string(&row, "args_json"),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    rows.sort_by(|left, right| {
        right
            .non_test_overhead_secs
            .partial_cmp(&left.non_test_overhead_secs)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| right.invocation_id.cmp(&left.invocation_id))
    });
    rows.truncate(limit);
    Ok(rows)
}

fn build_explain_interpretation(
    command_deltas: &[CommandDayComparison],
    test_overhead: &[ExplainTestOverheadRow],
    stage_totals: &[ExplainStageSummary],
) -> Vec<String> {
    let mut lines = Vec::new();
    if let Some(worst_ratio) = command_deltas
        .iter()
        .filter_map(|row| {
            row.avg_duration_ratio
                .map(|ratio| (row.command.as_str(), ratio))
        })
        .max_by(|left, right| {
            left.1
                .partial_cmp(&right.1)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
    {
        lines.push(format!(
            "{} has the largest average duration ratio versus baseline: {:.2}x",
            worst_ratio.0, worst_ratio.1
        ));
    }
    if let Some(row) = test_overhead.first() {
        if row.non_test_overhead_secs > row.test_body_duration_secs {
            lines.push(format!(
                "largest-overhead test row #{} is runner/setup dominated if its finish marker is trustworthy: {:.1}s overhead vs {:.1}s summed test bodies",
                row.invocation_id, row.non_test_overhead_secs, row.test_body_duration_secs
            ));
        }
    }
    if let Some(stage) = stage_totals.first() {
        lines.push(format!(
            "largest recorded stage bucket is {}:{} at {:.1}s total",
            stage.command, stage.stage_name, stage.total_duration_secs
        ));
    }
    if test_overhead.iter().any(|row| {
        row.io_full
            .is_some_and(|value| value >= crate::resources::thresholds::PSI_IO_FULL_WARN)
    }) {
        lines.push(
            "one or more slow test invocations overlapped recorded host io.full pressure above xtask's warning threshold".to_string(),
        );
    }
    if lines.is_empty() {
        lines.push("no clear compile/test runtime regression signal found in xtask history for this window".to_string());
    }
    lines
}

fn print_explain_report(report: &HistoryExplainReport) {
    println!(
        "{}",
        style(format!(
            "Build/test runtime explanation for {} vs {}:",
            report.day, report.against
        ))
        .bold()
    );
    let mut commands = Builder::new();
    commands.push_record([
        "CMD",
        "N BASE",
        "N DAY",
        "AVG BASE",
        "AVG DAY",
        "AVG RATIO",
        "MED RATIO",
        "IO Δ",
        "MEM Δ",
    ]);
    for row in &report.command_deltas {
        commands.push_record([
            row.command.clone(),
            row.baseline.invocation_count.to_string(),
            row.current.invocation_count.to_string(),
            fmt_opt_secs(row.baseline.avg_duration_secs),
            fmt_opt_secs(row.current.avg_duration_secs),
            fmt_opt_float(row.avg_duration_ratio),
            fmt_opt_float(row.median_duration_ratio),
            fmt_opt_float(row.io_full_avg_delta),
            fmt_opt_float(row.memory_full_avg_delta),
        ]);
    }
    let mut table = commands.build();
    table.with(Style::rounded());
    println!("{table}");

    if !report.test_overhead.is_empty() {
        println!("\nSlow test invocations by non-test overhead:");
        let mut tests = Builder::new();
        tests.push_record([
            "INV",
            "STATUS",
            "ELAPSED",
            "TEST BODY",
            "OVERHEAD",
            "BODY %",
            "IO.FULL",
        ]);
        for row in &report.test_overhead {
            tests.push_record([
                row.invocation_id.to_string(),
                row.status.clone(),
                format!("{:.1}s", row.duration_secs),
                format!("{:.1}s", row.test_body_duration_secs),
                format!("{:.1}s", row.non_test_overhead_secs),
                format!("{:.1}%", row.test_body_ratio * 100.0),
                fmt_opt_float(row.io_full),
            ]);
        }
        let mut table = tests.build();
        table.with(Style::rounded());
        println!("{table}");
    }

    if !report.stage_totals.is_empty() {
        println!("\nLargest recorded stage buckets:");
        let mut stages = Builder::new();
        stages.push_record(["CMD", "STAGE", "RUNS", "TOTAL", "AVG", "TAIL"]);
        for row in report.stage_totals.iter().take(10) {
            stages.push_record([
                row.command.clone(),
                row.stage_name.clone(),
                row.invocation_count.to_string(),
                format!("{:.1}s", row.total_duration_secs),
                fmt_opt_secs(row.avg_duration_secs),
                fmt_opt_secs(row.max_duration_secs),
            ]);
        }
        let mut table = stages.build();
        table.with(Style::rounded());
        println!("{table}");
    }

    println!("\nInterpretation:");
    for line in &report.interpretation {
        println!("- {line}");
    }
    println!("\nEvidence limits:");
    for line in &report.evidence_limits {
        println!("- {line}");
    }
    if !report.machine_followups.is_empty() {
        println!("\nMachine attribution follow-ups:");
        for line in &report.machine_followups {
            println!("- {line}");
        }
    }
}

pub(super) fn execute_resources(
    db: &HistoryDb,
    day: Option<&str>,
    days: u32,
    commands: &[String],
    limit: usize,
    include_background: bool,
    success_only: bool,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let (window_label, window_filter) = match day {
        Some(day) => {
            validate_history_day(day, "--day")?;
            (
                format!("UTC day {day}"),
                format!("date(started_at) = {}", sql_string_literal(day)),
            )
        }
        None => {
            let since = format_history_cutoff_timestamp(
                time::OffsetDateTime::now_utc() - time::Duration::days(i64::from(days)),
                "history resources cutoff",
            )?;
            (
                format!("last {days} day(s)"),
                format!("started_at >= {}", sql_string_literal(&since)),
            )
        }
    };
    let command_filter = if commands.is_empty() {
        String::new()
    } else {
        let command_list = commands
            .iter()
            .map(|command| sql_string_literal(command))
            .collect::<Vec<_>>()
            .join(", ");
        format!("AND command IN ({command_list})")
    };
    let background_filter = if include_background {
        String::new()
    } else {
        "AND NOT COALESCE(is_background, 0)".to_string()
    };
    let status_filter = if success_only {
        "AND status = 'success'"
    } else {
        "AND status IN ('success', 'failed', 'cancelled')"
    };

    let rows_sql = format!(
        r"
        SELECT id, command, status, started_at, duration_secs,
               COALESCE(is_background, 0) AS is_background,
               host_io_pressure_full_avg10_max AS io_full,
               host_memory_pressure_full_avg10_max AS memory_full,
               process_count_max,
               host_block_read_mib_delta,
               host_block_write_mib_delta,
               host_block_read_iops_avg,
               host_block_write_iops_avg,
               host_block_busiest_device,
               host_block_busiest_device_total_mib_delta,
               host_block_busiest_device_read_iops_avg,
               host_block_busiest_device_write_iops_avg,
               host_block_busiest_device_weighted_io_ms_per_s,
               args_json
        FROM invocations
        WHERE {window_filter}
          AND duration_secs IS NOT NULL
          AND NOT COALESCE(
              cancel_reason = 'stale_pid' AND cancelled_by = 'open_time_sweep',
              0
          )
          {status_filter}
          {background_filter}
          {command_filter}
        ORDER BY started_at ASC
        "
    );
    let mut rows = db
        .run_readonly_query(&rows_sql)?
        .into_iter()
        .map(|row| resource_row_from_json(&row))
        .collect::<Result<Vec<_>>>()?;
    rows.sort_by_key(|row| row.started_at.clone());

    let commands_for_report = if commands.is_empty() {
        unique_resource_commands(&rows)
    } else {
        commands.to_vec()
    };
    let summaries = summarize_resource_commands(&rows, &commands_for_report);
    let top_devices = summarize_resource_devices(&rows);
    let slowest = slowest_resource_invocations(&rows, limit);
    let report = ResourceWindowReport {
        window: window_label,
        commands: commands_for_report,
        include_background,
        success_only,
        invocation_count: rows.len(),
        rows: summaries,
        top_devices,
        slowest,
        evidence_limits: vec![
            "This report uses only xtask invocation history columns from this checkout.".to_string(),
            "Tail durations and slowest rows are outlier triage hints, not proof of actual runtime; missing or late finish registration can inflate them.".to_string(),
            "PSI fields are stall percentages sampled during invocations; they do not identify a causal process by themselves.".to_string(),
            "Host block fields are aggregate whole-device deltas sampled during xtask invocations; they quantify device load shape but do not partition service/process ownership.".to_string(),
            "Use Lynchpin machine telemetry for cgroup, process, and block-device attribution outside xtask's own invocation record.".to_string(),
        ],
    };

    let mut result = CommandResult::success()
        .with_message(format!(
            "Summarized {} invocation resource row(s)",
            report.invocation_count
        ))
        .with_duration(ctx.elapsed());
    if ctx.is_human() {
        print_resources_report(&report);
    } else {
        result = result.with_data(serde_json::to_value(&report)?);
    }
    Ok(result)
}

pub(super) fn resolve_history_day(
    value: Option<&str>,
    default: time::Date,
    flag: &'static str,
) -> Result<String> {
    let Some(value) = value else {
        return Ok(default.to_string());
    };
    let trimmed = value.trim();
    if trimmed.eq_ignore_ascii_case("today") {
        return Ok(time::OffsetDateTime::now_utc().date().to_string());
    }
    if trimmed.eq_ignore_ascii_case("yesterday") {
        return Ok((time::OffsetDateTime::now_utc().date() - time::Duration::days(1)).to_string());
    }
    validate_history_day(trimmed, flag)?;
    Ok(trimmed.to_string())
}

fn validate_history_day(value: &str, flag: &'static str) -> Result<()> {
    let valid_shape = value.len() == 10
        && value.as_bytes()[4] == b'-'
        && value.as_bytes()[7] == b'-'
        && value
            .bytes()
            .enumerate()
            .all(|(idx, byte)| idx == 4 || idx == 7 || byte.is_ascii_digit());
    if valid_shape {
        Ok(())
    } else {
        Err(color_eyre::eyre::eyre!(
            "{flag} must use YYYY-MM-DD format, got {value:?}"
        ))
    }
}

fn load_overlap_target(db: &HistoryDb, invocation_id: i64) -> Result<OverlapInvocation> {
    let sql = format!(
        r"
        SELECT id, command, status, started_at, finished_at, duration_secs,
               is_background, args_json
        FROM invocations
        WHERE id = {}
        LIMIT 1
        ",
        invocation_id
    );
    let row = db
        .run_readonly_query(&sql)?
        .into_iter()
        .next()
        .ok_or_else(|| color_eyre::eyre::eyre!("Invocation #{invocation_id} not found"))?;
    overlap_invocation_from_json(&row, None, None)
}

fn load_shared_resource_summary(
    db: &HistoryDb,
    invocation_id: i64,
) -> Result<SharedResourceSummary> {
    let sql = format!(
        r"
        SELECT process_cpu_usage_avg,
               process_memory_usage_max_mb,
               root_process_cpu_usage_avg,
               root_process_memory_usage_max_mb,
               shared_nix_daemon_cpu_usage_avg,
               shared_nix_daemon_memory_usage_max_mb,
               shared_nix_build_slice_cpu_usage_avg,
               shared_nix_build_slice_memory_usage_max_mb,
               shared_background_slice_cpu_usage_avg,
               shared_background_slice_memory_usage_max_mb,
               process_count_max,
               resource_sample_count,
               host_cpu_pressure_some_avg10_max,
               host_io_pressure_some_avg10_max,
               host_io_pressure_full_avg10_max,
               host_memory_pressure_some_avg10_max,
               host_memory_pressure_full_avg10_max,
               host_block_read_mib_delta,
               host_block_write_mib_delta,
               host_block_read_iops_avg,
               host_block_write_iops_avg,
               host_block_busiest_device,
               host_block_busiest_device_total_mib_delta,
               host_block_busiest_device_read_iops_avg,
               host_block_busiest_device_write_iops_avg,
               host_block_busiest_device_weighted_io_ms_per_s
        FROM invocations
        WHERE id = {}
        LIMIT 1
        ",
        invocation_id
    );
    let row = db
        .run_readonly_query(&sql)?
        .into_iter()
        .next()
        .ok_or_else(|| color_eyre::eyre::eyre!("Invocation #{invocation_id} not found"))?;

    Ok(SharedResourceSummary {
        process_cpu_avg: json_optional_f64(&row, "process_cpu_usage_avg"),
        process_memory_max_mb: json_optional_f64(&row, "process_memory_usage_max_mb"),
        root_process_cpu_avg: json_optional_f64(&row, "root_process_cpu_usage_avg"),
        root_process_memory_max_mb: json_optional_f64(&row, "root_process_memory_usage_max_mb"),
        shared_nix_daemon_cpu_avg: json_optional_f64(&row, "shared_nix_daemon_cpu_usage_avg"),
        shared_nix_daemon_memory_max_mb: json_optional_f64(
            &row,
            "shared_nix_daemon_memory_usage_max_mb",
        ),
        shared_nix_build_slice_cpu_avg: json_optional_f64(
            &row,
            "shared_nix_build_slice_cpu_usage_avg",
        ),
        shared_nix_build_slice_memory_max_mb: json_optional_f64(
            &row,
            "shared_nix_build_slice_memory_usage_max_mb",
        ),
        shared_background_slice_cpu_avg: json_optional_f64(
            &row,
            "shared_background_slice_cpu_usage_avg",
        ),
        shared_background_slice_memory_max_mb: json_optional_f64(
            &row,
            "shared_background_slice_memory_usage_max_mb",
        ),
        process_count_max: json_optional_i64(&row, "process_count_max"),
        resource_sample_count: json_optional_i64(&row, "resource_sample_count"),
        host_cpu_pressure_some_avg10_max: json_optional_f64(
            &row,
            "host_cpu_pressure_some_avg10_max",
        ),
        host_io_pressure_some_avg10_max: json_optional_f64(&row, "host_io_pressure_some_avg10_max"),
        host_io_pressure_full_avg10_max: json_optional_f64(&row, "host_io_pressure_full_avg10_max"),
        host_memory_pressure_some_avg10_max: json_optional_f64(
            &row,
            "host_memory_pressure_some_avg10_max",
        ),
        host_memory_pressure_full_avg10_max: json_optional_f64(
            &row,
            "host_memory_pressure_full_avg10_max",
        ),
        host_block_read_mib_delta: json_optional_f64(&row, "host_block_read_mib_delta"),
        host_block_write_mib_delta: json_optional_f64(&row, "host_block_write_mib_delta"),
        host_block_read_iops_avg: json_optional_f64(&row, "host_block_read_iops_avg"),
        host_block_write_iops_avg: json_optional_f64(&row, "host_block_write_iops_avg"),
        host_block_busiest_device: json_optional_string(&row, "host_block_busiest_device"),
        host_block_busiest_device_total_mib_delta: json_optional_f64(
            &row,
            "host_block_busiest_device_total_mib_delta",
        ),
        host_block_busiest_device_read_iops_avg: json_optional_f64(
            &row,
            "host_block_busiest_device_read_iops_avg",
        ),
        host_block_busiest_device_write_iops_avg: json_optional_f64(
            &row,
            "host_block_busiest_device_write_iops_avg",
        ),
        host_block_busiest_device_weighted_io_ms_per_s: json_optional_f64(
            &row,
            "host_block_busiest_device_weighted_io_ms_per_s",
        ),
    })
}

fn load_overlapping_invocations(
    db: &HistoryDb,
    target: &OverlapInvocation,
    target_start: time::OffsetDateTime,
    target_end: time::OffsetDateTime,
    target_duration_secs: f64,
) -> Result<Vec<OverlapInvocation>> {
    let target_start_sql = sql_string_literal(&target.started_at);
    let target_end_text = target
        .finished_at
        .clone()
        .unwrap_or_else(|| target_end.to_string());
    let target_end_sql = sql_string_literal(&target_end_text);
    let now_sql = sql_string_literal(&time::OffsetDateTime::now_utc().to_string());
    let sql = format!(
        r"
        SELECT id, command, status, started_at, finished_at, duration_secs,
               is_background, args_json
        FROM invocations
        WHERE id != {}
          AND started_at < {target_end_sql}
          AND COALESCE(finished_at, {now_sql}) > {target_start_sql}
        ORDER BY started_at ASC, id ASC
        ",
        target.id
    );

    let mut rows = db
        .run_readonly_query(&sql)?
        .into_iter()
        .map(|row| {
            let start = parse_history_time(&json_string(&row, "started_at")?, "started_at")?;
            let end = match row.get("finished_at").and_then(serde_json::Value::as_str) {
                Some(finished_at) => parse_history_time(finished_at, "finished_at")?,
                None => time::OffsetDateTime::now_utc(),
            };
            let overlap_secs = interval_overlap_secs(target_start, target_end, start, end);
            overlap_invocation_from_json(
                &row,
                Some(overlap_secs),
                overlap_pct(overlap_secs, target_duration_secs),
            )
        })
        .collect::<Result<Vec<_>>>()?;
    rows.sort_by(|left, right| {
        right
            .overlap_secs
            .unwrap_or(0.0)
            .partial_cmp(&left.overlap_secs.unwrap_or(0.0))
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    Ok(rows)
}

fn load_overlapping_background_jobs(
    db: &HistoryDb,
    target_start: time::OffsetDateTime,
    target_end: time::OffsetDateTime,
    target_duration_secs: f64,
) -> Result<Vec<OverlapBackgroundJob>> {
    let target_start_sql = sql_string_literal(&target_start.to_string());
    let target_end_sql = sql_string_literal(&target_end.to_string());
    let now_sql = sql_string_literal(&time::OffsetDateTime::now_utc().to_string());
    let sql = format!(
        r"
        SELECT id, invocation_id, command, job_status, pid, started_at, finished_at, args_json
        FROM background_jobs
        WHERE started_at < {target_end_sql}
          AND COALESCE(finished_at, {now_sql}) > {target_start_sql}
        ORDER BY started_at ASC, id ASC
        "
    );

    let mut rows = db
        .run_readonly_query(&sql)?
        .into_iter()
        .map(|row| {
            let start = parse_history_time(&json_string(&row, "started_at")?, "started_at")?;
            let end = match row.get("finished_at").and_then(serde_json::Value::as_str) {
                Some(finished_at) => parse_history_time(finished_at, "finished_at")?,
                None => time::OffsetDateTime::now_utc(),
            };
            let overlap_secs = interval_overlap_secs(target_start, target_end, start, end);
            Ok(OverlapBackgroundJob {
                id: json_i64(&row, "id")?,
                invocation_id: json_optional_i64(&row, "invocation_id"),
                command: json_string(&row, "command")?,
                job_status: json_string(&row, "job_status")?,
                pid: json_optional_i64(&row, "pid"),
                started_at: json_string(&row, "started_at")?,
                finished_at: json_optional_string(&row, "finished_at"),
                overlap_secs: Some(overlap_secs),
                overlap_pct_of_target: overlap_pct(overlap_secs, target_duration_secs),
                args_json: json_optional_string(&row, "args_json"),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    rows.sort_by(|left, right| {
        right
            .overlap_secs
            .unwrap_or(0.0)
            .partial_cmp(&left.overlap_secs.unwrap_or(0.0))
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    Ok(rows)
}

fn overlap_invocation_from_json(
    row: &serde_json::Map<String, serde_json::Value>,
    overlap_secs: Option<f64>,
    overlap_pct_of_target: Option<f64>,
) -> Result<OverlapInvocation> {
    Ok(OverlapInvocation {
        id: json_i64(row, "id")?,
        command: json_string(row, "command")?,
        status: json_string(row, "status")?,
        started_at: json_string(row, "started_at")?,
        finished_at: json_optional_string(row, "finished_at"),
        duration_secs: json_optional_f64(row, "duration_secs"),
        overlap_secs,
        overlap_pct_of_target,
        is_background: row
            .get("is_background")
            .and_then(serde_json::Value::as_i64)
            .is_some_and(|value| value != 0),
        args_json: json_optional_string(row, "args_json"),
    })
}

fn interval_overlap_secs(
    left_start: time::OffsetDateTime,
    left_end: time::OffsetDateTime,
    right_start: time::OffsetDateTime,
    right_end: time::OffsetDateTime,
) -> f64 {
    let start = left_start.max(right_start);
    let end = left_end.min(right_end);
    (end - start).as_seconds_f64().max(0.0)
}

fn overlap_pct(overlap_secs: f64, target_duration_secs: f64) -> Option<f64> {
    (target_duration_secs > 0.0).then_some((overlap_secs / target_duration_secs) * 100.0)
}

fn print_overlap_report(report: &InvocationOverlapReport) {
    println!(
        "Invocation #{} overlap attribution:",
        style(report.target.id).bold()
    );
    println!(
        "  target: {} {} ({}, {:.1}s)",
        report.target.command,
        report.target.args_json.as_deref().unwrap_or("[]"),
        report.target.status,
        report.target.duration_secs.unwrap_or_default()
    );
    println!("  started: {}", report.target.started_at);
    if let Some(finished_at) = &report.target.finished_at {
        println!("  finished: {finished_at}");
    }

    println!("\n{}", style("Recorded Shared Resources:").cyan().bold());
    let mut resources = Builder::new();
    resources.push_record(["METRIC", "VALUE"]);
    push_optional_metric(
        &mut resources,
        "process cpu avg",
        report.shared_resources.process_cpu_avg,
        "%",
    );
    push_optional_metric(
        &mut resources,
        "process memory max",
        report.shared_resources.process_memory_max_mb,
        " MB",
    );
    push_optional_metric(
        &mut resources,
        "shared nix-daemon cpu avg",
        report.shared_resources.shared_nix_daemon_cpu_avg,
        "%",
    );
    push_optional_metric(
        &mut resources,
        "shared nix-build slice cpu avg",
        report.shared_resources.shared_nix_build_slice_cpu_avg,
        "%",
    );
    push_optional_metric(
        &mut resources,
        "shared background slice cpu avg",
        report.shared_resources.shared_background_slice_cpu_avg,
        "%",
    );
    push_optional_metric(
        &mut resources,
        "host io.full avg10 max",
        report.shared_resources.host_io_pressure_full_avg10_max,
        "%",
    );
    push_optional_metric(
        &mut resources,
        "host memory.full avg10 max",
        report.shared_resources.host_memory_pressure_full_avg10_max,
        "%",
    );
    push_optional_metric(
        &mut resources,
        "host block read",
        report.shared_resources.host_block_read_mib_delta,
        " MiB",
    );
    push_optional_metric(
        &mut resources,
        "host block write",
        report.shared_resources.host_block_write_mib_delta,
        " MiB",
    );
    push_optional_metric(
        &mut resources,
        "host block read iops avg",
        report.shared_resources.host_block_read_iops_avg,
        "",
    );
    push_optional_metric(
        &mut resources,
        "host block write iops avg",
        report.shared_resources.host_block_write_iops_avg,
        "",
    );
    if let Some(device) = &report.shared_resources.host_block_busiest_device {
        resources.push_record(["host block busiest device".to_string(), device.clone()]);
    }
    push_optional_metric(
        &mut resources,
        "busiest device total",
        report
            .shared_resources
            .host_block_busiest_device_total_mib_delta,
        " MiB",
    );
    push_optional_metric(
        &mut resources,
        "busiest device read iops",
        report
            .shared_resources
            .host_block_busiest_device_read_iops_avg,
        "",
    );
    push_optional_metric(
        &mut resources,
        "busiest device write iops",
        report
            .shared_resources
            .host_block_busiest_device_write_iops_avg,
        "",
    );
    push_optional_metric(
        &mut resources,
        "busiest device weighted io",
        report
            .shared_resources
            .host_block_busiest_device_weighted_io_ms_per_s,
        " ms/s",
    );
    if let Some(samples) = report.shared_resources.resource_sample_count {
        resources.push_record(["resource samples".to_string(), samples.to_string()]);
    }
    let mut table = resources.build();
    table.with(Style::rounded());
    println!("{table}");

    println!("\n{}", style("Overlapping Invocations:").bold());
    if report.overlapping_invocations.is_empty() {
        println!("  none recorded");
    } else {
        let mut builder = Builder::new();
        builder.push_record([
            "ID",
            "CMD",
            "STATUS",
            "OVERLAP",
            "TARGET %",
            "BACKGROUND",
            "ARGS",
        ]);
        for row in &report.overlapping_invocations {
            builder.push_record([
                row.id.to_string(),
                row.command.clone(),
                row.status.clone(),
                format_optional_secs(row.overlap_secs),
                format_optional_pct(row.overlap_pct_of_target),
                row.is_background.to_string(),
                truncate_middle(row.args_json.as_deref().unwrap_or("[]"), 48),
            ]);
        }
        let mut table = builder.build();
        table.with(Style::rounded());
        println!("{table}");
    }

    println!("\n{}", style("Overlapping Background Jobs:").bold());
    if report.overlapping_background_jobs.is_empty() {
        println!("  none recorded");
    } else {
        let mut builder = Builder::new();
        builder.push_record(["JOB", "INV", "CMD", "STATUS", "PID", "OVERLAP", "TARGET %"]);
        for row in &report.overlapping_background_jobs {
            builder.push_record([
                row.id.to_string(),
                row.invocation_id
                    .map_or_else(|| "-".to_string(), |id| id.to_string()),
                row.command.clone(),
                row.job_status.clone(),
                row.pid
                    .map_or_else(|| "-".to_string(), |pid| pid.to_string()),
                format_optional_secs(row.overlap_secs),
                format_optional_pct(row.overlap_pct_of_target),
            ]);
        }
        let mut table = builder.build();
        table.with(Style::rounded());
        println!("{table}");
    }

    println!("\n{}", style("Evidence limits:").yellow().bold());
    for limit in &report.evidence_limits {
        println!("  - {limit}");
    }
}

fn push_optional_metric(builder: &mut Builder, name: &str, value: Option<f64>, suffix: &str) {
    builder.push_record([
        name.to_string(),
        value.map_or_else(
            || "unavailable".to_string(),
            |value| format!("{value:.2}{suffix}"),
        ),
    ]);
}

fn format_optional_secs(value: Option<f64>) -> String {
    value.map_or_else(|| "-".to_string(), |value| format!("{value:.1}s"))
}

fn format_optional_pct(value: Option<f64>) -> String {
    value.map_or_else(|| "-".to_string(), |value| format!("{value:.1}%"))
}

fn truncate_middle(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        return value.to_string();
    }
    let keep = max_chars.saturating_sub(3);
    let left = keep / 2;
    let right = keep - left;
    let prefix = value.chars().take(left).collect::<String>();
    let suffix = value
        .chars()
        .rev()
        .take(right)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect::<String>();
    format!("{prefix}...{suffix}")
}

fn compare_row_from_json(
    row: &serde_json::Map<String, serde_json::Value>,
) -> Result<CompareInvocationRow> {
    Ok(CompareInvocationRow {
        id: json_i64(row, "id")?,
        command: json_string(row, "command")?,
        status: json_string(row, "status")?,
        exit_code: row.get("exit_code").and_then(serde_json::Value::as_i64),
        started_at: json_string(row, "started_at")?,
        duration_secs: row
            .get("duration_secs")
            .and_then(serde_json::Value::as_f64)
            .ok_or_else(|| color_eyre::eyre::eyre!("history compare row missing duration_secs"))?,
        io_full: row.get("io_full").and_then(serde_json::Value::as_f64),
        memory_full: row.get("memory_full").and_then(serde_json::Value::as_f64),
        process_memory_mb: row
            .get("process_memory_mb")
            .and_then(serde_json::Value::as_f64),
        args_json: row
            .get("args_json")
            .and_then(serde_json::Value::as_str)
            .map(ToOwned::to_owned),
    })
}

fn summarize_compare_rows(rows: &[&CompareInvocationRow]) -> DayCommandSummary {
    let mut durations = rows.iter().map(|row| row.duration_secs).collect::<Vec<_>>();
    durations.sort_by(|left, right| left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal));
    DayCommandSummary {
        invocation_count: rows.len(),
        avg_duration_secs: average(durations.iter().copied()),
        median_duration_secs: median_sorted(&durations),
        min_duration_secs: durations.first().copied(),
        max_duration_secs: durations.last().copied(),
        avg_io_full: average(rows.iter().filter_map(|row| row.io_full)),
        max_io_full: max_option(rows.iter().filter_map(|row| row.io_full)),
        avg_memory_full: average(rows.iter().filter_map(|row| row.memory_full)),
        max_memory_full: max_option(rows.iter().filter_map(|row| row.memory_full)),
        avg_process_memory_mb: average(rows.iter().filter_map(|row| row.process_memory_mb)),
        failed_count: rows.iter().filter(|row| row.status == "failed").count(),
    }
}

fn average(values: impl Iterator<Item = f64>) -> Option<f64> {
    let mut count = 0usize;
    let mut sum = 0.0;
    for value in values {
        if value.is_finite() {
            count += 1;
            sum += value;
        }
    }
    (count > 0).then_some(sum / count as f64)
}

fn max_option(values: impl Iterator<Item = f64>) -> Option<f64> {
    values
        .filter(|value| value.is_finite())
        .max_by(|left, right| left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal))
}

fn median_sorted(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mid = values.len() / 2;
    if values.len().is_multiple_of(2) {
        Some((values[mid - 1] + values[mid]) / 2.0)
    } else {
        Some(values[mid])
    }
}

fn option_delta(current: Option<f64>, baseline: Option<f64>) -> Option<f64> {
    Some(current? - baseline?)
}

fn option_ratio(current: Option<f64>, baseline: Option<f64>) -> Option<f64> {
    let baseline = baseline?;
    if baseline.abs() < f64::EPSILON {
        None
    } else {
        Some(current? / baseline)
    }
}

fn fmt_opt_secs(value: Option<f64>) -> String {
    value.map_or_else(|| "-".to_string(), |value| format!("{value:.1}s"))
}

fn fmt_opt_float(value: Option<f64>) -> String {
    value.map_or_else(|| "-".to_string(), |value| format!("{value:.1}"))
}

fn fmt_opt_ratio(value: Option<f64>) -> String {
    value.map_or_else(|| "-".to_string(), |value| format!("{value:.2}x"))
}

fn fmt_delta_secs(value: Option<f64>) -> String {
    value.map_or_else(|| "-".to_string(), |value| format!("{value:+.1}s"))
}

fn print_compare_days_report(report: &DayComparisonReport) {
    println!(
        "History comparison: {} vs {}{}",
        report.day,
        report.against,
        if report.include_failures {
            " (success + failed)"
        } else {
            " (success only)"
        }
    );
    let mut builder = Builder::new();
    builder.push_record([
        "COMMAND",
        "BASE N",
        "DAY N",
        "AVG",
        "AVG Δ",
        "AVG ×",
        "MEDIAN",
        "MEDIAN Δ",
        "TAIL",
        "IO.FULL AVG Δ",
        "MEM.FULL AVG Δ",
    ]);
    for row in &report.rows {
        builder.push_record([
            row.command.clone(),
            row.baseline.invocation_count.to_string(),
            row.current.invocation_count.to_string(),
            fmt_opt_secs(row.current.avg_duration_secs),
            fmt_delta_secs(row.avg_duration_delta_secs),
            fmt_opt_ratio(row.avg_duration_ratio),
            fmt_opt_secs(row.current.median_duration_secs),
            fmt_delta_secs(row.median_duration_delta_secs),
            fmt_opt_secs(row.current.max_duration_secs),
            fmt_opt_float(row.io_full_avg_delta),
            fmt_opt_float(row.memory_full_avg_delta),
        ]);
    }
    let mut table = builder.build();
    table.with(Style::rounded());
    println!("{table}");

    if !report.slowest.is_empty() {
        println!();
        println!("Tail invocations on {}:", report.day);
        let mut builder = Builder::new();
        builder.push_record([
            "ID", "COMMAND", "STATUS", "DURATION", "IO.FULL", "MEM.FULL", "PROC MB", "STARTED",
        ]);
        for row in &report.slowest {
            builder.push_record([
                row.id.to_string(),
                row.command.clone(),
                row.status.clone(),
                format!("{:.1}s", row.duration_secs),
                fmt_opt_float(row.io_full),
                fmt_opt_float(row.memory_full),
                fmt_opt_float(row.process_memory_mb),
                row.started_at.clone(),
            ]);
        }
        let mut table = builder.build();
        table.with(Style::rounded());
        println!("{table}");
    }

    println!("\n{}", style("Evidence limits:").yellow().bold());
    for line in &report.evidence_limits {
        println!("  - {line}");
    }
}

fn resource_row_from_json(
    row: &serde_json::Map<String, serde_json::Value>,
) -> Result<ResourceInvocationRow> {
    Ok(ResourceInvocationRow {
        id: json_i64(row, "id")?,
        command: json_string(row, "command")?,
        status: json_string(row, "status")?,
        started_at: json_string(row, "started_at")?,
        duration_secs: row
            .get("duration_secs")
            .and_then(serde_json::Value::as_f64)
            .ok_or_else(|| color_eyre::eyre::eyre!("history resource row missing duration_secs"))?,
        is_background: row
            .get("is_background")
            .and_then(serde_json::Value::as_i64)
            .is_some_and(|value| value != 0),
        io_full: json_optional_f64(row, "io_full"),
        memory_full: json_optional_f64(row, "memory_full"),
        process_count_max: json_optional_i64(row, "process_count_max"),
        host_block_read_mib: json_optional_f64(row, "host_block_read_mib_delta"),
        host_block_write_mib: json_optional_f64(row, "host_block_write_mib_delta"),
        host_block_read_iops: json_optional_f64(row, "host_block_read_iops_avg"),
        host_block_write_iops: json_optional_f64(row, "host_block_write_iops_avg"),
        host_block_busiest_device: json_optional_string(row, "host_block_busiest_device"),
        host_block_busiest_device_total_mib: json_optional_f64(
            row,
            "host_block_busiest_device_total_mib_delta",
        ),
        host_block_busiest_device_read_iops: json_optional_f64(
            row,
            "host_block_busiest_device_read_iops_avg",
        ),
        host_block_busiest_device_write_iops: json_optional_f64(
            row,
            "host_block_busiest_device_write_iops_avg",
        ),
        host_block_busiest_device_weighted_io_ms_per_s: json_optional_f64(
            row,
            "host_block_busiest_device_weighted_io_ms_per_s",
        ),
        args_json: json_optional_string(row, "args_json"),
    })
}

fn unique_resource_commands(rows: &[ResourceInvocationRow]) -> Vec<String> {
    let mut commands = rows
        .iter()
        .map(|row| row.command.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    commands.sort();
    commands
}

fn summarize_resource_commands(
    rows: &[ResourceInvocationRow],
    commands: &[String],
) -> Vec<ResourceCommandSummary> {
    let mut summaries = commands
        .iter()
        .map(|command| {
            let command_rows = rows
                .iter()
                .filter(|row| row.command == *command)
                .collect::<Vec<_>>();
            summarize_resource_command(command, &command_rows)
        })
        .collect::<Vec<_>>();
    summaries.sort_by(|left, right| {
        right
            .total_duration_hours
            .total_cmp(&left.total_duration_hours)
            .then_with(|| left.command.cmp(&right.command))
    });
    summaries
}

fn summarize_resource_command(
    command: &str,
    rows: &[&ResourceInvocationRow],
) -> ResourceCommandSummary {
    let durations = rows.iter().map(|row| row.duration_secs).collect::<Vec<_>>();
    let process_counts = rows
        .iter()
        .filter_map(|row| row.process_count_max)
        .collect::<Vec<_>>();
    ResourceCommandSummary {
        command: command.to_string(),
        invocation_count: rows.len(),
        failed_count: rows.iter().filter(|row| row.status == "failed").count(),
        cancelled_count: rows.iter().filter(|row| row.status == "cancelled").count(),
        background_count: rows.iter().filter(|row| row.is_background).count(),
        total_duration_hours: secs_to_hours(durations.iter().sum::<f64>()),
        avg_duration_secs: average(durations.iter().copied()),
        max_duration_secs: max_option(durations.iter().copied()),
        avg_io_full: average(rows.iter().filter_map(|row| row.io_full)),
        max_io_full: max_option(rows.iter().filter_map(|row| row.io_full)),
        high_io_full_count: rows
            .iter()
            .filter(|row| {
                row.io_full
                    .is_some_and(|value| value >= crate::resources::thresholds::PSI_IO_FULL_WARN)
            })
            .count(),
        avg_memory_full: average(rows.iter().filter_map(|row| row.memory_full)),
        max_memory_full: max_option(rows.iter().filter_map(|row| row.memory_full)),
        avg_process_count_max: average(process_counts.iter().map(|value| *value as f64)),
        max_process_count_max: process_counts.into_iter().max(),
        host_block_read_mib: rows
            .iter()
            .filter_map(|row| row.host_block_read_mib)
            .sum::<f64>(),
        host_block_write_mib: rows
            .iter()
            .filter_map(|row| row.host_block_write_mib)
            .sum::<f64>(),
        avg_host_block_read_iops: average(rows.iter().filter_map(|row| row.host_block_read_iops)),
        avg_host_block_write_iops: average(rows.iter().filter_map(|row| row.host_block_write_iops)),
    }
}

fn summarize_resource_devices(rows: &[ResourceInvocationRow]) -> Vec<ResourceDeviceSummary> {
    let mut by_device: BTreeMap<String, Vec<&ResourceInvocationRow>> = BTreeMap::new();
    for row in rows {
        if let Some(device) = row.host_block_busiest_device.as_deref() {
            by_device.entry(device.to_string()).or_default().push(row);
        }
    }
    let mut devices = by_device
        .into_iter()
        .map(|(device, device_rows)| ResourceDeviceSummary {
            device,
            invocation_count: device_rows.len(),
            total_mib: device_rows
                .iter()
                .filter_map(|row| row.host_block_busiest_device_total_mib)
                .sum(),
            avg_read_iops: average(
                device_rows
                    .iter()
                    .filter_map(|row| row.host_block_busiest_device_read_iops),
            ),
            avg_write_iops: average(
                device_rows
                    .iter()
                    .filter_map(|row| row.host_block_busiest_device_write_iops),
            ),
            max_weighted_io_ms_per_s: max_option(
                device_rows
                    .iter()
                    .filter_map(|row| row.host_block_busiest_device_weighted_io_ms_per_s),
            ),
        })
        .collect::<Vec<_>>();
    devices.sort_by(|left, right| {
        right
            .total_mib
            .total_cmp(&left.total_mib)
            .then_with(|| right.invocation_count.cmp(&left.invocation_count))
            .then_with(|| left.device.cmp(&right.device))
    });
    devices
}

fn slowest_resource_invocations(
    rows: &[ResourceInvocationRow],
    limit: usize,
) -> Vec<ResourceInvocationSummary> {
    let mut rows = rows.iter().collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        right
            .duration_secs
            .total_cmp(&left.duration_secs)
            .then_with(|| {
                right
                    .io_full
                    .unwrap_or(0.0)
                    .total_cmp(&left.io_full.unwrap_or(0.0))
            })
            .then_with(|| left.id.cmp(&right.id))
    });
    rows.into_iter()
        .take(limit)
        .map(|row| ResourceInvocationSummary {
            id: row.id,
            command: row.command.clone(),
            status: row.status.clone(),
            started_at: row.started_at.clone(),
            duration_secs: row.duration_secs,
            io_full: row.io_full,
            memory_full: row.memory_full,
            process_count_max: row.process_count_max,
            host_block_read_mib: row.host_block_read_mib,
            host_block_write_mib: row.host_block_write_mib,
            host_block_busiest_device: row.host_block_busiest_device.clone(),
            host_block_busiest_device_total_mib: row.host_block_busiest_device_total_mib,
            args_json: row.args_json.clone(),
        })
        .collect()
}

fn print_resources_report(report: &ResourceWindowReport) {
    println!(
        "Recorded xtask resources for {}{}{}:",
        report.window,
        if report.include_background {
            " (foreground + background)"
        } else {
            " (foreground only)"
        },
        if report.success_only {
            ", success only"
        } else {
            ", success/failed/cancelled"
        }
    );
    if report.rows.is_empty() {
        println!("No invocation resource rows found.");
        return;
    }

    let mut builder = Builder::new();
    builder.push_record([
        "COMMAND",
        "N",
        "FAIL",
        "CANCEL",
        "HOURS",
        "AVG",
        "TAIL",
        "IO AVG/MAX",
        "MEM AVG/MAX",
        "PROC AVG/MAX",
        "BLK R/W MiB",
        "BLK R/W IOPS",
    ]);
    for row in &report.rows {
        builder.push_record([
            row.command.clone(),
            row.invocation_count.to_string(),
            row.failed_count.to_string(),
            row.cancelled_count.to_string(),
            format!("{:.2}", row.total_duration_hours),
            fmt_opt_secs(row.avg_duration_secs),
            fmt_opt_secs(row.max_duration_secs),
            format!(
                "{}/{}",
                fmt_opt_float(row.avg_io_full),
                fmt_opt_float(row.max_io_full)
            ),
            format!(
                "{}/{}",
                fmt_opt_float(row.avg_memory_full),
                fmt_opt_float(row.max_memory_full)
            ),
            format!(
                "{}/{}",
                fmt_opt_float(row.avg_process_count_max),
                row.max_process_count_max
                    .map_or_else(|| "-".to_string(), |value| value.to_string())
            ),
            format!(
                "{:.1}/{:.1}",
                row.host_block_read_mib, row.host_block_write_mib
            ),
            format!(
                "{}/{}",
                fmt_opt_float(row.avg_host_block_read_iops),
                fmt_opt_float(row.avg_host_block_write_iops)
            ),
        ]);
    }
    let mut table = builder.build();
    table.with(Style::rounded());
    println!("{table}");

    if !report.top_devices.is_empty() {
        println!("\nBusiest xtask-sampled devices:");
        let mut builder = Builder::new();
        builder.push_record([
            "DEVICE",
            "N",
            "TOTAL MiB",
            "R IOPS",
            "W IOPS",
            "TAIL WIO ms/s",
        ]);
        for device in report.top_devices.iter().take(8) {
            builder.push_record([
                device.device.clone(),
                device.invocation_count.to_string(),
                format!("{:.1}", device.total_mib),
                fmt_opt_float(device.avg_read_iops),
                fmt_opt_float(device.avg_write_iops),
                fmt_opt_float(device.max_weighted_io_ms_per_s),
            ]);
        }
        let mut table = builder.build();
        table.with(Style::rounded());
        println!("{table}");
    }

    if !report.slowest.is_empty() {
        println!("\nTail/high-pressure invocations:");
        let mut builder = Builder::new();
        builder.push_record([
            "ID", "COMMAND", "STATUS", "DURATION", "IO.FULL", "MEM.FULL", "PROCS", "DEVICE",
            "BLK MiB", "STARTED",
        ]);
        for row in &report.slowest {
            let total_block_mib = row.host_block_busiest_device_total_mib.or_else(|| {
                match (row.host_block_read_mib, row.host_block_write_mib) {
                    (Some(read), Some(write)) => Some(read + write),
                    (Some(read), None) => Some(read),
                    (None, Some(write)) => Some(write),
                    (None, None) => None,
                }
            });
            builder.push_record([
                row.id.to_string(),
                row.command.clone(),
                row.status.clone(),
                format!("{:.1}s", row.duration_secs),
                fmt_opt_float(row.io_full),
                fmt_opt_float(row.memory_full),
                row.process_count_max
                    .map_or_else(|| "-".to_string(), |value| value.to_string()),
                row.host_block_busiest_device
                    .as_deref()
                    .unwrap_or("-")
                    .to_string(),
                fmt_opt_float(total_block_mib),
                row.started_at.clone(),
            ]);
        }
        let mut table = builder.build();
        table.with(Style::rounded());
        println!("{table}");
    }

    println!("\n{}", style("Evidence limits:").yellow().bold());
    for limit in &report.evidence_limits {
        println!("  - {limit}");
    }
}
