use std::collections::{BTreeMap, BTreeSet};

use color_eyre::eyre::Result;
use tabled::{builder::Builder, settings::Style};

use crate::command::{CommandContext, CommandResult};
use crate::history::HistoryDb;

use super::{
    format_history_cutoff_timestamp, json_i64, json_string, parse_history_time, secs_to_hours,
    sql_string_literal,
};

#[derive(Debug, Clone)]
pub(super) struct CostInvocationRow {
    pub(super) id: i64,
    pub(super) command: String,
    pub(super) args_json: Option<String>,
    pub(super) started_at: time::OffsetDateTime,
    pub(super) finished_at: time::OffsetDateTime,
    pub(super) duration_secs: Option<f64>,
    pub(super) status: String,
    pub(super) cancel_reason: Option<String>,
    pub(super) is_background: bool,
    pub(super) tree_fingerprint: Option<String>,
    pub(super) scope_key: Option<String>,
    pub(super) is_stale_cleanup: bool,
}

#[derive(Debug, Clone, serde::Serialize)]
pub(super) struct RepeatedProofCandidate {
    pub(super) command: String,
    scope_key: String,
    tree_fingerprint: String,
    pub(super) run_count: usize,
    pub(super) repeated_invocation_count: usize,
    total_hours: f64,
    pub(super) repeated_hours: f64,
    pub(super) invocation_ids: Vec<i64>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub(super) struct HistoryCostSummary {
    days: u32,
    commands: Vec<String>,
    pub(super) invocation_count: usize,
    pub(super) stale_cleanup_rows_excluded: usize,
    pub(super) raw_invocation_hours: f64,
    pub(super) wrapper_invocation_hours: f64,
    pub(super) wrapper_wait_hours: f64,
    pub(super) non_wrapper_invocation_hours: f64,
    pub(super) unique_wall_hours: f64,
    pub(super) stage_hours: f64,
    wrapper_adjustment_hours: f64,
    pub(super) overlap_after_wrapper_adjustment_hours: f64,
    pub(super) stage_unaccounted_hours: f64,
    pub(super) cancelled_foreground_hours: f64,
    pub(super) stale_pid_rows: usize,
    pub(super) stale_pid_hours: f64,
    pub(super) repeated_proof_hours: f64,
    pub(super) repeated_proof_candidates: Vec<RepeatedProofCandidate>,
}

pub(super) fn execute_cost(
    db: &HistoryDb,
    commands: &[String],
    days: u32,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let commands = if commands.is_empty() {
        vec!["check".to_string(), "test".to_string()]
    } else {
        commands.to_vec()
    };
    let since = format_history_cutoff_timestamp(
        time::OffsetDateTime::now_utc() - time::Duration::days(i64::from(days)),
        "history cost cutoff",
    )?;
    let command_list = commands
        .iter()
        .map(|command| sql_string_literal(command))
        .collect::<Vec<_>>()
        .join(", ");

    let rows_sql = format!(
        r"
        SELECT id, command, args_json, started_at, finished_at, duration_secs,
               status, cancel_reason, is_background,
               tree_fingerprint, scope_key,
               COALESCE(cancel_reason = 'stale_pid' AND cancelled_by = 'open_time_sweep', 0)
                   AS is_stale_cleanup
        FROM invocations
        WHERE command IN ({command_list})
          AND started_at >= {}
          AND finished_at IS NOT NULL
        ORDER BY started_at ASC
        ",
        sql_string_literal(&since)
    );
    let stage_sql = format!(
        r"
        SELECT COALESCE(SUM(st.duration_secs), 0.0) AS stage_secs
        FROM stage_timings st
        JOIN invocations inv ON inv.id = st.invocation_id
        WHERE inv.command IN ({command_list})
          AND inv.started_at >= {}
          AND inv.finished_at IS NOT NULL
          AND NOT COALESCE(
              inv.cancel_reason = 'stale_pid' AND inv.cancelled_by = 'open_time_sweep',
              0
          )
        ",
        sql_string_literal(&since)
    );

    let rows = db
        .run_readonly_query(&rows_sql)?
        .into_iter()
        .map(|row| cost_row_from_json(&row))
        .collect::<Result<Vec<_>>>()?;
    let stage_secs = db
        .run_readonly_query(&stage_sql)?
        .first()
        .and_then(|row| row.get("stage_secs"))
        .and_then(serde_json::Value::as_f64)
        .unwrap_or(0.0);

    let summary = build_history_cost_summary(days, commands, &rows, stage_secs);

    if ctx.is_human() {
        println!(
            "Dev-loop cost for {} over {} days:",
            summary.commands.join(", "),
            summary.days
        );
        let mut builder = Builder::new();
        builder.push_record(["METRIC", "HOURS"]);
        builder.push_record([
            "raw invocation sum".to_string(),
            format!("{:.2}", summary.raw_invocation_hours),
        ]);
        builder.push_record([
            "provable wrapper rows".to_string(),
            format!("{:.2}", summary.wrapper_invocation_hours),
        ]);
        builder.push_record([
            "wrapper wait".to_string(),
            format!("{:.2}", summary.wrapper_wait_hours),
        ]);
        builder.push_record([
            "non-wrapper request sum".to_string(),
            format!("{:.2}", summary.non_wrapper_invocation_hours),
        ]);
        builder.push_record([
            "unique wallclock".to_string(),
            format!("{:.2}", summary.unique_wall_hours),
        ]);
        builder.push_record([
            "recorded stage time".to_string(),
            format!("{:.2}", summary.stage_hours),
        ]);
        builder.push_record([
            "post-wrapper overlap".to_string(),
            format!("{:.2}", summary.overlap_after_wrapper_adjustment_hours),
        ]);
        builder.push_record([
            "non-wrapper time without stages".to_string(),
            format!("{:.2}", summary.stage_unaccounted_hours),
        ]);
        builder.push_record([
            "cancelled foreground".to_string(),
            format!("{:.2}", summary.cancelled_foreground_hours),
        ]);
        builder.push_record([
            "stale-pid rows".to_string(),
            summary.stale_pid_rows.to_string(),
        ]);
        builder.push_record([
            "stale-pid recorded duration".to_string(),
            format!("{:.2}", summary.stale_pid_hours),
        ]);
        builder.push_record([
            "repeated proof candidates".to_string(),
            format!("{:.2}", summary.repeated_proof_hours),
        ]);
        let mut table = builder.build();
        table.with(Style::rounded());
        println!("{table}");
        println!(
            "Rows: {} included, {} stale cleanup row(s) excluded.",
            summary.invocation_count, summary.stale_cleanup_rows_excluded
        );
    } else {
        ctx.print_json(&summary)?;
    }

    Ok(CommandResult::success()
        .with_message("Computed dev-loop cost summary")
        .with_duration(ctx.elapsed())
        .with_data(serde_json::to_value(summary)?))
}

fn cost_row_from_json(
    row: &serde_json::Map<String, serde_json::Value>,
) -> Result<CostInvocationRow> {
    let id = json_i64(row, "id")?;
    let command = json_string(row, "command")?;
    let args_json = row
        .get("args_json")
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned);
    let started_at = parse_history_time(&json_string(row, "started_at")?, "started_at")?;
    let finished_at = parse_history_time(&json_string(row, "finished_at")?, "finished_at")?;
    let duration_secs = row.get("duration_secs").and_then(serde_json::Value::as_f64);
    let status = json_string(row, "status")?;
    let cancel_reason = row
        .get("cancel_reason")
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned);
    let is_background = row
        .get("is_background")
        .and_then(serde_json::Value::as_i64)
        .is_some_and(|value| value != 0);
    let tree_fingerprint = row
        .get("tree_fingerprint")
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned);
    let scope_key = row
        .get("scope_key")
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned);
    let is_stale_cleanup = row
        .get("is_stale_cleanup")
        .and_then(serde_json::Value::as_i64)
        .is_some_and(|value| value != 0);

    Ok(CostInvocationRow {
        id,
        command,
        args_json,
        started_at,
        finished_at,
        duration_secs,
        status,
        cancel_reason,
        is_background,
        tree_fingerprint,
        scope_key,
        is_stale_cleanup,
    })
}

pub(super) fn build_history_cost_summary(
    days: u32,
    commands: Vec<String>,
    rows: &[CostInvocationRow],
    stage_secs: f64,
) -> HistoryCostSummary {
    let stale_cleanup_rows_excluded = rows.iter().filter(|row| row.is_stale_cleanup).count();
    let included_rows = rows
        .iter()
        .filter(|row| !row.is_stale_cleanup)
        .collect::<Vec<_>>();
    let wrapper_ids = provable_wrapper_invocation_ids(&included_rows);
    let repeated_proof_candidates = repeated_proof_candidates(&included_rows, &wrapper_ids);

    let raw_secs = included_rows
        .iter()
        .filter_map(|row| row.duration_secs)
        .sum::<f64>();
    let wrapper_secs = included_rows
        .iter()
        .filter(|row| wrapper_ids.contains(&row.id))
        .filter_map(|row| row.duration_secs)
        .sum::<f64>();
    let non_wrapper_secs = raw_secs - wrapper_secs;
    let unique_wall_secs = unique_wall_secs(&included_rows);
    let overlap_after_wrapper_secs = (non_wrapper_secs - unique_wall_secs).max(0.0);
    let stage_unaccounted_secs = (non_wrapper_secs - stage_secs).max(0.0);
    let cancelled_foreground_secs = included_rows
        .iter()
        .filter(|row| row.status == "cancelled" && !row.is_background)
        .filter_map(|row| row.duration_secs)
        .sum::<f64>();
    let stale_pid_rows = rows
        .iter()
        .filter(|row| row.cancel_reason.as_deref() == Some("stale_pid"))
        .count();
    let stale_pid_secs = rows
        .iter()
        .filter(|row| row.cancel_reason.as_deref() == Some("stale_pid"))
        .filter_map(|row| row.duration_secs)
        .sum::<f64>();
    let repeated_proof_hours = repeated_proof_candidates
        .iter()
        .map(|candidate| candidate.repeated_hours)
        .sum::<f64>();

    HistoryCostSummary {
        days,
        commands,
        invocation_count: included_rows.len(),
        stale_cleanup_rows_excluded,
        raw_invocation_hours: secs_to_hours(raw_secs),
        wrapper_invocation_hours: secs_to_hours(wrapper_secs),
        wrapper_wait_hours: secs_to_hours(wrapper_secs),
        non_wrapper_invocation_hours: secs_to_hours(non_wrapper_secs),
        unique_wall_hours: secs_to_hours(unique_wall_secs),
        stage_hours: secs_to_hours(stage_secs),
        wrapper_adjustment_hours: secs_to_hours(wrapper_secs),
        overlap_after_wrapper_adjustment_hours: secs_to_hours(overlap_after_wrapper_secs),
        stage_unaccounted_hours: secs_to_hours(stage_unaccounted_secs),
        cancelled_foreground_hours: secs_to_hours(cancelled_foreground_secs),
        stale_pid_rows,
        stale_pid_hours: secs_to_hours(stale_pid_secs),
        repeated_proof_hours,
        repeated_proof_candidates,
    }
}

fn provable_wrapper_invocation_ids(rows: &[&CostInvocationRow]) -> BTreeSet<i64> {
    let mut wrapper_ids = BTreeSet::new();
    for candidate in rows {
        if !is_potential_wrapper(candidate) {
            continue;
        }
        if rows.iter().any(|other| {
            candidate.id != other.id
                && candidate.command == other.command
                && is_scoped_child_invocation(other)
                && candidate.started_at <= other.started_at
                && candidate.finished_at >= other.finished_at
        }) {
            wrapper_ids.insert(candidate.id);
        }
    }
    wrapper_ids
}

fn is_potential_wrapper(row: &CostInvocationRow) -> bool {
    matches!(row.command.as_str(), "test" | "ci")
        && row
            .args_json
            .as_deref()
            .is_none_or(|args| !args.contains("--scope="))
}

fn is_scoped_child_invocation(row: &CostInvocationRow) -> bool {
    row.args_json
        .as_deref()
        .is_some_and(|args| args.contains("--scope="))
}

fn unique_wall_secs(rows: &[&CostInvocationRow]) -> f64 {
    let mut intervals = rows
        .iter()
        .map(|row| (row.started_at, row.finished_at))
        .collect::<Vec<_>>();
    intervals.sort_by_key(|(start, _)| *start);

    let mut total = 0.0;
    let mut current: Option<(time::OffsetDateTime, time::OffsetDateTime)> = None;
    for (start, end) in intervals {
        match current {
            None => current = Some((start, end)),
            Some((cur_start, cur_end)) if start <= cur_end => {
                current = Some((cur_start, cur_end.max(end)));
            }
            Some((cur_start, cur_end)) => {
                total += (cur_end - cur_start).as_seconds_f64();
                current = Some((start, end));
            }
        }
    }
    if let Some((start, end)) = current {
        total += (end - start).as_seconds_f64();
    }
    total
}

fn repeated_proof_candidates(
    rows: &[&CostInvocationRow],
    wrapper_ids: &BTreeSet<i64>,
) -> Vec<RepeatedProofCandidate> {
    let mut groups: BTreeMap<(String, String, String), Vec<&CostInvocationRow>> = BTreeMap::new();

    for row in rows {
        if wrapper_ids.contains(&row.id) || row.status != "success" {
            continue;
        }
        let Some(tree_fingerprint) = row.tree_fingerprint.as_deref() else {
            continue;
        };
        let Some(scope_key) = row.scope_key.as_deref() else {
            continue;
        };
        groups
            .entry((
                row.command.clone(),
                scope_key.to_string(),
                tree_fingerprint.to_string(),
            ))
            .or_default()
            .push(*row);
    }

    let mut candidates = groups
        .into_iter()
        .filter_map(|((command, scope_key, tree_fingerprint), mut group)| {
            if group.len() < 2 {
                return None;
            }
            group.sort_by_key(|row| row.started_at);
            let total_secs = group
                .iter()
                .filter_map(|row| row.duration_secs)
                .sum::<f64>();
            let repeated_secs = group
                .iter()
                .skip(1)
                .filter_map(|row| row.duration_secs)
                .sum::<f64>();
            if repeated_secs <= 0.0 {
                return None;
            }

            Some(RepeatedProofCandidate {
                command,
                scope_key,
                tree_fingerprint,
                run_count: group.len(),
                repeated_invocation_count: group.len() - 1,
                total_hours: secs_to_hours(total_secs),
                repeated_hours: secs_to_hours(repeated_secs),
                invocation_ids: group.into_iter().map(|row| row.id).collect(),
            })
        })
        .collect::<Vec<_>>();

    candidates.sort_by(|left, right| {
        right
            .repeated_hours
            .total_cmp(&left.repeated_hours)
            .then_with(|| left.command.cmp(&right.command))
            .then_with(|| left.scope_key.cmp(&right.scope_key))
    });
    candidates.truncate(10);
    candidates
}
