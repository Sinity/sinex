use color_eyre::eyre::Result;
use serde::Serialize;
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use tabled::{builder::Builder, settings::Style};

use crate::command::{CommandContext, CommandResult};
use crate::history::{HistoryDb, WrapperEventRow};

use super::parse_history_time;

#[derive(Debug, Clone, serde::Deserialize)]
struct RawWrapperEvent {
    #[serde(default)]
    event: String,
    #[serde(default)]
    status: String,
    started_at: String,
    #[serde(default)]
    finished_at: Option<String>,
    #[serde(default)]
    duration_ms: Option<u64>,
    #[serde(default)]
    command: Option<String>,
    #[serde(default)]
    args: Option<String>,
    #[serde(default)]
    force_rebuild: bool,
    #[serde(default)]
    log_path: Option<String>,
    #[serde(default)]
    rebuild_trigger: Option<WrapperRebuildTrigger>,
    #[serde(default)]
    stage_durations_ms: BTreeMap<String, u64>,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize)]
pub(super) struct WrapperRebuildTrigger {
    pub(super) reason: String,
    #[serde(default)]
    pub(super) ref_path: Option<String>,
    #[serde(default)]
    pub(super) inputs: Vec<WrapperRebuildTriggerInput>,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize)]
pub(super) struct WrapperRebuildTriggerInput {
    pub(super) path: String,
    #[serde(default)]
    pub(super) rel_path: Option<String>,
    pub(super) kind: String,
    pub(super) status: String,
    #[serde(default)]
    pub(super) mtime_epoch: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct WrapperEvent {
    pub(super) event: String,
    pub(super) status: String,
    pub(super) started_at: String,
    pub(super) finished_at: Option<String>,
    pub(super) duration_secs: Option<f64>,
    pub(super) command: Option<String>,
    pub(super) args: Option<String>,
    pub(super) force_rebuild: bool,
    pub(super) log_path: Option<String>,
    pub(super) rebuild_trigger: Option<WrapperRebuildTrigger>,
    pub(super) stage_durations_ms: BTreeMap<String, u64>,
    pub(super) top_stage: Option<WrapperStageSummary>,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct WrapperStageSummary {
    pub(super) name: String,
    pub(super) duration_secs: f64,
}

#[derive(Debug, Clone, Serialize)]
struct WrapperEventsReport {
    days: u32,
    path: String,
    event_count: usize,
    total_duration_secs: f64,
    stage_totals: Vec<WrapperStageTotal>,
    trigger_totals: Vec<WrapperTriggerTotal>,
    events: Vec<WrapperEvent>,
    skipped_lines: usize,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct WrapperStageTotal {
    pub(super) name: String,
    pub(super) duration_secs: f64,
    pub(super) pct_of_total: f64,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct WrapperTriggerTotal {
    pub(super) reason: String,
    pub(super) count: usize,
    pub(super) duration_secs: f64,
}

pub(super) fn execute_wrapper_events(
    days: u32,
    limit: usize,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let path = wrapper_events_path(ctx.history_db_path());
    let cutoff = time::OffsetDateTime::now_utc() - time::Duration::days(i64::from(days));
    let (mut events, skipped_lines) = read_wrapper_events(&path, cutoff)?;

    // Persist parsed events into the history DB so checkout-local rebuild cost is
    // SQL-queryable (`xtask history query`) and joinable with `invocations`,
    // instead of living only in the append-only JSONL. Best-effort: a read-only
    // or busy DB must not fail the report.
    if let Ok(db) = HistoryDb::open(ctx.history_db_path()) {
        let rows: Vec<WrapperEventRow> = events.iter().map(wrapper_event_to_row).collect();
        let _ = db.upsert_wrapper_events(&rows);
    }

    events.sort_by(|left, right| right.started_at.cmp(&left.started_at));
    events.truncate(limit);

    let total_duration_secs = events
        .iter()
        .filter_map(|event| event.duration_secs)
        .sum::<f64>();
    let stage_totals = wrapper_stage_totals(&events, total_duration_secs);
    let trigger_totals = wrapper_trigger_totals(&events);
    let report = WrapperEventsReport {
        days,
        path: path.display().to_string(),
        event_count: events.len(),
        total_duration_secs,
        stage_totals,
        trigger_totals,
        events,
        skipped_lines,
    };

    if ctx.is_human() {
        println!(
            "Wrapper events over {} day(s): {} row(s), {:.1}s total",
            report.days, report.event_count, report.total_duration_secs
        );
        println!("Source: {}", report.path);
        if report.events.is_empty() {
            println!("No wrapper rebuild events recorded.");
        } else {
            let mut builder = Builder::new();
            builder.push_record([
                "STARTED",
                "EVENT",
                "STATUS",
                "SECS",
                "COMMAND",
                "FORCE",
                "TRIGGER",
                "TOP STAGE",
            ]);
            for event in &report.events {
                builder.push_record([
                    event.started_at.clone(),
                    event.event.clone(),
                    event.status.clone(),
                    event
                        .duration_secs
                        .map(|secs| format!("{secs:.1}"))
                        .unwrap_or_else(|| "-".to_string()),
                    event.command.clone().unwrap_or_else(|| "-".to_string()),
                    event.force_rebuild.to_string(),
                    wrapper_trigger_summary(event),
                    event
                        .top_stage
                        .as_ref()
                        .map(|stage| format!("{} {:.1}s", stage.name, stage.duration_secs))
                        .unwrap_or_else(|| "-".to_string()),
                ]);
            }
            let mut table = builder.build();
            table.with(Style::rounded());
            println!("{table}");

            if !report.stage_totals.is_empty() {
                println!("\nStage totals:");
                let mut stages = Builder::new();
                stages.push_record(["STAGE", "SECS", "% WALL"]);
                for stage in &report.stage_totals {
                    stages.push_record([
                        stage.name.clone(),
                        format!("{:.1}", stage.duration_secs),
                        format!("{:.1}%", stage.pct_of_total),
                    ]);
                }
                let mut table = stages.build();
                table.with(Style::rounded());
                println!("{table}");
            }

            if !report.trigger_totals.is_empty() {
                println!("\nTrigger totals:");
                let mut triggers = Builder::new();
                triggers.push_record(["TRIGGER", "EVENTS", "SECS"]);
                for trigger in &report.trigger_totals {
                    triggers.push_record([
                        trigger.reason.clone(),
                        trigger.count.to_string(),
                        format!("{:.1}", trigger.duration_secs),
                    ]);
                }
                let mut table = triggers.build();
                table.with(Style::rounded());
                println!("{table}");
            }
        }
        if report.skipped_lines > 0 {
            println!(
                "Skipped {} malformed wrapper event line(s).",
                report.skipped_lines
            );
        }
    } else {
        ctx.print_json(&report)?;
    }

    Ok(CommandResult::success()
        .with_message("Loaded wrapper event history")
        .with_duration(ctx.elapsed())
        .with_data(serde_json::to_value(report)?))
}

fn wrapper_event_to_row(event: &WrapperEvent) -> WrapperEventRow {
    WrapperEventRow {
        event: event.event.clone(),
        status: event.status.clone(),
        started_at: event.started_at.clone(),
        finished_at: event.finished_at.clone(),
        duration_secs: event.duration_secs,
        command: event.command.clone(),
        args: event.args.clone(),
        force_rebuild: event.force_rebuild,
        rebuild_reason: event.rebuild_trigger.as_ref().map(|t| t.reason.clone()),
        stage_durations_json: serde_json::to_string(&event.stage_durations_ms).ok(),
    }
}

pub(super) fn wrapper_events_path(history_db_path: &Path) -> std::path::PathBuf {
    history_db_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("xtask-wrapper-events.jsonl")
}

pub(super) fn read_wrapper_events(
    path: &Path,
    cutoff: time::OffsetDateTime,
) -> Result<(Vec<WrapperEvent>, usize)> {
    let Ok(content) = fs::read_to_string(path) else {
        return Ok((Vec::new(), 0));
    };
    let mut events = Vec::new();
    let mut skipped_lines = 0;

    if !content.trim().is_empty() {
        let mut parsed_all = true;
        for item in serde_json::Deserializer::from_str(&content).into_iter::<RawWrapperEvent>() {
            match item {
                Ok(raw) => {
                    if let Ok(Some(event)) = wrapper_event_from_raw(raw, cutoff) {
                        events.push(event);
                    }
                }
                Err(_) => {
                    parsed_all = false;
                    break;
                }
            }
        }
        if parsed_all {
            return Ok((events, 0));
        }
    }
    events.clear();

    for line in content.lines().filter(|line| !line.trim().is_empty()) {
        let Ok(raw) = serde_json::from_str::<RawWrapperEvent>(line) else {
            skipped_lines += 1;
            continue;
        };
        match wrapper_event_from_raw(raw, cutoff) {
            Ok(Some(event)) => events.push(event),
            Ok(None) => {}
            Err(()) => {
                skipped_lines += 1;
                continue;
            }
        }
    }

    Ok((events, skipped_lines))
}

fn wrapper_event_from_raw(
    raw: RawWrapperEvent,
    cutoff: time::OffsetDateTime,
) -> Result<Option<WrapperEvent>, ()> {
    let Ok(started_at) = parse_history_time(&raw.started_at, "wrapper event started_at") else {
        return Err(());
    };
    if started_at < cutoff {
        return Ok(None);
    }
    Ok(Some(WrapperEvent {
        event: raw.event,
        status: raw.status,
        started_at: raw.started_at,
        finished_at: raw.finished_at,
        duration_secs: raw.duration_ms.map(|ms| ms as f64 / 1000.0),
        command: raw.command.filter(|command| !command.is_empty()),
        args: raw.args,
        force_rebuild: raw.force_rebuild,
        log_path: raw.log_path,
        rebuild_trigger: raw.rebuild_trigger,
        top_stage: wrapper_top_stage(&raw.stage_durations_ms),
        stage_durations_ms: raw.stage_durations_ms,
    }))
}

pub(super) fn wrapper_trigger_summary(event: &WrapperEvent) -> String {
    let Some(trigger) = &event.rebuild_trigger else {
        return "-".to_string();
    };
    let Some(first_input) = trigger.inputs.first() else {
        return trigger.reason.clone();
    };
    let input = first_input
        .rel_path
        .as_deref()
        .filter(|path| !path.is_empty())
        .unwrap_or(first_input.path.as_str());
    let remaining = trigger.inputs.len().saturating_sub(1);
    if remaining == 0 {
        format!("{}: {input}", trigger.reason)
    } else {
        format!("{}: {input} +{remaining}", trigger.reason)
    }
}

fn wrapper_top_stage(stages: &BTreeMap<String, u64>) -> Option<WrapperStageSummary> {
    stages
        .iter()
        .max_by_key(|(_, duration_ms)| *duration_ms)
        .map(|(name, duration_ms)| WrapperStageSummary {
            name: name.clone(),
            duration_secs: *duration_ms as f64 / 1000.0,
        })
}

pub(super) fn wrapper_stage_totals(
    events: &[WrapperEvent],
    total_duration_secs: f64,
) -> Vec<WrapperStageTotal> {
    let mut totals = BTreeMap::<String, u64>::new();
    for event in events {
        for (stage, duration_ms) in &event.stage_durations_ms {
            *totals.entry(stage.clone()).or_default() += *duration_ms;
        }
    }

    let mut rows = totals
        .into_iter()
        .map(|(name, duration_ms)| {
            let duration_secs = duration_ms as f64 / 1000.0;
            WrapperStageTotal {
                name,
                duration_secs,
                pct_of_total: if total_duration_secs > 0.0 {
                    duration_secs / total_duration_secs * 100.0
                } else {
                    0.0
                },
            }
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        right
            .duration_secs
            .partial_cmp(&left.duration_secs)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.name.cmp(&right.name))
    });
    rows
}

pub(super) fn wrapper_trigger_totals(events: &[WrapperEvent]) -> Vec<WrapperTriggerTotal> {
    let mut totals = BTreeMap::<String, (usize, f64)>::new();
    for event in events {
        let reason = event
            .rebuild_trigger
            .as_ref()
            .map(|trigger| trigger.reason.clone())
            .unwrap_or_else(|| "unknown".to_string());
        let entry = totals.entry(reason).or_default();
        entry.0 += 1;
        entry.1 += event.duration_secs.unwrap_or_default();
    }

    let mut rows = totals
        .into_iter()
        .map(|(reason, (count, duration_secs))| WrapperTriggerTotal {
            reason,
            count,
            duration_secs,
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        right
            .duration_secs
            .partial_cmp(&left.duration_secs)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| right.count.cmp(&left.count))
            .then_with(|| left.reason.cmp(&right.reason))
    });
    rows
}
