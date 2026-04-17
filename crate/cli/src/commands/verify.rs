use std::time::Duration;

use clap::Args;
use color_eyre::{Result, eyre::eyre};
use console::{StyledObject, style};
use serde_json::json;
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::events::EventPayload;
use sinex_primitives::events::payloads::{BashCommandExecutedPayload, CanonicalCommandPayload};
use sinex_primitives::query::{
    AggregationMode, EventQuery, EventQueryResult, GroupByField, PayloadFilter, SortDirection,
    TimeRange,
};
use sinex_primitives::rpc::ingest::{EventIngestRequest, EventIngestResponse};
use sinex_primitives::temporal::Timestamp;

use crate::client::GatewayClient;

#[derive(Debug, Args, Default)]
pub struct VerifyCommand {
    /// Publish a synthetic event through `events.ingest` and query it back.
    #[arg(long, default_value_t = false)]
    gateway_smoke: bool,

    /// Actively exercise deployable automata through synthetic gateway-ingested events.
    #[arg(long, default_value_t = false)]
    automata_smoke: bool,

    /// Check whether historical-import event types have been persisted.
    #[arg(long, default_value_t = false)]
    historical_proof: bool,
}

const VERIFY_GATEWAY_SOURCE: &str = "sinexctl.verify";
const VERIFY_GATEWAY_EVENT_TYPE: &str = "test.ping";
const SESSION_DETECTOR_OUTPUT_SOURCE: &str = "derived.session-detector";
const SESSION_DETECTOR_OUTPUT_EVENT_TYPE: &str = "activity.session.boundary";

const TERMINAL_COMMAND_SOURCES: &[&str] = &[
    "shell.kitty",
    "shell.atuin",
    "shell.history.bash",
    "shell.history.zsh",
    "shell.history.fish",
];

const PASSIVE_DERIVED_SIGNAL_CHECKS: &[PassiveSignalCheck] = &[
    PassiveSignalCheck {
        label: "Terminal canonicalizer",
        input_sources: TERMINAL_COMMAND_SOURCES,
        input_event_type: "command.executed",
        output_sources: &["canonical.terminal"],
        output_event_type: "command.canonical",
        idle_message: "No terminal command.executed inputs observed; canonicalizer not evaluated",
        zero_message: "No command.canonical events despite terminal command.executed inputs",
    },
    PassiveSignalCheck {
        label: "Health automaton",
        input_sources: &[],
        input_event_type: "health.status",
        output_sources: &["health-aggregator"],
        output_event_type: "health.aggregated_report",
        idle_message: "No health.status inputs observed; health automaton not evaluated",
        zero_message: "No health.aggregated_report events despite health.status inputs",
    },
];

const HISTORICAL_SIGNAL_CHECKS: &[EventSignalCheck] = &[
    EventSignalCheck {
        label: "Shell history backfill",
        sources: &["shell.history"],
        event_type: "command.imported",
        zero_message: "No shell.history command.imported events found",
    },
    EventSignalCheck {
        label: "Desktop window history backfill",
        sources: &["desktop"],
        event_type: "window.wm_historical",
        zero_message: "No desktop window.wm_historical events found",
    },
    EventSignalCheck {
        label: "Desktop clipboard history backfill",
        sources: &["desktop"],
        event_type: "clipboard.historical",
        zero_message: "No desktop clipboard.historical events found",
    },
];

impl VerifyCommand {
    pub async fn execute(&self, client: &GatewayClient) -> Result<()> {
        println!();
        println!(
            "{}",
            style("Sinex Trustworthiness Verification").bold().cyan()
        );
        println!("{}", style("═".repeat(50)).dim());
        println!();

        let mut summary = VerificationSummary::default();

        let total_events = count_events(client).await?;
        if total_events > 0 {
            summary.pass(format!("Event store has {total_events} events"));
        } else {
            summary.warn("Event store is empty");
        }

        let sources = count_sources(client).await?;
        if sources >= 2 {
            summary.pass(format!("{sources} distinct sources active"));
        } else if sources == 1 {
            summary.warn("Only 1 source active");
        } else {
            summary.fail("No sources producing events");
        }

        for check in PASSIVE_DERIVED_SIGNAL_CHECKS {
            report_passive_signal_check(&mut summary, client, check).await?;
        }

        match client.health().await {
            Ok(health) => {
                if health.healthy {
                    summary.pass("Gateway healthy (DB: ok, NATS: ok)");
                } else {
                    summary.warn(format!(
                        "Gateway degraded: {}",
                        health.degradation_reasons.join(", ")
                    ));
                }
            }
            Err(error) => {
                summary.fail(format!("Gateway health check failed: {error}"));
            }
        }

        let recent = count_recent_events(client).await?;
        if recent > 0 {
            summary.pass(format!(
                "{recent} events in the last hour (pipeline flowing)"
            ));
        } else {
            summary.warn("No events in the last hour — pipeline may be stalled");
        }

        if self.gateway_smoke {
            match run_gateway_smoke(client).await {
                Ok(outcome) => summary.pass(format!(
                    "Gateway smoke round-tripped via events.ingest (event_id {}, sequence {})",
                    outcome.ingest_response.event_id, outcome.ingest_response.sequence
                )),
                Err(error) => summary.fail(format!("Gateway smoke failed: {error}")),
            }
        }

        if self.automata_smoke {
            run_automata_smoke(&mut summary, client).await?;
        } else {
            summary
                .skip("Automata deployment smoke not run — pass --automata-smoke to force outputs");
        }

        if self.historical_proof {
            for check in HISTORICAL_SIGNAL_CHECKS {
                report_signal_check(&mut summary, client, check).await?;
            }
        }

        println!();
        println!("{}", style("─".repeat(50)).dim());
        println!(
            "  {} passed  {} skipped  {} warnings  {} failed",
            style(summary.pass).green().bold(),
            style(summary.skip).dim().bold(),
            style(summary.warn).yellow().bold(),
            style(summary.fail).red().bold(),
        );

        if summary.fail > 0 {
            println!();
            println!(
                "{}",
                style("Verification FAILED — investigate failures above")
                    .red()
                    .bold()
            );
            std::process::exit(1);
        } else if summary.warn > 0 {
            println!();
            println!("{}", style("Verification passed with warnings").yellow());
        } else {
            println!();
            println!("{}", style("All checks passed ✓").green().bold());
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
struct EventSignalCheck {
    label: &'static str,
    sources: &'static [&'static str],
    event_type: &'static str,
    zero_message: &'static str,
}

#[derive(Debug, Clone, Copy)]
struct PassiveSignalCheck {
    label: &'static str,
    input_sources: &'static [&'static str],
    input_event_type: &'static str,
    output_sources: &'static [&'static str],
    output_event_type: &'static str,
    idle_message: &'static str,
    zero_message: &'static str,
}

#[derive(Debug, Default)]
struct VerificationSummary {
    pass: u32,
    skip: u32,
    warn: u32,
    fail: u32,
}

impl VerificationSummary {
    fn pass(&mut self, message: impl AsRef<str>) {
        self.record(VerificationStatus::Pass, message.as_ref());
    }

    fn warn(&mut self, message: impl AsRef<str>) {
        self.record(VerificationStatus::Warn, message.as_ref());
    }

    fn skip(&mut self, message: impl AsRef<str>) {
        self.record(VerificationStatus::Skip, message.as_ref());
    }

    fn fail(&mut self, message: impl AsRef<str>) {
        self.record(VerificationStatus::Fail, message.as_ref());
    }

    fn record(&mut self, status: VerificationStatus, message: &str) {
        println!("{} {}", status.symbol(), message);
        match status {
            VerificationStatus::Pass => self.pass += 1,
            VerificationStatus::Skip => self.skip += 1,
            VerificationStatus::Warn => self.warn += 1,
            VerificationStatus::Fail => self.fail += 1,
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum VerificationStatus {
    Pass,
    Skip,
    Warn,
    Fail,
}

impl VerificationStatus {
    fn symbol(self) -> StyledObject<&'static str> {
        match self {
            Self::Pass => style("✓").green(),
            Self::Skip => style("·").dim(),
            Self::Warn => style("⚠").yellow(),
            Self::Fail => style("✗").red(),
        }
    }
}

#[derive(Debug)]
struct GatewaySmokeOutcome {
    ingest_response: EventIngestResponse,
}

async fn report_signal_check(
    summary: &mut VerificationSummary,
    client: &GatewayClient,
    check: &EventSignalCheck,
) -> Result<()> {
    let count = count_events_matching(client, check.sources, check.event_type).await?;
    if count > 0 {
        summary.pass(format!(
            "{} emitted {} {} events",
            check.label, count, check.event_type
        ));
    } else {
        summary.warn(check.zero_message);
    }
    Ok(())
}

async fn report_passive_signal_check(
    summary: &mut VerificationSummary,
    client: &GatewayClient,
    check: &PassiveSignalCheck,
) -> Result<()> {
    let input_count =
        count_events_matching(client, check.input_sources, check.input_event_type).await?;
    if input_count == 0 {
        summary.skip(check.idle_message);
        return Ok(());
    }

    let output_count =
        count_events_matching(client, check.output_sources, check.output_event_type).await?;
    if output_count > 0 {
        summary.pass(format!(
            "{} emitted {} {} events",
            check.label, output_count, check.output_event_type
        ));
    } else {
        summary.warn(check.zero_message);
    }

    Ok(())
}

async fn count_events(client: &GatewayClient) -> Result<i64> {
    count_query(
        client,
        EventQuery {
            aggregation: Some(AggregationMode::Count),
            ..Default::default()
        },
    )
    .await
}

async fn count_sources(client: &GatewayClient) -> Result<i64> {
    let query = EventQuery {
        aggregation: Some(AggregationMode::CountBy {
            field: GroupByField::Source,
            limit: 100,
        }),
        direction: SortDirection::Desc,
        ..Default::default()
    };
    match client.query_events(query).await? {
        EventQueryResult::GroupedCounts { groups } => Ok(groups.len() as i64),
        other => Err(eyre!(
            "unexpected query result when counting sources: {}",
            result_kind(&other)
        )),
    }
}

async fn count_events_matching(
    client: &GatewayClient,
    sources: &[&str],
    event_type: &str,
) -> Result<i64> {
    let source_filters = sources
        .iter()
        .copied()
        .map(EventSource::new)
        .collect::<Result<Vec<_>, _>>()?;
    let query = EventQuery {
        sources: source_filters,
        event_types: vec![EventType::new(event_type)?],
        aggregation: Some(AggregationMode::Count),
        ..Default::default()
    };
    count_query(client, query).await
}

async fn count_recent_events(client: &GatewayClient) -> Result<i64> {
    let now = Timestamp::now();
    let one_hour_ago = Timestamp::new(now.inner() - time::Duration::hours(1));
    let time_range = TimeRange::new(Some(one_hour_ago), Some(now))?;

    count_query(
        client,
        EventQuery {
            time_range: Some(time_range),
            aggregation: Some(AggregationMode::Count),
            ..Default::default()
        },
    )
    .await
}

async fn count_query(client: &GatewayClient, query: EventQuery) -> Result<i64> {
    match client.query_events(query).await? {
        EventQueryResult::Count { count } => Ok(count),
        other => Err(eyre!(
            "unexpected query result for count query: {}",
            result_kind(&other)
        )),
    }
}

async fn run_gateway_smoke(client: &GatewayClient) -> Result<GatewaySmokeOutcome> {
    let marker = format!("sinexctl-verify-{:016x}", rand::random::<u64>());
    let emitted_at = Timestamp::now();
    let ingest_response = ingest_raw_event(
        client,
        VERIFY_GATEWAY_SOURCE,
        VERIFY_GATEWAY_EVENT_TYPE,
        emitted_at,
        json!({
            "marker": marker,
            "surface": "sinexctl.verify",
            "purpose": "gateway smoke round-trip"
        }),
    )
    .await?;
    let query = gateway_smoke_query(&marker, emitted_at)?;

    for _attempt in 0..20 {
        match client.query_events(query.clone()).await? {
            EventQueryResult::Events { events, .. } if !events.is_empty() => {
                return Ok(GatewaySmokeOutcome { ingest_response });
            }
            EventQueryResult::Events { .. } => {
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            other => {
                return Err(eyre!(
                    "unexpected query result for gateway smoke: {}",
                    result_kind(&other)
                ));
            }
        }
    }

    Err(eyre!(
        "events.ingest accepted the smoke event but it was not queryable within 5s"
    ))
}

async fn run_automata_smoke(
    summary: &mut VerificationSummary,
    client: &GatewayClient,
) -> Result<()> {
    match run_canonicalizer_smoke(client).await {
        Ok(()) => summary.pass("Canonicalizer smoke produced command.canonical output"),
        Err(error) => summary.fail(format!("Canonicalizer smoke failed: {error}")),
    }

    match run_health_smoke(client).await {
        Ok(()) => summary.pass("Health automaton smoke produced health.aggregated_report output"),
        Err(error) => summary.fail(format!("Health automaton smoke failed: {error}")),
    }

    match run_analytics_smoke(client).await {
        Ok(()) => summary.pass("Analytics automaton smoke produced analytics.insight output"),
        Err(error) => summary.fail(format!("Analytics automaton smoke failed: {error}")),
    }

    match run_session_smoke(client).await {
        Ok(()) => summary.pass("Session detector smoke produced activity.session.boundary output"),
        Err(error) => summary.fail(format!("Session detector smoke failed: {error}")),
    }

    Ok(())
}

async fn run_canonicalizer_smoke(client: &GatewayClient) -> Result<()> {
    let input_baseline = count_events_matching(
        client,
        &[BashCommandExecutedPayload::SOURCE.as_static_str()],
        BashCommandExecutedPayload::EVENT_TYPE.as_static_str(),
    )
    .await?;
    let baseline = count_events_matching(
        client,
        &[CanonicalCommandPayload::SOURCE.as_static_str()],
        CanonicalCommandPayload::EVENT_TYPE.as_static_str(),
    )
    .await?;
    let marker = format!("canonicalizer-{:016x}", rand::random::<u64>());
    ingest_raw_event(
        client,
        BashCommandExecutedPayload::SOURCE.as_static_str(),
        BashCommandExecutedPayload::EVENT_TYPE.as_static_str(),
        Timestamp::now(),
        bash_command_payload(&marker),
    )
    .await?;

    wait_for_count_increase(
        client,
        &[BashCommandExecutedPayload::SOURCE.as_static_str()],
        BashCommandExecutedPayload::EVENT_TYPE.as_static_str(),
        input_baseline,
        Duration::from_secs(10),
    )
    .await?;

    wait_for_count_increase(
        client,
        &[CanonicalCommandPayload::SOURCE.as_static_str()],
        CanonicalCommandPayload::EVENT_TYPE.as_static_str(),
        baseline,
        Duration::from_secs(10),
    )
    .await
}

async fn run_health_smoke(client: &GatewayClient) -> Result<()> {
    let baseline =
        count_events_matching(client, &["health-aggregator"], "health.aggregated_report").await?;
    let marker = format!("verify-health-{:016x}", rand::random::<u64>());
    ingest_raw_event(
        client,
        "sinex",
        "health.status",
        Timestamp::now(),
        json!({
            "component": marker,
            "previous_status": "unknown",
            "current_status": "healthy",
        }),
    )
    .await?;

    wait_for_count_increase(
        client,
        &["health-aggregator"],
        "health.aggregated_report",
        baseline,
        Duration::from_secs(10),
    )
    .await
}

async fn run_analytics_smoke(client: &GatewayClient) -> Result<()> {
    let baseline =
        count_events_matching(client, &["analytics-automaton"], "analytics.insight").await?;
    let marker = format!("analytics-{:016x}", rand::random::<u64>());
    let emitted_at = Timestamp::now();

    for index in 0..100 {
        ingest_raw_event(
            client,
            "sinexctl.verify.analytics",
            "test.analytics.ping",
            emitted_at,
            json!({
                "marker": marker,
                "index": index,
            }),
        )
        .await?;
    }

    wait_for_count_increase(
        client,
        &["analytics-automaton"],
        "analytics.insight",
        baseline,
        Duration::from_secs(15),
    )
    .await
}

async fn run_session_smoke(client: &GatewayClient) -> Result<()> {
    let input_baseline = count_events_matching(
        client,
        &[BashCommandExecutedPayload::SOURCE.as_static_str()],
        BashCommandExecutedPayload::EVENT_TYPE.as_static_str(),
    )
    .await?;
    let baseline = count_events_matching(
        client,
        &[SESSION_DETECTOR_OUTPUT_SOURCE],
        SESSION_DETECTOR_OUTPUT_EVENT_TYPE,
    )
    .await?;
    let marker = format!("session-smoke-{:016x}", rand::random::<u64>());
    let session_times = session_smoke_timestamps(Timestamp::now());

    for (ordinal, ts_orig) in session_times.into_iter().enumerate() {
        ingest_raw_event(
            client,
            BashCommandExecutedPayload::SOURCE.as_static_str(),
            BashCommandExecutedPayload::EVENT_TYPE.as_static_str(),
            ts_orig,
            bash_command_payload(&format!("{marker}-{ordinal}")),
        )
        .await?;
    }

    wait_for_count_increase(
        client,
        &[BashCommandExecutedPayload::SOURCE.as_static_str()],
        BashCommandExecutedPayload::EVENT_TYPE.as_static_str(),
        input_baseline,
        Duration::from_secs(10),
    )
    .await?;

    wait_for_count_increase(
        client,
        &[SESSION_DETECTOR_OUTPUT_SOURCE],
        SESSION_DETECTOR_OUTPUT_EVENT_TYPE,
        baseline,
        Duration::from_secs(15),
    )
    .await
}

fn bash_command_payload(marker: &str) -> serde_json::Value {
    json!({
        "command": format!("printf '%s' {marker}"),
        "working_directory": "/tmp",
        "exit_code": 0,
        "duration_ms": 1,
        "user": "sinexctl-verify",
        "session_id": marker,
        "environment_hash": null,
    })
}

fn session_smoke_timestamps(now: Timestamp) -> [Timestamp; 3] {
    let first = Timestamp::new(now.inner() + time::Duration::hours(1));
    let second = Timestamp::new(first.inner() + time::Duration::seconds(1));
    let third = Timestamp::new(first.inner() + time::Duration::minutes(6));
    [first, second, third]
}

async fn ingest_raw_event(
    client: &GatewayClient,
    source: &str,
    event_type: &str,
    ts_orig: Timestamp,
    payload: serde_json::Value,
) -> Result<EventIngestResponse> {
    client
        .ingest_event(EventIngestRequest {
            source: source.to_string(),
            event_type: event_type.to_string(),
            payload,
            ts_orig: ts_orig.format_rfc3339(),
            host: None,
        })
        .await
}

async fn wait_for_count_increase(
    client: &GatewayClient,
    sources: &[&str],
    event_type: &str,
    baseline: i64,
    timeout: Duration,
) -> Result<()> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let current = count_events_matching(client, sources, event_type).await?;
        if current > baseline {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            let source_label = if sources.is_empty() {
                "any-source".to_string()
            } else {
                sources.join(",")
            };
            return Err(eyre!(
                "expected {} {} count to increase beyond {}, but it stayed at {}",
                source_label,
                event_type,
                baseline,
                current
            ));
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

fn gateway_smoke_query(marker: &str, emitted_at: Timestamp) -> Result<EventQuery> {
    let start = Timestamp::new(emitted_at.inner() - time::Duration::minutes(1));
    let end = Timestamp::new(emitted_at.inner() + time::Duration::minutes(2));
    let time_range = TimeRange::new(Some(start), Some(end))?;

    Ok(EventQuery {
        sources: vec![EventSource::new(VERIFY_GATEWAY_SOURCE)?],
        event_types: vec![EventType::new(VERIFY_GATEWAY_EVENT_TYPE)?],
        payload: Some(PayloadFilter::Contains {
            value: json!({ "marker": marker }),
        }),
        time_range: Some(time_range),
        limit: 5,
        ..Default::default()
    })
}

fn result_kind(result: &EventQueryResult) -> &'static str {
    match result {
        EventQueryResult::Events { .. } => "events",
        EventQueryResult::Count { .. } => "count",
        EventQueryResult::GroupedCounts { .. } => "grouped_counts",
        EventQueryResult::TimeSeries { .. } => "time_series",
        EventQueryResult::SourceStats { .. } => "source_stats",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xtask::sandbox::prelude::*;

    #[sinex_test]
    async fn gateway_smoke_query_is_precisely_scoped() -> TestResult<()> {
        let emitted_at =
            Timestamp::parse_rfc3339("2026-04-17T12:00:00Z").expect("timestamp should parse");
        let query = gateway_smoke_query("marker-123", emitted_at).expect("query should build");

        assert_eq!(query.sources.len(), 1);
        assert_eq!(query.sources[0].as_str(), VERIFY_GATEWAY_SOURCE);
        assert_eq!(query.event_types.len(), 1);
        assert_eq!(query.event_types[0].as_str(), VERIFY_GATEWAY_EVENT_TYPE);
        assert!(matches!(
            query.payload,
            Some(PayloadFilter::Contains { value })
                if value == json!({ "marker": "marker-123" })
        ));

        let time_range = query.time_range.expect("time range should be present");
        assert_eq!(
            time_range.start().expect("start").format_rfc3339(),
            "2026-04-17T11:59:00Z"
        );
        assert_eq!(
            time_range.end().expect("end").format_rfc3339(),
            "2026-04-17T12:02:00Z"
        );
        Ok(())
    }

    #[sinex_test]
    async fn session_smoke_timestamps_force_a_gap_after_seeding() -> TestResult<()> {
        let now = Timestamp::parse_rfc3339("2026-04-17T12:00:00Z").expect("timestamp should parse");
        let [first, second, third] = session_smoke_timestamps(now);

        assert!(first > now);
        assert!(second > first);
        assert!(third > second);
        assert_eq!(second - first, time::Duration::seconds(1));
        assert_eq!(
            third - second,
            time::Duration::minutes(5) + time::Duration::seconds(59)
        );
        Ok(())
    }
}
