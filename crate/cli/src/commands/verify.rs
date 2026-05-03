use std::{collections::BTreeSet, path::PathBuf, time::Duration};

use clap::Args;
use color_eyre::{Result, eyre::eyre};
use console::{StyledObject, style};
use serde::Serialize;
use serde_json::json;
use sinex_primitives::DeploymentReadinessDescriptor;
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::query::{EventQuery, EventQueryResult, PayloadFilter, TimeRange};
use sinex_primitives::temporal::Timestamp;
use tokio::process::Command;

use crate::client::GatewayClient;
use crate::fmt::{format_json, format_yaml};
use crate::model::OutputFormat;

#[derive(Debug, Args, Default)]
pub struct VerifyCommand {
    /// Actively exercise the local managed document scan surface.
    #[arg(long, default_value_t = false)]
    document_smoke: bool,

    /// Require each enabled long-running collector surface to show recent or historical event evidence.
    #[arg(long, default_value_t = false)]
    source_proof: bool,

    /// Check whether historical-import event types have been persisted.
    #[arg(long, default_value_t = false)]
    historical_proof: bool,
}

const DOCUMENT_INGESTOR_SOURCE: &str = "document-ingestor";
const DOCUMENT_INGESTED_EVENT_TYPE: &str = "document.ingested";
const SOURCE_PROOF_RECENT_WINDOW: Duration = Duration::from_hours(1);
const VERIFY_EVENT_SAMPLE_LIMIT: i64 = 25;

const TERMINAL_COMMAND_SOURCES: &[&str] = &[
    "shell.kitty",
    "shell.atuin",
    "shell.history.bash",
    "shell.history.zsh",
    "shell.history.fish",
];
const TERMINAL_PROOF_SOURCES: &[&str] = &[
    "shell.kitty",
    "shell.atuin",
    "shell.history.bash",
    "shell.history.zsh",
    "shell.history.fish",
    "shell.history",
];
const BROWSER_PROOF_SOURCES: &[&str] = &["webhistory"];
const DESKTOP_PROOF_SOURCES: &[&str] = &["desktop", "activitywatch", "clipboard", "wm.hyprland"];
const FILESYSTEM_PROOF_SOURCES: &[&str] = &["fs-watcher"];
const SYSTEM_PROOF_SOURCES: &[&str] = &["system", "journald", "systemd", "dbus", "udev"];

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
        label: "Analytics automaton",
        input_sources: TERMINAL_COMMAND_SOURCES,
        input_event_type: "command.executed",
        output_sources: &["derived.activity-window"],
        output_event_type: "activity.window.summary",
        idle_message: "No terminal command.executed inputs observed; analytics automaton not evaluated",
        zero_message: "No activity.window.summary events despite terminal command.executed inputs",
    },
    PassiveSignalCheck {
        label: "Session detector",
        input_sources: TERMINAL_COMMAND_SOURCES,
        input_event_type: "command.executed",
        output_sources: &["derived.session-detector"],
        output_event_type: "activity.session.boundary",
        idle_message: "No terminal command.executed inputs observed; session detector not evaluated",
        zero_message: "No activity.session.boundary events despite terminal command.executed inputs",
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

#[derive(Debug, Clone, Copy)]
struct EnabledAutomata {
    canonicalizer: bool,
    health_aggregator: bool,
    analytics_automaton: bool,
    session_detector: bool,
}

impl EnabledAutomata {
    const fn all_enabled() -> Self {
        Self {
            canonicalizer: true,
            health_aggregator: true,
            analytics_automaton: true,
            session_detector: true,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct EnabledSourceSurfaces {
    filesystem: bool,
    terminal: bool,
    browser: bool,
    desktop: bool,
    system: bool,
}

const HISTORICAL_SIGNAL_CHECKS: &[HistoricalSignalCheck] = &[
    HistoricalSignalCheck {
        label: "Shell history backfill",
        surface: HistoricalSurface::Terminal,
        sources: &["shell.history"],
        event_type: "command.imported",
        zero_message: "No shell.history command.imported events found",
    },
    HistoricalSignalCheck {
        label: "Desktop ActivityWatch history backfill",
        surface: HistoricalSurface::Desktop,
        sources: &["activitywatch"],
        event_type: "window.active",
        zero_message: "No activitywatch window.active events found",
    },
    HistoricalSignalCheck {
        label: "Browser history ingestion",
        surface: HistoricalSurface::Browser,
        sources: &["webhistory"],
        event_type: "page.visited",
        zero_message: "No webhistory page.visited events found",
    },
];

impl VerifyCommand {
    /// `--format json|yaml` suppresses the human-readable check log and
    /// emits a structured summary with per-check status/message records and
    /// the overall pass/skip/warn/fail counts at the end.
    pub async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let table_mode = matches!(format, OutputFormat::Table);
        if table_mode {
            print_verification_header();
        }
        let mut summary = VerificationSummary::new(format);
        let descriptor = load_deployment_descriptor(&mut summary);
        let enabled_automata = enabled_automata(descriptor.as_ref());

        run_passive_verification(
            self,
            &mut summary,
            client,
            descriptor.as_ref(),
            enabled_automata,
        )
        .await?;
        run_active_verification(self, &mut summary, client, descriptor.as_ref()).await?;
        report_historical_proof(self, &mut summary, client, descriptor.as_ref()).await?;

        match format {
            OutputFormat::Table => print_verification_footer(&summary),
            OutputFormat::Json | OutputFormat::Dot => {
                println!("{}", format_json(&summary.as_json())?);
                if summary.fail > 0 {
                    std::process::exit(1);
                }
            }
            OutputFormat::Yaml => {
                println!("{}", format_yaml(&summary.as_json())?);
                if summary.fail > 0 {
                    std::process::exit(1);
                }
            }
        }
        Ok(())
    }
}

fn print_verification_header() {
    println!();
    println!(
        "{}",
        style("Sinex Trustworthiness Verification").bold().cyan()
    );
    println!("{}", style("═".repeat(50)).dim());
    println!();
}

async fn run_passive_verification(
    command: &VerifyCommand,
    summary: &mut VerificationSummary,
    client: &GatewayClient,
    descriptor: Option<&DeploymentReadinessDescriptor>,
    enabled_automata: EnabledAutomata,
) -> Result<()> {
    report_store_and_source_counts(summary, client).await?;
    report_passive_deployment_surfaces(command, summary, client, descriptor, enabled_automata)
        .await?;
    report_gateway_health(summary, client).await;
    report_recent_pipeline_activity(summary, client).await?;
    Ok(())
}

async fn report_store_and_source_counts(
    summary: &mut VerificationSummary,
    client: &GatewayClient,
) -> Result<()> {
    let sampled_events = sample_events(client).await?;
    if sampled_events > 0 {
        summary.pass(format!(
            "Event store query returned {sampled_events} latest events in a bounded sample"
        ));
    } else {
        summary.warn("Event store is empty");
    }

    let sources = sample_sources(client).await?;
    if sources >= 2 {
        summary.pass(format!(
            "Latest event sample includes {sources} distinct sources"
        ));
    } else if sources == 1 {
        summary.warn("Latest event sample includes only 1 source");
    } else {
        summary.fail("No sources producing events");
    }
    Ok(())
}

async fn report_passive_deployment_surfaces(
    command: &VerifyCommand,
    summary: &mut VerificationSummary,
    client: &GatewayClient,
    descriptor: Option<&DeploymentReadinessDescriptor>,
    enabled_automata: EnabledAutomata,
) -> Result<()> {
    report_passive_derived_signals(summary, client, enabled_automata).await?;
    if !command.document_smoke {
        report_document_surface_check(summary, client, descriptor).await?;
    }
    Ok(())
}

async fn report_passive_derived_signals(
    summary: &mut VerificationSummary,
    client: &GatewayClient,
    enabled_automata: EnabledAutomata,
) -> Result<()> {
    for check in PASSIVE_DERIVED_SIGNAL_CHECKS {
        report_passive_signal_check(summary, client, check, enabled_automata).await?;
    }
    Ok(())
}

async fn report_gateway_health(summary: &mut VerificationSummary, client: &GatewayClient) {
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
}

async fn report_recent_pipeline_activity(
    summary: &mut VerificationSummary,
    client: &GatewayClient,
) -> Result<()> {
    let recent = count_recent_events(client).await?;
    if recent > 0 {
        summary.pass(format!(
            "Observed {recent} events in a bounded last-hour sample (pipeline flowing)"
        ));
    } else {
        summary.warn("No events in the last hour — pipeline may be stalled");
    }
    Ok(())
}

async fn run_active_verification(
    command: &VerifyCommand,
    summary: &mut VerificationSummary,
    client: &GatewayClient,
    descriptor: Option<&DeploymentReadinessDescriptor>,
) -> Result<()> {
    report_document_smoke(command, summary, client, descriptor).await?;
    report_source_proof(command, summary, client, descriptor).await?;
    Ok(())
}

async fn report_document_smoke(
    command: &VerifyCommand,
    summary: &mut VerificationSummary,
    client: &GatewayClient,
    descriptor: Option<&DeploymentReadinessDescriptor>,
) -> Result<()> {
    if command.document_smoke {
        run_document_smoke(summary, client, descriptor).await?;
    } else {
        summary.skip(
            "Managed document deployment smoke not run — pass --document-smoke to exercise the scan surface",
        );
    }
    Ok(())
}

async fn report_source_proof(
    command: &VerifyCommand,
    summary: &mut VerificationSummary,
    client: &GatewayClient,
    descriptor: Option<&DeploymentReadinessDescriptor>,
) -> Result<()> {
    if command.source_proof {
        run_source_proof(summary, client, descriptor).await?;
    } else {
        summary.skip(
            "Source surface proof not run — pass --source-proof to require enabled collectors to show recent or historical event evidence",
        );
    }
    Ok(())
}

async fn report_historical_proof(
    command: &VerifyCommand,
    summary: &mut VerificationSummary,
    client: &GatewayClient,
    descriptor: Option<&DeploymentReadinessDescriptor>,
) -> Result<()> {
    if !command.historical_proof {
        return Ok(());
    }
    run_historical_proof(summary, client, descriptor).await?;
    Ok(())
}

fn print_verification_footer(summary: &VerificationSummary) {
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
}

#[derive(Debug, Clone, Copy)]
enum HistoricalSurface {
    Terminal,
    Browser,
    Desktop,
}

#[derive(Debug, Clone, Copy)]
struct HistoricalSignalCheck {
    label: &'static str,
    surface: HistoricalSurface,
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

#[derive(Debug, Serialize)]
struct VerificationRecord {
    status: &'static str,
    message: String,
}

#[derive(Debug)]
struct VerificationSummary {
    pass: u32,
    skip: u32,
    warn: u32,
    fail: u32,
    format: OutputFormat,
    records: Vec<VerificationRecord>,
}

impl Default for VerificationSummary {
    fn default() -> Self {
        Self::new(OutputFormat::Table)
    }
}

impl VerificationSummary {
    fn new(format: OutputFormat) -> Self {
        Self {
            pass: 0,
            skip: 0,
            warn: 0,
            fail: 0,
            format,
            records: Vec::new(),
        }
    }

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
        if matches!(self.format, OutputFormat::Table) {
            println!("{} {}", status.symbol(), message);
        }
        self.records.push(VerificationRecord {
            status: status.label(),
            message: message.to_string(),
        });
        match status {
            VerificationStatus::Pass => self.pass += 1,
            VerificationStatus::Skip => self.skip += 1,
            VerificationStatus::Warn => self.warn += 1,
            VerificationStatus::Fail => self.fail += 1,
        }
    }

    fn as_json(&self) -> serde_json::Value {
        json!({
            "pass": self.pass,
            "skip": self.skip,
            "warn": self.warn,
            "fail": self.fail,
            "records": self.records,
        })
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

    fn label(self) -> &'static str {
        match self {
            Self::Pass => "pass",
            Self::Skip => "skip",
            Self::Warn => "warn",
            Self::Fail => "fail",
        }
    }
}

fn load_deployment_descriptor(
    summary: &mut VerificationSummary,
) -> Option<DeploymentReadinessDescriptor> {
    match DeploymentReadinessDescriptor::load() {
        Ok(descriptor) => descriptor,
        Err(error) => {
            summary.warn(format!(
                "Failed to load local deployment readiness descriptor; deployment-scoped checks will fall back to generic assumptions: {error}"
            ));
            None
        }
    }
}

fn enabled_automata(descriptor: Option<&DeploymentReadinessDescriptor>) -> EnabledAutomata {
    descriptor.map_or_else(EnabledAutomata::all_enabled, |descriptor| {
        let enabled = EnabledAutomata {
            canonicalizer: descriptor.automata.canonicalizer,
            health_aggregator: descriptor.automata.health_aggregator,
            analytics_automaton: descriptor.automata.analytics_automaton,
            session_detector: descriptor.automata.session_detector,
        };
        if descriptor.automata.surface.enabled
            && !enabled.canonicalizer
            && !enabled.health_aggregator
            && !enabled.analytics_automaton
            && !enabled.session_detector
        {
            EnabledAutomata::all_enabled()
        } else {
            enabled
        }
    })
}

fn enabled_source_surfaces(
    descriptor: Option<&DeploymentReadinessDescriptor>,
) -> EnabledSourceSurfaces {
    descriptor.map_or_else(
        || EnabledSourceSurfaces {
            filesystem: false,
            terminal: false,
            browser: false,
            desktop: false,
            system: false,
        },
        |descriptor| EnabledSourceSurfaces {
            filesystem: descriptor.filesystem.enabled,
            terminal: descriptor.terminal.surface.enabled,
            browser: descriptor.browser.surface.enabled,
            desktop: descriptor.desktop.surface.enabled,
            system: descriptor.system.enabled,
        },
    )
}

async fn run_historical_proof(
    summary: &mut VerificationSummary,
    client: &GatewayClient,
    descriptor: Option<&DeploymentReadinessDescriptor>,
) -> Result<()> {
    let Some(descriptor) = descriptor else {
        summary.fail(
            "Historical proof requested, but no local deployment readiness descriptor is available",
        );
        return Ok(());
    };

    let enabled = enabled_source_surfaces(Some(descriptor));
    for check in HISTORICAL_SIGNAL_CHECKS {
        if !historical_surface_enabled(check.surface, enabled) {
            summary.skip(format!(
                "{} skipped because the owning collector is disabled in the local deployment",
                check.label
            ));
            continue;
        }

        let count = sample_events_matching(client, check.sources, check.event_type).await?;
        if count > 0 {
            summary.pass(format!(
                "{} emitted at least {} {} events",
                check.label, count, check.event_type
            ));
        } else {
            summary.fail(check.zero_message);
        }
    }

    Ok(())
}

const fn historical_surface_enabled(
    surface: HistoricalSurface,
    enabled: EnabledSourceSurfaces,
) -> bool {
    match surface {
        HistoricalSurface::Terminal => enabled.terminal,
        HistoricalSurface::Browser => enabled.browser,
        HistoricalSurface::Desktop => enabled.desktop,
    }
}

async fn report_document_surface_check(
    summary: &mut VerificationSummary,
    client: &GatewayClient,
    descriptor: Option<&DeploymentReadinessDescriptor>,
) -> Result<()> {
    let Some(descriptor) = descriptor else {
        summary.skip(
            "Local deployment readiness descriptor unavailable; managed document surface not evaluated",
        );
        return Ok(());
    };

    if !descriptor.document.surface.enabled {
        summary.skip("Managed document surface disabled");
        return Ok(());
    }

    let count = sample_events_matching(
        client,
        &[DOCUMENT_INGESTOR_SOURCE],
        DOCUMENT_INGESTED_EVENT_TYPE,
    )
    .await?;
    if count > 0 {
        summary.pass(format!(
            "Managed document surface emitted at least {count} {DOCUMENT_INGESTED_EVENT_TYPE} events"
        ));
    } else {
        summary
            .warn("Managed document surface enabled but no document.ingested events observed yet");
    }

    Ok(())
}

async fn run_source_proof(
    summary: &mut VerificationSummary,
    client: &GatewayClient,
    descriptor: Option<&DeploymentReadinessDescriptor>,
) -> Result<()> {
    let Some(descriptor) = descriptor else {
        summary.fail(
            "Source surface proof requested, but no local deployment readiness descriptor is available",
        );
        return Ok(());
    };

    let enabled = enabled_source_surfaces(Some(descriptor));
    if enabled.filesystem {
        report_source_surface_proof(
            summary,
            client,
            "Filesystem collector",
            FILESYSTEM_PROOF_SOURCES,
        )
        .await?;
    } else {
        summary.skip("Filesystem collector disabled in local deployment");
    }

    if enabled.terminal {
        report_source_surface_proof(
            summary,
            client,
            "Terminal collector",
            TERMINAL_PROOF_SOURCES,
        )
        .await?;
    } else {
        summary.skip("Terminal collector disabled in local deployment");
    }

    if enabled.browser {
        report_source_surface_proof(summary, client, "Browser collector", BROWSER_PROOF_SOURCES)
            .await?;
    } else {
        summary.skip("Browser collector disabled in local deployment");
    }

    if enabled.desktop {
        report_source_surface_proof(summary, client, "Desktop collector", DESKTOP_PROOF_SOURCES)
            .await?;
    } else {
        summary.skip("Desktop collector disabled in local deployment");
    }

    if enabled.system {
        report_source_surface_proof(summary, client, "System collector", SYSTEM_PROOF_SOURCES)
            .await?;
    } else {
        summary.skip("System collector disabled in local deployment");
    }

    Ok(())
}

async fn report_source_surface_proof(
    summary: &mut VerificationSummary,
    client: &GatewayClient,
    label: &str,
    sources: &[&str],
) -> Result<()> {
    let evidence =
        wait_for_source_surface_evidence(client, sources, Duration::from_secs(10)).await?;
    let recent_window_minutes = SOURCE_PROOF_RECENT_WINDOW.as_secs() / 60;

    if evidence.recent_sample_count > 0 {
        summary.pass(format!(
            "{label} emitted at least {} events in the last {} minutes",
            evidence.recent_sample_count, recent_window_minutes
        ));
    } else if evidence.persisted_sample_count > 0 {
        summary.warn(format!(
            "{label} has at least {} persisted events, but none in the last {} minutes",
            evidence.persisted_sample_count, recent_window_minutes
        ));
    } else {
        summary.fail(format!(
            "{label} is enabled in the local deployment but has no persisted event evidence yet"
        ));
    }
    Ok(())
}

async fn report_passive_signal_check(
    summary: &mut VerificationSummary,
    client: &GatewayClient,
    check: &PassiveSignalCheck,
    enabled_automata: EnabledAutomata,
) -> Result<()> {
    if !passive_signal_enabled(check, enabled_automata) {
        summary.skip(format!("{} disabled in local deployment", check.label));
        return Ok(());
    }

    let input_count =
        sample_events_matching(client, check.input_sources, check.input_event_type).await?;
    if input_count == 0 {
        summary.skip(check.idle_message);
        return Ok(());
    }

    let output_count =
        sample_events_matching(client, check.output_sources, check.output_event_type).await?;
    if output_count > 0 {
        summary.pass(format!(
            "{} emitted at least {} {} events",
            check.label, output_count, check.output_event_type
        ));
    } else {
        summary.warn(check.zero_message);
    }

    Ok(())
}

fn passive_signal_enabled(check: &PassiveSignalCheck, enabled_automata: EnabledAutomata) -> bool {
    match check.output_event_type {
        "command.canonical" => enabled_automata.canonicalizer,
        "health.aggregated_report" => enabled_automata.health_aggregator,
        "activity.window.summary" => enabled_automata.analytics_automaton,
        "activity.session.boundary" => enabled_automata.session_detector,
        _ => true,
    }
}

async fn sample_events(client: &GatewayClient) -> Result<i64> {
    sample_query_event_count(client, EventQuery::default()).await
}

async fn sample_sources(client: &GatewayClient) -> Result<i64> {
    let query = EventQuery {
        limit: VERIFY_EVENT_SAMPLE_LIMIT,
        ..Default::default()
    };
    match client.query_events(query).await? {
        EventQueryResult::Events { events, .. } => {
            let sources = events
                .iter()
                .map(|event| event.event.source.as_str().to_string())
                .collect::<BTreeSet<_>>();
            Ok(sources.len() as i64)
        }
        other => Err(eyre!(
            "unexpected query result when sampling sources: {}",
            result_kind(&other)
        )),
    }
}

async fn sample_events_matching(
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
        limit: VERIFY_EVENT_SAMPLE_LIMIT,
        ..Default::default()
    };
    sample_query_event_count(client, query).await
}

async fn sample_events_for_sources(client: &GatewayClient, sources: &[&str]) -> Result<i64> {
    let source_filters = sources
        .iter()
        .copied()
        .map(EventSource::new)
        .collect::<Result<Vec<_>, _>>()?;
    let query = EventQuery {
        sources: source_filters,
        limit: VERIFY_EVENT_SAMPLE_LIMIT,
        ..Default::default()
    };
    sample_query_event_count(client, query).await
}

async fn sample_recent_events_for_sources(client: &GatewayClient, sources: &[&str]) -> Result<i64> {
    let source_filters = sources
        .iter()
        .copied()
        .map(EventSource::new)
        .collect::<Result<Vec<_>, _>>()?;
    let now = Timestamp::now();
    let start = Timestamp::new(now.inner() - time::Duration::try_from(SOURCE_PROOF_RECENT_WINDOW)?);
    let query = EventQuery {
        sources: source_filters,
        time_range: Some(TimeRange::new(Some(start), Some(now))?),
        limit: VERIFY_EVENT_SAMPLE_LIMIT,
        ..Default::default()
    };
    sample_query_event_count(client, query).await
}

async fn count_recent_events(client: &GatewayClient) -> Result<i64> {
    let now = Timestamp::now();
    let one_hour_ago = Timestamp::new(now.inner() - time::Duration::hours(1));
    let time_range = TimeRange::new(Some(one_hour_ago), Some(now))?;

    sample_query_event_count(
        client,
        EventQuery {
            time_range: Some(time_range),
            limit: VERIFY_EVENT_SAMPLE_LIMIT,
            ..Default::default()
        },
    )
    .await
}

async fn sample_query_event_count(client: &GatewayClient, query: EventQuery) -> Result<i64> {
    match client.query_events(query).await? {
        EventQueryResult::Events { events, .. } => Ok(events.len() as i64),
        other => Err(eyre!(
            "unexpected query result for bounded event sample: {}",
            result_kind(&other)
        )),
    }
}

async fn run_document_smoke(
    summary: &mut VerificationSummary,
    client: &GatewayClient,
    descriptor: Option<&DeploymentReadinessDescriptor>,
) -> Result<()> {
    let Some(descriptor) = descriptor else {
        summary.fail(
            "Managed document deployment smoke requested, but no local deployment descriptor is available",
        );
        return Ok(());
    };

    if !descriptor.document.surface.enabled {
        summary.fail(
            "Managed document deployment smoke requested, but the surface is disabled in the local deployment",
        );
        return Ok(());
    }

    match run_document_surface_smoke(client, descriptor).await {
        Ok(()) => {
            summary.pass("Managed document deployment smoke produced document.ingested output");
        }
        Err(error) => summary.fail(format!("Managed document deployment smoke failed: {error}")),
    }

    Ok(())
}
async fn run_document_surface_smoke(
    client: &GatewayClient,
    descriptor: &DeploymentReadinessDescriptor,
) -> Result<()> {
    let Some(scan_service_unit) = descriptor.document.scan_service_unit.as_deref() else {
        return Err(eyre!(
            "deployment descriptor marked the managed document surface enabled but did not declare a scan service unit"
        ));
    };
    let document_path = build_document_smoke_path(descriptor)?;
    let marker = document_path.display().to_string();
    let _cleanup = DocumentSmokeCleanup::new(document_path.clone());
    std::fs::write(
        &document_path,
        format!("# sinex verify document smoke\n{marker}\n"),
    )
    .map_err(|error| {
        eyre!(
            "failed to write managed document smoke file at {}: {error}",
            document_path.display()
        )
    })?;

    let query = document_smoke_query(&marker)?;
    let baseline = query_event_count(client, query.clone()).await?;

    start_systemd_unit(scan_service_unit).await?;
    wait_for_query_count_increase(client, query, baseline, Duration::from_secs(20)).await
}

fn build_document_smoke_path(descriptor: &DeploymentReadinessDescriptor) -> Result<PathBuf> {
    let Some(root) = descriptor.document.allowed_roots.first() else {
        return Err(eyre!(
            "deployment descriptor marked the managed document surface enabled but did not declare any allowed roots"
        ));
    };
    let marker = format!(".sinex-verify-{:016x}.md", rand::random::<u64>());
    Ok(root.join(marker))
}

fn document_smoke_query(file_path: &str) -> Result<EventQuery> {
    Ok(EventQuery {
        sources: vec![EventSource::new(DOCUMENT_INGESTOR_SOURCE)?],
        event_types: vec![EventType::new(DOCUMENT_INGESTED_EVENT_TYPE)?],
        payload: Some(PayloadFilter::Contains {
            value: json!({ "file_path": file_path }),
        }),
        limit: 5,
        ..Default::default()
    })
}

async fn start_systemd_unit(unit: &str) -> Result<()> {
    let output = Command::new("systemctl")
        .args(["start", unit])
        .output()
        .await
        .map_err(|error| eyre!("failed to execute `systemctl start {unit}`: {error}"))?;
    if output.status.success() {
        return Ok(());
    }

    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let detail = if !stderr.is_empty() {
        stderr
    } else if !stdout.is_empty() {
        stdout
    } else {
        "systemctl returned a non-zero exit code without any output".to_string()
    };
    Err(eyre!("`systemctl start {unit}` failed: {detail}"))
}

struct DocumentSmokeCleanup {
    path: PathBuf,
}

impl DocumentSmokeCleanup {
    fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

impl Drop for DocumentSmokeCleanup {
    fn drop(&mut self) {
        if let Err(error) = std::fs::remove_file(&self.path)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            eprintln!(
                "warning: failed to remove managed document smoke file {}: {error}",
                self.path.display()
            );
        }
    }
}

async fn query_event_count(client: &GatewayClient, query: EventQuery) -> Result<i64> {
    match client.query_events(query).await? {
        EventQueryResult::Events { events, .. } => Ok(events.len() as i64),
        other => Err(eyre!(
            "unexpected query result for event smoke query: {}",
            result_kind(&other)
        )),
    }
}

async fn wait_for_query_count_increase(
    client: &GatewayClient,
    query: EventQuery,
    baseline: i64,
    timeout: Duration,
) -> Result<()> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let current = query_event_count(client, query.clone()).await?;
        if current > baseline {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(eyre!(
                "expected query result count to increase beyond {baseline}, but it stayed at {current}"
            ));
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct SourceSurfaceEvidence {
    recent_sample_count: i64,
    persisted_sample_count: i64,
}

async fn wait_for_source_surface_evidence(
    client: &GatewayClient,
    sources: &[&str],
    timeout: Duration,
) -> Result<SourceSurfaceEvidence> {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut evidence = SourceSurfaceEvidence::default();
    loop {
        evidence.recent_sample_count = sample_recent_events_for_sources(client, sources).await?;
        evidence.persisted_sample_count = sample_events_for_sources(client, sources).await?;
        if evidence.recent_sample_count > 0 {
            return Ok(evidence);
        }
        if tokio::time::Instant::now() >= deadline {
            return Ok(evidence);
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

fn result_kind(result: &EventQueryResult) -> &'static str {
    match result {
        EventQueryResult::Events { .. } => "events",
        EventQueryResult::Count { .. } => "count",
        EventQueryResult::GroupedCounts { .. } => "grouped_counts",
        EventQueryResult::GroupedValues { .. } => "grouped_values",
        EventQueryResult::TimeSeries { .. } => "time_series",
        EventQueryResult::SourceStats { .. } => "source_stats",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_primitives::{
        AutomataDeploymentSurface, BrowserDeploymentSurface, DeploymentSurface,
        DesktopDeploymentSurface, DocumentDeploymentSurface, TerminalDeploymentSurface,
    };
    use xtask::sandbox::prelude::*;

    #[sinex_test]
    async fn enabled_automata_follow_descriptor_shape() -> TestResult<()> {
        let enabled = enabled_automata(Some(&DeploymentReadinessDescriptor {
            automata: AutomataDeploymentSurface {
                canonicalizer: true,
                health_aggregator: false,
                analytics_automaton: true,
                session_detector: false,
                ..Default::default()
            },
            ..Default::default()
        }));

        assert!(enabled.canonicalizer);
        assert!(!enabled.health_aggregator);
        assert!(enabled.analytics_automaton);
        assert!(!enabled.session_detector);
        Ok(())
    }

    #[sinex_test]
    async fn enabled_source_surfaces_follow_descriptor_shape() -> TestResult<()> {
        let enabled = enabled_source_surfaces(Some(&DeploymentReadinessDescriptor {
            filesystem: DeploymentSurface {
                enabled: true,
                instances: Some(1),
            },
            terminal: TerminalDeploymentSurface {
                surface: DeploymentSurface {
                    enabled: false,
                    instances: Some(1),
                },
                ..Default::default()
            },
            browser: BrowserDeploymentSurface {
                surface: DeploymentSurface {
                    enabled: true,
                    instances: Some(1),
                },
                ..Default::default()
            },
            desktop: DesktopDeploymentSurface {
                surface: DeploymentSurface {
                    enabled: true,
                    instances: Some(1),
                },
                ..Default::default()
            },
            system: DeploymentSurface {
                enabled: false,
                instances: Some(1),
            },
            ..Default::default()
        }));

        assert!(enabled.filesystem);
        assert!(!enabled.terminal);
        assert!(enabled.browser);
        assert!(enabled.desktop);
        assert!(!enabled.system);
        Ok(())
    }

    #[sinex_test]
    async fn build_document_smoke_path_uses_declared_root() -> TestResult<()> {
        let path = build_document_smoke_path(&DeploymentReadinessDescriptor {
            document: DocumentDeploymentSurface {
                allowed_roots: vec![PathBuf::from("/tmp/sinex-docs")],
                ..Default::default()
            },
            ..Default::default()
        })?;

        assert_eq!(
            path.parent().expect("parent"),
            PathBuf::from("/tmp/sinex-docs")
        );
        assert!(
            path.file_name()
                .expect("file name")
                .to_string_lossy()
                .starts_with(".sinex-verify-")
        );
        Ok(())
    }

    #[sinex_test]
    async fn document_smoke_query_targets_the_specific_file_path() -> TestResult<()> {
        let query = document_smoke_query("/tmp/sinex-docs/.sinex-verify-abc.md")?;

        assert_eq!(query.sources.len(), 1);
        assert_eq!(query.sources[0].as_str(), DOCUMENT_INGESTOR_SOURCE);
        assert_eq!(query.event_types.len(), 1);
        assert_eq!(query.event_types[0].as_str(), DOCUMENT_INGESTED_EVENT_TYPE);
        assert!(matches!(
            query.payload,
            Some(PayloadFilter::Contains { value })
                if value == json!({ "file_path": "/tmp/sinex-docs/.sinex-verify-abc.md" })
        ));
        Ok(())
    }
}
