use crate::parse::parse_duration;
use crate::validation::parse_time_input_with_now;
use clap::Args;
use color_eyre::Result;
use console::style;
use serde_json::json;
#[cfg(test)]
use sinex_primitives::query::QueryResultEvent;
use sinex_primitives::query::{EventQuery, SortDirection, TimeRange};
use sinex_primitives::relations::{
    EventRelationExpr, EvidenceRef, EvidenceRole, EvidenceWindow, ExpansionStep, ExpansionStepKind,
    ExpansionTrace, ObservedRange, TimeBasis,
};
use sinex_primitives::sources::{is_self_observation_source, source_identity_matches_family};
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::views::{
    ActionAvailability, ActionAvailabilityState, CaveatView, ContextSourceView, ContextSummaryView,
    DesktopContextCandidateView, DesktopContextInputEvidence, DesktopContextInputState,
    DesktopContextView, DesktopFocusSessionListView, DesktopFocusSessionView,
    DesktopNotificationPressureView, DesktopProjectContextListView, DesktopProjectContextRowView,
    EVENT_CARD_LIST_SCHEMA_VERSION, EventCardListView, EventCardView, PrivacyStateKind,
    SinexObjectKind, SinexObjectRef, SourceCoverageContinuity, SourceCoverageListView,
    SourceCoverageReadiness, SourceCoverageView, ViewEnvelope,
};
use std::collections::HashMap;
use std::time::{Duration as StdDuration, Instant};

const MAX_FOCUS_SESSION_EVIDENCE_REFS: usize = 12;
const MAX_NOTIFICATION_PRESSURE_EVIDENCE_REFS: usize = 12;
const MAX_PROJECT_CONTEXT_EVIDENCE_REFS: usize = 12;
const CONTEXT_BASE_EVENT_CARDS_TIMEOUT: StdDuration = StdDuration::from_secs(20);
const CONTEXT_DIVERSITY_TOP_UP_TIMEOUT: StdDuration = StdDuration::from_secs(8);
const CONTEXT_SOURCE_CAVEATS_TIMEOUT: StdDuration = StdDuration::from_secs(5);
const CONTEXT_DIVERSITY_SOURCES: &[&str] = &[
    "shell.atuin",
    "shell.kitty",
    "shell.scrollback",
    "wm.hyprland",
    "activitywatch",
    "webhistory",
    "git",
    "fs-watcher",
    "derived.session-detector",
    "derived.activity-window",
    "derived.hourly-summarizer",
    "derived.daily-summarizer",
];
const RECALL_EXPECTED_SOURCES: &[RecallExpectedSource] = &[
    RecallExpectedSource {
        event_source_id: "shell.atuin",
        coverage_source_id: "terminal.atuin-history",
        family: Some("terminal"),
        label: "terminal",
    },
    RecallExpectedSource {
        event_source_id: "webhistory",
        coverage_source_id: "browser.history",
        family: Some("browser"),
        label: "browser",
    },
    RecallExpectedSource {
        event_source_id: "fs-watcher",
        coverage_source_id: "fs",
        family: None,
        label: "filesystem",
    },
    RecallExpectedSource {
        event_source_id: "git",
        coverage_source_id: "git-commit-history",
        family: None,
        label: "git",
    },
];

use crate::client::GatewayClient;
use crate::fmt::format_duration_age;
use crate::fmt::{render_envelope, render_finite_envelope};
use crate::model::OutputFormat;

/// Show activity context for session resumption ("what was I doing?")
#[derive(Debug, Args)]
#[command(after_help = "\
EXAMPLES:
    # What was I doing in the last 2 hours?
    sinexctl events context

    # Wider window
    sinexctl events context --since 4h

    # Narrow to last 30 minutes
    sinexctl events context --since 30m

    # Exact bounded window
    sinexctl events context --since 2026-07-02T18:00:00Z --until 2026-07-02T19:00:00Z
")]
pub struct ContextCommand {
    /// Time window start: duration lookback or absolute time (default: last 2 hours)
    #[arg(long, short = 's', default_value = "2h")]
    since: String,

    /// Time window end. When --since is a duration, the duration is measured back from this bound.
    #[arg(long, short = 'u')]
    until: Option<String>,

    /// Number of events to fetch (increase for busy systems)
    #[arg(long, default_value = "200")]
    limit: i32,

    /// Render the desktop.context current-view contract over recent evidence
    #[arg(long)]
    desktop: bool,

    /// Explain desktop.context current-view candidates as an EvidenceWindow
    #[arg(
        long,
        requires = "desktop",
        conflicts_with_all = ["notification_pressure", "focus_sessions"]
    )]
    explain: bool,

    /// Render notification-pressure projection over recent notification evidence
    #[arg(
        long,
        requires = "desktop",
        conflicts_with_all = ["explain", "focus_sessions"]
    )]
    notification_pressure: bool,

    /// Render focus-session projection over recent desktop activity evidence
    #[arg(
        long,
        requires = "desktop",
        conflicts_with_all = ["explain", "notification_pressure", "project_contexts"]
    )]
    focus_sessions: bool,

    /// Render project-context projection candidates over recent desktop activity evidence
    #[arg(
        long,
        requires = "desktop",
        conflicts_with_all = ["explain", "notification_pressure", "focus_sessions"]
    )]
    project_contexts: bool,
}

/// Recall activity around a point in time using the shared context substrate.
#[derive(Debug, Args)]
#[command(after_help = "\
EXAMPLES:
    # Recall the last 2 hours
    sinexctl recall

    # Recall the 4 hours leading up to a known point
    sinexctl recall --at 2026-07-02T19:00:00Z --window 4h

    # Machine-readable recall packet
    sinexctl recall --window 30m --format json
")]
pub struct RecallCommand {
    /// Time anchor/end bound for the recall window (default: now)
    #[arg(long)]
    at: Option<String>,

    /// Lookback window ending at --at (default: 2h)
    #[arg(long, short = 'w', default_value = "2h")]
    window: String,

    /// Number of events to fetch (increase for busy systems)
    #[arg(long, default_value = "200")]
    limit: i32,

    /// Include Sinex self-observation/runtime telemetry in the recall packet
    #[arg(long)]
    include_self_observation: bool,
}

impl ContextCommand {
    pub async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let now = Timestamp::now();
        let window = build_context_window(&self.since, self.until.as_deref(), now)?;
        let request = ContextRequest {
            window,
            limit: self.limit,
            machine_source_surface: "sinexctl.context",
            finite_error_label: "events context",
            table_title: "Context",
            self_observation: SelfObservationMode::Include,
            source_caveats: SourceCaveatMode::None,
        };
        execute_context_request(
            client,
            format,
            request,
            ContextDesktopMode {
                desktop: self.desktop,
                explain: self.explain,
                notification_pressure: self.notification_pressure,
                focus_sessions: self.focus_sessions,
                project_contexts: self.project_contexts,
            },
        )
        .await
    }
}

impl RecallCommand {
    pub async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let now = Timestamp::now();
        let window = build_context_window(&self.window, self.at.as_deref(), now)?;
        let request = ContextRequest {
            window,
            limit: self.limit,
            machine_source_surface: "sinexctl.recall",
            finite_error_label: "recall",
            table_title: "Recall",
            self_observation: if self.include_self_observation {
                SelfObservationMode::Include
            } else {
                SelfObservationMode::Exclude
            },
            source_caveats: SourceCaveatMode::RecallExpectedSources,
        };
        execute_context_request(client, format, request, ContextDesktopMode::default()).await
    }
}

#[derive(Debug)]
struct ContextRequest {
    window: ContextWindow,
    limit: i32,
    machine_source_surface: &'static str,
    finite_error_label: &'static str,
    table_title: &'static str,
    self_observation: SelfObservationMode,
    source_caveats: SourceCaveatMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SelfObservationMode {
    Include,
    Exclude,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SourceCaveatMode {
    None,
    RecallExpectedSources,
}

#[derive(Debug, Clone, Copy)]
struct RecallExpectedSource {
    event_source_id: &'static str,
    coverage_source_id: &'static str,
    family: Option<&'static str>,
    label: &'static str,
}

#[derive(Debug, Default)]
struct ContextDesktopMode {
    desktop: bool,
    explain: bool,
    notification_pressure: bool,
    focus_sessions: bool,
    project_contexts: bool,
}

#[derive(Debug, Default)]
struct ContextStageTimings {
    total: StdDuration,
    base_event_cards: StdDuration,
    diversity_top_up: StdDuration,
    self_observation_filter: StdDuration,
    source_caveats: StdDuration,
}

impl ContextStageTimings {
    fn to_json(&self) -> serde_json::Value {
        json!({
            "total": duration_millis(self.total),
            "base_event_cards": duration_millis(self.base_event_cards),
            "diversity_top_up": duration_millis(self.diversity_top_up),
            "self_observation_filter": duration_millis(self.self_observation_filter),
            "source_caveats": duration_millis(self.source_caveats),
        })
    }
}

fn duration_millis(duration: StdDuration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

async fn execute_context_request(
    client: &GatewayClient,
    format: OutputFormat,
    request: ContextRequest,
    desktop_mode: ContextDesktopMode,
) -> Result<()> {
    let ContextRequest {
        window,
        limit,
        machine_source_surface,
        finite_error_label,
        table_title,
        self_observation,
        source_caveats,
    } = request;

    let total_start = Instant::now();
    let mut timings = ContextStageTimings::default();
    let mut stage_caveats = Vec::new();
    let query = EventQuery {
        sources: vec![],
        event_types: vec![],
        time_range: Some(window.time_range),
        payload: None,
        limit: i64::from(limit),
        direction: SortDirection::Desc,
        ..Default::default()
    };

    let started = Instant::now();
    let mut event_cards =
        match tokio::time::timeout(CONTEXT_BASE_EVENT_CARDS_TIMEOUT, client.event_cards(query))
            .await
        {
            Ok(cards) => cards?,
            Err(_) => {
                stage_caveats.push(CaveatView {
                    id: "recall.event_cards_timeout".to_string(),
                    message: format!(
                        "base event-card query exceeded {}s; recall returned a finite degraded packet instead of waiting for the gateway timeout",
                        CONTEXT_BASE_EVENT_CARDS_TIMEOUT.as_secs()
                    ),
                    ref_: Some(SinexObjectRef::new(SinexObjectKind::RpcMethod, "events.cards")),
                });
                EventCardListView {
                    schema_version: EVENT_CARD_LIST_SCHEMA_VERSION.to_string(),
                    count: 0,
                    cards: Vec::new(),
                    next_cursor: None,
                    total_estimate: None,
                }
            }
        };
    timings.base_event_cards = started.elapsed();

    let started = Instant::now();
    let diversity_caveat = top_up_context_diversity(client, &mut event_cards, &window).await?;
    timings.diversity_top_up = started.elapsed();

    let started = Instant::now();
    apply_self_observation_mode(&mut event_cards, self_observation);
    timings.self_observation_filter = started.elapsed();

    let started = Instant::now();
    let mut source_caveats = context_source_caveats(client, &event_cards, source_caveats).await;
    timings.source_caveats = started.elapsed();
    source_caveats.extend(stage_caveats);
    if let Some(caveat) = diversity_caveat {
        source_caveats.push(caveat);
    }
    timings.total = total_start.elapsed();

    let sources = grouped_context_sources(&event_cards.cards);
    if desktop_mode.desktop {
        let output = render_desktop_context_output(
            &event_cards,
            &sources,
            &window.since,
            format,
            desktop_mode.explain,
            desktop_mode.notification_pressure,
            desktop_mode.focus_sessions,
            desktop_mode.project_contexts,
        )?;
        println!("{output}");
        return Ok(());
    }

    if let Some(output) = render_context_machine_output(
        &event_cards,
        &sources,
        &window,
        format,
        machine_source_surface,
        finite_error_label,
        &source_caveats,
        &timings,
    )? {
        println!("{output}");
        return Ok(());
    }

    if event_cards.cards.is_empty() {
        println!(
            "{} No activity found in {}",
            style("○").dim(),
            window.label()
        );
        return Ok(());
    }

    println!(
        "{} {}",
        style(format!("{table_title} ({}):", window.label()))
            .bold()
            .cyan(),
        style(format!("{} sources", sources.len())).dim()
    );
    println!("{}", style("─".repeat(60)).dim());

    let now = Timestamp::now();
    let max_source_len = sources
        .iter()
        .map(|(source, _)| display_source(source).len())
        .max()
        .unwrap_or(10);
    let label_width = max_source_len.max(8);

    for (source_key, card) in &sources {
        let label = display_source(source_key);
        let age = card
            .timestamp
            .original
            .map_or_else(|| "?".to_string(), |ts| format_age(now - ts));

        let detail = truncate(&card.summary, 55);

        println!(
            "  {:<label_width$}  {}  {}",
            style(&label).cyan(),
            style(format!("{age:>6}")).dim(),
            detail,
            label_width = label_width,
        );
    }

    println!("{}", style("─".repeat(60)).dim());
    println!(
        "  {} events across {} sources in {}",
        style(event_cards.count).bold(),
        style(sources.len()).bold(),
        window.label(),
    );
    for caveat in &source_caveats {
        println!("  {} {}", style("caveat").yellow(), caveat.message);
    }

    Ok(())
}

fn apply_self_observation_mode(event_cards: &mut EventCardListView, mode: SelfObservationMode) {
    if mode == SelfObservationMode::Include {
        return;
    }
    event_cards
        .cards
        .retain(|card| !is_self_observation_card(card));
    event_cards.count = event_cards.cards.len();
}

fn is_self_observation_card(card: &EventCardView) -> bool {
    is_self_observation_source(card.source.raw.as_str())
}

#[derive(Debug, Clone)]
struct ContextWindow {
    since: String,
    until: Option<String>,
    time_range: TimeRange,
}

impl ContextWindow {
    fn label(&self) -> String {
        match self.until.as_deref() {
            Some(until) => format!("{} to {}", self.since, until),
            None => format!("last {}", self.since),
        }
    }

    fn query_echo(&self) -> serde_json::Value {
        match self.until.as_deref() {
            Some(until) => json!({
                "since": self.since,
                "until": until
            }),
            None => json!({
                "since": self.since
            }),
        }
    }
}

fn build_context_window(since: &str, until: Option<&str>, now: Timestamp) -> Result<ContextWindow> {
    let end = until
        .map(|value| parse_time_input_with_now(value, now))
        .transpose()?;
    let start = match parse_duration(since) {
        Ok(duration) => Some(end.unwrap_or(now) - duration),
        Err(_) => Some(parse_time_input_with_now(since, now)?),
    };
    let time_range = TimeRange::new(start, end)?;

    Ok(ContextWindow {
        since: since.to_string(),
        until: until.map(str::to_string),
        time_range,
    })
}

fn grouped_context_sources(cards: &[EventCardView]) -> Vec<(String, &EventCardView)> {
    let mut by_source: HashMap<String, &EventCardView> = HashMap::new();
    for card in cards {
        let key = card.source.raw.clone();
        by_source.entry(key).or_insert(card);
    }

    let mut sources: Vec<_> = by_source.into_iter().collect();
    sources.sort_by(|a, b| {
        let ts_a = a.1.timestamp.original.unwrap_or(Timestamp::UNIX_EPOCH);
        let ts_b = b.1.timestamp.original.unwrap_or(Timestamp::UNIX_EPOCH);
        ts_b.inner().cmp(&ts_a.inner())
    });
    sources
}

async fn top_up_context_diversity(
    client: &GatewayClient,
    event_cards: &mut EventCardListView,
    window: &ContextWindow,
) -> Result<Option<CaveatView>> {
    let outcome = tokio::time::timeout(
        CONTEXT_DIVERSITY_TOP_UP_TIMEOUT,
        top_up_context_diversity_unbounded(client, event_cards, window),
    )
    .await;
    match outcome {
        Ok(result) => result.map(|()| None),
        Err(_) => Ok(Some(CaveatView {
            id: "recall.diversity_top_up_timeout".to_string(),
            message: format!(
                "source-diversity top-up exceeded {}s; recall output is bounded to the base event-card query plus completed stages",
                CONTEXT_DIVERSITY_TOP_UP_TIMEOUT.as_secs()
            ),
            ref_: Some(SinexObjectRef::new(
                SinexObjectKind::RpcMethod,
                "events.cards",
            )),
        })),
    }
}

async fn top_up_context_diversity_unbounded(
    client: &GatewayClient,
    event_cards: &mut EventCardListView,
    window: &ContextWindow,
) -> Result<()> {
    for source in CONTEXT_DIVERSITY_SOURCES {
        if context_cards_include_source(&event_cards.cards, source) {
            continue;
        }

        let query = EventQuery {
            sources: vec![(*source).to_string().into()],
            event_types: vec![],
            time_range: Some(window.time_range),
            payload: None,
            limit: 1,
            direction: SortDirection::Desc,
            ..Default::default()
        };

        let source_cards = client.event_cards(query).await?;
        merge_context_diversity_cards(event_cards, source_cards.cards);
    }

    Ok(())
}

fn merge_context_diversity_cards(
    event_cards: &mut EventCardListView,
    candidates: impl IntoIterator<Item = EventCardView>,
) {
    for candidate in candidates {
        if context_cards_include_source(&event_cards.cards, &candidate.source.raw) {
            continue;
        }
        event_cards.cards.push(candidate);
        event_cards.count += 1;
    }
}

fn context_cards_include_source(cards: &[EventCardView], source: &str) -> bool {
    cards.iter().any(|card| card.source.raw == source)
}

async fn context_source_caveats(
    client: &GatewayClient,
    event_cards: &EventCardListView,
    mode: SourceCaveatMode,
) -> Vec<CaveatView> {
    match mode {
        SourceCaveatMode::None => Vec::new(),
        SourceCaveatMode::RecallExpectedSources => {
            let outcome = tokio::time::timeout(
                CONTEXT_SOURCE_CAVEATS_TIMEOUT,
                client.sources_status_view_filtered(None, None, false),
            )
            .await;
            match outcome {
                Ok(Ok(envelope)) => recall_expected_source_caveats(event_cards, &envelope.payload),
                Ok(Err(error)) => vec![CaveatView {
                    id: "recall.source_status_unavailable".to_string(),
                    message: format!(
                        "source coverage caveats unavailable; recall cannot distinguish missing activity from source-status failure: {error}"
                    ),
                    ref_: Some(SinexObjectRef::new(
                        SinexObjectKind::RpcMethod,
                        "sources.status.view",
                    )),
                }],
                Err(_) => vec![CaveatView {
                    id: "recall.source_status_timeout".to_string(),
                    message: format!(
                        "source coverage caveats exceeded {}s; recall cannot fully distinguish missing activity from source-status latency",
                        CONTEXT_SOURCE_CAVEATS_TIMEOUT.as_secs()
                    ),
                    ref_: Some(SinexObjectRef::new(
                        SinexObjectKind::RpcMethod,
                        "sources.status.view",
                    )),
                }],
            }
        }
    }
}

fn recall_expected_source_caveats(
    event_cards: &EventCardListView,
    coverage: &SourceCoverageListView,
) -> Vec<CaveatView> {
    let mut caveats = Vec::new();
    for expected in RECALL_EXPECTED_SOURCES {
        let source = recall_expected_source_coverage(expected, coverage);
        if !recall_cards_include_expected_source(event_cards, expected) {
            caveats.push(recall_expected_source_caveat(expected, source));
        }
        if let Some(source) = source {
            caveats.extend(recall_source_gap_caveats(expected, source));
        }
    }
    caveats
}

fn recall_cards_include_expected_source(
    event_cards: &EventCardListView,
    expected: &RecallExpectedSource,
) -> bool {
    event_cards
        .cards
        .iter()
        .any(|card| recall_event_source_matches_expected(&card.source.raw, expected))
}

fn recall_event_source_matches_expected(source: &str, expected: &RecallExpectedSource) -> bool {
    source == expected.event_source_id
        || expected
            .family
            .is_some_and(|family| source_identity_matches_family(source, "", family))
}

fn recall_expected_source_caveat(
    expected: &RecallExpectedSource,
    source: Option<&SourceCoverageView>,
) -> CaveatView {
    let id = format!("recall.source.{}.absent", expected.label);
    let ref_ = Some(recall_expected_source_ref(expected));

    match source {
        Some(source)
            if source.readiness == SourceCoverageReadiness::Ready
                && source.continuity == SourceCoverageContinuity::Active
                && source.event_count > 0 =>
        {
            CaveatView {
                id,
                message: format!(
                    "{} source is active but contributed no events to this recall window",
                    expected.label
                ),
                ref_,
            }
        }
        Some(source) => CaveatView {
            id,
            message: format!(
                "{} source absent from recall; source status is readiness={} continuity={} events={} materials={}",
                expected.label,
                source.readiness.as_str(),
                source.continuity.as_str(),
                source.event_count,
                source.material_count
            ),
            ref_,
        },
        None => CaveatView {
            id,
            message: format!(
                "{} source absent from recall and not present in source coverage",
                expected.label
            ),
            ref_,
        },
    }
}

fn recall_source_gap_caveats(
    expected: &RecallExpectedSource,
    source: &SourceCoverageView,
) -> Vec<CaveatView> {
    source
        .gaps
        .iter()
        .map(|gap| CaveatView {
            id: format!(
                "recall.source.{}.gap.{}",
                expected.label,
                caveat_id_component(&gap.kind)
            ),
            message: format!(
                "{} source coverage gap: {}: {}",
                expected.label, gap.kind, gap.message
            ),
            ref_: Some(recall_expected_source_ref(expected)),
        })
        .collect()
}

fn caveat_id_component(value: &str) -> String {
    value
        .chars()
        .map(|ch| match ch {
            'a'..='z' | '0'..='9' | '_' | '-' => ch,
            'A'..='Z' => ch.to_ascii_lowercase(),
            _ => '_',
        })
        .collect()
}

fn recall_expected_source_ref(expected: &RecallExpectedSource) -> SinexObjectRef {
    SinexObjectRef::new(SinexObjectKind::SourceDriver, expected.coverage_source_id)
        .with_command_hint(format!(
            "sinexctl sources status {} --format json",
            expected.coverage_source_id
        ))
}

fn recall_expected_source_coverage<'a>(
    expected: &RecallExpectedSource,
    coverage: &'a SourceCoverageListView,
) -> Option<&'a SourceCoverageView> {
    coverage
        .sources
        .iter()
        .find(|source| source.source_id == expected.coverage_source_id)
        .or_else(|| {
            coverage
                .sources
                .iter()
                .find(|source| recall_coverage_source_matches_expected(source, expected))
        })
}

fn recall_coverage_source_matches_expected(
    source: &SourceCoverageView,
    expected: &RecallExpectedSource,
) -> bool {
    source.source_id == expected.coverage_source_id
        || expected.family.is_some_and(|family| {
            source_identity_matches_family(&source.source_id, &source.namespace, family)
        })
}

fn render_context_machine_output(
    event_cards: &EventCardListView,
    sources: &[(String, &EventCardView)],
    window: &ContextWindow,
    format: OutputFormat,
    source_surface: &'static str,
    finite_error_label: &'static str,
    source_caveats: &[CaveatView],
    stage_timings: &ContextStageTimings,
) -> Result<Option<String>> {
    match format {
        OutputFormat::Table => Ok(None),
        OutputFormat::Json | OutputFormat::Yaml => {
            let source_views = sources
                .iter()
                .map(|(source, result_event)| ContextSourceView {
                    source: source.clone(),
                    label: display_source(source),
                    latest_ts: result_event.timestamp.original,
                    latest_event: (*result_event).clone(),
                })
                .collect();
            let envelope = ViewEnvelope::new(
                source_surface,
                ContextSummaryView::new(&window.since, event_cards.count, source_views)
                    .with_source_caveats(source_caveats.to_vec()),
            )
            .with_query_echo({
                let mut query_echo = window.query_echo();
                if let serde_json::Value::Object(ref mut object) = query_echo {
                    object.insert("stage_timings_ms".to_string(), stage_timings.to_json());
                }
                query_echo
            });

            render_envelope(&envelope, &envelope.payload.sources, format)
        }
        OutputFormat::Ndjson | OutputFormat::Dot => Err(color_eyre::eyre::eyre!(
            "{finite_error_label} is a finite view; use json, yaml, or table"
        )),
    }
}

fn render_desktop_context_output(
    event_cards: &EventCardListView,
    sources: &[(String, &EventCardView)],
    since: &str,
    format: OutputFormat,
    explain: bool,
    notification_pressure: bool,
    focus_sessions: bool,
    project_contexts: bool,
) -> Result<String> {
    if matches!(format, OutputFormat::Ndjson | OutputFormat::Dot) {
        return Err(color_eyre::eyre::eyre!(
            "desktop context is a finite view; use json, yaml, or table"
        ));
    }

    let view = build_desktop_context_view(event_cards, sources, since);
    if explain {
        let envelope = desktop_context_evidence_window(&view)
            .into_view("sinexctl.events.context.desktop.explain")
            .with_query_echo(json!({
                "since": since,
                "limit": event_cards.count,
                "mode": "desktop_context_evidence_window"
            }));
        return render_finite_envelope(&envelope, format)?
            .ok_or_else(|| color_eyre::eyre::eyre!("desktop context evidence output expected"));
    }

    if notification_pressure {
        let view = build_notification_pressure_view(event_cards, since);
        if matches!(format, OutputFormat::Json | OutputFormat::Yaml) {
            let envelope = view
                .into_envelope("sinexctl.events.context.desktop.notification_pressure")
                .with_query_echo(json!({
                    "since": since,
                    "limit": event_cards.count,
                    "mode": "desktop_notification_pressure"
                }));
            return render_finite_envelope(&envelope, format)?.ok_or_else(|| {
                color_eyre::eyre::eyre!("desktop notification-pressure output expected")
            });
        }
        return Ok(render_notification_pressure_table(&view));
    }

    if focus_sessions {
        let view = build_focus_session_list_view(event_cards, since);
        if matches!(format, OutputFormat::Json | OutputFormat::Yaml) {
            let envelope = view
                .into_envelope("sinexctl.events.context.desktop.focus_sessions")
                .with_query_echo(json!({
                    "since": since,
                    "limit": event_cards.count,
                    "mode": "desktop_focus_sessions"
                }));
            return render_finite_envelope(&envelope, format)?
                .ok_or_else(|| color_eyre::eyre::eyre!("desktop focus-session output expected"));
        }
        return Ok(render_focus_session_table(&view));
    }

    if project_contexts {
        let view = build_project_context_list_view(event_cards, since);
        if matches!(format, OutputFormat::Json | OutputFormat::Yaml) {
            let envelope = view
                .into_envelope("sinexctl.events.context.desktop.project_contexts")
                .with_query_echo(json!({
                    "since": since,
                    "limit": event_cards.count,
                    "mode": "desktop_project_contexts"
                }));
            return render_finite_envelope(&envelope, format)?
                .ok_or_else(|| color_eyre::eyre::eyre!("desktop project-context output expected"));
        }
        return Ok(render_project_context_table(&view));
    }

    if matches!(format, OutputFormat::Json | OutputFormat::Yaml) {
        let envelope = view
            .clone()
            .into_envelope("sinexctl.events.context.desktop")
            .with_query_echo(json!({
                "since": since,
                "limit": event_cards.count,
                "mode": "desktop_context"
            }));
        return render_finite_envelope(&envelope, format)?
            .ok_or_else(|| color_eyre::eyre::eyre!("desktop context output expected"));
    }

    Ok(render_desktop_context_table(&view, since))
}

fn build_desktop_context_view(
    _event_cards: &EventCardListView,
    sources: &[(String, &EventCardView)],
    since: &str,
) -> DesktopContextView {
    let mut inputs = Vec::new();
    for family in ["desktop", "terminal", "browser", "notification"] {
        inputs.push(desktop_context_input_for_family(family, sources));
    }

    let mut view = DesktopContextView::current(
        sinex_primitives::DESKTOP_CONTEXT_CURRENT_VIEW_DERIVATION_ID,
        inputs,
    )
    .with_caveat(
        "context.derived_view",
        "desktop context is derived from admitted observations and does not create canonical context events",
        Some(SinexObjectRef::new(
            SinexObjectKind::Projection,
            "desktop.context.current_view",
        )),
    );

    if let Some((_, card)) = sources
        .iter()
        .find(|(_, card)| is_active_window_evidence(card))
    {
        view.active_window_ref = Some(card.ref_.clone());
    }

    view.candidates = desktop_context_candidates(sources);
    if !view.candidates.is_empty() {
        view = view.with_caveat(
            "context.candidates_ranked_view",
            "desktop context candidates are ranked view output; durable labels require Proposal/Judgment finalization",
            Some(SinexObjectRef::new(
                SinexObjectKind::Projection,
                "desktop.context.current_view",
            )),
        );
    }

    if view
        .inputs
        .iter()
        .any(|input| input.state == DesktopContextInputState::Missing)
    {
        view = view.with_caveat(
            "context.inputs_missing",
            format!(
                "one or more desktop-context input families have no events in the last {since}"
            ),
            None,
        );
    }

    view
}

fn desktop_context_candidates(
    sources: &[(String, &EventCardView)],
) -> Vec<DesktopContextCandidateView> {
    let mut candidates = Vec::new();

    if let Some((_, card)) = sources
        .iter()
        .find(|(_, card)| is_active_window_evidence(card))
    {
        candidates.push(DesktopContextCandidateView {
            label: format!("active window: {}", truncate(&card.summary, 80)),
            confidence: evidence_ref_confidence(1, MAX_FOCUS_SESSION_EVIDENCE_REFS),
            evidence_refs: vec![card.ref_.clone()],
            proposal_ref: None,
        });
    }

    let activity_refs = sources
        .iter()
        .filter(|(_, card)| {
            is_active_window_evidence(card)
                || is_terminal_evidence(card)
                || is_browser_evidence(card)
        })
        .map(|(_, card)| card.ref_.clone())
        .take(6)
        .collect::<Vec<_>>();

    if activity_refs.len() >= 2 {
        candidates.push(DesktopContextCandidateView {
            label: format!(
                "current activity from {} evidence refs",
                activity_refs.len()
            ),
            confidence: evidence_ref_confidence(
                activity_refs.len(),
                MAX_FOCUS_SESSION_EVIDENCE_REFS,
            ),
            evidence_refs: activity_refs,
            proposal_ref: None,
        });
    }

    candidates
}

fn evidence_ref_confidence(evidence_ref_count: usize, max_evidence_refs: usize) -> f32 {
    if max_evidence_refs == 0 {
        return 0.0;
    }
    let bounded_refs = evidence_ref_count.min(max_evidence_refs) as f32;
    bounded_refs / max_evidence_refs as f32
}

fn desktop_context_evidence_window(view: &DesktopContextView) -> EvidenceWindow {
    let mut support_refs = Vec::new();
    let mut expansion_steps = Vec::new();
    let observed_range = ObservedRange::unknown(TimeBasis::DerivedInterval);

    for candidate in &view.candidates {
        for object_ref in &candidate.evidence_refs {
            if support_refs
                .iter()
                .any(|existing: &EvidenceRef| same_ref(&existing.object, object_ref))
            {
                continue;
            }
            expansion_steps.push(ExpansionStep {
                kind: ExpansionStepKind::RelationIncluded,
                detail: format!("candidate `{}` cited this evidence", candidate.label),
                object_ref: Some(object_ref.clone()),
            });
            support_refs.push(EvidenceRef {
                object: object_ref.clone(),
                role: EvidenceRole::Support,
                observed_range,
                rationale: format!(
                    "supports ranked desktop-context candidate `{}`; confidence is view ranking only",
                    candidate.label
                ),
            });
        }
    }

    let mut caveats = view.caveats.clone();
    for input in &view.inputs {
        match input.state {
            DesktopContextInputState::Missing
            | DesktopContextInputState::Omitted
            | DesktopContextInputState::Redacted
            | DesktopContextInputState::Stale => {
                for caveat in &input.caveats {
                    expansion_steps.push(ExpansionStep {
                        kind: ExpansionStepKind::CoverageGapCaveat,
                        detail: format!("{} input caveat: {}", input.family, caveat.message),
                        object_ref: caveat.ref_.clone(),
                    });
                    caveats.push(caveat.clone());
                }
            }
            DesktopContextInputState::Included => {}
        }
    }

    if support_refs.is_empty() {
        caveats.push(CaveatView {
            id: "context.no_candidate_evidence".to_string(),
            message: "desktop context has no ranked candidates with evidence refs".to_string(),
            ref_: Some(SinexObjectRef::new(
                SinexObjectKind::Projection,
                "desktop.context.current_view",
            )),
        });
    }

    EvidenceWindow {
        seed_refs: view.active_window_ref.iter().cloned().collect(),
        support_refs,
        contradiction_refs: Vec::new(),
        caveats,
        observed_range,
        expansion_trace: ExpansionTrace {
            steps: expansion_steps,
        },
        generated_at: Timestamp::now(),
        query: EventRelationExpr::Sequence { within_secs: 0 },
    }
}

fn same_ref(left: &SinexObjectRef, right: &SinexObjectRef) -> bool {
    left.kind == right.kind && left.id == right.id
}

fn build_notification_pressure_view(
    event_cards: &EventCardListView,
    since: &str,
) -> DesktopNotificationPressureView {
    let mut view = DesktopNotificationPressureView::new(
        sinex_primitives::DESKTOP_NOTIFICATION_PRESSURE_DERIVATION_ID,
        since,
    );

    for card in event_cards
        .cards
        .iter()
        .filter(|card| is_notification_evidence(card))
    {
        match card.event_type.as_str() {
            "notification.sent" => view.sent_count += 1,
            "notification.action_invoked" => view.action_count += 1,
            "notification.closed" => view.closed_count += 1,
            _ => {}
        }
        view.total_notification_events += 1;

        if view.evidence_refs.len() < MAX_NOTIFICATION_PRESSURE_EVIDENCE_REFS
            && !view
                .evidence_refs
                .iter()
                .any(|existing| same_ref(existing, &card.ref_))
        {
            view.evidence_refs.push(card.ref_.clone());
        }

        for caveat in &card.caveats {
            if !view
                .caveats
                .iter()
                .any(|existing| existing.id == caveat.id && existing.ref_ == caveat.ref_)
            {
                view.caveats.push(caveat.clone());
            }
        }
    }

    if view.total_notification_events == 0 {
        view.caveats.push(CaveatView {
            id: "notification_pressure.no_recent_notifications".to_string(),
            message: format!("no notification evidence was found in the last {since}"),
            ref_: Some(SinexObjectRef::new(
                SinexObjectKind::Projection,
                "desktop.notification_pressure",
            )),
        });
    } else if view.total_notification_events > view.evidence_refs.len() {
        view.caveats.push(CaveatView {
            id: "notification_pressure.evidence_truncated".to_string(),
            message: format!(
                "showing {} notification evidence refs out of {} recent notification events",
                view.evidence_refs.len(),
                view.total_notification_events
            ),
            ref_: Some(SinexObjectRef::new(
                SinexObjectKind::Projection,
                "desktop.notification_pressure",
            )),
        });
    }

    view
}

fn build_focus_session_list_view(
    event_cards: &EventCardListView,
    since: &str,
) -> DesktopFocusSessionListView {
    let mut view = DesktopFocusSessionListView::new(
        sinex_primitives::DESKTOP_FOCUS_SESSION_DERIVATION_ID,
        since,
    );
    let mut activity_cards = event_cards
        .cards
        .iter()
        .filter(|card| is_focus_session_evidence(card))
        .collect::<Vec<_>>();
    activity_cards.sort_by(|left, right| {
        let left_ts = left.timestamp.original.unwrap_or(Timestamp::UNIX_EPOCH);
        let right_ts = right.timestamp.original.unwrap_or(Timestamp::UNIX_EPOCH);
        left_ts.inner().cmp(&right_ts.inner())
    });

    if activity_cards.is_empty() {
        view.caveats.push(CaveatView {
            id: "focus_session.no_recent_activity".to_string(),
            message: format!("no focus-session evidence was found in the last {since}"),
            ref_: Some(SinexObjectRef::new(
                SinexObjectKind::Projection,
                "desktop.focus_session",
            )),
        });
        return view;
    }

    let first = activity_cards
        .first()
        .expect("activity cards cannot be empty after guard");
    let last = activity_cards
        .last()
        .expect("activity cards cannot be empty after guard");
    let mut session = DesktopFocusSessionView {
        session_id: format!("desktop.focus_session:{}..{}", first.ref_.id, last.ref_.id),
        started_at: first.timestamp.original,
        ended_at: last.timestamp.original,
        event_count: activity_cards.len(),
        input_families: Vec::new(),
        evidence_refs: Vec::new(),
        caveats: Vec::new(),
    };

    for card in activity_cards {
        let family = desktop_context_family(card);
        if !session.input_families.iter().any(|known| known == &family) {
            session.input_families.push(family);
        }

        if session.evidence_refs.len() < MAX_FOCUS_SESSION_EVIDENCE_REFS
            && !session
                .evidence_refs
                .iter()
                .any(|existing| same_ref(existing, &card.ref_))
        {
            session.evidence_refs.push(card.ref_.clone());
        }

        for caveat in &card.caveats {
            if !session
                .caveats
                .iter()
                .any(|existing| existing.id == caveat.id && existing.ref_ == caveat.ref_)
            {
                session.caveats.push(caveat.clone());
            }
        }
    }

    if session.event_count > session.evidence_refs.len() {
        session.caveats.push(CaveatView {
            id: "focus_session.evidence_truncated".to_string(),
            message: format!(
                "showing {} focus-session evidence refs out of {} recent activity events",
                session.evidence_refs.len(),
                session.event_count
            ),
            ref_: Some(SinexObjectRef::new(
                SinexObjectKind::Projection,
                "desktop.focus_session",
            )),
        });
    }
    session.input_families.sort();
    view.caveats = session.caveats.clone();
    view.sessions.push(session);
    view.session_count = view.sessions.len();
    view
}

fn build_project_context_list_view(
    event_cards: &EventCardListView,
    since: &str,
) -> DesktopProjectContextListView {
    let mut view = DesktopProjectContextListView::new(
        sinex_primitives::DESKTOP_PROJECT_CONTEXT_DERIVATION_ID,
        since,
    );
    let project_cards = event_cards
        .cards
        .iter()
        .filter(|card| is_project_context_evidence(card))
        .collect::<Vec<_>>();

    if project_cards.is_empty() {
        view.caveats.push(CaveatView {
            id: "project_context.no_recent_activity".to_string(),
            message: format!("no project-context evidence was found in the last {since}"),
            ref_: Some(SinexObjectRef::new(
                SinexObjectKind::Projection,
                "desktop.project_context",
            )),
        });
        return view;
    }

    let evidence_refs = project_cards
        .iter()
        .map(|card| card.ref_.clone())
        .take(MAX_PROJECT_CONTEXT_EVIDENCE_REFS)
        .collect::<Vec<_>>();
    let mut input_families = project_cards
        .iter()
        .map(|card| desktop_context_family(card))
        .collect::<Vec<_>>();
    input_families.sort();
    input_families.dedup();

    let mut caveats = project_cards
        .iter()
        .flat_map(|card| card.caveats.clone())
        .collect::<Vec<_>>();
    caveats.push(CaveatView {
        id: "project_context.ranked_view_only".to_string(),
        message: "project context rows are ranked projection candidates; durable labels require Proposal/Judgment finalization".to_string(),
        ref_: Some(SinexObjectRef::new(
            SinexObjectKind::Projection,
            "desktop.project_context",
        )),
    });
    if project_cards.len() > evidence_refs.len() {
        caveats.push(CaveatView {
            id: "project_context.evidence_truncated".to_string(),
            message: format!(
                "showing {} project-context evidence refs out of {} recent activity events",
                evidence_refs.len(),
                project_cards.len()
            ),
            ref_: Some(SinexObjectRef::new(
                SinexObjectKind::Projection,
                "desktop.project_context",
            )),
        });
    }

    let label = project_context_label(&project_cards);
    let confidence =
        evidence_ref_confidence(evidence_refs.len(), MAX_PROJECT_CONTEXT_EVIDENCE_REFS);
    view.rows.push(DesktopProjectContextRowView {
        label,
        confidence,
        focus_session_ref: Some(SinexObjectRef::new(
            SinexObjectKind::Projection,
            "desktop.focus_session:current-window",
        )),
        input_families,
        evidence_refs,
        proposal_ref: None,
        caveats: caveats.clone(),
    });
    view.row_count = view.rows.len();
    view.caveats = caveats;
    view
}

fn desktop_context_input_for_family(
    family: &str,
    sources: &[(String, &EventCardView)],
) -> DesktopContextInputEvidence {
    let matching: Vec<_> = sources
        .iter()
        .filter(|(_, card)| desktop_context_family(card) == family)
        .collect();

    if matching.is_empty() {
        let coverage_ref = SinexObjectRef::new(
            SinexObjectKind::Projection,
            format!("source-coverage:{family}"),
        )
        .with_label(format!("{family} coverage"));
        return DesktopContextInputEvidence {
            family: family.to_string(),
            state: DesktopContextInputState::Missing,
            refs: vec![coverage_ref.clone()],
            caveats: vec![CaveatView {
                id: format!("input.{family}.missing"),
                message: format!("{family} input has no recent admitted evidence"),
                ref_: Some(coverage_ref),
            }],
            actions: vec![
                ActionAvailability::read(
                    format!("sources.{family}.check"),
                    format!("Check {family}"),
                    ActionAvailabilityState::Enabled,
                )
                .with_command_hint(format!("sinexctl sources readiness --family {family}")),
            ],
        };
    }

    let refs = matching.iter().map(|(_, card)| card.ref_.clone()).collect();
    let caveats = matching
        .iter()
        .flat_map(|(_, card)| card.caveats.clone())
        .collect::<Vec<_>>();
    let state = if matching.iter().any(|(_, card)| {
        card.privacy_state.state != PrivacyStateKind::RawVisible
            || card
                .caveats
                .iter()
                .any(|caveat| caveat.id.contains("redact") || caveat.id.contains("disclosure"))
    }) {
        DesktopContextInputState::Redacted
    } else {
        DesktopContextInputState::Included
    };

    DesktopContextInputEvidence {
        family: family.to_string(),
        state,
        refs,
        caveats,
        actions: Vec::new(),
    }
}

fn render_desktop_context_table(view: &DesktopContextView, since: &str) -> String {
    let mut lines = vec![
        format!("Desktop context (last {since})"),
        "input family        state      refs  caveats".to_string(),
        "────────────────────────────────────────────".to_string(),
    ];
    for input in &view.inputs {
        lines.push(format!(
            "{:<19} {:<10} {:>4}  {:>7}",
            input.family,
            serde_json::to_value(input.state)
                .ok()
                .and_then(|value| value.as_str().map(ToOwned::to_owned))
                .unwrap_or_else(|| "unknown".to_string()),
            input.refs.len(),
            input.caveats.len(),
        ));
    }
    if !view.caveats.is_empty() {
        lines.push(String::new());
        lines.push("caveats".to_string());
        for caveat in &view.caveats {
            lines.push(format!("- {}: {}", caveat.id, caveat.message));
        }
    }
    if !view.candidates.is_empty() {
        lines.push(String::new());
        lines.push("candidates".to_string());
        for candidate in &view.candidates {
            lines.push(format!(
                "- {:.0}% {} ({} refs)",
                candidate.confidence * 100.0,
                candidate.label,
                candidate.evidence_refs.len()
            ));
        }
    }
    lines.join("\n")
}

fn render_notification_pressure_table(view: &DesktopNotificationPressureView) -> String {
    let mut lines = vec![
        format!("Desktop notification pressure (last {})", view.since),
        format!("sent:   {}", view.sent_count),
        format!("action: {}", view.action_count),
        format!("closed: {}", view.closed_count),
        format!("refs:   {}", view.evidence_refs.len()),
    ];
    if !view.caveats.is_empty() {
        lines.push(String::new());
        lines.push("caveats".to_string());
        for caveat in &view.caveats {
            lines.push(format!("- {}: {}", caveat.id, caveat.message));
        }
    }
    lines.join("\n")
}

fn render_focus_session_table(view: &DesktopFocusSessionListView) -> String {
    let mut lines = vec![
        format!("Desktop focus sessions (last {})", view.since),
        format!("sessions: {}", view.session_count),
    ];
    for session in &view.sessions {
        lines.push(format!(
            "- {}: {} events, {} refs, families: {}",
            session.session_id,
            session.event_count,
            session.evidence_refs.len(),
            session.input_families.join(", ")
        ));
    }
    if !view.caveats.is_empty() {
        lines.push(String::new());
        lines.push("caveats".to_string());
        for caveat in &view.caveats {
            lines.push(format!("- {}: {}", caveat.id, caveat.message));
        }
    }
    lines.join("\n")
}

fn render_project_context_table(view: &DesktopProjectContextListView) -> String {
    let mut lines = vec![
        format!("Desktop project contexts (last {})", view.since),
        format!("rows: {}", view.row_count),
    ];
    for row in &view.rows {
        lines.push(format!(
            "- {:.0}% {} ({} refs, families: {})",
            row.confidence * 100.0,
            row.label,
            row.evidence_refs.len(),
            row.input_families.join(", ")
        ));
    }
    if !view.caveats.is_empty() {
        lines.push(String::new());
        lines.push("caveats".to_string());
        for caveat in &view.caveats {
            lines.push(format!("- {}: {}", caveat.id, caveat.message));
        }
    }
    lines.join("\n")
}

fn is_focus_session_evidence(card: &EventCardView) -> bool {
    is_active_window_evidence(card) || is_terminal_evidence(card) || is_browser_evidence(card)
}

fn is_project_context_evidence(card: &EventCardView) -> bool {
    is_terminal_evidence(card) || is_browser_evidence(card) || is_active_window_evidence(card)
}

fn project_context_label(cards: &[&EventCardView]) -> String {
    if let Some(card) = cards.iter().find(|card| is_terminal_evidence(card)) {
        return format!("terminal activity: {}", truncate(&card.summary, 72));
    }
    if let Some(card) = cards.iter().find(|card| is_browser_evidence(card)) {
        return format!("browser activity: {}", truncate(&card.summary, 72));
    }
    if let Some(card) = cards.iter().find(|card| is_active_window_evidence(card)) {
        return format!("window activity: {}", truncate(&card.summary, 72));
    }
    "ambiguous project context".to_string()
}

fn desktop_context_family(card: &EventCardView) -> String {
    if is_notification_evidence(card) {
        return "notification".to_string();
    }
    if is_browser_evidence(card) {
        return "browser".to_string();
    }
    if is_terminal_evidence(card) {
        return "terminal".to_string();
    }
    if is_desktop_evidence(card) {
        return "desktop".to_string();
    }
    display_source(card.source.raw.as_str())
}

fn is_notification_evidence(card: &EventCardView) -> bool {
    match card.source.raw.as_str() {
        "desktop.notification" | "desktop.notification.action" | "desktop.notification.closed" => {
            true
        }
        "dbus" => card.event_type.starts_with("notification."),
        _ => false,
    }
}

fn is_browser_evidence(card: &EventCardView) -> bool {
    match card.source.raw.as_str() {
        "webhistory" => true,
        source if source.starts_with("browser.") => true,
        "activitywatch" => card.event_type.starts_with("browser."),
        _ => false,
    }
}

fn is_terminal_evidence(card: &EventCardView) -> bool {
    let source = card.source.raw.as_str();
    source.starts_with("shell.") || source.starts_with("terminal.")
}

fn is_desktop_evidence(card: &EventCardView) -> bool {
    match card.source.raw.as_str() {
        "wm.hyprland" | "wm.unhandled" | "desktop" => true,
        "activitywatch" => !card.event_type.starts_with("browser."),
        _ => false,
    }
}

fn is_active_window_evidence(card: &EventCardView) -> bool {
    match card.source.raw.as_str() {
        "wm.hyprland" | "desktop" => {
            matches!(card.event_type.as_str(), "window.focused" | "window.active")
        }
        "activitywatch" => {
            matches!(
                card.event_type.as_str(),
                "window.active" | "app.window.active"
            )
        }
        _ => false,
    }
}

/// Produce a compact, human-readable source label from an event-source name.
///
/// The mapping table is keyed by the `event_source` namespace values used
/// inside `core.events` (e.g. `shell.atuin`, `wm.hyprland`, `fs-watcher`)
/// — these strings are emitted by source contracts hosted inside `sinexd`.
/// Old package names and the `sinexd` binary are not runtime identities.
fn display_source(source: &str) -> String {
    let friendly = match source {
        "shell.atuin" | "shell.asciinema" | "shell.kitty" | "shell.scrollback" => "terminal",
        "wm.hyprland" | "wm.unhandled" => "desktop",
        "fs-watcher" => "filesystem",
        "journald" | "dbus" | "udev" => "system",
        "clipboard" => "clipboard",
        "webhistory" => "browser",
        "git" => "git",
        s if s.starts_with("browser.") => "browser",
        s if s.starts_with("derived.") => "derived",
        s if s.starts_with("device.") => "device",
        s if s.starts_with("bluetooth.") => "bluetooth",
        s if s.starts_with("blob") => "blob-store",
        s if s.starts_with("sinex.") => "platform",
        s if s.starts_with("canonical.") => "canonical",
        _ => "",
    };

    if !friendly.is_empty() {
        return friendly.to_string();
    }

    // Fallback: strip common prefixes/suffixes
    let mut s = source;
    s = s.strip_prefix("sinex-").unwrap_or(s);
    s = s.strip_prefix("sinex.").unwrap_or(s);
    s = s.strip_suffix("-automaton").unwrap_or(s);
    s.to_string()
}

/// Format a Duration into a compact "`XmYs` ago" / "Xs ago" / "Xh ago" string.
fn format_age(d: time::Duration) -> String {
    format_duration_age(d)
}

/// Truncate a string with ellipsis if over `max` chars.
fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        // Truncate at char boundary
        let end = s
            .char_indices()
            .map(|(i, _)| i)
            .nth(max.saturating_sub(3))
            .unwrap_or(s.len());
        format!("{}...", &s[..end])
    }
}

#[cfg(test)]
#[path = "context_test.rs"]
mod tests;
