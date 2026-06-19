use crate::parse::parse_duration;
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
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::views::{
    ActionAvailability, ActionAvailabilityState, CaveatView, ContextSourceView, ContextSummaryView,
    DesktopContextCandidateView, DesktopContextInputEvidence, DesktopContextInputState,
    DesktopContextView, DesktopFocusSessionListView, DesktopFocusSessionView,
    DesktopNotificationPressureView, EventCardListView, EventCardView, PrivacyStateKind,
    SinexObjectKind, SinexObjectRef, ViewEnvelope,
};
use std::collections::HashMap;

const MAX_FOCUS_SESSION_EVIDENCE_REFS: usize = 12;
const MAX_NOTIFICATION_PRESSURE_EVIDENCE_REFS: usize = 12;

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
")]
pub struct ContextCommand {
    /// Time window to look back (default: last 2 hours)
    #[arg(long, short = 's', default_value = "2h")]
    since: String,

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
        conflicts_with_all = ["explain", "notification_pressure"]
    )]
    focus_sessions: bool,
}

impl ContextCommand {
    pub async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let since = parse_duration(&self.since)?;
        let now = Timestamp::now();
        let cutoff = now - since;

        let query = EventQuery {
            sources: vec![],
            event_types: vec![],
            time_range: TimeRange::new(Some(cutoff), None).ok(),
            payload: None,
            limit: i64::from(self.limit),
            direction: SortDirection::Desc,
            ..Default::default()
        };

        let event_cards = client.event_cards(query).await?;

        let sources = grouped_context_sources(&event_cards.cards);
        if self.desktop {
            let output = render_desktop_context_output(
                &event_cards,
                &sources,
                &self.since,
                format,
                self.explain,
                self.notification_pressure,
                self.focus_sessions,
            )?;
            println!("{output}");
            return Ok(());
        }

        if let Some(output) =
            render_context_machine_output(&event_cards, &sources, &self.since, format)?
        {
            println!("{output}");
            return Ok(());
        }

        if event_cards.cards.is_empty() {
            println!(
                "{} No activity found in the last {}",
                style("○").dim(),
                self.since
            );
            return Ok(());
        }

        println!(
            "{} {}",
            style(format!("Context (last {}):", self.since))
                .bold()
                .cyan(),
            style(format!("{} sources", sources.len())).dim()
        );
        println!("{}", style("─".repeat(60)).dim());

        // Column widths: source label padded to longest name for alignment
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
            "  {} events across {} sources in last {}",
            style(event_cards.count).bold(),
            style(sources.len()).bold(),
            self.since,
        );

        Ok(())
    }
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

fn render_context_machine_output(
    event_cards: &EventCardListView,
    sources: &[(String, &EventCardView)],
    since: &str,
    format: OutputFormat,
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
                "sinexctl.context",
                ContextSummaryView::new(since, event_cards.count, source_views),
            )
            .with_query_echo(json!({ "since": since }));

            render_envelope(&envelope, &envelope.payload.sources, format)
        }
        OutputFormat::Ndjson | OutputFormat::Dot => Err(color_eyre::eyre::eyre!(
            "events context is a finite view; use json, yaml, or table"
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
            confidence: 0.72,
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
            confidence: (0.55 + (activity_refs.len() as f32 * 0.05)).min(0.85),
            evidence_refs: activity_refs,
            proposal_ref: None,
        });
    }

    candidates
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

fn is_focus_session_evidence(card: &EventCardView) -> bool {
    is_active_window_evidence(card) || is_terminal_evidence(card) || is_browser_evidence(card)
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
mod tests {
    use super::*;
    use sinex_primitives::testing::event_fixture;
    use sinex_primitives::views::{
        CONTEXT_SUMMARY_SCHEMA_VERSION, CaveatView, EVENT_CARD_LIST_SCHEMA_VERSION,
        VIEW_ENVELOPE_SCHEMA_VERSION,
    };
    use xtask::sandbox::prelude::sinex_test;

    fn context_event(source: &'static str, event_type: &'static str) -> EventCardView {
        EventCardView::from_query_event(&QueryResultEvent {
            event: event_fixture(
                sinex_primitives::EventSource::from_static(source),
                sinex_primitives::EventType::from_static(event_type),
                json!({ "message": "context fixture" }),
            ),
            relevance_score: None,
            snippet: Some("context fixture".to_string()),
        })
    }

    fn context_event_with_ref(
        source: &'static str,
        event_type: &'static str,
        ref_id: impl Into<String>,
    ) -> EventCardView {
        let mut card = context_event(source, event_type);
        card.ref_.id = ref_id.into();
        card
    }

    #[sinex_test]
    async fn context_machine_output_uses_view_envelope_json() -> xtask::sandbox::TestResult<()> {
        let mut shell_card = context_event("shell.atuin", "command.executed");
        shell_card.caveats.push(CaveatView {
            id: "policy.disclosure_applied".to_string(),
            message: "payload field redacted by fixture policy".to_string(),
            ref_: None,
        });
        let event_cards = EventCardListView {
            schema_version: EVENT_CARD_LIST_SCHEMA_VERSION.to_string(),
            count: 2,
            cards: vec![shell_card, context_event("wm.hyprland", "window.focused")],
            next_cursor: None,
            total_estimate: None,
        };
        let sources = grouped_context_sources(&event_cards.cards);
        let output =
            render_context_machine_output(&event_cards, &sources, "2h", OutputFormat::Json)?
                .ok_or_else(|| color_eyre::eyre::eyre!("json output expected"))?;
        let value: serde_json::Value = serde_json::from_str(&output)?;

        assert_eq!(value["schema_version"], VIEW_ENVELOPE_SCHEMA_VERSION);
        assert_eq!(value["source_surface"], "sinexctl.context");
        assert_eq!(value["query_echo"]["since"], "2h");
        assert_eq!(
            value["payload"]["schema_version"],
            CONTEXT_SUMMARY_SCHEMA_VERSION
        );
        assert_eq!(value["payload"]["since"], "2h");
        assert_eq!(value["payload"]["total_events"], 2);
        assert_eq!(value["payload"]["source_count"], 2);
        assert_eq!(
            value["payload"]["sources"][0]["latest_event"]["summary"],
            "context fixture"
        );
        let source_views = value["payload"]["sources"]
            .as_array()
            .ok_or_else(|| color_eyre::eyre::eyre!("context sources must be an array"))?;
        assert!(
            source_views
                .iter()
                .filter_map(|source| source["latest_event"]["caveats"].as_array())
                .flatten()
                .any(|caveat| caveat["id"] == "policy.disclosure_applied"),
            "context cards must preserve disclosure caveats: {source_views:?}"
        );
        Ok(())
    }

    #[sinex_test]
    async fn context_machine_output_rejects_ndjson() -> xtask::sandbox::TestResult<()> {
        let event_cards = EventCardListView {
            schema_version: EVENT_CARD_LIST_SCHEMA_VERSION.to_string(),
            count: 1,
            cards: vec![context_event("shell.atuin", "command.executed")],
            next_cursor: None,
            total_estimate: None,
        };
        let sources = grouped_context_sources(&event_cards.cards);
        let result =
            render_context_machine_output(&event_cards, &sources, "2h", OutputFormat::Ndjson);
        assert!(result.is_err(), "context must remain a finite view");
        Ok(())
    }

    #[sinex_test]
    async fn desktop_context_json_uses_typed_view_with_missing_inputs()
    -> xtask::sandbox::TestResult<()> {
        let mut terminal_card = context_event("shell.atuin", "command.executed");
        terminal_card.caveats.push(CaveatView {
            id: "policy.disclosure_applied".to_string(),
            message: "terminal command hidden by fixture disclosure policy".to_string(),
            ref_: None,
        });
        let event_cards = EventCardListView {
            schema_version: EVENT_CARD_LIST_SCHEMA_VERSION.to_string(),
            count: 2,
            cards: vec![
                context_event("wm.hyprland", "window.focused"),
                terminal_card,
            ],
            next_cursor: None,
            total_estimate: None,
        };
        let sources = grouped_context_sources(&event_cards.cards);
        let output = render_desktop_context_output(
            &event_cards,
            &sources,
            "2h",
            OutputFormat::Json,
            false,
            false,
            false,
        )?;
        let value: serde_json::Value = serde_json::from_str(&output)?;

        assert_eq!(value["schema_version"], VIEW_ENVELOPE_SCHEMA_VERSION);
        assert_eq!(value["source_surface"], "sinexctl.events.context.desktop");
        assert_eq!(value["payload"]["output_kind"], "current_view");
        assert_eq!(
            value["payload"]["derivation_ref"],
            sinex_primitives::DESKTOP_CONTEXT_CURRENT_VIEW_DERIVATION_ID
        );

        let inputs = value["payload"]["inputs"]
            .as_array()
            .ok_or_else(|| color_eyre::eyre::eyre!("desktop inputs must be an array"))?;
        assert!(
            inputs
                .iter()
                .any(|input| input["family"] == "desktop" && input["state"] == "included")
        );
        assert!(
            inputs
                .iter()
                .any(|input| input["family"] == "terminal" && input["state"] == "redacted")
        );
        assert!(
            inputs
                .iter()
                .any(|input| input["family"] == "browser" && input["state"] == "missing")
        );
        assert!(
            inputs
                .iter()
                .any(|input| input["family"] == "notification" && input["state"] == "missing")
        );
        assert!(
            inputs.iter().any(
                |input| input["actions"].as_array().is_some_and(|actions| actions
                    .iter()
                    .any(|action| action["id"] == "sources.browser.check"))
            ),
            "missing browser evidence should surface an operator action"
        );
        assert!(value["caveats"].as_array().is_some_and(|caveats| {
            caveats
                .iter()
                .any(|caveat| caveat["id"] == "context.inputs_missing")
        }));
        Ok(())
    }

    #[sinex_test]
    async fn desktop_context_classifies_activitywatch_browser_events()
    -> xtask::sandbox::TestResult<()> {
        let event_cards = EventCardListView {
            schema_version: EVENT_CARD_LIST_SCHEMA_VERSION.to_string(),
            count: 2,
            cards: vec![
                context_event("activitywatch", "browser.tab.active"),
                context_event("wm.hyprland", "workspace.switched"),
            ],
            next_cursor: None,
            total_estimate: None,
        };
        let sources = grouped_context_sources(&event_cards.cards);
        let output = render_desktop_context_output(
            &event_cards,
            &sources,
            "2h",
            OutputFormat::Json,
            false,
            false,
            false,
        )?;
        let value: serde_json::Value = serde_json::from_str(&output)?;
        let inputs = value["payload"]["inputs"]
            .as_array()
            .ok_or_else(|| color_eyre::eyre::eyre!("desktop inputs must be an array"))?;

        assert!(
            inputs
                .iter()
                .any(|input| input["family"] == "browser" && input["state"] == "included"),
            "ActivityWatch browser observations should satisfy the browser input family"
        );
        assert!(
            value["payload"]["active_window_ref"].is_null(),
            "workspace events are desktop evidence but not active-window evidence"
        );
        Ok(())
    }

    #[sinex_test]
    async fn desktop_context_candidates_are_evidence_backed_view_output()
    -> xtask::sandbox::TestResult<()> {
        let event_cards = EventCardListView {
            schema_version: EVENT_CARD_LIST_SCHEMA_VERSION.to_string(),
            count: 3,
            cards: vec![
                context_event_with_ref("wm.hyprland", "window.focused", "event:desktop"),
                context_event_with_ref("shell.atuin", "command.executed", "event:terminal"),
                context_event_with_ref("activitywatch", "browser.tab.active", "event:browser"),
            ],
            next_cursor: None,
            total_estimate: None,
        };
        let sources = grouped_context_sources(&event_cards.cards);
        let output = render_desktop_context_output(
            &event_cards,
            &sources,
            "2h",
            OutputFormat::Json,
            false,
            false,
            false,
        )?;
        let value: serde_json::Value = serde_json::from_str(&output)?;

        let candidates = value["payload"]["candidates"]
            .as_array()
            .ok_or_else(|| color_eyre::eyre::eyre!("desktop candidates must be an array"))?;
        assert!(
            candidates.iter().any(|candidate| candidate["label"]
                .as_str()
                .is_some_and(|label| label.starts_with("active window:"))
                && candidate["evidence_refs"]
                    .as_array()
                    .is_some_and(|refs| refs.len() == 1)
                && candidate["proposal_ref"].is_null()),
            "active-window candidate should be evidence-backed view output: {candidates:?}"
        );
        assert!(
            candidates.iter().any(|candidate| candidate["label"]
                == "current activity from 3 evidence refs"
                && candidate["evidence_refs"]
                    .as_array()
                    .is_some_and(|refs| refs.len() == 3)
                && candidate["proposal_ref"].is_null()),
            "multi-signal activity candidate should cite each evidence ref without claiming authority: {candidates:?}"
        );
        assert!(value["caveats"].as_array().is_some_and(|caveats| {
            caveats
                .iter()
                .any(|caveat| caveat["id"] == "context.candidates_ranked_view")
        }));
        Ok(())
    }

    #[sinex_test]
    async fn desktop_context_table_shows_candidate_evidence_counts()
    -> xtask::sandbox::TestResult<()> {
        let event_cards = EventCardListView {
            schema_version: EVENT_CARD_LIST_SCHEMA_VERSION.to_string(),
            count: 2,
            cards: vec![
                context_event("wm.hyprland", "window.focused"),
                context_event("shell.atuin", "command.executed"),
            ],
            next_cursor: None,
            total_estimate: None,
        };
        let sources = grouped_context_sources(&event_cards.cards);
        let output = render_desktop_context_output(
            &event_cards,
            &sources,
            "2h",
            OutputFormat::Table,
            false,
            false,
            false,
        )?;

        assert!(output.contains("candidates"));
        assert!(output.contains("active window: context fixture (1 refs)"));
        assert!(output.contains("current activity from 2 evidence refs (2 refs)"));
        Ok(())
    }

    #[sinex_test]
    async fn desktop_context_explain_returns_evidence_window() -> xtask::sandbox::TestResult<()> {
        let event_cards = EventCardListView {
            schema_version: EVENT_CARD_LIST_SCHEMA_VERSION.to_string(),
            count: 3,
            cards: vec![
                context_event_with_ref("wm.hyprland", "window.focused", "event:desktop"),
                context_event_with_ref("shell.atuin", "command.executed", "event:terminal"),
                context_event_with_ref("activitywatch", "browser.tab.active", "event:browser"),
            ],
            next_cursor: None,
            total_estimate: None,
        };
        let sources = grouped_context_sources(&event_cards.cards);
        let output = render_desktop_context_output(
            &event_cards,
            &sources,
            "2h",
            OutputFormat::Json,
            true,
            false,
            false,
        )?;
        let value: serde_json::Value = serde_json::from_str(&output)?;

        assert_eq!(
            value["source_surface"],
            "sinexctl.events.context.desktop.explain"
        );
        assert_eq!(
            value["query_echo"]["mode"],
            "desktop_context_evidence_window"
        );
        assert_eq!(value["payload"]["query"]["relation"], "sequence");
        let support_refs = value["payload"]["support_refs"]
            .as_array()
            .ok_or_else(|| color_eyre::eyre::eyre!("support_refs must be an array"))?;
        for expected_id in ["event:desktop", "event:terminal", "event:browser"] {
            assert!(
                support_refs
                    .iter()
                    .any(|support| support["object"]["id"] == expected_id),
                "explain output should cite {expected_id}: {support_refs:?}"
            );
        }
        assert!(
            value["payload"]["contradiction_refs"]
                .as_array()
                .is_some_and(Vec::is_empty),
            "desktop context explain must not invent contradictions"
        );
        assert!(value["caveats"].as_array().is_some_and(|caveats| {
            caveats
                .iter()
                .any(|caveat| caveat["id"] == "context.candidates_ranked_view")
        }));
        Ok(())
    }

    #[sinex_test]
    async fn desktop_context_explain_surfaces_missing_input_caveats()
    -> xtask::sandbox::TestResult<()> {
        let event_cards = EventCardListView {
            schema_version: EVENT_CARD_LIST_SCHEMA_VERSION.to_string(),
            count: 1,
            cards: vec![context_event("wm.hyprland", "window.focused")],
            next_cursor: None,
            total_estimate: None,
        };
        let sources = grouped_context_sources(&event_cards.cards);
        let output = render_desktop_context_output(
            &event_cards,
            &sources,
            "2h",
            OutputFormat::Json,
            true,
            false,
            false,
        )?;
        let value: serde_json::Value = serde_json::from_str(&output)?;

        assert!(
            value["payload"]["expansion_trace"]["steps"]
                .as_array()
                .is_some_and(|steps| steps.iter().any(|step| step["kind"]
                    == "coverage_gap_caveat"
                    && step["detail"]
                        .as_str()
                        .is_some_and(|detail| detail.contains("browser input caveat"))))
        );
        assert!(value["caveats"].as_array().is_some_and(|caveats| {
            caveats
                .iter()
                .any(|caveat| caveat["id"] == "input.browser.missing")
        }));
        Ok(())
    }

    #[sinex_test]
    async fn desktop_notification_pressure_counts_notification_evidence()
    -> xtask::sandbox::TestResult<()> {
        let event_cards = EventCardListView {
            schema_version: EVENT_CARD_LIST_SCHEMA_VERSION.to_string(),
            count: 4,
            cards: vec![
                context_event_with_ref("desktop.notification", "notification.sent", "event:sent"),
                context_event_with_ref(
                    "desktop.notification.action",
                    "notification.action_invoked",
                    "event:action",
                ),
                context_event_with_ref(
                    "desktop.notification.closed",
                    "notification.closed",
                    "event:closed",
                ),
                context_event("wm.hyprland", "window.focused"),
            ],
            next_cursor: None,
            total_estimate: None,
        };
        let sources = grouped_context_sources(&event_cards.cards);
        let output = render_desktop_context_output(
            &event_cards,
            &sources,
            "2h",
            OutputFormat::Json,
            false,
            true,
            false,
        )?;
        let value: serde_json::Value = serde_json::from_str(&output)?;

        assert_eq!(
            value["source_surface"],
            "sinexctl.events.context.desktop.notification_pressure"
        );
        assert_eq!(
            value["payload"]["derivation_ref"],
            sinex_primitives::DESKTOP_NOTIFICATION_PRESSURE_DERIVATION_ID
        );
        assert_eq!(
            value["payload"]["output_kind"],
            "notification_pressure_projection"
        );
        assert_eq!(value["payload"]["sent_count"], 1);
        assert_eq!(value["payload"]["action_count"], 1);
        assert_eq!(value["payload"]["closed_count"], 1);
        assert_eq!(value["payload"]["total_notification_events"], 3);
        let refs = value["payload"]["evidence_refs"]
            .as_array()
            .ok_or_else(|| color_eyre::eyre::eyre!("evidence_refs must be an array"))?;
        for expected_id in ["event:sent", "event:action", "event:closed"] {
            assert!(
                refs.iter().any(|ref_| ref_["id"] == expected_id),
                "notification-pressure view should cite {expected_id}: {refs:?}"
            );
        }
        Ok(())
    }

    #[sinex_test]
    async fn desktop_notification_pressure_bounds_evidence_refs() -> xtask::sandbox::TestResult<()>
    {
        let mut cards = Vec::new();
        for index in 0..(MAX_NOTIFICATION_PRESSURE_EVIDENCE_REFS + 3) {
            cards.push(context_event_with_ref(
                "desktop.notification",
                "notification.sent",
                format!("event:notification:{index}"),
            ));
        }
        cards[0].caveats.push(CaveatView {
            id: "policy.disclosure_applied".to_string(),
            message: "notification body hidden by fixture disclosure policy".to_string(),
            ref_: None,
        });

        let event_cards = EventCardListView {
            schema_version: EVENT_CARD_LIST_SCHEMA_VERSION.to_string(),
            count: cards.len(),
            cards,
            next_cursor: None,
            total_estimate: None,
        };
        let sources = grouped_context_sources(&event_cards.cards);
        let output = render_desktop_context_output(
            &event_cards,
            &sources,
            "2h",
            OutputFormat::Json,
            false,
            true,
            false,
        )?;
        let value: serde_json::Value = serde_json::from_str(&output)?;

        assert_eq!(
            value["payload"]["total_notification_events"],
            MAX_NOTIFICATION_PRESSURE_EVIDENCE_REFS + 3
        );
        assert_eq!(
            value["payload"]["evidence_refs"].as_array().map(Vec::len),
            Some(MAX_NOTIFICATION_PRESSURE_EVIDENCE_REFS)
        );
        assert!(value["caveats"].as_array().is_some_and(|caveats| {
            caveats
                .iter()
                .any(|caveat| caveat["id"] == "notification_pressure.evidence_truncated")
                && caveats
                    .iter()
                    .any(|caveat| caveat["id"] == "policy.disclosure_applied")
        }));
        Ok(())
    }

    #[sinex_test]
    async fn desktop_focus_sessions_project_recent_activity_evidence()
    -> xtask::sandbox::TestResult<()> {
        let event_cards = EventCardListView {
            schema_version: EVENT_CARD_LIST_SCHEMA_VERSION.to_string(),
            count: 4,
            cards: vec![
                context_event_with_ref("wm.hyprland", "window.focused", "event:desktop"),
                context_event_with_ref("shell.atuin", "command.executed", "event:terminal"),
                context_event_with_ref("activitywatch", "browser.tab.active", "event:browser"),
                context_event_with_ref(
                    "desktop.notification",
                    "notification.sent",
                    "event:notification",
                ),
            ],
            next_cursor: None,
            total_estimate: None,
        };
        let sources = grouped_context_sources(&event_cards.cards);
        let output = render_desktop_context_output(
            &event_cards,
            &sources,
            "2h",
            OutputFormat::Json,
            false,
            false,
            true,
        )?;
        let value: serde_json::Value = serde_json::from_str(&output)?;

        assert_eq!(
            value["source_surface"],
            "sinexctl.events.context.desktop.focus_sessions"
        );
        assert_eq!(
            value["payload"]["derivation_ref"],
            sinex_primitives::DESKTOP_FOCUS_SESSION_DERIVATION_ID
        );
        assert_eq!(value["payload"]["output_kind"], "focus_session_projection");
        assert_eq!(value["payload"]["session_count"], 1);
        let session = &value["payload"]["sessions"][0];
        assert_eq!(session["event_count"], 3);
        assert!(
            session["input_families"]
                .as_array()
                .is_some_and(|families| ["browser", "desktop", "terminal"]
                    .iter()
                    .all(|family| families.iter().any(|value| value == family))),
            "focus-session projection should classify desktop, terminal, and browser evidence: {session:?}"
        );
        let refs = session["evidence_refs"]
            .as_array()
            .ok_or_else(|| color_eyre::eyre::eyre!("evidence_refs must be an array"))?;
        for expected_id in ["event:desktop", "event:terminal", "event:browser"] {
            assert!(
                refs.iter().any(|ref_| ref_["id"] == expected_id),
                "focus-session view should cite {expected_id}: {refs:?}"
            );
        }
        assert!(
            refs.iter().all(|ref_| ref_["id"] != "event:notification"),
            "notification pressure is a sibling projection, not focus-session evidence"
        );
        Ok(())
    }

    #[sinex_test]
    async fn desktop_focus_sessions_bound_evidence_refs_and_preserve_caveats()
    -> xtask::sandbox::TestResult<()> {
        let mut cards = Vec::new();
        for index in 0..(MAX_FOCUS_SESSION_EVIDENCE_REFS + 3) {
            cards.push(context_event_with_ref(
                "wm.hyprland",
                "window.focused",
                format!("event:window:{index}"),
            ));
        }
        cards[0].caveats.push(CaveatView {
            id: "policy.disclosure_applied".to_string(),
            message: "window title hidden by fixture disclosure policy".to_string(),
            ref_: None,
        });

        let event_cards = EventCardListView {
            schema_version: EVENT_CARD_LIST_SCHEMA_VERSION.to_string(),
            count: cards.len(),
            cards,
            next_cursor: None,
            total_estimate: None,
        };
        let sources = grouped_context_sources(&event_cards.cards);
        let output = render_desktop_context_output(
            &event_cards,
            &sources,
            "2h",
            OutputFormat::Json,
            false,
            false,
            true,
        )?;
        let value: serde_json::Value = serde_json::from_str(&output)?;

        let session = &value["payload"]["sessions"][0];
        assert_eq!(session["event_count"], MAX_FOCUS_SESSION_EVIDENCE_REFS + 3);
        assert_eq!(
            session["evidence_refs"].as_array().map(Vec::len),
            Some(MAX_FOCUS_SESSION_EVIDENCE_REFS)
        );
        assert!(value["caveats"].as_array().is_some_and(|caveats| {
            caveats
                .iter()
                .any(|caveat| caveat["id"] == "focus_session.evidence_truncated")
                && caveats
                    .iter()
                    .any(|caveat| caveat["id"] == "policy.disclosure_applied")
        }));
        Ok(())
    }

    #[sinex_test]
    async fn desktop_context_output_rejects_streaming_formats() -> xtask::sandbox::TestResult<()> {
        let event_cards = EventCardListView {
            schema_version: EVENT_CARD_LIST_SCHEMA_VERSION.to_string(),
            count: 0,
            cards: Vec::new(),
            next_cursor: None,
            total_estimate: None,
        };
        let sources = grouped_context_sources(&event_cards.cards);
        let result = render_desktop_context_output(
            &event_cards,
            &sources,
            "2h",
            OutputFormat::Ndjson,
            false,
            false,
            false,
        );

        assert!(result.is_err(), "desktop context must remain a finite view");
        Ok(())
    }
}
