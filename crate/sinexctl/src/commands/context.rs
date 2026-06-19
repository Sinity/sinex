use crate::parse::parse_duration;
use clap::Args;
use color_eyre::Result;
use console::style;
use serde_json::json;
#[cfg(test)]
use sinex_primitives::query::QueryResultEvent;
use sinex_primitives::query::{EventQuery, SortDirection, TimeRange};
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::views::{
    ActionAvailability, ActionAvailabilityState, CaveatView, ContextSourceView, ContextSummaryView,
    DesktopContextInputEvidence, DesktopContextInputState, DesktopContextView, EventCardListView,
    EventCardView, PrivacyStateKind, SinexObjectKind, SinexObjectRef, ViewEnvelope,
};
use std::collections::HashMap;

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
            let output =
                render_desktop_context_output(&event_cards, &sources, &self.since, format)?;
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
) -> Result<String> {
    if matches!(format, OutputFormat::Ndjson | OutputFormat::Dot) {
        return Err(color_eyre::eyre::eyre!(
            "desktop context is a finite view; use json, yaml, or table"
        ));
    }

    let view = build_desktop_context_view(event_cards, sources, since);
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
    lines.join("\n")
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
            matches!(card.event_type.as_str(), "window.active" | "app.window.active")
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
        let output =
            render_desktop_context_output(&event_cards, &sources, "2h", OutputFormat::Json)?;
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
        let output =
            render_desktop_context_output(&event_cards, &sources, "2h", OutputFormat::Json)?;
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
    async fn desktop_context_output_rejects_streaming_formats() -> xtask::sandbox::TestResult<()> {
        let event_cards = EventCardListView {
            schema_version: EVENT_CARD_LIST_SCHEMA_VERSION.to_string(),
            count: 0,
            cards: Vec::new(),
            next_cursor: None,
            total_estimate: None,
        };
        let sources = grouped_context_sources(&event_cards.cards);
        let result =
            render_desktop_context_output(&event_cards, &sources, "2h", OutputFormat::Ndjson);

        assert!(result.is_err(), "desktop context must remain a finite view");
        Ok(())
    }
}
