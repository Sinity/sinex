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
    ContextSourceView, ContextSummaryView, EventCardListView, EventCardView, ViewEnvelope,
};
use std::collections::HashMap;

use crate::client::GatewayClient;
use crate::fmt::format_duration_age;
use crate::fmt::render_envelope;
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
}
