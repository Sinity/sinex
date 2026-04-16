use clap::Args;
use color_eyre::Result;
use console::style;
use sinex_primitives::query::{EventQuery, EventQueryResult, SortDirection, TimeRange};
use sinex_primitives::temporal::{Duration, Timestamp};
use std::collections::HashMap;

use crate::client::GatewayClient;

/// Show activity context for session resumption ("what was I doing?")
#[derive(Debug, Args)]
#[command(after_help = "\
EXAMPLES:
    # What was I doing in the last 2 hours?
    sinexctl context

    # Wider window
    sinexctl context --since 4h

    # Narrow to last 30 minutes
    sinexctl context --since 30m
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
    pub async fn execute(&self, client: &GatewayClient) -> Result<()> {
        let since = parse_duration_str(&self.since)?;
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

        let events = match client.query_events(query).await? {
            EventQueryResult::Events { events, .. } => events,
            _ => vec![],
        };

        if events.is_empty() {
            println!(
                "{} No activity found in the last {}",
                style("○").dim(),
                self.since
            );
            return Ok(());
        }

        // Group by source — keep only the most-recent event per source.
        // Events are already sorted Desc so first occurrence wins.
        let mut by_source: HashMap<String, &sinex_primitives::query::QueryResultEvent> =
            HashMap::new();
        for result_event in &events {
            let key = result_event.event.source.as_str().to_string();
            by_source.entry(key).or_insert(result_event);
        }

        // Sort sources by recency of their latest event (most recent first).
        let mut sources: Vec<(&String, &&sinex_primitives::query::QueryResultEvent)> =
            by_source.iter().collect();
        sources.sort_by(|a, b| {
            let ts_a = a.1.event.ts_orig.unwrap_or(Timestamp::UNIX_EPOCH);
            let ts_b = b.1.event.ts_orig.unwrap_or(Timestamp::UNIX_EPOCH);
            ts_b.inner().cmp(&ts_a.inner())
        });

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
            .map(|(k, _)| display_source(k).len())
            .max()
            .unwrap_or(10);
        let label_width = max_source_len.max(8);

        for (source_key, result_event) in &sources {
            let label = display_source(source_key);
            let age = result_event
                .event
                .ts_orig
                .map_or_else(|| "?".to_string(), |ts| format_age(now - ts));

            let detail = build_detail(result_event);

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
            style(events.len()).bold(),
            style(by_source.len()).bold(),
            self.since,
        );

        Ok(())
    }
}

/// Strip common suffixes to produce a compact, human-readable source label.
/// "sinex-fs-ingestor" → "filesystem"
/// "sinex-terminal-ingestor" → "terminal"
/// "sinex-desktop-ingestor" → "desktop"
/// "sinex-system-ingestor" → "system"
/// Anything else: strip leading "sinex-" and trailing "-ingestor"/"-automaton".
fn display_source(source: &str) -> String {
    let friendly = match source {
        s if s.contains("fs") && s.contains("ingestor") => "filesystem",
        s if s.contains("terminal") && s.contains("ingestor") => "terminal",
        s if s.contains("desktop") && s.contains("ingestor") => "desktop",
        s if s.contains("system") && s.contains("ingestor") => "system",
        s if s.contains("document") && s.contains("ingestor") => "document",
        s if s.contains("analytics") && s.contains("automaton") => "analytics",
        s if s.contains("health") && s.contains("automaton") => "health",
        s if s.contains("terminal") && s.contains("command") => "cmd-canonical",
        _ => "",
    };

    if !friendly.is_empty() {
        return friendly.to_string();
    }

    // Generic strip
    let mut s = source;
    s = s.strip_prefix("sinex-").unwrap_or(s);
    s = s.strip_suffix("-ingestor").unwrap_or(s);
    s = s.strip_suffix("-automaton").unwrap_or(s);
    s.to_string()
}

/// Build a one-line activity description from the event, preferring a snippet
/// then falling back to well-known payload fields.
fn build_detail(result_event: &sinex_primitives::query::QueryResultEvent) -> String {
    // If the server produced a snippet, use it (already truncated server-side or here)
    if let Some(snippet) = &result_event.snippet
        && !snippet.is_empty()
    {
        return truncate(snippet, 55);
    }

    // Fallback: extract meaningful fields from the payload
    let payload = &result_event.event.payload;
    if let Some(obj) = payload.as_object() {
        // Priority order of fields to use as the summary
        for key in &[
            "command", "path", "title", "app_name", "unit", "message", "url", "name",
        ] {
            if let Some(val) = obj.get(*key).and_then(|v| v.as_str()) {
                let event_type = result_event.event.event_type.as_str();
                let label = short_event_type(event_type);
                return truncate(&format!("{label} {val}"), 55);
            }
        }
    }

    // Final fallback: just the event type
    truncate(result_event.event.event_type.as_str(), 55)
}

/// Reduce "file.created" → "created", "shell.command" → "command", etc.
fn short_event_type(event_type: &str) -> &str {
    event_type.rsplit('.').next().unwrap_or(event_type)
}

/// Format a Duration into a compact "`XmYs` ago" / "Xs ago" / "Xh ago" string.
fn format_age(d: time::Duration) -> String {
    let total_secs = d.whole_seconds().max(0) as u64;
    if total_secs < 60 {
        format!("{total_secs}s ago")
    } else if total_secs < 3600 {
        let mins = total_secs / 60;
        let secs = total_secs % 60;
        if secs == 0 {
            format!("{mins}m ago")
        } else {
            format!("{mins}m{secs}s ago")
        }
    } else {
        let hours = total_secs / 3600;
        let mins = (total_secs % 3600) / 60;
        if mins == 0 {
            format!("{hours}h ago")
        } else {
            format!("{hours}h{mins}m ago")
        }
    }
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

/// Parse "2h", "30m", "45s", "1d" into a `time::Duration`.
fn parse_duration_str(s: &str) -> Result<Duration> {
    let s = s.trim();
    if s.is_empty() {
        return Err(color_eyre::eyre::eyre!("Duration cannot be empty"));
    }

    let mut num_str = String::new();
    let mut unit = String::new();

    for ch in s.chars() {
        if ch.is_ascii_digit() {
            num_str.push(ch);
        } else {
            unit.push(ch);
        }
    }

    let num: i64 = num_str
        .parse()
        .map_err(|_| color_eyre::eyre::eyre!("Invalid duration number in: {s}"))?;

    match unit.as_str() {
        "s" | "sec" | "second" | "seconds" => Ok(Duration::seconds(num)),
        "m" | "min" | "minute" | "minutes" => Ok(Duration::minutes(num)),
        "h" | "hr" | "hour" | "hours" => Ok(Duration::hours(num)),
        "d" | "day" | "days" => Ok(Duration::days(num)),
        "w" | "week" | "weeks" => Ok(Duration::weeks(num)),
        _ => Err(color_eyre::eyre::eyre!("Unknown duration unit: {unit}")),
    }
}
