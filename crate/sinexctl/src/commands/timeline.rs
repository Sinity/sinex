//! Event timeline listing (#1025).

use crate::Result;
use crate::fmt::render_finite_envelope;
use crate::model::OutputFormat;
use clap::Args;
use console::{Term, style};
use serde_json::json;
use sinex_primitives::query::QueryResultEvent;
use sinex_primitives::views::{EventCardListView, EventCardView, ViewEnvelope};

/// List recent events as a timeline with source and type columns.
#[derive(Debug, Args)]
pub struct TimelineCommand {
    /// Maximum events to display (default 100)
    #[arg(long, default_value = "100")]
    limit: i64,

    /// Filter to events from this source (e.g. "terminal")
    #[arg(long)]
    source: Option<String>,

    /// Filter to events of this type (e.g. "command.executed")
    #[arg(long)]
    event_type: Option<String>,
}

impl TimelineCommand {
    pub async fn execute(
        &self,
        client: &crate::client::gateway::GatewayClient,
        format: OutputFormat,
    ) -> Result<()> {
        use sinex_primitives::domain::{EventSource, EventType};
        use sinex_primitives::query::EventQuery;

        let mut query = EventQuery::default();
        query.limit = self.limit;
        if let Some(ref s) = self.source {
            query.sources =
                vec![EventSource::new(s.clone()).map_err(|e| color_eyre::eyre::eyre!("{}", e))?];
        }
        if let Some(ref t) = self.event_type {
            query.event_types =
                vec![EventType::new(t.clone()).map_err(|e| color_eyre::eyre::eyre!("{}", e))?];
        }
        query.validate()?;

        let result = client.query_events(query).await?;
        let sinex_primitives::query::EventQueryResult::Events { events, .. } = result else {
            println!("Query returned non-list result");
            return Ok(());
        };

        if let Some(output) = render_timeline_machine_output(
            &events,
            self.limit,
            self.source.as_deref(),
            self.event_type.as_deref(),
            format,
        )? {
            print!("{output}");
            if !output.is_empty() && !output.ends_with('\n') {
                println!();
            }
            return Ok(());
        }

        render_table(&events)?;
        Ok(())
    }
}

fn render_timeline_machine_output(
    events: &[QueryResultEvent],
    limit: i64,
    source: Option<&str>,
    event_type: Option<&str>,
    format: OutputFormat,
) -> Result<Option<String>> {
    let timeline = EventCardListView::from_query_events(events);
    let envelope = ViewEnvelope::new("sinexctl.events.timeline", timeline).with_query_echo(json!({
        "limit": limit,
        "source": source,
        "event_type": event_type,
    }));
    render_finite_envelope(&envelope, format)
}

fn render_table(events: &[QueryResultEvent]) -> Result<()> {
    let term = Term::stdout();
    term.write_line(&format!(
        "{}  {} events",
        style("Timeline").bold().underlined(),
        style(events.len().to_string()).cyan(),
    ))?;

    for event in events {
        let card = EventCardView::from_query_event(event);
        let ts = card
            .timestamp
            .original
            .map_or_else(|| "?".into(), |t| t.to_string());
        term.write_line(&format!(
            "{}  {:<20} {:<30} {}",
            style(ts.chars().take(19).collect::<String>()).dim(),
            style(card.source.raw).yellow(),
            style(card.event_type).green(),
            card.summary,
        ))?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use xtask::sandbox::prelude::sinex_test;

    #[sinex_test]
    async fn timeline_machine_output_uses_view_envelope_json() -> xtask::sandbox::TestResult<()> {
        let output =
            render_timeline_machine_output(&[], 25, Some("shell.atuin"), None, OutputFormat::Json)?
                .expect("json should render");
        let value: serde_json::Value = serde_json::from_str(&output)?;

        assert_eq!(value["source_surface"], "sinexctl.events.timeline");
        assert_eq!(value["payload"]["count"], 0);
        assert_eq!(value["query_echo"]["limit"], 25);
        assert_eq!(value["query_echo"]["source"], "shell.atuin");
        Ok(())
    }

    #[sinex_test]
    async fn timeline_machine_output_rejects_ndjson() -> xtask::sandbox::TestResult<()> {
        let result = render_timeline_machine_output(&[], 100, None, None, OutputFormat::Ndjson);
        assert!(result.is_err(), "timeline is a finite view");
        Ok(())
    }
}
