//! Event timeline listing (#1025).

use crate::Result;
use crate::fmt::render_finite_envelope;
use crate::model::OutputFormat;
use clap::Args;
use console::{Term, style};
use serde_json::json;
use sinex_primitives::views::{EventCardListView, ViewEnvelope};

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

        let timeline = client.event_cards(query).await?;

        if let Some(output) = render_timeline_machine_output(
            &timeline,
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

        render_table(&timeline)?;
        Ok(())
    }
}

fn render_timeline_machine_output(
    timeline: &EventCardListView,
    limit: i64,
    source: Option<&str>,
    event_type: Option<&str>,
    format: OutputFormat,
) -> Result<Option<String>> {
    let envelope =
        ViewEnvelope::new("sinexctl.events.timeline", timeline.clone()).with_query_echo(json!({
            "limit": limit,
            "source": source,
            "event_type": event_type,
        }));
    render_finite_envelope(&envelope, format)
}

fn render_table(timeline: &EventCardListView) -> Result<()> {
    let term = Term::stdout();
    term.write_line(&format!(
        "{}  {} events",
        style("Timeline").bold().underlined(),
        style(timeline.count.to_string()).cyan(),
    ))?;

    for card in &timeline.cards {
        let ts = card
            .timestamp
            .original
            .map_or_else(|| "?".into(), |t| t.to_string());
        term.write_line(&format!(
            "{}  {:<20} {:<30} {}",
            style(ts.chars().take(19).collect::<String>()).dim(),
            style(card.source.raw.as_str()).yellow(),
            style(card.event_type.as_str()).green(),
            card.summary,
        ))?;
    }

    Ok(())
}

#[cfg(test)]
#[path = "timeline_test.rs"]
mod tests;
