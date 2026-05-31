//! Event timeline listing (#1025).

use crate::Result;
use clap::Args;
use console::{Term, style};

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
    pub async fn execute(&self, client: &crate::client::gateway::GatewayClient) -> Result<()> {
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

        let term = Term::stdout();
        term.write_line(&format!(
            "{}  {} events",
            style("Timeline").bold().underlined(),
            style(events.len().to_string()).cyan(),
        ))?;

        for event in &events {
            let ts = event
                .event
                .ts_orig.map_or_else(|| "?".into(), |t| t.to_string());
            let source = event.event.source.as_str();
            let etype = event.event.event_type.as_str();
            let summary = event
                .snippet
                .as_deref()
                .unwrap_or("")
                .chars()
                .take(80)
                .collect::<String>();
            term.write_line(&format!(
                "{}  {:<20} {:<30} {}",
                style(ts.chars().take(19).collect::<String>()).dim(),
                style(source).yellow(),
                style(etype).green(),
                summary,
            ))?;
        }

        Ok(())
    }
}
