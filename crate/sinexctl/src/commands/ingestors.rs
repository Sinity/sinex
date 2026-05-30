use clap::Args;
use console::style;
use sinex_primitives::rpc::ingestors::{IngestorStatus, IngestorsStatusResponse};
use tabled::{builder::Builder, settings::Style};

use crate::Result;
use crate::client::GatewayClient;
use crate::fmt::{CommandOutput, format_heartbeat_age};
use crate::model::OutputFormat;

/// Show ingestor runtime status (run, health, recent emissions).
///
/// Sibling to `sinexctl automata`. Reads `health.status` events emitted by the
/// SDK's `HealthReporter` on each status transition, joined with the per-run
/// last-heartbeat timestamp from `core.runs`.
#[derive(Debug, Args)]
#[command(after_help = "\
EXAMPLES:
    # Show all ingestor status
    sinexctl ingestors

    # Emit machine-readable status
    sinexctl ingestors --format json
")]
pub struct IngestorsCommand {
    /// Heartbeat age threshold for considering an ingestor live (seconds).
    #[arg(long, default_value_t = 300)]
    stale_after_secs: u64,

    /// Recent emissions window in seconds.
    #[arg(long, default_value_t = 300)]
    recent_window_secs: u64,
}

impl IngestorsCommand {
    pub async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let response = client
            .ingestors_status(self.stale_after_secs, self.recent_window_secs)
            .await?;
        CommandOutput::single(response, format_ingestors_status_table).display(&format)?;
        Ok(())
    }
}

fn format_optional_age(value: Option<&sinex_primitives::Timestamp>) -> String {
    value.map_or_else(|| style("-").dim().to_string(), format_heartbeat_age)
}

fn short_uuid(value: &sinex_primitives::Uuid) -> String {
    let value = value.to_string();
    format!("{}...", &value[..8])
}

fn format_health(status: &IngestorStatus) -> String {
    // TODO(#1576): unify with HealthStatus — `current_health: Option<String>` is a
    // re-serialized `HealthStatus` that the CLI re-discriminates by literal below.
    // After the health-enum unification (issue item 2), this should branch on
    // `Option<HealthStatus>` directly.
    match status.current_health.as_deref() {
        Some("healthy") => style("healthy").green().to_string(),
        Some("degraded") => style("degraded").yellow().to_string(),
        Some("failed") => style("failed").red().to_string(),
        Some(other) => other.to_string(),
        None => style("-").dim().to_string(),
    }
}

fn format_ingestors_status_table(response: &IngestorsStatusResponse) -> String {
    if response.ingestors.is_empty() {
        return "No ingestors registered.".to_string();
    }

    let mut builder = Builder::new();
    builder.push_record([
        "NODE",
        "LIVE",
        "RUN",
        "HEALTH",
        "HEARTBEAT",
        "RECENT EVENTS",
        "LAST EMITTED",
        "HEALTH CHANGED",
    ]);

    for ing in &response.ingestors {
        let live = if ing.live {
            style("yes").green().to_string()
        } else {
            style("no").red().to_string()
        };
        let run = ing
            .source_run_id
            .as_ref()
            .map_or_else(|| style("-").dim().to_string(), short_uuid);

        builder.push_record([
            ing.node_name.to_string(),
            live,
            run,
            format_health(ing),
            format_optional_age(ing.last_heartbeat_at.as_ref()),
            ing.recent_output_count.to_string(),
            format_optional_age(ing.last_output_at.as_ref()),
            format_optional_age(ing.health_changed_at.as_ref()),
        ]);
    }

    let mut table = builder.build();
    table.with(Style::rounded());
    table.to_string()
}
