use clap::Args;
use console::style;
use sinex_primitives::domain::HealthStatus;
use sinex_primitives::rpc::source_status::{SourceStatus, SourcesStatusResponse};
use tabled::{builder::Builder, settings::Style};

use crate::Result;
use crate::client::GatewayClient;
use crate::fmt::{CommandOutput, format_heartbeat_age};
use crate::model::OutputFormat;

/// Show source runtime status (run, health, recent emissions).
///
/// Sibling to `sinexctl automata`. Reads `health.status` events emitted by the
/// runtime's `HealthReporter` on each status transition, joined with the per-run
/// last-heartbeat timestamp from `core.runs`.
#[derive(Debug, Args)]
#[command(after_help = "\
EXAMPLES:
    # Show all source status
    sinexctl sources status

    # Emit machine-readable status
    sinexctl sources status --format json
")]
pub struct SourceStatusCommand {
    /// Heartbeat age threshold for considering a source live (seconds).
    #[arg(long, default_value_t = 300)]
    stale_after_secs: u64,

    /// Recent emissions window in seconds.
    #[arg(long, default_value_t = 300)]
    recent_window_secs: u64,
}

impl SourceStatusCommand {
    pub async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let response = client
            .sources_status(self.stale_after_secs, self.recent_window_secs)
            .await?;
        CommandOutput::single(response, format_sources_status_table).display(&format)?;
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

fn format_health(status: &SourceStatus) -> String {
    match status.current_health {
        Some(HealthStatus::Healthy) => style("healthy").green().to_string(),
        Some(HealthStatus::Degraded) => style("degraded").yellow().to_string(),
        Some(HealthStatus::Unhealthy) => style("unhealthy").red().to_string(),
        Some(HealthStatus::Unknown) => style("unknown").dim().to_string(),
        None => style("-").dim().to_string(),
    }
}

fn format_sources_status_table(response: &SourcesStatusResponse) -> String {
    if response.sources.is_empty() {
        return "No sources registered.".to_string();
    }

    let mut builder = Builder::new();
    builder.push_record([
        "SOURCE",
        "LIVE",
        "RUN",
        "HEALTH",
        "HEARTBEAT",
        "RECENT EVENTS",
        "LAST EMITTED",
        "HEALTH CHANGED",
    ]);

    for source in &response.sources {
        let live = if source.live {
            style("yes").green().to_string()
        } else {
            style("no").red().to_string()
        };
        let run = source
            .module_run_id
            .as_ref()
            .map_or_else(|| style("-").dim().to_string(), short_uuid);

        builder.push_record([
            source.module_name.to_string(),
            live,
            run,
            format_health(source),
            format_optional_age(source.last_heartbeat_at.as_ref()),
            source.recent_output_count.to_string(),
            format_optional_age(source.last_output_at.as_ref()),
            format_optional_age(source.health_changed_at.as_ref()),
        ]);
    }

    let mut table = builder.build();
    table.with(Style::rounded());
    table.to_string()
}
