use clap::Args;
use console::style;
use sinex_primitives::rpc::automata::{AutomataStatusResponse, AutomatonStatus};
use tabled::{builder::Builder, settings::Style};

use crate::Result;
use crate::client::GatewayClient;
use crate::fmt::{CommandOutput, format_heartbeat_age};
use crate::model::OutputFormat;

/// Show derived-node/automata runtime status
#[derive(Debug, Args)]
#[command(after_help = "\
EXAMPLES:
    # Show automata status
    sinexctl automata

    # Emit machine-readable status
    sinexctl automata --format json
")]
pub struct AutomataCommand {
    /// Heartbeat age threshold for considering an automaton live
    #[arg(long, default_value_t = 300)]
    stale_after_secs: u64,

    /// Recent output window in seconds
    #[arg(long, default_value_t = 300)]
    recent_window_secs: u64,

    /// Output format
    #[arg(long, short = 'f', value_enum, default_value = "table")]
    format: OutputFormat,
}

impl AutomataCommand {
    pub async fn execute(&self, client: &GatewayClient) -> Result<()> {
        let response = client
            .automata_status(self.stale_after_secs, self.recent_window_secs)
            .await?;
        CommandOutput::single(response, format_automata_status_table).display(&self.format)?;
        Ok(())
    }
}

fn format_optional_count(value: Option<i64>) -> String {
    value.map_or_else(|| style("-").dim().to_string(), |value| value.to_string())
}

fn format_optional_rate(value: Option<f64>) -> String {
    value.map_or_else(
        || style("-").dim().to_string(),
        |value| format!("{:.1}%", value * 100.0),
    )
}

fn format_optional_age(value: Option<&sinex_primitives::Timestamp>) -> String {
    value.map_or_else(|| style("-").dim().to_string(), format_heartbeat_age)
}

fn short_uuid(value: &sinex_primitives::Uuid) -> String {
    let value = value.to_string();
    format!("{}...", &value[..8])
}

fn checkpoint_summary(status: &AutomatonStatus) -> String {
    match (&status.checkpoint_kind, &status.checkpoint_position) {
        (Some(kind), Some(position)) => format!("{kind}:{position}"),
        (Some(kind), None) => kind.clone(),
        _ => style("-").dim().to_string(),
    }
}

fn format_automata_status_table(response: &AutomataStatusResponse) -> String {
    if response.automata.is_empty() {
        return "No automata registered.".to_string();
    }

    let mut builder = Builder::new();
    builder.push_record([
        "NODE",
        "LIVE",
        "RUN",
        "PROCESSED",
        "ERR 5M",
        "PENDING",
        "CHECKPOINT",
        "LAST OUTPUT",
        "LAST REPLAY",
    ]);

    for automaton in &response.automata {
        let live = if automaton.live {
            style("yes").green().to_string()
        } else {
            style("no").red().to_string()
        };
        let run = automaton
            .node_run_id
            .as_ref()
            .map_or_else(|| style("-").dim().to_string(), short_uuid);

        builder.push_record([
            automaton.node_name.to_string(),
            live,
            run,
            format_optional_count(automaton.events_processed_current_run),
            format_optional_rate(automaton.error_rate_5m),
            format_optional_count(automaton.pending_invalidation_count),
            checkpoint_summary(automaton),
            format_optional_age(automaton.last_output_at.as_ref()),
            format_optional_age(automaton.last_replay_at.as_ref()),
        ]);
    }

    let mut table = builder.build();
    table.with(Style::rounded());
    table.to_string()
}
