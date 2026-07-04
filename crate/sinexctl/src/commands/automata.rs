use clap::Args;
use console::style;
use sinex_primitives::rpc::automata::{AutomataStatusResponse, AutomatonStatus};
use sinex_primitives::views::{
    CaveatView, ReadinessCaveatId, SinexObjectKind, SinexObjectRef, ViewEnvelope,
};
use tabled::{builder::Builder, settings::Style};

use crate::Result;
use crate::client::GatewayClient;
use crate::fmt::{format_heartbeat_age, print_finite_envelope};
use crate::model::OutputFormat;

/// Show automata runtime status
#[derive(Debug, Args)]
#[command(after_help = "\
EXAMPLES:
    # Show automata status
    sinexctl runtime automata

    # Emit machine-readable status
    sinexctl runtime automata --format json
")]
pub struct AutomataCommand {
    /// Heartbeat age threshold for considering an automaton live
    #[arg(long, default_value_t = 300)]
    stale_after_secs: u64,

    /// Recent output window in seconds
    #[arg(long, default_value_t = 300)]
    recent_window_secs: u64,
}

impl AutomataCommand {
    pub async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let response = client
            .automata_status(self.stale_after_secs, self.recent_window_secs)
            .await?;
        let envelope = automata_status_envelope(response);
        if !print_finite_envelope(&envelope, format)? {
            println!("{}", format_automata_status_table(&envelope.payload));
        }
        Ok(())
    }
}

fn automata_status_envelope(
    response: AutomataStatusResponse,
) -> ViewEnvelope<AutomataStatusResponse> {
    let mut envelope = ViewEnvelope::new("sinexctl.runtime.automata", response);
    envelope.caveats = automata_status_caveats(&envelope.payload);
    envelope
}

fn automata_status_caveats(response: &AutomataStatusResponse) -> Vec<CaveatView> {
    let mut caveats = Vec::new();

    if response.automata.is_empty() {
        caveats.push(CaveatView {
            id: ReadinessCaveatId::SourceAbsent.as_str().to_string(),
            message: "no automata are registered in the runtime status response".to_string(),
            ref_: Some(automata_ref("automata.registry")),
        });
        return caveats;
    }

    let inactive = response
        .automata
        .iter()
        .filter(|automaton| !automaton.live)
        .take(5)
        .collect::<Vec<_>>();
    if !inactive.is_empty() {
        let names = inactive
            .iter()
            .map(|automaton| automaton.module_name.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        caveats.push(CaveatView {
            id: ReadinessCaveatId::WindowPartial.as_str().to_string(),
            message: format!(
                "{} automata are not live under stale_after_secs={}: {names}",
                response.automata.iter().filter(|automaton| !automaton.live).count(),
                response.stale_after_secs
            ),
            ref_: Some(automata_ref("automata.live")),
        });
    }

    let missing_recent_output = response
        .automata
        .iter()
        .filter(|automaton| automaton.recent_output_count == 0)
        .take(5)
        .collect::<Vec<_>>();
    if !missing_recent_output.is_empty() {
        let names = missing_recent_output
            .iter()
            .map(|automaton| automaton.module_name.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        caveats.push(CaveatView {
            id: ReadinessCaveatId::CoverageUnmeasurable.as_str().to_string(),
            message: format!(
                "{} automata have no outputs in recent_window_secs={}: {names}",
                response
                    .automata
                    .iter()
                    .filter(|automaton| automaton.recent_output_count == 0)
                    .count(),
                response.recent_window_secs
            ),
            ref_: Some(automata_ref("automata.recent_output")),
        });
    }

    caveats
}

fn automata_ref(id: &str) -> SinexObjectRef {
    SinexObjectRef::new(SinexObjectKind::RuntimeModule, id)
        .with_label(id)
        .with_command_hint("sinexctl runtime automata")
        .with_rpc_method("automata.status")
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

fn format_optional_ms(value: Option<f64>) -> String {
    value.map_or_else(
        || style("-").dim().to_string(),
        |value| format!("{value:.0}"),
    )
}

fn format_optional_eps(value: Option<f64>) -> String {
    value.map_or_else(
        || style("-").dim().to_string(),
        |value| format!("{value:.2}"),
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
        "LAG p50",
        "LAG p99",
        "TICK p99",
        "EPS",
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
            .module_run_id
            .as_ref()
            .map_or_else(|| style("-").dim().to_string(), short_uuid);

        builder.push_record([
            automaton.module_name.to_string(),
            live,
            run,
            format_optional_count(automaton.events_processed_current_run),
            format_optional_rate(automaton.error_rate_5m),
            format_optional_ms(automaton.event_lag_p50_ms),
            format_optional_ms(automaton.event_lag_p99_ms),
            format_optional_ms(automaton.tick_runtime_p99_ms),
            format_optional_eps(automaton.throughput_eps),
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

#[cfg(test)]
#[path = "automata_test.rs"]
mod tests;
