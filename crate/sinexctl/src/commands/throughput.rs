//! `sinexctl metrics throughput` — per-source / per-component event-rate summary.
//!
//! Issue #1172 AC-8. Reads `telemetry.throughput` and prints either a table
//! (default) or JSON. The handler picks fixed 1h/24h windows on the gateway
//! side; this CLI is a thin formatter.

use clap::Args;
use color_eyre::Result;
use sinex_primitives::rpc::telemetry::TelemetryThroughputResponse;
use sinex_primitives::views::{
    CaveatView, ReadinessCaveatId, SinexObjectKind, SinexObjectRef, ViewEnvelope,
};
use tabled::{builder::Builder, settings::Style};

use crate::client::GatewayClient;
use crate::fmt::print_finite_envelope;
use crate::model::OutputFormat;

#[derive(Debug, Args)]
#[command(after_help = "\
EXAMPLES:
    sinexctl metrics throughput
    sinexctl metrics throughput -f json
")]
pub struct ThroughputCommand {}

impl ThroughputCommand {
    pub async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let response = client.telemetry_throughput().await?;

        let envelope = throughput_envelope(response.clone());
        if print_finite_envelope(&envelope, format)? {
            return Ok(());
        }
        print_table(&response);
        Ok(())
    }
}

fn throughput_envelope(
    response: TelemetryThroughputResponse,
) -> ViewEnvelope<TelemetryThroughputResponse> {
    let mut envelope = ViewEnvelope::new("sinexctl.metrics.throughput", response);
    envelope.caveats = throughput_caveats(&envelope.payload);
    envelope
}

fn throughput_caveats(response: &TelemetryThroughputResponse) -> Vec<CaveatView> {
    let mut caveats = Vec::new();
    if response.per_source.is_empty() {
        caveats.push(throughput_caveat(
            ReadinessCaveatId::CoverageUnmeasurable,
            "throughput read model has no per-source rows for the fixed 24h window; this is not proof that no sources are configured",
            "throughput.per_source.empty",
        ));
    }
    if response.per_component.is_empty() {
        caveats.push(throughput_caveat(
            ReadinessCaveatId::CoverageUnmeasurable,
            "throughput read model has no per-component rows; component event-rate coverage is unmeasurable",
            "throughput.per_component.empty",
        ));
    }
    if !response.per_source.is_empty()
        && response
            .per_source
            .iter()
            .all(|entry| entry.events_last_1h == 0 && entry.events_last_24h == 0)
    {
        caveats.push(throughput_caveat(
            ReadinessCaveatId::WindowPartial,
            "all reported sources have zero events in the fixed 1h/24h throughput windows; live capture may be idle, stopped, or outside the window",
            "throughput.per_source.zero",
        ));
    }
    caveats
}

fn throughput_caveat(
    id: ReadinessCaveatId,
    message: impl Into<String>,
    ref_id: &str,
) -> CaveatView {
    CaveatView {
        id: id.as_str().to_string(),
        message: message.into(),
        ref_: Some(
            SinexObjectRef::new(SinexObjectKind::Projection, ref_id)
                .with_label(ref_id)
                .with_command_hint("sinexctl metrics throughput")
                .with_rpc_method("telemetry.throughput"),
        ),
    }
}

fn print_table(response: &TelemetryThroughputResponse) {
    println!();
    println!("Per-source throughput (events/sec)");
    let mut builder = Builder::default();
    builder.push_record(["Source", "Events 1h", "Events 24h", "EPS 1h", "EPS 24h"]);
    if response.per_source.is_empty() {
        builder.push_record(["(no events in last 24h)", "0", "0", "0.000", "0.000"]);
    } else {
        for entry in &response.per_source {
            builder.push_record([
                entry.source.as_str(),
                &entry.events_last_1h.to_string(),
                &entry.events_last_24h.to_string(),
                &format!("{:.3}", entry.eps_1h),
                &format!("{:.3}", entry.eps_24h),
            ]);
        }
    }
    let mut table = builder.build();
    table.with(Style::modern());
    println!("{table}");
    println!();

    println!("Per-component aggregate (events/sec)");
    let mut builder = Builder::default();
    builder.push_record(["Component", "EPS 1h", "EPS 24h"]);
    for entry in &response.per_component {
        builder.push_record([
            entry.component.as_str(),
            &format!("{:.3}", entry.eps_1h),
            &format!("{:.3}", entry.eps_24h),
        ]);
    }
    let mut table = builder.build();
    table.with(Style::modern());
    println!("{table}");
    println!();
}

#[cfg(test)]
#[path = "throughput_test.rs"]
mod tests;
