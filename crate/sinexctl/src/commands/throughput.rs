//! `sinexctl throughput` — per-source / per-component event-rate summary.
//!
//! Issue #1172 AC-8. Reads `telemetry.throughput` and prints either a table
//! (default) or JSON. The handler picks fixed 1h/24h windows on the gateway
//! side; this CLI is a thin formatter.

use clap::Args;
use color_eyre::Result;
use tabled::{builder::Builder, settings::Style};

use crate::client::GatewayClient;
use crate::fmt::format_json;
use crate::model::OutputFormat;

#[derive(Debug, Args)]
#[command(after_help = "\
EXAMPLES:
    sinexctl throughput
    sinexctl throughput -f json
")]
pub struct ThroughputCommand {}

impl ThroughputCommand {
    pub async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let response = client.telemetry_throughput().await?;

        match format {
            OutputFormat::Json | OutputFormat::Dot => {
                println!("{}", format_json(&response)?);
            }
            OutputFormat::Yaml => {
                println!("{}", crate::fmt::format_yaml(&response)?);
            }
            OutputFormat::Table => {
                print_table(&response);
            }
        }
        Ok(())
    }
}

fn print_table(response: &sinex_primitives::rpc::telemetry::TelemetryThroughputResponse) {
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
