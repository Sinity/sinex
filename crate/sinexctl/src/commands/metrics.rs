use clap::Subcommand;

use crate::Result;
use crate::client::GatewayClient;
use crate::commands::{ReportCommands, TelemetryCommands, ThroughputCommand};
use crate::model::OutputFormat;

/// Metrics, telemetry, and activity-report read surfaces.
#[derive(Debug, Subcommand)]
#[command(after_help = "\
EXAMPLES:
    # Per-source and per-component event throughput
    sinexctl metrics throughput

    # Operator telemetry views
    sinexctl metrics telemetry current-health
    sinexctl metrics telemetry gateway-stats --from 24h

    # Daily activity reports
    sinexctl metrics report today
")]
pub enum MetricsCommands {
    /// Per-source / per-component event throughput (#1172 AC-8)
    Throughput(ThroughputCommand),

    /// Telemetry data from event-time activity views and operator read models
    Telemetry {
        #[command(subcommand)]
        cmd: TelemetryCommands,
    },

    /// Daily activity reports (today, yesterday)
    Report {
        #[command(subcommand)]
        cmd: ReportCommands,
    },
}

impl MetricsCommands {
    pub async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        match self {
            Self::Throughput(cmd) => cmd.execute(client, format).await,
            Self::Telemetry { cmd } => cmd.execute(client, format).await,
            Self::Report { cmd } => cmd.execute(client, format).await,
        }
    }

    pub fn command_path(&self) -> &'static str {
        match self {
            Self::Throughput(_) => "metrics throughput",
            Self::Telemetry { cmd } => telemetry_command_path(cmd),
            Self::Report { cmd } => report_command_path(cmd),
        }
    }
}

fn telemetry_command_path(cmd: &TelemetryCommands) -> &'static str {
    match cmd {
        TelemetryCommands::CurrentHealth { .. } => "metrics telemetry current-health",
        TelemetryCommands::CurrentDeviceState { .. } => "metrics telemetry current-device-state",
        TelemetryCommands::WindowFocus { .. } => "metrics telemetry window-focus",
        TelemetryCommands::CommandFrequency { .. } => "metrics telemetry command-frequency",
        TelemetryCommands::FileActivity { .. } => "metrics telemetry file-activity",
        TelemetryCommands::RecentActivity { .. } => "metrics telemetry recent-activity",
        TelemetryCommands::SystemState { .. } => "metrics telemetry system-state",
        TelemetryCommands::GatewayStats { .. } => "metrics telemetry gateway-stats",
        TelemetryCommands::StreamStats { .. } => "metrics telemetry stream-stats",
        TelemetryCommands::AssemblyStats { .. } => "metrics telemetry assembly-stats",
        TelemetryCommands::SourceStats { .. } => "metrics telemetry source-stats",
        TelemetryCommands::MetricCounters { .. } => "metrics telemetry metric-counters",
        TelemetryCommands::EventEngineBatchStats { .. } => {
            "metrics telemetry event-engine-batch-stats"
        }
        TelemetryCommands::EventEngineValidation => "metrics telemetry event-engine-validation",
    }
}

fn report_command_path(cmd: &ReportCommands) -> &'static str {
    match cmd {
        ReportCommands::Today => "metrics report today",
        ReportCommands::Yesterday => "metrics report yesterday",
        ReportCommands::Calendar(_) => "metrics report calendar",
    }
}
