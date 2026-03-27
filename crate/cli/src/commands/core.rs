use clap::Subcommand;
use sinex_primitives::rpc::system::SystemHealthResponse;

use crate::Result;
use crate::client::GatewayClient;
use crate::fmt::CommandOutput;
use crate::model::OutputFormat;

/// Core system operations
#[derive(Debug, Subcommand)]
pub enum CoreCommands {
    /// Check system health
    Health,
}

impl CoreCommands {
    pub async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        match self {
            Self::Health => {
                let health = client.health().await?;
                CommandOutput::single(health, format_health_table).display(&format)?;
            }
        }
        Ok(())
    }
}

/// Format health response as table
fn format_health_table(health: &SystemHealthResponse) -> String {
    use sinex_primitives::domain::HealthStatus;
    let status_icon = match health.status {
        HealthStatus::Healthy => "✓",
        HealthStatus::Degraded => "⚠",
        _ => "✗",
    };

    let mut output = String::new();
    output.push_str(&format!(
        "System Health: {} {} (healthy: {}, serving: {})\n",
        status_icon, health.status, health.healthy, health.serving
    ));
    if !health.degradation_reasons.is_empty() {
        output.push_str("Degradation Reasons:\n");
        for reason in &health.degradation_reasons {
            output.push_str(&format!("  - {reason}\n"));
        }
    }
    output.push('\n');
    output.push_str("Components:\n");
    output.push_str(&format!(
        "  Database: {} (connected: {})\n",
        health.components.database.status, health.components.database.connected
    ));
    output.push_str(&format!(
        "  NATS: {} (connected: {})\n",
        health.components.nats.status, health.components.nats.connected
    ));
    if let Some(latency_ms) = health.components.nats.latency_ms {
        output.push_str(&format!("    Latency: {latency_ms:.2} ms\n"));
    }
    if let Some(ref detail) = health.components.nats.detail {
        output.push_str(&format!("    Detail: {detail}\n"));
    }
    output.push_str(&format!(
        "  Replay Control: {} (enabled: {}, connected: {})\n",
        health.components.replay_control.status,
        health.components.replay_control.enabled,
        health.components.replay_control.connected
    ));
    if let Some(ref err) = health.components.replay_control.last_error {
        output.push_str(&format!("    Last error: {err}\n"));
    }

    output
}
