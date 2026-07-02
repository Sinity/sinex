use clap::Subcommand;
use serde::{Deserialize, Serialize};
use sinex_primitives::domain::ModuleKind;
use sinex_primitives::rpc::coordination::InstanceHealthResponse;
use sinex_primitives::rpc::runtime::{RuntimeHeartbeatSource, RuntimeInfo};
use sinex_primitives::rpc::system::SystemHealthResponse;
use sinex_primitives::views::ViewEnvelope;

use crate::Result;
use crate::client::GatewayClient;
use crate::commands::{AutomataCommand, GatewayCommands, RuntimePresenceCommand};
use crate::fmt::{CommandOutput, format_heartbeat_age, render_envelope, with_spinner_result};
use crate::model::{OutputFormat, RuntimeModuleRole};

/// Schema version for the runtime module list view payload.
const RUNTIME_MODULE_LIST_SCHEMA_VERSION: &str = "sinex.runtime-module-list/v1";

/// Payload carried inside a [`ViewEnvelope`] for `sinexctl runtime list`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeModuleListView {
    pub schema_version: String,
    pub count: usize,
    pub modules: Vec<RuntimeInfo>,
}

impl RuntimeModuleListView {
    fn new(modules: Vec<RuntimeInfo>) -> Self {
        let count = modules.len();
        Self {
            schema_version: RUNTIME_MODULE_LIST_SCHEMA_VERSION.to_string(),
            count,
            modules,
        }
    }
}

/// Runtime module operations
#[derive(Debug, Subcommand)]
#[command(after_help = "\
EXAMPLES:
    # List all registered modules
    sinexctl runtime list

    # List only source modules
    sinexctl runtime list --role source

    # List running modules with health/staleness enrichment
    sinexctl runtime modules

    # Check status of a specific runtime module
    sinexctl runtime status terminal-source

    # Show automata runtime status
    sinexctl runtime automata

    # Check gateway reachability through the runtime surface
    sinexctl runtime gateway ping

    # Check full system health
    sinexctl runtime health

    # Drain a runtime module for maintenance
    sinexctl runtime drain terminal-source

    # Resume a drained runtime module
    sinexctl runtime resume terminal-source

    # Set horizon to replay last 24 hours
    sinexctl runtime set-horizon terminal-source 24h
")]
pub enum RuntimeCommands {
    /// List all modules
    List {
        /// Filter by role
        #[arg(long)]
        role: Option<RuntimeModuleRole>,
    },

    /// List running modules with status, health, and uptime
    Modules(RuntimePresenceCommand),

    /// Show automata runtime status
    Automata(AutomataCommand),

    /// Gateway reachability and version operations
    Gateway {
        #[command(subcommand)]
        cmd: GatewayCommands,
    },

    /// Check system health
    Health,

    /// Show runtime module status
    Status {
        /// Runtime module ID or name
        module: String,
    },

    /// Drain a runtime module for maintenance
    Drain {
        /// Runtime module ID or name
        module: String,
        /// Reason for draining
        #[arg(long, short)]
        reason: Option<String>,
    },

    /// Resume a drained runtime module
    Resume {
        /// Runtime module ID or name
        module: String,
    },

    /// Set runtime module horizon (cutoff time for event processing)
    SetHorizon {
        /// Runtime module ID or name
        module: String,

        /// Horizon timestamp (RFC3339 format or relative like "1h")
        horizon: String,
    },
}

impl RuntimeCommands {
    pub async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        match self {
            Self::List { role } => {
                let modules = client
                    .runtime_list_active(300)
                    .await?
                    .modules
                    .into_iter()
                    .filter(|module| runtime_role_matches(module.module_kind, *role))
                    .collect::<Vec<_>>();
                let envelope =
                    ViewEnvelope::new("sinexctl.runtime.list", RuntimeModuleListView::new(modules))
                        .with_query_echo(serde_json::json!({
                            "role": role,
                        }));

                if let Some(output) = render_envelope(&envelope, &envelope.payload.modules, format)?
                {
                    // Empty ndjson (zero modules) must stay empty — a blank line
                    // is not a valid NDJSON record (Codex review, PR #1766).
                    print!("{output}");
                    if !output.is_empty() && !output.ends_with('\n') {
                        println!();
                    }
                    return Ok(());
                }
                // OutputFormat::Table — fall through to human rendering
                if envelope.payload.modules.is_empty() {
                    println!("No modules found.");
                } else {
                    println!("{}", format_runtime_presence_table(&envelope.payload.modules));
                }
            }
            Self::Modules(cmd) => {
                cmd.execute(client, format).await?;
            }
            Self::Automata(cmd) => {
                cmd.execute(client, format).await?;
            }
            Self::Gateway { cmd } => {
                cmd.execute(client, format).await?;
            }
            Self::Health => {
                let health = client.health().await?;
                CommandOutput::single(health, format_health_table).display(&format)?;
            }
            Self::Status { module } => {
                let response = client.runtime_status(module).await?;
                CommandOutput::single(response, format_runtime_status_table).display(&format)?;
            }
            Self::Drain { module, reason } => {
                let response = with_spinner_result(
                    format!("Draining runtime module {module}..."),
                    format!("Runtime module {module} drained"),
                    client.drain_runtime(module, reason.as_deref()),
                )
                .await?;
                println!("Operation ID: {}", response.operation_id);
            }
            Self::Resume { module } => {
                let response = with_spinner_result(
                    format!("Resuming runtime module {module}..."),
                    format!("Runtime module {module} resumed"),
                    client.resume_runtime(module),
                )
                .await?;
                println!("Operation ID: {}", response.operation_id);
            }
            Self::SetHorizon { module, horizon } => {
                let response = with_spinner_result(
                    format!("Setting horizon for {module}..."),
                    format!("Runtime module {module} horizon set to {horizon}"),
                    client.set_runtime_horizon(module, horizon),
                )
                .await?;
                println!("Operation ID: {}", response.operation_id);
            }
        }
        Ok(())
    }
}

fn runtime_role_matches(kind: ModuleKind, role: Option<RuntimeModuleRole>) -> bool {
    match role {
        None => true,
        Some(RuntimeModuleRole::Capture) => kind == ModuleKind::Source,
        Some(RuntimeModuleRole::Derived) => kind == ModuleKind::Automaton,
        Some(RuntimeModuleRole::Core | RuntimeModuleRole::Gateway) => kind == ModuleKind::Service,
    }
}

fn format_runtime_presence_table(modules: &[RuntimeInfo]) -> String {
    let mut output = String::new();
    output.push_str("Runtime Modules:\n");
    for module in modules {
        let name = module
            .service_name
            .as_deref()
            .or(module.instance_id.as_deref())
            .unwrap_or_else(|| module.module_name.as_str());
        let last_seen = module
            .last_heartbeat_at
            .as_ref()
            .map_or_else(|| "never".to_string(), format_heartbeat_age);
        let host = module.host.as_deref().unwrap_or("-");
        output.push_str(&format!(
            "  {} ({}) - {} - last seen {} - host {} - {}\n",
            name,
            module.module_kind,
            module.status,
            last_seen,
            host,
            runtime_heartbeat_source_name(module.heartbeat_source)
        ));
    }
    output
}

fn runtime_heartbeat_source_name(source: RuntimeHeartbeatSource) -> &'static str {
    match source {
        RuntimeHeartbeatSource::Run => "run",
        RuntimeHeartbeatSource::Manifest => "manifest",
        RuntimeHeartbeatSource::Output => "output",
    }
}

/// Format system health as table
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
        "  Raw Ingest DLQ: {} (connected: {})\n",
        health.components.raw_ingest_dlq.status, health.components.raw_ingest_dlq.connected
    ));
    if let Some(ref detail) = health.components.raw_ingest_dlq.detail {
        output.push_str(&format!("    Detail: {detail}\n"));
    }
    output.push_str(&format!(
        "  Confirmation Buffer: {} (connected: {})\n",
        health.components.confirmation_buffer.status,
        health.components.confirmation_buffer.connected
    ));
    if let Some(ref detail) = health.components.confirmation_buffer.detail {
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
    output.push_str(&format!(
        "  SSE Confirmations: {} (connected: {})\n",
        health.components.sse_confirmation.status, health.components.sse_confirmation.connected
    ));
    if let Some(ref detail) = health.components.sse_confirmation.detail {
        output.push_str(&format!("    Detail: {detail}\n"));
    }

    output
}

/// Format runtime module status as table
fn format_runtime_status_table(response: &InstanceHealthResponse) -> String {
    let mut output = String::new();
    output.push_str("Runtime Module Status:\n");
    output.push_str(&format!(
        "  Instance ID: {}\n",
        response.instance.instance_id
    ));
    output.push_str(&format!("  Type: {}\n", response.instance.module_kind));
    if let Some(ref hostname) = response.instance.hostname {
        output.push_str(&format!("  Hostname: {hostname}\n"));
    }
    output.push_str(&format!(
        "  Status: {}\n",
        if response.healthy {
            "✓ Healthy"
        } else {
            "✗ Unhealthy"
        }
    ));
    if let Some(ref heartbeat) = response.instance.last_heartbeat {
        output.push_str(&format!("  Last Heartbeat: {heartbeat}\n"));
    }
    output.push_str(&format!(
        "  Leader: {}\n",
        if response.instance.is_leader {
            "Yes"
        } else {
            "No"
        }
    ));
    if let Some(ref err) = response.last_error {
        output.push_str(&format!("  Last Error: {err}\n"));
    }
    output
}

#[cfg(test)]
#[path = "runtime_test.rs"]
mod tests;
