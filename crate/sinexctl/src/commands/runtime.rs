use clap::Subcommand;
use sinex_primitives::rpc::coordination::InstanceHealthResponse;

use crate::Result;
use crate::client::GatewayClient;
use crate::fmt::{CommandOutput, format_table_runtime, with_spinner_result};
use crate::model::{RuntimeModuleRole, OutputFormat};

/// RuntimeActor operations
#[derive(Debug, Subcommand)]
#[command(after_help = "\
EXAMPLES:
    # List all registered modules
    sinexctl node list

    # List only ingestor modules
    sinexctl node list --role ingestor

    # Check status of a specific node
    sinexctl node status terminal-node

    # Drain a node for maintenance
    sinexctl node drain terminal-node

    # Resume a drained node
    sinexctl node resume terminal-node

    # Set horizon to replay last 24 hours
    sinexctl node set-horizon terminal-node 24h
")]
pub enum RuntimeCommands {
    /// List all modules
    List {
        /// Filter by role
        #[arg(long)]
        role: Option<RuntimeModuleRole>,
    },

    /// Show node status
    Status {
        /// RuntimeActor ID or name
        node: String,
    },

    /// Drain a node for maintenance
    Drain {
        /// RuntimeActor ID or name
        node: String,
        /// Reason for draining
        #[arg(long, short)]
        reason: Option<String>,
    },

    /// Resume a drained node
    Resume {
        /// RuntimeActor ID or name
        node: String,
    },

    /// Set node horizon (cutoff time for event processing)
    SetHorizon {
        /// RuntimeActor ID or name
        node: String,

        /// Horizon timestamp (RFC3339 format or relative like "1h")
        horizon: String,
    },
}

impl RuntimeCommands {
    pub async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        match self {
            Self::List { role } => {
                let modules = client.list_runtime(*role).await?;
                CommandOutput::list(modules, "No modules found.", format_table_runtime)
                    .display(&format)?;
            }
            Self::Status { node } => {
                let response = client.runtime_status(node).await?;
                CommandOutput::single(response, format_runtime_status_table).display(&format)?;
            }
            Self::Drain { node, reason } => {
                with_spinner_result(
                    format!("Draining node {node}..."),
                    format!("RuntimeActor {node} drained"),
                    client.drain_runtime(node, reason.as_deref()),
                )
                .await?;
            }
            Self::Resume { node } => {
                with_spinner_result(
                    format!("Resuming node {node}..."),
                    format!("RuntimeActor {node} resumed"),
                    client.resume_runtime(node),
                )
                .await?;
            }
            Self::SetHorizon { node, horizon } => {
                with_spinner_result(
                    format!("Setting horizon for {node}..."),
                    format!("RuntimeActor {node} horizon set to {horizon}"),
                    client.set_runtime_horizon(node, horizon),
                )
                .await?;
            }
        }
        Ok(())
    }
}

/// Format node status as table
fn format_runtime_status_table(response: &InstanceHealthResponse) -> String {
    let mut output = String::new();
    output.push_str("RuntimeActor Status:\n");
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
