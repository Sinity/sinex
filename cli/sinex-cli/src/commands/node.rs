use clap::Subcommand;
use sinex_core::rpc::coordination::InstanceHealthResponse;

use crate::client::GatewayClient;
use crate::fmt::{format_table_nodes, with_spinner_result, CommandOutput};
use crate::model::{NodeRole, OutputFormat};
use crate::Result;

/// Node operations
#[derive(Debug, Subcommand)]
#[command(after_help = "\
EXAMPLES:
    # List all registered nodes
    sinexctl node list

    # List only ingestor nodes
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
pub enum NodeCommands {
    /// List all nodes
    List {
        /// Filter by role
        #[arg(long)]
        role: Option<NodeRole>,

        /// Output format
        #[arg(long, short = 'f', value_enum, default_value = "table")]
        format: OutputFormat,
    },

    /// Show node status
    Status {
        /// Node ID or name
        node: String,

        /// Output format
        #[arg(long, short = 'f', value_enum, default_value = "table")]
        format: OutputFormat,
    },

    /// Drain a node for maintenance
    Drain {
        /// Node ID or name
        node: String,
        /// Reason for draining
        #[arg(long, short)]
        reason: Option<String>,
    },

    /// Resume a drained node
    Resume {
        /// Node ID or name
        node: String,
    },

    /// Set node horizon (cutoff time for event processing)
    SetHorizon {
        /// Node ID or name
        node: String,

        /// Horizon timestamp (RFC3339 format or relative like "1h")
        horizon: String,
    },
}

impl NodeCommands {
    pub async fn execute(&self, client: &GatewayClient) -> Result<()> {
        match self {
            Self::List { role, format } => {
                let nodes = client.list_nodes(*role).await?;
                CommandOutput::list(nodes, "No nodes found.", format_table_nodes)
                    .display(format)?;
            }
            Self::Status { node, format } => {
                let response = client.node_status(node).await?;
                CommandOutput::single(response, format_node_status_table).display(format)?;
            }
            Self::Drain { node, reason } => {
                with_spinner_result(
                    format!("Draining node {}...", node),
                    format!("Node {} drained", node),
                    client.drain_node(node, reason.as_deref()),
                )
                .await?;
            }
            Self::Resume { node } => {
                with_spinner_result(
                    format!("Resuming node {}...", node),
                    format!("Node {} resumed", node),
                    client.resume_node(node),
                )
                .await?;
            }
            Self::SetHorizon { node, horizon } => {
                with_spinner_result(
                    format!("Setting horizon for {}...", node),
                    format!("Node {} horizon set to {}", node, horizon),
                    client.set_node_horizon(node, horizon),
                )
                .await?;
            }
        }
        Ok(())
    }
}

/// Format node status as table
fn format_node_status_table(response: &InstanceHealthResponse) -> String {
    let mut output = String::new();
    output.push_str("Node Status:\n");
    output.push_str(&format!(
        "  Instance ID: {}\n",
        response.instance.instance_id
    ));
    output.push_str(&format!("  Type: {}\n", response.instance.node_type));
    if let Some(ref hostname) = response.instance.hostname {
        output.push_str(&format!("  Hostname: {}\n", hostname));
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
        output.push_str(&format!("  Last Heartbeat: {}\n", heartbeat));
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
        output.push_str(&format!("  Last Error: {}\n", err));
    }
    output
}
