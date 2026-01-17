use clap::Subcommand;

use crate::client::GatewayClient;
use crate::fmt::{format_json, format_table_nodes, format_yaml, Spinner};
use crate::model::{NodeRole, OutputFormat};
use crate::Result;

/// Node operations
#[derive(Debug, Subcommand)]
#[command(after_help = "\
EXAMPLES:
    # List all registered nodes
    sinexctl node list

    # List only satellite nodes
    sinexctl node list --role satellite

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
                match format {
                    OutputFormat::Table => {
                        if nodes.is_empty() {
                            println!("No nodes found.");
                        } else {
                            println!("{}", format_table_nodes(&nodes));
                        }
                    }
                    OutputFormat::Json => {
                        for node in &nodes {
                            println!("{}", format_json(node)?);
                        }
                    }
                    OutputFormat::Yaml => {
                        println!("{}", format_yaml(&nodes)?);
                    }
                }
            }
            Self::Status { node, format } => {
                let node_info = client.node_status(node).await?;
                match format {
                    OutputFormat::Table => {
                        println!("Node Status:");
                        println!("  ID: {}", node_info.id);
                        println!("  Name: {}", node_info.name);
                        println!("  Role: {}", node_info.role);
                        println!("  Status: {}", node_info.status);
                        println!("  Last Heartbeat: {}", node_info.last_heartbeat);
                        if let Some(is_leader) = node_info.is_leader {
                            println!("  Leader: {}", if is_leader { "Yes" } else { "No" });
                        }
                    }
                    OutputFormat::Json => {
                        println!("{}", format_json(&node_info)?);
                    }
                    OutputFormat::Yaml => {
                        println!("{}", format_yaml(&node_info)?);
                    }
                }
            }
            Self::Drain { node } => {
                let spinner = Spinner::new(&format!("Draining node {}...", node));
                match client.drain_node(node).await {
                    Ok(()) => {
                        spinner.finish_with_message(&format!("Node {} drained", node));
                    }
                    Err(e) => {
                        spinner.abandon_with_message(&format!("Failed to drain {}", node));
                        return Err(e);
                    }
                }
            }
            Self::Resume { node } => {
                let spinner = Spinner::new(&format!("Resuming node {}...", node));
                match client.resume_node(node).await {
                    Ok(()) => {
                        spinner.finish_with_message(&format!("Node {} resumed", node));
                    }
                    Err(e) => {
                        spinner.abandon_with_message(&format!("Failed to resume {}", node));
                        return Err(e);
                    }
                }
            }
            Self::SetHorizon { node, horizon } => {
                let spinner = Spinner::new(&format!("Setting horizon for {}...", node));
                match client.set_node_horizon(node, horizon).await {
                    Ok(()) => {
                        spinner.finish_with_message(&format!(
                            "Node {} horizon set to {}",
                            node, horizon
                        ));
                    }
                    Err(e) => {
                        spinner
                            .abandon_with_message(&format!("Failed to set horizon for {}", node));
                        return Err(e);
                    }
                }
            }
        }
        Ok(())
    }
}
