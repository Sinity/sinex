use clap::Subcommand;
use sinex_primitives::rpc::coordination::InstanceHealthResponse;

use crate::Result;
use crate::client::GatewayClient;
use crate::fmt::{CommandOutput, format_table_runtime, with_spinner_result};
use crate::model::{OutputFormat, RuntimeModuleRole};

/// Runtime module operations
#[derive(Debug, Subcommand)]
#[command(after_help = "\
EXAMPLES:
    # List all registered modules
    sinexctl runtime list

    # List only ingestor modules
    sinexctl runtime list --role ingestor

    # Check status of a specific runtime module
    sinexctl runtime status terminal-source

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
                let modules = client.list_runtime(*role).await?;
                CommandOutput::list(modules, "No modules found.", format_table_runtime)
                    .display(&format)?;
            }
            Self::Status { module } => {
                let response = client.runtime_status(module).await?;
                CommandOutput::single(response, format_runtime_status_table).display(&format)?;
            }
            Self::Drain { module, reason } => {
                with_spinner_result(
                    format!("Draining runtime module {module}..."),
                    format!("Runtime module {module} drained"),
                    client.drain_runtime(module, reason.as_deref()),
                )
                .await?;
            }
            Self::Resume { module } => {
                with_spinner_result(
                    format!("Resuming runtime module {module}..."),
                    format!("Runtime module {module} resumed"),
                    client.resume_runtime(module),
                )
                .await?;
            }
            Self::SetHorizon { module, horizon } => {
                with_spinner_result(
                    format!("Setting horizon for {module}..."),
                    format!("Runtime module {module} horizon set to {horizon}"),
                    client.set_runtime_horizon(module, horizon),
                )
                .await?;
            }
        }
        Ok(())
    }
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
