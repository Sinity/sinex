use clap::Subcommand;

use crate::client::GatewayClient;
use crate::fmt::{format_json, format_yaml};
use crate::model::OutputFormat;
use crate::Result;

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
                match format {
                    OutputFormat::Table => {
                        println!("System Health:");
                        println!("  Node ID: {}", health.id);
                        println!(
                            "  Status: {}",
                            if health.healthy {
                                "✓ Healthy"
                            } else {
                                "✗ Unhealthy"
                            }
                        );
                        if let Some(details) = &health.details {
                            println!("  Details: {}", details);
                        }
                        println!("  Checked: {}", health.checked_at);
                    }
                    OutputFormat::Json => {
                        println!("{}", format_json(&health)?);
                    }
                    OutputFormat::Yaml => {
                        println!("{}", format_yaml(&health)?);
                    }
                }
            }
        }
        Ok(())
    }
}
