use clap::Subcommand;

use crate::client::GatewayClient;
use crate::model::OutputFormat;
use crate::Result;

/// Gateway operations
#[derive(Debug, Subcommand)]
pub enum GatewayCommands {
    /// Ping the gateway
    Ping,

    /// Get gateway version
    Version,
}

impl GatewayCommands {
    pub async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        match self {
            Self::Ping => {
                let response = client.ping().await?;
                match format {
                    OutputFormat::Table => println!("{}", response),
                    OutputFormat::Json => {
                        println!("{}", serde_json::json!({"response": response}))
                    }
                    OutputFormat::Yaml => {
                        println!("response: {}", response)
                    }
                }
            }
            Self::Version => {
                let version = client.version().await?;
                match format {
                    OutputFormat::Table => println!("{}", version),
                    OutputFormat::Json => {
                        println!("{}", serde_json::json!({"version": version}))
                    }
                    OutputFormat::Yaml => {
                        println!("version: {}", version)
                    }
                }
            }
        }
        Ok(())
    }
}
