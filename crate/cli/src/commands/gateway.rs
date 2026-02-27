use clap::Subcommand;
use serde::Serialize;

use crate::client::GatewayClient;
use crate::fmt::CommandOutput;
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
                CommandOutput::single(GatewayResponseValue { value: response }, |r| r.value.clone())
                    .display(&format)?;
            }
            Self::Version => {
                let version = client.version().await?;
                CommandOutput::single(GatewayResponseValue { value: version }, |r| r.value.clone())
                    .display(&format)?;
            }
        }
        Ok(())
    }
}

#[derive(Serialize)]
struct GatewayResponseValue {
    #[serde(rename = "response")]
    value: String,
}
