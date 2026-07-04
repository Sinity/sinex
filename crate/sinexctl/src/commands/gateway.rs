use clap::Subcommand;
use serde::Serialize;
use sinex_primitives::views::ViewEnvelope;

use crate::Result;
use crate::client::GatewayClient;
use crate::fmt::{CommandOutput, print_finite_envelope};
use crate::model::OutputFormat;

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
                let envelope = gateway_envelope("sinexctl.runtime.gateway.ping", response);
                if print_finite_envelope(&envelope, format)? {
                    return Ok(());
                }
                CommandOutput::single(envelope.payload, |r| r.value.clone()).display(&format)?;
            }
            Self::Version => {
                let version = client.version().await?;
                let envelope = gateway_envelope("sinexctl.runtime.gateway.version", version);
                if print_finite_envelope(&envelope, format)? {
                    return Ok(());
                }
                CommandOutput::single(envelope.payload, |r| r.value.clone()).display(&format)?;
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

fn gateway_envelope(
    source_surface: &'static str,
    value: String,
) -> ViewEnvelope<GatewayResponseValue> {
    ViewEnvelope::new(source_surface, GatewayResponseValue { value })
}

#[cfg(test)]
#[path = "gateway_test.rs"]
mod tests;
