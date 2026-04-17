use clap::Subcommand;
use serde::Serialize;
use serde_json::Value;
use sinex_primitives::events::builder::get_hostname;
use sinex_primitives::rpc::ingest::EventIngestRequest;
use sinex_primitives::temporal::Timestamp;

use crate::Result;
use crate::client::GatewayClient;
use crate::fmt::CommandOutput;
use crate::model::OutputFormat;

/// Gateway operations
#[derive(Debug, Subcommand)]
pub enum GatewayCommands {
    /// Ping the gateway
    Ping,

    /// Get gateway version
    Version,

    /// Publish a single event through the gateway (end-to-end smoke test)
    ///
    /// Sends the event via the events.ingest RPC endpoint, which publishes it
    /// directly to NATS `JetStream` for ingestd to pick up and write to the DB.
    Ingest {
        /// Event source identifier (e.g. "fs-watcher" or "test")
        #[arg(long)]
        source: String,

        /// Event type string (e.g. "file.created" or "test.ping")
        #[arg(long)]
        event_type: String,

        /// JSON payload (defaults to `{}`)
        #[arg(long, default_value = "{}")]
        payload: String,

        /// Host override (defaults to current hostname)
        #[arg(long)]
        host: Option<String>,

        /// Original event timestamp in RFC3339 format (defaults to current time)
        #[arg(long)]
        ts_orig: Option<String>,
    },
}

impl GatewayCommands {
    pub async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        match self {
            Self::Ping => {
                let response = client.ping().await?;
                CommandOutput::single(GatewayResponseValue { value: response }, |r| {
                    r.value.clone()
                })
                .display(&format)?;
            }
            Self::Version => {
                let version = client.version().await?;
                CommandOutput::single(GatewayResponseValue { value: version }, |r| r.value.clone())
                    .display(&format)?;
            }
            Self::Ingest {
                source,
                event_type,
                payload,
                host,
                ts_orig,
            } => {
                let payload_value: Value = serde_json::from_str(payload)
                    .map_err(|e| color_eyre::eyre::eyre!("invalid --payload JSON: {e}"))?;

                let resolved_host = host.clone().unwrap_or_else(|| get_hostname().to_string());

                let req = EventIngestRequest {
                    source: source.clone(),
                    event_type: event_type.clone(),
                    payload: payload_value,
                    ts_orig: ts_orig
                        .clone()
                        .unwrap_or_else(|| Timestamp::now().format_rfc3339()),
                    host: Some(resolved_host),
                };

                let response = client.ingest_event(req).await?;
                CommandOutput::single(response, format_ingest_response).display(&format)?;
            }
        }
        Ok(())
    }
}

fn format_ingest_response(r: &sinex_primitives::rpc::ingest::EventIngestResponse) -> String {
    format!("event_id:  {}\nsequence:  {}", r.event_id, r.sequence)
}

#[derive(Serialize)]
struct GatewayResponseValue {
    #[serde(rename = "response")]
    value: String,
}
