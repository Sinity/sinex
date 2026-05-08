//! NATS listener for parse commands dispatched by the gateway replay engine.
//!
//! The gateway's replay execution publishes `SourceParseCommand` to
//! `sinex.control.sources.{source_id}.parse` for staged-source replay (#1060).
//! This listener subscribes to that subject and dispatches to the appropriate
//! parser capability.

use async_nats::Client;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use sinex_primitives::environment::SinexEnvironment;
use sinex_primitives::Uuid;
use tracing::{error, info, warn};

/// Command dispatched by the gateway replay engine to request a source parse.
#[derive(Debug, Serialize, Deserialize)]
pub struct SourceParseCommand {
    pub operation_id: Uuid,
    pub source_id: String,
    pub source_material_id: Option<Uuid>,
    pub source_version: Option<String>,
    pub executor: String,
}

/// Response sent back to the gateway after accepting or rejecting a parse command.
#[derive(Debug, Serialize, Deserialize)]
pub struct SourceParseAck {
    pub accepted: bool,
    pub error: Option<String>,
}

/// Subscribe to parse commands for a source unit and handle them.
///
/// Runs as a background task alongside the source unit's normal continuous
/// operation. Each source unit gets its own subscription so parse commands
/// for `source_id = "desktop"` only reach the desktop source.
pub async fn spawn_parse_listener(
    client: Client,
    env: &SinexEnvironment,
    source_id: &str,
) -> Result<tokio::task::JoinHandle<()>, async_nats::SubscribeError> {
    let subject = env.nats_subject(&format!("sinex.control.sources.{source_id}.parse"));
    let mut subscription = client.subscribe(subject.clone()).await?;

    let source_id = source_id.to_string();
    let client_clone = client.clone();

    Ok(tokio::spawn(async move {
        info!(
            source_id = %source_id,
            subject = %subject,
            "Parse listener started"
        );

        while let Some(message) = subscription.next().await {
            let client = client_clone.clone();
            let source_id = source_id.clone();

            tokio::spawn(async move {
                match handle_parse_command(&client, &source_id, &message).await {
                    Ok(()) => {}
                    Err(e) => {
                        warn!(
                            source_id = %source_id,
                            error = %e,
                            "Parse command handling failed"
                        );
                    }
                }
            });
        }

        warn!(
            source_id = %source_id,
            "Parse listener subscription closed"
        );
    }))
}

async fn handle_parse_command(
    client: &Client,
    source_id: &str,
    message: &async_nats::Message,
) -> Result<(), Box<dyn std::error::Error>> {
    let reply_subject = message.reply.clone();

    let ack = match serde_json::from_slice::<SourceParseCommand>(&message.payload) {
        Ok(cmd) => {
            info!(
                operation_id = %cmd.operation_id,
                source_id = %cmd.source_id,
                material_id = ?cmd.source_material_id,
                "Received parse command"
            );

            // Verify the command targets this source
            if cmd.source_id != source_id {
                SourceParseAck {
                    accepted: false,
                    error: Some(format!(
                        "Parse command source_id '{}' does not match listener '{}'",
                        cmd.source_id, source_id
                    )),
                }
            } else {
                // TODO: Invoke parser capability for the given material.
                // For now, accept the command. The parser invocation will be
                // wired when source-material parser dispatch is implemented.
                SourceParseAck {
                    accepted: true,
                    error: None,
                }
            }
        }
        Err(e) => {
            warn!(error = %e, "Failed to deserialize parse command");
            SourceParseAck {
                accepted: false,
                error: Some(format!("Invalid parse command payload: {e}")),
            }
        }
    };

    if let Some(reply) = reply_subject {
        let payload = serde_json::to_vec(&ack)?;
        if let Err(e) = client.publish(reply, payload.into()).await {
            error!(error = %e, "Failed to send parse command ack");
        }
    }

    Ok(())
}
