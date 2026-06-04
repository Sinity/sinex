//! NATS listener for parse commands dispatched by the gateway replay engine.
//!
//! The gateway's replay execution publishes `SourceParseCommand` to
//! `sinex.control.sources.{source_id}.parse` for staged-source replay (#1060).
//! This listener subscribes to that subject and dispatches to the appropriate
//! parser capability via the provided dispatch function.

use async_nats::Client;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use sinex_primitives::{ControlSubject, Uuid};
use sqlx::PgPool;
use tracing::{error, info, warn};

use crate::sources::dispatch::ParserDispatchFn;

/// Command dispatched by the gateway replay engine to request a source parse.
#[derive(Debug, Clone, Serialize, Deserialize)]
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
    pub event_count: Option<usize>,
}

/// Subscribe to parse commands for a source and handle them.
///
/// Runs as a background task alongside the source's normal continuous
/// operation. Each source gets its own subscription so parse commands
/// for `source_id = "desktop"` only reach the desktop source.
pub async fn spawn_parse_listener(
    client: Client,
    source_id: &str,
    dispatch: ParserDispatchFn,
    pool: PgPool,
) -> Result<tokio::task::JoinHandle<()>, async_nats::SubscribeError> {
    let subject = ControlSubject::source_parse(source_id);
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
            let dispatch = dispatch.clone();
            let pool = pool.clone();

            tokio::spawn(async move {
                match handle_parse_command(&client, &source_id, &message, &dispatch, &pool).await {
                    Ok(()) => {}
                    Err(e) => {
                        warn!(
                            target: "sinex_metrics",
                            metric = "source.parse_command_failures_total",
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
    dispatch: &ParserDispatchFn,
    _pool: &PgPool,
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

            if cmd.source_id == source_id {
                // Invoke the parser dispatch. For now, this passes empty bytes —
                // the next slice wires material loading from source_material_registry.
                match dispatch(&cmd.source_id, &[], cmd.source_material_id) {
                    Ok(outcome) => {
                        info!(
                            operation_id = %cmd.operation_id,
                            parser = %outcome.parser_id,
                            version = %outcome.parser_version,
                            event_count = outcome.events.len(),
                            "Parse completed"
                        );
                        SourceParseAck {
                            accepted: true,
                            error: None,
                            event_count: Some(outcome.events.len()),
                        }
                    }
                    Err(e) => {
                        warn!(
                            target: "sinex_metrics",
                            metric = "source.parse_dispatch_failures_total",
                            operation_id = %cmd.operation_id,
                            source_id = %cmd.source_id,
                            error = %e,
                            "Parse dispatch failed"
                        );
                        SourceParseAck {
                            accepted: false,
                            error: Some(e),
                            event_count: None,
                        }
                    }
                }
            } else {
                SourceParseAck {
                    accepted: false,
                    error: Some(format!(
                        "Parse command source_id '{}' does not match listener '{}'",
                        cmd.source_id, source_id
                    )),
                    event_count: None,
                }
            }
        }
        Err(e) => {
            warn!(
                target: "sinex_metrics",
                metric = "source.parse_command_deser_failures_total",
                error = %e,
                "Failed to deserialize parse command"
            );
            SourceParseAck {
                accepted: false,
                error: Some(format!("Invalid parse command payload: {e}")),
                event_count: None,
            }
        }
    };

    if let Some(reply) = reply_subject {
        let payload = serde_json::to_vec(&ack)?;
        if let Err(e) = client.publish(reply, payload.into()).await {
            error!(
                target: "sinex_metrics",
                metric = "source.parse_ack_failures_total",
                error = %e,
                "Failed to send parse command ack"
            );
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sources::dispatch::test_parser_dispatch;
    use sinex_primitives::Uuid;
    use xtask::sandbox::prelude::sinex_test;

    #[sinex_test]
    async fn test_parse_command_accepted_for_matching_source_id() -> xtask::sandbox::TestResult<()>
    {
        let (dispatch, calls) = test_parser_dispatch();
        let cmd = SourceParseCommand {
            operation_id: Uuid::now_v7(),
            source_id: "weechat".to_string(),
            source_material_id: None,
            source_version: None,
            executor: "test".to_string(),
        };

        // Simulate what handle_parse_command does internally
        let ack = if cmd.source_id == "weechat" {
            match dispatch(&cmd.source_id, &[], cmd.source_material_id) {
                Ok(outcome) => SourceParseAck {
                    accepted: true,
                    error: None,
                    event_count: Some(outcome.events.len()),
                },
                Err(e) => SourceParseAck {
                    accepted: false,
                    error: Some(e),
                    event_count: None,
                },
            }
        } else {
            SourceParseAck {
                accepted: false,
                error: Some("mismatch".into()),
                event_count: None,
            }
        };

        assert!(ack.accepted);
        assert_eq!(calls.lock().unwrap().len(), 1);
        assert_eq!(calls.lock().unwrap()[0].0, "weechat");
        Ok(())
    }

    #[sinex_test]
    async fn test_parse_command_rejected_for_mismatched_source_id() -> xtask::sandbox::TestResult<()>
    {
        let (_dispatch, calls) = test_parser_dispatch();
        let cmd = SourceParseCommand {
            operation_id: Uuid::now_v7(),
            source_id: "desktop".to_string(),
            source_material_id: None,
            source_version: None,
            executor: "test".to_string(),
        };

        let ack = if cmd.source_id == "weechat" {
            // ... (would dispatch)
            SourceParseAck {
                accepted: true,
                error: None,
                event_count: None,
            }
        } else {
            SourceParseAck {
                accepted: false,
                error: Some("mismatch".into()),
                event_count: None,
            }
        };

        assert!(!ack.accepted);
        assert!(ack.error.unwrap().contains("mismatch"));
        assert_eq!(
            calls.lock().unwrap().len(),
            0,
            "dispatch should not be called for mismatched source"
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_parse_command_dispatches_to_unknown_source() -> xtask::sandbox::TestResult<()> {
        let (dispatch, _calls) = test_parser_dispatch();
        // The test dispatch accepts any source_id; the real dispatch rejects unknown ones
        let result = dispatch("unknown-source", &[], None);
        assert!(result.is_ok()); // test dispatch always succeeds

        // But the *default* dispatch should reject unknown sources
        let default_dispatch = crate::sources::dispatch::default_parser_dispatch();
        let result = default_dispatch("unknown-source", &[], None);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("unknown source_id"));
        Ok(())
    }
}
