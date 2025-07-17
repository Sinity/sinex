//! Native messaging protocol for browser extension communication

use anyhow::{bail, Context, Result};
use byteorder::{NativeEndian, ReadBytesExt, WriteBytesExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::{self, Read, Write};
use tracing::{debug, error, info};

use crate::handlers::*;
use crate::service_container::ServiceContainer;

#[derive(Debug, Clone, Deserialize)]
struct NativeMessage {
    #[serde(rename = "type")]
    msg_type: String,
    method: Option<String>,
    params: Option<Value>,
    id: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct NativeResponse {
    #[serde(rename = "type")]
    msg_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<String>,
}

impl NativeResponse {
    fn success(id: Option<String>, result: Value) -> Self {
        Self {
            msg_type: "response".to_string(),
            result: Some(result),
            error: None,
            id,
        }
    }

    fn error(id: Option<String>, error: String) -> Self {
        Self {
            msg_type: "error".to_string(),
            result: None,
            error: Some(error),
            id,
        }
    }
}

/// Read a message from stdin using native messaging protocol
fn read_message() -> Result<Option<NativeMessage>> {
    let stdin = io::stdin();
    let mut handle = stdin.lock();

    // Read message length (4 bytes, native endian)
    let length = match handle.read_u32::<NativeEndian>() {
        Ok(len) => len as usize,
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    };

    // Validate length (Chrome/Firefox limit is 1MB)
    if length > 1024 * 1024 {
        bail!("Message too large: {} bytes", length);
    }

    // Read message content
    let mut buffer = vec![0u8; length];
    handle.read_exact(&mut buffer)?;

    // Parse JSON
    let message: NativeMessage =
        serde_json::from_slice(&buffer).context("Failed to parse native message")?;

    Ok(Some(message))
}

/// Write a message to stdout using native messaging protocol
fn write_message(response: &NativeResponse) -> Result<()> {
    let stdout = io::stdout();
    let mut handle = stdout.lock();

    // Serialize to JSON
    let json = serde_json::to_vec(response)?;

    // Write message length (4 bytes, native endian)
    handle.write_u32::<NativeEndian>(json.len() as u32)?;

    // Write message content
    handle.write_all(&json)?;
    handle.flush()?;

    Ok(())
}

/// Process a single message and return response
async fn process_message(services: &ServiceContainer, message: NativeMessage) -> NativeResponse {
    // Handle different message types
    match message.msg_type.as_str() {
        "ping" => NativeResponse::success(message.id, serde_json::json!({ "pong": true })),

        "rpc" => match (message.method, message.params) {
            (Some(method), Some(params)) => {
                match dispatch_method(services, &method, params).await {
                    Ok(result) => NativeResponse::success(message.id, result),
                    Err(err) => NativeResponse::error(message.id, err.to_string()),
                }
            }
            _ => NativeResponse::error(
                message.id,
                "RPC message must include method and params".to_string(),
            ),
        },

        _ => NativeResponse::error(
            message.id,
            format!("Unknown message type: {}", message.msg_type),
        ),
    }
}

/// Dispatch RPC method to appropriate handler
async fn dispatch_method(
    services: &ServiceContainer,
    method: &str,
    params: Value,
) -> Result<Value> {
    match method {
        // Analytics methods
        "analytics.event_count_by_source" => {
            handle_event_count_by_source(services.analytics.as_ref(), params).await
        }

        "analytics.activity_heatmap" => {
            handle_activity_heatmap(services.analytics.as_ref(), params).await
        }

        // PKM methods
        "pkm.create_note" => handle_create_note(services.pkm.as_ref(), params).await,

        "pkm.create_entities_from_list" => {
            handle_create_entities(services.pkm.as_ref(), params).await
        }

        "pkm.link_entities" => handle_link_entities(services.pkm.as_ref(), params).await,

        // Search methods
        "search.search_events" => handle_search_events(services.search.as_ref(), params).await,

        // Content methods
        "content.store_blob" => handle_store_blob(services.content.as_ref(), params).await,

        "content.retrieve_blob" => handle_retrieve_blob(services.content.as_ref(), params).await,

        _ => bail!("Unknown method: {}", method),
    }
}

/// Run the native messaging loop
pub async fn run(services: ServiceContainer) -> Result<()> {
    info!("Starting native messaging mode");

    // Main message loop
    loop {
        match read_message()? {
            Some(message) => {
                debug!("Received message: {:?}", message);

                let response = process_message(&services, message).await;

                if let Err(e) = write_message(&response) {
                    error!("Failed to write response: {}", e);
                    break;
                }
            }
            None => {
                info!("EOF reached, shutting down");
                break;
            }
        }
    }

    Ok(())
}
