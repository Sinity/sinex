#![doc = include_str!("../doc/native_messaging.md")]

use color_eyre::eyre::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::{self, Read, Write};
use tokio::task;
use tracing::{debug, error, info};

use crate::service_container::ServiceContainer;

const MAX_MESSAGE_SIZE: usize = 1024 * 1024; // 1MB

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

/// Read a message from stdin using native messaging protocol (blocking)
fn read_message_blocking() -> Result<Option<NativeMessage>> {
    let stdin = io::stdin();
    let mut handle = stdin.lock();

    // Read message length (4 bytes, little-endian per native messaging spec)
    let mut len_bytes = [0u8; 4];
    match handle.read_exact(&mut len_bytes) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    }
    let length = u32::from_le_bytes(len_bytes) as usize;

    // Validate length (Chrome/Firefox limit is 1MB)
    if length > MAX_MESSAGE_SIZE {
        bail!("Message too large: {} bytes", length);
    }

    // Read message content
    let mut buffer = vec![0u8; length];
    handle.read_exact(&mut buffer)?;

    // Parse JSON
    let message: NativeMessage =
        serde_json::from_slice(&buffer).wrap_err("Failed to parse native message")?;

    Ok(Some(message))
}

async fn read_message() -> Result<Option<NativeMessage>> {
    task::spawn_blocking(read_message_blocking)
        .await
        .map_err(|e| color_eyre::eyre::eyre!("read_message task panicked: {}", e))?
}

/// Write a message to stdout using native messaging protocol
fn write_message_blocking(response: &NativeResponse) -> Result<()> {
    let stdout = io::stdout();
    let mut handle = stdout.lock();

    // Serialize to JSON
    let json = serde_json::to_vec(response)?;

    // Write message length (4 bytes, little-endian per native messaging spec)
    let len_bytes = (json.len() as u32).to_le_bytes();
    handle.write_all(&len_bytes)?;

    // Write message content
    handle.write_all(&json)?;
    handle.flush()?;

    Ok(())
}

async fn write_message(response: &NativeResponse) -> Result<()> {
    let response = response.clone();
    task::spawn_blocking(move || write_message_blocking(&response))
        .await
        .map_err(|e| color_eyre::eyre::eyre!("write_message task panicked: {}", e))?
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

/// Dispatch RPC method to appropriate handler (shared with rpc_server)
async fn dispatch_method(
    services: &ServiceContainer,
    method: &str,
    params: Value,
) -> Result<Value> {
    // Use shared dispatch table from rpc_server
    crate::rpc_server::dispatch_rpc_method(services, method, params).await
}

/// Run the native messaging loop
pub async fn run(services: ServiceContainer) -> Result<()> {
    info!("Starting native messaging mode");

    // Main message loop
    loop {
        match read_message().await? {
            Some(message) => {
                debug!("Received message: {:?}", message);

                let response = process_message(&services, message).await;

                if let Err(e) = write_message(&response).await {
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
