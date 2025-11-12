#![doc = include_str!("../doc/native_messaging.md")]

use async_trait::async_trait;
use color_eyre::eyre::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::{self, Read, Write};
use tokio::task;
use tracing::{debug, error, info};

use crate::service_container::ServiceContainer;

/// Environment variable used to configure trusted native-messaging extensions.
const TRUSTED_EXTENSION_ENV: &str = "SINEX_NATIVE_MESSAGING_TRUSTED_EXTENSIONS";

/// Configuration knobs for the native messaging server.
#[derive(Debug, Clone, Default)]
pub struct NativeMessagingConfig {
    trusted_extensions: Vec<String>,
}

impl NativeMessagingConfig {
    /// Load configuration from environment variables.
    pub fn from_env() -> Self {
        let trusted_extensions = std::env::var(TRUSTED_EXTENSION_ENV)
            .ok()
            .map(|raw| {
                raw.split(',')
                    .map(|entry| entry.trim())
                    .filter(|entry| !entry.is_empty())
                    .map(|entry| entry.to_string())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        Self { trusted_extensions }
    }

    /// Helper for tests to build configs with known trusted extensions.
    #[allow(dead_code)]
    pub fn with_trusted_extensions<I, S>(ids: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            trusted_extensions: ids.into_iter().map(Into::into).collect(),
        }
    }

    fn enforce_extension(&self, message: &NativeMessage) -> Result<()> {
        if self.trusted_extensions.is_empty() {
            return Ok(());
        }

        let _ = message;
        // TODO: Require a successful handshake that validates extension ID / secret.
        Ok(())
    }
}

/// Transport abstraction so tests can drive the native messaging loop without stdin/stdout.
#[async_trait]
pub trait NativeMessagingTransport: Send {
    async fn read_message(&mut self) -> Result<Option<NativeMessage>>;
    async fn write_message(&mut self, response: &NativeResponse) -> Result<()>;
}

#[derive(Default)]
struct StdioNativeMessagingTransport;

#[async_trait]
impl NativeMessagingTransport for StdioNativeMessagingTransport {
    async fn read_message(&mut self) -> Result<Option<NativeMessage>> {
        read_message_from_stdio().await
    }

    async fn write_message(&mut self, response: &NativeResponse) -> Result<()> {
        write_message_to_stdio(response).await
    }
}

const MAX_MESSAGE_SIZE: usize = 1024 * 1024; // 1MB

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone, Deserialize)]
pub struct NativeMessage {
    #[serde(rename = "type")]
    msg_type: String,
    method: Option<String>,
    params: Option<Value>,
    id: Option<String>,
    #[serde(default)]
    #[allow(dead_code)]
    extension_id: Option<String>,
    #[serde(default)]
    #[allow(dead_code)]
    extension_secret: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct NativeResponse {
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

    /// Inspect the message type (used by tests to assert auth failures).
    #[allow(dead_code)]
    pub fn message_type(&self) -> &str {
        &self.msg_type
    }
}

impl NativeMessage {
    /// Convenience helper to build RPC messages for tests and harnesses.
    #[allow(dead_code)]
    pub fn rpc(method: impl Into<String>, params: Value, id: impl Into<String>) -> Self {
        Self {
            msg_type: "rpc".to_string(),
            method: Some(method.into()),
            params: Some(params),
            id: Some(id.into()),
            extension_id: None,
            extension_secret: None,
        }
    }

    /// Attach an extension identifier to the message metadata.
    #[allow(dead_code)]
    pub fn with_extension_id(mut self, extension_id: impl Into<String>) -> Self {
        self.extension_id = Some(extension_id.into());
        self
    }

    /// Attach an extension secret to the message metadata.
    #[allow(dead_code)]
    pub fn with_extension_secret(mut self, secret: impl Into<String>) -> Self {
        self.extension_secret = Some(secret.into());
        self
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

async fn read_message_from_stdio() -> Result<Option<NativeMessage>> {
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

async fn write_message_to_stdio(response: &NativeResponse) -> Result<()> {
    let response = response.clone();
    task::spawn_blocking(move || write_message_blocking(&response))
        .await
        .map_err(|e| color_eyre::eyre::eyre!("write_message task panicked: {}", e))?
}

/// Process a single message and return response
async fn process_message(
    services: &ServiceContainer,
    config: &NativeMessagingConfig,
    message: NativeMessage,
) -> NativeResponse {
    let message_id = message.id.clone();

    if let Err(err) = config.enforce_extension(&message) {
        return NativeResponse::error(message_id, format!("Extension rejected: {}", err));
    }

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

/// Run the native messaging loop using stdin/stdout transport.
pub async fn run(services: ServiceContainer) -> Result<()> {
    let config = NativeMessagingConfig::from_env();
    run_with_transport(services, config, StdioNativeMessagingTransport::default()).await
}

/// Run the native messaging loop with a custom transport and configuration.
pub async fn run_with_transport<T: NativeMessagingTransport>(
    services: ServiceContainer,
    config: NativeMessagingConfig,
    mut transport: T,
) -> Result<()> {
    info!("Starting native messaging mode");

    loop {
        match transport.read_message().await? {
            Some(message) => {
                debug!("Received message: {:?}", message);

                let response = process_message(&services, &config, message).await;

                if let Err(e) = transport.write_message(&response).await {
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
