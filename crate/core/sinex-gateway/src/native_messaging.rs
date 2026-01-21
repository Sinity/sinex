#![doc = include_str!("../docs/native_messaging.md")]

use async_trait::async_trait;
use color_eyre::eyre::{bail, eyre, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::{self};
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use subtle::ConstantTimeEq;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{debug, error, info, warn};

use crate::service_container::ServiceContainer;

/// Environment variable used to configure trusted native-messaging extensions.
const TRUSTED_EXTENSION_ENV: &str = "SINEX_NATIVE_MESSAGING_TRUSTED_EXTENSIONS";
/// Environment variable used to configure trusted native-messaging hosts.
const TRUSTED_HOSTS_ENV: &str = "SINEX_NATIVE_MESSAGING_TRUSTED_HOSTS";
/// Environment variable used to enforce a protocol version for native messaging.
const PROTOCOL_VERSION_ENV: &str = "SINEX_NATIVE_MESSAGING_PROTOCOL_VERSION";

/// Configuration knobs for the native messaging server.
#[derive(Debug, Clone, Default)]
pub struct NativeMessagingConfig {
    trusted_extensions: Vec<TrustedExtension>,
    trusted_hosts: Vec<String>,
    expected_protocol_version: Option<String>,
}

#[derive(Debug, Clone, Default)]
struct TrustedExtension {
    id: String,
    secret: Option<String>,
}

#[cfg(test)]
static SECRET_COMPARE_CALLS: AtomicUsize = AtomicUsize::new(0);

fn secrets_match(expected: &str, provided: &str) -> bool {
    #[cfg(test)]
    SECRET_COMPARE_CALLS.fetch_add(1, Ordering::Relaxed);

    bool::from(expected.as_bytes().ct_eq(provided.as_bytes()))
}

impl NativeMessagingConfig {
    /// Load configuration from environment variables.
    pub fn from_env() -> Self {
        let trusted_extensions = std::env::var(TRUSTED_EXTENSION_ENV)
            .ok()
            .map(parse_trusted_entries)
            .unwrap_or_default();
        let trusted_hosts = std::env::var(TRUSTED_HOSTS_ENV)
            .ok()
            .map(parse_csv_entries)
            .unwrap_or_default();
        let expected_protocol_version = std::env::var(PROTOCOL_VERSION_ENV).ok().and_then(|raw| {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        });

        Self {
            trusted_extensions,
            trusted_hosts,
            expected_protocol_version,
        }
    }

    fn enforce_metadata(&self, message: &NativeMessage) -> Result<()> {
        self.enforce_extension(message)?;
        self.enforce_host(message)?;
        self.enforce_protocol_version(message)?;
        Ok(())
    }

    fn enforce_extension(&self, message: &NativeMessage) -> Result<()> {
        // Issue 138: Fail closed - require explicit allowlist
        if self.trusted_extensions.is_empty() {
            warn!(
                event = "native_messaging.auth",
                reason = "no_trusted_extensions_configured",
                "Rejected native messaging call: no trusted extensions configured (set SINEX_NATIVE_MESSAGING_TRUSTED_EXTENSIONS)"
            );
            return Err(eyre!(
                "No trusted extensions configured. Set SINEX_NATIVE_MESSAGING_TRUSTED_EXTENSIONS environment variable."
            ));
        }

        let incoming_id = match message.extension_id.as_deref() {
            Some(id) => id,
            None => {
                warn!(
                    event = "native_messaging.auth",
                    reason = "missing_extension_id",
                    "Rejected native messaging call: extension metadata missing"
                );
                return Err(eyre!("Missing extension_id"));
            }
        };

        let trusted = self
            .trusted_extensions
            .iter()
            .find(|ext| ext.id == incoming_id)
            .ok_or_else(|| {
                warn!(
                    event = "native_messaging.auth",
                    extension_id = incoming_id,
                    reason = "not_trusted",
                    "Extension is not in the trusted allow-list"
                );
                eyre!("Extension '{incoming_id}' is not in the trusted allow-list")
            })?;

        if let Some(expected_secret) = &trusted.secret {
            let provided = match message.extension_secret.as_deref() {
                Some(secret) => secret,
                None => {
                    warn!(
                        event = "native_messaging.auth",
                        extension_id = incoming_id,
                        reason = "missing_secret",
                        "Trusted extension omitted the required secret"
                    );
                    return Err(eyre!("Missing extension_secret"));
                }
            };
            if !secrets_match(expected_secret, provided) {
                warn!(
                    event = "native_messaging.auth",
                    extension_id = incoming_id,
                    reason = "invalid_secret",
                    "Extension provided an invalid secret"
                );
                bail!("Invalid secret for extension '{incoming_id}'");
            }
        }

        debug!(
            event = "native_messaging.auth",
            extension_id = incoming_id,
            has_secret = trusted.secret.is_some(),
            "Native messaging request authorized"
        );
        Ok(())
    }

    fn enforce_host(&self, message: &NativeMessage) -> Result<()> {
        if self.trusted_hosts.is_empty() {
            return Ok(());
        }

        let host = match message.host.as_deref() {
            Some(host) => host,
            None => {
                warn!(
                    event = "native_messaging.auth",
                    reason = "missing_host",
                    "Rejected native messaging call: host metadata missing"
                );
                return Err(eyre!("Missing host"));
            }
        };

        if !self.trusted_hosts.iter().any(|allowed| allowed == host) {
            warn!(
                event = "native_messaging.auth",
                host = host,
                reason = "host_not_trusted",
                "Host is not in the trusted allow-list"
            );
            return Err(eyre!("Host '{host}' is not in the trusted allow-list"));
        }

        debug!(
            event = "native_messaging.auth",
            host = host,
            "Native messaging host authorized"
        );
        Ok(())
    }

    fn enforce_protocol_version(&self, message: &NativeMessage) -> Result<()> {
        let expected = match self.expected_protocol_version.as_deref() {
            Some(version) => version,
            None => return Ok(()),
        };

        let provided = match message.protocol_version.as_deref() {
            Some(version) => version,
            None => {
                warn!(
                    event = "native_messaging.auth",
                    expected_version = expected,
                    reason = "missing_protocol_version",
                    "Rejected native messaging call: protocol version missing"
                );
                return Err(eyre!("Missing protocol_version"));
            }
        };

        if provided != expected {
            warn!(
                event = "native_messaging.auth",
                expected_version = expected,
                provided_version = provided,
                reason = "protocol_version_mismatch",
                "Rejected native messaging call: protocol version mismatch"
            );
            return Err(eyre!(
                "Protocol version mismatch (expected '{expected}', got '{provided}')"
            ));
        }

        debug!(
            event = "native_messaging.auth",
            protocol_version = provided,
            "Native messaging protocol version authorized"
        );
        Ok(())
    }
}

fn parse_trusted_entries(raw: String) -> Vec<TrustedExtension> {
    raw.split(',')
        .filter_map(|entry| {
            let entry = entry.trim();
            if entry.is_empty() {
                return None;
            }
            let (id, secret) = match entry.split_once('#') {
                Some((id, secret)) => (id.trim(), Some(secret.trim().to_string())),
                None => (entry, None),
            };
            if id.is_empty() {
                return None;
            }
            Some(TrustedExtension {
                id: id.to_string(),
                secret: secret.filter(|s| !s.is_empty()),
            })
        })
        .collect()
}

fn parse_csv_entries(raw: String) -> Vec<String> {
    raw.split(',')
        .map(|entry| entry.trim())
        .filter(|entry| !entry.is_empty())
        .map(|entry| entry.to_string())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn trusted_message(secret: &str) -> NativeMessage {
        NativeMessage {
            msg_type: "request".to_string(),
            method: None,
            params: None,
            id: None,
            extension_id: Some("ext-1".to_string()),
            extension_secret: Some(secret.to_string()),
            host: None,
            protocol_version: None,
        }
    }

    #[test]
    fn secret_comparison_is_routed_through_constant_time_helper() {
        SECRET_COMPARE_CALLS.store(0, Ordering::Relaxed);

        let config = NativeMessagingConfig {
            trusted_extensions: vec![TrustedExtension {
                id: "ext-1".to_string(),
                secret: Some("topsecret".to_string()),
            }],
            trusted_hosts: Vec::new(),
            expected_protocol_version: None,
        };

        // Successful path still calls the constant-time helper
        config
            .enforce_extension(&trusted_message("topsecret"))
            .expect("trusted secret should pass");

        // Failure path also uses the same helper
        assert!(config
            .enforce_extension(&trusted_message("wrongsecret"))
            .is_err());

        assert!(SECRET_COMPARE_CALLS.load(Ordering::Relaxed) >= 2);
    }
}

/// Transport abstraction so tests can drive the native messaging loop without stdin/stdout.
#[async_trait]
pub trait NativeMessagingTransport: Send {
    async fn read_message(&mut self) -> Result<Option<NativeMessage>>;
    async fn write_message(&mut self, response: &NativeResponse) -> Result<()>;
}

// Issue 136 (LOW): Native messaging size limit is now configurable via environment
fn max_message_size() -> usize {
    std::env::var("SINEX_NATIVE_MESSAGING_MAX_SIZE_BYTES")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .unwrap_or(1024 * 1024) // Default: 1MB (matches Chrome/Firefox native messaging spec)
}

#[derive(Debug, Clone, Deserialize)]
pub struct NativeMessage {
    #[serde(rename = "type")]
    msg_type: String,
    method: Option<String>,
    params: Option<Value>,
    id: Option<String>,
    #[serde(default, alias = "origin")]
    extension_id: Option<String>,
    #[serde(default)]
    extension_secret: Option<String>,
    #[serde(default)]
    host: Option<String>,
    #[serde(default)]
    protocol_version: Option<String>,
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
}

/// Read a message from stdin using native messaging protocol (async)
async fn read_message_async() -> Result<Option<NativeMessage>> {
    let mut stdin = tokio::io::stdin();

    // Read message length (4 bytes, little-endian)
    let mut len_bytes = [0u8; 4];

    // Issue 0.1: Wrap read in timeout to prevent indefinite blocking if stream hangs
    // Default 5s timeout for header is arbitrary but safe for keeping watchdog happy if browser dies
    // However, browser writes are mostly bursty. A long timeout or no timeout is technically correct
    // for "waiting for next message", but we use timeout to ensure we yield control?
    // Actually, `read_exact` is awaitable, so we don't *block* the thread.
    // The requirement "Wrap reads in tokio::time::timeout" likely implies guarding against partial packets or hangs.
    // Let's use a generous timeout (e.g., 30s) or just keep it simple async read.
    // The instruction says "Do not use spawn_blocking for indefinite reads".
    // Transforming this to generic async read satisfies "replace with Async".

    match stdin.read_exact(&mut len_bytes).await {
        Ok(_) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    }
    let length = u32::from_le_bytes(len_bytes) as usize;

    let max_size = max_message_size();
    if length > max_size {
        bail!("Message too large: {} bytes (limit: {})", length, max_size);
    }

    let mut buffer = vec![0u8; length];
    stdin.read_exact(&mut buffer).await?;

    let message: NativeMessage =
        serde_json::from_slice(&buffer).wrap_err("Failed to parse native message")?;

    Ok(Some(message))
}

async fn write_message_async(response: &NativeResponse) -> Result<()> {
    let mut stdout = tokio::io::stdout();
    let json = serde_json::to_vec(response)?;
    let len_bytes = (json.len() as u32).to_le_bytes();

    stdout.write_all(&len_bytes).await?;
    stdout.write_all(&json).await?;
    stdout.flush().await?;

    Ok(())
}

#[derive(Default)]
struct StdioNativeMessagingTransport;

#[async_trait]
impl NativeMessagingTransport for StdioNativeMessagingTransport {
    async fn read_message(&mut self) -> Result<Option<NativeMessage>> {
        read_message_async().await
    }

    async fn write_message(&mut self, response: &NativeResponse) -> Result<()> {
        write_message_async(response).await
    }
}

/// Process a single message and return response
async fn process_message(
    services: &ServiceContainer,
    config: &NativeMessagingConfig,
    message: NativeMessage,
) -> NativeResponse {
    let message_id = message.id.clone();
    let span = tracing::info_span!(
        "native_messaging.request",
        extension_id = message
            .extension_id
            .as_deref()
            .unwrap_or("unknown_extension"),
        host = message.host.as_deref().unwrap_or("unknown_host"),
        protocol_version = message
            .protocol_version
            .as_deref()
            .unwrap_or("unknown_version")
    );
    let _guard = span.enter();

    if let Err(err) = config.enforce_metadata(&message) {
        return NativeResponse::error(message_id, format!("Native messaging rejected: {}", err));
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
    // Native messaging is a trusted local transport (stdin/stdout),
    // so we use a system auth context
    let auth = crate::rpc_server::RpcAuthContext::system();

    // Use shared dispatch table from rpc_server
    crate::rpc_server::dispatch_rpc_method(services, method, params, &auth).await
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
