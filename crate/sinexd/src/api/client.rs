use reqwest::{Client, Identity};
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::{Value, json};
use sinex_primitives::rpc::JsonRpcError;
use std::path::Path;
use std::time::Duration;
use tokio::fs;

#[derive(Debug, Deserialize)]
struct JsonRpcResponse<T> {
    jsonrpc: String,
    result: Option<T>,
    error: Option<JsonRpcError>,
    id: Option<Value>,
}

#[derive(Debug)]
pub enum ClientError {
    Io(std::io::Error),
    Request(reqwest::Error),
    Serialization(serde_json::Error),
    Gateway(String),
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(f, "IO error: {error}"),
            Self::Request(error) => write!(f, "Request failed: {error}"),
            Self::Serialization(error) => write!(f, "Serialization error: {error}"),
            Self::Gateway(message) => write!(f, "Gateway error: {message}"),
        }
    }
}

impl std::error::Error for ClientError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Request(error) => Some(error),
            Self::Serialization(error) => Some(error),
            Self::Gateway(_) => None,
        }
    }
}

impl From<std::io::Error> for ClientError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<reqwest::Error> for ClientError {
    fn from(error: reqwest::Error) -> Self {
        Self::Request(error)
    }
}

impl From<serde_json::Error> for ClientError {
    fn from(error: serde_json::Error) -> Self {
        Self::Serialization(error)
    }
}

impl From<ClientError> for sinex_primitives::error::SinexError {
    fn from(err: ClientError) -> Self {
        match err {
            ClientError::Io(ref source) => {
                sinex_primitives::error::SinexError::io("gateway client IO error")
                    .with_source(source)
            }
            ClientError::Request(ref source) => {
                sinex_primitives::error::SinexError::network("gateway client request failed")
                    .with_source(source)
            }
            ClientError::Serialization(ref source) => {
                sinex_primitives::error::SinexError::serialization(
                    "gateway client serialization error",
                )
                .with_source(source)
            }
            ClientError::Gateway(ref msg) => {
                sinex_primitives::error::SinexError::service("gateway error")
                    .with_context("detail", msg)
            }
        }
    }
}

fn format_http_error(status: reqwest::StatusCode, body: Option<&str>) -> String {
    match body.map(str::trim).filter(|body| !body.is_empty()) {
        Some(body) => format!("HTTP Error: {status}: {body}"),
        None => format!("HTTP Error: {status}"),
    }
}

fn should_accept_invalid_certs(base_url: &str) -> bool {
    let Ok(url) = reqwest::Url::parse(base_url) else {
        return false;
    };

    let Some(host) = url.host_str() else {
        return false;
    };
    let normalized_host = host.trim_start_matches('[').trim_end_matches(']');

    normalized_host.eq_ignore_ascii_case("localhost")
        || normalized_host == "127.0.0.1"
        || normalized_host == "::1"
}

/// A client for the Sinex Gateway JSON-RPC API.
///
/// Handles:
/// - mTLS authentication
/// - Request serialization
/// - Response parsing
#[derive(Clone)]
pub struct GatewayClient {
    base_url: String,
    inner: Client,
}

impl GatewayClient {
    /// Create a new builder for a `GatewayClient`.
    #[must_use]
    pub fn builder() -> GatewayClientBuilder {
        GatewayClientBuilder::default()
    }

    /// Send a JSON-RPC request to the gateway.
    pub async fn request<P: serde::Serialize, R: DeserializeOwned>(
        &self,
        method: &str,
        params: P,
    ) -> std::result::Result<R, ClientError> {
        let payload = json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": uuid::Uuid::new_v4().to_string(),
        });

        let response = self
            .inner
            .post(&self.base_url)
            .json(&payload)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = match response.text().await {
                Ok(body) => Some(body),
                Err(error) => Some(format!("<failed to read error body: {error}>")),
            };
            return Err(ClientError::Gateway(format_http_error(
                status,
                body.as_deref(),
            )));
        }

        let rpc_response: JsonRpcResponse<R> = response.json().await?;

        if rpc_response.jsonrpc != "2.0" {
            return Err(ClientError::Gateway(format!(
                "Invalid JSON-RPC version: {}",
                rpc_response.jsonrpc
            )));
        }

        if rpc_response.id.is_none() {
            return Err(ClientError::Gateway(
                "JSON-RPC response missing id".to_string(),
            ));
        }

        if let Some(error) = rpc_response.error {
            let data_suffix = error
                .data
                .map(|data| format!(" (data: {data})"))
                .unwrap_or_default();
            return Err(ClientError::Gateway(format!(
                "RPC Error {}: {}{}",
                error.code, error.message, data_suffix
            )));
        }

        rpc_response
            .result
            .ok_or_else(|| ClientError::Gateway("Empty response result".to_string()))
    }
}

pub struct GatewayClientBuilder {
    base_url: String,
    timeout: Duration,
    identity: Option<Identity>,
    root_cert: Option<reqwest::Certificate>,
}

impl Default for GatewayClientBuilder {
    fn default() -> Self {
        Self {
            base_url: "https://localhost:3000".to_string(),
            timeout: Duration::from_secs(30),
            identity: None,
            root_cert: None,
        }
    }
}

impl GatewayClientBuilder {
    pub fn base_url(mut self, url: impl Into<String>) -> Self {
        self.base_url = url.into();
        self
    }

    #[must_use]
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Enable mTLS using a raw PKCS#12 or PEM identity.
    #[must_use]
    pub fn identity(mut self, identity: Identity) -> Self {
        self.identity = Some(identity);
        self
    }

    /// Load mTLS identity from a PEM file containing both the certificate and key.
    pub async fn load_pem_identity(
        mut self,
        pem_path: impl AsRef<Path>,
    ) -> std::result::Result<Self, ClientError> {
        let pem = fs::read(pem_path).await?;
        self.identity = Some(Identity::from_pem(&pem)?);
        Ok(self)
    }

    /// Add a trusted root certificate (PEM format).
    pub async fn load_root_cert(
        mut self,
        cert_path: impl AsRef<Path>,
    ) -> std::result::Result<Self, ClientError> {
        let pem = fs::read(cert_path).await?;
        self.root_cert = Some(reqwest::Certificate::from_pem(&pem)?);
        Ok(self)
    }

    pub fn build(self) -> std::result::Result<GatewayClient, ClientError> {
        let mut builder = Client::builder().timeout(self.timeout).use_rustls_tls();

        if let Some(identity) = self.identity {
            builder = builder.identity(identity);
        }

        if let Some(root_cert) = self.root_cert {
            builder = builder.add_root_certificate(root_cert);
        }

        if should_accept_invalid_certs(&self.base_url) {
            builder = builder.danger_accept_invalid_certs(true);
        }

        let inner = builder.build()?;

        Ok(GatewayClient {
            base_url: self.base_url,
            inner,
        })
    }
}

#[cfg(test)]
#[path = "client_test.rs"]
mod tests;
