use reqwest::{Client, Identity};
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::{Value, json};
use sinex_primitives::rpc::JsonRpcError;
use std::path::Path;
use std::time::Duration;
use thiserror::Error;
use tokio::fs;

#[derive(Debug, Deserialize)]
struct JsonRpcResponse<T> {
    jsonrpc: String,
    result: Option<T>,
    error: Option<JsonRpcError>,
    id: Option<Value>,
}

#[derive(Error, Debug)]
pub enum ClientError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Request failed: {0}")]
    Request(#[from] reqwest::Error),
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("Gateway error: {0}")]
    Gateway(String),
}

fn format_http_error(status: reqwest::StatusCode, body: Option<&str>) -> String {
    match body.map(str::trim).filter(|body| !body.is_empty()) {
        Some(body) => format!("HTTP Error: {status}: {body}"),
        None => format!("HTTP Error: {status}"),
    }
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
            return Err(ClientError::Gateway(format_http_error(status, body.as_deref())));
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

        // In development, we might use self-signed certs
        if cfg!(debug_assertions) {
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
mod tests {
    use super::format_http_error;

    #[test]
    fn format_http_error_includes_non_empty_body() {
        let message = format_http_error(reqwest::StatusCode::BAD_REQUEST, Some("rpc exploded"));
        assert_eq!(message, "HTTP Error: 400 Bad Request: rpc exploded");
    }

    #[test]
    fn format_http_error_ignores_blank_body() {
        let message = format_http_error(reqwest::StatusCode::UNAUTHORIZED, Some("   "));
        assert_eq!(message, "HTTP Error: 401 Unauthorized");
    }
}
