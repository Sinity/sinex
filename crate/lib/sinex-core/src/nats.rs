//! NATS configuration and connection helpers.

use crate::SinexError;
use async_nats::{Client, ConnectOptions};
use bon::Builder;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tracing::info;

/// Configuration for NATS connections including TLS and Auth.
#[derive(Debug, Clone, Serialize, Deserialize, Builder, PartialEq)]
pub struct NatsConnectionConfig {
    /// NATS server URL (e.g. `nats://localhost:4222` or `tls://demo.nats.io:4443`)
    #[builder(default = String::from("nats://localhost:4222"))]
    pub url: String,

    /// Logical name for this connection (appears in NATS monitoring)
    pub name: Option<String>,

    /// Require TLS. If true, connection fails if URL is not tls:// or wss://,
    /// or if the handshake fails.
    #[builder(default)]
    pub require_tls: bool,

    /// Path to root CA certificate (PEM)
    pub ca_cert: Option<PathBuf>,

    /// Path to client certificate (PEM)
    pub client_cert: Option<PathBuf>,

    /// Path to client key (PEM)
    pub client_key: Option<PathBuf>,

    /// Path to NATS credentials file (JWT + Key)
    pub creds_file: Option<PathBuf>,

    /// Path to NKey seed file
    pub nkey_file: Option<PathBuf>,

    /// Auth token
    pub token: Option<String>,
}

impl Default for NatsConnectionConfig {
    fn default() -> Self {
        Self {
            url: "nats://localhost:4222".to_string(),
            name: None,
            require_tls: false,
            ca_cert: None,
            client_cert: None,
            client_key: None,
            creds_file: None,
            nkey_file: None,
            token: None,
        }
    }
}

impl NatsConnectionConfig {
    /// Load configuration from standard environment variables:
    /// - `SINEX_NATS_URL`
    /// - `SINEX_NATS_REQUIRE_TLS`
    /// - `SINEX_NATS_CA_CERT`, `SINEX_NATS_CLIENT_CERT`, `SINEX_NATS_CLIENT_KEY`
    /// - `SINEX_NATS_CREDS`, `SINEX_NATS_NKEY_SEED`
    /// - `SINEX_NATS_TOKEN`
    pub fn from_env() -> Self {
        let url =
            std::env::var("SINEX_NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string());

        let require_tls = env_bool("SINEX_NATS_REQUIRE_TLS");

        let ca_cert = env_path("SINEX_NATS_CA_CERT");
        let client_cert = env_path("SINEX_NATS_CLIENT_CERT");
        let client_key = env_path("SINEX_NATS_CLIENT_KEY");
        let env = crate::environment();
        let creds_file = env_path("SINEX_NATS_CREDS").or_else(|| env.nats_creds_path());
        let nkey_file = env_path("SINEX_NATS_NKEY_SEED");
        let token = std::env::var("SINEX_NATS_TOKEN").ok();

        Self {
            url,
            name: None,
            require_tls,
            ca_cert,
            client_cert,
            client_key,
            creds_file,
            nkey_file,
            token,
        }
    }

    /// Validate configuration compliance.
    /// Checks that if `require_tls` is set, the URL scheme is appropriate.
    pub fn validate(&self) -> Result<(), SinexError> {
        if self.url.trim().is_empty() {
            return Err(SinexError::configuration(
                "NATS URL cannot be empty".to_string(),
            ));
        }
        if self.require_tls {
            if !self.url.starts_with("tls://") && !self.url.starts_with("wss://") {
                return Err(SinexError::configuration(
                    "NATS URL must use tls:// or wss:// when require_tls is enabled".to_string(),
                ));
            }
        }
        Ok(())
    }

    /// Convert to `async_nats::ConnectOptions`.
    pub async fn to_options(&self) -> Result<ConnectOptions, SinexError> {
        let mut opts = ConnectOptions::new();

        if let Some(name) = &self.name {
            opts = opts.name(name);
        }

        if self.require_tls {
            opts = opts.require_tls(true);
        }

        if let Some(path) = &self.ca_cert {
            opts = opts.add_root_certificates(path.clone());
        }

        if let (Some(cert), Some(key)) = (&self.client_cert, &self.client_key) {
            opts = opts.add_client_certificate(cert.clone(), key.clone());
        }

        if let Some(path) = &self.creds_file {
            opts = opts.credentials_file(path).await.map_err(|e| {
                SinexError::configuration(format!(
                    "Failed to load NATS creds from {}: {}",
                    path.display(),
                    e
                ))
            })?;
        }

        if let Some(path) = &self.nkey_file {
            let seed = tokio::fs::read_to_string(path).await.map_err(|e| {
                SinexError::configuration(format!(
                    "Failed to read NKey seed from {}: {}",
                    path.display(),
                    e
                ))
            })?;
            // Trim whitespace/newlines often found in seed files
            opts = opts.nkey(seed.trim().to_string());
        }

        if let Some(token) = &self.token {
            opts = opts.token(token.clone());
        }

        Ok(opts)
    }

    /// Connect to the NATS server using this configuration.
    pub async fn connect(&self) -> Result<Client, SinexError> {
        self.validate()?;
        let opts = self.to_options().await?;

        info!(
            "Connecting to NATS at {} (TLS: {}, Creds: {})",
            self.url,
            self.require_tls,
            self.creds_file.is_some()
        );

        opts.connect(&self.url).await.map_err(|e| {
            SinexError::network(format!("Failed to connect to NATS at {}: {}", self.url, e))
        })
    }
}

fn env_bool(key: &str) -> bool {
    std::env::var(key)
        .map(|v| matches!(v.to_lowercase().as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(false)
}

fn env_path(key: &str) -> Option<PathBuf> {
    std::env::var(key).ok().map(PathBuf::from)
}
