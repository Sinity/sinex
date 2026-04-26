//! NATS configuration and connection helpers.

use crate::SinexError;
use async_nats::{Client, ConnectOptions, jetstream};
use bon::Builder;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tracing::info;

pub const NATS_TRAFFIC_CLASS_HEADER: &str = "Sinex-Traffic-Class";

/// Explicit traffic classes for NATS publish paths.
///
/// These classes are intentionally narrow:
/// - `RawEvent`: persisted raw/synthesized event traffic on `events.raw.*`
/// - `Telemetry`: self-observation events that still use the normal raw-event plane
/// - `RawIngestDlq`: operator-facing ingest/material DLQ traffic
/// - `ProcessingFailure`: derived/runtime processing-failure envelopes
/// - `Control`: non-event lifecycle/coordination traffic
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NatsTrafficClass {
    RawEvent,
    Telemetry,
    RawIngestDlq,
    ProcessingFailure,
    Control,
}

impl NatsTrafficClass {
    #[must_use]
    pub const fn as_header_value(self) -> &'static str {
        match self {
            Self::RawEvent => "raw_event",
            Self::Telemetry => "telemetry",
            Self::RawIngestDlq => "raw_ingest_dlq",
            Self::ProcessingFailure => "processing_failure",
            Self::Control => "control",
        }
    }
}

pub fn insert_traffic_class_header(
    headers: &mut async_nats::HeaderMap,
    traffic_class: NatsTrafficClass,
) {
    headers.insert(NATS_TRAFFIC_CLASS_HEADER, traffic_class.as_header_value());
}

/// Configuration for shared NATS transport, TLS, and authentication.
///
/// Deployment-facing configuration should normally come from typed NixOS
/// options, which are then exported into the `SINEX_NATS_*` environment
/// variables consumed here. Direct env/CLI use remains valid for development
/// and ad hoc runs.
#[derive(Debug, Clone, Serialize, Deserialize, Builder, PartialEq, Eq)]
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

    /// Path to a NATS credentials file (JWT + seed).
    ///
    /// This is the preferred deployed auth mode when the NATS deployment
    /// already issues `.creds` bundles.
    pub creds_file: Option<PathBuf>,

    /// Path to an `NKey` seed file.
    ///
    /// Use this only when the deployment expects direct `NKey` auth rather than
    /// credentials bundles.
    pub nkey_seed_file: Option<PathBuf>,

    /// Inline auth token.
    ///
    /// Keep this for direct/manual runs; prefer `token_file` in deployed setups.
    pub token: Option<String>,

    /// Path to a file containing the auth token.
    ///
    /// This is the preferred simple file-backed auth mode for deployed setups
    /// that do not use `.creds` bundles or direct `NKey` auth.
    pub token_file: Option<PathBuf>,
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
            nkey_seed_file: None,
            token: None,
            token_file: None,
        }
    }
}

impl NatsConnectionConfig {
    /// Load configuration from standard environment variables:
    /// - `SINEX_NATS_URL`
    /// - `SINEX_NATS_NAME`
    /// - `SINEX_NATS_REQUIRE_TLS`
    /// - `SINEX_NATS_CA_CERT`, `SINEX_NATS_CLIENT_CERT`, `SINEX_NATS_CLIENT_KEY`
    /// - `SINEX_NATS_CREDS_FILE`, `SINEX_NATS_NKEY_SEED_FILE`
    /// - `SINEX_NATS_TOKEN`, `SINEX_NATS_TOKEN_FILE`
    ///
    /// Deployed systems should usually prefer the file-backed auth variants.
    #[must_use]
    pub fn from_env() -> Self {
        use crate::env;

        let url = env::var_or("SINEX_NATS_URL", "nats://localhost:4222", "NATS connection");
        let name = env::var_optional("SINEX_NATS_NAME", "NATS connection");
        let require_tls = env::bool_or("SINEX_NATS_REQUIRE_TLS", false, "NATS connection");
        let ca_cert = env::path_optional("SINEX_NATS_CA_CERT", "NATS TLS");
        let client_cert = env::path_optional("SINEX_NATS_CLIENT_CERT", "NATS TLS");
        let client_key = env::path_optional("SINEX_NATS_CLIENT_KEY", "NATS TLS");
        let creds_file = env::path_optional("SINEX_NATS_CREDS_FILE", "NATS auth");
        let nkey_seed_file = env::path_optional("SINEX_NATS_NKEY_SEED_FILE", "NATS auth");
        let token = env::var_optional("SINEX_NATS_TOKEN", "NATS auth");
        let token_file = env::path_optional("SINEX_NATS_TOKEN_FILE", "NATS auth");

        Self {
            url,
            name,
            require_tls,
            ca_cert,
            client_cert,
            client_key,
            creds_file,
            nkey_seed_file,
            token,
            token_file,
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
        if self.require_tls && !self.url.starts_with("tls://") && !self.url.starts_with("wss://") {
            return Err(SinexError::configuration(
                "NATS URL must use tls:// or wss:// when require_tls is enabled".to_string(),
            ));
        }
        if self.client_cert.is_some() != self.client_key.is_some() {
            return Err(SinexError::configuration(
                "NATS mutual TLS requires both client_cert and client_key".to_string(),
            ));
        }
        let auth_modes = [
            self.creds_file.is_some(),
            self.nkey_seed_file.is_some(),
            self.token.is_some(),
            self.token_file.is_some(),
        ]
        .into_iter()
        .filter(|present| *present)
        .count();
        if auth_modes > 1 {
            return Err(SinexError::configuration(
                "NATS authentication is ambiguous; configure exactly one of creds_file, nkey_seed_file, token, or token_file".to_string(),
            ));
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

        if let Some(path) = &self.nkey_seed_file {
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

        if let Some(path) = &self.token_file {
            let token = tokio::fs::read_to_string(path).await.map_err(|e| {
                SinexError::configuration(format!(
                    "Failed to read NATS token from {}: {}",
                    path.display(),
                    e
                ))
            })?;
            opts = opts.token(token.trim().to_string());
        }

        Ok(opts)
    }

    /// Connect to the NATS server using this configuration.
    pub async fn connect(&self) -> Result<Client, SinexError> {
        self.validate()?;
        self.ensure_rustls_crypto_provider()?;
        let opts = self.to_options().await?;

        info!(
            "Connecting to NATS at {} (TLS: {}, auth_mode: {})",
            self.url,
            self.require_tls,
            self.auth_mode()
        );

        opts.connect(&self.url).await.map_err(|e| {
            SinexError::network(format!("Failed to connect to NATS at {}: {}", self.url, e))
        })
    }

    fn auth_mode(&self) -> &'static str {
        if self.creds_file.is_some() {
            "creds_file"
        } else if self.nkey_seed_file.is_some() {
            "nkey_seed_file"
        } else if self.token_file.is_some() {
            "token_file"
        } else if self.token.is_some() {
            "token"
        } else {
            "none"
        }
    }

    fn ensure_rustls_crypto_provider(&self) -> Result<(), SinexError> {
        let uses_tls = self.require_tls
            || self.ca_cert.is_some()
            || self.client_cert.is_some()
            || self.client_key.is_some()
            || self.url.starts_with("tls://")
            || self.url.starts_with("wss://");
        if !uses_tls {
            return Ok(());
        }

        if rustls::crypto::CryptoProvider::get_default().is_some() {
            return Ok(());
        }

        match rustls::crypto::aws_lc_rs::default_provider().install_default() {
            Ok(()) => Ok(()),
            Err(_) if rustls::crypto::CryptoProvider::get_default().is_some() => Ok(()),
            Err(_) => Err(SinexError::configuration(
                "Failed to install Rustls crypto provider for TLS-enabled NATS connection"
                    .to_string(),
            )),
        }
    }
}

pub async fn create_or_open_kv_store(
    js: &jetstream::Context,
    config: jetstream::kv::Config,
) -> Result<jetstream::kv::Store, SinexError> {
    let bucket = config.bucket.clone();
    match js.create_key_value(config).await {
        Ok(store) => Ok(store),
        Err(create_err) if kv_bucket_already_exists(&create_err) => {
            js.get_key_value(&bucket).await.map_err(|open_err| {
                SinexError::kv(format!(
                    "Failed to open existing NATS KV bucket {bucket} after create conflict"
                ))
                .with_std_error(&open_err)
            })
        }
        Err(create_err) => Err(
            SinexError::kv(format!("Failed to create NATS KV bucket {bucket}"))
                .with_std_error(&create_err),
        ),
    }
}

fn kv_bucket_already_exists(err: &jetstream::context::CreateKeyValueError) -> bool {
    std::error::Error::source(err)
        .and_then(|source| source.downcast_ref::<jetstream::context::CreateStreamError>())
        .is_some_and(|stream_err| {
            matches!(
                stream_err.kind(),
                jetstream::context::CreateStreamErrorKind::JetStream(js_err)
                    if js_err.error_code() == jetstream::ErrorCode::STREAM_NAME_EXIST
            )
        })
}

#[cfg(test)]
mod tests {
    // Small inline tests are justified here because they exercise private TLS
    // provider installation behavior and private KV error classification directly.
    use super::*;
    use serde_json::json;
    use xtask::sandbox::{EnvGuard, sinex_serial_test, sinex_test};

    #[sinex_test]
    async fn tls_provider_installation_is_idempotent() -> xtask::sandbox::TestResult<()> {
        let cfg = NatsConnectionConfig {
            url: "tls://localhost:4222".to_string(),
            require_tls: true,
            ..Default::default()
        };

        cfg.ensure_rustls_crypto_provider()?;
        cfg.ensure_rustls_crypto_provider()?;
        assert!(rustls::crypto::CryptoProvider::get_default().is_some());
        Ok(())
    }

    #[sinex_test]
    async fn non_tls_config_skips_provider_installation() -> xtask::sandbox::TestResult<()> {
        let cfg = NatsConnectionConfig::default();
        cfg.ensure_rustls_crypto_provider()?;
        Ok(())
    }

    #[sinex_serial_test]
    async fn from_env_parses_require_tls_strictly() -> xtask::sandbox::TestResult<()> {
        let mut env = EnvGuard::new();
        env.set("SINEX_NATS_REQUIRE_TLS", "true");
        assert!(NatsConnectionConfig::from_env().require_tls);

        env.set("SINEX_NATS_REQUIRE_TLS", "tru");
        assert!(
            !NatsConnectionConfig::from_env().require_tls,
            "invalid TLS override must fall back to the default rather than silently enabling TLS"
        );
        Ok(())
    }

    #[sinex_test]
    async fn kv_bucket_already_exists_matches_stream_name_conflict()
    -> xtask::sandbox::TestResult<()> {
        let stream_error = jetstream::context::CreateStreamError::new(
            jetstream::context::CreateStreamErrorKind::JetStream(serde_json::from_value(json!({
                "code": 400,
                "err_code": jetstream::ErrorCode::STREAM_NAME_EXIST.0,
                "description": "stream already exists",
            }))?),
        );
        let kv_error = jetstream::context::CreateKeyValueError::with_source(
            jetstream::context::CreateKeyValueErrorKind::BucketCreate,
            stream_error,
        );

        assert!(kv_bucket_already_exists(&kv_error));
        Ok(())
    }

    #[sinex_test]
    async fn kv_bucket_already_exists_rejects_other_bucket_create_errors()
    -> xtask::sandbox::TestResult<()> {
        let stream_error = jetstream::context::CreateStreamError::new(
            jetstream::context::CreateStreamErrorKind::JetStream(serde_json::from_value(json!({
                "code": 400,
                "err_code": jetstream::ErrorCode::STREAM_INVALID_CONFIG.0,
                "description": "invalid stream configuration",
            }))?),
        );
        let kv_error = jetstream::context::CreateKeyValueError::with_source(
            jetstream::context::CreateKeyValueErrorKind::BucketCreate,
            stream_error,
        );

        assert!(!kv_bucket_already_exists(&kv_error));
        Ok(())
    }
}

/// Standard `JetStream` topology for Sinex ingestion pipelines.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JetStreamTopology {
    pub events_stream: String,
    pub events_subject: String,
    pub confirmations_stream: String,
    pub confirmations_subject: String,
    pub confirmations_prefix: String,
    pub confirmation_retry_stream: String,
    pub confirmation_retry_subject: String,
    pub confirmation_retry_prefix: String,
    pub confirmation_retry_consumer: String,
    pub dlq_stream: String,
    pub dlq_subject: String,
    pub dlq_publish_subject: String,
    pub processing_failures_stream: String,
    pub processing_failures_subject: String,
    pub processing_failures_prefix: String,
    pub invalidation_stream: String,
    pub invalidation_subject: String,
    pub consumer_durable: String,
}

impl JetStreamTopology {
    #[must_use]
    pub fn new(
        env: &crate::environment::SinexEnvironment,
        base_stream: String,
        consumer_durable: String,
        namespace: Option<&str>,
    ) -> Self {
        let confirmations_stream = format!("{base_stream}_CONFIRMATIONS");
        let confirmation_retry_stream = format!("{base_stream}_CONFIRMATION_RETRIES");
        let dlq_stream = format!("{base_stream}_DLQ");
        let processing_failures_stream = format!("{base_stream}_PROCESSING_FAILURES");
        let invalidation_stream = format!("{base_stream}_DERIVED_INVALIDATIONS");
        let namespaced = |subject: &str| env.nats_subject_with_namespace(namespace, subject);
        let confirmations_prefix = format!("{}.", namespaced("events.confirmations"));
        let confirmation_retry_prefix =
            format!("{}.", namespaced("events.confirmation_retries"));
        let processing_failures_prefix =
            format!("{}.", namespaced("events.processing_failures"));

        Self {
            events_stream: base_stream,
            events_subject: namespaced("events.raw.>"),
            confirmations_stream,
            confirmations_subject: namespaced("events.confirmations.>"),
            confirmations_prefix,
            confirmation_retry_stream,
            confirmation_retry_subject: namespaced("events.confirmation_retries.>"),
            confirmation_retry_prefix,
            confirmation_retry_consumer: format!("{consumer_durable}_confirm_retries"),
            dlq_stream,
            dlq_subject: namespaced("events.dlq.>"),
            dlq_publish_subject: namespaced("events.dlq.ingestd"),
            processing_failures_stream,
            processing_failures_subject: namespaced("events.processing_failures.>"),
            processing_failures_prefix,
            invalidation_stream,
            invalidation_subject: namespaced("sinex.derived.invalidation"),
            consumer_durable,
        }
    }
}
