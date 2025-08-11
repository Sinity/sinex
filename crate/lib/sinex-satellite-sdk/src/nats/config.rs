//! NATS configuration

use figment::{
    providers::{Env, Format, Toml},
    Figment,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use validator::{Validate, ValidationError};

/// NATS connection configuration
#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct NatsConfig {
    /// NATS server URLs
    #[serde(default = "default_servers")]
    #[validate(length(min = 1, message = "At least one NATS server URL must be specified"))]
    #[validate(custom(function = "validate_nats_urls", message = "Invalid NATS server URLs"))]
    pub servers: Vec<String>,

    /// Client name
    #[serde(default = "default_client_name")]
    #[validate(length(
        min = 1,
        max = 100,
        message = "Client name must be between 1 and 100 characters"
    ))]
    pub client_name: String,

    /// Connection timeout
    #[serde(with = "humantime_serde", default = "default_connection_timeout")]
    pub connection_timeout: Duration,

    /// Request timeout
    #[serde(with = "humantime_serde", default = "default_request_timeout")]
    pub request_timeout: Duration,

    /// Maximum reconnect attempts
    #[serde(default = "default_max_reconnects")]
    #[validate(range(
        min = 1,
        max = 1000,
        message = "Max reconnects must be between 1 and 1000"
    ))]
    pub max_reconnects: usize,

    /// Reconnect delay
    #[serde(with = "humantime_serde", default = "default_reconnect_delay")]
    pub reconnect_delay: Duration,

    /// Enable TLS
    #[serde(default)]
    pub tls_enabled: bool,

    /// TLS configuration
    #[serde(default)]
    #[validate(nested)]
    pub tls: Option<TlsConfig>,

    /// Authentication configuration
    #[serde(default)]
    pub auth: Option<AuthConfig>,

    /// JetStream configuration
    #[serde(default)]
    #[validate(nested)]
    pub jetstream: JetStreamConfig,
}

/// JetStream configuration
#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct JetStreamConfig {
    /// Enable JetStream
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// JetStream domain
    pub domain: Option<String>,

    /// JetStream API prefix
    #[serde(default = "default_api_prefix")]
    pub api_prefix: String,

    /// Default stream configuration
    #[serde(default)]
    pub default_stream: StreamDefaults,

    /// Default consumer configuration
    #[serde(default)]
    pub default_consumer: ConsumerDefaults,
}

/// Default stream configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamDefaults {
    /// Maximum message age
    #[serde(with = "humantime_serde", default = "default_max_age")]
    pub max_age: Duration,

    /// Maximum number of messages
    #[serde(default = "default_max_msgs")]
    #[validate(range(min = -1, message = "Max messages must be -1 (unlimited) or positive"))]
    pub max_msgs: i64,

    /// Maximum bytes
    #[serde(default = "default_max_bytes")]
    #[validate(range(min = -1, message = "Max bytes must be -1 (unlimited) or positive"))]
    pub max_bytes: i64,

    /// Number of replicas
    #[serde(default = "default_replicas")]
    #[validate(range(min = 1, max = 5, message = "Replicas must be between 1 and 5"))]
    pub replicas: usize,

    /// Retention policy
    #[serde(default = "default_retention")]
    pub retention: RetentionPolicy,

    /// Discard policy
    #[serde(default = "default_discard")]
    pub discard: DiscardPolicy,
}

/// Default consumer configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerDefaults {
    /// Acknowledgment timeout
    #[serde(with = "humantime_serde", default = "default_ack_wait")]
    pub ack_wait: Duration,

    /// Maximum deliver attempts
    #[serde(default = "default_max_deliver")]
    pub max_deliver: i64,

    /// Maximum acknowledgment pending
    #[serde(default = "default_max_ack_pending")]
    pub max_ack_pending: i64,

    /// Replay policy
    #[serde(default = "default_replay")]
    pub replay: ReplayPolicy,
}

/// TLS configuration
#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct TlsConfig {
    /// CA certificate path
    #[validate(custom(
        function = "validate_optional_cert_path",
        message = "Invalid CA certificate path"
    ))]
    pub ca_cert: Option<String>,

    /// Client certificate path
    #[validate(custom(
        function = "validate_optional_cert_path",
        message = "Invalid client certificate path"
    ))]
    pub client_cert: Option<String>,

    /// Client key path
    #[validate(custom(
        function = "validate_optional_cert_path",
        message = "Invalid client key path"
    ))]
    pub client_key: Option<String>,
}

/// Authentication configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum AuthConfig {
    /// Username/password authentication
    UserPassword { username: String, password: String },

    /// Token authentication
    Token { token: String },

    /// NKey authentication
    NKey { seed: String },

    /// JWT authentication
    Jwt { jwt: String, seed: String },
}

/// Stream retention policy
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
pub enum RetentionPolicy {
    #[default]
    Limits,
    Interest,
    WorkQueue,
}

/// Stream discard policy
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
pub enum DiscardPolicy {
    #[default]
    Old,
    New,
}

/// Consumer replay policy
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
pub enum ReplayPolicy {
    #[default]
    Instant,
    Original,
}

impl Default for NatsConfig {
    fn default() -> Self {
        Self {
            servers: default_servers(),
            client_name: default_client_name(),
            connection_timeout: default_connection_timeout(),
            request_timeout: default_request_timeout(),
            max_reconnects: default_max_reconnects(),
            reconnect_delay: default_reconnect_delay(),
            tls_enabled: false,
            tls: None,
            auth: None,
            jetstream: JetStreamConfig::default(),
        }
    }
}

impl Default for JetStreamConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            domain: None,
            api_prefix: default_api_prefix(),
            default_stream: StreamDefaults::default(),
            default_consumer: ConsumerDefaults::default(),
        }
    }
}

impl Default for StreamDefaults {
    fn default() -> Self {
        Self {
            max_age: default_max_age(),
            max_msgs: default_max_msgs(),
            max_bytes: default_max_bytes(),
            replicas: default_replicas(),
            retention: RetentionPolicy::default(),
            discard: DiscardPolicy::default(),
        }
    }
}

impl Default for ConsumerDefaults {
    fn default() -> Self {
        Self {
            ack_wait: default_ack_wait(),
            max_deliver: default_max_deliver(),
            max_ack_pending: default_max_ack_pending(),
            replay: ReplayPolicy::default(),
        }
    }
}

impl NatsConfig {
    /// Load configuration from environment and files
    pub fn from_env() -> Result<Self, figment::Error> {
        Figment::new()
            .merge(Toml::file("sinex.toml").nested())
            .merge(Env::prefixed("SINEX_").split("_"))
            .extract()
    }

    /// Create a test configuration
    pub fn test() -> Self {
        Self {
            servers: vec!["nats://localhost:4222".to_string()],
            client_name: "sinex-test".to_string(),
            ..Default::default()
        }
    }
}

// Default value functions
fn default_servers() -> Vec<String> {
    vec!["nats://localhost:4222".to_string()]
}

fn default_client_name() -> String {
    format!("sinex-{}", std::process::id())
}

fn default_connection_timeout() -> Duration {
    Duration::from_secs(10)
}

fn default_request_timeout() -> Duration {
    Duration::from_secs(30)
}

fn default_max_reconnects() -> usize {
    10
}

fn default_reconnect_delay() -> Duration {
    Duration::from_secs(2)
}

fn default_true() -> bool {
    true
}

fn default_api_prefix() -> String {
    "$JS.API".to_string()
}

fn default_max_age() -> Duration {
    Duration::from_secs(86400 * 7) // 7 days
}

fn default_max_msgs() -> i64 {
    -1 // unlimited
}

fn default_max_bytes() -> i64 {
    -1 // unlimited
}

fn default_replicas() -> usize {
    1
}

fn default_ack_wait() -> Duration {
    Duration::from_secs(30)
}

fn default_max_deliver() -> i64 {
    3
}

fn default_max_ack_pending() -> i64 {
    1000
}

fn default_retention() -> RetentionPolicy {
    RetentionPolicy::Limits
}

fn default_discard() -> DiscardPolicy {
    DiscardPolicy::Old
}

fn default_replay() -> ReplayPolicy {
    ReplayPolicy::Instant
}

// Custom validation functions for NATS configuration

impl NatsConfig {
    /// Validate the configuration and return detailed error messages
    pub fn validate_config(&self) -> Result<(), String> {
        use validator::Validate as ValidateTrait;

        ValidateTrait::validate(self).map_err(|e| format_validation_errors(&e))
    }
}

impl JetStreamConfig {
    /// Validate the configuration
    pub fn validate_config(&self) -> Result<(), String> {
        use validator::Validate as ValidateTrait;

        ValidateTrait::validate(self).map_err(|e| format_validation_errors(&e))
    }
}

impl TlsConfig {
    /// Validate the configuration
    pub fn validate_config(&self) -> Result<(), String> {
        use validator::Validate as ValidateTrait;

        ValidateTrait::validate(self).map_err(|e| format_validation_errors(&e))
    }
}

/// Validate NATS server URLs
fn validate_nats_urls(urls: &[String]) -> Result<(), ValidationError> {
    for url in urls {
        if url.is_empty() {
            return Err(ValidationError::new("empty_url"));
        }

        // Basic NATS URL validation - should start with nats:// or tls://
        if !url.starts_with("nats://")
            && !url.starts_with("tls://")
            && !url.starts_with("ws://")
            && !url.starts_with("wss://")
        {
            return Err(ValidationError::new("invalid_nats_scheme"));
        }

        // Use basic URL parsing to validate structure
        if let Err(_) = url.parse::<url::Url>() {
            return Err(ValidationError::new("invalid_url_format"));
        }
    }
    Ok(())
}

/// Validate optional certificate paths
fn validate_optional_cert_path(path: &Option<String>) -> Result<(), ValidationError> {
    if let Some(p) = path {
        if p.is_empty() {
            return Err(ValidationError::new("empty_cert_path"));
        }

        // Check for path traversal attempts
        if p.contains("../") || p.contains("..\\") {
            return Err(ValidationError::new("path_traversal"));
        }

        // Basic file extension check for certificates
        if !p.ends_with(".pem")
            && !p.ends_with(".crt")
            && !p.ends_with(".cert")
            && !p.ends_with(".key")
        {
            return Err(ValidationError::new("invalid_cert_extension"));
        }
    }
    Ok(())
}

/// Format validation errors into user-friendly messages
fn format_validation_errors(errors: &validator::ValidationErrors) -> String {
    let mut messages = Vec::new();

    for (field, field_errors) in errors.field_errors() {
        for error in field_errors {
            let msg = match &error.code {
                std::borrow::Cow::Borrowed("range") => {
                    let min = error.params.get("min");
                    let max = error.params.get("max");
                    match (min, max) {
                        (Some(min), Some(max)) => {
                            format!("{}: must be between {} and {}", field, min, max)
                        }
                        (Some(min), None) => format!("{}: must be at least {}", field, min),
                        (None, Some(max)) => format!("{}: must be at most {}", field, max),
                        _ => format!("{}: value out of range", field),
                    }
                }
                std::borrow::Cow::Borrowed("length") => {
                    let min = error.params.get("min");
                    let max = error.params.get("max");
                    match (min, max) {
                        (Some(min), Some(max)) => {
                            format!("{}: length must be between {} and {}", field, min, max)
                        }
                        (Some(min), None) => format!("{}: length must be at least {}", field, min),
                        (None, Some(max)) => format!("{}: length must be at most {}", field, max),
                        _ => format!("{}: invalid length", field),
                    }
                }
                code => format!("{}: {}", field, code),
            };
            messages.push(msg);
        }
    }

    messages.join("; ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_test_utils::sinex_test;
    use validator::Validate;

    #[sinex_test]
    fn test_default_config() -> color_eyre::eyre::Result<()> {
        let config = NatsConfig::default();
        assert_eq!(config.servers, vec!["nats://localhost:4222"]);
        assert!(config.jetstream.enabled);
        
        // Should pass validation
        assert!(config.validate().is_ok());
        assert!(config.validate_config().is_ok());
        Ok(())
    }

    #[sinex_test]
    fn test_test_config() -> color_eyre::eyre::Result<()> {
        let config = NatsConfig::test();
        assert_eq!(config.client_name, "sinex-test");
        
        // Should pass validation
        assert!(config.validate().is_ok());
        Ok(())
    }

    #[sinex_test]
    fn test_invalid_nats_config_empty_servers() -> color_eyre::eyre::Result<()> {
        let mut config = NatsConfig::default();
        config.servers = vec![]; // Empty servers list

        let result = config.validate();
        assert!(result.is_err());
        
        let error_msg = config.validate_config().unwrap_err();
        assert!(error_msg.contains("At least one NATS server URL"));
        Ok(())
    }

    #[sinex_test]
    fn test_invalid_nats_config_bad_url() -> color_eyre::eyre::Result<()> {
        let mut config = NatsConfig::default();
        config.servers = vec!["invalid-url".to_string()]; // Invalid URL

        let result = config.validate();
        assert!(result.is_err());
        Ok(())
    }

    #[sinex_test]
    fn test_invalid_nats_config_wrong_scheme() -> color_eyre::eyre::Result<()> {
        let mut config = NatsConfig::default();
        config.servers = vec!["http://localhost:4222".to_string()]; // Wrong scheme

        let result = config.validate();
        assert!(result.is_err());
        Ok(())
    }

    #[sinex_test]
    fn test_invalid_nats_config_empty_client_name() -> color_eyre::eyre::Result<()> {
        let mut config = NatsConfig::default();
        config.client_name = "".to_string(); // Empty name

        let result = config.validate();
        assert!(result.is_err());
        
        let error_msg = config.validate_config().unwrap_err();
        assert!(error_msg.contains("between 1 and 100 characters"));
        Ok(())
    }

    #[sinex_test]
    fn test_invalid_nats_config_client_name_too_long() -> color_eyre::eyre::Result<()> {
        let mut config = NatsConfig::default();
        config.client_name = "a".repeat(200); // Too long

        let result = config.validate();
        assert!(result.is_err());
        Ok(())
    }

    #[sinex_test]
    fn test_invalid_nats_config_max_reconnects_zero() -> color_eyre::eyre::Result<()> {
        let mut config = NatsConfig::default();
        config.max_reconnects = 0; // Invalid - must be at least 1

        let result = config.validate();
        assert!(result.is_err());
        
        let error_msg = config.validate_config().unwrap_err();
        assert!(error_msg.contains("between 1 and 1000"));
        Ok(())
    }

    #[sinex_test]
    fn test_invalid_jetstream_config_invalid_replicas() -> color_eyre::eyre::Result<()> {
        let mut config = NatsConfig::default();
        config.jetstream.default_stream.replicas = 0; // Invalid

        let result = config.validate();
        assert!(result.is_err());
        Ok(())
    }

    #[sinex_test]
    fn test_invalid_jetstream_config_replicas_too_high() -> color_eyre::eyre::Result<()> {
        let mut config = NatsConfig::default();
        config.jetstream.default_stream.replicas = 10; // Too high

        let result = config.validate();
        assert!(result.is_err());
        Ok(())
    }

    #[sinex_test]
    fn test_invalid_tls_config_bad_cert_path() -> color_eyre::eyre::Result<()> {
        let mut config = NatsConfig::default();
        config.tls = Some(TlsConfig {
            ca_cert: Some("../../../etc/passwd".to_string()), // Path traversal
            client_cert: None,
            client_key: None,
        });

        let result = config.validate();
        assert!(result.is_err());
        Ok(())
    }

    #[sinex_test]
    fn test_invalid_tls_config_wrong_extension() -> color_eyre::eyre::Result<()> {
        let mut config = NatsConfig::default();
        config.tls = Some(TlsConfig {
            ca_cert: Some("/path/to/cert.txt".to_string()), // Wrong extension
            client_cert: None,
            client_key: None,
        });

        let result = config.validate();
        assert!(result.is_err());
        Ok(())
    }

    #[sinex_test]
    fn test_valid_tls_config() -> color_eyre::eyre::Result<()> {
        let mut config = NatsConfig::default();
        config.tls = Some(TlsConfig {
            ca_cert: Some("/path/to/ca.pem".to_string()),
            client_cert: Some("/path/to/client.crt".to_string()),
            client_key: Some("/path/to/client.key".to_string()),
        });

        let result = config.validate();
        assert!(result.is_ok());
        Ok(())
    }

    #[sinex_test]
    fn test_nats_config_multiple_validation_errors() -> color_eyre::eyre::Result<()> {
        let config = NatsConfig {
            servers: vec![], // Empty - should fail
            client_name: "".to_string(), // Empty - should fail
            max_reconnects: 0, // Invalid - should fail
            ..Default::default()
        };

        let error_msg = config.validate_config().unwrap_err();
        
        // Should contain multiple specific error messages
        assert!(!error_msg.is_empty());
        assert!(error_msg.contains("At least one NATS server URL"));
        assert!(error_msg.contains("between 1 and 100 characters"));
        assert!(error_msg.contains("between 1 and 1000"));
        Ok(())
    }
}
