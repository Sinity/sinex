//! NATS configuration

use figment::{
    providers::{Env, Format, Toml},
    Figment,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// NATS connection configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NatsConfig {
    /// NATS server URLs
    #[serde(default = "default_servers")]
    pub servers: Vec<String>,

    /// Client name
    #[serde(default = "default_client_name")]
    pub client_name: String,

    /// Connection timeout
    #[serde(with = "humantime_serde", default = "default_connection_timeout")]
    pub connection_timeout: Duration,

    /// Request timeout
    #[serde(with = "humantime_serde", default = "default_request_timeout")]
    pub request_timeout: Duration,

    /// Maximum reconnect attempts
    #[serde(default = "default_max_reconnects")]
    pub max_reconnects: usize,

    /// Reconnect delay
    #[serde(with = "humantime_serde", default = "default_reconnect_delay")]
    pub reconnect_delay: Duration,

    /// Enable TLS
    #[serde(default)]
    pub tls_enabled: bool,

    /// TLS configuration
    #[serde(default)]
    pub tls: Option<TlsConfig>,

    /// Authentication configuration
    #[serde(default)]
    pub auth: Option<AuthConfig>,

    /// JetStream configuration
    #[serde(default)]
    pub jetstream: JetStreamConfig,
}

/// JetStream configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
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
    pub max_msgs: i64,

    /// Maximum bytes
    #[serde(default = "default_max_bytes")]
    pub max_bytes: i64,

    /// Number of replicas
    #[serde(default = "default_replicas")]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsConfig {
    /// CA certificate path
    pub ca_cert: Option<String>,

    /// Client certificate path
    pub client_cert: Option<String>,

    /// Client key path
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = NatsConfig::default();
        assert_eq!(config.servers, vec!["nats://localhost:4222"]);
        assert!(config.jetstream.enabled);
    }

    #[test]
    fn test_test_config() {
        let config = NatsConfig::test();
        assert_eq!(config.client_name, "sinex-test");
    }
}
