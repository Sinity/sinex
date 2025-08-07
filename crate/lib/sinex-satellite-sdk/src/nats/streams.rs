//! NATS JetStream stream management for Sinex

use super::{
    config::{DiscardPolicy, RetentionPolicy, StreamDefaults},
    error::Result,
    jetstream::JetStream,
};
use async_nats::jetstream;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, info};

/// Stream configuration for Sinex events
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamConfig {
    /// Stream name
    pub name: String,

    /// Subject patterns this stream captures
    pub subjects: Vec<String>,

    /// Stream description
    pub description: Option<String>,

    /// Maximum message age (0 = unlimited)
    #[serde(with = "humantime_serde")]
    pub max_age: std::time::Duration,

    /// Maximum number of messages (0 = unlimited)
    pub max_msgs: i64,

    /// Maximum total size in bytes (0 = unlimited)
    pub max_bytes: i64,

    /// Number of replicas
    pub replicas: usize,

    /// Retention policy
    pub retention: RetentionPolicy,

    /// Discard policy when limits are reached
    pub discard: DiscardPolicy,
}

impl StreamConfig {
    /// Create a stream config for raw events
    pub fn raw_events() -> Self {
        Self {
            name: "SINEX_RAW_EVENTS".to_string(),
            subjects: vec!["sinex.events.raw.>".to_string()],
            description: Some("Raw event stream for all Sinex events".to_string()),
            max_age: std::time::Duration::from_secs(86400 * 30), // 30 days
            max_msgs: 0,                                         // unlimited
            max_bytes: 0,                                        // unlimited
            replicas: 3,
            retention: RetentionPolicy::Limits,
            discard: DiscardPolicy::Old,
        }
    }

    /// Create a stream config for processed events
    pub fn processed_events() -> Self {
        Self {
            name: "SINEX_PROCESSED_EVENTS".to_string(),
            subjects: vec!["sinex.events.processed.>".to_string()],
            description: Some("Processed event stream for canonicalized events".to_string()),
            max_age: std::time::Duration::from_secs(86400 * 90), // 90 days
            max_msgs: 0,
            max_bytes: 0,
            replicas: 3,
            retention: RetentionPolicy::Limits,
            discard: DiscardPolicy::Old,
        }
    }

    /// Create a stream config for metrics
    pub fn metrics() -> Self {
        Self {
            name: "SINEX_METRICS".to_string(),
            subjects: vec!["sinex.metrics.>".to_string()],
            description: Some("Metrics stream for system telemetry".to_string()),
            max_age: std::time::Duration::from_secs(86400 * 7), // 7 days
            max_msgs: 0,
            max_bytes: 0,
            replicas: 1,
            retention: RetentionPolicy::Limits,
            discard: DiscardPolicy::Old,
        }
    }

    /// Create a stream config for alerts
    pub fn alerts() -> Self {
        Self {
            name: "SINEX_ALERTS".to_string(),
            subjects: vec!["sinex.alerts.>".to_string()],
            description: Some("Alert stream for system notifications".to_string()),
            max_age: std::time::Duration::from_secs(86400 * 30), // 30 days
            max_msgs: 10000,
            max_bytes: 0,
            replicas: 3,
            retention: RetentionPolicy::Limits,
            discard: DiscardPolicy::Old,
        }
    }

    /// Create a stream config for satellite coordination
    pub fn satellite_control() -> Self {
        Self {
            name: "SINEX_SATELLITE_CONTROL".to_string(),
            subjects: vec!["sinex.satellite.control.>".to_string()],
            description: Some("Control stream for satellite coordination".to_string()),
            max_age: std::time::Duration::from_secs(3600), // 1 hour
            max_msgs: 1000,
            max_bytes: 0,
            replicas: 3,
            retention: RetentionPolicy::WorkQueue,
            discard: DiscardPolicy::New,
        }
    }

    /// Convert to NATS JetStream config
    pub fn to_jetstream_config(&self) -> jetstream::stream::Config {
        let retention = match self.retention {
            RetentionPolicy::Limits => jetstream::stream::RetentionPolicy::Limits,
            RetentionPolicy::Interest => jetstream::stream::RetentionPolicy::Interest,
            RetentionPolicy::WorkQueue => jetstream::stream::RetentionPolicy::WorkQueue,
        };

        let discard = match self.discard {
            DiscardPolicy::Old => jetstream::stream::DiscardPolicy::Old,
            DiscardPolicy::New => jetstream::stream::DiscardPolicy::New,
        };

        jetstream::stream::Config {
            name: self.name.clone(),
            subjects: self.subjects.clone(),
            description: self.description.clone(),
            max_age: self.max_age,
            max_messages: self.max_msgs,
            max_bytes: self.max_bytes,
            retention,
            discard,
            num_replicas: self.replicas,
            ..Default::default()
        }
    }
}

impl From<StreamDefaults> for StreamConfig {
    fn from(defaults: StreamDefaults) -> Self {
        Self {
            name: String::new(),
            subjects: Vec::new(),
            description: None,
            max_age: defaults.max_age,
            max_msgs: defaults.max_msgs,
            max_bytes: defaults.max_bytes,
            replicas: defaults.replicas,
            retention: defaults.retention,
            discard: defaults.discard,
        }
    }
}

/// Stream manager for creating and managing JetStream streams
pub struct StreamManager {
    jetstream: JetStream,
    streams: HashMap<String, StreamConfig>,
}

impl StreamManager {
    /// Create a new stream manager
    pub fn new(jetstream: JetStream) -> Self {
        let mut streams = HashMap::new();

        // Register default streams
        streams.insert("raw_events".to_string(), StreamConfig::raw_events());
        streams.insert(
            "processed_events".to_string(),
            StreamConfig::processed_events(),
        );
        streams.insert("metrics".to_string(), StreamConfig::metrics());
        streams.insert("alerts".to_string(), StreamConfig::alerts());
        streams.insert(
            "satellite_control".to_string(),
            StreamConfig::satellite_control(),
        );

        Self { jetstream, streams }
    }

    /// Initialize all configured streams
    pub async fn initialize_streams(&self) -> Result<()> {
        info!("Initializing JetStream streams");

        for (key, config) in &self.streams {
            debug!("Creating stream: {} ({})", config.name, key);

            let js_config = config.to_jetstream_config();
            self.jetstream.get_or_create_stream(js_config).await?;
        }

        info!("Initialized {} streams", self.streams.len());
        Ok(())
    }

    /// Get a stream configuration by key
    pub fn get_stream_config(&self, key: &str) -> Option<&StreamConfig> {
        self.streams.get(key)
    }

    /// Add a custom stream configuration
    pub fn add_stream(&mut self, key: String, config: StreamConfig) {
        self.streams.insert(key, config);
    }

    /// Remove a stream configuration
    pub fn remove_stream(&mut self, key: &str) -> Option<StreamConfig> {
        self.streams.remove(key)
    }

    /// Create a subject for a specific event source and type
    pub fn event_subject(source: &str, event_type: &str) -> String {
        format!("sinex.events.raw.{}.{}", source, event_type)
    }

    /// Create a subject for processed events
    pub fn processed_subject(source: &str, event_type: &str) -> String {
        format!("sinex.events.processed.{}.{}", source, event_type)
    }

    /// Create a subject for metrics
    pub fn metrics_subject(component: &str, metric_type: &str) -> String {
        format!("sinex.metrics.{}.{}", component, metric_type)
    }

    /// Create a subject for alerts
    pub fn alert_subject(severity: &str, component: &str) -> String {
        format!("sinex.alerts.{}.{}", severity, component)
    }

    /// Create a subject for satellite control
    pub fn control_subject(satellite: &str, command: &str) -> String {
        format!("sinex.satellite.control.{}.{}", satellite, command)
    }

    /// List all configured streams
    pub fn list_configured_streams(&self) -> Vec<(&str, &StreamConfig)> {
        self.streams.iter().map(|(k, v)| (k.as_str(), v)).collect()
    }

    /// Verify all streams are created in JetStream
    pub async fn verify_streams(&self) -> Result<HashMap<String, bool>> {
        let mut results = HashMap::new();

        for (key, config) in &self.streams {
            let exists = self.jetstream.get_stream(&config.name).await.is_ok();
            results.insert(key.clone(), exists);
        }

        Ok(results)
    }

    /// Get stream statistics
    pub async fn get_stream_stats(&self) -> Result<HashMap<String, jetstream::stream::Info>> {
        let mut stats = HashMap::new();

        for (key, config) in &self.streams {
            if let Ok(info) = self.jetstream.stream_info(&config.name).await {
                stats.insert(key.clone(), info);
            }
        }

        Ok(stats)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[sinex_test]
    fn test_stream_configs() {
        let raw = StreamConfig::raw_events();
        assert_eq!(raw.name, "SINEX_RAW_EVENTS");
        assert_eq!(raw.subjects, vec!["sinex.events.raw.>"]);

        let processed = StreamConfig::processed_events();
        assert_eq!(processed.name, "SINEX_PROCESSED_EVENTS");

        let metrics = StreamConfig::metrics();
        assert_eq!(metrics.name, "SINEX_METRICS");
    }

    #[sinex_test]
    fn test_subject_creation() {
        assert_eq!(
            StreamManager::event_subject("filesystem", "created"),
            "sinex.events.raw.filesystem.created"
        );

        assert_eq!(
            StreamManager::processed_subject("terminal", "command"),
            "sinex.events.processed.terminal.command"
        );

        assert_eq!(
            StreamManager::metrics_subject("ingestd", "throughput"),
            "sinex.metrics.ingestd.throughput"
        );

        assert_eq!(
            StreamManager::alert_subject("critical", "database"),
            "sinex.alerts.critical.database"
        );

        assert_eq!(
            StreamManager::control_subject("fs-watcher", "restart"),
            "sinex.satellite.control.fs-watcher.restart"
        );
    }
}
