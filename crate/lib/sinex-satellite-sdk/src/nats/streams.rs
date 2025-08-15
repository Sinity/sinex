//! NATS JetStream stream management for Sinex

use super::{
    config::{DiscardPolicy, RetentionPolicy, StreamDefaults},
    error::Result,
    jetstream::JetStream,
};
use async_nats::jetstream;
use serde::{Deserialize, Serialize};
use sinex_core::{
    domain::{NatsSubject, ServiceName},
    environment::environment,
    EventSource, EventType,
};
use std::collections::HashMap;
use tracing::{debug, info};

/// Stream configuration for Sinex events
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamConfig {
    /// Stream name
    pub name: String,

    /// Subject patterns this stream captures
    pub subjects: Vec<NatsSubject>,

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
        let env = environment();
        Self {
            name: env.nats_stream_name("SINEX_RAW_EVENTS"),
            subjects: vec![NatsSubject::from(env.nats_subject("sinex.events.raw.>"))],
            description: Some(format!(
                "Raw event stream for all Sinex events ({})",
                env.name()
            )),
            max_age: std::time::Duration::from_secs(86400 * 30), // 30 days
            max_msgs: 0,                                         // unlimited
            max_bytes: 0,                                        // unlimited
            replicas: if env.is_dev() { 1 } else { 3 },          // single replica for dev
            retention: RetentionPolicy::Limits,
            discard: DiscardPolicy::Old,
        }
    }

    /// Create a stream config for processed events
    pub fn processed_events() -> Self {
        let env = environment();
        Self {
            name: env.nats_stream_name("SINEX_PROCESSED_EVENTS"),
            subjects: vec![NatsSubject::from(
                env.nats_subject("sinex.events.processed.>"),
            )],
            description: Some(format!(
                "Processed event stream for canonicalized events ({})",
                env.name()
            )),
            max_age: std::time::Duration::from_secs(86400 * 90), // 90 days
            max_msgs: 0,
            max_bytes: 0,
            replicas: if env.is_dev() { 1 } else { 3 },
            retention: RetentionPolicy::Limits,
            discard: DiscardPolicy::Old,
        }
    }

    /// Create a stream config for metrics
    pub fn metrics() -> Self {
        let env = environment();
        Self {
            name: env.nats_stream_name("SINEX_METRICS"),
            subjects: vec![NatsSubject::from(env.nats_subject("sinex.metrics.>"))],
            description: Some(format!(
                "Metrics stream for system telemetry ({})",
                env.name()
            )),
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
        let env = environment();
        Self {
            name: env.nats_stream_name("SINEX_ALERTS"),
            subjects: vec![NatsSubject::from(env.nats_subject("sinex.alerts.>"))],
            description: Some(format!(
                "Alert stream for system notifications ({})",
                env.name()
            )),
            max_age: std::time::Duration::from_secs(86400 * 30), // 30 days
            max_msgs: 10000,
            max_bytes: 0,
            replicas: if env.is_dev() { 1 } else { 3 },
            retention: RetentionPolicy::Limits,
            discard: DiscardPolicy::Old,
        }
    }

    /// Create a stream config for satellite coordination
    pub fn satellite_control() -> Self {
        let env = environment();
        Self {
            name: env.nats_stream_name("SINEX_SATELLITE_CONTROL"),
            subjects: vec![NatsSubject::from(
                env.nats_subject("sinex.satellite.control.>"),
            )],
            description: Some(format!(
                "Control stream for satellite coordination ({})",
                env.name()
            )),
            max_age: std::time::Duration::from_secs(3600), // 1 hour
            max_msgs: 1000,
            max_bytes: 0,
            replicas: if env.is_dev() { 1 } else { 3 },
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
            subjects: self
                .subjects
                .iter()
                .map(|s| s.as_str().to_string())
                .collect(),
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
    pub fn event_subject(source: &EventSource, event_type: &EventType) -> String {
        let env = environment();
        env.nats_subject(&format!(
            "sinex.events.raw.{}.{}",
            source.as_str(),
            event_type.as_str()
        ))
    }

    /// Create a subject for processed events
    pub fn processed_subject(source: &EventSource, event_type: &EventType) -> String {
        let env = environment();
        env.nats_subject(&format!(
            "sinex.events.processed.{}.{}",
            source.as_str(),
            event_type.as_str()
        ))
    }

    /// Create a subject for metrics
    pub fn metrics_subject(component: &ServiceName, metric_type: &str) -> String {
        let env = environment();
        env.nats_subject(&format!(
            "sinex.metrics.{}.{}",
            component.as_str(),
            metric_type
        ))
    }

    /// Create a subject for alerts
    pub fn alert_subject(severity: &str, component: &str) -> String {
        let env = environment();
        env.nats_subject(&format!("sinex.alerts.{}.{}", severity, component))
    }

    /// Create a subject for satellite control
    pub fn control_subject(satellite: &str, command: &str) -> String {
        let env = environment();
        env.nats_subject(&format!(
            "sinex.satellite.control.{}.{}",
            satellite, command
        ))
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
    use sinex_test_utils::sinex_test;

    #[sinex_test]
    fn test_stream_configs() -> color_eyre::eyre::Result<()> {
        let raw = StreamConfig::raw_events();
        assert_eq!(raw.name, "SINEX_RAW_EVENTS");
        assert_eq!(
            raw.subjects,
            vec![NatsSubject::from("sinex.events.raw.>".to_string())]
        );

        let processed = StreamConfig::processed_events();
        assert_eq!(processed.name, "SINEX_PROCESSED_EVENTS");

        let metrics = StreamConfig::metrics();
        assert_eq!(metrics.name, "SINEX_METRICS");
        Ok(())
    }

    #[sinex_test]
    fn test_subject_creation() -> color_eyre::eyre::Result<()> {
        let source = EventSource::from_static("filesystem");
        let event_type = EventType::from_static("created");
        assert_eq!(
            StreamManager::event_subject(&source, &event_type),
            "sinex.events.raw.filesystem.created"
        );

        let source2 = EventSource::from_static("terminal");
        let event_type2 = EventType::from_static("command");
        assert_eq!(
            StreamManager::processed_subject(&source2, &event_type2),
            "sinex.events.processed.terminal.command"
        );

        let service = ServiceName::from_static("ingestd");
        assert_eq!(
            StreamManager::metrics_subject(&service, "throughput"),
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
        Ok(())
    }
}
