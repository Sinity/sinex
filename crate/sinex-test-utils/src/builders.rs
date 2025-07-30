//! Consolidated test data builders with fluent interfaces using bon derive macros
//!
//! This module provides builder patterns for creating test data,
//! making tests more readable and reducing boilerplate code.
//! It combines both test-specific builders and re-exports from sinex-events.

use crate::prelude::*;
use bon::Builder;
use chrono::{DateTime, Utc};
use serde_json::{json, Value as JsonValue};
use sinex_core_types::{DbPool, RawEvent};
use sinex_db;
use sinex_events::EventFactory;
use sinex_ulid::Ulid;

// Re-export only necessary production builders from sinex-events
// These are used by tests that need to create events directly
pub(crate) use sinex_events::event_builders::*;

// Additional type aliases for test compatibility
pub(crate) type HyprlandEventType = WindowManagerEventType;

/// Generic event builder that can create any type of event
#[derive(Builder)]
pub(crate) struct GenericEventBuilder {
    source: String,
    event_type: String,
    #[builder(default = serde_json::json!({}))]
    payload: serde_json::Value,
    #[builder(default)]
    timestamp: Option<chrono::DateTime<chrono::Utc>>,
}

impl GenericEventBuilder {
    pub fn build(self) -> sinex_core_types::RawEvent {
        let factory = EventFactory::new(&self.source);
        factory.create_event(&self.event_type, self.payload)
    }

    // Terminal-specific methods
    pub fn command(mut self, cmd: impl Into<String>) -> Self {
        let mut payload = self.payload.unwrap_or_else(|| serde_json::json!({}));
        payload["command"] = serde_json::json!(cmd.into());
        self.payload = Some(payload);
        self
    }

    pub fn success(mut self) -> Self {
        let mut payload = self.payload.unwrap_or_else(|| serde_json::json!({}));
        payload["exit_status"] = serde_json::json!(0);
        self.payload = Some(payload);
        self
    }

    pub fn duration_ms(mut self, ms: u64) -> Self {
        let mut payload = self.payload.unwrap_or_else(|| serde_json::json!({}));
        payload["execution_time_ms"] = serde_json::json!(ms);
        self.payload = Some(payload);
        self
    }

    // Agent-specific methods
    pub fn name(mut self, name: impl Into<String>) -> Self {
        let mut payload = self.payload.unwrap_or_else(|| serde_json::json!({}));
        payload["agent_name"] = serde_json::json!(name.into());
        self.payload = Some(payload);
        self
    }

    pub fn heartbeat(mut self) -> Self {
        self.event_type = "automaton.heartbeat".to_string();
        let mut payload = self.payload.unwrap_or_else(|| serde_json::json!({}));
        payload["status"] = serde_json::json!("running");
        self.payload = Some(payload);
        self
    }

    pub fn startup(mut self) -> Self {
        self.event_type = "automaton.startup".to_string();
        let mut payload = self.payload.unwrap_or_else(|| serde_json::json!({}));
        payload["status"] = serde_json::json!("starting");
        self.payload = Some(payload);
        self
    }

    pub fn error(mut self, error_msg: impl Into<String>) -> Self {
        self.event_type = "automaton.error".to_string();
        let mut payload = self.payload.unwrap_or_else(|| serde_json::json!({}));
        payload["error_message"] = serde_json::json!(error_msg.into());
        payload["status"] = serde_json::json!("error");
        self.payload = Some(payload);
        self
    }

    pub fn uptime_seconds(mut self, seconds: u64) -> Self {
        let mut payload = self.payload.unwrap_or_else(|| serde_json::json!({}));
        payload["uptime_seconds"] = serde_json::json!(seconds);
        self.payload = Some(payload);
        self
    }

    pub fn version(mut self, version: impl Into<String>) -> Self {
        let mut payload = self.payload.unwrap_or_else(|| serde_json::json!({}));
        payload["version"] = serde_json::json!(version.into());
        self.payload = Some(payload);
        self
    }

    pub fn events_processed(mut self, count: u64) -> Self {
        let mut payload = self.payload.unwrap_or_else(|| serde_json::json!({}));
        payload["events_processed_session"] = serde_json::json!(count);
        self.payload = Some(payload);
        self
    }
}

/// Builder for checkpoint test data
#[derive(Debug, Clone, Builder)]
#[builder(on(String, into))]
pub(crate) struct TestCheckpointBuilder {
    processor_name: String,
    consumer_group: Option<String>,
    consumer_name: Option<String>,
    last_processed_id: Option<Ulid>,
    #[builder(default = 0)]
    processed_count: i64,
    state_data: Option<JsonValue>,
    #[builder(default = 1)]
    checkpoint_version: i32,
    checkpoint_data: Option<JsonValue>,
}

impl TestCheckpointBuilder {
    /// Create a new checkpoint builder
    pub fn new(processor_name: &str) -> Self {
        TestCheckpointBuilder::builder()
            .processor_name(processor_name)
            .build()
    }

    /// Set the consumer group
    pub fn with_group(mut self, group: &str) -> Self {
        self.consumer_group = Some(group.to_string());
        self
    }

    /// Set the consumer name
    pub fn with_consumer(mut self, consumer: &str) -> Self {
        self.consumer_name = Some(consumer.to_string());
        self
    }

    /// Set the last processed ID
    pub fn with_last_processed(mut self, id: Ulid) -> Self {
        self.last_processed_id = Some(id);
        self
    }

    /// Set the processed count
    pub fn with_processed_count(mut self, count: i64) -> Self {
        self.processed_count = count;
        self
    }

    /// Set state data
    pub fn with_state(mut self, state: JsonValue) -> Self {
        self.state_data = Some(state);
        self
    }

    /// Set checkpoint version
    pub fn with_version(mut self, version: i32) -> Self {
        self.checkpoint_version = version;
        self
    }

    /// Set checkpoint data
    pub fn with_checkpoint_data(mut self, data: JsonValue) -> Self {
        self.checkpoint_data = Some(data);
        self
    }

    /// Insert the checkpoint
    pub async fn insert(self, pool: &DbPool) -> Result<()> {
        use sinex_core_types::domain::{ConsumerGroup, ConsumerName, ProcessorName};
        use sinex_db::repositories::{CheckpointRepository, Repository};

        let processor_name = ProcessorName::new(&self.processor_name);
        let group = ConsumerGroup::new(
            &self
                .consumer_group
                .unwrap_or_else(|| format!("{}-group", self.processor_name)),
        );
        let consumer = ConsumerName::new(
            &self
                .consumer_name
                .unwrap_or_else(|| format!("{}-consumer", self.processor_name)),
        );

        let checkpoint_repo = CheckpointRepository::new(&pool);
        checkpoint_repo
            .upsert(
                &processor_name,
                &group,
                &consumer,
                self.last_processed_id
                    .map(|id| sinex_core_types::ids::EventId::from_ulid(id)),
                Some(Utc::now()),
                self.checkpoint_data,
                self.state_data,
            )
            .await?;

        Ok(())
    }
}

/// Builder for test scenarios with multiple events
#[derive(Debug, Builder)]
pub(crate) struct TestScenarioBuilder {
    #[builder(default = Vec::new())]
    events: Vec<RawEvent>,
    #[builder(default = Vec::new())]
    checkpoints: Vec<TestCheckpointBuilder>,
    pool: Option<DbPool>,
}

impl TestScenarioBuilder {
    /// Create a new scenario builder
    pub fn new() -> Self {
        TestScenarioBuilder::builder().build()
    }

    /// Add an event to the scenario
    pub fn with_event(mut self, event: RawEvent) -> Self {
        self.events.push(event);
        self
    }

    /// Add multiple events from the same source
    pub fn with_events_from_source(mut self, source: &str, event_type: &str, count: usize) -> Self {
        let factory = EventFactory::new(source);
        for i in 0..count {
            let event = factory.create_event(
                event_type,
                json!({
                    "index": i,
                    "batch": true
                }),
            );
            self.events.push(event);
        }
        self
    }

    /// Add a checkpoint to the scenario
    pub fn with_checkpoint(mut self, checkpoint: TestCheckpointBuilder) -> Self {
        self.checkpoints.push(checkpoint);
        self
    }

    /// Execute the scenario
    pub async fn execute(self, pool: &DbPool) -> Result<Vec<Ulid>> {
        // Insert all events
        let mut event_ids = Vec::new();
        for event in self.events {
            let inserted = sinex_db::insert_event_with_validator(pool, &event, None).await?;
            event_ids.push(inserted.id);
        }

        // Insert all checkpoints
        for checkpoint in self.checkpoints {
            checkpoint.insert(&pool).await?;
        }

        Ok(event_ids)
    }
}

/// Builder for creating database analysis metrics
#[derive(Clone, Debug, Builder)]
#[builder(on(String, into))]
pub(crate) struct DatabaseMetricsBuilder {
    #[builder(default = 0)]
    total_events: u64,
    #[builder(default = 0)]
    unique_sources: u32,
    #[builder(default = 0)]
    unique_event_types: u32,
    #[builder(default = std::collections::HashMap::new())]
    events_by_source: std::collections::HashMap<String, u64>,
    #[builder(default = std::collections::HashMap::new())]
    events_by_type: std::collections::HashMap<String, u64>,
    time_range: Option<(DateTime<Utc>, DateTime<Utc>)>,
}

impl DatabaseMetricsBuilder {
    /// Create a new metrics builder
    pub fn new() -> Self {
        DatabaseMetricsBuilder::builder().build()
    }

    /// Set total event count
    pub fn with_total_events(mut self, count: u64) -> Self {
        self.total_events = count;
        self
    }

    /// Add events by source
    pub fn with_source_count(mut self, source: &str, count: u64) -> Self {
        self.events_by_source.insert(source.to_string(), count);
        self.unique_sources = self.events_by_source.len() as u32;
        self
    }

    /// Add events by type
    pub fn with_type_count(mut self, event_type: &str, count: u64) -> Self {
        self.events_by_type.insert(event_type.to_string(), count);
        self.unique_event_types = self.events_by_type.len() as u32;
        self
    }

    /// Set time range
    pub fn with_time_range(mut self, start: DateTime<Utc>, end: DateTime<Utc>) -> Self {
        self.time_range = Some((start, end));
        self
    }

    /// Build the metrics
    pub fn build(self) -> JsonValue {
        json!({
            "total_events": self.total_events,
            "unique_sources": self.unique_sources,
            "unique_event_types": self.unique_event_types,
            "events_by_source": self.events_by_source,
            "events_by_type": self.events_by_type,
            "time_range": self.time_range.map(|(start, end)| {
                json!({
                    "start": start.to_rfc3339(),
                    "end": end.to_rfc3339()
                })
            })
        })
    }
}
