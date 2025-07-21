//! Enhanced test data builders with fluent interfaces
//!
//! This module provides builder patterns for creating test data,
//! making tests more readable and reducing boilerplate code.

use crate::common::prelude::*;
use crate::common::query_helpers::TestQueries;
use sinex_db::RawEvent;
use sinex_events::EventFactory;
use serde_json::Value as JsonValue;
use sinex_ulid::Ulid;
use chrono::{DateTime, Utc};
use std::collections::HashMap;

/// Fluent builder for test events
#[derive(Debug, Clone)]
pub struct TestEventBuilder {
    source: String,
    event_type: String,
    host: Option<String>,
    payload: JsonValue,
    ts_orig: Option<DateTime<Utc>>,
    ingestor_version: Option<String>,
    payload_schema_id: Option<Ulid>,
    source_event_ids: Option<Vec<Ulid>>,
}

impl TestEventBuilder {
    /// Create a new test event builder
    pub fn new(source: &str, event_type: &str) -> Self {
        Self {
            source: source.to_string(),
            event_type: event_type.to_string(),
            host: None,
            payload: json!({}),
            ts_orig: None,
            ingestor_version: None,
            payload_schema_id: None,
            source_event_ids: None,
        }
    }

    /// Set the payload
    pub fn with_payload(mut self, payload: JsonValue) -> Self {
        self.payload = payload;
        self
    }

    /// Add a field to the payload
    pub fn with_field(mut self, key: &str, value: JsonValue) -> Self {
        if let Some(obj) = self.payload.as_object_mut() {
            obj.insert(key.to_string(), value);
        }
        self
    }

    /// Set the original timestamp
    pub fn with_timestamp(mut self, ts: DateTime<Utc>) -> Self {
        self.ts_orig = Some(ts);
        self
    }

    /// Set the host
    pub fn with_host(mut self, host: &str) -> Self {
        self.host = Some(host.to_string());
        self
    }

    /// Set the ingestor version
    pub fn with_version(mut self, version: &str) -> Self {
        self.ingestor_version = Some(version.to_string());
        self
    }

    /// Set the payload schema ID
    pub fn with_schema(mut self, schema_id: Ulid) -> Self {
        self.payload_schema_id = Some(schema_id);
        self
    }

    /// Set source event IDs
    pub fn with_source_events(mut self, ids: Vec<Ulid>) -> Self {
        self.source_event_ids = Some(ids);
        self
    }

    /// Build the event without inserting
    pub fn build(self) -> RawEvent {
        let mut event = EventFactory::new(&self.source).create_event(&self.event_type, self.payload);
        
        if let Some(host) = self.host {
            event.host = host;
        }
        if let Some(ts) = self.ts_orig {
            event.ts_orig = Some(ts);
        }
        if let Some(version) = self.ingestor_version {
            event.ingestor_version = Some(version);
        }
        if let Some(schema_id) = self.payload_schema_id {
            event.payload_schema_id = Some(schema_id);
        }
        if let Some(ids) = self.source_event_ids {
            event.source_event_ids = Some(ids);
        }
        
        event
    }

    /// Insert the event into the database
    pub async fn insert(self, pool: &DbPool) -> AnyhowResult<RawEvent> {
        let host = self.host.clone()
            .unwrap_or_else(|| gethostname::gethostname().to_string_lossy().to_string());
        
        TestQueries::insert_full_event(
            pool,
            &self.source,
            &self.event_type,
            &host,
            self.payload,
            self.ts_orig,
            self.ingestor_version,
            self.payload_schema_id,
            self.source_event_ids,
        )
        .await
    }
}

/// Builder for checkpoint test data
#[derive(Debug, Clone)]
pub struct TestCheckpointBuilder {
    automaton_name: String,
    consumer_group: Option<String>,
    consumer_name: Option<String>,
    last_processed_id: Option<String>,
    processed_count: i64,
    state_data: Option<JsonValue>,
    checkpoint_version: i32,
    checkpoint_data: Option<JsonValue>,
}

impl TestCheckpointBuilder {
    /// Create a new checkpoint builder
    pub fn new(automaton_name: &str) -> Self {
        Self {
            automaton_name: automaton_name.to_string(),
            consumer_group: None,
            consumer_name: None,
            last_processed_id: None,
            processed_count: 0,
            state_data: None,
            checkpoint_version: 1,
            checkpoint_data: None,
        }
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
    pub fn with_last_processed(mut self, id: &str) -> Self {
        self.last_processed_id = Some(id.to_string());
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
    pub async fn insert(self, pool: &DbPool) -> AnyhowResult<()> {
        use sinex_db::queries::CheckpointQueries;
        
        let group = self.consumer_group
            .unwrap_or_else(|| format!("{}-group", self.automaton_name));
        let consumer = self.consumer_name
            .unwrap_or_else(|| format!("{}-consumer", self.automaton_name));
        
        CheckpointQueries::upsert_checkpoint(
            Ulid::new(),
            self.automaton_name,
            group,
            consumer,
            self.last_processed_id,
            self.processed_count,
            Utc::now(),
            self.state_data,
            self.checkpoint_version,
            self.checkpoint_data,
            Utc::now(),
            Utc::now(),
        )
        .execute(pool)
        .await?;
        
        Ok(())
    }
}

/// Builder for test scenarios with multiple events
#[derive(Debug)]
pub struct TestScenarioBuilder {
    events: Vec<TestEventBuilder>,
    checkpoints: Vec<TestCheckpointBuilder>,
}

impl TestScenarioBuilder {
    /// Create a new scenario builder
    pub fn new() -> Self {
        Self {
            events: Vec::new(),
            checkpoints: Vec::new(),
        }
    }

    /// Add an event to the scenario
    pub fn with_event(mut self, event: TestEventBuilder) -> Self {
        self.events.push(event);
        self
    }

    /// Add multiple events from the same source
    pub fn with_events_from_source(mut self, source: &str, event_type: &str, count: usize) -> Self {
        for i in 0..count {
            let event = TestEventBuilder::new(source, event_type)
                .with_field("index", json!(i))
                .with_field("batch", json!(true));
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
    pub async fn execute(self, pool: &DbPool) -> AnyhowResult<ScenarioResult> {
        let mut event_ids = Vec::new();
        
        // Insert all events
        for event_builder in self.events {
            let event = event_builder.insert(pool).await?;
            event_ids.push(event.id);
        }
        
        // Insert all checkpoints
        for checkpoint_builder in self.checkpoints {
            checkpoint_builder.insert(pool).await?;
        }
        
        let event_count = event_ids.len();
        Ok(ScenarioResult {
            event_ids,
            event_count,
        })
    }
}

/// Result of executing a test scenario
#[derive(Debug)]
pub struct ScenarioResult {
    pub event_ids: Vec<Ulid>,
    pub event_count: usize,
}

/// Builder for batch event operations
pub struct BatchEventBuilder {
    base_source: String,
    base_event_type: String,
    count: usize,
    payload_generator: Box<dyn Fn(usize) -> JsonValue>,
    time_spacing: Option<chrono::Duration>,
    start_time: DateTime<Utc>,
}

impl BatchEventBuilder {
    /// Create a new batch builder
    pub fn new(source: &str, event_type: &str, count: usize) -> Self {
        Self {
            base_source: source.to_string(),
            base_event_type: event_type.to_string(),
            count,
            payload_generator: Box::new(|i| json!({"index": i})),
            time_spacing: None,
            start_time: Utc::now(),
        }
    }

    /// Set custom payload generator
    pub fn with_payload_generator<F>(mut self, f: F) -> Self
    where
        F: Fn(usize) -> JsonValue + 'static,
    {
        self.payload_generator = Box::new(f);
        self
    }

    /// Set time spacing between events
    pub fn with_time_spacing(mut self, spacing: chrono::Duration) -> Self {
        self.time_spacing = Some(spacing);
        self
    }

    /// Set start time for the batch
    pub fn with_start_time(mut self, start: DateTime<Utc>) -> Self {
        self.start_time = start;
        self
    }

    /// Build all events without inserting
    pub fn build(self) -> Vec<RawEvent> {
        (0..self.count)
            .map(|i| {
                let mut builder = TestEventBuilder::new(&self.base_source, &self.base_event_type)
                    .with_payload((self.payload_generator)(i));
                
                if let Some(spacing) = self.time_spacing {
                    let event_time = self.start_time + spacing * i as i32;
                    builder = builder.with_timestamp(event_time);
                }
                
                builder.build()
            })
            .collect()
    }

    /// Insert all events in batch
    pub async fn insert(self, pool: &DbPool) -> AnyhowResult<Vec<RawEvent>> {
        let mut results = Vec::new();
        
        for i in 0..self.count {
            let mut builder = TestEventBuilder::new(&self.base_source, &self.base_event_type)
                .with_payload((self.payload_generator)(i));
            
            if let Some(spacing) = self.time_spacing {
                let event_time = self.start_time + spacing * i as i32;
                builder = builder.with_timestamp(event_time);
            }
            
            let event = builder.insert(pool).await?;
            results.push(event);
        }
        
        Ok(results)
    }
}

/// Common test event patterns
pub struct TestEvents;

impl TestEvents {
    /// Create a filesystem event
    pub fn filesystem(path: &str) -> TestEventBuilder {
        TestEventBuilder::new("fs", "file.created")
            .with_field("path", json!(path))
            .with_field("size", json!(1024))
    }

    /// Create a shell command event
    pub fn shell_command(command: &str) -> TestEventBuilder {
        TestEventBuilder::new("shell", "command.executed")
            .with_field("command", json!(command))
            .with_field("exit_code", json!(0))
            .with_field("duration_ms", json!(100))
    }

    /// Create a clipboard event
    pub fn clipboard(content: &str) -> TestEventBuilder {
        TestEventBuilder::new("clipboard", "content.changed")
            .with_field("content", json!(content))
            .with_field("content_type", json!("text/plain"))
    }

    /// Create an automaton heartbeat
    pub fn heartbeat(automaton_name: &str) -> TestEventBuilder {
        TestEventBuilder::new("sinex", "automaton.heartbeat")
            .with_field("automaton_name", json!(automaton_name))
            .with_field("status", json!("running"))
            .with_field("version", json!("1.0.0"))
    }

    /// Create a test event with minimal fields
    pub fn minimal() -> TestEventBuilder {
        TestEventBuilder::new("test", "test.event")
            .with_field("minimal", json!(true))
    }

    /// Create a test event with large payload
    pub fn large_payload(size_kb: usize) -> TestEventBuilder {
        let data = "x".repeat(size_kb * 1024);
        TestEventBuilder::new("test", "large.payload")
            .with_field("data", json!(data))
            .with_field("size_kb", json!(size_kb))
    }
}