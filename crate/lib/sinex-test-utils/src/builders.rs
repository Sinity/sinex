//! Consolidated test data builders with fluent interfaces using bon derive macros
//!
//! This module provides builder patterns for creating test data,
//! making tests more readable and reducing boilerplate code.
//! It combines both test-specific builders and re-exports from sinex-events.

use crate::prelude::*;
use bon::Builder;
use chrono::{DateTime, Utc};
use serde_json::{json, Value as JsonValue};
use sinex_core::db::{repositories::DbPoolExt, DbPool};

// Test data builders using bon derive macros

/// Builder for checkpoint test data - uses bon::Builder for fluent interface
#[derive(Debug, Clone, Builder)]
pub(crate) struct TestCheckpointBuilder {
    processor_name: String,
    #[builder(default)]
    consumer_group: Option<String>,
    #[builder(default)]
    consumer_name: Option<String>,
    #[builder(default)]
    last_processed_id: Option<Id<RawEvent>>,
    #[builder(default = 0)]
    processed_count: i64,
    #[builder(default)]
    state_data: Option<JsonValue>,
    #[builder(default = 1)]
    checkpoint_version: i32,
    #[builder(default)]
    checkpoint_data: Option<JsonValue>,
}

impl TestCheckpointBuilder {
    /// Create a new checkpoint builder using bon::Builder
    pub fn new(processor_name: &str) -> TestCheckpointBuilderBuilder<(String,)> {
        Self::builder().processor_name(processor_name.to_string())
    }

    /// Insert the checkpoint
    pub async fn insert(self, pool: &DbPool) -> Result<()> {
        use sinex_core::db::repositories::*;
        use sinex_core::types::domain::*;

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

        pool.checkpoints()
            .upsert(
                &processor_name,
                &group,
                &consumer,
                self.last_processed_id,
                Some(Utc::now()),
                self.checkpoint_data,
                self.state_data,
            )
            .await?;

        Ok(())
    }
}

/// Builder for test scenarios with multiple events - uses bon::Builder
#[derive(Debug, Builder)]
pub(crate) struct TestScenarioBuilder {
    #[builder(default = Vec::new())]
    events: Vec<RawEvent>,
    #[builder(default = Vec::new())]
    checkpoints: Vec<TestCheckpointBuilder>,
    #[builder(default)]
    pool: Option<DbPool>,
}

impl TestScenarioBuilder {
    /// Create a new scenario builder using bon::Builder
    pub fn new() -> TestScenarioBuilderBuilder<()> {
        Self::builder()
    }
    /// Add multiple events from the same source
    pub fn with_events_from_source(
        mut self,
        source: &EventSource,
        event_type: &EventType,
        count: usize,
    ) -> Self {
        for i in 0..count {
            let event = RawEvent::schemaless(
                source.clone(),
                event_type.clone(),
                json!({
                    "index": i,
                    "batch": true
                }),
            );
            self.events.push(event);
        }
        self
    }

    /// Execute the scenario
    pub async fn execute(self, pool: &DbPool) -> Result<Vec<Ulid>> {
        // Insert all events
        let mut event_ids = Vec::new();
        for event in self.events {
            let inserted = pool.events().insert(event).await?;
            event_ids.push(inserted.id.expect("Inserted event must have ID").into());
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

    /// Add events by source (updates unique_sources automatically)
    pub fn with_source_count(mut self, source: &EventSource, count: u64) -> Self {
        self.events_by_source
            .insert(source.as_str().to_string(), count);
        self.unique_sources = self.events_by_source.len() as u32;
        self
    }

    /// Add events by type (updates unique_event_types automatically)  
    pub fn with_type_count(mut self, event_type: &EventType, count: u64) -> Self {
        self.events_by_type
            .insert(event_type.as_str().to_string(), count);
        self.unique_event_types = self.events_by_type.len() as u32;
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
