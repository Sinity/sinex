//! Dataset seeding utilities for integration tests
//!
//! Provides structured event seeding for tests with deterministic timestamps
//! and predefined datasets for analytics and search testing.

use crate::sandbox::prelude::*;
use serde_json::{json, Value as JsonValue};
use sinex_primitives::events::payloads::{
    FileCreatedPayload, FileModifiedPayload, KittyCommandExecutedPayload,
};
use sinex_primitives::events::{DynamicPayload, Publishable};
use sinex_primitives::temporal::{Duration, Timestamp};
use sinex_schema::primitives::Ulid;
use std::sync::atomic::{AtomicI64, Ordering};

/// Clock for generating sequential test timestamps
///
/// Ensures events have predictable ordering in tests.
pub struct SeedClock {
    base: Timestamp,
    offset_ms: AtomicI64,
}

impl SeedClock {
    /// Create a new seed clock starting from now - 1 hour
    #[must_use]
    pub fn new() -> Self {
        Self {
            base: Timestamp::now() - Duration::hours(1),
            offset_ms: AtomicI64::new(0),
        }
    }

    /// Create a seed clock starting from a specific time
    #[must_use]
    pub fn from_base(base: Timestamp) -> Self {
        Self {
            base,
            offset_ms: AtomicI64::new(0),
        }
    }

    /// Get current timestamp and advance by given milliseconds
    pub fn tick(&self, advance_ms: i64) -> Timestamp {
        let offset = self.offset_ms.fetch_add(advance_ms, Ordering::SeqCst);
        self.timestamp_at_offset(offset)
    }

    /// Get current timestamp without advancing
    pub fn now(&self) -> Timestamp {
        let offset = self.offset_ms.load(Ordering::SeqCst);
        self.timestamp_at_offset(offset)
    }

    /// Reset to base time
    pub fn reset(&self) {
        self.offset_ms.store(0, Ordering::SeqCst);
    }

    fn timestamp_at_offset(&self, offset: i64) -> Timestamp {
        self.base + Duration::milliseconds(offset)
    }
}

impl Default for SeedClock {
    fn default() -> Self {
        Self::new()
    }
}

/// Specification for a test event
#[derive(Debug, Clone)]
pub struct EventSpec {
    pub source: String,
    pub event_type: String,
    pub payload: JsonValue,
    pub timestamp: Option<Timestamp>,
}

impl EventSpec {
    /// Create a new event spec with raw source/type strings and empty payload.
    pub fn new(source: impl Into<String>, event_type: impl Into<String>) -> Self {
        Self {
            source: source.into(),
            event_type: event_type.into(),
            payload: json!({}),
            timestamp: None,
        }
    }

    /// Create an event spec from a typed payload.
    ///
    /// Captures the correct source, event type, and JSON representation from
    /// the typed payload's `Publishable` impl, ensuring the JSON matches
    /// the payload's schema rather than being a hand-crafted approximation.
    pub fn from_typed(payload: &impl Publishable) -> sinex_primitives::error::Result<Self> {
        Ok(Self {
            source: payload.source().to_string(),
            event_type: payload.event_type().to_string(),
            payload: payload.to_json_value()?,
            timestamp: None,
        })
    }

    /// Set payload
    #[must_use]
    pub fn with_payload(mut self, payload: JsonValue) -> Self {
        self.payload = payload;
        self
    }

    /// Set timestamp
    #[must_use]
    pub fn at(mut self, timestamp: Timestamp) -> Self {
        self.timestamp = Some(timestamp);
        self
    }
}

/// Seed events into the database via the test context
pub async fn seed_events_via_scope(
    ctx: &Sandbox,
    clock: &SeedClock,
    events: Vec<EventSpec>,
) -> TestResult<Vec<Ulid>> {
    let mut ids = Vec::with_capacity(events.len());

    for spec in events {
        let timestamp = spec.timestamp.unwrap_or_else(|| clock.tick(100));

        let payload =
            DynamicPayload::new(spec.source.as_str(), spec.event_type.as_str(), spec.payload);

        let event = ctx.publish_at(payload, timestamp).await?;
        if let Some(id) = event.id {
            ids.push(*id.as_ulid() as Ulid);
        }
    }

    Ok(ids)
}

/// Predefined analytics test dataset
#[derive(Debug, Clone)]
pub struct AnalyticsDataset {
    pub name: String,
    pub events: Vec<EventSpec>,
    pub expected_total: i64,
    pub expected_source_counts: std::collections::HashMap<String, i64>,
    pub expected_event_type_counts: std::collections::HashMap<String, i64>,
    pub expected_command_counts: std::collections::HashMap<String, i64>,
}

impl AnalyticsDataset {
    /// Create minimal semantic dataset for analytics tests.
    ///
    /// Uses typed payloads where available to ensure JSON matches the payload schema.
    /// Shell commands use `KittyCommandExecutedPayload` (source "shell.kitty"),
    /// filesystem events use `FileCreatedPayload`/`FileModifiedPayload` (source "fs-watcher").
    pub fn semantic_min() -> sinex_primitives::error::Result<Self> {
        let events = vec![
            EventSpec::from_typed(&KittyCommandExecutedPayload::test_default("ls"))?,
            EventSpec::from_typed(&KittyCommandExecutedPayload::test_default("git status"))?,
            EventSpec::from_typed(&KittyCommandExecutedPayload::test_default("ls"))?,
            EventSpec::from_typed(&FileCreatedPayload::test_default("/tmp/test.txt"))?,
            EventSpec::from_typed(&FileModifiedPayload::test_default("/tmp/test.txt"))?,
        ];

        let mut expected_source_counts = std::collections::HashMap::new();
        expected_source_counts.insert("shell.kitty".to_string(), 3);
        expected_source_counts.insert("fs-watcher".to_string(), 2);

        let mut expected_event_type_counts = std::collections::HashMap::new();
        expected_event_type_counts.insert("command.executed".to_string(), 3);
        expected_event_type_counts.insert("file.created".to_string(), 1);
        expected_event_type_counts.insert("file.modified".to_string(), 1);

        let mut expected_command_counts = std::collections::HashMap::new();
        expected_command_counts.insert("ls".to_string(), 2);
        expected_command_counts.insert("git status".to_string(), 1);

        Ok(Self {
            name: "analytics-semantic-min".to_string(),
            expected_total: 5,
            events,
            expected_source_counts,
            expected_event_type_counts,
            expected_command_counts,
        })
    }

    /// Create performance dataset with many events
    #[must_use]
    pub fn perf(count: usize) -> Self {
        let mut events = Vec::with_capacity(count);
        for i in 0..count {
            events.push(
                EventSpec::new("shell.bash", "command.executed")
                    .with_payload(json!({"command": format!("cmd-{}", i), "exit_code": 0})),
            );
        }

        let mut expected_source_counts = std::collections::HashMap::new();
        expected_source_counts.insert("shell.bash".to_string(), count as i64);

        let mut expected_event_type_counts = std::collections::HashMap::new();
        expected_event_type_counts.insert("command.executed".to_string(), count as i64);

        Self {
            name: "analytics-perf".to_string(),
            expected_total: count as i64,
            events,
            expected_source_counts,
            expected_event_type_counts,
            expected_command_counts: std::collections::HashMap::new(),
        }
    }
}

/// Predefined query test dataset
#[derive(Debug, Clone)]
pub struct QueryDataset {
    pub name: String,
    pub events: Vec<EventSpec>,
    pub expected_total: usize,
}

impl QueryDataset {
    /// Create minimal semantic dataset for query/search tests.
    ///
    /// Uses typed payloads where available.
    pub fn semantic_min() -> sinex_primitives::error::Result<Self> {
        let events = vec![
            EventSpec::from_typed(&KittyCommandExecutedPayload::test_default("cargo build"))?,
            EventSpec::from_typed(&KittyCommandExecutedPayload::test_default("cargo test"))?,
            EventSpec::from_typed(&FileCreatedPayload::test_default("/project/src/main.rs"))?,
        ];
        Ok(Self {
            name: "query-semantic-min".to_string(),
            expected_total: events.len(),
            events,
        })
    }
}

/// Seed the minimal semantic analytics dataset and return the dataset with expected counts
pub async fn seed_analytics_dataset_semantic_min_via_scope(
    ctx: &Sandbox,
    clock: &SeedClock,
) -> TestResult<AnalyticsDataset> {
    let dataset = AnalyticsDataset::semantic_min()?;
    seed_events_via_scope(ctx, clock, dataset.events.clone()).await?;
    Ok(dataset)
}

/// Seed the performance analytics dataset and return the dataset
pub async fn seed_analytics_dataset_perf_via_scope(
    ctx: &Sandbox,
    clock: &SeedClock,
    count: usize,
) -> TestResult<AnalyticsDataset> {
    let dataset = AnalyticsDataset::perf(count);
    seed_events_via_scope(ctx, clock, dataset.events.clone()).await?;
    Ok(dataset)
}

/// Seed the minimal semantic query dataset and return the dataset
pub async fn seed_query_dataset_semantic_min_via_scope(
    ctx: &Sandbox,
    clock: &SeedClock,
) -> TestResult<QueryDataset> {
    let dataset = QueryDataset::semantic_min()?;
    seed_events_via_scope(ctx, clock, dataset.events.clone()).await?;
    Ok(dataset)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::sinex_test;

    #[sinex_test]
    fn test_seed_clock_advances() -> ::xtask::sandbox::TestResult<()> {
        let clock = SeedClock::new();
        let t1 = clock.tick(100);
        let t2 = clock.tick(100);
        assert!(t2 > t1);
        Ok(())
    }

    #[sinex_test]
    fn test_event_spec_builder() -> ::xtask::sandbox::TestResult<()> {
        let spec = EventSpec::new("source", "type").with_payload(json!({"key": "value"}));
        assert_eq!(spec.source, "source");
        assert_eq!(spec.event_type, "type");
        assert_eq!(spec.payload["key"], "value");
        Ok(())
    }

    #[sinex_test]
    fn test_event_spec_from_typed_captures_source_and_type() -> ::xtask::sandbox::TestResult<()> {
        let spec = EventSpec::from_typed(&FileCreatedPayload::test_default("/test"))?;
        assert_eq!(spec.source, "fs-watcher");
        assert_eq!(spec.event_type, "file.created");
        // Typed payload serializes with correct structure
        assert!(spec.payload.get("path").is_some());
        assert!(spec.payload.get("size").is_some());
        assert!(spec.payload.get("created_at").is_some());
        Ok(())
    }

    #[sinex_test]
    fn test_analytics_dataset_semantic_min_uses_typed_payloads() -> ::xtask::sandbox::TestResult<()>
    {
        let dataset = AnalyticsDataset::semantic_min()?;
        assert_eq!(dataset.expected_total, 5);
        // Shell commands should have correct source from KittyCommandExecutedPayload
        assert_eq!(dataset.expected_source_counts.get("shell.kitty"), Some(&3));
        assert_eq!(dataset.expected_source_counts.get("fs-watcher"), Some(&2));
        Ok(())
    }
}
