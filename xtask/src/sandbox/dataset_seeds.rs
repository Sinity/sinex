//! Dataset seeding utilities for integration tests
//!
//! Provides structured event seeding for tests with deterministic timestamps
//! and predefined datasets for analytics and search testing.

use crate::sandbox::prelude::*;
use serde_json::{json, Value as JsonValue};
use std::sync::atomic::{AtomicI64, Ordering};
use sinex_primitives::Timestamp;
use time::{Duration, OffsetDateTime};

/// Clock for generating sequential test timestamps
///
/// Ensures events have predictable ordering in tests.
pub struct SeedClock {
    base: OffsetDateTime,
    offset_ms: AtomicI64,
}

impl SeedClock {
    /// Create a new seed clock starting from now - 1 hour
    pub fn new() -> Self {
        Self {
            base: OffsetDateTime::now_utc() - Duration::hours(1),
            offset_ms: AtomicI64::new(0),
        }
    }

    /// Create a seed clock starting from a specific time
    pub fn from_base(base: Timestamp) -> Self {
        Self {
            base: base.inner(),
            offset_ms: AtomicI64::new(0),
        }
    }

    /// Get current timestamp and advance by given milliseconds
    pub fn tick(&self, advance_ms: i64) -> Timestamp {
        let offset = self.offset_ms.fetch_add(advance_ms, Ordering::SeqCst);
        Timestamp::new(self.base + Duration::milliseconds(offset))
    }

    /// Get current timestamp without advancing
    pub fn now(&self) -> Timestamp {
        let offset = self.offset_ms.load(Ordering::SeqCst);
        Timestamp::new(self.base + Duration::milliseconds(offset))
    }

    /// Reset to base time
    pub fn reset(&self) {
        self.offset_ms.store(0, Ordering::SeqCst);
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
    /// Create a new event spec
    pub fn new(source: impl Into<String>, event_type: impl Into<String>) -> Self {
        Self {
            source: source.into(),
            event_type: event_type.into(),
            payload: json!({}),
            timestamp: None,
        }
    }

    /// Set payload
    pub fn with_payload(mut self, payload: JsonValue) -> Self {
        self.payload = payload;
        self
    }

    /// Set timestamp
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
        let _timestamp = spec.timestamp.unwrap_or_else(|| clock.tick(100));
        let payload = DynamicPayload::new(spec.source.as_str(), spec.event_type.as_str(), spec.payload);
        let event = ctx.publish(payload).await?;
        if let Some(id) = event.id {
            ids.push(*id.as_ulid());
        }
    }

    Ok(ids)
}

/// Predefined analytics test dataset
#[derive(Debug, Clone)]
pub struct AnalyticsDataset {
    pub name: String,
    pub events: Vec<EventSpec>,
}

impl AnalyticsDataset {
    /// Create minimal semantic dataset for analytics tests
    pub fn semantic_min() -> Self {
        Self {
            name: "analytics-semantic-min".to_string(),
            events: vec![
                EventSpec::new("shell.bash", "command.executed")
                    .with_payload(json!({"command": "ls", "exit_code": 0})),
                EventSpec::new("shell.bash", "command.executed")
                    .with_payload(json!({"command": "git status", "exit_code": 0})),
                EventSpec::new("shell.bash", "command.executed")
                    .with_payload(json!({"command": "ls", "exit_code": 0})),
                EventSpec::new("fs-watcher", "file.created")
                    .with_payload(json!({"path": "/tmp/test.txt", "size": 100})),
                EventSpec::new("fs-watcher", "file.modified")
                    .with_payload(json!({"path": "/tmp/test.txt", "size": 150})),
            ],
        }
    }

    /// Create performance dataset with many events
    pub fn perf(count: usize) -> Self {
        let mut events = Vec::with_capacity(count);
        for i in 0..count {
            events.push(
                EventSpec::new("shell.bash", "command.executed")
                    .with_payload(json!({"command": format!("cmd-{}", i), "exit_code": 0})),
            );
        }
        Self {
            name: "analytics-perf".to_string(),
            events,
        }
    }
}

/// Predefined query test dataset
#[derive(Debug, Clone)]
pub struct QueryDataset {
    pub name: String,
    pub events: Vec<EventSpec>,
}

impl QueryDataset {
    /// Create minimal semantic dataset for query/search tests
    pub fn semantic_min() -> Self {
        Self {
            name: "query-semantic-min".to_string(),
            events: vec![
                EventSpec::new("shell.bash", "command.executed")
                    .with_payload(json!({"command": "cargo build", "exit_code": 0})),
                EventSpec::new("shell.bash", "command.executed")
                    .with_payload(json!({"command": "cargo test", "exit_code": 0})),
                EventSpec::new("fs-watcher", "file.created")
                    .with_payload(json!({"path": "/project/src/main.rs", "size": 500})),
            ],
        }
    }
}

/// Seed the minimal semantic analytics dataset
pub async fn seed_analytics_dataset_semantic_min_via_scope(
    ctx: &Sandbox,
    clock: &SeedClock,
) -> TestResult<Vec<Ulid>> {
    let dataset = AnalyticsDataset::semantic_min();
    seed_events_via_scope(ctx, clock, dataset.events).await
}

/// Seed the performance analytics dataset
pub async fn seed_analytics_dataset_perf_via_scope(
    ctx: &Sandbox,
    clock: &SeedClock,
    count: usize,
) -> TestResult<Vec<Ulid>> {
    let dataset = AnalyticsDataset::perf(count);
    seed_events_via_scope(ctx, clock, dataset.events).await
}

/// Seed the minimal semantic query dataset
pub async fn seed_query_dataset_semantic_min_via_scope(
    ctx: &Sandbox,
    clock: &SeedClock,
) -> TestResult<Vec<Ulid>> {
    let dataset = QueryDataset::semantic_min();
    seed_events_via_scope(ctx, clock, dataset.events).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_seed_clock_advances() {
        let clock = SeedClock::new();
        let t1 = clock.tick(100);
        let t2 = clock.tick(100);
        assert!(t2 > t1);
    }

    #[test]
    fn test_event_spec_builder() {
        let spec = EventSpec::new("source", "type")
            .with_payload(json!({"key": "value"}));
        assert_eq!(spec.source, "source");
        assert_eq!(spec.event_type, "type");
        assert_eq!(spec.payload["key"], "value");
    }
}
