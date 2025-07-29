//! Consolidated test data builders with fluent interfaces
//!
//! This module provides builder patterns for creating test data,
//! making tests more readable and reducing boilerplate code.
//! It combines both test-specific builders and re-exports from sinex-events.

use crate::prelude::*;
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
pub(crate) struct GenericEventBuilder {
    factory: EventFactory,
    event_type: String,
    payload: Option<serde_json::Value>,
    timestamp: Option<chrono::DateTime<chrono::Utc>>,
}

impl GenericEventBuilder {
    pub fn new(source: &str, event_type: &str) -> Self {
        Self {
            factory: EventFactory::new(source),
            event_type: event_type.to_string(),
            payload: None,
            timestamp: None,
        }
    }

    pub fn payload(mut self, payload: serde_json::Value) -> Self {
        self.payload = Some(payload);
        self
    }

    pub fn timestamp(mut self, ts: chrono::DateTime<chrono::Utc>) -> Self {
        self.timestamp = Some(ts);
        self
    }

    pub fn build(self) -> sinex_core_types::RawEvent {
        let payload = self.payload.unwrap_or_else(|| serde_json::json!({}));
        self.factory.create_event(&self.event_type, payload)
    }

    // Terminal-specific methods
    pub fn command(self, cmd: impl Into<String>) -> Self {
        let mut payload = self.payload.unwrap_or_else(|| serde_json::json!({}));
        payload["command"] = serde_json::json!(cmd.into());
        Self {
            payload: Some(payload),
            ..self
        }
    }

    pub fn success(self) -> Self {
        let mut payload = self.payload.unwrap_or_else(|| serde_json::json!({}));
        payload["exit_status"] = serde_json::json!(0);
        Self {
            payload: Some(payload),
            ..self
        }
    }

    pub fn duration_ms(self, ms: u64) -> Self {
        let mut payload = self.payload.unwrap_or_else(|| serde_json::json!({}));
        payload["execution_time_ms"] = serde_json::json!(ms);
        Self {
            payload: Some(payload),
            ..self
        }
    }

    // Agent-specific methods
    pub fn name(self, name: impl Into<String>) -> Self {
        let mut payload = self.payload.unwrap_or_else(|| serde_json::json!({}));
        payload["agent_name"] = serde_json::json!(name.into());
        Self {
            payload: Some(payload),
            ..self
        }
    }

    pub fn heartbeat(self) -> Self {
        let mut new_builder = Self {
            event_type: "automaton.heartbeat".to_string(),
            ..self
        };
        let mut payload = new_builder.payload.unwrap_or_else(|| serde_json::json!({}));
        payload["status"] = serde_json::json!("running");
        new_builder.payload = Some(payload);
        new_builder
    }

    pub fn startup(self) -> Self {
        let mut new_builder = Self {
            event_type: "automaton.startup".to_string(),
            ..self
        };
        let mut payload = new_builder.payload.unwrap_or_else(|| serde_json::json!({}));
        payload["status"] = serde_json::json!("starting");
        new_builder.payload = Some(payload);
        new_builder
    }

    pub fn error(self, error_msg: impl Into<String>) -> Self {
        let mut new_builder = Self {
            event_type: "automaton.error".to_string(),
            ..self
        };
        let mut payload = new_builder.payload.unwrap_or_else(|| serde_json::json!({}));
        payload["error_message"] = serde_json::json!(error_msg.into());
        payload["status"] = serde_json::json!("error");
        new_builder.payload = Some(payload);
        new_builder
    }

    pub fn uptime_seconds(self, seconds: u64) -> Self {
        let mut payload = self.payload.unwrap_or_else(|| serde_json::json!({}));
        payload["uptime_seconds"] = serde_json::json!(seconds);
        Self {
            payload: Some(payload),
            ..self
        }
    }

    pub fn version(self, version: impl Into<String>) -> Self {
        let mut payload = self.payload.unwrap_or_else(|| serde_json::json!({}));
        payload["version"] = serde_json::json!(version.into());
        Self {
            payload: Some(payload),
            ..self
        }
    }

    pub fn events_processed(self, count: u64) -> Self {
        let mut payload = self.payload.unwrap_or_else(|| serde_json::json!({}));
        payload["events_processed_session"] = serde_json::json!(count);
        Self {
            payload: Some(payload),
            ..self
        }
    }
}

/// Builder for checkpoint test data
#[derive(Debug, Clone)]
pub(crate) struct TestCheckpointBuilder {
    processor_name: String,
    consumer_group: Option<String>,
    consumer_name: Option<String>,
    last_processed_id: Option<Ulid>,
    processed_count: i64,
    state_data: Option<JsonValue>,
    checkpoint_version: i32,
    checkpoint_data: Option<JsonValue>,
}

impl TestCheckpointBuilder {
    /// Create a new checkpoint builder
    pub fn new(processor_name: &str) -> Self {
        Self {
            processor_name: processor_name.to_string(),
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
        use sinex_db::queries::CheckpointQueries;

        let group = self
            .consumer_group
            .unwrap_or_else(|| format!("{}-group", self.processor_name));
        let consumer = self
            .consumer_name
            .unwrap_or_else(|| format!("{}-consumer", self.processor_name));

        CheckpointQueries::upsert_checkpoint(
            Ulid::new(),
            self.processor_name,
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
pub(crate) struct TestScenarioBuilder {
    events: Vec<RawEvent>,
    checkpoints: Vec<TestCheckpointBuilder>,
    pool: Option<DbPool>,
}

impl TestScenarioBuilder {
    /// Create a new scenario builder
    pub fn new() -> Self {
        Self {
            events: Vec::new(),
            checkpoints: Vec::new(),
            pool: None,
        }
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
    pub async fn execute(self, pool: &DbPool) -> Result<ScenarioResult> {
        let mut event_ids = Vec::new();

        // Insert all events
        for event in self.events {
            let inserted = sinex_db::insert_event_with_validator(pool, &event, None).await?;
            event_ids.push(inserted.id);
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
pub(crate) struct ScenarioResult {
    pub event_ids: Vec<Ulid>,
    pub event_count: usize,
}

/// Builder for batch event operations
pub(crate) struct BatchEventBuilder {
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
        let factory = EventFactory::new(&self.base_source);
        (0..self.count)
            .map(|i| {
                let mut event =
                    factory.create_event(&self.base_event_type, (self.payload_generator)(i));

                if let Some(spacing) = self.time_spacing {
                    let event_time = self.start_time + spacing * i as i32;
                    event.ts_orig = Some(event_time);
                }

                event
            })
            .collect()
    }

    /// Insert all events in batch
    pub async fn insert(self, pool: &DbPool) -> Result<Vec<RawEvent>> {
        let factory = EventFactory::new(&self.base_source);
        let mut results = Vec::new();

        for i in 0..self.count {
            let mut event =
                factory.create_event(&self.base_event_type, (self.payload_generator)(i));

            if let Some(spacing) = self.time_spacing {
                let event_time = self.start_time + spacing * i as i32;
                event.ts_orig = Some(event_time);
            }

            let inserted = sinex_db::insert_event_with_validator(pool, &event, None).await?;
            results.push(inserted);
        }

        Ok(results)
    }
}

/// Generic event builder for test flexibility
pub(crate) struct EventBuilder;

impl EventBuilder {
    /// Create a generic event builder with source and type
    pub fn generic(source: &str, event_type: &str) -> GenericEventBuilder {
        GenericEventBuilder::new(source, event_type)
    }

    /// Create a filesystem event builder
    pub fn filesystem() -> FilesystemEventBuilder {
        let factory = EventFactory::new("fs");
        factory.filesystem()
    }

    /// Create a terminal event builder  
    pub fn terminal() -> GenericEventBuilder {
        GenericEventBuilder::new("shell.kitty", "command.executed")
    }

    /// Create a clipboard event builder
    pub fn clipboard() -> ClipboardEventBuilder {
        let factory = EventFactory::new("clipboard");
        factory.clipboard()
    }

    /// Create a hyprland event builder
    pub fn hyprland() -> WindowManagerEventBuilder {
        let factory = EventFactory::new("wm.hyprland");
        factory.window_manager()
    }

    /// Create an agent event builder
    pub fn agent() -> GenericEventBuilder {
        GenericEventBuilder::new("sinex", "automaton.heartbeat")
    }
}

// Comprehensive builder tests
#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::*;

    #[test]
    fn test_batch_builder_ordering() {
        // Test batch builder with proptest - pure function, no DB needed
        use ::proptest::prelude::*;

        proptest!(|(
            source in "[a-zA-Z][a-zA-Z0-9_.-]{2,20}",
            event_type in "[a-zA-Z][a-zA-Z0-9_-]{1,20}\\.[a-zA-Z][a-zA-Z0-9_-]{1,20}",
            count in 2..20usize,
            spacing_ms in 1..100u64
        )| {
            let spacing = chrono::Duration::milliseconds(spacing_ms as i64);
            let batch = BatchEventBuilder::new(&source, &event_type, count)
                .with_time_spacing(spacing)
                .build();

            prop_assert_eq!(batch.len(), count);

            // Verify ordering and spacing
            for window in batch.windows(2) {
                let event1 = &window[0];
                let event2 = &window[1];

                // IDs should be ordered
                prop_assert!(event1.id < event2.id);

                // Timestamps should respect spacing
                if let (Some(ts1), Some(ts2)) = (event1.ts_orig, event2.ts_orig) {
                    let diff = ts2 - ts1;
                    prop_assert_eq!(diff, spacing);
                }
            }
        });
    }

    #[sinex_test]
    async fn test_scenario_builder(_ctx: TestContext) -> Result<()> {
        // Test scenario builder with multiple sources
        let sources = vec!["source1", "source2", "source3"];
        let counts = vec![3, 5, 2];

        let mut scenario = TestScenarioBuilder::new();
        let mut expected_total = 0;

        for (source, count) in sources.iter().zip(counts.iter()) {
            scenario = scenario.with_events_from_source(source, "test.event", *count);
            expected_total += count;
        }

        // Build the scenario (without inserting to DB)
        let events: Vec<_> = scenario.events;

        assert_eq!(events.len(), expected_total);

        // Verify all events have unique IDs
        let ids: std::collections::HashSet<_> = events.iter().map(|e| e.id).collect();
        assert_eq!(ids.len(), events.len());

        Ok(())
    }

    #[test]
    fn test_generic_builder_methods() {
        // Test generic builder with proptest
        use ::proptest::prelude::*;

        proptest!(|(
            source in "[a-zA-Z][a-zA-Z0-9_.-]{2,20}",
            command in "[a-zA-Z0-9 _-]{1,50}",
            duration_ms in 1..1000u64
        )| {
            // Test terminal-specific methods
            let event = GenericEventBuilder::new(&source, "command.executed")
                .command(&command)
                .duration_ms(duration_ms)
                .build();

            prop_assert_eq!(&event.payload["command"], &json!(command));
            prop_assert_eq!(&event.payload["execution_time_ms"], &json!(duration_ms));

            // Test agent-specific methods
            let agent_name = format!("agent-{}", source);
            let heartbeat = GenericEventBuilder::new(&source, "agent.status")
                .name(&agent_name)
                .heartbeat()
                .uptime_seconds(3600)
                .events_processed(1000)
                .build();

            prop_assert_eq!(&heartbeat.event_type, "automaton.heartbeat");
            prop_assert_eq!(&heartbeat.payload["agent_name"], &json!(agent_name));
            prop_assert_eq!(&heartbeat.payload["uptime_seconds"], &json!(3600));
            prop_assert_eq!(&heartbeat.payload["events_processed_session"], &json!(1000));
        });
    }

    #[sinex_test]
    async fn test_checkpoint_builder(ctx: TestContext) -> Result<()> {
        // Test checkpoint insertion with various configurations
        let automata = vec!["analytics", "search", "content", "pkm"];

        for (i, name) in automata.iter().enumerate() {
            TestCheckpointBuilder::new(name)
                .with_group(&format!("{}-group", name))
                .with_consumer(&format!("{}-01", name))
                .with_processed_count((i + 1) as i64 * 100)
                .with_state(json!({
                    "last_run": "2024-01-01T00:00:00Z",
                    "status": "active"
                }))
                .insert(ctx.pool())
                .await?;
        }

        // Verify checkpoints were created
        let result: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM core.processor_checkpoints WHERE processor_name = ANY($1)",
        )
        .bind(&automata)
        .fetch_one(ctx.pool())
        .await?;

        assert_eq!(result, automata.len() as i64);
        Ok(())
    }

    #[sinex_test]
    async fn test_scenario_execution(ctx: TestContext) -> Result<()> {
        // Build and execute a complex scenario
        let result = TestScenarioBuilder::new()
            .with_events_from_source("fs", "file.created", 5)
            .with_events_from_source("shell", "command.executed", 3)
            .with_checkpoint(TestCheckpointBuilder::new("test-automaton").with_processed_count(8))
            .execute(ctx.pool())
            .await?;

        assert_eq!(result.event_count, 8);
        assert_eq!(result.event_ids.len(), 8);

        // Verify all events exist
        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM core.events WHERE event_id = ANY($1)")
                .bind(
                    &result
                        .event_ids
                        .iter()
                        .map(|id| id.to_uuid())
                        .collect::<Vec<_>>(),
                )
                .fetch_one(ctx.pool())
                .await?;

        assert_eq!(count, 8);
        Ok(())
    }

    #[sinex_test]
    async fn test_batch_insertion(ctx: TestContext) -> Result<()> {
        // Test batch insertion with time spacing
        let start = Utc::now() - chrono::Duration::hours(1);
        let events = BatchEventBuilder::new("monitoring", "metric.recorded", 10)
            .with_start_time(start)
            .with_time_spacing(chrono::Duration::minutes(5))
            .with_payload_generator(|i| {
                json!({
                    "metric": "cpu_usage",
                    "value": 50 + i * 5,
                    "host": format!("server-{}", i % 3)
                })
            })
            .insert(ctx.pool())
            .await?;

        assert_eq!(events.len(), 10);

        // Verify time spacing
        for window in events.windows(2) {
            if let (Some(ts1), Some(ts2)) = (window[0].ts_orig, window[1].ts_orig) {
                assert_eq!(ts2 - ts1, chrono::Duration::minutes(5));
            }
        }

        Ok(())
    }

    #[sinex_test]
    async fn test_all_domain_specific_builders(ctx: TestContext) -> Result<()> {
        // Test all specialized event builders work correctly
        let fs_event = ctx
            .event()
            .filesystem()
            .path("/test/file.txt")
            .size(1024)
            .created()
            .insert()
            .await?;
        assert_eq!(fs_event.source, "fs");
        assert_eq!(fs_event.event_type, "fs.file.created");

        let term_event = ctx
            .event()
            .terminal()
            .command("ls -la")
            .working_dir("/home")
            .exit_code(0)
            .insert()
            .await?;
        assert_eq!(term_event.source, "shell-terminal");

        let clip_event = ctx
            .event()
            .clipboard()
            .content("test text")
            .copied()
            .insert()
            .await?;
        assert_eq!(clip_event.source, "clipboard");
        assert_eq!(clip_event.event_type, "clipboard.copy");

        let win_event = ctx
            .event()
            .window()
            .title("Test App")
            .focused()
            .insert()
            .await?;
        assert_eq!(win_event.source, "window-manager");

        let sys_event = ctx
            .event()
            .system()
            .service("nginx")
            .started()
            .insert()
            .await?;
        assert_eq!(sys_event.source, "systemd");

        Ok(())
    }

    #[sinex_test]
    async fn test_builder_validation(ctx: TestContext) -> Result<()> {
        // Empty source/type should fail
        assert!(ctx.event().source("").type_("test").insert().await.is_err());
        assert!(ctx.event().source("test").type_("").insert().await.is_err());

        // Test other validation rules
        assert!(ctx
            .event()
            .source("test")
            .type_("") // Empty type
            .insert()
            .await
            .is_err());

        Ok(())
    }
}
