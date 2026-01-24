//! Common test utilities and specialized imports
//!
//! This module provides test-specific imports and utilities that are commonly
//! used across test files but don't belong in the main sinex-test-utils prelude.

// Re-export everything from the main prelude for convenience
pub use sinex_test_utils::prelude::*;

// Test-specific payload imports that are frequently used
pub use sinex_core::types::events::payloads::{
    AtuinCommandExecutedPayload,
    ClipboardCopiedPayload,
    FileCreatedPayload,
    FileDeletedPayload,
    FileModifiedPayload,
    KittyCommandExecutedPayload,
};

// DynamicPayload for runtime-specified source/type
pub use sinex_core::DynamicPayload;

// Common test patterns for nested modules
pub mod patterns {
    use super::*;
    use sinex_core::db::models::{Event, JsonValue};

    /// Common assertion pattern for event validation
    pub fn assert_event_basic_structure(event: &Event<JsonValue>) {
        assert!(!event.source.is_empty(), "Event source should not be empty");
        assert!(!event.event_type.is_empty(), "Event type should not be empty");
        assert!(!event.host.is_empty(), "Event host should not be empty");
    }
}

// Test-specific constants
pub mod constants {
    /// Standard test event sources
    pub const TEST_SOURCES: &[&str] = &[
        "fs-watcher",
        "terminal", 
        "desktop",
        "system",
        "test-source",
    ];
    
    /// Standard test event types  
    pub const TEST_EVENT_TYPES: &[&str] = &[
        "file.created",
        "file.modified", 
        "file.deleted",
        "command.executed",
        "window.focused",
        "test.event",
    ];
    
    /// Standard test paths
    pub const TEST_PATHS: &[&str] = &[
        "/tmp/test.txt",
        "/home/user/document.pdf",
        "/var/log/system.log", 
        "/opt/app/config.toml",
    ];
}

// Test-specific builder patterns
pub mod builders {
    use super::*;
    
    /// Builder for creating filesystem test events
    pub struct FilesystemEventBuilder {
        path: String,
        size: Option<u64>,
        event_type: String,
    }
    
    impl FilesystemEventBuilder {
        pub fn new(path: impl Into<String>) -> Self {
            Self {
                path: path.into(),
                size: None,
                event_type: "file.created".to_string(),
            }
        }
        
        pub fn with_size(mut self, size: u64) -> Self {
            self.size = Some(size);
            self
        }
        
        pub fn created(mut self) -> Self {
            self.event_type = "file.created".to_string();
            self
        }
        
        pub fn modified(mut self) -> Self {
            self.event_type = "file.modified".to_string();
            self
        }
        
        pub fn deleted(mut self) -> Self {
            self.event_type = "file.deleted".to_string();
            self
        }
        
        pub async fn build(self, ctx: &TestContext) -> Result<RawEvent> {
            let mut payload = json!({"path": self.path});

            if let Some(size) = self.size {
                payload["size"] = json!(size);
            }

            payload["timestamp"] = json!(Utc::now().to_rfc3339());

            ctx.publish(DynamicPayload::new("fs-watcher", self.event_type.as_str(), payload)).await
        }
    }
}

// Property testing helpers
#[cfg(feature = "proptest")]
pub mod proptest_helpers {
    use super::*;
    
    /// Generate arbitrary valid event sources  
    pub fn arbitrary_event_source() -> impl Strategy<Value = EventSource> {
        "[a-z][a-z0-9_-]{2,49}".prop_map(|s| EventSource::new(s))
    }
    
    /// Generate arbitrary valid event types
    pub fn arbitrary_event_type() -> impl Strategy<Value = EventType> {
        "[a-z][a-z0-9_.]{2,99}".prop_map(|s| EventType::new(s))
    }
    
    /// Generate arbitrary JSON payloads for testing
    pub fn arbitrary_json_payload() -> impl Strategy<Value = Value> {
        prop_oneof![
            Just(json!({})),
            Just(json!({"key": "value"})), 
            Just(json!({"data": "test", "size": 1024})),
            Just(json!({"path": "/tmp/test.txt", "size_bytes": 4096})),
            Just(json!({"command": "ls -la", "exit_code": 0})),
        ]
    }
}

// Performance testing utilities  
pub mod performance {
    use super::*;
    
    /// Helper to create bulk events for performance testing
    pub async fn create_bulk_events(
        ctx: &TestContext,
        count: usize,
        source: &str,
        event_type: &str,
    ) -> Result<Vec<Event<JsonValue>>> {
        let mut events = Vec::new();

        for i in 0..count {
            let event = ctx.publish(
                DynamicPayload::new(
                    source,
                    event_type,
                    json!({"index": i, "batch_id": uuid::Uuid::new_v4()}),
                ),
            ).await?;
            events.push(event);
        }

        Ok(events)
    }
    
    /// Helper to measure operation duration
    pub async fn measure_operation<F, R>(operation: F) -> (R, Duration)
    where
        F: std::future::Future<Output = R>,
    {
        let start = Instant::now();
        let result = operation.await;
        let duration = start.elapsed();
        (result, duration)
    }
}
