#![cfg(test)]

use anyhow::Result;
use chrono::{DateTime, Utc};
use sinex_db::models::RawEvent;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::info;

/// Test fixture for ingestor testing
pub struct IngestorTestFixture {
    pub event_sink: Arc<crate::MemorySink>,
    pub config: serde_json::Value,
    pub test_data: TestDataGenerator,
    pub time_controller: TimeController,
}

impl IngestorTestFixture {
    pub fn new() -> Self {
        Self {
            event_sink: Arc::new(crate::MemorySink::new()),
            config: serde_json::json!({}),
            test_data: TestDataGenerator::new(),
            time_controller: TimeController::new(),
        }
    }
    
    pub fn with_config(mut self, config: serde_json::Value) -> Self {
        self.config = config;
        self
    }
    
    pub async fn wait_for_events(&self, count: usize, timeout: Duration) -> Result<Vec<RawEvent>> {
        let start = tokio::time::Instant::now();
        
        loop {
            let events = self.event_sink.get_events().await;
            if events.len() >= count {
                return Ok(events);
            }
            
            if start.elapsed() > timeout {
                anyhow::bail!(
                    "Timeout waiting for {} events, got {}",
                    count,
                    events.len()
                );
            }
            
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    
    pub async fn assert_event_matches<F>(&self, predicate: F) -> Result<RawEvent>
    where
        F: Fn(&RawEvent) -> bool,
    {
        let events = self.event_sink.get_events().await;
        events
            .into_iter()
            .find(|e| predicate(e))
            .ok_or_else(|| anyhow::anyhow!("No event matched predicate"))
    }
}

/// Time controller for deterministic testing
pub struct TimeController {
    current_time: Arc<Mutex<DateTime<Utc>>>,
    auto_advance: Arc<Mutex<Option<Duration>>>,
}

impl TimeController {
    pub fn new() -> Self {
        Self {
            current_time: Arc::new(Mutex::new(Utc::now())),
            auto_advance: Arc::new(Mutex::new(None)),
        }
    }
    
    pub fn now(&self) -> DateTime<Utc> {
        let time = *self.current_time.lock().unwrap();
        
        // Auto-advance if configured
        if let Some(duration) = *self.auto_advance.lock().unwrap() {
            self.advance(duration);
        }
        
        time
    }
    
    pub fn advance(&self, duration: Duration) {
        let mut time = self.current_time.lock().unwrap();
        *time = *time + chrono::Duration::from_std(duration).unwrap();
    }
    
    pub fn set_time(&self, time: DateTime<Utc>) {
        *self.current_time.lock().unwrap() = time;
    }
    
    pub fn set_auto_advance(&self, duration: Option<Duration>) {
        *self.auto_advance.lock().unwrap() = duration;
    }
}

/// Test data generator
pub struct TestDataGenerator {
    seed: u64,
}

impl TestDataGenerator {
    pub fn new() -> Self {
        Self { seed: 42 }
    }
    
    pub fn with_seed(seed: u64) -> Self {
        Self { seed }
    }
    
    /// Generate sample event payload
    pub fn sample_event_payload(&self) -> serde_json::Value {
        serde_json::json!({
            "test_field": "test_value",
            "timestamp": Utc::now(),
            "seed": self.seed
        })
    }
    
    /// Generate sample filesystem path
    pub fn sample_path(&self) -> String {
        format!("/test/path/{}", self.seed)
    }
    
    /// Generate sample window title
    pub fn sample_window_title(&self) -> String {
        format!("Test Window {}", self.seed)
    }
}

/// Mock implementations for testing
pub mod mocks {
    use super::*;
    
    /// Mock event source that generates predictable events
    pub struct MockEventSource {
        events: Vec<RawEvent>,
        index: Arc<Mutex<usize>>,
        delay: Option<Duration>,
    }
    
    impl MockEventSource {
        pub fn new(events: Vec<RawEvent>) -> Self {
            Self {
                events,
                index: Arc::new(Mutex::new(0)),
                delay: None,
            }
        }
        
        pub fn with_delay(mut self, delay: Duration) -> Self {
            self.delay = Some(delay);
            self
        }
        
        pub async fn next_event(&self) -> Option<RawEvent> {
            if let Some(delay) = self.delay {
                tokio::time::sleep(delay).await;
            }
            
            let mut index = self.index.lock().unwrap();
            if *index < self.events.len() {
                let event = self.events[*index].clone();
                *index += 1;
                Some(event)
            } else {
                None
            }
        }
    }
    
    /// Mock service with controllable failures
    pub struct FaultyService {
        failure_rate: f32,
        failure_type: FailureType,
        call_count: Arc<Mutex<u32>>,
    }
    
    #[derive(Clone)]
    pub enum FailureType {
        Timeout,
        ConnectionError,
        ValidationError,
        Random,
    }
    
    impl FaultyService {
        pub fn new(failure_rate: f32, failure_type: FailureType) -> Self {
            Self {
                failure_rate,
                failure_type,
                call_count: Arc::new(Mutex::new(0)),
            }
        }
        
        pub async fn call(&self) -> Result<()> {
            let mut count = self.call_count.lock().unwrap();
            *count += 1;
            
            // Deterministic failure based on call count
            let should_fail = (*count as f32 % (1.0 / self.failure_rate)) < 1.0;
            
            if should_fail {
                match self.failure_type {
                    FailureType::Timeout => {
                        tokio::time::sleep(Duration::from_secs(30)).await;
                        Err(anyhow::anyhow!("Operation timed out"))
                    }
                    FailureType::ConnectionError => {
                        Err(anyhow::anyhow!("Connection refused"))
                    }
                    FailureType::ValidationError => {
                        Err(anyhow::anyhow!("Validation failed"))
                    }
                    FailureType::Random => {
                        let errors = vec![
                            "Network error",
                            "Database error",
                            "Unknown error",
                        ];
                        let idx = *count as usize % errors.len();
                        Err(anyhow::anyhow!(errors[idx]))
                    }
                }
            } else {
                Ok(())
            }
        }
        
        pub fn reset(&self) {
            *self.call_count.lock().unwrap() = 0;
        }
    }
}

/// Assertion helpers
#[macro_export]
macro_rules! assert_event_type {
    ($event:expr, $source:expr, $event_type:expr) => {
        assert_eq!($event.source, $source, "Event source mismatch");
        assert_eq!($event.event_type, $event_type, "Event type mismatch");
    };
}

#[macro_export]
macro_rules! assert_event_payload {
    ($event:expr, $($key:expr => $value:expr),+) => {
        let payload = &$event.payload;
        $(
            assert_eq!(
                payload.get($key),
                Some(&serde_json::json!($value)),
                "Payload field {} mismatch",
                $key
            );
        )+
    };
}

/// Snapshot testing support
pub struct SnapshotTest {
    name: String,
    path: std::path::PathBuf,
}

impl SnapshotTest {
    pub fn new(name: impl Into<String>) -> Self {
        let name = name.into();
        let path = std::path::Path::new("tests/snapshots").join(&name);
        Self { name, path }
    }
    
    pub fn assert_json_snapshot(&self, value: &serde_json::Value) -> Result<()> {
        let pretty = serde_json::to_string_pretty(value)?;
        
        if self.path.exists() {
            let expected = std::fs::read_to_string(&self.path)?;
            if expected != pretty {
                if std::env::var("UPDATE_SNAPSHOTS").is_ok() {
                    std::fs::write(&self.path, &pretty)?;
                    info!("Updated snapshot: {}", self.name);
                } else {
                    anyhow::bail!(
                        "Snapshot mismatch for {}. Run with UPDATE_SNAPSHOTS=1 to update.",
                        self.name
                    );
                }
            }
        } else {
            std::fs::create_dir_all(self.path.parent().unwrap())?;
            std::fs::write(&self.path, &pretty)?;
            info!("Created new snapshot: {}", self.name);
        }
        
        Ok(())
    }
}