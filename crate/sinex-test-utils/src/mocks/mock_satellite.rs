// Mock satellite implementation for testing

use crate::prelude::*;
use sinex_events::{RawEvent, EventFactory};
use sinex_satellite_sdk::config::SatelliteConfig;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

/// Configuration for mock satellite behavior
#[derive(Debug, Clone)]
pub struct MockSatelliteConfig {
    /// Base satellite configuration
    pub base_config: SatelliteConfig,
    /// Event generation interval (milliseconds)
    pub event_interval_ms: u64,
    /// Maximum events to generate (None = unlimited)
    pub max_events: Option<usize>,
    /// Event template for generation
    pub event_template: EventTemplate,
    /// Simulate connection failures
    pub connection_failure_rate: f64,
    /// Batch size for event sending
    pub batch_size: usize,
    /// Whether to include sequence numbers
    pub include_sequence: bool,
}

impl Default for MockSatelliteConfig {
    fn default() -> Self {
        Self {
            base_config: SatelliteConfig {
                service_name: "mock-satellite".to_string(),
                log_level: "info".to_string(),
                ingest_socket_path: "/tmp/test-ingestd.sock".to_string(),
                redis_url: "redis://127.0.0.1:6379/".to_string(),
                database_url: None,
                database_pool_size: 5,
                work_dir: std::path::PathBuf::from("/tmp/sinex-test"),
                dry_run: false,
                replay: None,
            },
            event_interval_ms: 100,
            max_events: Some(10),
            event_template: EventTemplate::default(),
            connection_failure_rate: 0.0,
            batch_size: 1,
            include_sequence: true,
        }
    }
}

/// Template for generating test events
#[derive(Debug, Clone)]
pub struct EventTemplate {
    pub source: String,
    pub event_type: String,
    pub base_payload: serde_json::Value,
}

impl Default for EventTemplate {
    fn default() -> Self {
        Self {
            source: "test.satellite".to_string(),
            event_type: "test.generated".to_string(),
            base_payload: serde_json::json!({
                "test": true,
                "generator": "mock_satellite"
            }),
        }
    }
}

/// Mock satellite for testing
pub struct MockSatellite {
    pub id: String,
    pub config: MockSatelliteConfig,
    pub events_generated: Arc<Mutex<Vec<RawEvent>>>,
    pub events_sent: Arc<Mutex<Vec<RawEvent>>>,
    pub task_handle: Option<JoinHandle<()>>,
    pub is_running: Arc<Mutex<bool>>,
}

impl MockSatellite {
    /// Create a new mock satellite
    pub fn new(config: MockSatelliteConfig) -> Self {
        let id = format!("mock-satellite-{}", sinex_ulid::Ulid::new());

        Self {
            id,
            config,
            events_generated: Arc::new(Mutex::new(Vec::new())),
            events_sent: Arc::new(Mutex::new(Vec::new())),
            task_handle: None,
            is_running: Arc::new(Mutex::new(false)),
        }
    }

    /// Start the mock satellite
    pub async fn start(&mut self) -> TestResult<()> {
        *self.is_running.lock().await = true;

        let config = self.config.clone();
        let events_generated = self.events_generated.clone();
        let events_sent = self.events_sent.clone();
        let is_running = self.is_running.clone();
        let satellite_id = self.id.clone();

        let task_handle = tokio::spawn(async move {
            let mut sequence = 0usize;
            let mut batch = Vec::new();

            let interval = std::time::Duration::from_millis(config.event_interval_ms);
            let mut ticker = tokio::time::interval(interval);

            while *is_running.lock().await {
                ticker.tick().await;

                // Check if we've reached the maximum
                if let Some(max) = config.max_events {
                    if sequence >= max {
                        break;
                    }
                }

                // Generate event
                let event = generate_event(&config.event_template, &satellite_id, sequence);
                events_generated.lock().await.push(event.clone());
                batch.push(event);
                sequence += 1;

                // Send batch when full or on interval
                if batch.len() >= config.batch_size {
                    if let Err(e) = send_events_batch(&config, &batch, &events_sent).await {
                        eprintln!("Failed to send events: {}", e);
                    }
                    batch.clear();
                }
            }

            // Send any remaining events
            if !batch.is_empty() {
                let _ = send_events_batch(&config, &batch, &events_sent).await;
            }
        });

        self.task_handle = Some(task_handle);
        Ok(())
    }

    /// Stop the mock satellite
    pub async fn stop(&mut self) -> TestResult<()> {
        *self.is_running.lock().await = false;

        if let Some(handle) = self.task_handle.take() {
            handle.abort();
        }

        Ok(())
    }

    /// Simulate a crash (abrupt stop)
    pub async fn crash(&mut self) {
        *self.is_running.lock().await = false;

        if let Some(handle) = self.task_handle.take() {
            handle.abort();
        }
    }

    /// Get events generated by this satellite
    pub async fn get_generated_events(&self) -> Vec<RawEvent> {
        self.events_generated.lock().await.clone()
    }

    /// Get events successfully sent by this satellite
    pub async fn get_sent_events(&self) -> Vec<RawEvent> {
        self.events_sent.lock().await.clone()
    }

    /// Get count of events generated
    pub async fn generated_count(&self) -> usize {
        self.events_generated.lock().await.len()
    }

    /// Get count of events sent
    pub async fn sent_count(&self) -> usize {
        self.events_sent.lock().await.len()
    }

    /// Wait for satellite to generate expected number of events
    pub async fn wait_for_generation(
        &self,
        expected: usize,
        timeout_secs: u64,
    ) -> TestResult<()> {
        let timeout = std::time::Duration::from_secs(timeout_secs);
        let start = std::time::Instant::now();

        loop {
            let count = self.generated_count().await;
            if count >= expected {
                return Ok(());
            }

            if start.elapsed() > timeout {
                return Err(CoreError::Unknown(format!(
                    "Timeout waiting for {} events to be generated, got {}",
                    expected,
                    count
                )));
            }

            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }

    /// Wait for satellite to send expected number of events
    pub async fn wait_for_sending(&self, expected: usize, timeout_secs: u64) -> TestResult<()> {
        let timeout = std::time::Duration::from_secs(timeout_secs);
        let start = std::time::Instant::now();

        loop {
            let count = self.sent_count().await;
            if count >= expected {
                return Ok(());
            }

            if start.elapsed() > timeout {
                return Err(CoreError::Unknown(format!(
                    "Timeout waiting for {} events to be sent, got {}",
                    expected,
                    count
                )));
            }

            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }

    /// Check if satellite is running
    pub async fn is_running(&self) -> bool {
        *self.is_running.lock().await
    }
}

/// Generate a test event from template
fn generate_event(template: &EventTemplate, satellite_id: &str, sequence: usize) -> RawEvent {
    let mut payload = template.base_payload.clone();

    // Add satellite-specific data
    if let serde_json::Value::Object(ref mut map) = payload {
        map.insert(
            "satellite_id".to_string(),
            serde_json::Value::String(satellite_id.to_string()),
        );
        map.insert(
            "sequence".to_string(),
            serde_json::Value::Number(serde_json::Number::from(sequence)),
        );
        map.insert(
            "timestamp".to_string(),
            serde_json::Value::String(chrono::Utc::now().to_rfc3339()),
        );
    }

    let factory = EventFactory::new(&template.source);
    factory.create_event(&template.event_type, payload)
}

/// Send a batch of events to ingestd
async fn send_events_batch(
    config: &MockSatelliteConfig,
    events: &[RawEvent],
    sent_events: &Arc<Mutex<Vec<RawEvent>>>,
) -> TestResult<()> {
    // Simulate connection failures
    if config.connection_failure_rate > 0.0 {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        if rng.gen::<f64>() < config.connection_failure_rate {
            return Err(CoreError::Unknown(format!("Simulated connection failure")));
        }
    }

    // In a real implementation, this would send to ingestd via gRPC
    // For testing, we just mark them as sent
    let mut sent = sent_events.lock().await;
    sent.extend_from_slice(events);

    Ok(())
}

/// Builder for mock satellite configuration
pub struct MockSatelliteBuilder {
    config: MockSatelliteConfig,
}

impl MockSatelliteBuilder {
    /// Create a new mock satellite builder
    pub fn new() -> Self {
        Self {
            config: MockSatelliteConfig::default(),
        }
    }

    /// Set the satellite service name
    pub fn with_service_name(mut self, name: &str) -> Self {
        self.config.base_config.service_name = name.to_string();
        self
    }

    /// Set the ingestd socket path
    pub fn with_ingestd_socket(mut self, socket_path: &str) -> Self {
        self.config.base_config.ingest_socket_path = socket_path.to_string();
        self
    }

    /// Set event generation interval
    pub fn with_interval(mut self, interval_ms: u64) -> Self {
        self.config.event_interval_ms = interval_ms;
        self
    }

    /// Set maximum events to generate
    pub fn with_max_events(mut self, max_events: usize) -> Self {
        self.config.max_events = Some(max_events);
        self
    }

    /// Set unlimited event generation
    pub fn unlimited_events(mut self) -> Self {
        self.config.max_events = None;
        self
    }

    /// Set event template
    pub fn with_event_template(
        mut self,
        source: &str,
        event_type: &str,
        payload: serde_json::Value,
    ) -> Self {
        self.config.event_template = EventTemplate {
            source: source.to_string(),
            event_type: event_type.to_string(),
            base_payload: payload,
        };
        self
    }

    /// Set connection failure rate
    pub fn with_failure_rate(mut self, failure_rate: f64) -> Self {
        self.config.connection_failure_rate = failure_rate;
        self
    }

    /// Set batch size
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.config.batch_size = batch_size;
        self
    }

    /// Build the mock satellite
    pub fn build(self) -> MockSatellite {
        MockSatellite::new(self.config)
    }
}

impl Default for MockSatelliteBuilder {
    fn default() -> Self {
        Self::new()
    }
}
