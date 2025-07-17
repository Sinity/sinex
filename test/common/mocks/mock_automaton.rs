// Mock automaton implementation for testing
//
// Provides a controllable automaton that can:
// - Process events from Redis streams
// - Maintain checkpoints
// - Simulate various processing scenarios

use crate::common::prelude::*;
use redis::aio::MultiplexedConnection;
use sinex_events::RawEvent;
use sinex_satellite_sdk::{checkpoint::{CheckpointManager, CheckpointState}, stream_processor::Checkpoint};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

/// Configuration for mock automaton behavior
#[derive(Debug, Clone)]
pub struct MockAutomatonConfig {
    /// Automaton name/type
    pub automaton_name: String,

    /// Consumer group name
    pub consumer_group: String,

    /// Consumer name within group
    pub consumer_name: String,

    /// Redis stream to consume from
    pub stream_key: String,

    /// Processing interval (milliseconds)
    pub processing_interval_ms: u64,

    /// Events to process per batch
    pub batch_size: usize,

    /// Processing delay per event (milliseconds)
    pub processing_delay_ms: u64,

    /// Failure rate for processing (0.0 - 1.0)
    pub processing_failure_rate: f64,

    /// Whether to save checkpoints
    pub save_checkpoints: bool,

    /// Maximum events to process before stopping
    pub max_events: Option<usize>,

    /// Custom processing function
    pub custom_processor: Option<ProcessorFunction>,
}

impl Default for MockAutomatonConfig {
    fn default() -> Self {
        Self {
            automaton_name: "mock-automaton".to_string(),
            consumer_group: "mock-group".to_string(),
            consumer_name: "mock-consumer".to_string(),
            stream_key: "test:events".to_string(),
            processing_interval_ms: 100,
            batch_size: 10,
            processing_delay_ms: 0,
            processing_failure_rate: 0.0,
            save_checkpoints: true,
            max_events: None,
            custom_processor: None,
        }
    }
}

/// Custom processing function type
pub type ProcessorFunction = Arc<dyn Fn(&RawEvent) -> AnyhowResult<ProcessingResult> + Send + Sync>;

/// Result of processing an event
#[derive(Debug, Clone)]
pub struct ProcessingResult {
    pub success: bool,
    pub output_events: Vec<RawEvent>,
    pub metadata: Option<serde_json::Value>,
}

impl Default for ProcessingResult {
    fn default() -> Self {
        Self {
            success: true,
            output_events: Vec::new(),
            metadata: None,
        }
    }
}

/// Mock automaton for testing
pub struct MockAutomaton {
    pub id: String,
    pub config: MockAutomatonConfig,
    pub checkpoint_manager: CheckpointManager,
    pub events_processed: Arc<Mutex<Vec<String>>>,
    pub processing_results: Arc<Mutex<Vec<ProcessingResult>>>,
    pub task_handle: Option<JoinHandle<()>>,
    pub is_running: Arc<Mutex<bool>>,
    redis: sinex_satellite_sdk::redis_client::RedisStreamClient,
}

impl MockAutomaton {
    /// Create a new mock automaton
    pub fn new(
        config: MockAutomatonConfig,
        pool: sqlx::PgPool,
        redis: sinex_satellite_sdk::redis_client::RedisStreamClient,
    ) -> Self {
        let id = format!("{}-{}", config.automaton_name, sinex_ulid::Ulid::new());

        let checkpoint_manager = CheckpointManager::new(
            pool,
            config.automaton_name.clone(),
            config.consumer_group.clone(),
            config.consumer_name.clone(),
        );

        Self {
            id,
            config,
            checkpoint_manager,
            events_processed: Arc::new(Mutex::new(Vec::new())),
            processing_results: Arc::new(Mutex::new(Vec::new())),
            task_handle: None,
            is_running: Arc::new(Mutex::new(false)),
            redis,
        }
    }

    /// Start the mock automaton
    pub async fn start(&mut self) -> AnyhowResult<()> {
        *self.is_running.lock().await = true;

        let config = self.config.clone();
        let events_processed = self.events_processed.clone();
        let processing_results = self.processing_results.clone();
        let is_running = self.is_running.clone();
        let mut redis = self.redis.clone();
        let checkpoint_manager = self.checkpoint_manager.clone();

        let task_handle = tokio::spawn(async move {
            use redis::AsyncCommands;

            let mut processed_count = 0usize;

            // Create consumer group if it doesn't exist
            let _: Result<String, redis::RedisError> = redis
                .xgroup_create(&config.stream_key, &config.consumer_group, "$")
                .await;

            let interval = std::time::Duration::from_millis(config.processing_interval_ms);
            let mut ticker = tokio::time::interval(interval);

            while *is_running.lock().await {
                ticker.tick().await;

                // Check if we've reached the maximum
                if let Some(max) = config.max_events {
                    if processed_count >= max {
                        break;
                    }
                }

                // Process batch of events
                match process_event_batch(
                    &mut redis,
                    &config,
                    &events_processed,
                    &processing_results,
                )
                .await
                {
                    Ok(batch_count) => {
                        if batch_count > 0 {
                            processed_count += batch_count;

                            // Save checkpoint if configured
                            if config.save_checkpoints {
                                let state = CheckpointState {
                                    checkpoint: Checkpoint::None, // No specific event processed yet
                                    processed_count: processed_count as u64,
                                    last_activity: chrono::Utc::now(),
                                    data: None,
                                    version: 2,
                                };
                                let _ = checkpoint_manager.save_checkpoint(&state).await;
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("Error processing events: {}", e);
                    }
                }
            }
        });

        self.task_handle = Some(task_handle);
        Ok(())
    }

    /// Stop the mock automaton
    pub async fn stop(&mut self) -> AnyhowResult<()> {
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

    /// Get current checkpoint state
    pub async fn get_checkpoint(&self) -> AnyhowResult<CheckpointState> {
        self.checkpoint_manager.load_checkpoint().await
    }

    /// Get events processed by this automaton
    pub async fn get_processed_events(&self) -> Vec<String> {
        self.events_processed.lock().await.clone()
    }

    /// Get processing results
    pub async fn get_processing_results(&self) -> Vec<ProcessingResult> {
        self.processing_results.lock().await.clone()
    }

    /// Get count of events processed
    pub async fn processed_count(&self) -> usize {
        self.events_processed.lock().await.len()
    }

    /// Wait for automaton to process expected number of events
    pub async fn wait_for_processing(
        &self,
        expected: usize,
        timeout_secs: u64,
    ) -> AnyhowResult<()> {
        let timeout = std::time::Duration::from_secs(timeout_secs);
        let start = std::time::Instant::now();

        loop {
            let count = self.processed_count().await;
            if count >= expected {
                return Ok(());
            }

            if start.elapsed() > timeout {
                return Err(anyhow::anyhow!(
                    "Timeout waiting for {} events to be processed, got {}",
                    expected,
                    count
                ));
            }

            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }

    /// Wait for checkpoint to reach expected count
    pub async fn wait_for_checkpoint(
        &self,
        expected_count: u64,
        timeout_secs: u64,
    ) -> AnyhowResult<CheckpointState> {
        let timeout = std::time::Duration::from_secs(timeout_secs);
        let start = std::time::Instant::now();

        loop {
            let checkpoint = self.get_checkpoint().await?;
            if checkpoint.processed_count >= expected_count {
                return Ok(checkpoint);
            }

            if start.elapsed() > timeout {
                return Err(anyhow::anyhow!(
                    "Timeout waiting for checkpoint to reach {}, got {}",
                    expected_count,
                    checkpoint.processed_count
                ));
            }

            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }

    /// Check if automaton is running
    pub async fn is_running(&self) -> bool {
        *self.is_running.lock().await
    }

    /// Get success rate of processing
    pub async fn success_rate(&self) -> f64 {
        let results = self.processing_results.lock().await;
        if results.is_empty() {
            return 1.0;
        }

        let successes = results.iter().filter(|r| r.success).count();
        successes as f64 / results.len() as f64
    }
}

/// Process a batch of events from Redis stream
async fn process_event_batch(
    redis: &mut sinex_satellite_sdk::redis_client::RedisStreamClient,
    config: &MockAutomatonConfig,
    events_processed: &Arc<Mutex<Vec<String>>>,
    processing_results: &Arc<Mutex<Vec<ProcessingResult>>>,
) -> AnyhowResult<usize> {
    use redis::{cmd, AsyncCommands};

    // Get connection from RedisStreamClient
    let mut conn = redis.get_connection().await
        .map_err(|e| anyhow::anyhow!("Failed to get Redis connection: {}", e))?;

    // Read events from stream using XREADGROUP command
    let result: redis::streams::StreamReadReply = cmd("XREADGROUP")
        .arg("GROUP")
        .arg(&config.consumer_group)
        .arg(&config.consumer_name)
        .arg("COUNT")
        .arg(config.batch_size)
        .arg("STREAMS")
        .arg(&config.stream_key)
        .arg(">")
        .query_async(&mut conn)
        .await?;

    let mut processed_count = 0;

    for stream_key in result.keys {
        for stream_id in stream_key.ids {
            // Convert HashMap<String, Value> to Vec<(String, String)> for compatibility
            let fields: Vec<(String, String)> = stream_id.map.into_iter()
                .filter_map(|(k, v)| {
                    if let redis::Value::Data(data) = v {
                        Some((k, String::from_utf8_lossy(&data).to_string()))
                    } else {
                        None
                    }
                })
                .collect();
            
            // Extract event from fields
            let event = extract_event_from_fields(&fields)?;

            // Simulate processing delay
            if config.processing_delay_ms > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(config.processing_delay_ms))
                    .await;
            }

            // Process the event
            let processing_result = if let Some(ref processor) = config.custom_processor {
                processor(&event)?
            } else {
                default_process_event(&event, config)
            };

            // Record processing result
            processing_results
                .lock()
                .await
                .push(processing_result.clone());

            if processing_result.success {
                // Acknowledge the message
                let _: i64 = conn
                    .xack(&config.stream_key, &config.consumer_group, &[&stream_id.id])
                    .await?;

                // Record as processed
                events_processed.lock().await.push(stream_id.id.clone());
                processed_count += 1;
            }
        }
    }

    Ok(processed_count)
}

/// Extract event from Redis stream fields
fn extract_event_from_fields(fields: &[(String, String)]) -> AnyhowResult<RawEvent> {
    for (key, value) in fields {
        if key == "event" {
            return serde_json::from_str(value)
                .map_err(|e| anyhow::anyhow!("Failed to parse event: {}", e));
        }
    }
    Err(anyhow::anyhow!("No event field found in stream message"))
}

/// Default event processing function
fn default_process_event(event: &RawEvent, config: &MockAutomatonConfig) -> ProcessingResult {
    // Simulate processing failures
    if config.processing_failure_rate > 0.0 {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        if rng.gen::<f64>() < config.processing_failure_rate {
            return ProcessingResult {
                success: false,
                output_events: Vec::new(),
                metadata: Some(serde_json::json!({
                    "error": "Simulated processing failure",
                    "event_id": event.id.to_string()
                })),
            };
        }
    }

    ProcessingResult {
        success: true,
        output_events: Vec::new(),
        metadata: Some(serde_json::json!({
            "processed_at": chrono::Utc::now(),
            "event_id": event.id.to_string(),
            "automaton": config.automaton_name
        })),
    }
}

/// Builder for mock automaton configuration
pub struct MockAutomatonBuilder {
    config: MockAutomatonConfig,
}

impl MockAutomatonBuilder {
    /// Create a new mock automaton builder
    pub fn new(automaton_name: &str) -> Self {
        let mut config = MockAutomatonConfig::default();
        config.automaton_name = automaton_name.to_string();
        config.consumer_group = format!("{}-group", automaton_name);
        config.consumer_name = format!("{}-consumer", automaton_name);

        Self { config }
    }

    /// Set the Redis stream to consume from
    pub fn with_stream(mut self, stream_key: &str) -> Self {
        self.config.stream_key = stream_key.to_string();
        self
    }

    /// Set processing interval
    pub fn with_interval(mut self, interval_ms: u64) -> Self {
        self.config.processing_interval_ms = interval_ms;
        self
    }

    /// Set batch size
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.config.batch_size = batch_size;
        self
    }

    /// Set processing delay per event
    pub fn with_processing_delay(mut self, delay_ms: u64) -> Self {
        self.config.processing_delay_ms = delay_ms;
        self
    }

    /// Set processing failure rate
    pub fn with_failure_rate(mut self, failure_rate: f64) -> Self {
        self.config.processing_failure_rate = failure_rate;
        self
    }

    /// Disable checkpoint saving
    pub fn without_checkpoints(mut self) -> Self {
        self.config.save_checkpoints = false;
        self
    }

    /// Set maximum events to process
    pub fn with_max_events(mut self, max_events: usize) -> Self {
        self.config.max_events = Some(max_events);
        self
    }

    /// Set unlimited event processing
    pub fn unlimited_processing(mut self) -> Self {
        self.config.max_events = None;
        self
    }

    /// Set custom processing function
    pub fn with_custom_processor(mut self, processor: ProcessorFunction) -> Self {
        self.config.custom_processor = Some(processor);
        self
    }

    /// Build the mock automaton
    pub fn build(
        self,
        pool: sqlx::PgPool,
        redis: sinex_satellite_sdk::redis_client::RedisStreamClient,
    ) -> MockAutomaton {
        MockAutomaton::new(self.config, pool, redis)
    }
}

impl Default for MockAutomatonBuilder {
    fn default() -> Self {
        Self::new("default-mock")
    }
}
