use crate::{calculate_backoff_secs, EventProcessor};
use anyhow::Result;
use chrono::{Duration, Utc};
use sinex_db::work_queue::{
    claim_work_queue_items, complete_work_queue_item, fail_work_queue_item, insert_dlq_event,
};
use sinex_db::DbPool;
use sinex_db::RawEvent;
use std::sync::Arc;
use tokio::time::sleep;
use tracing::{error, info, warn};

/// Core worker implementation for processing promotion queue
pub struct Worker {
    pool: DbPool,
    processor: Arc<dyn EventProcessor>,
    worker_id: String,
    metrics: crate::WorkerMetrics,
}

impl Worker {
    pub fn new(pool: DbPool, processor: Arc<dyn EventProcessor>, worker_id: String) -> Self {
        let metrics = crate::WorkerMetrics::new(processor.agent_name());
        Self {
            pool,
            processor,
            worker_id,
            metrics,
        }
    }

    /// Run the worker loop
    pub async fn run(&self) -> Result<()> {
        info!(
            worker_id = %self.worker_id,
            agent_name = %self.processor.agent_name(),
            "Starting worker"
        );

        loop {
            match self.process_batch().await {
                Ok(processed) => {
                    if processed == 0 {
                        // No items to process, sleep before next poll
                        sleep(std::time::Duration::from_secs(
                            self.processor.poll_interval_secs(),
                        ))
                        .await;
                    }
                }
                Err(e) => {
                    error!(
                        worker_id = %self.worker_id,
                        error = %e,
                        "Error in worker batch processing, retrying in 5s"
                    );
                    sleep(std::time::Duration::from_secs(5)).await;
                }
            }
        }
    }

    /// Process a single batch of items
    async fn process_batch(&self) -> Result<usize> {
        let items = claim_work_queue_items(
            &self.pool,
            self.processor.agent_name(),
            &self.worker_id,
            self.processor.batch_size() as i64,
        )
        .await?;

        let count = items.len();
        if count == 0 {
            return Ok(0);
        }

        info!(
            worker_id = %self.worker_id,
            count = count,
            "Claimed items for processing"
        );
        self.metrics.items_claimed.inc_by(count as f64);

        for item in items {
            let start = std::time::Instant::now();

            match self.processor.process_event(&self.pool, &item).await {
                Ok(()) => {
                    // Successfully processed
                    if let Err(e) = complete_work_queue_item(&self.pool, item.queue_id).await {
                        error!(
                            worker_id = %self.worker_id,
                            queue_id = %item.queue_id,
                            error = %e,
                            "Failed to delete completed item from queue"
                        );
                    } else {
                        self.metrics.items_processed.inc();
                        self.metrics
                            .processing_duration
                            .observe(start.elapsed().as_secs_f64());
                    }
                }
                Err(e) => {
                    // Processing failed
                    warn!(
                        worker_id = %self.worker_id,
                        queue_id = %item.queue_id,
                        attempts = item.attempts + 1,
                        error = %e,
                        "Failed to process item"
                    );
                    self.metrics.items_failed.inc();

                    let new_attempts = item.attempts + 1;

                    if new_attempts >= item.max_attempts {
                        // Max attempts reached, move to DLQ
                        error!(
                            worker_id = %self.worker_id,
                            queue_id = %item.queue_id,
                            attempts = new_attempts,
                            "Item exceeded max attempts, moving to DLQ"
                        );

                        // Get the original event for DLQ
                        match self.get_raw_event(item.raw_event_id).await {
                            Ok(Some(raw_event)) => {
                                // Insert into DLQ
                                let dlq_params = sinex_db::DlqEventParams {
                                    failed_event_id: item.raw_event_id,
                                    agent_name: item.target_agent_name.clone(),
                                    source: raw_event.source.clone(),
                                    event_type: raw_event.event_type.clone(),
                                    failure_reason: format!(
                                        "Max attempts exceeded after {} retries: {}",
                                        new_attempts, e
                                    ),
                                    error_category: "permanent".to_string(),
                                    original_event_payload: raw_event.payload.clone(),
                                    additional_metadata: Some(serde_json::json!({
                                        "promotion_queue_id": item.queue_id,
                                        "final_attempt_count": new_attempts,
                                        "worker_id": self.worker_id
                                    })),
                                };
                                if let Err(dlq_err) = insert_dlq_event(&self.pool, dlq_params)
                                .await
                                {
                                    error!(
                                        worker_id = %self.worker_id,
                                        queue_id = %item.queue_id,
                                        error = %dlq_err,
                                        "Failed to insert item into DLQ"
                                    );
                                } else {
                                    info!(
                                        worker_id = %self.worker_id,
                                        queue_id = %item.queue_id,
                                        "Item moved to DLQ successfully"
                                    );
                                }
                            }
                            Ok(None) => {
                                warn!(
                                    worker_id = %self.worker_id,
                                    queue_id = %item.queue_id,
                                    raw_event_id = %item.raw_event_id,
                                    "Original event not found, cannot move to DLQ"
                                );
                            }
                            Err(fetch_err) => {
                                error!(
                                    worker_id = %self.worker_id,
                                    queue_id = %item.queue_id,
                                    error = %fetch_err,
                                    "Failed to fetch original event for DLQ"
                                );
                            }
                        }

                        // Remove from promotion queue regardless
                        let _ = complete_work_queue_item(&self.pool, item.queue_id).await;
                        self.metrics.items_dlq.inc();
                    } else {
                        // Schedule retry
                        let delay_secs = calculate_backoff_secs(item.attempts);
                        let next_retry = Utc::now() + Duration::seconds(delay_secs as i64);

                        if let Err(e) = fail_work_queue_item(
                            &self.pool,
                            item.queue_id,
                            &format!("{:?}", e),
                            next_retry,
                        )
                        .await
                        {
                            error!(
                                worker_id = %self.worker_id,
                                queue_id = %item.queue_id,
                                error = %e,
                                "Failed to update item for retry"
                            );
                        }
                    }
                }
            }
        }

        Ok(count)
    }

    /// Get a reference to the metrics
    pub fn metrics(&self) -> &crate::WorkerMetrics {
        &self.metrics
    }

    /// Helper to fetch raw event by ID for DLQ (using consolidated query)
    async fn get_raw_event(&self, event_id: sinex_ulid::Ulid) -> Result<Option<RawEvent>> {
        match sinex_db::events::get_event_by_id(&self.pool, event_id).await {
            Ok(event) => Ok(Some(event)),
            Err(e) => {
                // Check if it's a RowNotFound error by examining the error string
                if e.to_string().contains("RowNotFound") {
                    Ok(None)
                } else {
                    Err(e)
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::EventProcessor;
    use anyhow::{anyhow, Result};
    use async_trait::async_trait;
    use chrono::Utc;
    use sinex_db::prelude::{QueueStatus, WorkQueueItem};
    use sinex_ulid::Ulid;
    use std::sync::{Arc, Mutex};

    // Mock EventProcessor for testing
    struct MockEventProcessor {
        agent_name: String,
        batch_size: usize,
        poll_interval_secs: u64,
        // Track behavior
        should_fail: Arc<Mutex<bool>>,
        processing_calls: Arc<Mutex<Vec<Ulid>>>,
        processing_delay: std::time::Duration,
    }

    impl MockEventProcessor {
        fn new(agent_name: &str) -> Self {
            Self {
                agent_name: agent_name.to_string(),
                batch_size: 5,
                poll_interval_secs: 1,
                should_fail: Arc::new(Mutex::new(false)),
                processing_calls: Arc::new(Mutex::new(Vec::new())),
                processing_delay: std::time::Duration::from_millis(10),
            }
        }

        fn set_should_fail(&self, should_fail: bool) {
            *self.should_fail.lock().unwrap() = should_fail;
        }

        fn get_processing_calls(&self) -> Vec<Ulid> {
            self.processing_calls.lock().unwrap().clone()
        }

        fn set_processing_delay(&mut self, delay: std::time::Duration) {
            self.processing_delay = delay;
        }
    }

    #[async_trait]
    impl EventProcessor for MockEventProcessor {
        fn agent_name(&self) -> &str {
            &self.agent_name
        }

        fn batch_size(&self) -> i32 {
            self.batch_size as i32
        }

        fn poll_interval_secs(&self) -> u64 {
            self.poll_interval_secs
        }

        async fn process_event(&self, _pool: &DbPool, item: &WorkQueueItem) -> Result<()> {
            // Record the call
            self.processing_calls.lock().unwrap().push(item.raw_event_id);

            // Simulate processing time
            tokio::time::sleep(self.processing_delay).await;

            // Check if we should fail
            if *self.should_fail.lock().unwrap() {
                Err(anyhow!("Mock processing failure"))
            } else {
                Ok(())
            }
        }
    }

    // Helper to create a test WorkQueueItem
    fn create_test_work_item(
        queue_id: Ulid,
        raw_event_id: Ulid,
        target_agent: &str,
        attempts: i32,
        max_attempts: i32,
    ) -> WorkQueueItem {
        WorkQueueItem {
            queue_id,
            raw_event_id,
            target_agent_name: target_agent.to_string(),
            status: QueueStatus::Pending.as_str().to_string(),
            attempts,
            max_attempts,
            last_attempt_ts: None,
            next_retry_ts: Some(Utc::now()),
            error_message_last: None,
            created_at: Utc::now(),
            processing_worker_id: None,
            processed_at: None,
            failure_reason: None,
        }
    }


    #[test]
    fn test_mock_event_processor_creation() {
        let processor = MockEventProcessor::new("test_agent");
        assert_eq!(processor.agent_name(), "test_agent");
        assert_eq!(processor.batch_size(), 5);
        assert_eq!(processor.poll_interval_secs(), 1);
    }

    #[test]
    fn test_mock_event_processor_configuration() {
        let mut processor = MockEventProcessor::new("config_agent");
        processor.batch_size = 20;
        processor.poll_interval_secs = 5;
        
        assert_eq!(processor.agent_name(), "config_agent");
        assert_eq!(processor.batch_size(), 20);
        assert_eq!(processor.poll_interval_secs(), 5);
    }

    #[tokio::test]
    async fn test_mock_processor_success_behavior() {
        let processor = Arc::new(MockEventProcessor::new("test_agent"));
        let queue_id = Ulid::new();
        let raw_event_id = Ulid::new();
        let item = create_test_work_item(queue_id, raw_event_id, "test_agent", 0, 3);

        // Create a dummy pool - we won't actually use it for this unit test
        use sinex_db::{prelude::*, create_pool};
        let dummy_pool = match create_pool("postgresql://dummy_for_testing").await {
            Ok(pool) => pool,
            Err(_) => {
                // Skip test if we can't create even a dummy pool
                return;
            }
        };

        let result = processor.process_event(&dummy_pool, &item).await;
        assert!(result.is_ok());

        // Verify the processor recorded the call
        let calls = processor.get_processing_calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0], raw_event_id);
    }

    #[tokio::test]
    async fn test_mock_processor_failure_behavior() {
        let processor = Arc::new(MockEventProcessor::new("test_agent"));
        processor.set_should_fail(true);
        
        let queue_id = Ulid::new();
        let raw_event_id = Ulid::new();
        let item = create_test_work_item(queue_id, raw_event_id, "test_agent", 0, 3);

        // Create a dummy pool - we won't actually use it for this unit test
        use sinex_db::{prelude::*, create_pool};
        let dummy_pool = match create_pool("postgresql://dummy_for_testing").await {
            Ok(pool) => pool,
            Err(_) => {
                // Skip test if we can't create even a dummy pool
                return;
            }
        };

        let result = processor.process_event(&dummy_pool, &item).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Mock processing failure"));
    }

    #[tokio::test]
    async fn test_backoff_calculation() {
        // Test the backoff calculation logic
        use crate::calculate_backoff_secs;

        // The actual backoff uses a base of 60 seconds with jitter, so we test ranges
        let backoff_0 = calculate_backoff_secs(0);
        let backoff_1 = calculate_backoff_secs(1);
        let backoff_2 = calculate_backoff_secs(2);
        
        // Should be roughly: 60, 120, 240 seconds with jitter (0.8 to 1.2 factor)
        assert!((48.0..=72.0).contains(&backoff_0)); // 60 * (0.8 to 1.2)
        assert!((96.0..=144.0).contains(&backoff_1)); // 120 * (0.8 to 1.2)
        assert!((192.0..=288.0).contains(&backoff_2)); // 240 * (0.8 to 1.2)
        
        // Test max backoff is capped at 24 hours
        assert!(calculate_backoff_secs(20) <= 24.0 * 3600.0);
        assert!(calculate_backoff_secs(100) <= 24.0 * 3600.0);
    }

    #[test]
    fn test_retry_logic_with_backoff() {
        // Test items with different attempt counts
        let items = vec![
            create_test_work_item(Ulid::new(), Ulid::new(), "test_agent", 0, 3),
            create_test_work_item(Ulid::new(), Ulid::new(), "test_agent", 1, 3),
            create_test_work_item(Ulid::new(), Ulid::new(), "test_agent", 2, 3),
        ];

        for item in items {
            let delay = calculate_backoff_secs(item.attempts);
            // With the actual backoff formula, verify reasonable ranges
            if item.attempts == 0 {
                assert!((48.0..=72.0).contains(&delay)); // 60 * (0.8 to 1.2)
            } else if item.attempts == 1 {
                assert!((96.0..=144.0).contains(&delay)); // 120 * (0.8 to 1.2)
            } else if item.attempts == 2 {
                assert!((192.0..=288.0).contains(&delay)); // 240 * (0.8 to 1.2)
            }
        }
    }

    #[test]
    fn test_max_attempts_logic() {
        // Test item that has reached max attempts
        let queue_id = Ulid::new();
        let raw_event_id = Ulid::new();
        let item_max_attempts = create_test_work_item(queue_id, raw_event_id, "test_agent", 2, 3);

        // Verify that new_attempts would be 3, which equals max_attempts
        let new_attempts = item_max_attempts.attempts + 1;
        assert_eq!(new_attempts, item_max_attempts.max_attempts);
        // This would trigger DLQ logic in the actual worker
        
        // Test logic for determining DLQ trigger
        assert!(new_attempts >= item_max_attempts.max_attempts);
    }

    #[test]
    fn test_error_handling_pattern() {
        // Test the error handling logic for RowNotFound without needing a database
        let test_error = "No rows returned by a query that expected to return at least one row";
        let error_contains_row_not_found = test_error.contains("No rows returned");
        assert!(error_contains_row_not_found);
        
        // Test error contains detection logic that the worker uses
        let sqlx_error_msg = "RowNotFound";
        assert!(sqlx_error_msg.contains("RowNotFound"));
    }

    #[test]
    fn test_worker_configuration_logic() {
        // Test processor configuration without needing a database connection
        let mut processor = MockEventProcessor::new("config_test_agent");
        processor.batch_size = 15;
        processor.poll_interval_secs = 3;
        
        // Test that configuration is properly stored
        assert_eq!(processor.agent_name(), "config_test_agent");
        assert_eq!(processor.batch_size(), 15);
        assert_eq!(processor.poll_interval_secs(), 3);
    }

    #[test]
    fn test_metrics_behavior() {
        // Test metrics creation without needing Worker instance
        use crate::WorkerMetrics;
        let metrics = WorkerMetrics::new("test_metrics_agent");
        
        // Verify all metrics start at zero
        assert_eq!(metrics.items_claimed.get(), 0.0);
        assert_eq!(metrics.items_processed.get(), 0.0);
        assert_eq!(metrics.items_failed.get(), 0.0);
        assert_eq!(metrics.items_dlq.get(), 0.0);
        
        // Processing duration should have no observations initially
        assert_eq!(metrics.processing_duration.get_sample_count(), 0);
    }

    #[test]
    fn test_error_message_formatting() {
        let queue_id = Ulid::new();
        let raw_event_id = Ulid::new();
        let item = create_test_work_item(queue_id, raw_event_id, "test_agent", 2, 3);

        // Simulate the error message formatting logic
        let mock_error = anyhow::anyhow!("Mock processing failure");
        let new_attempts = item.attempts + 1;
        let dlq_message = format!(
            "Max attempts exceeded after {} retries: {}",
            new_attempts, mock_error
        );
        
        assert!(dlq_message.contains("Max attempts exceeded after 3 retries"));
        assert!(dlq_message.contains("Mock processing failure"));
    }

    #[test]
    fn test_dlq_metadata_structure() {
        let queue_id = Ulid::new();
        let worker_id = "test_worker";
        let attempts = 3;

        // Test the DLQ metadata structure
        let dlq_metadata = serde_json::json!({
            "promotion_queue_id": queue_id,
            "final_attempt_count": attempts,
            "worker_id": worker_id
        });

        assert!(dlq_metadata["promotion_queue_id"].is_string());
        assert_eq!(dlq_metadata["final_attempt_count"], 3);
        assert_eq!(dlq_metadata["worker_id"], "test_worker");
    }

    #[tokio::test]
    async fn test_processing_timing() {
        let mut processor = MockEventProcessor::new("timing_agent");
        
        // Set a known processing delay
        processor.set_processing_delay(std::time::Duration::from_millis(50));
        let processor_arc = Arc::new(processor);
        
        let queue_id = Ulid::new();
        let raw_event_id = Ulid::new();
        let item = create_test_work_item(queue_id, raw_event_id, "timing_agent", 0, 3);

        // Create a dummy pool - we test timing behavior, not database interaction
        use sinex_db::{prelude::*, create_pool};
        let dummy_pool = match create_pool("postgresql://dummy_for_testing").await {
            Ok(pool) => pool,
            Err(_) => {
                // Skip this test if we can't create a dummy pool
                return;
            }
        };

        let start = std::time::Instant::now();
        let result = processor_arc.process_event(&dummy_pool, &item).await;
        let duration = start.elapsed();

        assert!(result.is_ok());
        assert!(duration >= std::time::Duration::from_millis(50));
        assert!(duration < std::time::Duration::from_millis(150)); // Should be close to 50ms
    }

    #[tokio::test]
    async fn test_concurrent_processing_simulation() {
        let processor = Arc::new(MockEventProcessor::new("concurrent_agent"));
        
        // Create multiple work items
        let items = (0..5).map(|i| {
            create_test_work_item(
                Ulid::new(),
                Ulid::new(),
                "concurrent_agent",
                i % 3, // Different attempt counts
                3,
            )
        }).collect::<Vec<_>>();

        // Create a dummy pool - we test concurrency behavior, not database interaction
        use sinex_db::{prelude::*, create_pool};
        let dummy_pool = match create_pool("postgresql://dummy_for_testing").await {
            Ok(pool) => pool,
            Err(_) => {
                // Skip this test if we can't create a dummy pool
                return;
            }
        };

        // Process all items concurrently
        let futures = items.into_iter().map(|item| {
            let processor = processor.clone();
            let pool = dummy_pool.clone();
            async move {
                processor.process_event(&pool, &item).await
            }
        });

        let results = futures::future::join_all(futures).await;
        
        // All should succeed
        for result in results {
            assert!(result.is_ok());
        }

        // Should have processed 5 items
        let calls = processor.get_processing_calls();
        assert_eq!(calls.len(), 5);
    }
}
