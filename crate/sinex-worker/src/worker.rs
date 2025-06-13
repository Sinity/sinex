use crate::{calculate_backoff_secs, EventProcessor};
use anyhow::Result;
use chrono::{Duration, Utc};
use sinex_db::queries::{
    claim_promotion_queue_items, complete_promotion_queue_item, fail_promotion_queue_item,
    insert_dlq_event,
};
use sinex_db::models::RawEvent;
use sqlx::PgPool;
use std::sync::Arc;
use tokio::time::sleep;
use tracing::{error, info, warn};

/// Core worker implementation for processing promotion queue
pub struct Worker {
    pool: PgPool,
    processor: Arc<dyn EventProcessor>,
    worker_id: String,
    metrics: crate::WorkerMetrics,
}

impl Worker {
    pub fn new(pool: PgPool, processor: Arc<dyn EventProcessor>, worker_id: String) -> Self {
        let metrics = crate::WorkerMetrics::new(&processor.agent_name());
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
        let items = claim_promotion_queue_items(
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
                    if let Err(e) = complete_promotion_queue_item(&self.pool, item.queue_id.into()).await {
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
                                if let Err(dlq_err) = insert_dlq_event(
                                    &self.pool,
                                    item.raw_event_id,
                                    &item.target_agent_name,
                                    &raw_event.source,
                                    &raw_event.event_type,
                                    &format!("Max attempts exceeded after {} retries: {}", new_attempts, e),
                                    "permanent", // Permanent failure after max retries
                                    raw_event.payload,
                                    Some(serde_json::json!({
                                        "promotion_queue_id": item.queue_id,
                                        "final_attempt_count": new_attempts,
                                        "worker_id": self.worker_id
                                    })),
                                ).await {
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
                        let _ = complete_promotion_queue_item(&self.pool, item.queue_id).await;
                        self.metrics.items_dlq.inc();
                    } else {
                        // Schedule retry
                        let delay_secs = calculate_backoff_secs(item.attempts);
                        let next_retry = Utc::now() + Duration::seconds(delay_secs as i64);
                        
                        if let Err(e) = fail_promotion_queue_item(
                            &self.pool,
                            item.queue_id.into(),
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
    
    /// Helper to fetch raw event by ID for DLQ
    async fn get_raw_event(&self, event_id: sinex_ulid::Ulid) -> Result<Option<RawEvent>> {
        let record = sqlx::query!(
            r#"
            SELECT 
                id::uuid as "id!",
                source as "source!",
                event_type as "event_type!",
                ts_ingest as "ts_ingest!",
                ts_orig,
                host as "host!",
                ingestor_version,
                payload_schema_id::uuid as "payload_schema_id",
                payload as "payload!"
            FROM raw.events
            WHERE id = $1::uuid::ulid
            "#,
            event_id.to_uuid()
        )
        .fetch_optional(&self.pool)
        .await?;
        
        if let Some(record) = record {
            Ok(Some(RawEvent {
                id: sinex_ulid::Ulid::from_uuid(record.id),
                source: record.source,
                event_type: record.event_type,
                ts_ingest: record.ts_ingest,
                ts_orig: record.ts_orig,
                host: record.host,
                ingestor_version: record.ingestor_version,
                payload_schema_id: record.payload_schema_id.map(sinex_ulid::Ulid::from_uuid),
                payload: record.payload,
            }))
        } else {
            Ok(None)
        }
    }
}