use anyhow::Result;
use sinex_db::models::PromotionQueueItem;
use sinex_worker::{EventProcessor, WorkerMetrics, calculate_backoff_secs};
use sqlx::PgPool;
use std::sync::{Arc, atomic::{AtomicBool, AtomicU32, Ordering}};
use std::time::Duration;
use sinex_ulid::Ulid;
use async_trait::async_trait;

// Import test setup macros
use crate::db_test;

async fn insert_test_promotion_item(pool: &PgPool, agent_name: &str) -> Result<Ulid> {
    let queue_id = Ulid::new();
    let raw_event_id = Ulid::new();
    
    // Insert a raw event first
    sqlx::query(
        "INSERT INTO raw.events (id, source, event_type, ts_ingest, host, payload) 
         VALUES ($1, $2, $3, NOW(), $4, $5)"
    )
    .bind(raw_event_id.to_uuid())
    .bind("test_source")
    .bind("test_event")
    .bind("test_host")
    .bind(serde_json::json!({"test": "data"}))
    .execute(pool)
    .await?;
    
    // Insert into promotion queue
    sqlx::query(
        "INSERT INTO sinex_schemas.promotion_queue 
         (queue_id, raw_event_id, target_agent_name, status, attempts, max_attempts, created_at) 
         VALUES ($1, $2, $3, 'pending', 0, 3, NOW())"
    )
    .bind(queue_id.to_uuid())
    .bind(raw_event_id.to_uuid())
    .bind(agent_name)
    .execute(pool)
    .await?;
    
    Ok(queue_id)
}

struct TestEventProcessor {
    agent_name: String,
    process_count: Arc<AtomicU32>,
    should_fail: Arc<AtomicBool>,
    processing_delay: Duration,
}

impl TestEventProcessor {
    fn new(agent_name: String) -> Self {
        Self {
            agent_name,
            process_count: Arc::new(AtomicU32::new(0)),
            should_fail: Arc::new(AtomicBool::new(false)),
            processing_delay: Duration::from_millis(10),
        }
    }
}

#[async_trait]
impl EventProcessor for TestEventProcessor {
    async fn process_event(
        &self,
        _pool: &PgPool,
        _item: &PromotionQueueItem,
    ) -> Result<()> {
        self.process_count.fetch_add(1, Ordering::SeqCst);
        
        tokio::time::sleep(self.processing_delay).await;
        
        if self.should_fail.load(Ordering::SeqCst) {
            return Err(anyhow::anyhow!("Test processor failure"));
        }
        
        Ok(())
    }
    
    fn agent_name(&self) -> &str {
        &self.agent_name
    }
    
    fn batch_size(&self) -> i32 {
        1
    }
    
    fn poll_interval_secs(&self) -> u64 {
        1
    }
}

db_test! {
    async fn test_event_processor_basic_processing(pool: PgPool) -> Result<()> {
        let processor = TestEventProcessor::new("test_agent".to_string());
        let process_count = processor.process_count.clone();
        
        // Insert a test item
        let _queue_id = insert_test_promotion_item(&pool, "test_agent").await?;
        
        // Process the item
        let item = sqlx::query_as::<_, PromotionQueueItem>(
            "SELECT queue_id, raw_event_id, target_agent_name, status, attempts, max_attempts, 
                    last_attempt_ts, next_retry_ts, error_message_last, created_at, processing_worker_id
             FROM sinex_schemas.promotion_queue 
             WHERE target_agent_name = $1 AND status = 'pending'"
        )
        .bind("test_agent")
        .fetch_one(&pool)
        .await?;
        
        processor.process_event(&pool, &item).await?;
        
        assert_eq!(process_count.load(Ordering::SeqCst), 1);
        
        Ok(())
    }
}

db_test! {
    async fn test_event_processor_failure_handling(pool: PgPool) -> Result<()> {
        let processor = TestEventProcessor::new("test_agent".to_string());
        
        processor.should_fail.store(true, Ordering::SeqCst);
        
        let _queue_id = insert_test_promotion_item(&pool, "test_agent").await?;
        
        let item = sqlx::query_as::<_, PromotionQueueItem>(
            "SELECT queue_id, raw_event_id, target_agent_name, status, attempts, max_attempts, 
                    last_attempt_ts, next_retry_ts, error_message_last, created_at, processing_worker_id
             FROM sinex_schemas.promotion_queue 
             WHERE target_agent_name = $1 AND status = 'pending'"
        )
        .bind("test_agent")
        .fetch_one(&pool)
        .await?;
        
        let result = processor.process_event(&pool, &item).await;
        assert!(result.is_err());
        
        Ok(())
    }
}

#[tokio::test]
async fn test_backoff_calculation() {
    // Test backoff calculation function
    let backoff_0 = calculate_backoff_secs(0);
    let backoff_1 = calculate_backoff_secs(1);
    let backoff_5 = calculate_backoff_secs(5);
    let backoff_10 = calculate_backoff_secs(10);
    
    // Backoff should increase with attempts
    assert!(backoff_1 > backoff_0);
    assert!(backoff_5 > backoff_1);
    
    // Should not exceed maximum (24 hours)
    assert!(backoff_10 <= 24.0 * 3600.0);
    
    println!("Backoff progression: 0:{:.1}s, 1:{:.1}s, 5:{:.1}s, 10:{:.1}s", 
        backoff_0, backoff_1, backoff_5, backoff_10);
}

#[tokio::test]
async fn test_worker_metrics_creation() {
    let metrics = WorkerMetrics::new("test_agent");
    
    // Test that metrics can be incremented
    metrics.items_claimed.inc();
    metrics.items_processed.inc();
    metrics.items_failed.inc();
    
    // Observe processing duration
    metrics.processing_duration.observe(0.5);
    
    // Just ensure metrics don't panic
    assert_eq!(metrics.items_claimed.get(), 1.0);
    assert_eq!(metrics.items_processed.get(), 1.0);
    assert_eq!(metrics.items_failed.get(), 1.0);
}

db_test! {
    async fn test_multiple_processors_different_agents(pool: PgPool) -> Result<()> {
        let processor_a = TestEventProcessor::new("agent_a".to_string());
        let processor_b = TestEventProcessor::new("agent_b".to_string());
        
        let count_a = processor_a.process_count.clone();
        let count_b = processor_b.process_count.clone();
        
        // Insert items for each agent
        let _queue_id_a = insert_test_promotion_item(&pool, "agent_a").await?;
        let _queue_id_b = insert_test_promotion_item(&pool, "agent_b").await?;
        
        // Get and process items for each agent
        let item_a = sqlx::query_as::<_, PromotionQueueItem>(
            "SELECT queue_id, raw_event_id, target_agent_name, status, attempts, max_attempts, 
                    last_attempt_ts, next_retry_ts, error_message_last, created_at, processing_worker_id
             FROM sinex_schemas.promotion_queue 
             WHERE target_agent_name = 'agent_a'"
        )
        .fetch_one(&pool)
        .await?;
        
        let item_b = sqlx::query_as::<_, PromotionQueueItem>(
            "SELECT queue_id, raw_event_id, target_agent_name, status, attempts, max_attempts, 
                    last_attempt_ts, next_retry_ts, error_message_last, created_at, processing_worker_id
             FROM sinex_schemas.promotion_queue 
             WHERE target_agent_name = 'agent_b'"
        )
        .fetch_one(&pool)
        .await?;
        
        processor_a.process_event(&pool, &item_a).await?;
        processor_b.process_event(&pool, &item_b).await?;
        
        assert_eq!(count_a.load(Ordering::SeqCst), 1);
        assert_eq!(count_b.load(Ordering::SeqCst), 1);
        
        Ok(())
    }
}

#[tokio::test]
async fn test_processor_configuration() {
    let processor = TestEventProcessor::new("test_agent".to_string());
    
    assert_eq!(processor.agent_name(), "test_agent");
    assert_eq!(processor.batch_size(), 1);
    assert_eq!(processor.poll_interval_secs(), 1);
}

struct SlowProcessor {
    agent_name: String,
}

#[async_trait]
impl EventProcessor for SlowProcessor {
    async fn process_event(
        &self,
        _pool: &PgPool,
        _item: &PromotionQueueItem,
    ) -> Result<()> {
        tokio::time::sleep(Duration::from_millis(200)).await;
        Ok(())
    }
    
    fn agent_name(&self) -> &str {
        &self.agent_name
    }
    
    fn batch_size(&self) -> i32 {
        5
    }
    
    fn poll_interval_secs(&self) -> u64 {
        2
    }
}

#[tokio::test]
async fn test_processor_custom_configuration() {
    let processor = SlowProcessor {
        agent_name: "slow_agent".to_string(),
    };
    
    assert_eq!(processor.agent_name(), "slow_agent");
    assert_eq!(processor.batch_size(), 5);
    assert_eq!(processor.poll_interval_secs(), 2);
}