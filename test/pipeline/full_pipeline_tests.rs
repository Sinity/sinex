use anyhow::Result;
use sinex_core::{RawEvent, SimpleIngestor, IngestorRuntime, IngestorConfig};
use sinex_db::{create_pool_from_env, models::promotion_queue::PromotionQueue};
use sinex_worker::{Worker, WorkerConfig, ProcessingResult};
use sinex_ulid::Ulid;
use sqlx::PgPool;
use std::sync::{Arc, atomic::{AtomicU32, Ordering}};
use std::time::Duration;
use tokio::sync::mpsc;
use serde_json::json;
use async_trait::async_trait;

async fn setup_test_environment() -> Result<PgPool> {
    let pool = create_pool_from_env(None).await?;
    
    // Clean all tables
    sqlx::query("TRUNCATE TABLE raw.events CASCADE")
        .execute(&pool)
        .await?;
    
    sqlx::query("TRUNCATE TABLE sinex_schemas.promotion_queue CASCADE")
        .execute(&pool)
        .await?;
    
    sqlx::query("TRUNCATE TABLE sinex_schemas.agent_manifests CASCADE")
        .execute(&pool)
        .await?;
    
    Ok(pool)
}

// Test ingestor that generates events at a controlled rate
struct PipelineTestIngestor {
    events_to_generate: u32,
    events_generated: Arc<AtomicU32>,
    generation_rate: Duration,
}

#[async_trait]
impl SimpleIngestor for PipelineTestIngestor {
    fn name() -> &'static str {
        "pipeline-test-ingestor"
    }
    
    fn version() -> &'static str {
        "1.0.0"
    }
    
    async fn capture_events(&mut self, event_tx: mpsc::Sender<RawEvent>) -> Result<()> {
        for i in 0..self.events_to_generate {
            let event = RawEvent::new(
                "pipeline_test",
                "test_event",
                json!({
                    "sequence": i,
                    "data": format!("Test event {}", i),
                    "timestamp": chrono::Utc::now().to_rfc3339(),
                }),
            );
            
            event_tx.send(event).await?;
            self.events_generated.fetch_add(1, Ordering::SeqCst);
            
            tokio::time::sleep(self.generation_rate).await;
        }
        
        // Keep running to allow pipeline to process
        loop {
            tokio::time::sleep(Duration::from_secs(10)).await;
        }
    }
}

// Test worker that tracks processing
struct PipelineTestWorker {
    events_processed: Arc<AtomicU32>,
    processing_delay: Duration,
    derived_events_created: Arc<AtomicU32>,
}

#[async_trait]
impl Worker for PipelineTestWorker {
    fn name(&self) -> &'static str {
        "pipeline-test-worker"
    }
    
    async fn process_event(
        &mut self,
        event: &RawEvent,
        pool: &PgPool,
    ) -> Result<ProcessingResult> {
        // Simulate processing
        tokio::time::sleep(self.processing_delay).await;
        
        // Extract sequence number
        let sequence = event.payload.get("sequence")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as u32;
        
        // Create a derived event
        let derived_event = RawEvent::new(
            "pipeline_test_derived",
            "processed_event",
            json!({
                "original_sequence": sequence,
                "processed_at": chrono::Utc::now().to_rfc3339(),
                "processor": self.name(),
            }),
        );
        
        // Store derived event
        sqlx::query(
            "INSERT INTO raw.events (id, event_type, source, timestamp, payload, metadata) 
             VALUES ($1, $2, $3, $4, $5, $6)"
        )
        .bind(derived_event.id.as_uuid())
        .bind(&derived_event.event_type)
        .bind(&derived_event.source)
        .bind(derived_event.timestamp)
        .bind(&derived_event.payload)
        .bind(&derived_event.metadata)
        .execute(pool)
        .await?;
        
        self.events_processed.fetch_add(1, Ordering::SeqCst);
        self.derived_events_created.fetch_add(1, Ordering::SeqCst);
        
        Ok(ProcessingResult::Success)
    }
}

#[tokio::test]
async fn test_full_pipeline_end_to_end() -> Result<()> {
    let pool = setup_test_environment().await?;
    
    let events_to_generate = 10;
    let events_generated = Arc::new(AtomicU32::new(0));
    let events_processed = Arc::new(AtomicU32::new(0));
    let derived_events_created = Arc::new(AtomicU32::new(0));
    
    // Create ingestor
    let ingestor = PipelineTestIngestor {
        events_to_generate,
        events_generated: events_generated.clone(),
        generation_rate: Duration::from_millis(50),
    };
    
    let ingestor_config = IngestorConfig {
        heartbeat_interval: Duration::from_millis(500),
        batch_size: 3,
        batch_timeout: Duration::from_millis(100),
        dlq_enabled: true,
        dlq_path: None,
        retry_max_attempts: 3,
        retry_backoff: Duration::from_millis(100),
    };
    
    // Create promotion inserter (simulates promotion worker)
    let pool_clone = pool.clone();
    let promotion_inserter = tokio::spawn(async move {
        loop {
            // Check for new events to promote
            let new_events: Vec<(Ulid, String)> = sqlx::query_as(
                "SELECT e.id, e.event_type 
                 FROM raw.events e
                 LEFT JOIN sinex_schemas.promotion_queue pq ON pq.event_id = e.id::uuid
                 WHERE pq.id IS NULL
                 AND e.source = 'pipeline_test'
                 LIMIT 10"
            )
            .fetch_all(&pool_clone)
            .await
            .unwrap_or_default();
            
            for (event_id, event_type) in new_events {
                sqlx::query(
                    "INSERT INTO sinex_schemas.promotion_queue 
                     (id, event_id, event_type, priority, retry_count, created_at) 
                     VALUES ($1, $2, $3, $4, 0, NOW())"
                )
                .bind(Ulid::new().as_uuid())
                .bind(event_id.as_uuid())
                .bind(event_type)
                .bind(5)
                .execute(&pool_clone)
                .await
                .unwrap();
            }
            
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });
    
    // Create worker
    let worker = PipelineTestWorker {
        events_processed: events_processed.clone(),
        processing_delay: Duration::from_millis(20),
        derived_events_created: derived_events_created.clone(),
    };
    
    let worker_config = WorkerConfig {
        batch_size: 2,
        poll_interval: Duration::from_millis(50),
        max_retries: 3,
        retry_backoff_base: Duration::from_millis(100),
    };
    
    // Start all components
    let runtime = IngestorRuntime::new(ingestor, pool.clone(), ingestor_config);
    
    let ingestor_handle = tokio::spawn(async move {
        runtime.run().await
    });
    
    let pool_clone = pool.clone();
    let mut worker = worker;
    let worker_handle = tokio::spawn(async move {
        worker.run(&pool_clone, worker_config).await
    });
    
    // Wait for pipeline to process all events
    let start = std::time::Instant::now();
    let timeout_duration = Duration::from_secs(10);
    
    loop {
        let generated = events_generated.load(Ordering::SeqCst);
        let processed = events_processed.load(Ordering::SeqCst);
        
        if generated >= events_to_generate && processed >= events_to_generate {
            break;
        }
        
        if start.elapsed() > timeout_duration {
            panic!(
                "Pipeline timeout: generated={}, processed={}", 
                generated, processed
            );
        }
        
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    
    // Stop all components
    ingestor_handle.abort();
    worker_handle.abort();
    promotion_inserter.abort();
    
    // Verify results
    assert_eq!(
        events_generated.load(Ordering::SeqCst), 
        events_to_generate,
        "All events should be generated"
    );
    
    assert_eq!(
        events_processed.load(Ordering::SeqCst), 
        events_to_generate,
        "All events should be processed"
    );
    
    assert_eq!(
        derived_events_created.load(Ordering::SeqCst), 
        events_to_generate,
        "Derived events should be created for each processed event"
    );
    
    // Verify database state
    let raw_event_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM raw.events WHERE source = 'pipeline_test'"
    )
    .fetch_one(&pool)
    .await?;
    
    assert_eq!(raw_event_count, events_to_generate as i64);
    
    let derived_event_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM raw.events WHERE source = 'pipeline_test_derived'"
    )
    .fetch_one(&pool)
    .await?;
    
    assert_eq!(derived_event_count, events_to_generate as i64);
    
    let completed_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sinex_schemas.promotion_queue WHERE status = 'completed'"
    )
    .fetch_one(&pool)
    .await?;
    
    assert_eq!(completed_count, events_to_generate as i64);
    
    Ok(())
}

#[tokio::test]
async fn test_pipeline_with_multiple_workers() -> Result<()> {
    let pool = setup_test_environment().await?;
    
    let events_to_generate = 20;
    let events_generated = Arc::new(AtomicU32::new(0));
    let total_processed = Arc::new(AtomicU32::new(0));
    
    // Create ingestor
    let ingestor = PipelineTestIngestor {
        events_to_generate,
        events_generated: events_generated.clone(),
        generation_rate: Duration::from_millis(25),
    };
    
    let ingestor_config = IngestorConfig {
        heartbeat_interval: Duration::from_secs(1),
        batch_size: 5,
        batch_timeout: Duration::from_millis(100),
        dlq_enabled: false,
        dlq_path: None,
        retry_max_attempts: 3,
        retry_backoff: Duration::from_millis(100),
    };
    
    // Promotion inserter
    let pool_clone = pool.clone();
    let promotion_inserter = tokio::spawn(async move {
        loop {
            let new_events: Vec<(Ulid, String)> = sqlx::query_as(
                "SELECT e.id, e.event_type 
                 FROM raw.events e
                 LEFT JOIN sinex_schemas.promotion_queue pq ON pq.event_id = e.id::uuid
                 WHERE pq.id IS NULL
                 LIMIT 20"
            )
            .fetch_all(&pool_clone)
            .await
            .unwrap_or_default();
            
            for (event_id, event_type) in new_events {
                sqlx::query(
                    "INSERT INTO sinex_schemas.promotion_queue 
                     (id, event_id, event_type, priority, retry_count, created_at) 
                     VALUES ($1, $2, $3, $4, 0, NOW())"
                )
                .bind(Ulid::new().as_uuid())
                .bind(event_id.as_uuid())
                .bind(event_type)
                .bind(1)
                .execute(&pool_clone)
                .await
                .unwrap();
            }
            
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    });
    
    // Start ingestor
    let runtime = IngestorRuntime::new(ingestor, pool.clone(), ingestor_config);
    let ingestor_handle = tokio::spawn(async move {
        runtime.run().await
    });
    
    // Start multiple workers
    let num_workers = 3;
    let mut worker_handles = Vec::new();
    
    for worker_id in 0..num_workers {
        let pool_clone = pool.clone();
        let total_processed = total_processed.clone();
        
        let worker_handle = tokio::spawn(async move {
            let worker = PipelineTestWorker {
                events_processed: Arc::new(AtomicU32::new(0)),
                processing_delay: Duration::from_millis(50),
                derived_events_created: Arc::new(AtomicU32::new(0)),
            };
            
            let worker_config = WorkerConfig {
                batch_size: 1,
                poll_interval: Duration::from_millis(100),
                max_retries: 3,
                retry_backoff_base: Duration::from_millis(100),
            };
            
            let events_processed = worker.events_processed.clone();
            let mut worker = worker;
            
            let result = worker.run(&pool_clone, worker_config).await;
            
            let processed = events_processed.load(Ordering::SeqCst);
            total_processed.fetch_add(processed, Ordering::SeqCst);
            
            (worker_id, processed, result)
        });
        
        worker_handles.push(worker_handle);
    }
    
    // Wait for completion
    let start = std::time::Instant::now();
    loop {
        let processed = total_processed.load(Ordering::SeqCst);
        
        if processed >= events_to_generate {
            break;
        }
        
        if start.elapsed() > Duration::from_secs(15) {
            panic!("Pipeline timeout: processed={}/{}", processed, events_to_generate);
        }
        
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    
    // Stop components
    ingestor_handle.abort();
    promotion_inserter.abort();
    
    for handle in worker_handles {
        handle.abort();
    }
    
    // Verify work was distributed among workers
    println!("Total events processed: {}", total_processed.load(Ordering::SeqCst));
    
    let completed_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sinex_schemas.promotion_queue WHERE status = 'completed'"
    )
    .fetch_one(&pool)
    .await?;
    
    assert_eq!(completed_count, events_to_generate as i64);
    
    Ok(())
}

#[tokio::test]
async fn test_pipeline_error_recovery() -> Result<()> {
    let pool = setup_test_environment().await?;
    
    // Insert some events that will cause errors
    for i in 0..5 {
        let event = RawEvent::new(
            "error_test",
            if i % 2 == 0 { "good_event" } else { "bad_event" },
            json!({"sequence": i}),
        );
        
        sqlx::query(
            "INSERT INTO raw.events (id, event_type, source, timestamp, payload, metadata) 
             VALUES ($1, $2, $3, $4, $5, $6)"
        )
        .bind(event.id.as_uuid())
        .bind(&event.event_type)
        .bind(&event.source)
        .bind(event.timestamp)
        .bind(&event.payload)
        .bind(&event.metadata)
        .execute(&pool)
        .await?;
        
        // Add to promotion queue
        sqlx::query(
            "INSERT INTO sinex_schemas.promotion_queue 
             (id, event_id, event_type, priority, retry_count, created_at) 
             VALUES ($1, $2, $3, $4, 0, NOW())"
        )
        .bind(Ulid::new().as_uuid())
        .bind(event.id.as_uuid())
        .bind(&event.event_type)
        .bind(1)
        .execute(&pool)
        .await?;
    }
    
    // Worker that fails on bad events
    struct ErrorTestWorker {
        processed_good: Arc<AtomicU32>,
        processed_bad: Arc<AtomicU32>,
    }
    
    #[async_trait]
    impl Worker for ErrorTestWorker {
        fn name(&self) -> &'static str {
            "error-test-worker"
        }
        
        async fn process_event(
            &mut self,
            event: &RawEvent,
            _pool: &PgPool,
        ) -> Result<ProcessingResult> {
            if event.event_type == "bad_event" {
                self.processed_bad.fetch_add(1, Ordering::SeqCst);
                Ok(ProcessingResult::Failed("Bad event type".to_string()))
            } else {
                self.processed_good.fetch_add(1, Ordering::SeqCst);
                Ok(ProcessingResult::Success)
            }
        }
    }
    
    let worker = ErrorTestWorker {
        processed_good: Arc::new(AtomicU32::new(0)),
        processed_bad: Arc::new(AtomicU32::new(0)),
    };
    
    let processed_good = worker.processed_good.clone();
    let processed_bad = worker.processed_bad.clone();
    
    let worker_config = WorkerConfig {
        batch_size: 5,
        poll_interval: Duration::from_millis(50),
        max_retries: 2,
        retry_backoff_base: Duration::from_millis(50),
    };
    
    let mut worker = worker;
    let worker_handle = tokio::spawn(async move {
        worker.run(&pool, worker_config).await
    });
    
    // Wait for processing
    tokio::time::sleep(Duration::from_millis(500)).await;
    
    worker_handle.abort();
    
    // Good events should be completed
    assert_eq!(processed_good.load(Ordering::SeqCst), 3);
    
    // Bad events should have been retried
    assert!(processed_bad.load(Ordering::SeqCst) >= 2); // At least initial + 1 retry
    
    // Check database state
    let completed: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sinex_schemas.promotion_queue 
         WHERE status = 'completed' AND event_type = 'good_event'"
    )
    .fetch_one(&pool)
    .await?;
    
    assert_eq!(completed, 3);
    
    let failed: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sinex_schemas.promotion_queue 
         WHERE status = 'failed' AND event_type = 'bad_event'"
    )
    .fetch_one(&pool)
    .await?;
    
    assert_eq!(failed, 2);
    
    Ok(())
}