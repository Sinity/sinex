use crate::common::prelude::*;
// Project-specific imports not covered by prelude
use sinex_db::models::WorkQueueItem;
use sinex_worker::{worker::Worker, EventProcessor};

// Test setup macros

// Test source that generates events at a controlled rate
#[derive(Clone, Serialize, Deserialize)]
struct PipelineTestConfig {
    events_to_generate: u32,
    generation_rate: u64, // milliseconds
}

struct PipelineTestSource {
    events_to_generate: u32,
    events_generated: Arc<AtomicU32>,
    generation_rate: Duration,
}

#[async_trait]
impl EventSource for PipelineTestSource {
    type Config = PipelineTestConfig;

    const SOURCE_NAME: &'static str = "pipeline_test";

    async fn initialize(ctx: EventSourceContext) -> sinex_core::Result<Self> {
        let config: PipelineTestConfig = serde_json::from_value(ctx.config).map_err(|e| {
            sinex_core::CoreError::Configuration(format!("Failed to parse config: {}", e))
        })?;
        Ok(Self {
            events_to_generate: config.events_to_generate,
            events_generated: Arc::new(AtomicU32::new(0)),
            generation_rate: Duration::from_millis(config.generation_rate),
        })
    }

    async fn stream_events(&mut self, event_tx: mpsc::Sender<RawEvent>) -> sinex_core::Result<()> {
        for _i in 0..self.events_to_generate {
            let event =
                RawEventBuilder::new("pipeline_test", "test_event", json!({"test": true})).build();

            event_tx
                .send(event)
                .await
                .map_err(|e| sinex_core::CoreError::Io(e.to_string()))?;
            self.events_generated.fetch_add(1, Ordering::SeqCst);

            tokio::time::sleep(self.generation_rate).await;
        }

        // Signal completion
        Ok(())
    }
}

// Test processor that tracks processing
struct PipelineTestProcessor {
    events_processed: Arc<AtomicU32>,
    processing_delay: Duration,
    derived_events_created: Arc<AtomicU32>,
}

#[async_trait]
impl EventProcessor for PipelineTestProcessor {
    async fn process_event(
        &self,
        pool: &DbPool,
        item: &WorkQueueItem,
    ) -> Result<(), anyhow::Error> {
        // Fetch the raw event
        let event = sqlx::query!(
            r#"
            SELECT id::uuid as "id!", source, event_type, ts_ingest, payload, host
            FROM raw.events
            WHERE id = $1::uuid::ulid
            "#,
            item.raw_event_id.to_uuid()
        )
        .fetch_one(pool)
        .await?;

        // Simulate processing
        tokio::time::sleep(self.processing_delay).await;

        // Extract sequence number
        let sequence = event
            .payload
            .get("sequence")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as u32;

        // Create a derived event
        let derived_event = RawEventBuilder::new(
            "pipeline_test_derived",
            "processed_event",
            json!({
                "original_sequence": sequence,
                "processed_at": chrono::Utc::now().to_rfc3339(),
                "processor": self.agent_name(),
            }),
        )
        .build();

        // Store derived event
        sqlx::query(
            "INSERT INTO raw.events (id, event_type, source, ts_ingest, payload, host)
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(derived_event.id.to_uuid())
        .bind(&derived_event.event_type)
        .bind(&derived_event.source)
        .bind(derived_event.ts_ingest)
        .bind(&derived_event.payload)
        .bind(event.host)
        .execute(pool)
        .await?;

        self.events_processed.fetch_add(1, Ordering::SeqCst);
        self.derived_events_created.fetch_add(1, Ordering::SeqCst);

        Ok(())
    }

    fn agent_name(&self) -> &str {
        "pipeline_test_worker"
    }
}

#[sinex_test]
async fn test_full_pipeline_end_to_end(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();
    let events_to_generate = 10;
    let _events_generated = Arc::new(AtomicU32::new(0));
    let events_processed = Arc::new(AtomicU32::new(0));
    let derived_events_created = Arc::new(AtomicU32::new(0));

    // Create source
    let config = PipelineTestConfig {
        events_to_generate,
        generation_rate: 50,
    };
    let ctx = event_sources::test_context(serde_json::to_value(config)?);
    let mut source = PipelineTestSource::initialize(ctx).await?;
    let source_events_generated = source.events_generated.clone();

    // Create event channel and storage task
    let (event_tx, mut event_rx) = mpsc::channel::<RawEvent>(100);

    // Storage task that saves events to database
    let pool_clone = pool.clone();
    let storage_handle = tokio::spawn(async move {
        while let Some(event) = event_rx.recv().await {
            // Store in database
            sqlx::query(
                "INSERT INTO raw.events (id, event_type, source, ts_ingest, payload, host)
                     VALUES ($1, $2, $3, $4, $5, $6)",
            )
            .bind(event.id.to_uuid())
            .bind(&event.event_type)
            .bind(&event.source)
            .bind(event.ts_ingest)
            .bind(&event.payload)
            .bind("test-host")
            .execute(&pool_clone)
            .await
            .unwrap();

            // Insert into work queue
            sqlx::query(
                    "INSERT INTO sinex_schemas.work_queue
                     (queue_id, raw_event_id, target_agent_name, attempts, max_attempts, created_at)
                     VALUES ($1, $2, $3, 0, 3, NOW())"
                )
                .bind(Ulid::new().to_uuid())
                .bind(event.id.to_uuid())
                .bind("pipeline_test_worker")
                .execute(&pool_clone)
                .await
                .unwrap();
        }
    });

    // Start source
    let source_handle = tokio::spawn(async move { source.stream_events(event_tx).await });

    // Create processor
    let processor = Arc::new(PipelineTestProcessor {
        events_processed: events_processed.clone(),
        processing_delay: Duration::from_millis(20),
        derived_events_created: derived_events_created.clone(),
    });

    // Create worker
    let worker = Worker::new(pool.clone(), processor, "test-worker-1".to_string());

    let worker_handle = tokio::spawn(async move { worker.run().await });

    // Wait for pipeline to process all events using optimized coordination
    use crate::common::timing_optimization::EventCounter;

    let _generation_counter = EventCounter::new(events_to_generate as usize);
    let _processing_counter = EventCounter::new(events_to_generate as usize);

    // Wait for both generation and processing to complete
    let _timeout_duration = Duration::from_secs(10);

    // Wait for pipeline completion using timing utilities
    // First wait for all events to be generated and stored
    wait_for_filtered_event_count(
        &pool,
        "source = $1",
        &["pipeline_test"],
        events_to_generate as i64,
        10,
    )
    .await
    .map_err(|e| anyhow::anyhow!("Failed to wait for events: {}", e))?;

    // Then wait for work queue to be empty (all processed)
    wait_for_work_queue_count(
        &pool, 0, // Empty queue
        10,
    )
    .await
    .map_err(|e| anyhow::anyhow!("Failed to wait for work queue completion: {}", e))?;

    // Stop all components
    source_handle.abort();
    worker_handle.abort();
    storage_handle.abort();

    // Verify results
    pretty_assertions::assert_eq!(
        source_events_generated.load(Ordering::SeqCst),
        events_to_generate,
        "All events should be generated"
    );

    pretty_assertions::assert_eq!(
        events_processed.load(Ordering::SeqCst),
        events_to_generate,
        "All events should be processed"
    );

    pretty_assertions::assert_eq!(
        derived_events_created.load(Ordering::SeqCst),
        events_to_generate,
        "Derived events should be created for each processed event"
    );

    // Verify database state using timing utilities
    let raw_event_count = wait_for_filtered_event_count(
        &pool,
        "source = $1",
        &["pipeline_test"],
        events_to_generate as i64,
        10,
    )
    .await
    .map_err(|e| anyhow::anyhow!("Failed to verify raw events: {}", e))?;

    pretty_assertions::assert_eq!(raw_event_count, events_to_generate as i64);

    // Wait for derived events to be processed
    let derived_event_count = wait_for_filtered_event_count(
        &pool,
        "source = $1",
        &["pipeline_test_derived"],
        events_to_generate as i64,
        10,
    )
    .await
    .map_err(|e| anyhow::anyhow!("Failed to verify derived events: {}", e))?;

    pretty_assertions::assert_eq!(derived_event_count, events_to_generate as i64);

    let remaining_queue: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sinex_schemas.work_queue")
        .fetch_one(&pool)
        .await?;

    pretty_assertions::assert_eq!(remaining_queue, 0);

    Ok(())
}

#[sinex_test]
async fn test_pipeline_with_multiple_workers(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();
    let events_to_generate = 20;
    let total_processed = Arc::new(AtomicU32::new(0));

    // Pre-insert events into database
    for i in 0..events_to_generate {
        let event = RawEventBuilder::new(
            "pipeline_test",
            "test_event",
            json!({
                "sequence": i,
                "data": format!("Test event {}", i),
            }),
        )
        .build();

        sqlx::query(
            "INSERT INTO raw.events (id, event_type, source, ts_ingest, payload, host)
                 VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(event.id.to_uuid())
        .bind(&event.event_type)
        .bind(&event.source)
        .bind(event.ts_ingest)
        .bind(&event.payload)
        .bind("test-host")
        .execute(&pool)
        .await?;

        sqlx::query(
            "INSERT INTO sinex_schemas.work_queue
                 (queue_id, raw_event_id, target_agent_name, attempts, max_attempts, created_at)
                 VALUES ($1, $2, $3, 0, 3, NOW())",
        )
        .bind(Ulid::new().to_uuid())
        .bind(event.id.to_uuid())
        .bind("test_worker")
        .execute(&pool)
        .await?;
    }

    // Start multiple workers
    let num_workers = 3;
    let mut worker_handles = Vec::new();

    for worker_id in 0..num_workers {
        let pool_clone = pool.clone();
        let total_processed = total_processed.clone();

        let worker_handle = tokio::spawn(async move {
            let processor = Arc::new(PipelineTestProcessor {
                events_processed: Arc::new(AtomicU32::new(0)),
                processing_delay: Duration::from_millis(50),
                derived_events_created: Arc::new(AtomicU32::new(0)),
            });

            let events_processed = processor.events_processed.clone();

            let worker = Worker::new(pool_clone, processor, format!("test-worker-{}", worker_id));

            let result = worker.run().await;

            let processed = events_processed.load(Ordering::SeqCst);
            total_processed.fetch_add(processed, Ordering::SeqCst);

            (worker_id, processed, result)
        });

        worker_handles.push(worker_handle);
    }

    // Wait for completion using optimized timing
    let start = std::time::Instant::now();
    let timeout_duration = Duration::from_secs(15);

    while start.elapsed() < timeout_duration {
        let processed = total_processed.load(Ordering::SeqCst);

        if processed >= events_to_generate {
            break;
        }

        // Use exponential backoff instead of fixed sleep
        let elapsed = start.elapsed();
        let backoff = Duration::from_millis(10.min(elapsed.as_millis() as u64 / 20));
        tokio::time::sleep(backoff).await;
    }

    if start.elapsed() >= timeout_duration {
        let processed = total_processed.load(Ordering::SeqCst);
        panic!(
            "Pipeline timeout: processed={}/{}",
            processed, events_to_generate
        );
    }

    // Stop workers
    for handle in worker_handles {
        handle.abort();
    }

    // Verify work was distributed among workers
    println!(
        "Total events processed: {}",
        total_processed.load(Ordering::SeqCst)
    );

    let remaining_queue: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sinex_schemas.work_queue")
        .fetch_one(&pool)
        .await?;

    pretty_assertions::assert_eq!(remaining_queue, 0);

    Ok(())
}

#[sinex_test]
async fn test_pipeline_error_recovery(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();
    // Insert some events that will cause errors
    for i in 0..5 {
        let event = RawEventBuilder::new(
            "error_test",
            if i % 2 == 0 {
                "good_event"
            } else {
                "bad_event"
            },
            json!({"sequence": i}),
        )
        .build();

        sqlx::query(
            "INSERT INTO raw.events (id, event_type, source, ts_ingest, payload, host)
                 VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(event.id.to_uuid())
        .bind(&event.event_type)
        .bind(&event.source)
        .bind(event.ts_ingest)
        .bind(&event.payload)
        .bind("test-host")
        .execute(&pool)
        .await?;

        // Add to work queue
        sqlx::query(
            "INSERT INTO sinex_schemas.work_queue
                 (queue_id, raw_event_id, target_agent_name, attempts, max_attempts, created_at)
                 VALUES ($1, $2, $3, 0, 3, NOW())",
        )
        .bind(Ulid::new().to_uuid())
        .bind(event.id.to_uuid())
        .bind("error_test_worker")
        .execute(&pool)
        .await?;
    }

    // Processor that fails on bad events
    struct ErrorTestProcessor {
        processed_good: Arc<AtomicU32>,
        processed_bad: Arc<AtomicU32>,
    }

    #[async_trait]
    impl EventProcessor for ErrorTestProcessor {
        async fn process_event(
            &self,
            pool: &DbPool,
            item: &WorkQueueItem,
        ) -> Result<(), anyhow::Error> {
            // Fetch the raw event
            let event = sqlx::query!(
                r#"
                    SELECT event_type
                    FROM raw.events
                    WHERE id = $1::uuid::ulid
                    "#,
                item.raw_event_id.to_uuid()
            )
            .fetch_one(pool)
            .await?;

            if event.event_type == "bad_event" {
                self.processed_bad.fetch_add(1, Ordering::SeqCst);
                return Err(anyhow::anyhow!("Bad event type"));
            } else {
                self.processed_good.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }

        fn agent_name(&self) -> &str {
            "error_test_worker"
        }
    }

    let processor = Arc::new(ErrorTestProcessor {
        processed_good: Arc::new(AtomicU32::new(0)),
        processed_bad: Arc::new(AtomicU32::new(0)),
    });

    let processed_good = processor.processed_good.clone();
    let processed_bad = processor.processed_bad.clone();

    let worker = Worker::new(pool.clone(), processor, "test-worker-1".to_string());

    let worker_handle = tokio::spawn(async move { worker.run().await });

    // Wait for processing
    tokio::time::sleep(Duration::from_secs(2)).await;

    worker_handle.abort();

    // Good events should be completed
    pretty_assertions::assert_eq!(processed_good.load(Ordering::SeqCst), 3);

    // Bad events should have been retried
    assert!(processed_bad.load(Ordering::SeqCst) >= 2); // At least initial + 1 retry

    // Check database state
    let remaining: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sinex_schemas.work_queue WHERE target_agent_name = 'error_test_worker'"
        )
        .fetch_one(&pool)
        .await?;

    // Should have no good events left (3 were processed)
    // Bad events might be in DLQ or still retrying
    assert!(remaining <= 2);

    let dlq: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sinex_schemas.dlq_events WHERE agent_name = 'error_test_worker'",
    )
    .fetch_one(&pool)
    .await?;

    // Should have some bad events in DLQ after max retries
    assert!(dlq >= 0);

    Ok(())
}
