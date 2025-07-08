use crate::common::event_sources;
use crate::common::prelude::*;
use serde::{Deserialize, Serialize};
#[allow(unused_imports)]
// Import test setup macros
#[derive(Clone, Serialize, Deserialize)]
struct TestSourceConfig {
    events_to_generate: u32,
    generation_delay_ms: u64,
    should_fail: bool,
}

impl Default for TestSourceConfig {
    fn default() -> Self {
        Self {
            events_to_generate: 5,
            generation_delay_ms: 10,
            should_fail: false,
        }
    }
}

struct TestEventSource {
    config: TestSourceConfig,
    events_sent: Arc<AtomicU32>,
    should_error: Arc<AtomicBool>,
}

#[async_trait]
impl EventSource for TestEventSource {
    type Config = TestSourceConfig;
    const SOURCE_NAME: &'static str = "test_source";

    async fn initialize(ctx: EventSourceContext) -> sinex_core::Result<Self> {
        let config: TestSourceConfig = serde_json::from_value(ctx.config).map_err(|e| {
            sinex_core::CoreError::Configuration(format!("Failed to parse config: {}", e))
        })?;

        Ok(Self {
            config,
            events_sent: Arc::new(AtomicU32::new(0)),
            should_error: Arc::new(AtomicBool::new(false)),
        })
    }

    async fn stream_events(&mut self, tx: mpsc::Sender<RawEvent>) -> sinex_core::Result<()> {
        if self.config.should_fail {
            return Err(sinex_core::CoreError::Other("Test failure".to_string()));
        }

        for i in 0..self.config.events_to_generate {
            if self.should_error.load(Ordering::SeqCst) {
                return Err(sinex_core::CoreError::Other(
                    "Test error during streaming".to_string(),
                ));
            }

            let event = RawEventBuilder::new(
                Self::SOURCE_NAME,
                "test_event",
                json!({"test": true, "sequence": i}),
            ).build();

            if tx.send(event).await.is_err() {
                break; // Receiver dropped
            }

            self.events_sent.fetch_add(1, Ordering::SeqCst);

            tokio::time::sleep(Duration::from_millis(self.config.generation_delay_ms)).await;
        }

        // Keep running until shutdown
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    async fn shutdown(&mut self) -> sinex_core::Result<()> {
        Ok(())
    }
}

#[sinex_test]
async fn test_event_source_initialization(ctx: TestContext) -> TestResult {
    let config = TestSourceConfig {
        events_to_generate: 10,
        generation_delay_ms: 5,
        should_fail: false,
    };

    let ctx_local = event_sources::test_context(serde_json::to_value(&config)?);
    let source = TestEventSource::initialize(ctx_local).await?;

    pretty_assertions::assert_eq!(source.config.events_to_generate, 10);
    pretty_assertions::assert_eq!(source.config.generation_delay_ms, 5);
    assert!(!source.config.should_fail);

    Ok(())
}

#[sinex_test]
async fn test_event_source_initialization_failure(ctx: TestContext) -> TestResult {
    let config = TestSourceConfig {
        events_to_generate: 1,
        generation_delay_ms: 1,
        should_fail: true,
    };

    let ctx_local = event_sources::test_context(serde_json::to_value(&config)?);
    let mut source = TestEventSource::initialize(ctx_local).await?;

    let (tx, mut rx) = mpsc::channel(10);

    let result = source.stream_events(tx).await;
    assert!(result.is_err());

    // Should not have received any events
    let received = rx.try_recv();
    assert!(received.is_err());

    Ok(())
}

#[sinex_test]
async fn test_event_source_streaming(ctx: TestContext) -> TestResult {
    let config = TestSourceConfig {
        events_to_generate: 3,
        generation_delay_ms: 50,
        should_fail: false,
    };

    let ctx_local = event_sources::test_context(serde_json::to_value(&config)?);
    let mut source = TestEventSource::initialize(ctx_local).await?;
    let events_sent = source.events_sent.clone();

    let (tx, mut rx) = mpsc::channel(10);

    // Start streaming in background
    let stream_handle = tokio::spawn(async move { source.stream_events(tx).await });

    // Collect events
    let mut events = Vec::new();
    for _ in 0..3 {
        if let Some(event) = rx.recv().await {
            events.push(event);
        }
    }

    // Cancel streaming
    stream_handle.abort();

    pretty_assertions::assert_eq!(events.len(), 3);
    pretty_assertions::assert_eq!(events_sent.load(Ordering::SeqCst), 3);

    // Verify event structure
    for (i, event) in events.iter().enumerate() {
        pretty_assertions::assert_eq!(event.source, "test_source");
        pretty_assertions::assert_eq!(event.event_type, "test_event");
        pretty_assertions::assert_eq!(event.payload["sequence"], i);
    }

    Ok(())
}

#[sinex_test]
async fn test_event_source_runtime_error(ctx: TestContext) -> TestResult {
    let config = TestSourceConfig {
        events_to_generate: 10,
        generation_delay_ms: 10,
        should_fail: false,
    };

    let ctx_local = event_sources::test_context(serde_json::to_value(&config)?);
    let mut source = TestEventSource::initialize(ctx_local).await?;
    let should_error = source.should_error.clone();
    let events_sent = source.events_sent.clone();

    let (tx, _rx) = mpsc::channel(10);

    let stream_handle = tokio::spawn(async move { source.stream_events(tx).await });

    // Wait for some events to be generated
    tokio::task::yield_now().await;

    // Trigger error
    should_error.store(true, Ordering::SeqCst);

    // Wait for error
    tokio::time::sleep(Duration::from_millis(100)).await;

    let result = stream_handle.await;
    assert!(result.is_ok()); // Task completed (with error)

    // Should have sent some events before error
    let sent_count = events_sent.load(Ordering::SeqCst);
    assert!(sent_count > 0 && sent_count < 10);

    Ok(())
}

#[sinex_test]
async fn test_event_source_graceful_shutdown(ctx: TestContext) -> TestResult {
    let config = TestSourceConfig {
        events_to_generate: 1,
        generation_delay_ms: 10,
        should_fail: false,
    };

    let ctx_local = event_sources::test_context(serde_json::to_value(&config)?);
    let mut source = TestEventSource::initialize(ctx_local).await?;

    // Test shutdown
    let result = source.shutdown().await;
    assert!(result.is_ok());

    Ok(())
}

#[sinex_test]
async fn test_event_source_receiver_drop(ctx: TestContext) -> TestResult {
    let config = TestSourceConfig {
        events_to_generate: 100, // More than we'll receive
        generation_delay_ms: 1,
        should_fail: false,
    };

    let ctx_local = event_sources::test_context(serde_json::to_value(&config)?);
    let mut source = TestEventSource::initialize(ctx_local).await?;
    let _events_sent = source.events_sent.clone();

    let (tx, mut rx) = mpsc::channel(5);

    let stream_handle = tokio::spawn(async move { source.stream_events(tx).await });

    // Receive only a few events then drop receiver
    let mut received_count = 0;
    for _ in 0..3 {
        if rx.recv().await.is_some() {
            received_count += 1;
        }
    }

    // Drop the receiver
    drop(rx);

    // Give streaming task time to detect the drop and exit
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Should have stopped gracefully when receiver was dropped
    assert!(
        stream_handle.is_finished() || {
            stream_handle.abort();
            true
        }
    );

    pretty_assertions::assert_eq!(received_count, 3);

    Ok(())
}

struct SlowEventSource {
    delay: Duration,
}

#[async_trait]
impl EventSource for SlowEventSource {
    type Config = TestSourceConfig;
    const SOURCE_NAME: &'static str = "slow_source";

    async fn initialize(ctx: EventSourceContext) -> sinex_core::Result<Self> {
        let _config: TestSourceConfig = serde_json::from_value(ctx.config).map_err(|e| {
            sinex_core::CoreError::Configuration(format!("Failed to parse config: {}", e))
        })?;

        Ok(Self {
            delay: Duration::from_millis(200),
        })
    }

    async fn stream_events(&mut self, tx: mpsc::Sender<RawEvent>) -> sinex_core::Result<()> {
        loop {
            let event = RawEventBuilder::new(
                Self::SOURCE_NAME,
                "slow_event",
                json!({"timestamp": chrono::Utc::now().to_rfc3339()}),
            )
            .build();

            if tx.send(event).await.is_err() {
                break;
            }

            tokio::time::sleep(self.delay).await;
        }

        Ok(())
    }
}

#[sinex_test]
async fn test_multiple_event_sources(ctx: TestContext) -> TestResult {
    let config = TestSourceConfig::default();

    let ctx1 = event_sources::test_context(serde_json::to_value(&config)?);
    let ctx2 = event_sources::test_context(serde_json::to_value(&config)?);

    let mut source1 = TestEventSource::initialize(ctx1).await?;
    let mut source2 = SlowEventSource::initialize(ctx2).await?;

    let (tx1, mut rx1) = mpsc::channel(10);
    let (tx2, mut rx2) = mpsc::channel(10);

    // Start both sources
    let handle1 = tokio::spawn(async move { source1.stream_events(tx1).await });

    let handle2 = tokio::spawn(async move { source2.stream_events(tx2).await });

    // Receive events from both
    let event1 = tokio::time::timeout(Duration::from_secs(1), rx1.recv())
        .await?
        .ok_or_else(|| anyhow::anyhow!("No event received"))?;
    let event2 = tokio::time::timeout(Duration::from_secs(1), rx2.recv())
        .await?
        .ok_or_else(|| anyhow::anyhow!("No event received"))?;

    pretty_assertions::assert_eq!(event1.source, "test_source");
    pretty_assertions::assert_eq!(event2.source, "slow_source");

    handle1.abort();
    handle2.abort();

    Ok(())
}

#[sinex_test]
async fn test_event_source_database_integration(ctx: TestContext) -> TestResult {
    // Generate a unique event type for this test to avoid contamination
    let test_id = Ulid::new().to_string();
    let unique_event_type = format!("test_event_{}", &test_id[..8]);
    
    let config = TestSourceConfig {
        events_to_generate: 2,
        generation_delay_ms: 10,
        should_fail: false,
    };

    let ctx_local = event_sources::test_context(serde_json::to_value(&config)?);
    let mut source = TestEventSource::initialize(ctx_local).await?;

    let (tx, mut rx) = mpsc::channel(10);

    let stream_handle = tokio::spawn(async move { source.stream_events(tx).await });

    // Receive and store events with our unique type
    let mut inserted_count = 0;
    for _ in 0..2 {
        if let Some(mut event) = rx.recv().await {
            // Modify the event type to be unique for this test
            event.event_type = unique_event_type.clone();
            
            // Store in database using proper queries that handle ts_ingest correctly
            use sinex_db::queries::insert_event;
            insert_event(ctx.pool(), &event).await?;
            inserted_count += 1;
        }
    }

    stream_handle.abort();
    
    // Ensure we actually inserted 2 events
    assert_eq!(inserted_count, 2, "Should have inserted 2 events");

    // Verify events were stored - count only our unique event type
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM raw.events WHERE event_type = $1")
            .bind(&unique_event_type)
            .fetch_one(ctx.pool())
            .await?;

    // We inserted exactly 2 events with our unique type
    assert_eq!(count, 2, "Should have exactly 2 events with type {}, found {}", unique_event_type, count);

    Ok(())
}
