use anyhow::Result;
use async_trait::async_trait;
use sinex_core::{RawEvent, SimpleIngestor, IngestorRuntime, IngestorConfig};
use sinex_db::create_pool_from_env;
use sqlx::PgPool;
use std::sync::{Arc, atomic::{AtomicU32, AtomicBool, Ordering}};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::timeout;
use serde_json::json;

async fn setup_test_db() -> Result<PgPool> {
    let pool = create_pool_from_env(None).await?;
    
    sqlx::query("TRUNCATE TABLE raw.events CASCADE")
        .execute(&pool)
        .await?;
    
    sqlx::query("TRUNCATE TABLE sinex_schemas.agent_manifests CASCADE")
        .execute(&pool)
        .await?;
    
    Ok(pool)
}

struct TestIngestor {
    events_to_send: Vec<RawEvent>,
    send_count: Arc<AtomicU32>,
    should_error: Arc<AtomicBool>,
    send_delay: Duration,
}

impl TestIngestor {
    fn new(events: Vec<RawEvent>) -> Self {
        Self {
            events_to_send: events,
            send_count: Arc::new(AtomicU32::new(0)),
            should_error: Arc::new(AtomicBool::new(false)),
            send_delay: Duration::from_millis(10),
        }
    }
}

#[async_trait]
impl SimpleIngestor for TestIngestor {
    fn name() -> &'static str {
        "test-ingestor"
    }
    
    fn version() -> &'static str {
        "1.0.0"
    }
    
    async fn capture_events(&mut self, event_tx: mpsc::Sender<RawEvent>) -> Result<()> {
        for event in &self.events_to_send {
            if self.should_error.load(Ordering::SeqCst) {
                return Err(anyhow::anyhow!("Test error"));
            }
            
            tokio::time::sleep(self.send_delay).await;
            
            event_tx.send(event.clone()).await?;
            self.send_count.fetch_add(1, Ordering::SeqCst);
        }
        
        // Keep running to test shutdown
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}

#[tokio::test]
async fn test_ingestor_runtime_basic_operation() -> Result<()> {
    let pool = setup_test_db().await?;
    
    let events = vec![
        RawEvent::new("test", "event1", json!({"id": 1})),
        RawEvent::new("test", "event2", json!({"id": 2})),
        RawEvent::new("test", "event3", json!({"id": 3})),
    ];
    
    let ingestor = TestIngestor::new(events.clone());
    let send_count = ingestor.send_count.clone();
    
    let config = IngestorConfig {
        heartbeat_interval: Duration::from_millis(100),
        batch_size: 2,
        batch_timeout: Duration::from_millis(50),
        dlq_enabled: true,
        dlq_path: None,
        retry_max_attempts: 3,
        retry_backoff: Duration::from_millis(100),
    };
    
    let runtime = IngestorRuntime::new(ingestor, pool.clone(), config);
    
    // Run for a short time
    let runtime_handle = tokio::spawn(async move {
        runtime.run().await
    });
    
    // Wait for events to be captured
    tokio::time::sleep(Duration::from_millis(200)).await;
    
    // Verify events were sent
    assert_eq!(send_count.load(Ordering::SeqCst), 3);
    
    // Cancel runtime
    runtime_handle.abort();
    
    // Verify events were stored
    let event_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM raw.events WHERE source = 'test'"
    )
    .fetch_one(&pool)
    .await?;
    
    assert_eq!(event_count, 3);
    
    // Verify manifest was registered
    let manifest_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sinex_schemas.agent_manifests WHERE name = 'test-ingestor'"
    )
    .fetch_one(&pool)
    .await?;
    
    assert_eq!(manifest_count, 1);
    
    Ok(())
}

#[tokio::test]
async fn test_ingestor_runtime_heartbeat() -> Result<()> {
    let pool = setup_test_db().await?;
    
    let ingestor = TestIngestor::new(vec![]);
    
    let config = IngestorConfig {
        heartbeat_interval: Duration::from_millis(50),
        batch_size: 10,
        batch_timeout: Duration::from_secs(1),
        dlq_enabled: false,
        dlq_path: None,
        retry_max_attempts: 3,
        retry_backoff: Duration::from_millis(100),
    };
    
    let runtime = IngestorRuntime::new(ingestor, pool.clone(), config);
    
    let runtime_handle = tokio::spawn(async move {
        runtime.run().await
    });
    
    // Wait for multiple heartbeats
    tokio::time::sleep(Duration::from_millis(200)).await;
    
    runtime_handle.abort();
    
    // Check heartbeat was updated
    let last_heartbeat: Option<chrono::DateTime<chrono::Utc>> = sqlx::query_scalar(
        "SELECT last_heartbeat FROM sinex_schemas.agent_manifests WHERE name = 'test-ingestor'"
    )
    .fetch_optional(&pool)
    .await?;
    
    assert!(last_heartbeat.is_some());
    
    let time_diff = chrono::Utc::now() - last_heartbeat.unwrap();
    assert!(time_diff.num_seconds() < 1, "Heartbeat should be recent");
    
    Ok(())
}

#[tokio::test]
async fn test_ingestor_runtime_batch_processing() -> Result<()> {
    let pool = setup_test_db().await?;
    
    // Create 10 events
    let events: Vec<_> = (0..10)
        .map(|i| RawEvent::new("test", "batch_event", json!({"id": i})))
        .collect();
    
    let mut ingestor = TestIngestor::new(events);
    ingestor.send_delay = Duration::from_millis(5); // Fast sending
    
    let config = IngestorConfig {
        heartbeat_interval: Duration::from_secs(1),
        batch_size: 3, // Small batches
        batch_timeout: Duration::from_millis(100),
        dlq_enabled: false,
        dlq_path: None,
        retry_max_attempts: 3,
        retry_backoff: Duration::from_millis(100),
    };
    
    let runtime = IngestorRuntime::new(ingestor, pool.clone(), config);
    
    let runtime_handle = tokio::spawn(async move {
        runtime.run().await
    });
    
    // Wait for processing
    tokio::time::sleep(Duration::from_millis(300)).await;
    
    runtime_handle.abort();
    
    // All events should be stored despite batching
    let event_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM raw.events WHERE event_type = 'batch_event'"
    )
    .fetch_one(&pool)
    .await?;
    
    assert_eq!(event_count, 10);
    
    Ok(())
}

#[tokio::test]
async fn test_ingestor_runtime_error_recovery() -> Result<()> {
    let pool = setup_test_db().await?;
    
    let events = vec![
        RawEvent::new("test", "event1", json!({"id": 1})),
        RawEvent::new("test", "event2", json!({"id": 2})),
    ];
    
    let ingestor = TestIngestor::new(events);
    let should_error = ingestor.should_error.clone();
    let send_count = ingestor.send_count.clone();
    
    let config = IngestorConfig {
        heartbeat_interval: Duration::from_secs(1),
        batch_size: 10,
        batch_timeout: Duration::from_millis(100),
        dlq_enabled: true,
        dlq_path: None,
        retry_max_attempts: 3,
        retry_backoff: Duration::from_millis(50),
    };
    
    let runtime = IngestorRuntime::new(ingestor, pool.clone(), config);
    
    // Make ingestor error initially
    should_error.store(true, Ordering::SeqCst);
    
    let runtime_handle = tokio::spawn(async move {
        runtime.run().await
    });
    
    // Wait for initial error
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // Stop erroring
    should_error.store(false, Ordering::SeqCst);
    
    // Wait for recovery and retry
    tokio::time::sleep(Duration::from_millis(200)).await;
    
    runtime_handle.abort();
    
    // Should have eventually sent events after recovery
    assert!(send_count.load(Ordering::SeqCst) > 0, "Should have sent events after recovery");
    
    Ok(())
}

#[tokio::test]
async fn test_ingestor_runtime_graceful_shutdown() -> Result<()> {
    let pool = setup_test_db().await?;
    
    let events = vec![RawEvent::new("test", "shutdown_test", json!({}))];
    let ingestor = TestIngestor::new(events);
    
    let config = IngestorConfig {
        heartbeat_interval: Duration::from_secs(1),
        batch_size: 10,
        batch_timeout: Duration::from_millis(100),
        dlq_enabled: false,
        dlq_path: None,
        retry_max_attempts: 3,
        retry_backoff: Duration::from_millis(100),
    };
    
    let runtime = IngestorRuntime::new(ingestor, pool.clone(), config);
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    
    let runtime_handle = tokio::spawn(async move {
        tokio::select! {
            result = runtime.run() => result,
            _ = shutdown_rx => {
                println!("Runtime received shutdown signal");
                Ok(())
            }
        }
    });
    
    // Let it run briefly
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // Send shutdown
    let _ = shutdown_tx.send(());
    
    // Should complete gracefully
    let result = timeout(Duration::from_secs(1), runtime_handle).await;
    assert!(result.is_ok(), "Runtime should shut down gracefully");
    
    Ok(())
}

struct PanicIngestor;

#[async_trait]
impl SimpleIngestor for PanicIngestor {
    fn name() -> &'static str {
        "panic-ingestor"
    }
    
    fn version() -> &'static str {
        "1.0.0"
    }
    
    async fn capture_events(&mut self, _event_tx: mpsc::Sender<RawEvent>) -> Result<()> {
        panic!("Ingestor panicked!");
    }
}

#[tokio::test]
async fn test_ingestor_runtime_panic_handling() -> Result<()> {
    let pool = setup_test_db().await?;
    
    let ingestor = PanicIngestor;
    
    let config = IngestorConfig {
        heartbeat_interval: Duration::from_secs(1),
        batch_size: 10,
        batch_timeout: Duration::from_millis(100),
        dlq_enabled: false,
        dlq_path: None,
        retry_max_attempts: 2,
        retry_backoff: Duration::from_millis(100),
    };
    
    let runtime = IngestorRuntime::new(ingestor, pool.clone(), config);
    
    let runtime_handle = tokio::spawn(async move {
        runtime.run().await
    });
    
    // Wait for panic and retries
    tokio::time::sleep(Duration::from_millis(500)).await;
    
    // Runtime should have failed
    assert!(runtime_handle.is_finished());
    
    Ok(())
}

struct SlowIngestor {
    event_count: u32,
}

#[async_trait]
impl SimpleIngestor for SlowIngestor {
    fn name() -> &'static str {
        "slow-ingestor"
    }
    
    fn version() -> &'static str {
        "1.0.0"
    }
    
    async fn capture_events(&mut self, event_tx: mpsc::Sender<RawEvent>) -> Result<()> {
        loop {
            // Slow event generation
            tokio::time::sleep(Duration::from_millis(200)).await;
            
            let event = RawEvent::new("slow", "event", json!({"count": self.event_count}));
            event_tx.send(event).await?;
            self.event_count += 1;
        }
    }
}

#[tokio::test]
async fn test_ingestor_runtime_batch_timeout() -> Result<()> {
    let pool = setup_test_db().await?;
    
    let ingestor = SlowIngestor { event_count: 0 };
    
    let config = IngestorConfig {
        heartbeat_interval: Duration::from_secs(1),
        batch_size: 10, // Large batch
        batch_timeout: Duration::from_millis(100), // Short timeout
        dlq_enabled: false,
        dlq_path: None,
        retry_max_attempts: 3,
        retry_backoff: Duration::from_millis(100),
    };
    
    let runtime = IngestorRuntime::new(ingestor, pool.clone(), config);
    
    let runtime_handle = tokio::spawn(async move {
        runtime.run().await
    });
    
    // Wait for several batch timeouts
    tokio::time::sleep(Duration::from_millis(500)).await;
    
    runtime_handle.abort();
    
    // Should have stored events despite not reaching batch size
    let event_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM raw.events WHERE source = 'slow'"
    )
    .fetch_one(&pool)
    .await?;
    
    assert!(event_count >= 2, "Should have stored at least 2 events via timeout");
    
    Ok(())
}