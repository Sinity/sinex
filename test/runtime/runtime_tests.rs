use anyhow::Result;
use async_trait::async_trait;
use sinex_shared::{
    SimpleIngestor, IngestorRuntime, RuntimeConfig, MemorySink,
    event_types::RawEventBuilder, sources, EventSink,
};
use sinex_db::models::RawEvent;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time;

/// Test ingestor that emits events at regular intervals
struct TestIngestor {
    event_count: usize,
    interval_ms: u64,
}

impl TestIngestor {
    fn new(event_count: usize, interval_ms: u64) -> Self {
        Self { event_count, interval_ms }
    }
}

#[async_trait]
impl SimpleIngestor for TestIngestor {
    fn name() -> &'static str {
        "test-ingestor"
    }
    
    fn version() -> &'static str {
        "0.1.0"
    }
    
    async fn capture_events(&mut self, event_tx: mpsc::Sender<RawEvent>) -> Result<()> {
        let mut interval = time::interval(Duration::from_millis(self.interval_ms));
        
        for i in 0..self.event_count {
            interval.tick().await;
            
            let event = RawEventBuilder::new(
                "test",
                "test.event",
                serde_json::json!({
                    "index": i,
                    "message": format!("Test event {}", i),
                }),
            )
            .build();
            
            event_tx.send(event).await?;
        }
        
        Ok(())
    }
}

/// Test ingestor that fails after emitting some events
struct FailingIngestor {
    events_before_failure: usize,
}

#[async_trait]
impl SimpleIngestor for FailingIngestor {
    fn name() -> &'static str {
        "failing-ingestor"
    }
    
    fn version() -> &'static str {
        "0.1.0"
    }
    
    async fn capture_events(&mut self, event_tx: mpsc::Sender<RawEvent>) -> Result<()> {
        for i in 0..self.events_before_failure {
            let event = RawEventBuilder::new(
                "test",
                "test.event",
                serde_json::json!({
                    "index": i,
                }),
            )
            .build();
            
            event_tx.send(event).await?;
        }
        
        Err(anyhow::anyhow!("Simulated failure"))
    }
}

#[tokio::test]
async fn test_runtime_startup_shutdown_events() {
    let ingestor = TestIngestor::new(0, 100);
    let memory_sink = Arc::new(MemorySink::new());
    let event_sink: Arc<dyn EventSink> = Arc::clone(&memory_sink) as Arc<dyn EventSink>;
    let runtime = IngestorRuntime::new(
        ingestor,
        event_sink,
        RuntimeConfig::default(),
    ).unwrap();
    
    // Run the runtime
    runtime.run().await.unwrap();
    
    // Check events - should have at least one heartbeat
    let events = memory_sink.get_events().await;
    assert!(events.len() >= 1, "Should have at least one heartbeat event");
    
    // Check for heartbeat
    let has_heartbeat = events.iter().any(|e| 
        e.source == sources::SINEX && e.event_type == "agent.heartbeat"
    );
    assert!(has_heartbeat, "Should have emitted heartbeat");
}

#[tokio::test]
async fn test_runtime_event_processing() {
    let ingestor = TestIngestor::new(5, 10);
    let memory_sink = Arc::new(MemorySink::new());
    let event_sink: Arc<dyn EventSink> = Arc::clone(&memory_sink) as Arc<dyn EventSink>;
    let runtime = IngestorRuntime::new(
        ingestor,
        event_sink,
        RuntimeConfig::default(),
    ).unwrap();
    
    // Run the runtime
    runtime.run().await.unwrap();
    
    // Check events
    let events = memory_sink.get_events().await;
    
    // Should have 5 test events + at least 1 heartbeat
    assert!(events.len() >= 6, "Should have test events and heartbeat");
    
    // Count test events
    let test_events: Vec<_> = events.iter()
        .filter(|e| e.source == "test" && e.event_type == "test.event")
        .collect();
    assert_eq!(test_events.len(), 5, "Should have exactly 5 test events");
    
    // Verify event order
    for (i, event) in test_events.iter().enumerate() {
        let index = event.payload.get("index").and_then(|v| v.as_u64()).unwrap();
        assert_eq!(index as usize, i, "Events should be in order");
    }
}

#[tokio::test]
async fn test_runtime_batch_processing() {
    let ingestor = TestIngestor::new(10, 5);
    let memory_sink = Arc::new(MemorySink::new());
    let event_sink: Arc<dyn EventSink> = Arc::clone(&memory_sink) as Arc<dyn EventSink>;
    
    let config = RuntimeConfig {
        batch_size: Some(3),
        batch_timeout_ms: Some(50),
        ..Default::default()
    };
    
    let runtime = IngestorRuntime::new(
        ingestor,
        event_sink,
        config,
    ).unwrap();
    
    // Run the runtime
    runtime.run().await.unwrap();
    
    // Check events
    let events = memory_sink.get_events().await;
    
    // Should have all 10 test events
    let test_events: Vec<_> = events.iter()
        .filter(|e| e.source == "test" && e.event_type == "test.event")
        .collect();
    assert_eq!(test_events.len(), 10, "Should have all test events despite batching");
}

#[tokio::test]
async fn test_runtime_handles_ingestor_failure() {
    let ingestor = FailingIngestor { events_before_failure: 3 };
    let memory_sink = Arc::new(MemorySink::new());
    let event_sink: Arc<dyn EventSink> = Arc::clone(&memory_sink) as Arc<dyn EventSink>;
    let runtime = IngestorRuntime::new(
        ingestor,
        event_sink,
        RuntimeConfig::default(),
    ).unwrap();
    
    // Run should complete even with failure
    let result = runtime.run().await;
    assert!(result.is_err(), "Runtime should propagate ingestor error");
    
    // But events before failure should be processed
    let events = memory_sink.get_events().await;
    let test_events: Vec<_> = events.iter()
        .filter(|e| e.source == "test" && e.event_type == "test.event")
        .collect();
    assert_eq!(test_events.len(), 3, "Should have events emitted before failure");
}

#[tokio::test]
async fn test_runtime_heartbeat_interval() {
    let ingestor = TestIngestor::new(0, 100);
    let memory_sink = Arc::new(MemorySink::new());
    let event_sink: Arc<dyn EventSink> = Arc::clone(&memory_sink) as Arc<dyn EventSink>;
    
    let config = RuntimeConfig {
        heartbeat_interval_secs: 1, // Fast heartbeat for testing
        ..Default::default()
    };
    
    let runtime = IngestorRuntime::new(
        ingestor,
        event_sink,
        config,
    ).unwrap();
    
    // Run for a bit to collect heartbeats
    let runtime_handle = tokio::spawn(async move {
        runtime.run().await
    });
    
    // Wait for 2.5 seconds
    tokio::time::sleep(Duration::from_millis(2500)).await;
    
    // Cancel the runtime
    runtime_handle.abort();
    
    // Check heartbeats
    let events = memory_sink.get_events().await;
    let heartbeats: Vec<_> = events.iter()
        .filter(|e| e.source == sources::SINEX && e.event_type == "agent.heartbeat")
        .collect();
    
    // Should have at least 2 heartbeats (one at start, one after 1s, maybe one after 2s)
    assert!(heartbeats.len() >= 2, "Should have multiple heartbeats, got {}", heartbeats.len());
}