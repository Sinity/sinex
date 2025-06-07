use anyhow::Result;
use async_trait::async_trait;
use sinex_shared::{
    SimpleIngestor, IngestorRuntime, RuntimeConfig, MemorySink,
    event_types::RawEventBuilder,
};
use sinex_db::models::RawEvent;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::timeout;

/// A simple test ingestor that emits a few events then stops
struct TestIngestor {
    events_to_emit: usize,
    emitted: usize,
}

impl TestIngestor {
    fn new(events_to_emit: usize) -> Self {
        Self {
            events_to_emit,
            emitted: 0,
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
        while self.emitted < self.events_to_emit {
            let event = RawEventBuilder::new(
                "test",
                "test.event",
                serde_json::json!({
                    "counter": self.emitted,
                    "message": format!("Test event {}", self.emitted),
                }),
            )
            .build();
            
            event_tx.send(event).await?;
            self.emitted += 1;
            
            // Small delay between events
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        
        // Keep running to let runtime handle shutdown
        tokio::time::sleep(Duration::from_secs(60)).await;
        Ok(())
    }
}

#[tokio::test]
async fn test_ingestor_runtime_basic() {
    // Create a memory sink to capture events
    let memory_sink = Arc::new(MemorySink::new());
    let sink: Arc<dyn sinex_shared::EventSink> = Arc::clone(&memory_sink) as Arc<dyn sinex_shared::EventSink>;
    
    // Create runtime config with fast heartbeat for testing
    let config = RuntimeConfig {
        heartbeat_interval_secs: 1,
        batch_size: None,
        batch_timeout_ms: None,
        ..Default::default()
    };
    
    // Create the test ingestor
    let ingestor = TestIngestor::new(5);
    
    // Create and run the runtime
    let runtime = IngestorRuntime::new(ingestor, sink, config).unwrap();
    
    // Run in background
    let runtime_handle = tokio::spawn(async move {
        let _ = runtime.run().await;
    });
    
    // Wait a bit for events to be captured
    tokio::time::sleep(Duration::from_millis(200)).await;
    
    // Check that we got the expected events
    let events = memory_sink.get_events().await;
    
    // Should have at least the test events
    let test_events: Vec<_> = events.iter()
        .filter(|e| e.source == "test" && e.event_type == "test.event")
        .collect();
    
    assert_eq!(test_events.len(), 5, "Should have captured 5 test events");
    
    // Should also have heartbeats
    let heartbeat_events: Vec<_> = events.iter()
        .filter(|e| e.source == "sinex" && e.event_type == "agent.heartbeat")
        .collect();
    
    assert!(!heartbeat_events.is_empty(), "Should have captured at least one heartbeat");
    
    // Verify heartbeat content
    if let Some(heartbeat) = heartbeat_events.first() {
        let agent_name = heartbeat.payload["agent_name"].as_str().unwrap();
        assert_eq!(agent_name, "test-ingestor");
    }
    
    // Cancel runtime
    runtime_handle.abort();
}

#[tokio::test]
async fn test_ingestor_runtime_with_batching() {
    // Create a memory sink
    let memory_sink = Arc::new(MemorySink::new());
    let sink: Arc<dyn sinex_shared::EventSink> = Arc::clone(&memory_sink) as Arc<dyn sinex_shared::EventSink>;
    
    // Create runtime config with batching
    let config = RuntimeConfig {
        heartbeat_interval_secs: 60, // Slow heartbeat
        batch_size: Some(3),
        batch_timeout_ms: Some(100),
        ..Default::default()
    };
    
    // Create test ingestor
    let ingestor = TestIngestor::new(10);
    
    // Create and run runtime
    let runtime = IngestorRuntime::new(ingestor, sink, config).unwrap();
    
    let runtime_handle = tokio::spawn(async move {
        let _ = runtime.run().await;
    });
    
    // Wait for batching to happen
    tokio::time::sleep(Duration::from_millis(500)).await;
    
    // Check events
    let events = memory_sink.get_events().await;
    let test_events: Vec<_> = events.iter()
        .filter(|e| e.source == "test")
        .collect();
    
    assert_eq!(test_events.len(), 10, "Should have all 10 test events");
    
    runtime_handle.abort();
}

#[tokio::test] 
async fn test_runtime_shutdown_signal() {
    use std::sync::atomic::{AtomicBool, Ordering};
    
    // Custom ingestor that tracks shutdown
    struct ShutdownTestIngestor {
        running: Arc<AtomicBool>,
    }
    
    #[async_trait]
    impl SimpleIngestor for ShutdownTestIngestor {
        fn name() -> &'static str {
            "shutdown-test"
        }
        
        fn version() -> &'static str {
            "1.0.0"
        }
        
        async fn capture_events(&mut self, _event_tx: mpsc::Sender<RawEvent>) -> Result<()> {
            self.running.store(true, Ordering::Relaxed);
            
            // Just wait for shutdown
            loop {
                tokio::time::sleep(Duration::from_millis(100)).await;
                
                if !self.running.load(Ordering::Relaxed) {
                    break;
                }
            }
            
            Ok(())
        }
    }
    
    let running = Arc::new(AtomicBool::new(false));
    let ingestor = ShutdownTestIngestor {
        running: Arc::clone(&running),
    };
    
    let sink = Arc::new(MemorySink::new());
    let runtime = IngestorRuntime::new(ingestor, sink, RuntimeConfig::default()).unwrap();
    
    // Run with timeout
    let result = timeout(Duration::from_secs(1), runtime.run()).await;
    
    // Should timeout since we don't trigger shutdown
    assert!(result.is_err());
}

#[tokio::test]
async fn test_error_handling_in_runtime() {
    // Ingestor that generates an error
    struct ErrorIngestor {
        error_after: usize,
        count: usize,
    }
    
    #[async_trait]
    impl SimpleIngestor for ErrorIngestor {
        fn name() -> &'static str {
            "error-test"
        }
        
        fn version() -> &'static str {
            "1.0.0"
        }
        
        async fn capture_events(&mut self, event_tx: mpsc::Sender<RawEvent>) -> Result<()> {
            loop {
                if self.count >= self.error_after {
                    return Err(anyhow::anyhow!("Simulated error"));
                }
                
                let event = RawEventBuilder::new(
                    "test",
                    "test.event",
                    serde_json::json!({"count": self.count}),
                )
                .build();
                
                event_tx.send(event).await?;
                self.count += 1;
                
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    }
    
    let memory_sink = Arc::new(MemorySink::new());
    let sink: Arc<dyn sinex_shared::EventSink> = Arc::clone(&memory_sink) as Arc<dyn sinex_shared::EventSink>;
    let ingestor = ErrorIngestor {
        error_after: 3,
        count: 0,
    };
    
    let runtime = IngestorRuntime::new(ingestor, sink, RuntimeConfig::default()).unwrap();
    
    // Run should complete with error
    let result = runtime.run().await;
    assert!(result.is_err());
    
    // But we should still have captured the events before the error
    let events = memory_sink.get_events().await;
    let test_events: Vec<_> = events.iter()
        .filter(|e| e.source == "test")
        .collect();
    
    assert_eq!(test_events.len(), 3, "Should have captured 3 events before error");
}