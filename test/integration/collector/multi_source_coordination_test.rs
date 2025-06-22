use anyhow::Result;
use sinex_core::{EventSource, EventSourceContext, RawEvent, create_registry};
use async_trait::async_trait;
use std::sync::{Arc, atomic::{AtomicU32, AtomicBool, Ordering}};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Barrier};
use serde_json::json;
use std::collections::HashMap;
use crate::common::event_sources;

// Test source that can simulate different behaviors
struct TestCoordinatedSource {
    source_id: String,
    events_generated: Arc<AtomicU32>,
    should_fail: Arc<AtomicBool>,
    startup_delay_ms: u64,
    event_delay_ms: u64,
}

#[async_trait]
impl EventSource for TestCoordinatedSource {
    type Config = serde_json::Value;
    const SOURCE_NAME: &'static str = "test_coordinated";
    
    async fn initialize(ctx: EventSourceContext) -> sinex_core::Result<Self> {
        let source_id = ctx.config["source_id"]
            .as_str()
            .unwrap_or("unknown")
            .to_string();
        
        let startup_delay_ms = ctx.config["startup_delay_ms"]
            .as_u64()
            .unwrap_or(0);
            
        let event_delay_ms = ctx.config["event_delay_ms"]
            .as_u64()
            .unwrap_or(100);
        
        // Simulate initialization time
        if startup_delay_ms > 0 {
            tokio::time::sleep(Duration::from_millis(startup_delay_ms)).await;
        }
        
        Ok(Self {
            source_id,
            events_generated: Arc::new(AtomicU32::new(0)),
            should_fail: Arc::new(AtomicBool::new(false)),
            startup_delay_ms,
            event_delay_ms,
        })
    }
    
    async fn stream_events(&mut self, tx: mpsc::Sender<RawEvent>) -> sinex_core::Result<()> {
        while !self.should_fail.load(Ordering::Relaxed) {
            let count = self.events_generated.fetch_add(1, Ordering::Relaxed);
            
            let event = sinex_core::RawEventBuilder::new(
                Self::SOURCE_NAME,
                "coordination.test",
                json!({
                    "source_id": self.source_id,
                    "event_number": count,
                    "timestamp": Instant::now().elapsed().as_millis(),
                })
            ).build();
            
            if tx.send(event).await.is_err() {
                break;
            }
            
            tokio::time::sleep(Duration::from_millis(self.event_delay_ms)).await;
        }
        
        Ok(())
    }
}

#[tokio::test]
async fn test_multiple_sources_lifecycle_management() -> Result<()> {
    let (tx, mut rx) = mpsc::channel::<RawEvent>(1000);
    
    // Start multiple sources with different configurations
    let mut handles = Vec::new();
    let mut source_controls = Vec::new();
    
    for i in 0..3 {
        let ctx = event_sources::test_context(json!({
            "source_id": format!("source_{}", i),
            "startup_delay_ms": i * 100, // Staggered startup
            "event_delay_ms": 50,
        }));
        
        let mut source = TestCoordinatedSource::initialize(ctx).await?;
        let events_generated = source.events_generated.clone();
        let should_fail = source.should_fail.clone();
        
        source_controls.push((events_generated, should_fail));
        
        let tx_clone = tx.clone();
        let handle = tokio::spawn(async move {
            source.stream_events(tx_clone).await
        });
        handles.push(handle);
    }
    
    // Let sources run
    tokio::time::sleep(Duration::from_millis(500)).await;
    
    // Stop sources in reverse order
    for (_, should_fail) in source_controls.iter().rev() {
        should_fail.store(true, Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    
    // Wait for all to complete
    for handle in handles {
        handle.await??;
    }
    
    drop(tx);
    
    // Collect all events
    let mut events_by_source: HashMap<String, Vec<RawEvent>> = HashMap::new();
    while let Ok(event) = rx.try_recv() {
        let source_id = event.payload["source_id"].as_str().unwrap().to_string();
        events_by_source.entry(source_id).or_default().push(event);
    }
    
    // Verify all sources produced events
    assert_eq!(events_by_source.len(), 3);
    
    for i in 0..3 {
        let source_id = format!("source_{}", i);
        assert!(events_by_source.contains_key(&source_id));
        assert!(!events_by_source[&source_id].is_empty());
    }
    
    Ok(())
}

#[tokio::test]
async fn test_source_failure_isolation() -> Result<()> {
    let (tx, mut rx) = mpsc::channel::<RawEvent>(1000);
    
    // Start multiple sources, one will fail
    let mut handles = Vec::new();
    let mut source_controls = Vec::new();
    
    for i in 0..3 {
        let ctx = event_sources::test_context(json!({
            "source_id": format!("source_{}", i),
            "event_delay_ms": 50,
        }));
        
        let mut source = TestCoordinatedSource::initialize(ctx).await?;
        let should_fail = source.should_fail.clone();
        
        source_controls.push(should_fail);
        
        let tx_clone = tx.clone();
        let handle = tokio::spawn(async move {
            source.stream_events(tx_clone).await
        });
        handles.push(handle);
    }
    
    // Let sources run
    tokio::time::sleep(Duration::from_millis(200)).await;
    
    // Fail the middle source
    source_controls[1].store(true, Ordering::Relaxed);
    
    // Other sources should continue
    tokio::time::sleep(Duration::from_millis(200)).await;
    
    // Stop remaining sources
    source_controls[0].store(true, Ordering::Relaxed);
    source_controls[2].store(true, Ordering::Relaxed);
    
    for handle in handles {
        handle.await??;
    }
    
    drop(tx);
    
    // Count events per source
    let mut event_counts: HashMap<String, usize> = HashMap::new();
    while let Ok(event) = rx.try_recv() {
        let source_id = event.payload["source_id"].as_str().unwrap().to_string();
        *event_counts.entry(source_id).or_default() += 1;
    }
    
    // All sources should have produced some events
    assert_eq!(event_counts.len(), 3);
    
    // Source 1 should have fewer events (failed early)
    assert!(event_counts["source_1"] < event_counts["source_0"]);
    assert!(event_counts["source_1"] < event_counts["source_2"]);
    
    Ok(())
}

#[tokio::test]
async fn test_source_startup_synchronization() -> Result<()> {
    let (tx, mut rx) = mpsc::channel::<RawEvent>(1000);
    let barrier = Arc::new(Barrier::new(3));
    
    // Start sources that need to coordinate startup
    let mut handles = Vec::new();
    
    for i in 0..3 {
        let barrier_clone = barrier.clone();
        let tx_clone = tx.clone();
        
        let handle = tokio::spawn(async move {
            let ctx = event_sources::test_context(json!({
                "source_id": format!("source_{}", i),
                "event_delay_ms": 50,
            }));
            
            let mut _source = TestCoordinatedSource::initialize(ctx).await?;
            
            // Wait for all sources to initialize
            barrier_clone.wait().await;
            
            // Now start streaming
            let mut event_count = 0;
            loop {
                let event = sinex_core::RawEventBuilder::new(
                    "test_coordinated",
                    "sync.test",
                    json!({
                        "source_id": format!("source_{}", i),
                        "sync_event": true,
                    })
                ).build();
                
                if tx_clone.send(event).await.is_err() {
                    break;
                }
                
                event_count += 1;
                if event_count >= 3 {
                    break;
                }
                
                tokio::task::yield_now().await;
            }
            
            Ok::<_, anyhow::Error>(())
        });
        
        handles.push(handle);
    }
    
    // Wait for all to complete
    for handle in handles {
        handle.await??;
    }
    
    drop(tx);
    
    // Verify synchronized startup
    let mut first_events = Vec::new();
    while let Ok(event) = rx.try_recv() {
        if event.payload["sync_event"].as_bool().unwrap_or(false) {
            first_events.push(event);
        }
    }
    
    // Should have events from all sources
    assert!(first_events.len() >= 9); // 3 sources × 3 events each
    
    Ok(())
}

#[tokio::test]
async fn test_registry_based_source_discovery() -> Result<()> {
    // Test that sources can be discovered and started from registry
    let registry = create_registry();
    
    // Get all available event types
    let all_types = registry.event_types;
    assert!(!all_types.is_empty());
    
    // Group by source
    let mut sources: HashMap<&str, Vec<&str>> = HashMap::new();
    for (event_type, source) in registry.event_to_source {
        sources.entry(source)
            .or_default()
            .push(event_type);
    }
    
    // Verify we have multiple sources registered
    assert!(sources.len() > 1);
    
    // Common sources should be present
    assert!(sources.contains_key("filesystem"));
    assert!(sources.contains_key("terminal.kitty"));
    
    Ok(())
}

#[tokio::test]
async fn test_dynamic_source_addition() -> Result<()> {
    let (tx, mut rx) = mpsc::channel::<RawEvent>(1000);
    let mut handles = Vec::new();
    
    // Start with 2 sources
    for i in 0..2 {
        let ctx = event_sources::test_context(json!({
            "source_id": format!("initial_{}", i),
            "event_delay_ms": 100,
        }));
        
        let mut source = TestCoordinatedSource::initialize(ctx).await?;
        let should_fail = source.should_fail.clone();
        
        let tx_clone = tx.clone();
        let handle = tokio::spawn(async move {
            tokio::time::timeout(
                Duration::from_millis(500),
                source.stream_events(tx_clone)
            ).await
        });
        
        handles.push((handle, should_fail));
    }
    
    // Let initial sources run
    tokio::time::sleep(Duration::from_millis(200)).await;
    
    // Add a new source dynamically
    let ctx = event_sources::test_context(json!({
        "source_id": "dynamic_source",
        "event_delay_ms": 50,
    }));
    
    let mut new_source = TestCoordinatedSource::initialize(ctx).await?;
    let new_should_fail = new_source.should_fail.clone();
    
    let tx_clone = tx.clone();
    let new_handle = tokio::spawn(async move {
        tokio::time::timeout(
            Duration::from_millis(300),
            new_source.stream_events(tx_clone)
        ).await
    });
    
    handles.push((new_handle, new_should_fail));
    
    // Wait for all to timeout/complete
    for (handle, _) in handles {
        let _ = handle.await;
    }
    
    drop(tx);
    
    // Count events by source
    let mut source_counts: HashMap<String, usize> = HashMap::new();
    while let Ok(event) = rx.try_recv() {
        let source_id = event.payload["source_id"].as_str().unwrap();
        *source_counts.entry(source_id.to_string()).or_default() += 1;
    }
    
    // Should have events from all sources including dynamically added
    assert!(source_counts.contains_key("initial_0"));
    assert!(source_counts.contains_key("initial_1"));
    assert!(source_counts.contains_key("dynamic_source"));
    
    // Dynamic source should have produced events despite late start
    assert!(source_counts["dynamic_source"] > 0);
    
    Ok(())
}