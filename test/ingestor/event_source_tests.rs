use anyhow::Result;
use sinex_db::models::RawEvent;
use sinex_shared::event_types::RawEventBuilder;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::{timeout, sleep};

/// Test helper to create a mock event source
struct MockEventSource {
    name: String,
    event_type: String,
    interval: Duration,
    error_after: Option<usize>,
    events_sent: usize,
}

impl MockEventSource {
    fn new(name: &str, event_type: &str, interval: Duration) -> Self {
        Self {
            name: name.to_string(),
            event_type: event_type.to_string(),
            interval,
            error_after: None,
            events_sent: 0,
        }
    }
    
    fn with_error_after(mut self, count: usize) -> Self {
        self.error_after = Some(count);
        self
    }
    
    async fn stream_events(&mut self, tx: mpsc::Sender<RawEvent>) -> Result<()> {
        loop {
            sleep(self.interval).await;
            
            // Check if we should simulate an error
            if let Some(error_count) = self.error_after {
                if self.events_sent >= error_count {
                    return Err(anyhow::anyhow!("Simulated error after {} events", error_count));
                }
            }
            
            let event = RawEventBuilder::new(
                &self.name,
                &self.event_type,
                serde_json::json!({
                    "source": self.name.clone(),
                    "sequence": self.events_sent,
                    "timestamp": chrono::Utc::now(),
                }),
            )
            .build();
            
            tx.send(event).await?;
            self.events_sent += 1;
        }
    }
}

#[tokio::test]
async fn test_single_event_source_streaming() -> Result<()> {
    let (tx, mut rx) = mpsc::channel(100);
    
    let mut source = MockEventSource::new("test_source", "test.event", Duration::from_millis(100));
    
    // Stream events for a limited time
    let stream_task = tokio::spawn(async move {
        let _ = timeout(
            Duration::from_millis(550),
            source.stream_events(tx),
        )
        .await;
    });
    
    // Collect events
    let mut events = Vec::new();
    sleep(Duration::from_millis(600)).await;
    
    while let Ok(event) = rx.try_recv() {
        events.push(event);
    }
    
    stream_task.await?;
    
    // Should have ~5 events (one every 100ms for 550ms)
    assert!(events.len() >= 4 && events.len() <= 6, "Expected 4-6 events, got {}", events.len());
    
    // Verify event structure
    for (i, event) in events.iter().enumerate() {
        assert_eq!(event.source, "test_source");
        assert_eq!(event.event_type, "test.event");
        assert_eq!(event.payload["sequence"], i as i64);
    }
    
    Ok(())
}

#[tokio::test]
async fn test_multiple_event_sources_concurrent() -> Result<()> {
    let (tx, mut rx) = mpsc::channel(100);
    
    // Create sources with different intervals
    let mut fast_source = MockEventSource::new("fast", "fast.event", Duration::from_millis(50));
    let mut slow_source = MockEventSource::new("slow", "slow.event", Duration::from_millis(150));
    
    let tx1 = tx.clone();
    let tx2 = tx.clone();
    drop(tx); // Drop original to ensure channel closes when tasks complete
    
    // Run sources concurrently
    let fast_task = tokio::spawn(async move {
        let _ = timeout(
            Duration::from_millis(300),
            fast_source.stream_events(tx1),
        )
        .await;
    });
    
    let slow_task = tokio::spawn(async move {
        let _ = timeout(
            Duration::from_millis(300),
            slow_source.stream_events(tx2),
        )
        .await;
    });
    
    // Wait for tasks
    sleep(Duration::from_millis(350)).await;
    
    // Collect and categorize events
    let mut fast_events = 0;
    let mut slow_events = 0;
    
    while let Ok(event) = rx.try_recv() {
        match event.source.as_str() {
            "fast" => fast_events += 1,
            "slow" => slow_events += 1,
            _ => panic!("Unexpected source"),
        }
    }
    
    fast_task.await?;
    slow_task.await?;
    
    // Fast source (50ms interval) should have ~6 events in 300ms
    assert!(fast_events >= 5 && fast_events <= 7, "Expected 5-7 fast events, got {}", fast_events);
    
    // Slow source (150ms interval) should have ~2 events in 300ms
    assert!(slow_events >= 1 && slow_events <= 3, "Expected 1-3 slow events, got {}", slow_events);
    
    Ok(())
}

#[tokio::test]
async fn test_event_source_error_handling() -> Result<()> {
    let (tx, mut rx) = mpsc::channel(100);
    
    // Create source that errors after 3 events
    let mut source = MockEventSource::new("error_source", "test.event", Duration::from_millis(100))
        .with_error_after(3);
    
    let result = source.stream_events(tx).await;
    
    // Should have failed
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Simulated error"));
    
    // Should have received exactly 3 events before error
    let mut count = 0;
    while let Ok(_) = rx.try_recv() {
        count += 1;
    }
    assert_eq!(count, 3);
    
    Ok(())
}

#[tokio::test]
async fn test_event_source_backpressure() -> Result<()> {
    // Small channel to test backpressure
    let (tx, mut rx) = mpsc::channel(2);
    
    let mut source = MockEventSource::new("pressure", "test.event", Duration::from_millis(10));
    
    // Start streaming
    let stream_task = tokio::spawn(async move {
        let _ = timeout(
            Duration::from_millis(100),
            source.stream_events(tx),
        )
        .await;
    });
    
    // Don't read immediately to create backpressure
    sleep(Duration::from_millis(50)).await;
    
    // Now drain the channel
    let mut events = Vec::new();
    while let Ok(event) = rx.try_recv() {
        events.push(event);
    }
    
    stream_task.await?;
    
    // Should have events, but limited by backpressure
    assert!(!events.is_empty());
    assert!(events.len() <= 10); // Reasonable upper bound
    
    Ok(())
}

#[tokio::test]
async fn test_event_source_ordering() -> Result<()> {
    let (tx, mut rx) = mpsc::channel(100);
    
    let mut source = MockEventSource::new("ordered", "test.event", Duration::from_millis(50));
    
    let stream_task = tokio::spawn(async move {
        let _ = timeout(
            Duration::from_millis(250),
            source.stream_events(tx),
        )
        .await;
    });
    
    sleep(Duration::from_millis(300)).await;
    
    // Collect events and verify ordering
    let mut events = Vec::new();
    while let Ok(event) = rx.try_recv() {
        events.push(event);
    }
    
    stream_task.await?;
    
    // Verify sequence numbers are in order
    for (i, event) in events.iter().enumerate() {
        assert_eq!(event.payload["sequence"], i as i64);
    }
    
    Ok(())
}

/// Test event source initialization and configuration
#[tokio::test]
async fn test_event_source_initialization() -> Result<()> {
    // Test various initialization scenarios
    let sources = vec![
        MockEventSource::new("source1", "type1", Duration::from_secs(1)),
        MockEventSource::new("source2", "type2", Duration::from_millis(500)),
        MockEventSource::new("source3", "type3", Duration::from_millis(100)),
    ];
    
    // Verify each source has correct properties
    assert_eq!(sources[0].name, "source1");
    assert_eq!(sources[0].event_type, "type1");
    assert_eq!(sources[0].interval, Duration::from_secs(1));
    
    assert_eq!(sources[1].name, "source2");
    assert_eq!(sources[1].event_type, "type2");
    assert_eq!(sources[1].interval, Duration::from_millis(500));
    
    assert_eq!(sources[2].name, "source3");
    assert_eq!(sources[2].event_type, "type3");
    assert_eq!(sources[2].interval, Duration::from_millis(100));
    
    Ok(())
}

/// Test dynamic event source selection
#[tokio::test]
async fn test_dynamic_source_selection() -> Result<()> {
    let (tx, mut rx) = mpsc::channel(100);
    
    // Configuration specifying which sources to enable
    let enabled_sources = vec!["source1", "source3"];
    
    // Create all possible sources
    let all_sources = vec![
        ("source1", MockEventSource::new("source1", "type1", Duration::from_millis(100))),
        ("source2", MockEventSource::new("source2", "type2", Duration::from_millis(100))),
        ("source3", MockEventSource::new("source3", "type3", Duration::from_millis(100))),
    ];
    
    // Start only enabled sources
    let mut tasks = Vec::new();
    for (name, mut source) in all_sources {
        if enabled_sources.contains(&name) {
            let tx_clone = tx.clone();
            tasks.push(tokio::spawn(async move {
                let _ = timeout(
                    Duration::from_millis(250),
                    source.stream_events(tx_clone),
                )
                .await;
            }));
        }
    }
    
    drop(tx); // Allow channel to close when tasks complete
    
    // Wait for all tasks
    sleep(Duration::from_millis(300)).await;
    
    // Collect events and verify only enabled sources sent events
    let mut sources_seen = std::collections::HashSet::new();
    while let Ok(event) = rx.try_recv() {
        sources_seen.insert(event.source.clone());
    }
    
    // Wait for tasks to complete
    for task in tasks {
        task.await?;
    }
    
    // Verify only enabled sources produced events
    assert!(sources_seen.contains("source1"));
    assert!(!sources_seen.contains("source2")); // Not enabled
    assert!(sources_seen.contains("source3"));
    
    Ok(())
}

/// Test event batching behavior
#[tokio::test]
async fn test_event_source_batching() -> Result<()> {
    let (tx, mut rx) = mpsc::channel(100);
    
    // Create a fast source to generate many events quickly
    let mut source = MockEventSource::new("batch_test", "batch.event", Duration::from_millis(10));
    
    let stream_task = tokio::spawn(async move {
        let _ = timeout(
            Duration::from_millis(100),
            source.stream_events(tx),
        )
        .await;
    });
    
    // Wait for events to accumulate
    sleep(Duration::from_millis(150)).await;
    
    // Collect events in batches
    let mut batches = Vec::new();
    let mut current_batch = Vec::new();
    
    while let Ok(event) = rx.try_recv() {
        current_batch.push(event);
        
        // Simulate batch processing every 5 events
        if current_batch.len() >= 5 {
            batches.push(std::mem::take(&mut current_batch));
        }
    }
    
    // Don't forget the last partial batch
    if !current_batch.is_empty() {
        batches.push(current_batch);
    }
    
    stream_task.await?;
    
    // Should have at least one full batch
    assert!(!batches.is_empty());
    
    // Verify batch integrity
    let mut total_events = 0;
    for batch in &batches {
        total_events += batch.len();
        
        // Each event in a batch should be from the same source
        for event in batch {
            assert_eq!(event.source, "batch_test");
        }
    }
    
    assert!(total_events >= 8); // ~10 events expected (100ms / 10ms interval)
    
    Ok(())
}