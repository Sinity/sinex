use sinex_core::{EventSource, EventSourceContext, RawEvent, EventSender, sources};
use sinex_db::test::*;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tokio::time::{sleep, timeout};
use tokio::sync::Mutex;
use serde_json::json;
use async_trait::async_trait;

/// Mock event source that generates events at a configurable rate
struct MockEventSource {
    source_name: String,
    event_count: usize,
    event_interval: Duration,
    event_size: usize,
    events_sent: Arc<AtomicUsize>,
    should_stop: Arc<AtomicBool>,
}

impl MockEventSource {
    fn new(source_name: &str, event_count: usize, event_interval: Duration, event_size: usize) -> Self {
        Self {
            source_name: source_name.to_string(),
            event_count,
            event_interval,
            event_size,
            events_sent: Arc::new(AtomicUsize::new(0)),
            should_stop: Arc::new(AtomicBool::new(false)),
        }
    }
    
    fn stop(&self) {
        self.should_stop.store(true, Ordering::Relaxed);
    }
    
    fn events_sent(&self) -> usize {
        self.events_sent.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl EventSource for MockEventSource {
    type Config = serde_json::Value;
    const SOURCE_NAME: &'static str = "test.mock";

    async fn initialize(_ctx: EventSourceContext) -> sinex_core::Result<Self> {
        Ok(Self::new("test.mock", 100, Duration::from_millis(10), 1024))
    }

    async fn stream_events(&mut self, tx: EventSender) -> sinex_core::Result<()> {
        for i in 0..self.event_count {
            if self.should_stop.load(Ordering::Relaxed) {
                break;
            }
            
            let content = "x".repeat(self.event_size);
            let event = sinex_core::RawEventBuilder::new(
                &self.source_name,
                "mock.event",
                json!({
                    "sequence": i,
                    "source": self.source_name,
                    "content": content,
                    "timestamp": chrono::Utc::now().to_rfc3339()
                })
            ).build();
            
            if tx.send(event).await.is_err() {
                return Err(sinex_core::CoreError::Other("Channel closed".to_string()));
            }
            
            self.events_sent.fetch_add(1, Ordering::Relaxed);
            
            if self.event_interval > Duration::ZERO {
                sleep(self.event_interval).await;
            }
        }
        
        Ok(())
    }
}

#[sinex_test]
async fn test_dual_source_concurrent_events(ctx: TestContext) -> TestResult {
    // Test two event sources running concurrently
    let (tx, mut rx) = tokio::sync::mpsc::channel(1000);
    
    // Create two sources with different characteristics
    let mut source1 = MockEventSource::new("fs", 50, Duration::from_millis(20), 512);
    let mut source2 = MockEventSource::new("clipboard", 30, Duration::from_millis(30), 1024);
    
    // Run both sources concurrently
    let tx1 = tx.clone();
    let tx2 = tx.clone();
    
    let task1 = tokio::spawn(async move {
        source1.stream_events(tx1).await
    });
    
    let task2 = tokio::spawn(async move {
        source2.stream_events(tx2).await
    });
    
    drop(tx); // Close the original sender
    
    // Collect events from both sources
    let mut events_by_source = HashMap::new();
    let mut total_events = 0;
    
    while let Some(event) = rx.recv().await {
        *events_by_source.entry(event.source.clone()).or_insert(0) += 1;
        total_events += 1;
        
        // Insert into database
        sinex_db::insert_event(ctx.pool(), &event).await?;
    }
    
    // Wait for both tasks to complete
    let result1 = task1.await.unwrap();
    let result2 = task2.await.unwrap();
    
    assert!(result1.is_ok(), "Source 1 should complete successfully");
    assert!(result2.is_ok(), "Source 2 should complete successfully");
    
    // Verify we received events from both sources
    assert_eq!(events_by_source.len(), 2, "Should have events from both sources");
    assert_eq!(events_by_source.get("fs").unwrap_or(&0), &50, "Should have 50 events from fs");
    assert_eq!(events_by_source.get("clipboard").unwrap_or(&0), &30, "Should have 30 events from clipboard");
    assert_eq!(total_events, 80, "Should have total of 80 events");
    
    // Verify database consistency
    let stored_count = sinex_db::count_events(ctx.pool()).await?;
    assert_eq!(stored_count, total_events, "All events should be stored in database");
    
    Ok(())
}

#[sinex_test]
async fn test_many_sources_high_throughput(ctx: TestContext) -> TestResult {
    // Test many event sources running concurrently with high throughput
    let source_count = 8;
    let events_per_source = 25;
    let (tx, mut rx) = tokio::sync::mpsc::channel(source_count * events_per_source * 2);
    
    let mut tasks = Vec::new();
    let mut sources = Vec::new();
    
    // Create multiple sources
    for i in 0..source_count {
        let source_name = format!("source_{}", i);
        let mut source = MockEventSource::new(
            &source_name,
            events_per_source,
            Duration::from_millis(5), // High frequency
            256 // Smaller payloads for speed
        );
        
        let tx_clone = tx.clone();
        let task = tokio::spawn(async move {
            let start = Instant::now();
            let result = source.stream_events(tx_clone).await;
            let duration = start.elapsed();
            (source_name, result, duration, source.events_sent())
        });
        
        tasks.push(task);
        sources.push(source);
    }
    
    drop(tx); // Close the original sender
    
    // Collect events with timing
    let start_collection = Instant::now();
    let mut events_by_source = HashMap::new();
    let mut total_events = 0;
    let mut event_times = Vec::new();
    
    while let Some(event) = rx.recv().await {
        event_times.push(Instant::now());
        *events_by_source.entry(event.source.clone()).or_insert(0) += 1;
        total_events += 1;
        
        // Insert with batching for performance
        sinex_db::insert_event(ctx.pool(), &event).await?;
    }
    
    let collection_duration = start_collection.elapsed();
    
    // Wait for all tasks to complete
    let mut successful_sources = 0;
    let mut total_duration = Duration::ZERO;
    
    for task in tasks {
        let (source_name, result, duration, events_sent) = task.await.unwrap();
        
        match result {
            Ok(()) => {
                successful_sources += 1;
                total_duration += duration;
                println!("Source '{}' sent {} events in {:?}", source_name, events_sent, duration);
            }
            Err(e) => {
                println!("Source '{}' failed: {}", source_name, e);
            }
        }
    }
    
    // Performance analysis
    let expected_total = source_count * events_per_source;
    let events_per_second = total_events as f64 / collection_duration.as_secs_f64();
    
    println!("Performance Summary:");
    println!("- Total events: {}/{}", total_events, expected_total);
    println!("- Successful sources: {}/{}", successful_sources, source_count);
    println!("- Collection time: {:?}", collection_duration);
    println!("- Events per second: {:.2}", events_per_second);
    println!("- Average source duration: {:?}", total_duration / successful_sources as u32);
    
    // Verify results
    assert_eq!(successful_sources, source_count, "All sources should complete successfully");
    assert_eq!(total_events, expected_total, "Should receive all expected events");
    assert_eq!(events_by_source.len(), source_count, "Should have events from all sources");
    
    // Performance assertions
    assert!(events_per_second > 100.0, "Should process at least 100 events per second");
    assert!(collection_duration < Duration::from_secs(30), "Should complete within 30 seconds");
    
    // Verify database consistency
    let stored_count = sinex_db::count_events(ctx.pool()).await?;
    assert_eq!(stored_count, total_events, "All events should be stored in database");
    
    Ok(())
}

#[sinex_test]
async fn test_mixed_load_sources(ctx: TestContext) -> TestResult {
    // Test sources with different load characteristics
    let (tx, mut rx) = tokio::sync::mpsc::channel(2000);
    
    // High-frequency, small events (like filesystem)
    let mut high_freq_source = MockEventSource::new("fs", 100, Duration::from_millis(5), 128);
    
    // Medium-frequency, medium events (like clipboard)
    let mut medium_freq_source = MockEventSource::new("clipboard", 50, Duration::from_millis(20), 512);
    
    // Low-frequency, large events (like terminal output)
    let mut low_freq_source = MockEventSource::new("terminal", 20, Duration::from_millis(50), 2048);
    
    // Burst source (sporadic high activity)
    let mut burst_source = MockEventSource::new("dbus", 30, Duration::from_millis(100), 256);
    
    // Run all sources concurrently
    let tasks = vec![
        tokio::spawn({
            let tx = tx.clone();
            async move { high_freq_source.stream_events(tx).await }
        }),
        tokio::spawn({
            let tx = tx.clone();
            async move { medium_freq_source.stream_events(tx).await }
        }),
        tokio::spawn({
            let tx = tx.clone();
            async move { low_freq_source.stream_events(tx).await }
        }),
        tokio::spawn({
            let tx = tx.clone();
            async move { burst_source.stream_events(tx).await }
        }),
    ];
    
    drop(tx);
    
    // Collect events and analyze patterns
    let mut events_by_source = HashMap::new();
    let mut events_timeline = Vec::new();
    let start_time = Instant::now();
    
    while let Some(event) = rx.recv().await {
        let timestamp = start_time.elapsed();
        events_timeline.push((event.source.clone(), timestamp));
        *events_by_source.entry(event.source.clone()).or_insert(0) += 1;
        
        sinex_db::insert_event(ctx.pool(), &event).await?;
    }
    
    // Wait for completion
    for task in tasks {
        task.await.unwrap().unwrap();
    }
    
    // Analyze event distribution
    assert_eq!(events_by_source.get("fs").unwrap_or(&0), &100);
    assert_eq!(events_by_source.get("clipboard").unwrap_or(&0), &50);
    assert_eq!(events_by_source.get("terminal").unwrap_or(&0), &20);
    assert_eq!(events_by_source.get("dbus").unwrap_or(&0), &30);
    
    // Analyze timing patterns
    let fs_events: Vec<_> = events_timeline.iter()
        .filter(|(source, _)| source == "fs")
        .collect();
    
    let clipboard_events: Vec<_> = events_timeline.iter()
        .filter(|(source, _)| source == "clipboard")
        .collect();
    
    // Verify frequency patterns (fs should have tighter clustering)
    if fs_events.len() > 1 {
        let fs_intervals: Vec<_> = fs_events.windows(2)
            .map(|pair| pair[1].1 - pair[0].1)
            .collect();
        let avg_fs_interval = fs_intervals.iter().sum::<Duration>() / fs_intervals.len() as u32;
        
        if clipboard_events.len() > 1 {
            let clipboard_intervals: Vec<_> = clipboard_events.windows(2)
                .map(|pair| pair[1].1 - pair[0].1)
                .collect();
            let avg_clipboard_interval = clipboard_intervals.iter().sum::<Duration>() / clipboard_intervals.len() as u32;
            
            assert!(avg_fs_interval < avg_clipboard_interval, 
                "FS events should have shorter intervals than clipboard events");
        }
    }
    
    Ok(())
}

#[sinex_test]
async fn test_resource_contention_handling(ctx: TestContext) -> TestResult {
    // Test behavior under resource contention (database connections, memory, etc.)
    let source_count = 20; // More sources than typical connection pool
    let events_per_source = 10;
    let (tx, mut rx) = tokio::sync::mpsc::channel(source_count * events_per_source);
    
    let mut tasks = Vec::new();
    
    // Create many sources that will compete for database connections
    for i in 0..source_count {
        let source_name = format!("contention_source_{}", i);
        let mut source = MockEventSource::new(
            &source_name,
            events_per_source,
            Duration::from_millis(10),
            1024 // Larger payloads to increase contention
        );
        
        let tx_clone = tx.clone();
        let pool = ctx.pool().clone();
        
        let task = tokio::spawn(async move {
            let mut local_events = Vec::new();
            
            // Generate events
            for j in 0..events_per_source {
                let content = "x".repeat(1024);
                let event = sinex_core::RawEventBuilder::new(
                    &source_name,
                    "contention.event",
                    json!({
                        "sequence": j,
                        "source": source_name.clone(),
                        "content": content,
                        "timestamp": chrono::Utc::now().to_rfc3339()
                    })
                ).build();
                
                local_events.push(event.clone());
                
                if tx_clone.send(event).await.is_err() {
                    break;
                }
            }
            
            // Try to insert all events (this will cause database contention)
            let mut successful_inserts = 0;
            for event in local_events {
                match sinex_db::insert_event(&pool, &event).await {
                    Ok(()) => successful_inserts += 1,
                    Err(e) => {
                        println!("Insert failed for {}: {}", source_name, e);
                    }
                }
            }
            
            (source_name, successful_inserts)
        });
        
        tasks.push(task);
    }
    
    drop(tx);
    
    // Collect events from channel
    let mut channel_events = 0;
    while let Some(_event) = rx.recv().await {
        channel_events += 1;
    }
    
    // Wait for all insertion tasks
    let mut total_successful_inserts = 0;
    let mut successful_tasks = 0;
    
    for task in tasks {
        let (source_name, successful_inserts) = task.await.unwrap();
        total_successful_inserts += successful_inserts;
        successful_tasks += 1;
        println!("Source '{}' successfully inserted {} events", source_name, successful_inserts);
    }
    
    println!("Resource Contention Summary:");
    println!("- Channel events: {}", channel_events);
    println!("- Successful database inserts: {}", total_successful_inserts);
    println!("- Successful tasks: {}/{}", successful_tasks, source_count);
    
    // Verify that the system handled contention gracefully
    assert_eq!(successful_tasks, source_count, "All tasks should complete");
    assert_eq!(channel_events, source_count * events_per_source, "All events should pass through channel");
    
    // Database inserts might be less than channel events due to contention, but should be significant
    let insert_ratio = total_successful_inserts as f64 / channel_events as f64;
    assert!(insert_ratio > 0.5, "At least 50% of events should be successfully inserted despite contention");
    
    // Verify database consistency
    let stored_count = sinex_db::count_events(ctx.pool()).await?;
    assert_eq!(stored_count, total_successful_inserts, "Database count should match successful inserts");
    
    Ok(())
}

#[sinex_test] 
async fn test_backpressure_handling(ctx: TestContext) -> TestResult {
    // Test system behavior when event processing can't keep up
    let (tx, mut rx) = tokio::sync::mpsc::channel(10); // Small channel buffer
    
    // Fast producer
    let mut fast_source = MockEventSource::new("fast_producer", 200, Duration::ZERO, 128);
    
    // Slow consumer simulation
    let consumer_delay = Duration::from_millis(20);
    
    let producer_task = tokio::spawn(async move {
        let result = fast_source.stream_events(tx).await;
        (result, fast_source.events_sent())
    });
    
    // Slow consumer
    let consumer_task = tokio::spawn(async move {
        let mut consumed_events = 0;
        let start_time = Instant::now();
        
        while let Some(event) = rx.recv().await {
            consumed_events += 1;
            
            // Simulate slow processing
            sleep(consumer_delay).await;
            
            // Stop after reasonable time to avoid test timeout
            if start_time.elapsed() > Duration::from_secs(10) {
                break;
            }
        }
        
        consumed_events
    });
    
    // Wait for both tasks
    let (producer_result, events_sent) = producer_task.await.unwrap();
    let events_consumed = consumer_task.await.unwrap();
    
    println!("Backpressure Test Results:");
    println!("- Events sent by producer: {}", events_sent);
    println!("- Events consumed: {}", events_consumed);
    println!("- Producer result: {:?}", producer_result);
    
    // Under backpressure, producer might not send all events or might fail
    // This is expected behavior - verify system handles it gracefully
    assert!(events_consumed > 0, "Should consume some events");
    assert!(events_sent <= 200, "Producer should not send more than intended");
    
    // If producer failed, it should be due to channel closure (backpressure)
    if producer_result.is_err() {
        let error_msg = producer_result.unwrap_err().to_string();
        assert!(error_msg.contains("closed"), "Producer failure should be due to channel closure");
    }
    
    Ok(())
}

#[sinex_test]
async fn test_event_ordering_across_sources(ctx: TestContext) -> TestResult {
    // Test that event ordering is preserved within sources under concurrent load
    let (tx, mut rx) = tokio::sync::mpsc::channel(1000);
    
    let source_count = 4;
    let events_per_source = 25;
    
    let mut tasks = Vec::new();
    
    for i in 0..source_count {
        let source_name = format!("ordered_source_{}", i);
        let tx_clone = tx.clone();
        
        let task = tokio::spawn(async move {
            // Send events with explicit ordering
            for j in 0..events_per_source {
                let event = sinex_core::RawEventBuilder::new(
                    &source_name,
                    "ordered.event",
                    json!({
                        "sequence": j,
                        "source": source_name.clone(),
                        "timestamp": chrono::Utc::now().to_rfc3339(),
                        "order_test": true
                    })
                ).build();
                
                if tx_clone.send(event).await.is_err() {
                    break;
                }
                
                // Small delay to ensure ordering
                sleep(Duration::from_millis(1)).await;
            }
        });
        
        tasks.push(task);
    }
    
    drop(tx);
    
    // Collect events and track ordering per source
    let mut events_by_source: HashMap<String, Vec<(usize, sinex_ulid::Ulid)>> = HashMap::new();
    
    while let Some(event) = rx.recv().await {
        if let Some(sequence) = event.payload["sequence"].as_u64() {
            events_by_source
                .entry(event.source.clone())
                .or_insert_with(Vec::new)
                .push((sequence as usize, event.id));
        }
        
        sinex_db::insert_event(ctx.pool(), &event).await?;
    }
    
    // Wait for all producers to complete
    for task in tasks {
        task.await.unwrap();
    }
    
    // Verify ordering within each source
    for (source_name, mut events) in events_by_source {
        assert_eq!(events.len(), events_per_source, "Should have all events from source {}", source_name);
        
        // Sort by sequence number
        events.sort_by_key(|(seq, _)| *seq);
        
        // Verify sequence numbers are consecutive
        for (i, (seq, _)) in events.iter().enumerate() {
            assert_eq!(*seq, i, "Sequence numbers should be consecutive for source {}", source_name);
        }
        
        // Verify ULID ordering matches sequence ordering
        for i in 1..events.len() {
            let (_, prev_ulid) = events[i - 1];
            let (_, curr_ulid) = events[i];
            
            assert!(
                prev_ulid.timestamp() <= curr_ulid.timestamp(),
                "ULID timestamps should be ordered for source {}", source_name
            );
        }
        
        println!("Source '{}': ordering verified for {} events", source_name, events.len());
    }
    
    Ok(())
}

#[sinex_test]
async fn test_graceful_shutdown_with_multiple_sources(ctx: TestContext) -> TestResult {
    // Test graceful shutdown when multiple sources are running
    let (tx, mut rx) = tokio::sync::mpsc::channel(1000);
    
    let mut sources = Vec::new();
    let mut tasks = Vec::new();
    
    // Create long-running sources
    for i in 0..5 {
        let source_name = format!("long_running_{}", i);
        let source = MockEventSource::new(&source_name, 1000, Duration::from_millis(10), 256);
        sources.push(source);
    }
    
    // Start all sources
    for mut source in sources {
        let tx_clone = tx.clone();
        let task = tokio::spawn(async move {
            let result = timeout(Duration::from_secs(30), source.stream_events(tx_clone)).await;
            (source.source_name, result, source.events_sent())
        });
        tasks.push(task);
    }
    
    drop(tx);
    
    // Let sources run for a short time
    sleep(Duration::from_millis(500)).await;
    
    // Simulate shutdown by cancelling tasks
    for task in &tasks {
        task.abort();
    }
    
    // Collect any remaining events
    let mut total_events = 0;
    while let Ok(Some(event)) = timeout(Duration::from_millis(100), rx.recv()).await {
        total_events += 1;
        sinex_db::insert_event(ctx.pool(), &event).await?;
    }
    
    println!("Graceful shutdown collected {} events", total_events);
    
    // Verify database consistency after shutdown
    let stored_count = sinex_db::count_events(ctx.pool()).await?;
    assert_eq!(stored_count, total_events, "All collected events should be stored");
    
    // Verify some events were processed before shutdown
    assert!(total_events > 0, "Should have processed some events before shutdown");
    
    Ok(())
}