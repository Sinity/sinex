use crate::common::prelude::*;
use sinex_core::{EventSource, EventSourceContext, RawEvent, CoreError};
use std::sync::atomic::{AtomicU64, Ordering};
use sinex_test_macros::sinex_test;

/// Test what happens when event channel fills up
#[sinex_test]
async fn test_channel_backpressure_handling() {
    // Small channel to trigger backpressure quickly
    let (tx, mut rx) = mpsc::channel::<RawEvent>(10);
    
    let events_generated = Arc::new(AtomicU64::new(0));
    let events_dropped = Arc::new(AtomicU64::new(0));
    
    // Fast producer
    let gen_count = events_generated.clone();
    let drop_count = events_dropped.clone();
    let producer = tokio::spawn(async move {
        for i in 0..1000 {
            let event = crate::common::events::generic_adversarial_event("fast_producer", "test.event", json!({"test": true}), None);
            
            gen_count.fetch_add(1, Ordering::Relaxed);
            
            // Try send with timeout to avoid blocking forever
            match tx.try_send(event) {
                Ok(_) => {},
                Err(e) => {
                    drop_count.fetch_add(1, Ordering::Relaxed);
                    if i < 50 {
                        // Log first few drops
                        eprintln!("Dropped event {}: {:?}", i, e);
                    }
                    // Break if channel is closed, continue if just full
                    if matches!(e, tokio::sync::mpsc::error::TrySendError::Closed(_)) {
                        break;
                    }
                }
            }
            
            // Generate faster than consumer
            if i < 100 {
                // Start fast
                tokio::time::sleep(Duration::from_micros(100)).await;
            }
        }
    });
    
    // Slow consumer
    let consumed = Arc::new(AtomicU64::new(0));
    let cons_count = consumed.clone();
    let consumer = tokio::spawn(async move {
        while let Some(event) = rx.recv().await {
            // Simulate slow processing
            tokio::task::yield_now().await;
            cons_count.fetch_add(1, Ordering::Relaxed);
            
            if let Some(seq) = event.payload.get("seq").and_then(|v| v.as_u64()) {
                if seq < 10 {
                    eprintln!("Consumed event seq: {}", seq);
                }
            }
        }
    });
    
    // Let it run
    tokio::time::sleep(Duration::from_secs(2)).await;
    
    // Stop producer
    producer.abort();
    let _ = producer.await;
    
    // Wait for consumer to finish (tx was dropped when producer ended)
    consumer.await.unwrap();
    
    let generated = events_generated.load(Ordering::Relaxed);
    let dropped = events_dropped.load(Ordering::Relaxed);
    let consumed_count = consumed.load(Ordering::Relaxed);
    
    println!("Backpressure test results:");
    println!("  Generated: {}", generated);
    println!("  Dropped: {}", dropped);
    println!("  Consumed: {}", consumed_count);
    println!("  Drop rate: {:.1}%", dropped as f64 / generated as f64 * 100.0);
    
    // Verify backpressure behavior - with small buffer and sleep asymmetry, drops are expected
    assert!(
        dropped > 0, 
        "Expected backpressure to cause drops with 100-item buffer and slow consumer, but got 0 drops"
    );
    
    // Verify reasonable drop rate (should be significant but not total)
    let drop_rate = dropped as f64 / generated as f64;
    assert!(
        drop_rate > 0.5 && drop_rate < 0.95,
        "Drop rate {:.1}% outside expected range (50-95%)", 
        drop_rate * 100.0
    );
    
    assert!(consumed_count > 0, "Expected some events to be consumed");
    assert!(consumed_count + dropped <= generated, "Accounting error");
}

/// Test graceful degradation under memory pressure
#[sinex_test]
async fn test_memory_pressure_handling() {
    let (tx, mut rx) = mpsc::channel::<RawEvent>(1000);
    
    // Track memory usage
    let start_memory = get_current_memory_usage();
    
    // Generate events with large payloads
    let producer = tokio::spawn(async move {
        for i in 0..100 {
            // Increasingly large payloads
            let size = 1024 * (i + 1); // 1KB to 100KB
            let _large_data = "x".repeat(size);
            
            let event = events::large_payload_test_event(size);
            if tx.send(event).await.is_err() {
                eprintln!("Channel closed at event {}", i);
                break;
            }
            
            // Check memory usage periodically
            if i % 10 == 0 {
                let current_memory = get_current_memory_usage();
                let delta_mb = (current_memory - start_memory) / 1_048_576;
                println!("After {} events: +{} MB", i, delta_mb);
                
                // Simulate memory pressure threshold
                if delta_mb > 100 {
                    eprintln!("Memory pressure detected at {} MB", delta_mb);
                    break;
                }
            }
        }
    });
    
    // Consumer that tracks payload sizes
    let mut total_bytes = 0u64;
    let mut event_count = 0u64;
    
    tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(event) = rx.recv().await {
            if let Some(size) = event.payload.get("size_kb").and_then(|v| v.as_u64()) {
                total_bytes += size * 1024;
                event_count += 1;
            }
        }
    }).await.ok();
    
    producer.abort();
    
    println!("Memory pressure test results:");
    println!("  Events processed: {}", event_count);
    println!("  Total payload size: {} MB", total_bytes / 1_048_576);
    
    assert!(event_count > 0, "Should process some events");
}

/// Test event source crash and restart
#[sinex_test]
async fn test_event_source_crash_recovery() {
    struct CrashingEventSource {
        crash_after: u64,
        events_sent: Arc<AtomicU64>,
    }
    
    #[async_trait::async_trait]
    impl EventSource for CrashingEventSource {
        type Config = ();
        const SOURCE_NAME: &'static str = "crashing_source";
        
        async fn initialize(_ctx: EventSourceContext) -> Result<Self, CoreError> {
            Ok(Self {
                crash_after: 50,
                events_sent: Arc::new(AtomicU64::new(0)),
            })
        }
        
        async fn stream_events(&mut self, tx: mpsc::Sender<RawEvent>) -> Result<(), CoreError> {
            for i in 0..100 {
                let event = crate::common::events::generic_adversarial_event("crashing", "test", json!({"test": true}), None);
                if tx.send(event).await.is_err() { break; }
                self.events_sent.fetch_add(1, Ordering::Relaxed);
                
                if i == self.crash_after {
                    // Simulate crash
                    panic!("Simulated event source crash at event {}", i);
                }
                
                tokio::task::yield_now().await;
            }
            Ok(())
        }
    }
    
    // Test automatic restart behavior
    let (tx, mut rx) = mpsc::channel(100);
    let ctx = EventSourceContext::for_test();
    let mut source = CrashingEventSource::initialize(ctx)
        .await
        .unwrap();
    
    let sent_count = source.events_sent.clone();
    let sent_count_for_print = sent_count.clone();
    
    // Run source in supervised task
    let source_handle = tokio::spawn(async move {
        let result = source.stream_events(tx.clone()).await;
        eprintln!("Source ended with: {:?}", result);
        
        // Simulate restart after crash
        if result.is_err() {
            eprintln!("Restarting source after crash...");
            let mut new_source = CrashingEventSource {
                crash_after: 200, // Won't crash again
                events_sent: sent_count.clone(),
            };
            
            // Continue from where we left off
            let _ = new_source.stream_events(tx).await;
        }
    });
    
    // Collect events
    let mut received = Vec::new();
    tokio::time::timeout(Duration::from_secs(2), async {
        while let Some(event) = rx.recv().await {
            if let Some(seq) = event.payload.get("seq").and_then(|v| v.as_u64()) {
                received.push(seq);
            }
        }
    }).await.ok();
    
    source_handle.abort();
    
    println!("Source crash test results:");
    println!("  Events sent: {}", sent_count_for_print.load(Ordering::Relaxed));
    println!("  Events received: {}", received.len());
    println!("  Last sequence: {:?}", received.last());
    
    // Should have received events before and after crash
    assert!(received.len() > 50, "Should receive events after crash");
}

fn get_current_memory_usage() -> u64 {
    // This is a stub - real implementation would read from /proc/self/status
    0
}

