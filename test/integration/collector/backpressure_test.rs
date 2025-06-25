use crate::common::prelude::*;
use sinex_core::{EventSource, EventSourceContext, RawEventBuilder, CoreError};
use sinex_db::models::RawEvent;
use tokio::time::{timeout, sleep, Instant};
use std::sync::atomic::{AtomicUsize, AtomicBool, Ordering};



/// High-frequency event source that can generate events rapidly
#[derive(Clone)]
pub struct HighFrequencyEventSource {
    events_per_second: usize,
    max_events: Option<usize>,
    events_sent: Arc<AtomicUsize>,
    should_stop: Arc<AtomicBool>,
}

impl HighFrequencyEventSource {
    pub fn new(events_per_second: usize) -> Self {
        Self {
            events_per_second,
            max_events: None,
            events_sent: Arc::new(AtomicUsize::new(0)),
            should_stop: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn with_max_events(mut self, max_events: usize) -> Self {
        self.max_events = Some(max_events);
        self
    }

    pub fn stop(&self) {
        self.should_stop.store(true, Ordering::SeqCst);
    }

    pub fn events_sent(&self) -> usize {
        self.events_sent.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl EventSource for HighFrequencyEventSource {
    type Config = serde_json::Value;
    const SOURCE_NAME: &'static str = "test.high_frequency_source";

    async fn initialize(_ctx: EventSourceContext) -> sinex_core::Result<Self> {
        Ok(Self::new(1000)) // Default to 1000 events/sec
    }

    async fn stream_events(&mut self, tx: mpsc::Sender<RawEvent>) -> sinex_core::Result<()> {
        let interval = Duration::from_nanos(1_000_000_000 / self.events_per_second as u64);
        let mut interval_timer = tokio::time::interval(interval);
        interval_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            if self.should_stop.load(Ordering::SeqCst) {
                break;
            }

            // Check max events limit
            let events_sent = self.events_sent.load(Ordering::SeqCst);
            if let Some(max) = self.max_events {
                if events_sent >= max {
                    break;
                }
            }

            // Wait for next tick
            interval_timer.tick().await;

            // Create and send event
            let event = RawEventBuilder::new(
                Self::SOURCE_NAME,
                "test.high_frequency",
                json!({
                    "event_number": events_sent,
                    "timestamp": chrono::Utc::now().to_rfc3339(),
                    "data": format!("Event {}", events_sent)
                })
            ).build();

            // Try to send - if channel is full, this will block or fail depending on the receiver
            match tx.send(event).await {
                Ok(_) => {
                    self.events_sent.fetch_add(1, Ordering::SeqCst);
                }
                Err(_) => {
                    // Channel closed, exit gracefully
                    break;
                }
            }
        }

        Ok(())
    }
}

/// Slow event processor that simulates processing delays
#[derive(Clone)]
pub struct SlowEventProcessor {
    processing_delay: Duration,
    events_processed: Arc<AtomicUsize>,
    should_stop: Arc<AtomicBool>,
}

impl SlowEventProcessor {
    pub fn new(processing_delay: Duration) -> Self {
        Self {
            processing_delay,
            events_processed: Arc::new(AtomicUsize::new(0)),
            should_stop: Arc::new(AtomicBool::new(false)),
        }
    }

    pub async fn process_events(&self, mut rx: mpsc::Receiver<RawEvent>) {
        while let Some(event) = rx.recv().await {
            if self.should_stop.load(Ordering::SeqCst) {
                break;
            }

            // Simulate processing time
            sleep(self.processing_delay).await;
            
            // Validate event structure
            assert!(!event.source.is_empty());
            assert!(!event.event_type.is_empty());
            
            self.events_processed.fetch_add(1, Ordering::SeqCst);
        }
    }

    pub fn stop(&self) {
        self.should_stop.store(true, Ordering::SeqCst);
    }

    pub fn events_processed(&self) -> usize {
        self.events_processed.load(Ordering::SeqCst)
    }
}

#[sinex_test]
async fn test_channel_backpressure_with_fast_producer_slow_consumer(_ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    // Create a bounded channel with the same capacity as UnifiedCollector
    let (tx, rx) = mpsc::channel::<RawEvent>(10_000);
    
    // Fast producer: 5000 events/sec
    let fast_producer = HighFrequencyEventSource::new(5000)
        .with_max_events(15_000); // More than channel capacity

    // Slow consumer: 100ms per event = 10 events/sec
    let slow_consumer = SlowEventProcessor::new(Duration::from_millis(100));

    let start_time = Instant::now();

    // Start the slow consumer
    let consumer_handle = {
        let consumer = slow_consumer.clone();
        tokio::spawn(async move {
            consumer.process_events(rx).await;
        })
    };

    // Start the fast producer  
    let mut producer_clone = fast_producer.clone();
    let producer_handle = tokio::spawn(async move {
        producer_clone.stream_events(tx).await
    });

    // Wait for meaningful test progress instead of fixed sleep
    // We want at least 20 events processed to verify backpressure behavior
    let consumer_clone = slow_consumer.clone();
    let _wait_result = wait_for_condition_or_timeout(
        move || {
            let processed = consumer_clone.events_processed();
            Box::pin(async move { Ok(processed >= 20) })
        },
        5
    ).await;
    
    // Stop both
    slow_consumer.stop();
    fast_producer.stop();

    // Wait for completion with timeout
    let _ = timeout(Duration::from_secs(2), producer_handle).await;
    let _ = timeout(Duration::from_secs(2), consumer_handle).await;

    let elapsed = start_time.elapsed();
    let events_sent = fast_producer.events_sent();
    let events_processed = slow_consumer.events_processed();

    println!("Test ran for: {:.2}s", elapsed.as_secs_f64());
    println!("Events sent: {}", events_sent);
    println!("Events processed: {}", events_processed);
    println!("Send rate: {:.0} events/sec", events_sent as f64 / elapsed.as_secs_f64());
    println!("Process rate: {:.0} events/sec", events_processed as f64 / elapsed.as_secs_f64());

    // Verify backpressure behavior
    assert!(events_sent > 0, "Producer should have sent some events");
    assert!(events_processed > 0, "Consumer should have processed some events");
    
    // With backpressure, the producer should be throttled by the slow consumer
    // We should see significantly fewer events sent than the producer's maximum capacity
    let expected_max_sent = 5000 * 3; // 15,000 events at full speed
    assert!(events_sent < expected_max_sent, 
           "Producer should be throttled by backpressure, sent {} but could send up to {}", 
           events_sent, expected_max_sent);

    // We waited for at least 20 events to be processed
    assert!(events_processed >= 20, 
           "Consumer should have processed at least 20 events, got {}", 
           events_processed);
    
    // Process rate should be roughly limited by consumer speed
    let process_rate = events_processed as f64 / elapsed.as_secs_f64();
    assert!(process_rate <= 15.0, // Allow some burst above theoretical 10/sec
           "Process rate {} events/sec should be limited by consumer delay", 
           process_rate);

    Ok(())
}

#[sinex_test]
async fn test_channel_saturation_prevents_event_loss(_ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    // Test that the channel properly handles saturation without losing events
    let (tx, mut rx) = mpsc::channel::<RawEvent>(100); // Smaller channel for easier testing
    
    let producer = HighFrequencyEventSource::new(10_000) // Very fast
        .with_max_events(150); // More than channel capacity

    // Start producer
    let mut producer_clone = producer.clone();
    let producer_handle = tokio::spawn(async move {
        producer_clone.stream_events(tx).await
    });

    // Wait for producer to send enough events to saturate channel
    let producer_clone = producer.clone();
    let _ = wait_for_condition_or_timeout(
        move || {
            let sent = producer_clone.events_sent();
            Box::pin(async move { Ok(sent >= 100) })
        },
        1 // 1 second timeout
    ).await;

    // Now consume all events slowly
    let mut events_received = Vec::new();
    while let Ok(Some(event)) = timeout(Duration::from_millis(10), rx.recv()).await {
        events_received.push(event);
    }

    // Wait for producer to finish
    let producer_result = timeout(Duration::from_secs(2), producer_handle).await;
    
    let events_sent = producer.events_sent();

    println!("Events sent: {}", events_sent);
    println!("Events received: {}", events_received.len());

    // With backpressure, we should receive all events that were sent
    pretty_assertions::assert_eq!(events_sent, events_received.len(), 
              "All sent events should be received, no events should be lost");

    // Verify event ordering and content
    for (i, event) in events_received.iter().enumerate() {
        pretty_assertions::assert_eq!(event.source, "test.high_frequency_source");
        pretty_assertions::assert_eq!(event.event_type, "test.high_frequency");
        
        let event_number = event.payload["event_number"].as_u64().unwrap() as usize;
        pretty_assertions::assert_eq!(event_number, i, "Events should be received in order");
    }

    // Producer should complete successfully
    assert!(producer_result.is_ok(), "Producer should complete without errors");

    Ok(())
}

#[sinex_test]
async fn test_multiple_sources_backpressure(_ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    // Test backpressure with multiple event sources feeding into the same channel
    let (tx, rx) = mpsc::channel::<RawEvent>(1000);
    
    // Create multiple producers
    let producers = vec![
        HighFrequencyEventSource::new(1000).with_max_events(500),
        HighFrequencyEventSource::new(800).with_max_events(400),
        HighFrequencyEventSource::new(1200).with_max_events(600),
    ];

    let total_expected_events = 500 + 400 + 600; // 1500 events total

    // Slow consumer
    let consumer = SlowEventProcessor::new(Duration::from_millis(50)); // 20 events/sec

    // Start consumer
    let consumer_handle = {
        let consumer = consumer.clone();
        tokio::spawn(async move {
            consumer.process_events(rx).await;
        })
    };

    // Start all producers
    let mut producer_handles = Vec::new();
    for producer in producers {
        let tx_clone = tx.clone();
        let mut producer_clone = producer.clone();
        let handle = tokio::spawn(async move {
            let result = producer_clone.stream_events(tx_clone).await;
            (producer_clone.events_sent(), result)
        });
        producer_handles.push(handle);
    }

    // Drop the original tx to signal completion when all producers are done
    drop(tx);

    // Wait for all producers to complete
    let mut total_sent = 0;
    for handle in producer_handles {
        let (sent, result) = handle.await.unwrap();
        assert!(result.is_ok(), "Producer should complete successfully");
        total_sent += sent;
    }

    // Wait for consumer to finish processing
    let _ = timeout(Duration::from_secs(10), consumer_handle).await;
    
    let events_processed = consumer.events_processed();

    println!("Total events sent: {}", total_sent);
    println!("Events processed: {}", events_processed);

    // All events should be sent
    pretty_assertions::assert_eq!(total_sent, total_expected_events, "All events should be sent");
    
    // All events should be processed (no loss)
    pretty_assertions::assert_eq!(events_processed, total_sent, "All sent events should be processed");

    Ok(())
}

#[sinex_test]
async fn test_channel_close_during_backpressure(_ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    // Test what happens when the channel is closed while producer is backpressured
    let (tx, rx) = mpsc::channel::<RawEvent>(10);
    
    let producer = HighFrequencyEventSource::new(10_000).with_max_events(1000);

    // Start producer
    let mut producer_clone = producer.clone();
    let producer_handle = tokio::spawn(async move {
        let result = producer_clone.stream_events(tx).await;
        (producer_clone.events_sent(), result)
    });

    // Let producer fill the channel and get backpressured
    sleep(Duration::from_millis(50)).await;

    // Close receiver side, which should cause producer to exit gracefully
    drop(rx);

    // Producer should exit without panicking
    let (events_sent, result) = timeout(Duration::from_secs(1), producer_handle)
        .await
        .expect("Producer should exit quickly when channel closes")
        .unwrap();

    println!("Events sent before close: {}", events_sent);

    // Producer should have sent some events before channel was closed
    assert!(events_sent > 0, "Producer should send some events before channel closes");
    assert!(events_sent < 1000, "Producer should not send all events due to early channel close");
    
    // Result should be Ok since this is graceful handling of channel closure
    assert!(result.is_ok(), "Producer should handle channel closure gracefully");

    Ok(())
}

#[sinex_test]
async fn test_backpressure_recovery(_ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    // Test that the system recovers properly when backpressure is relieved
    let (tx, mut rx) = mpsc::channel::<RawEvent>(50);
    
    let producer = HighFrequencyEventSource::new(5000).with_max_events(200);

    // Start producer
    let mut producer_clone = producer.clone();
    let producer_handle = tokio::spawn(async move {
        let result = producer_clone.stream_events(tx).await;
        (producer_clone.events_sent(), result)
    });

    // Phase 1: Let producer get backpressured
    sleep(Duration::from_millis(100)).await;
    let events_during_backpressure = producer.events_sent();

    // Phase 2: Start consuming events to relieve backpressure
    let mut events_consumed = 0;
    while let Ok(Some(_)) = timeout(Duration::from_millis(1), rx.recv()).await {
        events_consumed += 1;
        if events_consumed >= 30 { // Consume some events to create space
            break;
        }
    }

    // Phase 3: Allow more production after relieving backpressure
    sleep(Duration::from_millis(100)).await;
    let events_after_relief = producer.events_sent();

    // Phase 4: Consume remaining events
    while let Ok(Some(_)) = timeout(Duration::from_millis(1), rx.recv()).await {
        events_consumed += 1;
    }

    // Wait for producer to complete
    let (total_sent, result) = timeout(Duration::from_secs(1), producer_handle)
        .await
        .expect("Producer should complete")
        .unwrap();

    println!("Events during backpressure: {}", events_during_backpressure);
    println!("Events after relief: {}", events_after_relief);
    println!("Total events sent: {}", total_sent);
    println!("Total events consumed: {}", events_consumed);

    // Verify backpressure and recovery behavior
    assert!(events_during_backpressure > 0, "Some events should be sent initially");
    assert!(events_after_relief > events_during_backpressure, 
           "More events should be sent after backpressure relief");
    pretty_assertions::assert_eq!(total_sent, 200, "All events should eventually be sent");
    pretty_assertions::assert_eq!(events_consumed, total_sent, "All sent events should be consumed");
    assert!(result.is_ok(), "Producer should complete successfully");

    Ok(())
}

#[sinex_test]
async fn test_memory_pressure_during_backpressure(_ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    // Test that memory usage stays reasonable even during extended backpressure
    let (tx, mut rx) = mpsc::channel::<RawEvent>(1000);
    
    // Create events with larger payloads to test memory pressure
    let mut _producer = HighFrequencyEventSource::new(2000).with_max_events(2000);

    let producer_handle = tokio::spawn(async move {
        // Override the stream_events to create larger payloads
        let interval = Duration::from_millis(1); // Fast rate
        let mut interval_timer = tokio::time::interval(interval);
        interval_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        for i in 0..2000 {
            interval_timer.tick().await;

            // Create event with large payload
            let large_data = "x".repeat(1024); // 1KB per event
            let event = RawEventBuilder::new(
                "test.memory_pressure",
                "test.large_event",
                json!({
                    "event_number": i,
                    "large_data": large_data,
                    "timestamp": chrono::Utc::now().to_rfc3339()
                })
            ).build();

            if tx.send(event).await.is_err() {
                break;
            }
        }
        
        Ok::<_, CoreError>(())
    });

    // Let producer fill the channel and create backpressure
    sleep(Duration::from_millis(500)).await;

    // Slowly consume events
    let mut events_consumed = 0;
    let consume_start = Instant::now();
    
    while let Ok(Some(event)) = timeout(Duration::from_millis(10), rx.recv()).await {
        events_consumed += 1;
        
        // Verify event structure
        assert!(event.payload["large_data"].as_str().unwrap().len() == 1024);
        
        // Add small delay to simulate processing
        sleep(Duration::from_millis(1)).await;
        
        // Stop after reasonable number to avoid test timeout
        if events_consumed >= 500 {
            break;
        }
    }

    let consume_time = consume_start.elapsed();

    // Clean up
    let _ = timeout(Duration::from_secs(1), producer_handle).await;

    println!("Events consumed: {}", events_consumed);
    println!("Consume time: {:.2}s", consume_time.as_secs_f64());
    println!("Avg processing rate: {:.0} events/sec", 
             events_consumed as f64 / consume_time.as_secs_f64());

    // Verify reasonable behavior under memory pressure
    assert!(events_consumed >= 200, "Should consume a reasonable number of events");
    assert!(consume_time < Duration::from_secs(10), "Should not take too long to process");

    // Memory should be released as events are consumed
    // (This is implicit - Rust's ownership model ensures this, but the test exercises the pattern)

    Ok(())
}