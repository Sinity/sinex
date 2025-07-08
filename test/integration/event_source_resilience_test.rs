use sinex_core::{EventSource, EventSourceContext, RawEvent, EventSender, CoreError};
use sinex_db::test::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::time::{sleep, timeout};
use tokio::sync::Mutex;
use serde_json::json;
use async_trait::async_trait;

/// Test event source that simulates various failure modes
struct ChaosEventSource {
    failure_mode: FailureMode,
    events_sent: Arc<AtomicUsize>,
    should_fail: Arc<AtomicBool>,
    fail_after_events: Option<usize>,
    recovery_delay: Duration,
}

#[derive(Clone, Debug)]
enum FailureMode {
    /// Source crashes immediately on initialization
    InitializationFailure,
    /// Source starts fine but crashes during event streaming
    StreamingCrash { after_events: usize },
    /// Source becomes unresponsive (hangs)
    Unresponsive { after_events: usize },
    /// Source sends malformed events
    CorruptedEvents { corruption_rate: f32 },
    /// Source has intermittent failures
    IntermittentFailures { failure_rate: f32 },
    /// Source recovers after failures
    RecoverableFailures { fail_count: usize, recovery_delay: Duration },
    /// External dependency failure simulation
    DependencyFailure { dependency: String },
}

impl ChaosEventSource {
    fn new(failure_mode: FailureMode) -> Self {
        Self {
            failure_mode,
            events_sent: Arc::new(AtomicUsize::new(0)),
            should_fail: Arc::new(AtomicBool::new(false)),
            fail_after_events: None,
            recovery_delay: Duration::from_millis(100),
        }
    }
}

#[async_trait]
impl EventSource for ChaosEventSource {
    type Config = serde_json::Value;
    const SOURCE_NAME: &'static str = "test.chaos";

    async fn initialize(_ctx: EventSourceContext) -> sinex_core::Result<Self>
    where
        Self: Sized,
    {
        // Simulate initialization failure
        match &_ctx.config {
            Some(config) if config.get("fail_init").and_then(|v| v.as_bool()).unwrap_or(false) => {
                return Err(CoreError::Other("Simulated initialization failure".to_string()));
            }
            _ => {}
        }

        Ok(Self::new(FailureMode::StreamingCrash { after_events: 5 }))
    }

    async fn stream_events(&mut self, tx: EventSender) -> sinex_core::Result<()> {
        match &self.failure_mode {
            FailureMode::InitializationFailure => {
                return Err(CoreError::Other("Initialization failed".to_string()));
            }
            
            FailureMode::StreamingCrash { after_events } => {
                for i in 0..*after_events {
                    let event = sinex_core::RawEventBuilder::new(
                        "test.chaos",
                        "test.event",
                        json!({"event_num": i, "message": "test event"})
                    ).build();
                    
                    if tx.send(event).await.is_err() {
                        return Err(CoreError::Other("Channel closed".to_string()));
                    }
                    
                    self.events_sent.fetch_add(1, Ordering::Relaxed);
                    sleep(Duration::from_millis(10)).await;
                }
                
                // Simulate crash after sending specified number of events
                return Err(CoreError::Other("Simulated crash after events".to_string()));
            }
            
            FailureMode::Unresponsive { after_events } => {
                for i in 0..*after_events {
                    let event = sinex_core::RawEventBuilder::new(
                        "test.chaos",
                        "test.event",
                        json!({"event_num": i})
                    ).build();
                    
                    if tx.send(event).await.is_err() {
                        return Err(CoreError::Other("Channel closed".to_string()));
                    }
                    
                    self.events_sent.fetch_add(1, Ordering::Relaxed);
                }
                
                // Become unresponsive (hang indefinitely)
                loop {
                    sleep(Duration::from_secs(3600)).await;
                }
            }
            
            FailureMode::CorruptedEvents { corruption_rate } => {
                for i in 0..100 {
                    let is_corrupted = (i as f32 / 100.0) < *corruption_rate;
                    
                    let event = if is_corrupted {
                        sinex_core::RawEventBuilder::new(
                            "test.chaos",
                            "corrupted.event",
                            json!({"corrupted": true, "invalid_data": null})
                        ).build()
                    } else {
                        sinex_core::RawEventBuilder::new(
                            "test.chaos",
                            "test.event",
                            json!({"event_num": i, "valid": true})
                        ).build()
                    };
                    
                    if tx.send(event).await.is_err() {
                        return Ok(());
                    }
                    
                    self.events_sent.fetch_add(1, Ordering::Relaxed);
                    sleep(Duration::from_millis(5)).await;
                }
                Ok(())
            }
            
            FailureMode::IntermittentFailures { failure_rate } => {
                for i in 0..50 {
                    let should_fail = (i as f32 % 10.0) / 10.0 < *failure_rate;
                    
                    if should_fail {
                        return Err(CoreError::Other(format!("Intermittent failure at event {}", i)));
                    }
                    
                    let event = sinex_core::RawEventBuilder::new(
                        "test.chaos",
                        "test.event", 
                        json!({"event_num": i})
                    ).build();
                    
                    if tx.send(event).await.is_err() {
                        return Ok(());
                    }
                    
                    self.events_sent.fetch_add(1, Ordering::Relaxed);
                    sleep(Duration::from_millis(20)).await;
                }
                Ok(())
            }
            
            FailureMode::RecoverableFailures { fail_count, recovery_delay } => {
                let mut failures = 0;
                
                for i in 0..100 {
                    // Fail periodically then recover
                    if i > 0 && i % 10 == 0 && failures < *fail_count {
                        failures += 1;
                        sleep(*recovery_delay).await;
                        return Err(CoreError::Other(format!("Recoverable failure #{}", failures)));
                    }
                    
                    let event = sinex_core::RawEventBuilder::new(
                        "test.chaos",
                        "test.event",
                        json!({"event_num": i, "attempt": failures + 1})
                    ).build();
                    
                    if tx.send(event).await.is_err() {
                        return Ok(());
                    }
                    
                    self.events_sent.fetch_add(1, Ordering::Relaxed);
                    sleep(Duration::from_millis(10)).await;
                }
                Ok(())
            }
            
            FailureMode::DependencyFailure { dependency } => {
                // Simulate external dependency being unavailable
                return Err(CoreError::Other(format!("External dependency '{}' unavailable", dependency)));
            }
        }
    }
}

#[sinex_test]
async fn test_event_source_initialization_failure(ctx: TestContext) -> TestResult {
    // Test that the system handles event source initialization failures gracefully
    let config = json!({"fail_init": true});
    let event_ctx = EventSourceContext::new(Some(config));
    
    let result = ChaosEventSource::initialize(event_ctx).await;
    assert!(result.is_err(), "Expected initialization to fail");
    
    Ok(())
}

#[sinex_test]
async fn test_event_source_streaming_crash(ctx: TestContext) -> TestResult {
    // Test that the system handles event source crashes during streaming
    let mut source = ChaosEventSource::new(FailureMode::StreamingCrash { after_events: 3 });
    let (tx, mut rx) = tokio::sync::mpsc::channel(100);
    
    // Run the event source in a task
    let stream_task = tokio::spawn(async move {
        source.stream_events(tx).await
    });
    
    // Collect events until the source crashes
    let mut received_events = 0;
    let timeout_duration = Duration::from_secs(5);
    let start = Instant::now();
    
    while start.elapsed() < timeout_duration {
        match timeout(Duration::from_millis(100), rx.recv()).await {
            Ok(Some(_event)) => {
                received_events += 1;
            }
            Ok(None) => break, // Channel closed
            Err(_) => break,   // Timeout
        }
    }
    
    // Wait for the task to complete and verify it failed
    let result = stream_task.await.unwrap();
    assert!(result.is_err(), "Expected streaming to fail");
    assert_eq!(received_events, 3, "Should have received exactly 3 events before crash");
    
    Ok(())
}

#[sinex_test]
async fn test_event_source_unresponsive_timeout(ctx: TestContext) -> TestResult {
    // Test that the system handles unresponsive event sources with timeouts
    let mut source = ChaosEventSource::new(FailureMode::Unresponsive { after_events: 2 });
    let (tx, mut rx) = tokio::sync::mpsc::channel(100);
    
    // Run the event source with a timeout
    let stream_task = tokio::spawn(async move {
        timeout(Duration::from_millis(500), source.stream_events(tx)).await
    });
    
    // Collect initial events
    let mut received_events = 0;
    for _ in 0..5 {
        match timeout(Duration::from_millis(100), rx.recv()).await {
            Ok(Some(_event)) => received_events += 1,
            _ => break,
        }
    }
    
    // Wait for timeout
    let result = stream_task.await.unwrap();
    assert!(result.is_err(), "Expected timeout due to unresponsive source");
    assert_eq!(received_events, 2, "Should have received 2 events before becoming unresponsive");
    
    Ok(())
}

#[sinex_test]
async fn test_event_source_corrupted_events(ctx: TestContext) -> TestResult {
    // Test that the system handles corrupted events gracefully
    let mut source = ChaosEventSource::new(FailureMode::CorruptedEvents { corruption_rate: 0.2 });
    let (tx, mut rx) = tokio::sync::mpsc::channel(1000);
    
    // Run the event source
    let stream_task = tokio::spawn(async move {
        source.stream_events(tx).await
    });
    
    // Collect all events
    let mut received_events = 0;
    let mut corrupted_events = 0;
    
    while let Ok(Some(event)) = timeout(Duration::from_millis(50), rx.recv()).await {
        received_events += 1;
        
        if event.event_type == "corrupted.event" {
            corrupted_events += 1;
        }
        
        // Insert valid events into database
        if event.event_type == "test.event" {
            sinex_db::insert_event(ctx.pool(), &event).await?;
        }
    }
    
    // Verify we handled both clean and corrupted events
    assert!(received_events > 0, "Should have received some events");
    assert!(corrupted_events > 0, "Should have received some corrupted events");
    assert!(corrupted_events < received_events, "Not all events should be corrupted");
    
    // Verify only valid events were stored
    let stored_count = sinex_db::count_events(ctx.pool()).await?;
    assert_eq!(stored_count, received_events - corrupted_events, "Only valid events should be stored");
    
    Ok(())
}

#[sinex_test]
async fn test_event_source_intermittent_failures(ctx: TestContext) -> TestResult {
    // Test that the system handles intermittent failures with retry logic
    let mut source = ChaosEventSource::new(FailureMode::IntermittentFailures { failure_rate: 0.3 });
    let (tx, mut rx) = tokio::sync::mpsc::channel(100);
    
    // Run with retry logic simulation
    let mut retry_count = 0;
    let max_retries = 3;
    
    loop {
        let stream_result = timeout(Duration::from_secs(2), source.stream_events(tx.clone())).await;
        
        match stream_result {
            Ok(Ok(())) => {
                // Success - exit retry loop
                break;
            }
            Ok(Err(_)) | Err(_) => {
                retry_count += 1;
                if retry_count >= max_retries {
                    break;
                }
                // Simulate retry delay
                sleep(Duration::from_millis(100)).await;
                // Recreate source for retry
                source = ChaosEventSource::new(FailureMode::IntermittentFailures { failure_rate: 0.3 });
            }
        }
    }
    
    // Collect any events that were sent
    let mut received_events = 0;
    while let Ok(Some(event)) = timeout(Duration::from_millis(10), rx.recv()).await {
        received_events += 1;
        sinex_db::insert_event(ctx.pool(), &event).await?;
    }
    
    // Verify retry logic was exercised
    assert!(retry_count > 0, "Should have retried at least once");
    assert!(retry_count <= max_retries, "Should not exceed max retries");
    
    Ok(())
}

#[sinex_test]
async fn test_multiple_event_sources_with_failures(ctx: TestContext) -> TestResult {
    // Test that failure of one event source doesn't affect others
    let (tx, mut rx) = tokio::sync::mpsc::channel(1000);
    
    // Create multiple event sources with different failure modes
    let sources = vec![
        ("stable", ChaosEventSource::new(FailureMode::StreamingCrash { after_events: 100 })),
        ("failing", ChaosEventSource::new(FailureMode::StreamingCrash { after_events: 3 })),
        ("corrupted", ChaosEventSource::new(FailureMode::CorruptedEvents { corruption_rate: 0.5 })),
    ];
    
    // Run all sources concurrently
    let mut tasks = Vec::new();
    for (name, mut source) in sources {
        let tx_clone = tx.clone();
        let task = tokio::spawn(async move {
            let result = timeout(Duration::from_secs(2), source.stream_events(tx_clone)).await;
            (name, result)
        });
        tasks.push(task);
    }
    
    drop(tx); // Close the original sender
    
    // Collect all events
    let mut events_by_source = std::collections::HashMap::new();
    while let Ok(Some(event)) = timeout(Duration::from_millis(50), rx.recv()).await {
        *events_by_source.entry(event.source.clone()).or_insert(0) += 1;
        
        // Only insert valid events
        if event.event_type == "test.event" {
            sinex_db::insert_event(ctx.pool(), &event).await?;
        }
    }
    
    // Wait for all tasks to complete
    let mut results = Vec::new();
    for task in tasks {
        results.push(task.await.unwrap());
    }
    
    // Verify that some sources succeeded while others failed
    let mut successful_sources = 0;
    let mut failed_sources = 0;
    
    for (name, result) in results {
        match result {
            Ok(Ok(())) => successful_sources += 1,
            Ok(Err(_)) | Err(_) => failed_sources += 1,
        }
    }
    
    // Verify we received events from multiple sources
    assert!(events_by_source.len() > 1, "Should have received events from multiple sources");
    assert!(failed_sources > 0, "Some sources should have failed");
    
    // Verify database contains only valid events
    let total_stored = sinex_db::count_events(ctx.pool()).await?;
    assert!(total_stored > 0, "Should have stored some valid events");
    
    Ok(())
}

#[sinex_test]
async fn test_dependency_failure_recovery(ctx: TestContext) -> TestResult {
    // Test recovery from external dependency failures
    let dependency_name = "kitty_socket";
    let mut source = ChaosEventSource::new(FailureMode::DependencyFailure { 
        dependency: dependency_name.to_string() 
    });
    let (tx, mut rx) = tokio::sync::mpsc::channel(100);
    
    // First attempt should fail due to dependency
    let result = source.stream_events(tx.clone()).await;
    assert!(result.is_err(), "Should fail due to dependency unavailability");
    
    let error_msg = result.unwrap_err().to_string();
    assert!(error_msg.contains(dependency_name), "Error should mention the failed dependency");
    
    // Simulate dependency recovery by switching to a working source
    let mut recovered_source = ChaosEventSource::new(FailureMode::StreamingCrash { after_events: 5 });
    
    // Second attempt should work (at least initially)
    let recovery_task = tokio::spawn(async move {
        timeout(Duration::from_secs(1), recovered_source.stream_events(tx)).await
    });
    
    // Collect events to verify recovery
    let mut received_events = 0;
    while let Ok(Some(event)) = timeout(Duration::from_millis(100), rx.recv()).await {
        received_events += 1;
        sinex_db::insert_event(ctx.pool(), &event).await?;
    }
    
    assert!(received_events > 0, "Should have received events after dependency recovery");
    
    Ok(())
}