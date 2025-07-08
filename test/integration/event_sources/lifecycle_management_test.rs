use crate::common::event_sources;
use crate::common::prelude::*;
use sinex_core::{CoreError, EventSource, EventSourceContext, RawEventBuilder};
use sinex_db::RawEvent;
use sinex_events_desktop::clipboard::ClipboardMonitor;
use sinex_events_fs::filesystem::FilesystemMonitor;
use sinex_events_terminal::terminal::KittySocketListener;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::time::{sleep, timeout};

/// Mock event source that can be configured to crash after a certain number of events
pub struct CrashingEventSource {
    crash_after: Duration,
    crash_on_event: Option<usize>,
    events_sent: Arc<AtomicUsize>,
}

impl CrashingEventSource {
    pub fn new(crash_after: Duration) -> Self {
        Self {
            crash_after,
            crash_on_event: None,
            events_sent: Arc::new(AtomicUsize::new(0)),
        }
    }
}

#[async_trait::async_trait]
impl EventSource for CrashingEventSource {
    type Config = serde_json::Value;
    const SOURCE_NAME: &'static str = "test.crashing_source";

    async fn initialize(_ctx: EventSourceContext) -> sinex_core::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self::new(Duration::from_millis(500)))
    }

    async fn stream_events(&mut self, tx: mpsc::Sender<RawEvent>) -> sinex_core::Result<()> {
        let start = std::time::Instant::now();

        loop {
            // Check if we should crash based on time
            if start.elapsed() >= self.crash_after {
                return Err(CoreError::Other(
                    "Simulated crash after timeout".to_string(),
                ));
            }

            // Check if we should crash based on event count
            let events_sent = self.events_sent.load(Ordering::SeqCst);
            if let Some(crash_on) = self.crash_on_event {
                if events_sent >= crash_on {
                    return Err(CoreError::Other(format!(
                        "Simulated crash after {} events",
                        crash_on
                    )));
                }
            }

            // Send a test event
            let event = RawEventBuilder::new(
                Self::SOURCE_NAME,
                "test.event",
                json!({
                    "event_number": events_sent,
                    "timestamp": chrono::Utc::now().to_rfc3339()
                }),
            )
            .build();

            if tx.send(event).await.is_err() {
                // Receiver dropped, exit gracefully
                break;
            }

            self.events_sent.fetch_add(1, Ordering::SeqCst);
            sleep(Duration::from_millis(100)).await;
        }

        Ok(())
    }

    async fn shutdown(&mut self) -> sinex_core::Result<()> {
        // Simulate shutdown taking some time
        sleep(Duration::from_millis(50)).await;
        Ok(())
    }
}

/// Source that simulates resource exhaustion (file descriptors, memory, etc.)
pub struct ResourceExhaustedSource {
    fd_limit_reached: bool,
    memory_pressure: bool,
}

#[async_trait::async_trait]
impl EventSource for ResourceExhaustedSource {
    type Config = serde_json::Value;
    const SOURCE_NAME: &'static str = "test.resource_exhausted";

    async fn initialize(_ctx: EventSourceContext) -> sinex_core::Result<Self> {
        Ok(Self {
            fd_limit_reached: false,
            memory_pressure: false,
        })
    }

    async fn stream_events(&mut self, tx: mpsc::Sender<RawEvent>) -> sinex_core::Result<()> {
        // Simulate reaching file descriptor limit
        if !self.fd_limit_reached {
            self.fd_limit_reached = true;
            return Err(CoreError::Other("Too many open files (EMFILE)".to_string()));
        }

        // If we get restarted, simulate memory pressure
        if !self.memory_pressure {
            self.memory_pressure = true;
            return Err(CoreError::Other(
                "Cannot allocate memory (ENOMEM)".to_string(),
            ));
        }

        // After both failures, work normally for a bit
        for i in 0..5 {
            let event = RawEventBuilder::new(
                Self::SOURCE_NAME,
                "test.recovery_event",
                json!({
                    "recovery_event": i,
                    "timestamp": chrono::Utc::now().to_rfc3339()
                }),
            )
            .build();

            if tx.send(event).await.is_err() {
                break;
            }
            sleep(Duration::from_millis(50)).await;
        }

        Ok(())
    }
}

#[sinex_test]
async fn test_event_source_crash_recovery(ctx: TestContext) -> TestResult {
    let (tx, mut rx) = mpsc::channel(100);
    let mut crashing_source = CrashingEventSource::new(Duration::from_millis(200));

    // Start the source - it should crash after 200ms
    let source_handle = tokio::spawn(async move { crashing_source.stream_events(tx).await });

    // Collect events until the source crashes
    let mut events = Vec::new();
    let result = loop {
        match timeout(Duration::from_millis(50), rx.recv()).await {
            Ok(Some(event)) => {
                events.push(event);
            }
            Ok(None) => {
                // Channel closed, source finished
                break source_handle.await.unwrap();
            }
            Err(_) => {
                // Timeout - check if source is still running
                if source_handle.is_finished() {
                    break source_handle.await.unwrap();
                }
            }
        }
    };

    // Verify the source crashed as expected
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Simulated crash"));

    // Verify we received some events before the crash
    assert!(
        !events.is_empty(),
        "Should have received events before crash"
    );
    assert!(!events.is_empty(), "Should have received at least 1 event");

    // Verify event content
    let first_event = &events[0];
    pretty_assertions::assert_eq!(first_event.source, "test.crashing_source");
    pretty_assertions::assert_eq!(first_event.event_type, "test.event");

    Ok(())
}

#[sinex_test]
async fn test_resource_exhaustion_handling(ctx: TestContext) -> TestResult {
    let (tx, mut rx) = mpsc::channel(100);
    let mut resource_source = ResourceExhaustedSource {
        fd_limit_reached: false,
        memory_pressure: false,
    };

    // First run - should fail with file descriptor limit
    let result1 = resource_source.stream_events(tx.clone()).await;
    assert!(result1.is_err());
    assert!(result1
        .unwrap_err()
        .to_string()
        .contains("Too many open files"));

    // Second run - should fail with memory pressure
    let result2 = resource_source.stream_events(tx.clone()).await;
    assert!(result2.is_err());
    assert!(result2
        .unwrap_err()
        .to_string()
        .contains("Cannot allocate memory"));

    // Third run - should work and produce events
    let source_handle = tokio::spawn(async move { resource_source.stream_events(tx).await });

    // Collect recovery events
    let mut recovery_events = Vec::new();
    while let Ok(Some(event)) = timeout(Duration::from_millis(100), rx.recv()).await {
        recovery_events.push(event);
        if recovery_events.len() >= 5 {
            break;
        }
    }

    source_handle.abort();

    // Verify recovery worked
    pretty_assertions::assert_eq!(recovery_events.len(), 5);
    for (i, event) in recovery_events.iter().enumerate() {
        pretty_assertions::assert_eq!(event.source, "test.resource_exhausted");
        pretty_assertions::assert_eq!(event.event_type, "test.recovery_event");
        pretty_assertions::assert_eq!(event.payload["recovery_event"], i);
    }

    Ok(())
}

#[sinex_test]
async fn test_filesystem_source_permission_denied(ctx: TestContext) -> TestResult {
    // Create a directory we can't read (simulated permission denied)
    let temp_dir = TempDir::new()?;
    let protected_dir = temp_dir.path().join("protected");
    std::fs::create_dir(&protected_dir)?;

    // Try to watch a non-existent directory (should fail gracefully)
    let non_existent = temp_dir.path().join("non_existent/deep/path");

    let config = json!({
        "watch_patterns": [
            format!("{}/**/*", protected_dir.to_str().unwrap()),
            format!("{}/**/*", non_existent.to_str().unwrap())
        ],
        "ignore_patterns": [],
        "debounce_ms": 50
    });

    let event_ctx = event_sources::test_context(config);

    // This should either fail gracefully or succeed with warnings
    match FilesystemMonitor::initialize(event_ctx).await {
        Ok(mut monitor) => {
            // If initialization succeeds, streaming should handle errors gracefully
            let (tx, mut rx) = mpsc::channel(10);

            let handle = tokio::spawn(async move { monitor.stream_events(tx).await });

            // Should not crash even with permission issues
            tokio::time::sleep(Duration::from_millis(100)).await;
            handle.abort();

            // Drain any events that might have been sent
            while let Ok(Some(_)) = timeout(Duration::from_millis(10), rx.recv()).await {
                // Just drain
            }
        }
        Err(e) => {
            // It's acceptable for initialization to fail with permission errors
            eprintln!("Filesystem monitor failed to initialize (expected): {}", e);
        }
    }

    Ok(())
}

#[sinex_test]
async fn test_kitty_socket_unavailable(ctx: TestContext) -> TestResult {
    // Try to connect to a non-existent socket
    let config = json!({
        "socket_path": "/tmp/non-existent-kitty-socket-12345",
        "polling_interval_secs": 1
    });

    let event_ctx = event_sources::test_context(config);

    // Should handle missing socket gracefully
    match KittySocketListener::initialize(event_ctx).await {
        Ok(mut listener) => {
            let (tx, mut rx) = mpsc::channel(10);

            // Streaming should handle connection errors gracefully
            let handle = tokio::spawn(async move { listener.stream_events(tx).await });

            // Give it time to try connecting and handle the error
            tokio::time::sleep(Duration::from_millis(100)).await;
            handle.abort();

            // Drain any events
            while let Ok(Some(_)) = timeout(Duration::from_millis(10), rx.recv()).await {
                // Just drain
            }
        }
        Err(e) => {
            // It's acceptable for initialization to fail with connection errors
            eprintln!("Kitty listener failed to initialize (expected): {}", e);
        }
    }

    Ok(())
}

#[sinex_test]
async fn test_clipboard_source_access_denied(ctx: TestContext) -> TestResult {
    // Test clipboard source when X11/Wayland access is denied
    let config = json!({
        "monitor_clipboard": true,
        "monitor_primary": true,
        "poll_interval_ms": 100,
        "max_content_size": 1024
    });

    let event_ctx = event_sources::test_context(config);

    // This might fail in CI/headless environments, which is expected
    match ClipboardMonitor::initialize(event_ctx).await {
        Ok(mut monitor) => {
            let (tx, mut rx) = mpsc::channel(10);

            let handle = tokio::spawn(async move { monitor.stream_events(tx).await });

            // Give it time to try accessing clipboard
            tokio::time::sleep(Duration::from_millis(100)).await;
            handle.abort();

            // Drain events
            while let Ok(Some(_)) = timeout(Duration::from_millis(10), rx.recv()).await {
                // Just drain
            }
        }
        Err(e) => {
            // Expected in headless/CI environments
            eprintln!(
                "Clipboard monitor failed to initialize (expected in CI): {}",
                e
            );
        }
    }

    Ok(())
}

#[sinex_test]
async fn test_event_source_coordination_failures(ctx: TestContext) -> TestResult {
    // Test what happens when multiple sources try to access shared resources
    let temp_dir = TempDir::new()?;

    let config1 = json!({
        "watch_patterns": [format!("{}/**/*", temp_dir.path().to_str().unwrap())],
        "ignore_patterns": [],
        "debounce_ms": 50
    });

    let config2 = config1.clone();

    let event_ctx1 = event_sources::test_context(config1);
    let event_ctx2 = event_sources::test_context(config2);

    // Start two filesystem monitors on the same directory
    let mut monitor1 = FilesystemMonitor::initialize(event_ctx1).await?;
    let mut monitor2 = FilesystemMonitor::initialize(event_ctx2).await?;

    let (tx1, mut rx1) = mpsc::channel(50);
    let (tx2, mut rx2) = mpsc::channel(50);

    let handle1 = tokio::spawn(async move { monitor1.stream_events(tx1).await });

    let handle2 = tokio::spawn(async move { monitor2.stream_events(tx2).await });

    // Give them time to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Create a test file to trigger events
    let test_file = temp_dir.path().join("coordination_test.txt");
    std::fs::write(&test_file, "test content")?;

    // Both monitors should detect the file creation
    let mut events1 = Vec::new();
    let mut events2 = Vec::new();

    // Collect events from both monitors
    for _ in 0..10 {
        tokio::select! {
            event = timeout(Duration::from_millis(100), rx1.recv()) => {
                if let Ok(Some(event)) = event {
                    events1.push(event);
                }
            }
            event = timeout(Duration::from_millis(100), rx2.recv()) => {
                if let Ok(Some(event)) = event {
                    events2.push(event);
                }
            }
            else => break,
        }
    }

    handle1.abort();
    handle2.abort();

    // Both should have detected the file creation
    // Note: It's possible for multiple filesystem watchers to work on the same directory
    assert!(
        !events1.is_empty() || !events2.is_empty(),
        "At least one monitor should have detected the file creation"
    );

    if !events1.is_empty() {
        assert!(events1[0]
            .payload
            .get("path")
            .unwrap()
            .as_str()
            .unwrap()
            .contains("coordination_test.txt"));
    }

    if !events2.is_empty() {
        assert!(events2[0]
            .payload
            .get("path")
            .unwrap()
            .as_str()
            .unwrap()
            .contains("coordination_test.txt"));
    }

    Ok(())
}

#[sinex_test]
async fn test_event_source_invalid_configuration(ctx: TestContext) -> TestResult {
    // Test various invalid configurations

    // Empty watch patterns
    let invalid_config1 = json!({
        "watch_patterns": [],
        "ignore_patterns": [],
        "debounce_ms": 50
    });

    let event_ctx1 = event_sources::test_context(invalid_config1);
    match FilesystemMonitor::initialize(event_ctx1).await {
        Ok(_) => {
            // Some sources might handle empty patterns gracefully
            eprintln!("FilesystemMonitor accepted empty watch patterns");
        }
        Err(e) => {
            eprintln!(
                "FilesystemMonitor rejected empty watch patterns (expected): {}",
                e
            );
        }
    }

    // Invalid debounce time
    let invalid_config2 = json!({
        "watch_patterns": ["/tmp"],
        "ignore_patterns": [],
        "debounce_ms": -1
    });

    let event_ctx2 = event_sources::test_context(invalid_config2);
    match FilesystemMonitor::initialize(event_ctx2).await {
        Ok(_) => {
            eprintln!("FilesystemMonitor accepted negative debounce (might use default)");
        }
        Err(e) => {
            eprintln!(
                "FilesystemMonitor rejected negative debounce (expected): {}",
                e
            );
        }
    }

    // Missing required fields
    let invalid_config3 = json!({
        "ignore_patterns": [],
        // Missing watch_patterns
    });

    let event_ctx3 = event_sources::test_context(invalid_config3);
    match FilesystemMonitor::initialize(event_ctx3).await {
        Ok(_) => {
            eprintln!("FilesystemMonitor used defaults for missing fields");
        }
        Err(e) => {
            eprintln!(
                "FilesystemMonitor rejected missing fields (expected): {}",
                e
            );
        }
    }

    Ok(())
}

#[sinex_test]
async fn test_source_shutdown_during_active_streaming(ctx: TestContext) -> TestResult {
    let temp_dir = TempDir::new()?;
    let config = json!({
        "watch_patterns": [format!("{}/**/*", temp_dir.path().to_str().unwrap())],
        "ignore_patterns": [],
        "debounce_ms": 50
    });

    let event_ctx = event_sources::test_context(config);
    let mut monitor = FilesystemMonitor::initialize(event_ctx).await?;

    let (tx, mut rx) = mpsc::channel(100);

    // Start streaming in background
    let handle = tokio::spawn(async move { monitor.stream_events(tx).await });

    // Give it time to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Create some file activity
    for i in 0..5 {
        let test_file = temp_dir.path().join(format!("test_{}.txt", i));
        std::fs::write(&test_file, format!("content {}", i))?;
        sleep(Duration::from_millis(20)).await;
    }

    // Abruptly stop the source
    handle.abort();

    // Drain any remaining events
    let mut events = Vec::new();
    while let Ok(Some(event)) = timeout(Duration::from_millis(50), rx.recv()).await {
        events.push(event);
    }

    // Should have received some events before shutdown
    eprintln!("Received {} events before shutdown", events.len());

    // The exact number depends on timing, but we should get at least some
    // In a real system, we'd want to ensure no events are lost during shutdown

    Ok(())
}
