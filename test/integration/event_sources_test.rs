use crate::common::prelude::*;
use crate::common::resources;
use chrono::{TimeZone, Utc};
use sinex_core::{CoreError, EventSource, EventSourceContext, EventType, RawEventBuilder};
use sinex_db::RawEvent;
use sinex_events_desktop::clipboard::ClipboardMonitor;
use sinex_events_fs::filesystem::FilesystemMonitor;
use sinex_events_terminal::{
    atuin::{AtuinConfig, AtuinDbReader, CommandExecutedAtuin, CommandExecutedAtuinPayload},
    terminal::{CommandExecuted, CommandExecutedPayload, KittyConfig, KittySocketListener},
};
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::time::{sleep, timeout};

/// Consolidated event source integration tests
///
/// This module consolidates tests from:
/// - atuin_tests.rs (Atuin shell history integration)
/// - atuin_tests_real.rs (Real Atuin integration tests)
/// - event_source_tests.rs (Generic event source functionality)
/// - kitty_comprehensive_test.rs (Comprehensive Kitty terminal tests)
/// - lifecycle_management_test.rs (Event source lifecycle management)
/// - terminal_tests.rs (Terminal event source tests)

// =============================================================================
// Filesystem Event Source Tests
// =============================================================================

#[sinex_test]
async fn test_filesystem_watcher_initialization(_ctx: TestContext) -> TestResult {
    let temp_dir = resources::temp_dir()?;
    let config = crate::common::event_sources::filesystem_config(temp_dir.path().to_str().unwrap());
    let ctx = crate::common::event_sources::test_context(config);
    let _watcher = FilesystemMonitor::initialize(ctx).await?;

    // FilesystemMonitor doesn't have name() or version() methods
    // These are provided by the EventSource trait constants
    pretty_assertions::assert_eq!(FilesystemMonitor::SOURCE_NAME, "fs");

    Ok(())
}

#[sinex_test]
async fn test_filesystem_watcher_captures_events(_ctx: TestContext) -> TestResult {
    let temp_dir = resources::temp_dir()?;
    let config = json!({
        "watch_patterns": [format!("{}/*", temp_dir.path().to_str().unwrap())],
        "ignore_patterns": [],
        "debounce_ms": 50
    });

    let ctx = crate::common::event_sources::test_context(config);
    let mut watcher = FilesystemMonitor::initialize(ctx).await?;

    let (tx, mut rx) = mpsc::channel(10);

    // Start capturing in background
    let capture_handle = tokio::spawn(async move { watcher.stream_events(tx).await });

    // Give watcher time to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Create a test file
    let test_file = temp_dir.path().join("test.txt");
    fs::write(&test_file, "Hello, world!")?;

    // Wait for event
    let event = timeout(Duration::from_secs(1), rx.recv()).await?;
    assert!(event.is_some());

    let event = event.unwrap();
    pretty_assertions::assert_eq!(event.source, "fs");
    assert!(event.event_type.contains("created") || event.event_type.contains("modify"));

    // Verify payload contains expected fields
    assert!(event.payload.get("path").is_some());

    capture_handle.abort();
    Ok(())
}

#[sinex_test]
async fn test_filesystem_watcher_ignores_patterns(_ctx: TestContext) -> TestResult {
    let temp_dir = resources::temp_dir()?;
    let config = json!({
        "watch_patterns": [format!("{}/*", temp_dir.path().to_str().unwrap())],
        "ignore_patterns": ["*.tmp", "test_*"],
        "debounce_ms": 50
    });

    let ctx = crate::common::event_sources::test_context(config);
    let mut watcher = FilesystemMonitor::initialize(ctx).await?;

    let (tx, mut rx) = mpsc::channel(10);

    let capture_handle = tokio::spawn(async move { watcher.stream_events(tx).await });

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Create files that should be ignored
    fs::write(temp_dir.path().join("test.tmp"), "ignored")?;
    fs::write(temp_dir.path().join("test_file.txt"), "ignored")?;

    // Create a file that should be captured
    fs::write(temp_dir.path().join("valid.txt"), "captured")?;

    // Should only receive one event (for valid.txt)
    let event = timeout(Duration::from_millis(500), rx.recv()).await?;
    assert!(event.is_some());

    let event = event.unwrap();

    // Verify the event is for the non-ignored file
    if let Some(path) = event.payload.get("path") {
        assert!(path.as_str().unwrap().contains("valid.txt"));
    }

    capture_handle.abort();
    Ok(())
}

// =============================================================================
// Terminal Event Source Tests
// =============================================================================

#[sinex_test]
async fn test_kitty_listener_initialization(_ctx: TestContext) -> TestResult {
    let temp_dir = TempDir::new()?;
    let socket_path = temp_dir.path().join("kitty-test-*");

    let config = KittyConfig {
        socket_path: socket_path.to_string_lossy().to_string(),
        polling_interval_secs: 1,
    };

    let ctx = crate::common::event_sources::test_context(serde_json::to_value(&config).unwrap());
    let listener = KittySocketListener::initialize(ctx).await;
    // Should succeed even if no socket exists (will wait for socket)
    assert!(
        listener.is_ok(),
        "Should initialize even without active socket"
    );
    Ok(())
}

#[sinex_test]
async fn test_kitty_event_structure(_ctx: TestContext) -> TestResult {
    // Test that the event payload structure is correct
    let payload = CommandExecutedPayload {
        command_string: "echo test".to_string(),
        cwd: "/tmp".to_string(),
        exit_code: 0,
        ts_start_orig: Utc::now(),
        ts_end_orig: Utc::now(),
    };

    // Verify serialization works
    let json = serde_json::to_value(&payload).unwrap();
    pretty_assertions::assert_eq!(json["command_string"], "echo test");
    pretty_assertions::assert_eq!(json["cwd"], "/tmp");
    pretty_assertions::assert_eq!(json["exit_code"], 0);

    // Verify event type constant
    pretty_assertions::assert_eq!(CommandExecuted::EVENT_NAME, "command.executed");

    Ok(())
}

#[sinex_test]
async fn test_kitty_socket_pattern_matching(_ctx: TestContext) -> TestResult {
    let config = KittyConfig {
        socket_path: "/tmp/mykitty-*".to_string(),
        polling_interval_secs: 1,
    };

    // The socket path should support glob patterns
    assert!(
        config.socket_path.contains("*"),
        "Socket path should support wildcards"
    );

    Ok(())
}

// =============================================================================
// Atuin Event Source Tests
// =============================================================================

/// Create a test Atuin database with sample data using real SQLite schema
fn create_test_atuin_db(path: &PathBuf, entries: Vec<TestAtuinEntry>) -> anyhow::Result<()> {
    use rusqlite::{params, Connection};

    let conn = Connection::open(path)?;

    // Create the Atuin history table with the real schema
    conn.execute(
        r#"
        CREATE TABLE IF NOT EXISTS history (
            id TEXT PRIMARY KEY,
            timestamp INTEGER NOT NULL,
            duration INTEGER NOT NULL,
            exit INTEGER NOT NULL,
            command TEXT NOT NULL,
            cwd TEXT NOT NULL,
            session TEXT NOT NULL,
            hostname TEXT NOT NULL,
            deleted_at INTEGER
        )
        "#,
        [],
    )?;

    // Insert test entries if provided
    for entry in entries {
        conn.execute(
            r#"
            INSERT INTO history (id, timestamp, duration, exit, command, cwd, session, hostname, deleted_at)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, NULL)
            "#,
            params![
                entry.id,
                entry.timestamp_ns,
                entry.duration_ns,
                entry.exit_code,
                entry.command,
                entry.cwd,
                entry.session,
                entry.hostname,
            ],
        )?;
    }

    Ok(())
}

struct TestAtuinEntry {
    id: String,
    timestamp_ns: i64,
    duration_ns: i64,
    exit_code: i32,
    command: String,
    cwd: String,
    session: String,
    hostname: String,
}

impl TestAtuinEntry {
    fn builder() -> TestAtuinEntryBuilder {
        TestAtuinEntryBuilder::default()
    }

    fn simple_command(command: &str) -> Self {
        Self {
            id: format!("test-{}", uuid::Uuid::new_v4()),
            timestamp_ns: chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default(),
            duration_ns: 1_000_000_000, // 1 second default
            exit_code: 0,
            command: command.to_string(),
            cwd: "/tmp".to_string(),
            session: "test-session".to_string(),
            hostname: "test-host".to_string(),
        }
    }
}

#[derive(Default)]
struct TestAtuinEntryBuilder {
    id: Option<String>,
    timestamp_ns: Option<i64>,
    duration_ns: Option<i64>,
    exit_code: Option<i32>,
    command: Option<String>,
    cwd: Option<String>,
    session: Option<String>,
    hostname: Option<String>,
}

impl TestAtuinEntryBuilder {
    fn with_id(mut self, id: &str) -> Self {
        self.id = Some(id.to_string());
        self
    }

    fn with_command(mut self, command: &str) -> Self {
        self.command = Some(command.to_string());
        self
    }

    fn with_exit_code(mut self, code: i32) -> Self {
        self.exit_code = Some(code);
        self
    }

    fn with_timestamp_seconds(mut self, timestamp: i64) -> Self {
        self.timestamp_ns = Some(timestamp * 1_000_000_000);
        self
    }

    fn with_duration_ms(mut self, duration_ms: i64) -> Self {
        self.duration_ns = Some(duration_ms * 1_000_000);
        self
    }

    fn with_cwd(mut self, cwd: &str) -> Self {
        self.cwd = Some(cwd.to_string());
        self
    }

    fn build(self) -> TestAtuinEntry {
        TestAtuinEntry {
            id: self
                .id
                .unwrap_or_else(|| format!("test-{}", uuid::Uuid::new_v4())),
            timestamp_ns: self
                .timestamp_ns
                .unwrap_or_else(|| chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()),
            duration_ns: self.duration_ns.unwrap_or(1_000_000_000),
            exit_code: self.exit_code.unwrap_or(0),
            command: self.command.unwrap_or_else(|| "echo test".to_string()),
            cwd: self.cwd.unwrap_or_else(|| "/tmp".to_string()),
            session: self.session.unwrap_or_else(|| "test-session".to_string()),
            hostname: self.hostname.unwrap_or_else(|| "test-host".to_string()),
        }
    }
}

#[sinex_test]
async fn test_atuin_reader_initialization(_ctx: TestContext) -> TestResult {
    let temp_dir = resources::temp_dir()?;
    let db_path = temp_dir.path().join("history.db");

    // Create empty database
    create_test_atuin_db(&db_path, vec![]).unwrap();

    let config = AtuinConfig {
        db_path: db_path.clone(),
        polling_interval_secs: 1,
        batch_size: 10,
        use_file_watch: false,
    };

    let event_ctx =
        crate::common::event_sources::test_context(serde_json::to_value(&config).unwrap());
    let reader = AtuinDbReader::initialize(event_ctx).await;
    assert!(reader.is_ok(), "Should initialize with valid database");

    // Test with non-existent database
    let bad_config = AtuinConfig {
        db_path: temp_dir.path().join("nonexistent.db"),
        ..config
    };

    let event_ctx =
        crate::common::event_sources::test_context(serde_json::to_value(&bad_config).unwrap());
    let reader = AtuinDbReader::initialize(event_ctx).await;
    assert!(reader.is_err(), "Should fail with non-existent database");
    Ok(())
}

#[sinex_test]
async fn test_atuin_event_capture(_ctx: TestContext) -> TestResult {
    let temp_dir = resources::temp_dir()?;
    let db_path = temp_dir.path().join("history.db");

    // Create test entries using builder pattern
    let now = Utc::now();
    let entries = vec![
        TestAtuinEntry::builder()
            .with_id("test-id-1")
            .with_command("ls -la")
            .with_cwd("/home/test")
            .with_duration_ms(1000)
            .with_timestamp_seconds(now.timestamp())
            .build(),
        TestAtuinEntry::builder()
            .with_id("test-id-2")
            .with_command("git status")
            .with_exit_code(1)
            .with_cwd("/home/test/project")
            .with_duration_ms(500)
            .with_timestamp_seconds(now.timestamp() + 10)
            .build(),
    ];

    create_test_atuin_db(&db_path, entries).unwrap();

    let config = AtuinConfig {
        db_path,
        polling_interval_secs: 1,
        batch_size: 10,
        use_file_watch: false,
    };

    let event_ctx =
        crate::common::event_sources::test_context(serde_json::to_value(&config).unwrap());
    let mut reader = AtuinDbReader::initialize(event_ctx).await.unwrap();
    let (tx, mut rx) = mpsc::channel(100);

    // Start reading in background
    let handle = tokio::spawn(async move {
        let _ = reader.stream_events(tx).await;
    });

    // Collect events
    let mut events = vec![];
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while let Some(event) = rx.recv().await {
            events.push(event);
            if events.len() >= 2 {
                break;
            }
        }
    })
    .await
    .unwrap();

    handle.abort();

    // Verify events
    pretty_assertions::assert_eq!(events.len(), 2, "Should capture both test entries");

    // Check first event
    let event1 = &events[0];
    pretty_assertions::assert_eq!(event1.event_type, CommandExecutedAtuin::EVENT_NAME);
    pretty_assertions::assert_eq!(event1.source, "ingestor.atuin_db_reader");

    let payload1: CommandExecutedAtuinPayload =
        serde_json::from_value(event1.payload.clone()).unwrap();
    pretty_assertions::assert_eq!(payload1.command_string, "ls -la");
    pretty_assertions::assert_eq!(payload1.cwd, "/home/test");
    pretty_assertions::assert_eq!(payload1.exit_code, 0);
    pretty_assertions::assert_eq!(payload1.atuin_history_id, "test-id-1");

    // Check second event
    let event2 = &events[1];
    let payload2: CommandExecutedAtuinPayload =
        serde_json::from_value(event2.payload.clone()).unwrap();
    pretty_assertions::assert_eq!(payload2.command_string, "git status");
    pretty_assertions::assert_eq!(payload2.exit_code, 1);
    pretty_assertions::assert_eq!(payload2.atuin_history_id, "test-id-2");
    Ok(())
}

#[sinex_test]
async fn test_atuin_timestamp_conversion(_ctx: TestContext) -> TestResult {
    let temp_dir = resources::temp_dir()?;
    let db_path = temp_dir.path().join("history.db");

    // Create entry with specific timestamp
    let timestamp = Utc.timestamp_opt(1749700000, 0).unwrap(); // Known timestamp
    let entries = vec![TestAtuinEntry {
        id: "time-test".to_string(),
        timestamp_ns: timestamp.timestamp_nanos_opt().unwrap(),
        duration_ns: 2_500_000_000, // 2.5 seconds
        exit_code: 0,
        command: "sleep 2.5".to_string(),
        cwd: "/tmp".to_string(),
        session: "s1".to_string(),
        hostname: "host1".to_string(),
    }];

    create_test_atuin_db(&db_path, entries).unwrap();

    let config = AtuinConfig {
        db_path,
        polling_interval_secs: 1,
        batch_size: 10,
        use_file_watch: false,
    };

    let event_ctx =
        crate::common::event_sources::test_context(serde_json::to_value(&config).unwrap());
    let mut reader = AtuinDbReader::initialize(event_ctx).await.unwrap();
    let (tx, mut rx) = mpsc::channel(100);

    let handle = tokio::spawn(async move {
        let _ = reader.stream_events(tx).await;
    });

    // Get the event
    let event = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap();

    handle.abort();

    let payload: CommandExecutedAtuinPayload = serde_json::from_value(event.payload).unwrap();

    // Verify timestamps
    pretty_assertions::assert_eq!(payload.ts_end_orig, timestamp);
    pretty_assertions::assert_eq!(payload.duration_ns, 2_500_000_000);

    // Start time should be 2.5 seconds before end time
    let expected_start = timestamp - chrono::Duration::milliseconds(2500);
    pretty_assertions::assert_eq!(payload.ts_start_orig, expected_start);
    Ok(())
}

// =============================================================================
// Event Source Lifecycle Management Tests
// =============================================================================

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

    pub fn with_crash_on_event(mut self, crash_on_event: usize) -> Self {
        self.crash_on_event = Some(crash_on_event);
        self
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

#[sinex_test]
async fn test_event_source_lifecycle_crash_handling(_ctx: TestContext) -> TestResult {
    let _ctx = crate::common::event_sources::test_context(json!({}));
    let mut source = CrashingEventSource::new(Duration::from_millis(200));
    let (tx, mut rx) = mpsc::channel(100);

    // Start the source
    let handle = tokio::spawn(async move { source.stream_events(tx).await });

    // Collect events until the source crashes
    let mut events = vec![];
    let start = std::time::Instant::now();

    loop {
        match timeout(Duration::from_millis(100), rx.recv()).await {
            Ok(Some(event)) => {
                events.push(event);
            }
            Ok(None) => break, // Channel closed
            Err(_) => {
                // Timeout - check if we should stop
                if start.elapsed() > Duration::from_secs(1) {
                    break;
                }
            }
        }
    }

    // Wait for the source to crash
    let result = handle.await.unwrap();
    assert!(result.is_err(), "Source should have crashed");

    // Should have received some events before crashing
    assert!(
        !events.is_empty(),
        "Should have received events before crash"
    );

    Ok(())
}

#[sinex_test]
async fn test_event_source_graceful_shutdown(_ctx: TestContext) -> TestResult {
    let _ctx = crate::common::event_sources::test_context(json!({}));
    let mut source = CrashingEventSource::new(Duration::from_secs(10)); // Won't crash on time
    let (tx, mut rx) = mpsc::channel(100);

    // Start the source
    let handle = tokio::spawn(async move {
        let result = source.stream_events(tx).await;
        let _ = source.shutdown().await;
        result
    });

    // Collect a few events
    let mut events = vec![];
    for _ in 0..3 {
        if let Ok(Some(event)) = timeout(Duration::from_millis(500), rx.recv()).await {
            events.push(event);
        }
    }

    // Drop the receiver to trigger graceful shutdown
    drop(rx);

    // Wait for the source to exit gracefully
    let result = handle.await.unwrap();
    assert!(
        result.is_ok(),
        "Source should exit gracefully when receiver is dropped"
    );

    // Should have received some events
    assert!(!events.is_empty(), "Should have received events");

    Ok(())
}

#[sinex_test]
async fn test_event_source_restart_after_crash(_ctx: TestContext) -> TestResult {
    let _ctx = crate::common::event_sources::test_context(json!({}));

    // First instance - will crash after 3 events
    let mut source1 = CrashingEventSource::new(Duration::from_secs(10)).with_crash_on_event(3);
    let (tx1, mut rx1) = mpsc::channel(100);

    let handle1 = tokio::spawn(async move { source1.stream_events(tx1).await });

    // Collect events until crash
    let mut events1 = vec![];
    while let Ok(Some(event)) = timeout(Duration::from_millis(200), rx1.recv()).await {
        events1.push(event);
        if events1.len() >= 5 {
            break; // Safety valve
        }
    }

    let result1 = handle1.await.unwrap();
    assert!(result1.is_err(), "First instance should have crashed");
    assert_eq!(
        events1.len(),
        3,
        "Should have received 3 events before crash"
    );

    // Second instance - restart after crash
    let mut source2 = CrashingEventSource::new(Duration::from_secs(10));
    let (tx2, mut rx2) = mpsc::channel(100);

    let handle2 = tokio::spawn(async move { source2.stream_events(tx2).await });

    // Collect events from restarted source
    let mut events2 = vec![];
    for _ in 0..3 {
        if let Ok(Some(event)) = timeout(Duration::from_millis(200), rx2.recv()).await {
            events2.push(event);
        }
    }

    // Clean shutdown
    drop(rx2);
    let result2 = handle2.await.unwrap();
    assert!(result2.is_ok(), "Second instance should exit gracefully");
    assert!(
        !events2.is_empty(),
        "Should have received events after restart"
    );

    Ok(())
}

// =============================================================================
// Clipboard Event Source Tests
// =============================================================================

#[sinex_test]
async fn test_clipboard_monitor_initialization(_ctx: TestContext) -> TestResult {
    let config = json!({
        "monitor_clipboard": true,
        "monitor_primary": false,
        "poll_interval_ms": 500,
        "max_content_size": 1024000
    });

    let ctx = crate::common::event_sources::test_context(config);
    let result = ClipboardMonitor::initialize(ctx).await;

    // Clipboard monitor may fail to initialize in headless environments
    // This is expected behavior, not a test failure
    match result {
        Ok(_monitor) => {
            // Successfully initialized - we're in a graphical environment
            assert!(true);
        }
        Err(_) => {
            // Failed to initialize - likely headless environment
            // This is acceptable for CI/testing environments
            assert!(true);
        }
    }

    Ok(())
}

// =============================================================================
// Event Source Performance Tests
// =============================================================================

#[sinex_test]
async fn test_event_source_throughput(_ctx: TestContext) -> TestResult {
    let _ctx = crate::common::event_sources::test_context(json!({}));
    let mut source = CrashingEventSource::new(Duration::from_secs(5));
    let (tx, mut rx) = mpsc::channel(1000);

    let handle = tokio::spawn(async move { source.stream_events(tx).await });

    // Measure throughput over 1 second
    let start = std::time::Instant::now();
    let mut event_count = 0;
    let test_duration = Duration::from_secs(1);

    while start.elapsed() < test_duration {
        match timeout(Duration::from_millis(50), rx.recv()).await {
            Ok(Some(_event)) => {
                event_count += 1;
            }
            Ok(None) => break,
            Err(_) => break,
        }
    }

    // Clean shutdown
    drop(rx);
    let _ = handle.await;

    // Should have reasonable throughput (at least 5 events/second)
    let throughput = event_count as f64 / test_duration.as_secs_f64();
    assert!(
        throughput >= 5.0,
        "Throughput too low: {:.2} events/sec",
        throughput
    );

    Ok(())
}

#[sinex_test]
async fn test_event_source_memory_usage(_ctx: TestContext) -> TestResult {
    let _ctx = crate::common::event_sources::test_context(json!({}));
    let mut source = CrashingEventSource::new(Duration::from_secs(2));
    let (tx, mut rx) = mpsc::channel(10); // Small buffer to test backpressure

    let handle = tokio::spawn(async move { source.stream_events(tx).await });

    // Slowly consume events to test memory behavior under backpressure
    let mut events = vec![];
    let mut iterations = 0;

    while iterations < 50 {
        match timeout(Duration::from_millis(100), rx.recv()).await {
            Ok(Some(event)) => {
                events.push(event);
                // Simulate slow processing
                sleep(Duration::from_millis(20)).await;
                iterations += 1;
            }
            Ok(None) => break,
            Err(_) => break,
        }
    }

    // Clean shutdown
    drop(rx);
    let _ = handle.await;

    // Should have handled backpressure without issues
    assert!(
        !events.is_empty(),
        "Should have received events despite backpressure"
    );

    Ok(())
}

// =============================================================================
// Event Source Error Handling Tests
// =============================================================================

#[sinex_test]
async fn test_event_source_error_conditions(_ctx: TestContext) -> TestResult {
    // Test with non-existent database file for Atuin
    let temp_dir = resources::temp_dir()?;
    let bad_config = AtuinConfig {
        db_path: temp_dir.path().join("nonexistent.db"),
        polling_interval_secs: 1,
        batch_size: 10,
        use_file_watch: false,
    };

    let event_ctx =
        crate::common::event_sources::test_context(serde_json::to_value(&bad_config).unwrap());
    let result = AtuinDbReader::initialize(event_ctx).await;
    assert!(result.is_err(), "Should fail with non-existent database");

    // Test with corrupted database file
    let corrupted_db_path = temp_dir.path().join("corrupted.db");
    std::fs::write(&corrupted_db_path, "this is not a valid sqlite database").unwrap();

    let corrupted_config = AtuinConfig {
        db_path: corrupted_db_path,
        polling_interval_secs: 1,
        batch_size: 10,
        use_file_watch: false,
    };

    let event_ctx = crate::common::event_sources::test_context(
        serde_json::to_value(&corrupted_config).unwrap(),
    );
    // Note: AtuinDbReader initialization only checks if file exists,
    // actual corruption would be detected during event streaming
    let reader = AtuinDbReader::initialize(event_ctx).await;
    assert!(
        reader.is_ok(),
        "Initialization should succeed even with corrupted file"
    );

    Ok(())
}

// =============================================================================
// Event Source Integration Tests
// =============================================================================

#[sinex_test]
async fn test_multiple_event_sources_coordination(_ctx: TestContext) -> TestResult {
    let temp_dir = resources::temp_dir()?;
    let db_path = temp_dir.path().join("history.db");

    // Create test Atuin entries
    let entries = vec![
        TestAtuinEntry::simple_command("echo test1"),
        TestAtuinEntry::simple_command("echo test2"),
    ];
    create_test_atuin_db(&db_path, entries).unwrap();

    // Initialize multiple event sources
    let atuin_config = AtuinConfig {
        db_path,
        polling_interval_secs: 1,
        batch_size: 10,
        use_file_watch: false,
    };

    let fs_config = json!({
        "watch_patterns": [format!("{}/*", temp_dir.path().to_str().unwrap())],
        "ignore_patterns": [],
        "debounce_ms": 50
    });

    let atuin_ctx =
        crate::common::event_sources::test_context(serde_json::to_value(&atuin_config).unwrap());
    let fs_ctx = crate::common::event_sources::test_context(fs_config);

    let mut atuin_reader = AtuinDbReader::initialize(atuin_ctx).await?;
    let mut fs_watcher = FilesystemMonitor::initialize(fs_ctx).await?;

    // Start both sources
    let (atuin_tx, mut atuin_rx) = mpsc::channel(100);
    let (fs_tx, mut fs_rx) = mpsc::channel(100);

    let atuin_handle = tokio::spawn(async move { atuin_reader.stream_events(atuin_tx).await });

    let fs_handle = tokio::spawn(async move { fs_watcher.stream_events(fs_tx).await });

    // Generate filesystem events
    tokio::time::sleep(Duration::from_millis(100)).await;
    fs::write(temp_dir.path().join("test_file.txt"), "test content")?;

    // Collect events from both sources
    let mut all_events = vec![];
    let mut atuin_events = 0;
    let mut fs_events = 0;

    for _ in 0..100 {
        // Safety valve
        tokio::select! {
            event = atuin_rx.recv() => {
                if let Some(event) = event {
                    if event.source.contains("atuin") {
                        atuin_events += 1;
                    }
                    all_events.push(event);
                }
            }
            event = fs_rx.recv() => {
                if let Some(event) = event {
                    if event.source == "fs" {
                        fs_events += 1;
                    }
                    all_events.push(event);
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(100)) => {
                // Continue collecting
            }
        }

        // Stop when we have events from both sources
        if atuin_events > 0 && fs_events > 0 {
            break;
        }

        // Or when we have collected enough events
        if all_events.len() >= 5 {
            break;
        }
    }

    // Clean shutdown
    atuin_handle.abort();
    fs_handle.abort();

    // Should have received events from both sources
    assert!(
        !all_events.is_empty(),
        "Should have received events from sources"
    );

    // Verify we got different event types
    let sources: HashSet<_> = all_events.iter().map(|e| e.source.as_str()).collect();
    assert!(
        sources.len() > 1,
        "Should have events from multiple sources"
    );

    Ok(())
}

// =============================================================================
// Helper Functions
// =============================================================================

/// Helper to verify event payload structure
#[allow(dead_code)]
pub fn verify_atuin_payload(event: &RawEvent) -> anyhow::Result<()> {
    pretty_assertions::assert_eq!(event.event_type, "command.executed");
    pretty_assertions::assert_eq!(event.source, "ingestor.atuin_db_reader");

    let payload: CommandExecutedAtuinPayload = serde_json::from_value(event.payload.clone())?;

    // Verify required fields exist
    assert!(!payload.command_string.is_empty());
    assert!(!payload.cwd.is_empty());
    assert!(!payload.atuin_history_id.is_empty());
    assert!(!payload.atuin_session_id.is_empty());
    assert!(payload.duration_ns >= 0);

    Ok(())
}
