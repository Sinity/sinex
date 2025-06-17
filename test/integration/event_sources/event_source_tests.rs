use anyhow::Result;
use sinex_core::{EventSource, EventSourceContext};
use sinex_events::{
    filesystem::FilesystemMonitor,
    terminal::KittySocketListener,
    asciinema::AsciinemaRecorder, 
    clipboard::ClipboardMonitor,
    scrollback::ScrollbackCapture,
};
use std::time::Duration;
use tempfile::TempDir;
use tokio::sync::mpsc;
use tokio::time::timeout;
use serde_json::{json, Value};
use std::fs;

fn create_test_context(config: Value) -> EventSourceContext {
    EventSourceContext::new(config)
}

#[tokio::test]
async fn test_filesystem_watcher_initialization() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let config = json!({
        "watch_patterns": [format!("{}/**/*", temp_dir.path().to_str().unwrap())],
        "ignore_patterns": ["*.tmp", "*.log"],
        "debounce_ms": 100
    });
    
    let ctx = create_test_context(config);
    let _watcher = FilesystemMonitor::initialize(ctx).await?;
    
    // FilesystemMonitor doesn't have name() or version() methods
    // These are provided by the EventSource trait constants
    assert_eq!(FilesystemMonitor::SOURCE_NAME, "filesystem");
    
    Ok(())
}

#[tokio::test]
async fn test_filesystem_watcher_captures_events() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let config = json!({
        "watch_patterns": [format!("{}/*", temp_dir.path().to_str().unwrap())],
        "ignore_patterns": [],
        "debounce_ms": 50
    });
    
    let ctx = create_test_context(config);
    let mut watcher = FilesystemMonitor::initialize(ctx).await?;
    
    let (tx, mut rx) = mpsc::channel(10);
    
    // Start capturing in background
    let capture_handle = tokio::spawn(async move {
        watcher.stream_events(tx).await
    });
    
    // Give watcher time to start
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // Create a test file
    let test_file = temp_dir.path().join("test.txt");
    fs::write(&test_file, "Hello, world!")?;
    
    // Wait for event
    let event = timeout(Duration::from_secs(1), rx.recv()).await?;
    assert!(event.is_some());
    
    let event = event.unwrap();
    assert_eq!(event.source, "filesystem");
    assert!(event.event_type.contains("created") || event.event_type.contains("modify"));
    
    // Verify payload contains expected fields
    assert!(event.payload.get("path").is_some());
    
    capture_handle.abort();
    Ok(())
}

#[tokio::test]
async fn test_filesystem_watcher_ignores_patterns() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let config = json!({
        "watch_patterns": [format!("{}/*", temp_dir.path().to_str().unwrap())],
        "ignore_patterns": ["*.tmp", "test_*"],
        "debounce_ms": 50
    });
    
    let ctx = create_test_context(config);
    let mut watcher = FilesystemMonitor::initialize(ctx).await?;
    
    let (tx, mut rx) = mpsc::channel(10);
    
    let capture_handle = tokio::spawn(async move {
        watcher.stream_events(tx).await
    });
    
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
    
    // Debug: Print the actual event payload to understand what we're getting
    eprintln!("DEBUG: Received event payload: {:?}", event.payload);
    if let Some(path) = event.payload.get("path") {
        eprintln!("DEBUG: Event path: {:?}", path.as_str());
    }
    
    assert!(event.payload.get("path").unwrap().as_str().unwrap().contains("valid.txt"));
    
    // Should not receive more events (the ignored files)
    let no_event = timeout(Duration::from_millis(200), rx.recv()).await;
    assert!(no_event.is_err() || no_event.unwrap().is_none());
    
    capture_handle.abort();
    Ok(())
}

#[tokio::test]
async fn test_kitty_socket_listener_initialization() -> Result<()> {
    let config = json!({
        "socket_path": "/tmp/test-kitty-socket",
        "polling_interval_secs": 2
    });
    
    let ctx = create_test_context(config);
    let _listener = KittySocketListener::initialize(ctx).await?;
    
    assert_eq!(KittySocketListener::SOURCE_NAME, "terminal.kitty");
    
    Ok(())
}

#[tokio::test]
async fn test_asciinema_recorder_initialization() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let config = json!({
        "recordings_dir": temp_dir.path().to_str().unwrap(),
        "file_pattern": "*.cast",
        "polling_interval_secs": 5,
        "auto_start_recording": false,
        "record_command": "asciinema rec --quiet --overwrite",
        "git_annex_repo": null,
        "auto_annex": false
    });
    
    let ctx = create_test_context(config);
    let _recorder = AsciinemaRecorder::initialize(ctx).await?;
    
    assert_eq!(AsciinemaRecorder::SOURCE_NAME, "ingestor.asciinema_recorder");
    
    Ok(())
}

#[tokio::test] 
async fn test_clipboard_monitor_initialization() -> Result<()> {
    let config = json!({
        "monitor_clipboard": true,
        "monitor_primary": true,
        "monitor_secondary": false,
        "poll_interval_ms": 500,
        "hash_file_content": false,
        "max_preview_length": 100,
        "enable_history": false,
        "max_history_entries": 100,
        "max_content_size": 1048576,
        "annex_repo_path": null
    });
    
    let ctx = create_test_context(config);
    let _monitor = ClipboardMonitor::initialize(ctx).await?;
    
    assert_eq!(ClipboardMonitor::SOURCE_NAME, "clipboard.monitor");
    
    Ok(())
}

#[tokio::test]
async fn test_filesystem_watcher_ignore_patterns_comprehensive() -> Result<()> {
    let temp_dir = TempDir::new()?;
    
    // Create a subdirectory to test path-based patterns
    let sub_dir = temp_dir.path().join("logs");
    fs::create_dir(&sub_dir)?;
    
    let config = json!({
        "watch_patterns": [format!("{}/**/*", temp_dir.path().to_str().unwrap())],
        "ignore_patterns": [
            "*.tmp",           // filename pattern
            "test_*",          // filename pattern 
            "logs/*.log",      // path-based pattern
            "**/debug/**"      // recursive path pattern
        ],
        "debounce_ms": 50
    });
    
    let ctx = create_test_context(config);
    let mut watcher = FilesystemMonitor::initialize(ctx).await?;
    
    let (tx, mut rx) = mpsc::channel(20);
    
    let capture_handle = tokio::spawn(async move {
        watcher.stream_events(tx).await
    });
    
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // Files that should be ignored
    fs::write(temp_dir.path().join("backup.tmp"), "ignored")?;           // *.tmp
    fs::write(temp_dir.path().join("test_data.txt"), "ignored")?;        // test_*  
    fs::write(sub_dir.join("error.log"), "ignored")?;                    // logs/*.log
    
    // Files that should be captured
    fs::write(temp_dir.path().join("valid.txt"), "captured")?;
    fs::write(sub_dir.join("config.json"), "captured")?;
    
    // Collect events for a short period
    let mut events = Vec::new();
    for _ in 0..5 {
        match timeout(Duration::from_millis(100), rx.recv()).await {
            Ok(Some(event)) => events.push(event),
            _ => break,
        }
    }
    
    // Should have received events for valid.txt and config.json only
    assert!(events.len() >= 2, "Expected at least 2 events, got {}", events.len());
    
    for event in &events {
        let path = event.payload.get("path").unwrap().as_str().unwrap();
        assert!(
            path.contains("valid.txt") || path.contains("config.json"),
            "Unexpected event for path: {}", path
        );
    }
    
    capture_handle.abort();
    Ok(())
}

#[tokio::test]
async fn test_scrollback_capture_initialization() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let config = json!({
        "kitty_socket_path": "/tmp/test-kitty-socket",
        "capture_interval_secs": 15,
        "max_scrollback_lines": 5000,
        "include_ansi_codes": false,
        "capture_command_output": true,
        "save_to_files": false,
        "scrollback_dir": temp_dir.path().to_str().unwrap(),
        "capture_on_command": false,
        "command_capture_delay_ms": 500
    });
    
    let ctx = create_test_context(config);
    let _capture = ScrollbackCapture::initialize(ctx).await?;
    
    assert_eq!(ScrollbackCapture::SOURCE_NAME, "ingestor.scrollback_capture");
    
    Ok(())
}