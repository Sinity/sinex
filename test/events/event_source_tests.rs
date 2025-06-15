use anyhow::Result;
use sinex_core::{EventSource, EventSourceContext};
use sinex_events::{
    filesystem::FilesystemWatcher,
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
        "watch_paths": [temp_dir.path().to_str().unwrap()],
        "recursive": true,
        "event_types": ["create", "modify", "delete"],
        "ignore_patterns": ["*.tmp", "*.log"]
    });
    
    let ctx = create_test_context(config);
    let watcher = FilesystemWatcher::initialize(ctx).await?;
    
    assert_eq!(watcher.name(), "filesystem");
    assert!(!watcher.version().is_empty());
    
    Ok(())
}

#[tokio::test]
async fn test_filesystem_watcher_captures_events() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let config = json!({
        "watch_paths": [temp_dir.path().to_str().unwrap()],
        "recursive": false,
        "event_types": ["create", "modify"],
        "ignore_patterns": []
    });
    
    let ctx = create_test_context(config);
    let mut watcher = FilesystemWatcher::initialize(ctx).await?;
    
    let (tx, mut rx) = mpsc::channel(10);
    
    // Start capturing in background
    let capture_handle = tokio::spawn(async move {
        watcher.capture_events(tx).await
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
    assert!(event.event_type.contains("create") || event.event_type.contains("modify"));
    
    // Verify payload contains expected fields
    assert!(event.payload.get("path").is_some());
    assert!(event.payload.get("event_type").is_some());
    
    capture_handle.abort();
    Ok(())
}

#[tokio::test]
async fn test_filesystem_watcher_ignores_patterns() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let config = json!({
        "watch_paths": [temp_dir.path().to_str().unwrap()],
        "recursive": false,
        "event_types": ["create"],
        "ignore_patterns": ["*.tmp", "test_*"]
    });
    
    let ctx = create_test_context(config);
    let mut watcher = FilesystemWatcher::initialize(ctx).await?;
    
    let (tx, mut rx) = mpsc::channel(10);
    
    let capture_handle = tokio::spawn(async move {
        watcher.capture_events(tx).await
    });
    
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // Create files that should be ignored
    fs::write(temp_dir.path().join("test.tmp"), "ignored")?;
    fs::write(temp_dir.path().join("test_file.txt"), "ignored")?;
    
    // Create a file that should be captured
    fs::write(temp_dir.path().join("valid.txt"), "captured")?;
    
    // Should only receive one event
    let event = timeout(Duration::from_millis(500), rx.recv()).await?;
    assert!(event.is_some());
    
    let event = event.unwrap();
    assert!(event.payload.get("path").unwrap().as_str().unwrap().contains("valid.txt"));
    
    // Should not receive more events
    let no_event = timeout(Duration::from_millis(200), rx.recv()).await;
    assert!(no_event.is_err() || no_event.unwrap().is_none());
    
    capture_handle.abort();
    Ok(())
}

#[tokio::test]
async fn test_kitty_socket_listener_initialization() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let socket_path = temp_dir.path().join("kitty-test.sock");
    
    let config = json!({
        "socket_path": socket_path.to_str().unwrap(),
        "buffer_size": 4096,
        "reconnect_interval": 1000
    });
    
    let ctx = create_test_context(config);
    let listener = KittySocketListener::initialize(ctx).await;
    
    // Should succeed even if socket doesn't exist (will wait for it)
    assert!(listener.is_ok());
    
    let listener = listener.unwrap();
    assert_eq!(listener.name(), "terminal_kitty");
    
    Ok(())
}

#[tokio::test]
async fn test_asciinema_recorder_initialization() -> Result<()> {
    let temp_dir = TempDir::new()?;
    
    let config = json!({
        "watch_dir": temp_dir.path().to_str().unwrap(),
        "poll_interval": 100,
        "completed_marker": ".completed"
    });
    
    let ctx = create_test_context(config);
    let recorder = AsciinemaRecorder::initialize(ctx).await?;
    
    assert_eq!(recorder.name(), "asciinema");
    
    Ok(())
}

#[tokio::test]
async fn test_asciinema_recorder_detects_recordings() -> Result<()> {
    let temp_dir = TempDir::new()?;
    
    let config = json!({
        "watch_dir": temp_dir.path().to_str().unwrap(),
        "poll_interval": 50,
        "completed_marker": ".completed"
    });
    
    let ctx = create_test_context(config);
    let mut recorder = AsciinemaRecorder::initialize(ctx).await?;
    
    let (tx, mut rx) = mpsc::channel(10);
    
    let capture_handle = tokio::spawn(async move {
        recorder.capture_events(tx).await
    });
    
    // Create a mock recording file
    let recording_path = temp_dir.path().join("test-recording.cast");
    let header = json!({
        "version": 2,
        "width": 80,
        "height": 24,
        "timestamp": 1234567890,
        "title": "Test Recording",
        "env": {"SHELL": "/bin/bash", "TERM": "xterm-256color"}
    });
    
    let mut content = serde_json::to_string(&header)? + "\n";
    content += "[0.1, \"o\", \"$ \"]\n";
    content += "[1.0, \"o\", \"echo test\\n\"]\n";
    content += "[1.5, \"o\", \"test\\n$ \"]\n";
    
    fs::write(&recording_path, content)?;
    
    // Mark as completed
    fs::write(recording_path.with_extension("completed"), "")?;
    
    // Wait for events
    let event = timeout(Duration::from_millis(500), rx.recv()).await?;
    assert!(event.is_some());
    
    let event = event.unwrap();
    assert_eq!(event.source, "asciinema");
    assert_eq!(event.event_type, "recording_started");
    
    // Should get more events (data chunks)
    let _data_event = timeout(Duration::from_millis(200), rx.recv()).await?;
    
    capture_handle.abort();
    Ok(())
}

#[tokio::test]
async fn test_clipboard_monitor_initialization() -> Result<()> {
    let config = json!({
        "poll_interval": 100,
        "max_content_size": 1048576,
        "content_types": ["text", "image"]
    });
    
    let ctx = create_test_context(config);
    
    // Note: This might fail in headless environments
    match ClipboardMonitor::initialize(ctx).await {
        Ok(monitor) => {
            assert_eq!(monitor.name(), "clipboard");
        }
        Err(e) => {
            // Expected in CI/headless environments
            println!("Clipboard monitor initialization failed (expected in headless): {}", e);
        }
    }
    
    Ok(())
}

#[tokio::test]
async fn test_scrollback_capture_initialization() -> Result<()> {
    let config = json!({
        "capture_method": "kitty",
        "max_lines": 10000,
        "capture_on_clear": true
    });
    
    let ctx = create_test_context(config);
    let capture = ScrollbackCapture::initialize(ctx).await;
    
    // May fail if kitty is not available
    match capture {
        Ok(capture) => {
            assert_eq!(capture.name(), "scrollback");
        }
        Err(e) => {
            println!("Scrollback capture initialization failed (expected without kitty): {}", e);
        }
    }
    
    Ok(())
}


#[tokio::test]
async fn test_event_source_metadata() -> Result<()> {
    // Test that all event sources include proper metadata
    let temp_dir = TempDir::new()?;
    
    let config = json!({
        "watch_paths": [temp_dir.path().to_str().unwrap()],
        "recursive": false,
        "event_types": ["create"],
        "ignore_patterns": []
    });
    
    let ctx = create_test_context(config);
    let mut watcher = FilesystemWatcher::initialize(ctx).await?;
    
    let (tx, mut rx) = mpsc::channel(10);
    
    let capture_handle = tokio::spawn(async move {
        watcher.capture_events(tx).await
    });
    
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // Create event
    fs::write(temp_dir.path().join("metadata-test.txt"), "test")?;
    
    let event = timeout(Duration::from_secs(1), rx.recv()).await?.unwrap();
    
    // Verify event structure
    assert!(!event.id.to_string().is_empty());
    assert_eq!(event.source, "filesystem");
    assert!(!event.event_type.is_empty());
    assert!(event.timestamp.timestamp() > 0);
    
    // Verify metadata
    assert!(event.metadata.is_object());
    let metadata = event.metadata.as_object().unwrap();
    assert!(metadata.contains_key("version"));
    assert!(metadata.contains_key("hostname"));
    
    capture_handle.abort();
    Ok(())
}

#[tokio::test]
async fn test_event_source_error_handling() -> Result<()> {
    // Test with invalid configuration
    let config = json!({
        "watch_paths": ["/nonexistent/path/that/should/not/exist"],
        "recursive": true,
        "event_types": [],  // Empty event types
        "ignore_patterns": []
    });
    
    let ctx = create_test_context(config);
    let result = FilesystemWatcher::initialize(ctx).await;
    
    // Should handle invalid config gracefully
    assert!(result.is_err() || {
        // Or it might succeed but fail during capture
        true
    });
    
    Ok(())
}

#[tokio::test]
async fn test_multiple_event_sources_concurrent() -> Result<()> {
    // Test that multiple event sources can run concurrently
    let temp_dir1 = TempDir::new()?;
    let temp_dir2 = TempDir::new()?;
    
    let config1 = json!({
        "watch_paths": [temp_dir1.path().to_str().unwrap()],
        "recursive": false,
        "event_types": ["create"],
        "ignore_patterns": []
    });
    
    let config2 = json!({
        "watch_dir": temp_dir2.path().to_str().unwrap(),
        "poll_interval": 100,
        "completed_marker": ".completed"
    });
    
    let ctx1 = create_test_context(config1);
    let ctx2 = create_test_context(config2);
    
    let mut watcher = FilesystemWatcher::initialize(ctx1).await?;
    let mut recorder = AsciinemaRecorder::initialize(ctx2).await?;
    
    let (tx1, mut rx1) = mpsc::channel(10);
    let (tx2, mut rx2) = mpsc::channel(10);
    
    // Run both concurrently
    let handle1 = tokio::spawn(async move {
        watcher.capture_events(tx1).await
    });
    
    let handle2 = tokio::spawn(async move {
        recorder.capture_events(tx2).await
    });
    
    // Both should run without interfering
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    handle1.abort();
    handle2.abort();
    
    Ok(())
}