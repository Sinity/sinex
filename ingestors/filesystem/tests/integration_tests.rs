use anyhow::Result;
use sinex_shared::{IngestorRuntime, RuntimeConfig, MemorySink, sources, event_type_constants};
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::time::{sleep, Duration};

use filesystem_ingestor::{FilesystemConfig, FilesystemIngestor};

#[tokio::test]
async fn test_filesystem_ingestor_captures_file_events() -> Result<()> {
    // Create a temporary directory to watch
    let temp_dir = TempDir::new()?;
    let watch_path = temp_dir.path().to_path_buf();
    
    // Configure the ingestor
    let config = FilesystemConfig {
        watch_directories: vec![watch_path.clone()],
        exclude_patterns: vec![],
        include_patterns: vec![],
        debounce_ms: 100,
        batch_size_events: 10,
        batch_timeout_ms: 500,
        hash_files: false,
        max_hash_size_bytes: 10 * 1024 * 1024,
        heartbeat_interval_secs: 60,
        max_retries: 3,
        retry_delay_secs: 5,
    };
    
    let ingestor = FilesystemIngestor::new(config);
    let event_sink = Arc::new(MemorySink::new());
    
    let runtime_config = RuntimeConfig {
        heartbeat_interval_secs: 60,
        batch_size: Some(10),
        batch_timeout_ms: Some(500),
        ..Default::default()
    };
    
    let runtime = IngestorRuntime::new(ingestor, Arc::clone(&event_sink), runtime_config)?;
    
    // Run the ingestor in the background
    let runtime_handle = tokio::spawn(async move {
        runtime.run().await
    });
    
    // Give it time to start watching
    sleep(Duration::from_millis(500)).await;
    
    // Create a test file
    let test_file = watch_path.join("test.txt");
    fs::write(&test_file, "Hello, world!")?;
    
    // Wait for the event to be processed
    sleep(Duration::from_millis(500)).await;
    
    // Modify the file
    fs::write(&test_file, "Hello, world! Updated")?;
    
    // Wait for the event
    sleep(Duration::from_millis(500)).await;
    
    // Delete the file
    fs::remove_file(&test_file)?;
    
    // Wait for the event
    sleep(Duration::from_millis(500)).await;
    
    // Stop the runtime
    runtime_handle.abort();
    
    // Check captured events
    let events = event_sink.get_events().await;
    
    // Filter filesystem events
    let fs_events: Vec<_> = events.iter()
        .filter(|e| e.source == sources::FILESYSTEM)
        .collect();
    
    // Should have captured create, modify, and delete events
    assert!(fs_events.len() >= 3, "Should have at least 3 filesystem events, got {}", fs_events.len());
    
    // Check event types
    let has_create = fs_events.iter().any(|e| 
        e.event_type == event_type_constants::filesystem::FILE_CREATED
    );
    let has_modify = fs_events.iter().any(|e| 
        e.event_type == event_type_constants::filesystem::FILE_MODIFIED
    );
    let has_delete = fs_events.iter().any(|e| 
        e.event_type == event_type_constants::filesystem::FILE_DELETED
    );
    
    assert!(has_create, "Should have file created event");
    assert!(has_modify, "Should have file modified event");
    assert!(has_delete, "Should have file deleted event");
    
    Ok(())
}

#[tokio::test]
async fn test_filesystem_ingestor_exclude_patterns() -> Result<()> {
    // Create a temporary directory to watch
    let temp_dir = TempDir::new()?;
    let watch_path = temp_dir.path().to_path_buf();
    
    // Configure the ingestor with exclusions
    let config = FilesystemConfig {
        watch_directories: vec![watch_path.clone()],
        exclude_patterns: vec!["*.tmp".to_string(), "*.log".to_string()],
        include_patterns: vec![],
        debounce_ms: 100,
        batch_size_events: 10,
        batch_timeout_ms: 500,
        hash_files: false,
        max_hash_size_bytes: 10 * 1024 * 1024,
        heartbeat_interval_secs: 60,
        max_retries: 3,
        retry_delay_secs: 5,
    };
    
    let ingestor = FilesystemIngestor::new(config);
    let event_sink = Arc::new(MemorySink::new());
    
    let runtime_config = RuntimeConfig::default();
    let runtime = IngestorRuntime::new(ingestor, Arc::clone(&event_sink), runtime_config)?;
    
    // Run the ingestor in the background
    let runtime_handle = tokio::spawn(async move {
        runtime.run().await
    });
    
    // Give it time to start watching
    sleep(Duration::from_millis(500)).await;
    
    // Create files that should be excluded
    fs::write(watch_path.join("test.tmp"), "temp file")?;
    fs::write(watch_path.join("test.log"), "log file")?;
    
    // Create a file that should be included
    fs::write(watch_path.join("test.txt"), "normal file")?;
    
    // Wait for events to be processed
    sleep(Duration::from_millis(500)).await;
    
    // Stop the runtime
    runtime_handle.abort();
    
    // Check captured events
    let events = event_sink.get_events().await;
    
    // Filter filesystem events
    let fs_events: Vec<_> = events.iter()
        .filter(|e| e.source == sources::FILESYSTEM)
        .collect();
    
    // Should only have events for test.txt, not the excluded files
    for event in &fs_events {
        if let Some(path) = event.payload.get("path").and_then(|v| v.as_str()) {
            assert!(!path.ends_with(".tmp"), "Should not have events for .tmp files");
            assert!(!path.ends_with(".log"), "Should not have events for .log files");
        }
    }
    
    // Should have at least one event for the .txt file
    let has_txt_event = fs_events.iter().any(|e| {
        e.payload.get("path")
            .and_then(|v| v.as_str())
            .map(|p| p.ends_with("test.txt"))
            .unwrap_or(false)
    });
    
    assert!(has_txt_event, "Should have event for test.txt file");
    
    Ok(())
}

#[tokio::test]
async fn test_filesystem_ingestor_heartbeats() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let watch_path = temp_dir.path().to_path_buf();
    
    let config = FilesystemConfig {
        watch_directories: vec![watch_path],
        exclude_patterns: vec![],
        include_patterns: vec![],
        debounce_ms: 100,
        batch_size_events: 10,
        batch_timeout_ms: 500,
        hash_files: false,
        max_hash_size_bytes: 10 * 1024 * 1024,
        heartbeat_interval_secs: 1, // Fast heartbeat for testing
        max_retries: 3,
        retry_delay_secs: 5,
    };
    
    let ingestor = FilesystemIngestor::new(config);
    let event_sink = Arc::new(MemorySink::new());
    
    let runtime_config = RuntimeConfig {
        heartbeat_interval_secs: 1, // Fast heartbeat
        ..Default::default()
    };
    
    let runtime = IngestorRuntime::new(ingestor, Arc::clone(&event_sink), runtime_config)?;
    
    // Run the ingestor in the background
    let runtime_handle = tokio::spawn(async move {
        runtime.run().await
    });
    
    // Wait for multiple heartbeats
    sleep(Duration::from_millis(2500)).await;
    
    // Stop the runtime
    runtime_handle.abort();
    
    // Check heartbeat events
    let events = event_sink.get_events().await;
    let heartbeats: Vec<_> = events.iter()
        .filter(|e| e.source == sources::SINEX && e.event_type == "agent.heartbeat")
        .collect();
    
    assert!(heartbeats.len() >= 2, "Should have multiple heartbeats");
    
    // Verify heartbeat payload
    for heartbeat in &heartbeats {
        assert!(heartbeat.payload.get("agent_name").is_some());
        assert_eq!(
            heartbeat.payload.get("agent_name").and_then(|v| v.as_str()),
            Some("filesystem-ingestor")
        );
    }
    
    Ok(())
}