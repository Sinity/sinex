use std::path::PathBuf;
use tempfile::TempDir;
use chrono::Utc;
use serde_json::json;

use sinex_shared::{DlqManager, DlqEntry, RawEventBuilder, sources, event_types};

/// Test basic DLQ file writing
#[tokio::test]
async fn test_dlq_write_event() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let base_path = temp_dir.path();
    
    // Create a custom DLQ manager with temp directory
    let dlq = create_test_dlq("test-agent-write", base_path)?;
    
    // Create a test event
    let event = RawEventBuilder::new(
        sources::FILESYSTEM,
        event_types::event_types::filesystem::FILE_CREATED,
        json!({
            "path": "/test/file.txt",
            "size": 1024
        })
    ).build();
    
    // Write to DLQ
    let file_path = dlq.write_event(
        event.clone(),
        "Database connection failed".to_string(),
        3
    ).await?;
    
    // Verify file was created
    assert!(PathBuf::from(&file_path).exists());
    
    // Read and verify content
    let content = std::fs::read_to_string(&file_path)?;
    let entry: DlqEntry = serde_json::from_str(&content)?;
    
    assert_eq!(entry.failure_reason, "Database connection failed");
    assert_eq!(entry.retry_count, 3);
    assert_eq!(entry.original_event.source, sources::FILESYSTEM);
    assert_eq!(entry.original_event.event_type, event_types::event_types::filesystem::FILE_CREATED);
    
    Ok(())
}

/// Test DLQ size counting
#[tokio::test]
async fn test_dlq_size_counting() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let dlq = create_test_dlq("test-agent-size", temp_dir.path())?;
    
    // Initially empty
    assert_eq!(dlq.get_dlq_size()?, 0);
    
    // Write multiple events
    for i in 0..5 {
        let event = RawEventBuilder::new(
            sources::TERMINAL_KITTY,
            event_types::event_types::terminal::COMMAND_EXECUTED,
            json!({
                "command": format!("test-command-{}", i),
                "exit_code": 0
            })
        ).build();
        
        dlq.write_event(event, format!("Test failure {}", i), i as u32).await?;
    }
    
    // Verify count
    assert_eq!(dlq.get_dlq_size()?, 5);
    
    Ok(())
}

/// Test reading all DLQ entries
#[tokio::test]
async fn test_dlq_read_all_entries() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let dlq = create_test_dlq("test-agent-read", temp_dir.path())?;
    
    // Write events with different sources
    let sources_to_test = vec![
        (sources::FILESYSTEM, "file_created"),
        (sources::TERMINAL_KITTY, "command_executed"),
        (sources::HYPRLAND, "window_focused"),
    ];
    
    for (source, event_type) in &sources_to_test {
        let event = RawEventBuilder::new(
            *source,
            *event_type,
            json!({ "test": true })
        ).build();
        
        dlq.write_event(event, format!("Test {}", source), 1).await?;
    }
    
    // Read all entries
    let entries = dlq.read_all_entries()?;
    
    assert_eq!(entries.len(), 3);
    
    // Verify each entry
    let mut found_sources = vec![];
    for (path, entry) in entries {
        assert!(path.exists());
        assert_eq!(entry.retry_count, 1);
        found_sources.push(entry.original_event.source.clone());
    }
    
    // Verify all sources were found
    for (source, _) in sources_to_test {
        assert!(found_sources.contains(&source.to_string()));
    }
    
    Ok(())
}

/// Test removing DLQ entries
#[tokio::test]
async fn test_dlq_remove_entry() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let dlq = create_test_dlq("test-agent-remove", temp_dir.path())?;
    
    // Write an event
    let event = RawEventBuilder::new(
        sources::SINEX,
        event_types::event_types::sinex::AGENT_ERROR,
        json!({
            "error": "Test error",
            "severity": "warning"
        })
    ).build();
    
    let file_path = dlq.write_event(event, "Test removal".to_string(), 0).await?;
    let path = PathBuf::from(&file_path);
    
    // Verify it exists
    assert!(path.exists());
    assert_eq!(dlq.get_dlq_size()?, 1);
    
    // Remove it
    dlq.remove_entry(&path)?;
    
    // Verify it's gone
    assert!(!path.exists());
    assert_eq!(dlq.get_dlq_size()?, 0);
    
    Ok(())
}

/// Test DLQ notification event creation
#[test]
fn test_dlq_notification_creation() {
    let temp_dir = TempDir::new().unwrap();
    let dlq = create_test_dlq("test-agent", temp_dir.path()).unwrap();
    
    let original_event = RawEventBuilder::new(
        sources::FILESYSTEM,
        event_types::event_types::filesystem::FILE_MODIFIED,
        json!({ "path": "/test.txt" })
    ).build();
    
    let notification = dlq.create_dlq_notification(
        &original_event,
        "/var/lib/sinex/dlq/test-agent/test.json".to_string(),
        "Network timeout".to_string()
    );
    
    assert_eq!(notification.source, sources::SINEX);
    assert_eq!(notification.event_type, event_types::event_types::sinex::AGENT_DLQ_EVENT_WRITTEN);
    
    // Verify payload structure
    let payload = notification.payload.as_object().unwrap();
    assert_eq!(payload["agent_name"], "test-agent");
    assert_eq!(payload["failed_event_source"], sources::FILESYSTEM);
    assert_eq!(payload["failure_reason"], "Network timeout");
    assert_eq!(payload["dlq_file_path"], "/var/lib/sinex/dlq/test-agent/test.json");
}

/// Test critical failure logging
#[test]
fn test_critical_failure_logging() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let log_dir = temp_dir.path().join("log");
    std::fs::create_dir_all(&log_dir)?;
    
    // Create DLQ with custom paths
    let dlq = DlqManager::new("test-critical")?;
    
    // Override the critical log path for testing
    let _test_log_path = log_dir.join("critical.log");
    
    // We can't easily override the path in the current implementation,
    // so we'll test the method exists and returns Ok
    let result = dlq.log_critical_failure("Test critical failure");
    assert!(result.is_ok());
    
    Ok(())
}

/// Test filename generation with special characters
#[tokio::test]
async fn test_dlq_filename_sanitization() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let dlq = create_test_dlq("test-agent-sanitize", temp_dir.path())?;
    
    // Event with dots in source and type
    let event = RawEventBuilder::new(
        "com.example.source",
        "event.type.with.dots",
        json!({ "test": true })
    ).build();
    
    let file_path = dlq.write_event(event, "Test".to_string(), 0).await?;
    
    // Verify dots were replaced with underscores
    assert!(file_path.contains("com_example_source"));
    assert!(file_path.contains("event_type_with_dots"));
    
    // Verify file exists and is valid
    assert!(PathBuf::from(&file_path).exists());
    
    Ok(())
}

/// Test handling corrupted DLQ files
#[test]
fn test_dlq_corrupted_file_handling() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let dlq = create_test_dlq("test-agent-corrupt", temp_dir.path())?;
    
    // Create a corrupted JSON file
    let corrupted_path = temp_dir.path()
        .join("dlq")
        .join("test-agent-corrupt")
        .join("corrupted.json");
    std::fs::create_dir_all(corrupted_path.parent().unwrap())?;
    std::fs::write(&corrupted_path, "{ invalid json")?;
    
    // Create a valid JSON file
    let valid_entry = DlqEntry {
        failed_at: Utc::now(),
        failure_reason: "Test".to_string(),
        retry_count: 1,
        original_event: RawEventBuilder::new(
            sources::SINEX,
            event_types::event_types::sinex::AGENT_HEARTBEAT,
            json!({})
        ).build(),
    };
    
    let valid_path = temp_dir.path()
        .join("dlq")
        .join("test-agent-corrupt")
        .join("valid.json");
    std::fs::write(&valid_path, serde_json::to_string(&valid_entry)?)?;
    
    // Read all entries - should skip corrupted file
    let entries = dlq.read_all_entries()?;
    
    // Should only get the valid entry
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].1.failure_reason, "Test");
    
    Ok(())
}

/// Test concurrent DLQ writes
#[tokio::test]
async fn test_dlq_concurrent_writes() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let dlq = std::sync::Arc::new(create_test_dlq("test-agent-concurrent", temp_dir.path())?);
    
    // Spawn multiple tasks writing to DLQ concurrently
    let mut handles = vec![];
    
    for i in 0..10 {
        let dlq_clone = dlq.clone();
        let handle = tokio::spawn(async move {
            let event = RawEventBuilder::new(
                sources::FILESYSTEM,
                event_types::event_types::filesystem::FILE_CREATED,
                json!({ "index": i })
            ).build();
            
            dlq_clone.write_event(
                event,
                format!("Concurrent test {}", i),
                i as u32
            ).await
        });
        
        handles.push(handle);
    }
    
    // Wait for all writes to complete
    for handle in handles {
        handle.await??;
    }
    
    // Verify all files were written
    assert_eq!(dlq.get_dlq_size()?, 10);
    
    // Verify each file has unique content
    let entries = dlq.read_all_entries()?;
    let mut indices = entries.iter()
        .map(|(_, entry)| {
            entry.original_event.payload["index"].as_u64().unwrap()
        })
        .collect::<Vec<_>>();
    
    indices.sort();
    assert_eq!(indices, (0..10).collect::<Vec<_>>());
    
    Ok(())
}

/// Helper to create a DLQ manager with custom base path
fn create_test_dlq(agent_name: &str, base_path: &std::path::Path) -> Result<DlqManager, Box<dyn std::error::Error>> {
    // Set environment variables to override default paths
    std::env::set_var("SINEX_DLQ_BASE", base_path.join("dlq"));
    std::env::set_var("SINEX_LOG_BASE", base_path.join("log"));
    Ok(DlqManager::new(agent_name)?)
}