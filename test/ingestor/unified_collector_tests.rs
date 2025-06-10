use anyhow::Result;
use sinex_shared::{IngestorRuntime, RuntimeConfig, SimpleIngestor};
use sinex_shared::ingestor_framework::IngestorConfig;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::timeout;

// Re-export the unified collector types for testing
use sinex_unified_collector::{CollectionConfig, DatabaseConfig, LoggingConfig, UnifiedCollector, UnifiedConfig};

/// Test helper to create a default test config
fn create_test_config() -> UnifiedConfig {
    UnifiedConfig {
        database: DatabaseConfig {
            url: "postgresql://test".to_string(),
            max_connections: 5,
            connection_timeout_secs: 5,
        },
        logging: LoggingConfig {
            level: "debug".to_string(),
            format: "json".to_string(),
        },
        collection: CollectionConfig {
            enabled_sources: vec!["system".to_string(), "network".to_string()],
            poll_interval_secs: 1, // Fast for tests
            batch_size: 10,
            batch_timeout_ms: 100,
            heartbeat_interval_secs: 30,
        },
    }
}

#[tokio::test]
async fn test_unified_collector_initialization() -> Result<()> {
    let config = create_test_config();
    let _collector = UnifiedCollector::new(config.clone());
    
    // Verify the collector was created with the right config
    assert_eq!(UnifiedCollector::name(), "unified-collector");
    assert!(!UnifiedCollector::version().is_empty());
    
    Ok(())
}

#[tokio::test]
async fn test_event_source_selection() -> Result<()> {
    let mut config = create_test_config();
    
    // Test with only system events enabled
    config.collection.enabled_sources = vec!["system".to_string()];
    let mut collector = UnifiedCollector::new(config.clone());
    
    let (tx, mut rx) = mpsc::channel(100);
    
    // Run collection for a short time
    let capture_task = tokio::spawn(async move {
        let _ = timeout(
            Duration::from_secs(2),
            collector.capture_events(tx),
        )
        .await;
    });
    
    // Collect events
    let mut events = Vec::new();
    while let Ok(event) = rx.try_recv() {
        events.push(event);
    }
    
    capture_task.await?;
    
    // Should have system events only
    assert!(!events.is_empty());
    assert!(events.iter().all(|e| e.source == "system"));
    
    Ok(())
}

#[tokio::test]
async fn test_multiple_concurrent_sources() -> Result<()> {
    let config = create_test_config();
    let mut collector = UnifiedCollector::new(config.clone());
    
    let (tx, mut rx) = mpsc::channel(100);
    
    // Run collection for a short time
    let capture_task = tokio::spawn(async move {
        let _ = timeout(
            Duration::from_secs(2),
            collector.capture_events(tx),
        )
        .await;
    });
    
    // Collect events
    let mut system_events = 0;
    let mut network_events = 0;
    
    tokio::time::sleep(Duration::from_millis(1500)).await;
    
    while let Ok(event) = rx.try_recv() {
        match event.source.as_str() {
            "system" => system_events += 1,
            "network" => network_events += 1,
            _ => {}
        }
    }
    
    capture_task.await?;
    
    // Should have events from both sources
    assert!(system_events > 0);
    assert!(network_events > 0);
    
    Ok(())
}

#[tokio::test]
async fn test_event_streaming_rate() -> Result<()> {
    let mut config = create_test_config();
    config.collection.poll_interval_secs = 1; // 1 second interval
    
    let mut collector = UnifiedCollector::new(config.clone());
    let (tx, mut rx) = mpsc::channel(100);
    
    // Run for 3 seconds
    let capture_task = tokio::spawn(async move {
        let _ = timeout(
            Duration::from_secs(3),
            collector.capture_events(tx),
        )
        .await;
    });
    
    tokio::time::sleep(Duration::from_millis(3500)).await;
    
    let mut event_count = 0;
    while let Ok(_) = rx.try_recv() {
        event_count += 1;
    }
    
    capture_task.await?;
    
    // With 1 second interval over 3 seconds, we should have at least 6 events
    // (2 sources * 3 intervals)
    assert!(event_count >= 6, "Expected at least 6 events, got {}", event_count);
    
    Ok(())
}

#[tokio::test]
async fn test_configuration_loading() -> Result<()> {
    use std::fs;
    use tempfile::TempDir;
    
    // Create a temporary directory for test config
    let temp_dir = TempDir::new()?;
    let config_path = temp_dir.path().join("test_unified.toml");
    
    // Write test configuration
    let test_config = r#"
[database]
url = "postgresql://custom_test"
max_connections = 20
connection_timeout_secs = 15

[logging]
level = "trace"
format = "compact"

[collection]
enabled_sources = ["system", "network", "process"]
poll_interval_secs = 10
batch_size = 50
batch_timeout_ms = 500
heartbeat_interval_secs = 120
"#;
    
    fs::write(&config_path, test_config)?;
    
    // Load configuration from file
    let loaded_config = UnifiedConfig::load_from_file(&config_path)?;
    
    // Verify loaded values
    assert_eq!(loaded_config.database.url, "postgresql://custom_test");
    assert_eq!(loaded_config.database.max_connections, 20);
    assert_eq!(loaded_config.logging.level, "trace");
    assert_eq!(loaded_config.collection.enabled_sources.len(), 3);
    assert_eq!(loaded_config.collection.poll_interval_secs, 10);
    
    Ok(())
}

#[tokio::test]
async fn test_configuration_merging() -> Result<()> {
    let mut config = create_test_config();
    
    // Test database URL override
    let original_url = config.database.url.clone();
    config.set_database_url("postgresql://override".to_string());
    assert_eq!(config.database_url(), "postgresql://override");
    assert_ne!(config.database_url(), original_url);
    
    // Test log level override
    config.set_log_level("error".to_string());
    assert_eq!(config.log_level(), "error");
    
    Ok(())
}

#[tokio::test]
async fn test_dry_run_mode() -> Result<()> {
    use sinex_shared::event_sink::MemorySink;
    
    let config = create_test_config();
    let collector = UnifiedCollector::new(config.clone());
    
    // Create a memory sink for dry-run mode
    let sink = Arc::new(MemorySink::new());
    
    let runtime_config = RuntimeConfig {
        heartbeat_interval_secs: 30,
        batch_size: Some(10),
        batch_timeout_ms: Some(100),
        ..Default::default()
    };
    
    let runtime = IngestorRuntime::new(collector, sink.clone(), runtime_config)?;
    
    // Run for a short time
    let runtime_task = tokio::spawn(async move {
        let _ = timeout(Duration::from_secs(2), runtime.run()).await;
    });
    
    tokio::time::sleep(Duration::from_millis(2500)).await;
    runtime_task.await?;
    
    // Check that events were captured in memory
    let events = sink.get_events().await;
    assert!(!events.is_empty(), "Should have captured events in dry-run mode");
    
    // Verify event structure
    for event in &events {
        assert!(!event.source.is_empty());
        assert!(!event.event_type.is_empty());
        assert!(event.payload.is_object());
    }
    
    Ok(())
}

#[tokio::test]
async fn test_error_handling_in_event_collection() -> Result<()> {
    // This test verifies that errors in one source don't affect others
    let mut config = create_test_config();
    config.collection.enabled_sources = vec![
        "system".to_string(),
        "unknown_source".to_string(), // This should log a debug message
        "network".to_string(),
    ];
    
    let mut collector = UnifiedCollector::new(config);
    let (tx, mut rx) = mpsc::channel(100);
    
    // Run collection
    let capture_task = tokio::spawn(async move {
        let _ = timeout(
            Duration::from_secs(2),
            collector.capture_events(tx),
        )
        .await;
    });
    
    tokio::time::sleep(Duration::from_millis(1500)).await;
    
    let mut sources = std::collections::HashSet::new();
    while let Ok(event) = rx.try_recv() {
        sources.insert(event.source.clone());
    }
    
    capture_task.await?;
    
    // Should still have events from valid sources
    assert!(sources.contains("system"));
    assert!(sources.contains("network"));
    assert!(!sources.contains("unknown_source"));
    
    Ok(())
}

#[tokio::test]
async fn test_event_payload_structure() -> Result<()> {
    let config = create_test_config();
    let mut collector = UnifiedCollector::new(config);
    
    let (tx, mut rx) = mpsc::channel(100);
    
    // Collect one batch of events
    let capture_task = tokio::spawn(async move {
        let _ = timeout(
            Duration::from_millis(1500),
            collector.capture_events(tx),
        )
        .await;
    });
    
    tokio::time::sleep(Duration::from_millis(1200)).await;
    capture_task.await?;
    
    // Verify event payloads have expected structure
    while let Ok(event) = rx.try_recv() {
        match (event.source.as_str(), event.event_type.as_str()) {
            ("system", "cpu.usage") => {
                assert!(event.payload["usage_percent"].is_number());
                assert!(event.payload["timestamp"].is_string());
            }
            ("system", "memory.usage") => {
                assert!(event.payload["total"].is_number());
                assert!(event.payload["used"].is_number());
                assert!(event.payload["free"].is_number());
            }
            ("network", "interface.stats") => {
                assert!(event.payload["interface"].is_string());
                assert!(event.payload["rx_bytes"].is_number());
                assert!(event.payload["tx_bytes"].is_number());
            }
            ("process", "snapshot") => {
                assert!(event.payload["process_count"].is_number());
                assert!(event.payload["processes"].is_array());
            }
            _ => {}
        }
    }
    
    Ok(())
}

#[tokio::test]
async fn test_runtime_integration() -> Result<()> {
    use sinex_shared::event_sink::LogSink;
    
    let config = create_test_config();
    let collector = UnifiedCollector::new(config.clone());
    
    // Use log sink for this test
    let sink = Arc::new(LogSink::new("test"));
    
    let runtime_config = RuntimeConfig {
        heartbeat_interval_secs: 5, // Fast heartbeat for testing
        batch_size: Some(5),
        batch_timeout_ms: Some(200),
        ..Default::default()
    };
    
    let runtime = IngestorRuntime::new(collector, sink, runtime_config)?;
    
    // Run for a short time to ensure it starts properly
    let runtime_task = tokio::spawn(async move {
        let _ = timeout(Duration::from_secs(3), runtime.run()).await;
        Ok::<(), anyhow::Error>(())
    });
    
    // Wait and ensure no panics
    let result = runtime_task.await?;
    assert!(result.is_ok());
    
    Ok(())
}