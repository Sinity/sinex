use anyhow::Result;
use sinex_shared::event_sink::DatabaseSink;
use sinex_shared::DatabaseService;
use sinex_shared::ingestor_framework::{Ingestor, IngestorConfig};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{sleep, timeout};
use sinex_unified_collector::{UnifiedIngestor, UnifiedConfig, CollectionConfig, DatabaseConfig, LoggingConfig};

// Import test database utilities
use crate::test_setup::get_test_db;

/// End-to-end test that verifies the unified collector works with a real database
#[tokio::test]
async fn test_unified_collector_full_pipeline() -> Result<()> {
    // Setup test database
    let pool = get_test_db().await;
    
    // Create configuration with test database
    let config = UnifiedConfig {
        database: DatabaseConfig {
            url: "postgresql://test".to_string(),
            max_connections: 5,
            connection_timeout_secs: 10,
        },
        logging: LoggingConfig {
            level: "debug".to_string(),
            format: "json".to_string(),
        },
        collection: CollectionConfig {
            enabled_sources: vec![
                "system".to_string(),
                "network".to_string(),
                "process".to_string(),
            ],
            poll_interval_secs: 1, // Fast polling for tests
            batch_size: 10,
            batch_timeout_ms: 100,
            heartbeat_interval_secs: 10,
        },
    };
    
    // Create database event sink
    let db_service = Arc::new(DatabaseService::from_pool(pool.as_ref().clone()));
    let event_sink = Arc::new(DatabaseSink::new(db_service));
    
    // Create and start the ingestor
    let mut ingestor = UnifiedIngestor::new(config, event_sink).await?;
    
    // Run the ingestor for a few seconds
    let ingestor_task = tokio::spawn(async move {
        let _ = timeout(Duration::from_secs(5), ingestor.run()).await;
    });
    
    // Wait for events to be collected
    sleep(Duration::from_secs(4)).await;
    
    // Query the database for events
    let rows = sqlx::query!(
        r#"
        SELECT source, event_type, COUNT(*) as count
        FROM raw.events
        WHERE source IN ('system', 'network', 'process')
        GROUP BY source, event_type
        ORDER BY source, event_type
        "#
    )
    .fetch_all(pool.as_ref())
    .await?;
    
    // Verify we have events from all sources
    let sources: Vec<String> = rows.iter()
        .map(|r| r.source.clone())
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();
    
    assert!(sources.contains(&"system".to_string()), "Missing system events");
    assert!(sources.contains(&"network".to_string()), "Missing network events");
    assert!(sources.contains(&"process".to_string()), "Missing process events");
    
    // Verify event types
    for row in &rows {
        match row.source.as_str() {
            "system" => {
                assert!(
                    row.event_type == "cpu.usage" || row.event_type == "memory.usage",
                    "Unexpected system event type: {}",
                    row.event_type
                );
            }
            "network" => {
                assert_eq!(row.event_type, "interface.stats");
            }
            "process" => {
                assert_eq!(row.event_type, "snapshot");
            }
            _ => panic!("Unexpected source: {}", row.source),
        }
        
        // Should have at least 2 events of each type (given 4 second runtime)
        assert!(row.count.unwrap_or(0) >= 2, "Too few events for {}/{}", row.source, row.event_type);
    }
    
    // Wait for ingestor to finish
    ingestor_task.await?;
    
    Ok(())
}

/// Test that verifies proper heartbeat registration and updates
#[tokio::test]
async fn test_unified_collector_heartbeat_integration() -> Result<()> {
    let pool = get_test_db().await;
    
    let config = UnifiedConfig {
        database: DatabaseConfig {
            url: "postgresql://test".to_string(),
            max_connections: 5,
            connection_timeout_secs: 10,
        },
        logging: LoggingConfig {
            level: "info".to_string(),
            format: "json".to_string(),
        },
        collection: CollectionConfig {
            enabled_sources: vec!["system".to_string()],
            poll_interval_secs: 5, // Slower polling
            batch_size: 10,
            batch_timeout_ms: 100,
            heartbeat_interval_secs: 2, // Fast heartbeat for testing
        },
    };
    
    let db_service = Arc::new(DatabaseService::from_pool(pool.as_ref().clone()));
    let event_sink = Arc::new(DatabaseSink::new(db_service));
    
    let mut ingestor = UnifiedIngestor::new(config, event_sink).await?;
    
    // Run for long enough to see multiple heartbeats
    let ingestor_task = tokio::spawn(async move {
        let _ = timeout(Duration::from_secs(5), ingestor.run()).await;
    });
    
    sleep(Duration::from_secs(4)).await;
    
    // Check agent manifest was created
    let manifest = sqlx::query!(
        r#"
        SELECT agent_name, agent_name as agent_id, version, last_heartbeat_ts as last_heartbeat_at
        FROM sinex_schemas.agent_manifests
        WHERE agent_name = 'unified-collector'
        "#
    )
    .fetch_one(pool.as_ref())
    .await?;
    
    assert_eq!(manifest.agent_name, "unified-collector");
    assert!(!manifest.version.is_empty());
    assert!(manifest.last_heartbeat_at.is_some());
    
    // Check that heartbeat events were recorded
    let heartbeat_count = sqlx::query!(
        r#"
        SELECT COUNT(*) as count
        FROM raw.events
        WHERE source = 'sinex.agent' AND event_type = 'heartbeat'
        "#
    )
    .fetch_one(pool.as_ref())
    .await?;
    
    // With 2 second heartbeat interval over 4 seconds, expect at least 2 heartbeats
    assert!(heartbeat_count.count.unwrap_or(0) >= 2);
    
    ingestor_task.await?;
    Ok(())
}

/// Test configuration override behavior
#[tokio::test]
async fn test_unified_collector_config_override() -> Result<()> {
    use std::env;
    use tempfile::NamedTempFile;
    use std::io::Write;
    
    // Create temporary config file
    let mut temp_file = NamedTempFile::new()?;
    writeln!(
        temp_file,
        r#"
[database]
url = "postgresql://config_file_db"
max_connections = 15
connection_timeout_secs = 20

[logging]
level = "warn"
format = "compact"

[collection]
enabled_sources = ["system"]
poll_interval_secs = 30
batch_size = 100
batch_timeout_ms = 1000
heartbeat_interval_secs = 300
"#
    )?;
    temp_file.flush()?;
    
    // Set environment to use config file
    env::set_var("UNIFIED_CONFIG", temp_file.path());
    
    // Load config - this would normally happen in the ingestor
    let config = UnifiedConfig::load()?;
    
    // Verify config was loaded from file
    assert_eq!(config.database.max_connections, 15);
    assert_eq!(config.logging.level, "warn");
    assert_eq!(config.collection.poll_interval_secs, 30);
    
    // Clean up
    env::remove_var("UNIFIED_CONFIG");
    
    Ok(())
}

/// Test concurrent execution of multiple unified collectors
#[tokio::test]
async fn test_multiple_unified_collectors() -> Result<()> {
    let pool = get_test_db().await;
    
    // Create two collectors with different configurations
    let config1 = UnifiedConfig {
        database: DatabaseConfig {
            url: "postgresql://test".to_string(),
            max_connections: 3,
            connection_timeout_secs: 10,
        },
        logging: LoggingConfig {
            level: "info".to_string(),
            format: "json".to_string(),
        },
        collection: CollectionConfig {
            enabled_sources: vec!["system".to_string()],
            poll_interval_secs: 1,
            batch_size: 5,
            batch_timeout_ms: 100,
            heartbeat_interval_secs: 30,
        },
    };
    
    let config2 = UnifiedConfig {
        database: DatabaseConfig {
            url: "postgresql://test".to_string(),
            max_connections: 3,
            connection_timeout_secs: 10,
        },
        logging: LoggingConfig {
            level: "info".to_string(),
            format: "json".to_string(),
        },
        collection: CollectionConfig {
            enabled_sources: vec!["network".to_string(), "process".to_string()],
            poll_interval_secs: 1,
            batch_size: 5,
            batch_timeout_ms: 100,
            heartbeat_interval_secs: 30,
        },
    };
    
    // Create two ingestors
    let db_service1 = Arc::new(DatabaseService::from_pool(pool.as_ref().clone()));
    let db_service2 = Arc::new(DatabaseService::from_pool(pool.as_ref().clone()));
    let sink1 = Arc::new(DatabaseSink::new(db_service1));
    let sink2 = Arc::new(DatabaseSink::new(db_service2));
    
    let mut ingestor1 = UnifiedIngestor::new(config1, sink1).await?;
    let mut ingestor2 = UnifiedIngestor::new(config2, sink2).await?;
    
    // Run both concurrently
    let task1 = tokio::spawn(async move {
        let _ = timeout(Duration::from_secs(3), ingestor1.run()).await;
    });
    
    let task2 = tokio::spawn(async move {
        let _ = timeout(Duration::from_secs(3), ingestor2.run()).await;
    });
    
    sleep(Duration::from_secs(2)).await;
    
    // Query for events from all sources
    let event_counts = sqlx::query!(
        r#"
        SELECT source, COUNT(*) as count
        FROM raw.events
        WHERE source IN ('system', 'network', 'process')
        GROUP BY source
        ORDER BY source
        "#
    )
    .fetch_all(pool.as_ref())
    .await?;
    
    // Verify we have events from all configured sources
    let source_map: std::collections::HashMap<String, i64> = event_counts
        .into_iter()
        .map(|r| (r.source, r.count.unwrap_or(0)))
        .collect();
    
    assert!(source_map.get("system").copied().unwrap_or(0) > 0);
    assert!(source_map.get("network").copied().unwrap_or(0) > 0);
    assert!(source_map.get("process").copied().unwrap_or(0) > 0);
    
    // Wait for tasks to complete
    task1.await?;
    task2.await?;
    
    Ok(())
}

/// Test error recovery and resilience
#[tokio::test]
async fn test_unified_collector_error_recovery() -> Result<()> {
    let pool = get_test_db().await;
    
    // Configure with a mix of valid and invalid sources
    let config = UnifiedConfig {
        database: DatabaseConfig {
            url: "postgresql://test".to_string(),
            max_connections: 5,
            connection_timeout_secs: 10,
        },
        logging: LoggingConfig {
            level: "debug".to_string(),
            format: "json".to_string(),
        },
        collection: CollectionConfig {
            enabled_sources: vec![
                "system".to_string(),
                "invalid_source".to_string(), // This should be handled gracefully
                "network".to_string(),
            ],
            poll_interval_secs: 1,
            batch_size: 10,
            batch_timeout_ms: 100,
            heartbeat_interval_secs: 30,
        },
    };
    
    let db_service = Arc::new(DatabaseService::from_pool(pool.as_ref().clone()));
    let event_sink = Arc::new(DatabaseSink::new(db_service));
    
    let mut ingestor = UnifiedIngestor::new(config, event_sink).await?;
    
    // Run and verify it doesn't crash due to invalid source
    let ingestor_task = tokio::spawn(async move {
        let _ = timeout(Duration::from_secs(3), ingestor.run()).await;
    });
    
    sleep(Duration::from_secs(2)).await;
    
    // Should still have events from valid sources
    let valid_events = sqlx::query!(
        r#"
        SELECT COUNT(*) as count
        FROM raw.events
        WHERE source IN ('system', 'network')
        "#
    )
    .fetch_one(pool.as_ref())
    .await?;
    
    assert!(valid_events.count.unwrap_or(0) > 0);
    
    // Should not have events from invalid source
    let invalid_events = sqlx::query!(
        r#"
        SELECT COUNT(*) as count
        FROM raw.events
        WHERE source = 'invalid_source'
        "#
    )
    .fetch_one(pool.as_ref())
    .await?;
    
    assert_eq!(invalid_events.count.unwrap_or(0), 0);
    
    ingestor_task.await?;
    Ok(())
}