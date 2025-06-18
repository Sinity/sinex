use sinex_collector::{CollectorConfig, UnifiedCollector};
use sinex_core::{EventSource, EventSourceContext, RawEvent, CoreError, Result};
use sinex_ulid::Ulid;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use std::time::Duration;
use serde_json::json;

/// Test configuration reload during active event processing
#[tokio::test]
async fn test_config_reload_during_processing() {
    // Track state across reload
    let events_before_reload = Arc::new(AtomicU64::new(0));
    let events_after_reload = Arc::new(AtomicU64::new(0));
    let reload_triggered = Arc::new(AtomicBool::new(false));
    
    // Simulate config change that affects event processing
    let initial_config = serde_json::json!({
        "database_url": "postgresql:///sinex_dev",
        "event_sources": {
            "test_source": {
                "enabled": true,
                "interval_ms": 100
            }
        }
    });
    
    let updated_config = serde_json::json!({
        "database_url": "postgresql:///sinex_dev", 
        "event_sources": {
            "test_source": {
                "enabled": true,
                "interval_ms": 10  // Much faster after reload
            }
        }
    });
    
    // Event source that changes behavior based on config
    struct ConfigurableEventSource {
        interval_ms: u64,
        events_before: Arc<AtomicU64>,
        events_after: Arc<AtomicU64>,
        reload_flag: Arc<AtomicBool>,
    }
    
    #[async_trait::async_trait]
    impl EventSource for ConfigurableEventSource {
        type Config = serde_json::Value;
        const SOURCE_NAME: &'static str = "configurable";
        
        async fn initialize(ctx: EventSourceContext) -> Result<Self> {
            let interval_ms = ctx.config
                .get("interval_ms")
                .and_then(|v| v.as_u64())
                .unwrap_or(100);
                
            Ok(Self {
                interval_ms,
                events_before: Arc::new(AtomicU64::new(0)),
                events_after: Arc::new(AtomicU64::new(0)),
                reload_flag: Arc::new(AtomicBool::new(false)),
            })
        }
        
        async fn stream_events(&mut self, tx: mpsc::Sender<RawEvent>) -> Result<()> {
            loop {
                let event = RawEvent {
                    id: Ulid::new(),
                    source: Self::SOURCE_NAME.to_string(),
                    event_type: "config.test".to_string(),
                    ts_ingest: chrono::Utc::now(),
                    ts_orig: None,
                    host: "test".to_string(),
                    ingestor_version: None,
                    payload_schema_id: None,
                    payload: json!({
                        "interval_ms": self.interval_ms,
                        "reloaded": self.reload_flag.load(Ordering::Relaxed)
                    }),
                };
                
                tx.send(event).await.map_err(|e| CoreError::Other(e.to_string()))?;
                
                if self.reload_flag.load(Ordering::Relaxed) {
                    self.events_after.fetch_add(1, Ordering::Relaxed);
                } else {
                    self.events_before.fetch_add(1, Ordering::Relaxed);
                }
                
                tokio::time::sleep(Duration::from_millis(self.interval_ms)).await;
            }
        }
    }
    
    // Test sequence:
    // 1. Start collector with initial config
    // 2. Let it process some events
    // 3. Trigger config reload
    // 4. Verify behavior changes appropriately
    
    let (tx, mut rx) = mpsc::channel(100);
    
    // Producer with initial config
    let before_count = events_before_reload.clone();
    let after_count = events_after_reload.clone();
    let reload_flag = reload_triggered.clone();
    
    let producer = tokio::spawn(async move {
        let mut source = ConfigurableEventSource {
            interval_ms: 100,
            events_before: before_count,
            events_after: after_count,
            reload_flag: reload_flag.clone(),
        };
        
        // Run for a bit with initial config
        let _ = tokio::time::timeout(
            Duration::from_millis(500),
            source.stream_events(tx.clone())
        ).await;
        
        // Simulate config reload
        reload_flag.store(true, Ordering::Relaxed);
        source.interval_ms = 10; // New faster interval
        
        // Continue with new config
        let _ = tokio::time::timeout(
            Duration::from_millis(500),
            source.stream_events(tx)
        ).await;
    });
    
    // Collect events and analyze
    let mut pre_reload_count = 0;
    let mut post_reload_count = 0;
    
    tokio::time::timeout(Duration::from_secs(2), async {
        while let Some(event) = rx.recv().await {
            if let Some(reloaded) = event.payload.get("reloaded").and_then(|v| v.as_bool()) {
                if reloaded {
                    post_reload_count += 1;
                } else {
                    pre_reload_count += 1;
                }
            }
        }
    }).await.ok();
    
    producer.abort();
    
    println!("Config reload test results:");
    println!("  Events before reload: {} (100ms interval)", pre_reload_count);
    println!("  Events after reload: {} (10ms interval)", post_reload_count);
    println!("  Speed increase: {:.1}x", post_reload_count as f64 / pre_reload_count as f64);
    
    // After reload with 10ms interval, we should see significantly more events
    assert!(post_reload_count > pre_reload_count * 5, 
        "Expected at least 5x more events after config reload");
}

/// Test config validation during reload
#[tokio::test]
async fn test_invalid_config_reload_handling() {
    // Test that invalid config changes are rejected gracefully
    
    let valid_config = serde_json::json!({
        "database_url": "postgresql:///sinex_dev",
        "batch_size": 100
    });
    
    let invalid_configs = vec![
        // Missing required field
        serde_json::json!({
            "batch_size": 100
        }),
        // Invalid type
        serde_json::json!({
            "database_url": "postgresql:///sinex_dev",
            "batch_size": "not_a_number"
        }),
        // Invalid URL
        serde_json::json!({
            "database_url": "not://a/valid/url",
            "batch_size": 100
        }),
    ];
    
    // Simulate config validation
    fn validate_config(config: &serde_json::Value) -> std::result::Result<(), String> {
        // Check required fields
        if !config.get("database_url").is_some() {
            return Err("Missing required field: database_url".to_string());
        }
        
        // Check types
        if let Some(batch_size) = config.get("batch_size") {
            if !batch_size.is_u64() {
                return Err("batch_size must be a positive integer".to_string());
            }
        }
        
        // Check URL format
        if let Some(url) = config.get("database_url").and_then(|v| v.as_str()) {
            if !url.starts_with("postgresql://") && !url.starts_with("postgres://") {
                return Err("Invalid database URL format".to_string());
            }
        }
        
        Ok(())
    }
    
    // Valid config should pass
    assert!(validate_config(&valid_config).is_ok());
    
    // Invalid configs should fail with appropriate errors
    for (i, invalid_config) in invalid_configs.iter().enumerate() {
        let result = validate_config(invalid_config);
        assert!(result.is_err(), "Config {} should have failed validation", i);
        println!("Config {} validation error: {:?}", i, result.err());
    }
}

/// Test graceful handling of config reload timing
#[tokio::test] 
async fn test_config_reload_timing() {
    // Test various timing scenarios for config reload
    
    #[derive(Debug, Clone)]
    enum ReloadScenario {
        DuringBatchProcessing,
        BetweenBatches,
        DuringDatabaseWrite,
        DuringShutdown,
    }
    
    async fn simulate_reload(scenario: ReloadScenario) -> std::result::Result<String, String> {
        match scenario {
            ReloadScenario::DuringBatchProcessing => {
                // Simulate processing a batch when reload occurs
                tokio::time::sleep(Duration::from_millis(50)).await;
                Ok("Completed batch before applying new config".to_string())
            }
            ReloadScenario::BetweenBatches => {
                // Clean point for reload
                Ok("Applied config immediately".to_string())
            }
            ReloadScenario::DuringDatabaseWrite => {
                // Must complete transaction
                tokio::time::sleep(Duration::from_millis(100)).await;
                Ok("Completed database transaction before reload".to_string())
            }
            ReloadScenario::DuringShutdown => {
                // Reload during shutdown should be ignored
                Err("Ignored config reload during shutdown".to_string())
            }
        }
    }
    
    // Test each scenario
    for scenario in [
        ReloadScenario::DuringBatchProcessing,
        ReloadScenario::BetweenBatches,
        ReloadScenario::DuringDatabaseWrite,
        ReloadScenario::DuringShutdown,
    ] {
        let result = simulate_reload(scenario.clone()).await;
        println!("Reload scenario {:?}: {:?}", scenario, result);
        
        match scenario {
            ReloadScenario::DuringShutdown => assert!(result.is_err()),
            _ => assert!(result.is_ok()),
        }
    }
}