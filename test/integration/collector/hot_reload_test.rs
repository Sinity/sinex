use crate::common::prelude::*;
use sinex_collector::config::CollectorConfig;
use sinex_core::ConfigValue;
use std::sync::{Arc, atomic::{AtomicU32, AtomicBool, Ordering}};
use tokio::sync::{mpsc, Mutex};
use std::io::Write;

// Mock event source that can track configuration changes
struct ConfigurableEventSource {
    config_version: Arc<AtomicU32>,
    should_stop: Arc<AtomicBool>,
    event_interval_ms: Arc<AtomicU32>,
}

#[async_trait]
impl EventSource for ConfigurableEventSource {
    type Config = serde_json::Value;
    const SOURCE_NAME: &'static str = "configurable_source";
    
    async fn initialize(ctx: EventSourceContext) -> sinex_core::Result<Self> {
        let interval = ctx.config["event_interval_ms"]
            .as_u64()
            .unwrap_or(100) as u32;
        
        Ok(Self {
            config_version: Arc::new(AtomicU32::new(1)),
            should_stop: Arc::new(AtomicBool::new(false)),
            event_interval_ms: Arc::new(AtomicU32::new(interval)),
        })
    }
    
    async fn stream_events(&mut self, tx: mpsc::Sender<RawEvent>) -> sinex_core::Result<()> {
        let mut event_count = 0;
        
        while !self.should_stop.load(Ordering::Relaxed) {
            let interval = self.event_interval_ms.load(Ordering::Relaxed);
            
            let event = sinex_core::RawEventBuilder::new(
                Self::SOURCE_NAME,
                "config.test",
                json!({
                    "event_number": event_count,
                    "config_version": self.config_version.load(Ordering::Relaxed),
                    "interval_ms": interval,
                })
            ).build();
            
            if tx.send(event).await.is_err() {
                break;
            }
            
            event_count += 1;
            tokio::time::sleep(Duration::from_millis(interval as u64)).await;
        }
        
        Ok(())
    }
}

#[sinex_test]
async fn test_config_hot_reload_without_data_loss(ctx: TestContext) -> TestResult {
    // Create initial config file
    let config_file = NamedTempFile::new()?;
    let mut event_config = HashMap::new();
    event_config.insert(
        "configurable_source".to_string(),
        ConfigValue::Table({
            let mut table = toml::map::Map::new();
            table.insert("event_interval_ms".to_string(), ConfigValue::Integer(100));
            table
        })
    );
    
    let initial_config = CollectorConfig {
        enabled_events: vec!["config.test".to_string()],
        event: event_config,
        ..Default::default()
    };
    
    let config_str = toml::to_string(&initial_config)?;
    config_file.as_file().write_all(config_str.as_bytes())?;
    config_file.as_file().sync_all()?;
    
    // Track received events
    let received_events = Arc::new(Mutex::new(Vec::new()));
    let (tx, mut rx) = mpsc::channel::<RawEvent>(1000);
    
    // Start event receiver
    let events_clone = received_events.clone();
    let receiver_task = tokio::spawn(async move {
        while let Some(event) = rx.recv().await {
            events_clone.lock().await.push(event);
        }
    });
    
    // Simulate collector behavior - start source
    let source_ctx = EventSourceContext::new(json!({
        "event_interval_ms": 100
    }));
    let mut source = ConfigurableEventSource::initialize(source_ctx).await?;
    let should_stop = source.should_stop.clone();
    let config_version = source.config_version.clone();
    let event_interval = source.event_interval_ms.clone();
    
    // Start streaming events
    let tx_clone = tx.clone();
    let stream_task = tokio::spawn(async move {
        source.stream_events(tx_clone).await
    });
    
    // Let it run for a bit
    tokio::time::sleep(Duration::from_millis(350)).await;
    
    // Simulate config reload - update interval
    config_version.store(2, Ordering::Relaxed);
    event_interval.store(50, Ordering::Relaxed);
    
    // Continue running with new config
    tokio::time::sleep(Duration::from_millis(350)).await;
    
    // Stop source
    should_stop.store(true, Ordering::Relaxed);
    drop(tx);
    
    // Wait for tasks to complete
    let _ = stream_task.await?;
    let _ = receiver_task.await?;
    
    // Verify results
    let events = received_events.lock().await;
    
    // Should have events from both config versions
    let v1_events: Vec<_> = events.iter()
        .filter(|e| e.payload["config_version"] == 1)
        .collect();
    let v2_events: Vec<_> = events.iter()
        .filter(|e| e.payload["config_version"] == 2)
        .collect();
    
    assert!(!v1_events.is_empty(), "Should have events from config v1");
    assert!(!v2_events.is_empty(), "Should have events from config v2");
    
    // Events should have different intervals
    pretty_assertions::assert_eq!(v1_events[0].payload["interval_ms"], 100);
    pretty_assertions::assert_eq!(v2_events[0].payload["interval_ms"], 50);
    
    // Check no events were lost - sequential event numbers
    let mut last_num = None;
    for event in events.iter() {
        let num = event.payload["event_number"].as_u64().unwrap();
        if let Some(last) = last_num {
            pretty_assertions::assert_eq!(num, last + 1, "Event sequence broken");
        }
        last_num = Some(num);
    }
    
    Ok(())
}

#[sinex_test]
async fn test_config_reload_with_source_restart(ctx: TestContext) -> TestResult {
    // Test that sources can be gracefully restarted with new config
    let (tx, mut rx) = mpsc::channel::<RawEvent>(100);
    
    // Start with one configuration
    let ctx1 = EventSourceContext::new(json!({
        "event_interval_ms": 200,
        "source_id": "instance_1"
    }));
    let mut source1 = ConfigurableEventSource::initialize(ctx1).await?;
    let stop1 = source1.should_stop.clone();
    
    let tx1 = tx.clone();
    let handle1 = tokio::spawn(async move {
        source1.stream_events(tx1).await
    });
    
    // Collect some events
    let mut events = Vec::new();
    for _ in 0..3 {
        if let Some(event) = rx.recv().await {
            events.push(event);
        }
    }
    
    // Stop first source
    stop1.store(true, Ordering::Relaxed);
    handle1.await??;
    
    // Start new source with different config
    let ctx2 = EventSourceContext::new(json!({
        "event_interval_ms": 100,
        "source_id": "instance_2"
    }));
    let mut source2 = ConfigurableEventSource::initialize(ctx2).await?;
    let stop2 = source2.should_stop.clone();
    
    let handle2 = tokio::spawn(async move {
        source2.stream_events(tx).await
    });
    
    // Collect more events
    for _ in 0..3 {
        if let Some(event) = rx.recv().await {
            events.push(event);
        }
    }
    
    // Stop second source
    stop2.store(true, Ordering::Relaxed);
    handle2.await??;
    
    // Verify we got events from both configurations
    assert!(events.len() >= 6);
    
    // First events should have 200ms interval
    pretty_assertions::assert_eq!(events[0].payload["interval_ms"], 200);
    
    // Later events should have 100ms interval
    pretty_assertions::assert_eq!(events[events.len() - 1].payload["interval_ms"], 100);
    
    Ok(())
}

#[sinex_test]
async fn test_config_validation_before_reload(ctx: TestContext) -> TestResult {
    // Test that invalid configs are rejected without affecting running sources
    
    let valid_config = CollectorConfig {
        enabled_events: vec!["test.event".to_string()],
        ..Default::default()
    };
    
    let invalid_config = CollectorConfig {
        enabled_events: vec!["invalid_format".to_string()], // No dot separator
        ..Default::default()
    };
    
    // Valid config should pass validation
    assert!(valid_config.validate().is_ok());
    
    // Invalid config should fail validation
    assert!(invalid_config.validate().is_err());
    
    // In a real collector, the invalid config would be rejected
    // and the old config would continue to be used
    
    Ok(())
}

#[sinex_test]
async fn test_partial_reload_capability(ctx: TestContext) -> TestResult {
    // Test that specific sources can be reloaded without affecting others
    
    let (tx, mut rx) = mpsc::channel::<RawEvent>(100);
    
    // Start two independent sources
    let mut sources = Vec::new();
    let mut stop_flags = Vec::new();
    
    for i in 0..2 {
        let source_ctx = EventSourceContext::new(json!({
            "event_interval_ms": 100,
            "source_id": format!("source_{}", i)
        }));
        let mut source = ConfigurableEventSource::initialize(source_ctx).await?;
        let stop = source.should_stop.clone();
        stop_flags.push(stop);
        
        let tx_clone = tx.clone();
        let handle = tokio::spawn(async move {
            source.stream_events(tx_clone).await
        });
        sources.push(handle);
    }
    
    // Let both run
    tokio::time::sleep(Duration::from_millis(250)).await;
    
    // Stop only the first source (simulating reload of just that source)
    stop_flags[0].store(true, Ordering::Relaxed);
    
    // Second source should continue
    tokio::time::sleep(Duration::from_millis(250)).await;
    
    // Stop second source
    stop_flags[1].store(true, Ordering::Relaxed);
    drop(tx);
    
    // Collect all events
    let mut events = Vec::new();
    while let Ok(event) = rx.try_recv() {
        events.push(event);
    }
    
    // Should have events from both sources
    let source_0_events: Vec<_> = events.iter()
        .filter(|e| e.source == "configurable_source")
        .collect();
    
    assert!(!source_0_events.is_empty());
    
    Ok(())
}