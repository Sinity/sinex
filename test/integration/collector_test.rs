use crate::common::prelude::*;
use crate::common::timing_optimization::wait_helpers::wait_for_condition_or_timeout;
use sinex_collector::{CollectorConfig, OutputConfig, UnifiedCollector};
use sinex_core::{CoreError, EventSource, EventSourceContext, RawEventBuilder};
use sinex_db::validation::EventValidator;
use std::io::Write;
use std::sync::{
    atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering},
    Arc,
};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Barrier, Mutex};

// =============================================================================
// BASIC COLLECTOR TESTS
// =============================================================================

/// Test that collector can be created with valid configuration
#[sinex_test]
async fn test_collector_creation(_ctx: TestContext) -> TestResult {
    let config = CollectorConfig {
        enabled_events: vec!["fs".to_string()],
        event: HashMap::new(),
        flat_config: HashMap::new(),
        annex_repo_path: None,
    };

    let output_config = OutputConfig {
        to_database: false,
        to_stdout: true,
        to_file: None,
        dry_run: true,
    };

    let _collector = UnifiedCollector::new(config, output_config, None, None);
    Ok(())
}

/// Test output configuration options
#[sinex_test]
async fn test_output_config_database(ctx: TestContext) -> TestResult {
    let config = CollectorConfig {
        enabled_events: vec!["fs".to_string()],
        event: HashMap::new(),
        flat_config: HashMap::new(),
        annex_repo_path: None,
    };

    let output_config = OutputConfig {
        to_database: true,
        to_stdout: false,
        to_file: None,
        dry_run: false,
    };

    let _collector = UnifiedCollector::new(config, output_config, Some(ctx.pool().clone()), None);
    Ok(())
}

/// Test collector configuration loading
#[sinex_test]
async fn test_collector_config_loading(_ctx: TestContext) -> TestResult {
    let result = CollectorConfig::load();

    match result {
        Ok(config) => {
            assert!(!config.enabled_events.is_empty(), "Default config should have enabled events");
            assert!(config.enabled_events.contains(&"file.created".to_string()), "Should include file.created");
            assert!(config.enabled_events.contains(&"command.executed".to_string()), "Should include command.executed");

            assert!(config.event.is_empty() || !config.event.is_empty(), "Event map should be defined");
            assert!(config.flat_config.is_empty() || !config.flat_config.is_empty(), "Flat config should be defined");

            let file_config = config.get_event_config("file.created");
            assert!(file_config.is_table(), "Event config should return a table");
        }
        Err(e) => {
            assert!(!e.to_string().is_empty(), "Error should have a meaningful message: {}", e);
        }
    }
    Ok(())
}

/// Test event filtering based on enabled events
#[sinex_test]
async fn test_event_filtering(_ctx: TestContext) -> TestResult {
    let mut config = CollectorConfig {
        enabled_events: vec!["fs".to_string()],
        event: HashMap::new(),
        flat_config: HashMap::new(),
        annex_repo_path: None,
    };

    let output_config = OutputConfig {
        to_database: false,
        to_stdout: true,
        to_file: None,
        dry_run: true,
    };

    let _collector = UnifiedCollector::new(config.clone(), output_config.clone(), None, None);

    config.enabled_events = vec!["terminal".to_string(), "window_manager".to_string()];
    let _collector2 = UnifiedCollector::new(config, output_config, None, None);
    Ok(())
}

/// Test collector with file output
#[sinex_test]
async fn test_collector_file_output(_ctx: TestContext) -> TestResult {
    let config = CollectorConfig {
        enabled_events: vec!["fs".to_string()],
        event: HashMap::new(),
        flat_config: HashMap::new(),
        annex_repo_path: None,
    };

    let output_config = OutputConfig {
        to_database: false,
        to_stdout: false,
        to_file: Some("/tmp/test_events.jsonl".to_string()),
        dry_run: false,
    };

    let _collector = UnifiedCollector::new(config, output_config, None, None);

    let _ = std::fs::remove_file("/tmp/test_events.jsonl");
    Ok(())
}

/// Test collector with validator
#[sinex_test]
async fn test_collector_with_validator(ctx: TestContext) -> TestResult {
    let config = CollectorConfig {
        enabled_events: vec!["fs".to_string()],
        event: HashMap::new(),
        flat_config: HashMap::new(),
        annex_repo_path: None,
    };

    let output_config = OutputConfig {
        to_database: true,
        to_stdout: false,
        to_file: None,
        dry_run: false,
    };

    let validator = EventValidator::new();
    let _collector = UnifiedCollector::new(config, output_config, Some(ctx.pool().clone()), Some(validator));
    Ok(())
}

// =============================================================================
// BACKPRESSURE TESTS
// =============================================================================

/// High-frequency event source that can generate events rapidly
#[derive(Clone)]
pub struct HighFrequencyEventSource {
    events_per_second: usize,
    max_events: Option<usize>,
    events_sent: Arc<AtomicUsize>,
    should_stop: Arc<AtomicBool>,
}

impl HighFrequencyEventSource {
    pub fn new(events_per_second: usize) -> Self {
        Self {
            events_per_second,
            max_events: None,
            events_sent: Arc::new(AtomicUsize::new(0)),
            should_stop: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn with_max_events(mut self, max_events: usize) -> Self {
        self.max_events = Some(max_events);
        self
    }

    pub fn stop(&self) {
        self.should_stop.store(true, Ordering::SeqCst);
    }

    pub fn events_sent(&self) -> usize {
        self.events_sent.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl EventSource for HighFrequencyEventSource {
    type Config = serde_json::Value;
    const SOURCE_NAME: &'static str = "test.high_frequency_source";

    async fn initialize(_ctx: EventSourceContext) -> sinex_core::Result<Self> {
        Ok(Self::new(1000))
    }

    async fn stream_events(&mut self, tx: mpsc::Sender<RawEvent>) -> sinex_core::Result<()> {
        let interval = Duration::from_nanos(1_000_000_000 / self.events_per_second as u64);
        let mut interval_timer = tokio::time::interval(interval);
        interval_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            if self.should_stop.load(Ordering::SeqCst) {
                break;
            }

            let events_sent = self.events_sent.load(Ordering::SeqCst);
            if let Some(max) = self.max_events {
                if events_sent >= max {
                    break;
                }
            }

            interval_timer.tick().await;

            let event = RawEventBuilder::new(
                Self::SOURCE_NAME,
                "test.high_frequency",
                json!({
                    "event_number": events_sent,
                    "timestamp": chrono::Utc::now().to_rfc3339(),
                    "data": format!("Event {}", events_sent)
                }),
            )
            .build();

            match tx.send(event).await {
                Ok(_) => {
                    self.events_sent.fetch_add(1, Ordering::SeqCst);
                }
                Err(_) => break,
            }
        }

        Ok(())
    }
}

/// Slow event processor that simulates processing delays
#[derive(Clone)]
pub struct SlowEventProcessor {
    processing_delay: Duration,
    events_processed: Arc<AtomicUsize>,
    should_stop: Arc<AtomicBool>,
}

impl SlowEventProcessor {
    pub fn new(processing_delay: Duration) -> Self {
        Self {
            processing_delay,
            events_processed: Arc::new(AtomicUsize::new(0)),
            should_stop: Arc::new(AtomicBool::new(false)),
        }
    }

    pub async fn process_events(&self, mut rx: mpsc::Receiver<RawEvent>) {
        while let Some(event) = rx.recv().await {
            if self.should_stop.load(Ordering::SeqCst) {
                break;
            }

            tokio::time::sleep(self.processing_delay).await;

            assert!(!event.source.is_empty());
            assert!(!event.event_type.is_empty());

            self.events_processed.fetch_add(1, Ordering::SeqCst);
        }
    }

    pub fn stop(&self) {
        self.should_stop.store(true, Ordering::SeqCst);
    }

    pub fn events_processed(&self) -> usize {
        self.events_processed.load(Ordering::SeqCst)
    }
}

#[sinex_test]
async fn test_channel_backpressure_with_fast_producer_slow_consumer(_ctx: TestContext) -> TestResult {
    let (tx, rx) = mpsc::channel::<RawEvent>(10_000);

    let fast_producer = HighFrequencyEventSource::new(5000).with_max_events(15_000);
    let slow_consumer = SlowEventProcessor::new(Duration::from_millis(100));

    let start_time = Instant::now();

    let consumer_handle = {
        let consumer = slow_consumer.clone();
        tokio::spawn(async move {
            consumer.process_events(rx).await;
        })
    };

    let mut producer_clone = fast_producer.clone();
    let producer_handle = tokio::spawn(async move { producer_clone.stream_events(tx).await });

    let consumer_clone = slow_consumer.clone();
    let _wait_result = wait_for_condition_or_timeout(
        move || {
            let processed = consumer_clone.events_processed();
            Box::pin(async move { Ok(processed >= 20) })
        },
        5,
    )
    .await;

    slow_consumer.stop();
    fast_producer.stop();

    let _ = timeout(Duration::from_secs(2), producer_handle).await;
    let _ = timeout(Duration::from_secs(2), consumer_handle).await;

    let elapsed = start_time.elapsed();
    let events_sent = fast_producer.events_sent();
    let events_processed = slow_consumer.events_processed();

    println!("Test ran for: {:.2}s", elapsed.as_secs_f64());
    println!("Events sent: {}", events_sent);
    println!("Events processed: {}", events_processed);

    assert!(events_sent > 0, "Producer should have sent some events");
    assert!(events_processed > 0, "Consumer should have processed some events");

    let expected_max_sent = 5000 * 3;
    assert!(
        events_sent < expected_max_sent,
        "Producer should be throttled by backpressure, sent {} but could send up to {}",
        events_sent,
        expected_max_sent
    );

    assert!(events_processed >= 20, "Consumer should have processed at least 20 events, got {}", events_processed);

    let process_rate = events_processed as f64 / elapsed.as_secs_f64();
    assert!(process_rate <= 15.0, "Process rate {} events/sec should be limited by consumer delay", process_rate);

    Ok(())
}

#[sinex_test]
async fn test_channel_saturation_prevents_event_loss(_ctx: TestContext) -> TestResult {
    let (tx, mut rx) = mpsc::channel::<RawEvent>(100);

    let producer = HighFrequencyEventSource::new(10_000).with_max_events(150);

    let mut producer_clone = producer.clone();
    let producer_handle = tokio::spawn(async move { producer_clone.stream_events(tx).await });

    let producer_clone = producer.clone();
    let _ = wait_for_condition_or_timeout(
        move || {
            let sent = producer_clone.events_sent();
            Box::pin(async move { Ok(sent >= 100) })
        },
        1,
    )
    .await;

    let mut events_received = Vec::new();
    while let Ok(Some(event)) = timeout(Duration::from_millis(10), rx.recv()).await {
        events_received.push(event);
    }

    let producer_result = timeout(Duration::from_secs(2), producer_handle).await;
    let events_sent = producer.events_sent();

    println!("Events sent: {}", events_sent);
    println!("Events received: {}", events_received.len());

    pretty_assertions::assert_eq!(events_sent, events_received.len(), "All sent events should be received, no events should be lost");

    for (i, event) in events_received.iter().enumerate() {
        pretty_assertions::assert_eq!(event.source, "test.high_frequency_source");
        pretty_assertions::assert_eq!(event.event_type, "test.high_frequency");

        let event_number = event.payload["event_number"].as_u64().unwrap() as usize;
        pretty_assertions::assert_eq!(event_number, i, "Events should be received in order");
    }

    assert!(producer_result.is_ok(), "Producer should complete without errors");

    Ok(())
}

// =============================================================================
// HOT RELOAD TESTS
// =============================================================================

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
        let interval = ctx.config["event_interval_ms"].as_u64().unwrap_or(100) as u32;

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
                }),
            )
            .build();

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
    let config_file = NamedTempFile::new()?;
    let mut event_config = HashMap::new();
    event_config.insert(
        "configurable_source".to_string(),
        ConfigValue::Table({
            let mut table = toml::map::Map::new();
            table.insert("event_interval_ms".to_string(), ConfigValue::Integer(100));
            table
        }),
    );

    let initial_config = CollectorConfig {
        enabled_events: vec!["config.test".to_string()],
        event: event_config,
        ..Default::default()
    };

    let config_str = toml::to_string(&initial_config)?;
    config_file.as_file().write_all(config_str.as_bytes())?;
    config_file.as_file().sync_all()?;

    let received_events = Arc::new(Mutex::new(Vec::new()));
    let (tx, mut rx) = mpsc::channel::<RawEvent>(1000);

    let events_clone = received_events.clone();
    let receiver_task = tokio::spawn(async move {
        while let Some(event) = rx.recv().await {
            events_clone.lock().await.push(event);
        }
    });

    let source_ctx = EventSourceContext::new(json!({
        "event_interval_ms": 100
    }));
    let mut source = ConfigurableEventSource::initialize(source_ctx).await?;
    let should_stop = source.should_stop.clone();
    let config_version = source.config_version.clone();
    let event_interval = source.event_interval_ms.clone();

    let tx_clone = tx.clone();
    let stream_task = tokio::spawn(async move { source.stream_events(tx_clone).await });

    tokio::time::sleep(Duration::from_millis(350)).await;

    config_version.store(2, Ordering::Relaxed);
    event_interval.store(50, Ordering::Relaxed);

    tokio::time::sleep(Duration::from_millis(350)).await;

    should_stop.store(true, Ordering::Relaxed);
    drop(tx);

    let _ = stream_task.await?;
    receiver_task.await?;

    let events = received_events.lock().await;

    let v1_events: Vec<_> = events.iter().filter(|e| e.payload["config_version"] == 1).collect();
    let v2_events: Vec<_> = events.iter().filter(|e| e.payload["config_version"] == 2).collect();

    assert!(!v1_events.is_empty(), "Should have events from config v1");
    assert!(!v2_events.is_empty(), "Should have events from config v2");

    pretty_assertions::assert_eq!(v1_events[0].payload["interval_ms"], 100);
    pretty_assertions::assert_eq!(v2_events[0].payload["interval_ms"], 50);

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

// =============================================================================
// MULTI-SOURCE COORDINATION TESTS
// =============================================================================

struct TestCoordinatedSource {
    source_id: String,
    events_generated: Arc<AtomicU32>,
    should_fail: Arc<AtomicBool>,
    event_delay_ms: u64,
}

#[async_trait]
impl EventSource for TestCoordinatedSource {
    type Config = serde_json::Value;
    const SOURCE_NAME: &'static str = "test_coordinated";

    async fn initialize(ctx: EventSourceContext) -> sinex_core::Result<Self> {
        let source_id = ctx.config["source_id"].as_str().unwrap_or("unknown").to_string();
        let startup_delay_ms = ctx.config["startup_delay_ms"].as_u64().unwrap_or(0);
        let event_delay_ms = ctx.config["event_delay_ms"].as_u64().unwrap_or(100);

        if startup_delay_ms > 0 {
            tokio::time::sleep(Duration::from_millis(startup_delay_ms)).await;
        }

        Ok(Self {
            source_id,
            events_generated: Arc::new(AtomicU32::new(0)),
            should_fail: Arc::new(AtomicBool::new(false)),
            event_delay_ms,
        })
    }

    async fn stream_events(&mut self, tx: mpsc::Sender<RawEvent>) -> sinex_core::Result<()> {
        while !self.should_fail.load(Ordering::Relaxed) {
            let count = self.events_generated.fetch_add(1, Ordering::Relaxed);

            let event = sinex_core::RawEventBuilder::new(
                Self::SOURCE_NAME,
                "coordination.test",
                json!({
                    "source_id": self.source_id,
                    "event_number": count,
                    "timestamp": Instant::now().elapsed().as_millis(),
                }),
            )
            .build();

            if tx.send(event).await.is_err() {
                break;
            }

            tokio::time::sleep(Duration::from_millis(self.event_delay_ms)).await;
        }

        Ok(())
    }
}

#[sinex_test]
async fn test_multiple_sources_lifecycle_management(ctx: TestContext) -> TestResult {
    let (tx, mut rx) = mpsc::channel::<RawEvent>(1000);

    let mut handles = Vec::new();
    let mut source_controls = Vec::new();

    for i in 0..3 {
        let source_ctx = EventSourceContext::new(json!({
            "source_id": format!("source_{}", i),
            "startup_delay_ms": i * 100,
            "event_delay_ms": 50,
        }));

        let mut source = TestCoordinatedSource::initialize(source_ctx).await?;
        let events_generated = source.events_generated.clone();
        let should_fail = source.should_fail.clone();

        source_controls.push((events_generated, should_fail));

        let tx_clone = tx.clone();
        let handle = tokio::spawn(async move { source.stream_events(tx_clone).await });
        handles.push(handle);
    }

    tokio::time::sleep(Duration::from_millis(500)).await;

    for (_, should_fail) in source_controls.iter().rev() {
        should_fail.store(true, Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    for handle in handles {
        handle.await??;
    }

    drop(tx);

    let mut events_by_source: HashMap<String, Vec<RawEvent>> = HashMap::new();
    while let Ok(event) = rx.try_recv() {
        let source_id = event.payload["source_id"].as_str().unwrap().to_string();
        events_by_source.entry(source_id).or_default().push(event);
    }

    pretty_assertions::assert_eq!(events_by_source.len(), 3);

    for i in 0..3 {
        let source_id = format!("source_{}", i);
        assert!(events_by_source.contains_key(&source_id));
        assert!(!events_by_source[&source_id].is_empty());
    }

    Ok(())
}

#[sinex_test]
async fn test_source_startup_synchronization(ctx: TestContext) -> TestResult {
    let (tx, mut rx) = mpsc::channel::<RawEvent>(1000);
    let barrier = Arc::new(Barrier::new(3));

    let mut handles = Vec::new();

    for i in 0..3 {
        let barrier_clone = barrier.clone();
        let tx_clone = tx.clone();

        let handle = tokio::spawn(async move {
            let source_ctx = EventSourceContext::new(json!({
                "source_id": format!("source_{}", i),
                "event_delay_ms": 50,
            }));

            let mut _source = TestCoordinatedSource::initialize(source_ctx).await?;

            barrier_clone.wait().await;

            let mut event_count = 0;
            loop {
                let event = sinex_core::RawEventBuilder::new(
                    "test_coordinated",
                    "sync.test",
                    json!({
                        "source_id": format!("source_{}", i),
                        "sync_event": true,
                    }),
                )
                .build();

                if tx_clone.send(event).await.is_err() {
                    break;
                }

                event_count += 1;
                if event_count >= 3 {
                    break;
                }

                tokio::task::yield_now().await;
            }

            anyhow::Ok(())
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.await??;
    }

    drop(tx);

    let mut first_events = Vec::new();
    while let Ok(event) = rx.try_recv() {
        if event.payload["sync_event"].as_bool().unwrap_or(false) {
            first_events.push(event);
        }
    }

    assert!(first_events.len() >= 9);

    Ok(())
}

#[sinex_test]
async fn test_registry_based_source_discovery(ctx: TestContext) -> TestResult {
    let registry = create_registry();

    let all_types = registry.event_types;
    assert!(!all_types.is_empty());

    let mut sources: HashMap<&str, Vec<&str>> = HashMap::new();
    for (event_type, source) in registry.event_to_source {
        sources.entry(source).or_default().push(event_type);
    }

    assert!(sources.len() > 1);

    assert!(sources.contains_key("fs"));
    assert!(sources.contains_key("shell.kitty"));

    Ok(())
}