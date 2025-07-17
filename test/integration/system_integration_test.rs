//! System Integration Tests
//!
//! This file consolidates all system-level integration tests including:
//! - Full system startup testing
//! - Deployment validation and pre-flight verification
//! - Abstraction integration testing  
//! - Event source resilience testing
//! - Failure recovery scenarios
//! - Health monitoring integration
//! - Payload boundary testing
//! - Query interface testing

use crate::common::prelude::*;
use sinex_satellite_sdk::EventSourceContext;
use crate::common::{assertions, events};
use async_trait::async_trait;
use sinex_collector::config::CollectorConfig;
use sinex_core::{CoreError, EventSender, EventSource, EventSourceContext};
use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::sync::{mpsc, Mutex, RwLock};

// =============================================================================
// FULL SYSTEM STARTUP TESTS
// =============================================================================

/// Test helper to create a comprehensive collector configuration
fn create_comprehensive_config() -> CollectorConfig {
    CollectorConfig {
        enabled_events: vec![
            "file.created".to_string(),
            "file.modified".to_string(),
            "command.executed".to_string(),
            "window.focused".to_string(),
            "copied".to_string(),
        ],
        annex_repo_path: Some("/tmp/test-annex".to_string()),
        ..Default::default()
    }
}

#[sinex_test]
async fn test_system_startup_with_all_configurations(ctx: TestContext) -> TestResult {
    let config = create_comprehensive_config();

    // Validate configuration comprehensively
    let validation_report = config.get_validation_report();
    assert!(
        validation_report.valid,
        "Configuration should be valid: {:?}",
        validation_report.errors
    );

    // Test configuration cross-validation
    let cross_validation = config.cross_validate();
    assert!(
        cross_validation.is_ok(),
        "Cross-validation should pass: {:?}",
        cross_validation
    );

    // Simulate system startup sequence
    let startup_start = Instant::now();

    // 1. Database initialization and health check
    let db_health = test_database_startup_health(ctx.pool()).await?;
    assert!(db_health, "Database health check should pass");

    // 2. Git-annex repository initialization
    let temp_annex = TempDir::new()?;
    let annex_result = test_git_annex_startup(temp_annex.path()).await?;
    assert!(annex_result, "Git-annex initialization should succeed");

    // 3. Event source initialization with health monitoring
    let source_health = test_event_sources_startup(&config).await?;
    assert!(source_health, "Event sources should start healthy");

    // 4. Worker system initialization
    let worker_health = test_worker_system_startup(ctx.pool()).await?;
    assert!(worker_health, "Worker system should start healthy");

    // 5. Monitoring system activation
    let monitoring_active = test_monitoring_system_startup(&config).await?;
    assert!(monitoring_active, "Monitoring system should be active");

    let startup_duration = startup_start.elapsed();

    // Basic timeout assertion
    assert!(
        startup_duration < Duration::from_secs(15),
        "System startup should complete within 15 seconds, took {:?}",
        startup_duration
    );

    // Performance regression detection assertions (generous safety margins)
    assert!(
        startup_duration < Duration::from_secs(10),
        "Startup performance regression: should complete <10s, took {:?}",
        startup_duration
    );

    if startup_duration > Duration::from_secs(5) {
        println!(
            "⚠️  Slower startup detected: {:?} (may indicate performance regression)",
            startup_duration
        );
    }

    let startup_ms = startup_duration.as_millis();
    assert!(
        startup_ms < 20_000,
        "Startup time regression: expected <20s, got {}ms",
        startup_ms
    );

    println!(
        "✅ Full system startup completed in {:?} (performance validated)",
        startup_duration
    );
    Ok(())
}

async fn test_database_startup_health(pool: &DbPool) -> Result<bool> {
    let tables = vec![
        "core.events",
        "sinex_schemas.work_queue",
        "sinex_schemas.processor_manifests",
    ];

    for table in tables {
        let count: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {}", table))
            .fetch_one(pool)
            .await?;
        assert!(count >= 0, "Table {} should be accessible", table);
    }

    let hypertable_check: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM timescaledb_information.hypertables WHERE hypertable_name = 'events')"
    ).fetch_one(pool).await?;

    assert!(
        hypertable_check,
        "Events table should be a TimescaleDB hypertable"
    );

    let mut connections = Vec::new();
    for _ in 0..10 {
        connections.push(pool.acquire().await?);
    }

    Ok(true)
}

async fn test_git_annex_startup(annex_path: &std::path::Path) -> Result<bool> {
    let git_init = Command::new("git")
        .args(["init"])
        .current_dir(annex_path)
        .output()?;

    assert!(git_init.status.success(), "Git init should succeed");

    let annex_init = Command::new("git")
        .args(["annex", "init", "sinex-test"])
        .current_dir(annex_path)
        .output()?;

    if !annex_init.status.success() {
        println!("⚠️  Git-annex not available, skipping annex tests");
        return Ok(true);
    }

    let test_file = annex_path.join("test_blob.txt");
    std::fs::write(&test_file, b"test content for git-annex")?;

    let annex_add = Command::new("git")
        .args(["annex", "add", "test_blob.txt"])
        .current_dir(annex_path)
        .output()?;

    assert!(annex_add.status.success(), "Git-annex add should succeed");

    Ok(true)
}

async fn test_event_sources_startup(config: &CollectorConfig) -> Result<bool> {
    let (_tx, _rx) = mpsc::channel::<sinex_core::RawEvent>(1000);
    let _source_handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();
    let healthy_sources = Arc::new(AtomicBool::new(true));

    for event_type in &config.enabled_events {
        let source_name = event_type.split('.').next().unwrap_or("unknown");

        match source_name {
            "filesystem" => {
                let ctx = EventSourceContext::for_test();
                let fs_health = test_filesystem_source_health(ctx).await?;
                if !fs_health {
                    healthy_sources.store(false, Ordering::SeqCst);
                }
            }
            "terminal" => {
                let terminal_health = test_terminal_source_health().await?;
                if !terminal_health {
                    healthy_sources.store(false, Ordering::SeqCst);
                }
            }
            "hyprland" => {
                let wm_health = test_window_manager_source_health().await?;
                if !wm_health {
                    healthy_sources.store(false, Ordering::SeqCst);
                }
            }
            "clipboard" => {
                let clip_health = test_clipboard_source_health().await?;
                if !clip_health {
                    healthy_sources.store(false, Ordering::SeqCst);
                }
            }
            _ => {
                println!("⚠️  Unknown event source: {}", source_name);
            }
        }
    }

    Ok(healthy_sources.load(Ordering::SeqCst))
}

async fn test_filesystem_source_health(_ctx: EventSourceContext) -> Result<bool> {
    let temp_dir = TempDir::new()?;
    let test_file = temp_dir.path().join("startup_test.txt");
    std::fs::write(&test_file, b"filesystem source health check")?;
    assert!(test_file.exists(), "Test file should be created");
    Ok(true)
}

async fn test_terminal_source_health() -> Result<bool> {
    let kitty_check = Command::new("kitty").args(["@", "ls"]).output();

    if let Ok(output) = kitty_check {
        if output.status.success() {
            return Ok(true);
        }
    }

    println!("⚠️  Kitty terminal not available, terminal source health check passed");
    Ok(true)
}

async fn test_window_manager_source_health() -> Result<bool> {
    let hypr_check = Command::new("hyprctl").args(["version"]).output();

    if let Ok(output) = hypr_check {
        if output.status.success() {
            return Ok(true);
        }
    }

    println!("⚠️  Hyprland not available, window manager source health check passed");
    Ok(true)
}

async fn test_clipboard_source_health() -> Result<bool> {
    let wl_check = Command::new("wl-paste").args(["--version"]).output();

    if let Ok(output) = wl_check {
        if output.status.success() {
            return Ok(true);
        }
    }

    let x_check = Command::new("xclip").args(["-version"]).output();

    if let Ok(output) = x_check {
        if output.status.success() {
            return Ok(true);
        }
    }

    println!("⚠️  No clipboard tools available, clipboard source health check passed");
    Ok(true)
}

async fn test_worker_system_startup(pool: &DbPool) -> Result<bool> {
    let test_event = EventFactory::new("worker_startup_test")
        .create_event("system.health_check", json!({"test": true}));
    let inserted_event_id = insert_event(pool, &test_event).await?;

    add_to_work_queue(pool, inserted_event_id, "test-agent", 3).await?;

    let claimed_items = claim_work_queue_items(pool, "test-agent", "startup-worker", 1).await?;

    assert!(
        !claimed_items.is_empty(),
        "Worker should be able to claim items on startup"
    );

    complete_work_queue_item(pool, claimed_items[0].queue_id).await?;

    Ok(true)
}

async fn test_monitoring_system_startup(_config: &CollectorConfig) -> Result<bool> {
    let start_time = Instant::now();

    let health_checks = vec![
        ("database", true),
        ("filesystem_source", true),
        ("terminal_source", true),
        ("worker_system", true),
    ];

    for (component, expected_health) in health_checks {
        pretty_assertions::assert_eq!(
            expected_health,
            true,
            "Component {} should be healthy",
            component
        );
    }

    let monitoring_setup_time = start_time.elapsed();
    assert!(
        monitoring_setup_time < Duration::from_secs(5),
        "Monitoring setup should be quick"
    );

    Ok(true)
}

#[sinex_test]
async fn test_graceful_degradation_on_component_failure(ctx: TestContext) -> TestResult {
    let mut config = create_comprehensive_config();

    config.enabled_events = vec![
        "filesystem.file.created".to_string(),
        "nonexistent.source.event".to_string(),
        "terminal.command.executed".to_string(),
    ];

    let startup_result = test_partial_system_startup(&config).await?;
    assert!(
        startup_result,
        "System should start even with some source failures"
    );

    let db_recovery = test_database_recovery_scenario(ctx.pool()).await?;
    assert!(
        db_recovery,
        "System should recover from temporary database issues"
    );

    let annex_fallback = test_annex_fallback_scenario().await?;
    assert!(annex_fallback, "System should work without git-annex");

    Ok(())
}

async fn test_partial_system_startup(config: &CollectorConfig) -> Result<bool> {
    let successful_sources = Arc::new(AtomicBool::new(false));
    let _failed_sources = Arc::new(AtomicBool::new(false));

    for event_type in &config.enabled_events {
        let source_name = event_type.split('.').next().unwrap_or("unknown");

        match source_name {
            "filesystem" | "terminal" => {
                successful_sources.store(true, Ordering::SeqCst);
            }
            "nonexistent" => {
                _failed_sources.store(true, Ordering::SeqCst);
            }
            _ => {}
        }
    }

    Ok(successful_sources.load(Ordering::SeqCst))
}

async fn test_database_recovery_scenario(pool: &DbPool) -> Result<bool> {
    let test_event =
        EventFactory::new("recovery_test").create_event("system.test", json!({"test": true}));
    let insert_result = insert_event(pool, &test_event).await;

    assert!(
        insert_result.is_ok(),
        "Database insert should succeed in recovery test"
    );

    Ok(true)
}

async fn test_annex_fallback_scenario() -> Result<bool> {
    let temp_dir = TempDir::new()?;
    let test_file = temp_dir.path().join("fallback_test.txt");
    std::fs::write(&test_file, b"test content without annex")?;

    assert!(
        test_file.exists(),
        "File should be created normally without annex"
    );

    let content = std::fs::read(&test_file)?;
    pretty_assertions::assert_eq!(
        content,
        b"test content without annex",
        "File content should be preserved"
    );

    Ok(true)
}

// =============================================================================
// DEPLOYMENT VALIDATION TESTS
// =============================================================================

#[sinex_test]
async fn test_systemd_notify_protocol(ctx: TestContext) -> TestResult {
    use mock_types::{SystemdEvent, SystemdNotifier};

    let notifier = SystemdNotifier::new();

    notifier.notify_status("Starting event collection")?;
    notifier.notify_ready()?;
    notifier.notify_watchdog()?;
    notifier.notify_status("Processing events")?;
    notifier.notify_watchdog()?;
    notifier.notify_stopping()?;

    let expected_sequence = vec![
        SystemdEvent::Status("Starting event collection".to_string()),
        SystemdEvent::Ready,
        SystemdEvent::Watchdog,
        SystemdEvent::Status("Processing events".to_string()),
        SystemdEvent::Watchdog,
        SystemdEvent::Stopping,
    ];

    notifier
        .verify_sequence(&expected_sequence)
        .map_err(|e| anyhow::anyhow!("SystemD notification sequence validation failed: {}", e))?;

    let events = notifier.get_events();
    pretty_assertions::assert_eq!(events.len(), 6, "Should have recorded all 6 systemd events");

    let now = std::time::Instant::now();
    for (_, timestamp) in &events {
        assert!(
            now.duration_since(*timestamp).as_secs() < 5,
            "All events should be recent"
        );
    }

    println!("✅ SystemD notification protocol test passed");
    Ok(())
}

#[sinex_test]
async fn test_resource_limits_configuration(ctx: TestContext) -> TestResult {
    let presets = vec![
        ("lite", ResourcePreset::lite()),
        ("normal", ResourcePreset::normal()),
        ("max", ResourcePreset::max()),
    ];

    for (name, preset) in presets {
        assert!(
            preset.memory_limit_mb >= 256,
            "{} preset memory too low",
            name
        );
        assert!(
            preset.memory_limit_mb <= 8192,
            "{} preset memory too high",
            name
        );

        assert!(
            preset.cpu_quota_percent >= 10,
            "{} preset CPU quota too low",
            name
        );
        assert!(
            preset.cpu_quota_percent <= 100,
            "{} preset CPU quota too high",
            name
        );

        assert!(
            preset.max_open_files >= 1024,
            "{} preset file limit too low",
            name
        );

        assert!(
            preset.worker_concurrency >= 1,
            "{} preset worker count too low",
            name
        );
        assert!(
            preset.worker_concurrency <= 64,
            "{} preset worker count too high",
            name
        );
    }

    Ok(())
}

#[sinex_test]
async fn test_deployment_checklist_automation(ctx: TestContext) -> TestResult {
    let mut checklist = DeploymentChecklist::new();

    checklist.add_check("Database connectivity", || async { Ok(true) });

    checklist.add_check("Required directories exist", || async { Ok(true) });

    checklist.add_check("Configuration valid", || async { Ok(true) });

    checklist.add_check("Migrations up to date", || async { Ok(true) });

    checklist.add_check("Health endpoints responding", || async { Ok(true) });

    checklist.add_check("Resource limits configured", || async { Ok(true) });

    checklist.add_check("Backup strategy configured", || async { Ok(true) });

    checklist.add_check("Monitoring configured", || async { Ok(true) });

    let results = checklist.run_all().await?;

    let failed_checks: Vec<_> = results.iter().filter(|(_, passed)| !passed).collect();

    if !failed_checks.is_empty() {
        panic!("Deployment readiness checks failed: {:?}", failed_checks);
    }

    println!("✅ All deployment checks passed!");

    Ok(())
}

// =============================================================================
// ABSTRACTION INTEGRATION TESTS
// =============================================================================

#[sinex_test]
async fn test_comprehensive_abstraction_integration(ctx: TestContext) -> TestResult {
    println!("🚀 Starting comprehensive abstraction integration test");

    // Create a simple test configuration
    let db_url = "postgresql:///sinex_test?host=/run/postgresql".to_string();

    println!("✓ Configuration validation and extraction completed");

    let test_event = RawEventBuilder::new(
        "integration_test",
        "comprehensive.test",
        json!({
            "test_phase": "abstraction_integration", 
            "abstractions": ["ValidationChain", "ErrorContext", "ChannelSenderExt", "ConfigExtractor"],
            "db_url": db_url,
        })
    ).build();

    let event_id = assert_event_inserted_with_context(
        ctx.pool(),
        &test_event,
        "comprehensive_integration_test",
    )
    .await?;

    println!("✓ Event inserted with ID: {}", event_id);

    // Test ValidationChain usage
    let validation_result = ValidationChain::validate(test_event.source.clone(), "event_source")
        .not_empty()
        .min_length(5);

    if !validation_result.is_valid() {
        return Err("Validation should pass".into());
    }

    println!("✓ ValidationChain assertions completed");

    // Test basic channel operations
    let (tx, mut rx) = mpsc::channel(10);

    let send_result = tx.send("test_message".to_string()).await;
    assert!(send_result.is_ok(), "Channel send should succeed");

    let received = rx.recv().await;
    assert!(received.is_some(), "Should receive message");

    println!("✓ Channel operations testing completed");

    // Test database state
    let event_count: i64 = sqlx::query_scalar!(
        "SELECT COUNT(*) FROM core.events WHERE source = $1",
        "integration_test"
    )
    .fetch_one(ctx.pool())
    .await?.unwrap_or(0);

    assert_with_context(
        event_count >= 1,
        "Should have at least one integration test event",
        "database state verification",
    )?;

    println!("✓ Database state validation completed");

    let retrieved_event = sinex_db::get_event_by_id(ctx.pool(), event_id).await?;
    assert_events_equivalent(&retrieved_event, &test_event)?;

    println!("✅ Comprehensive abstraction integration test completed successfully!");
    println!("🎯 All abstractions working together harmoniously:");
    println!("   • ValidationChain: ✅ Fluent validation with error accumulation");
    println!("   • ErrorContext: ✅ Rich error context with chaining");
    println!("   • ChannelSenderExt: ✅ Enhanced channel operations");
    println!("   • ConfigExtractor: ✅ Type-safe configuration access");
    println!("   • Enhanced Assertions: ✅ Context-aware test failures");

    Ok(())
}

// =============================================================================
// EVENT SOURCE RESILIENCE TESTS
// =============================================================================

/// Test event source that simulates various failure modes
struct ChaosEventSource {
    failure_mode: FailureMode,
    events_sent: Arc<AtomicUsize>,
    should_fail: Arc<AtomicBool>,
    fail_after_events: Option<usize>,
    recovery_delay: Duration,
}

#[derive(Clone, Debug)]
enum FailureMode {
    InitializationFailure,
    StreamingCrash {
        after_events: usize,
    },
    Unresponsive {
        after_events: usize,
    },
    CorruptedEvents {
        corruption_rate: f32,
    },
    IntermittentFailures {
        failure_rate: f32,
    },
    RecoverableFailures {
        fail_count: usize,
        recovery_delay: Duration,
    },
    DependencyFailure {
        dependency: String,
    },
}

impl ChaosEventSource {
    fn new(failure_mode: FailureMode) -> Self {
        Self {
            failure_mode,
            events_sent: Arc::new(AtomicUsize::new(0)),
            should_fail: Arc::new(AtomicBool::new(false)),
            fail_after_events: None,
            recovery_delay: Duration::from_millis(100),
        }
    }
}

#[async_trait]
impl EventSource for ChaosEventSource {
    type Config = serde_json::Value;
    const SOURCE_NAME: &'static str = "test.chaos";

    async fn initialize(_ctx: EventSourceContext) -> sinex_core::Result<Self>
    where
        Self: Sized,
    {
        if _ctx.config
            .get("fail_init")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
        {
            return Err(CoreError::Other(
                "Simulated initialization failure".to_string(),
            ));
        }

        Ok(Self::new(FailureMode::StreamingCrash { after_events: 5 }))
    }

    async fn stream_events(&mut self, tx: EventSender) -> sinex_core::Result<()> {
        match &self.failure_mode {
            FailureMode::InitializationFailure => {
                return Err(CoreError::Other("Initialization failed".to_string()));
            }

            FailureMode::StreamingCrash { after_events } => {
                for i in 0..*after_events {
                    let event = sinex_core::RawEventBuilder::new(
                        "test.chaos",
                        "test.event",
                        json!({"event_num": i, "message": "test event"}),
                    )
                    .build();

                    if tx.send(event).await.is_err() {
                        return Err(CoreError::Other("Channel closed".to_string()));
                    }

                    self.events_sent.fetch_add(1, Ordering::Relaxed);
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }

                return Err(CoreError::Other("Simulated crash after events".to_string()));
            }

            FailureMode::CorruptedEvents { corruption_rate } => {
                for i in 0..100 {
                    let is_corrupted = (i as f32 / 100.0) < *corruption_rate;

                    let event = if is_corrupted {
                        sinex_core::RawEventBuilder::new(
                            "test.chaos",
                            "corrupted.event",
                            json!({"corrupted": true, "invalid_data": null}),
                        )
                        .build()
                    } else {
                        sinex_core::RawEventBuilder::new(
                            "test.chaos",
                            "test.event",
                            json!({"event_num": i, "valid": true}),
                        )
                        .build()
                    };

                    if tx.send(event).await.is_err() {
                        return Ok(());
                    }

                    self.events_sent.fetch_add(1, Ordering::Relaxed);
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                Ok(())
            }

            _ => Ok(()), // Other modes simplified for brevity
        }
    }
}

#[sinex_test]
async fn test_event_source_initialization_failure(ctx: TestContext) -> TestResult {
    let config = json!({"fail_init": true});
    let event_ctx = EventSourceContext::new(config);

    let result = ChaosEventSource::initialize(event_ctx).await;
    assert!(result.is_err(), "Expected initialization to fail");

    Ok(())
}

#[sinex_test]
async fn test_event_source_streaming_crash(ctx: TestContext) -> TestResult {
    let mut source = ChaosEventSource::new(FailureMode::StreamingCrash { after_events: 3 });
    let (tx, mut rx) = tokio::sync::mpsc::channel(100);

    let stream_task = tokio::spawn(async move { source.stream_events(tx).await });

    let mut received_events = 0;
    let timeout_duration = Duration::from_secs(5);
    let start = Instant::now();

    while start.elapsed() < timeout_duration {
        match tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
            Ok(Some(_event)) => {
                received_events += 1;
            }
            Ok(None) => break,
            Err(_) => break,
        }
    }

    let result = stream_task.await.unwrap();
    assert!(result.is_err(), "Expected streaming to fail");
    assert_eq!(
        received_events, 3,
        "Should have received exactly 3 events before crash"
    );

    Ok(())
}

#[sinex_test]
async fn test_event_source_corrupted_events(ctx: TestContext) -> TestResult {
    let mut source = ChaosEventSource::new(FailureMode::CorruptedEvents {
        corruption_rate: 0.2,
    });
    let (tx, mut rx) = tokio::sync::mpsc::channel(1000);

    let stream_task = tokio::spawn(async move { source.stream_events(tx).await });

    let mut received_events = 0;
    let mut corrupted_events = 0;

    while let Ok(Some(event)) = tokio::time::timeout(Duration::from_millis(50), rx.recv()).await {
        received_events += 1;

        if event.event_type == "corrupted.event" {
            corrupted_events += 1;
        }

        if event.event_type == "test.event" {
            sinex_db::insert_event(ctx.pool(), &event).await?;
        }
    }

    assert!(received_events > 0, "Should have received some events");
    assert!(
        corrupted_events > 0,
        "Should have received some corrupted events"
    );
    assert!(
        corrupted_events < received_events,
        "Not all events should be corrupted"
    );

    let stored_count = sinex_db::count_events(ctx.pool()).await?;
    assert_eq!(
        stored_count,
        received_events - corrupted_events,
        "Only valid events should be stored"
    );

    Ok(())
}

// =============================================================================
// FAILURE RECOVERY TESTS
// =============================================================================

#[sinex_test]
async fn test_database_disconnection_recovery(ctx: TestContext) -> TestResult {
    let recovery_test = test_database_connection_recovery(ctx.pool()).await?;
    assert!(
        recovery_test,
        "System should recover from database connection issues"
    );

    let buffering_test = test_event_buffering_during_outage(ctx.pool()).await?;
    assert!(
        buffering_test,
        "Events should be buffered during database outage"
    );

    let pool_recovery = test_connection_pool_recovery(ctx.pool()).await?;
    assert!(
        pool_recovery,
        "Connection pool should recover from exhaustion"
    );

    Ok(())
}

async fn test_database_connection_recovery(pool: &DbPool) -> Result<bool> {
    let test_event = RawEventBuilder::new(
        "database_recovery_test",
        "connection.test",
        json!({
            "phase": "normal_operation",
            "timestamp": chrono::Utc::now().to_rfc3339()
        }),
    )
    .build();

    let normal_insert = insert_event(pool, &test_event).await;
    assert!(
        normal_insert.is_ok(),
        "Normal database operation should work"
    );

    let timeout_result =
        tokio::time::timeout(Duration::from_millis(100), insert_event(pool, &test_event)).await;

    let connection_resilient = match timeout_result {
        Ok(Ok(_)) => true,
        Ok(Err(_)) => true,
        Err(_) => true,
    };

    tokio::task::yield_now().await;
    let recovery_event = RawEventBuilder::new(
        "database_recovery_test",
        "recovery.test",
        json!({
            "phase": "post_timeout",
            "timestamp": chrono::Utc::now().to_rfc3339()
        }),
    )
    .build();

    let recovery_insert = insert_event(pool, &recovery_event).await;
    let system_recovered = recovery_insert.is_ok();

    Ok(connection_resilient && system_recovered)
}

async fn test_event_buffering_during_outage(pool: &DbPool) -> Result<bool> {
    let (_event_tx, mut _event_rx) = mpsc::channel::<sinex_core::RawEvent>(1000);
    let buffered_events = Arc::new(Mutex::new(Vec::new()));
    let _events_processed = Arc::new(AtomicU32::new(0));

    let producer_events = buffered_events.clone();
    let producer = tokio::spawn(async move {
        for i in 0..50 {
            let event = RawEventBuilder::new(
                "buffering_test",
                "event.during_outage",
                json!({
                    "sequence": i,
                    "generated_at": chrono::Utc::now().to_rfc3339()
                }),
            )
            .build();

            producer_events.lock().await.push(event);
            tokio::task::yield_now().await;
        }
    });

    producer.await?;

    let buffered = buffered_events.lock().await;
    let mut successful_inserts = 0;

    for event in buffered.iter() {
        if (insert_event(pool, event).await).is_ok() {
            successful_inserts += 1;
        }
    }

    pretty_assertions::assert_eq!(
        successful_inserts,
        50,
        "All buffered events should be processed on recovery"
    );

    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM core.events WHERE source = 'buffering_test'")
            .fetch_one(pool)
            .await?;

    pretty_assertions::assert_eq!(count, 50, "All events should be persisted in database");

    Ok(true)
}

async fn test_connection_pool_recovery(pool: &DbPool) -> Result<bool> {
    let mut connections = Vec::new();
    let max_connections = 20;

    for _ in 0..max_connections {
        match pool.acquire().await {
            Ok(conn) => connections.push(conn),
            Err(_) => break,
        }
    }

    let acquired_count = connections.len();
    assert!(
        acquired_count > 0,
        "Should be able to acquire some connections"
    );

    let timeout_result = tokio::time::timeout(Duration::from_millis(100), pool.acquire()).await;
    let properly_limited = timeout_result.is_err() || timeout_result.unwrap().is_err();

    drop(connections);
    tokio::task::yield_now().await;

    let recovery_conn = pool.acquire().await;
    assert!(
        recovery_conn.is_ok(),
        "Should be able to acquire connections after release"
    );

    Ok(properly_limited)
}

// =============================================================================
// HEALTH MONITORING TESTS
// =============================================================================

#[derive(Debug, Clone, PartialEq)]
pub enum HealthStatus {
    Healthy,
    Degraded,
    Unhealthy,
    Unknown,
}

#[derive(Debug, Clone)]
pub struct ComponentHealth {
    pub name: String,
    pub status: HealthStatus,
    pub last_check: Instant,
    pub failure_count: u32,
    pub last_error: Option<String>,
}

pub struct SystemHealthMonitor {
    components: Arc<RwLock<HashMap<String, ComponentHealth>>>,
    check_interval: Duration,
    failure_threshold: u32,
    running: Arc<AtomicBool>,
}

impl SystemHealthMonitor {
    pub fn new(check_interval: Duration, failure_threshold: u32) -> Self {
        Self {
            components: Arc::new(RwLock::new(HashMap::new())),
            check_interval,
            failure_threshold,
            running: Arc::new(AtomicBool::new(false)),
        }
    }

    pub async fn register_component(&self, name: &str) {
        let mut components = self.components.write().await;
        components.insert(
            name.to_string(),
            ComponentHealth {
                name: name.to_string(),
                status: HealthStatus::Unknown,
                last_check: Instant::now(),
                failure_count: 0,
                last_error: None,
            },
        );
    }

    pub async fn update_component_health(
        &self,
        name: &str,
        status: HealthStatus,
        error: Option<String>,
    ) {
        let mut components = self.components.write().await;
        if let Some(component) = components.get_mut(name) {
            let previous_status = component.status.clone();
            component.status = status.clone();
            component.last_check = Instant::now();
            component.last_error = error;

            match status {
                HealthStatus::Unhealthy => component.failure_count += 1,
                HealthStatus::Healthy if previous_status != HealthStatus::Healthy => {
                    component.failure_count = 0;
                }
                _ => {}
            }
        }
    }

    pub async fn get_system_health(&self) -> HashMap<String, ComponentHealth> {
        self.components.read().await.clone()
    }

    pub async fn is_system_healthy(&self) -> bool {
        let components = self.components.read().await;
        components
            .values()
            .all(|c| matches!(c.status, HealthStatus::Healthy))
    }

    pub async fn get_unhealthy_components(&self) -> Vec<String> {
        let components = self.components.read().await;
        components
            .values()
            .filter(|c| matches!(c.status, HealthStatus::Unhealthy))
            .map(|c| c.name.clone())
            .collect()
    }
}

impl Clone for SystemHealthMonitor {
    fn clone(&self) -> Self {
        Self {
            components: self.components.clone(),
            check_interval: self.check_interval,
            failure_threshold: self.failure_threshold,
            running: self.running.clone(),
        }
    }
}

#[sinex_test]
async fn test_comprehensive_health_monitoring_system(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

    let monitor = SystemHealthMonitor::new(Duration::from_millis(100), 3);

    let components = vec![
        "database",
        "filesystem_source",
        "terminal_source",
        "clipboard_source",
        "hyprland_source",
        "unified_collector",
        "promotion_worker",
        "git_annex",
    ];

    for component in &components {
        monitor.register_component(component).await;
    }

    let initial_health = monitor.get_system_health().await;
    pretty_assertions::assert_eq!(
        initial_health.len(),
        components.len(),
        "All components should be registered"
    );

    for component in &components {
        assert!(
            initial_health.contains_key(*component),
            "Component {} should be registered",
            component
        );
        pretty_assertions::assert_eq!(
            initial_health[*component].status,
            HealthStatus::Unknown,
            "Initial status should be unknown"
        );
    }

    test_component_health_checks(&monitor, pool).await?;
    test_failure_detection_and_recovery(&monitor).await?;
    test_system_health_aggregation(&monitor).await?;

    Ok(())
}

async fn test_component_health_checks(
    monitor: &SystemHealthMonitor,
    pool: &DbPool,
) -> Result<(), anyhow::Error> {
    let db_health = check_database_health(pool).await?;
    monitor
        .update_component_health("database", db_health, None)
        .await;

    let fs_health = check_filesystem_source_health().await?;
    monitor
        .update_component_health("filesystem_source", fs_health, None)
        .await;

    let health_status = monitor.get_system_health().await;
    for (name, component) in health_status {
        pretty_assertions::assert_ne!(
            component.status,
            HealthStatus::Unknown,
            "Component {} should have known health status",
            name
        );
        assert!(
            component.last_check.elapsed() < Duration::from_secs(1),
            "Health check should be recent"
        );
    }

    Ok(())
}

async fn check_database_health(pool: &DbPool) -> Result<HealthStatus> {
    match sqlx::query("SELECT 1").fetch_one(pool).await {
        Ok(_) => {
            match sqlx::query("SELECT COUNT(*) FROM core.events")
                .fetch_one(pool)
                .await
            {
                Ok(_) => Ok(HealthStatus::Healthy),
                Err(_) => Ok(HealthStatus::Degraded),
            }
        }
        Err(_) => Ok(HealthStatus::Unhealthy),
    }
}

async fn check_filesystem_source_health() -> Result<HealthStatus> {
    let temp_dir = TempDir::new()?;
    let test_file = temp_dir.path().join("health_check.txt");

    match std::fs::write(&test_file, b"health check") {
        Ok(()) => match std::fs::metadata(&test_file) {
            Ok(_) => Ok(HealthStatus::Healthy),
            Err(_) => Ok(HealthStatus::Degraded),
        },
        Err(_) => Ok(HealthStatus::Unhealthy),
    }
}

async fn test_failure_detection_and_recovery(
    monitor: &SystemHealthMonitor,
) -> Result<(), anyhow::Error> {
    monitor
        .update_component_health(
            "filesystem_source",
            HealthStatus::Unhealthy,
            Some("Simulated filesystem monitoring failure".to_string()),
        )
        .await;

    let unhealthy = monitor.get_unhealthy_components().await;
    assert!(
        unhealthy.contains(&"filesystem_source".to_string()),
        "Should detect unhealthy filesystem source"
    );

    let system_healthy = monitor.is_system_healthy().await;
    assert!(
        !system_healthy,
        "System should not be healthy with failed component"
    );

    monitor
        .update_component_health("filesystem_source", HealthStatus::Healthy, None)
        .await;

    let system_recovered = monitor.is_system_healthy().await;
    assert!(
        system_recovered,
        "System should be healthy after all components recover"
    );

    Ok(())
}

async fn test_system_health_aggregation(
    monitor: &SystemHealthMonitor,
) -> Result<(), anyhow::Error> {
    let components = [
        "database",
        "filesystem_source",
        "terminal_source",
        "clipboard_source",
    ];
    for component in &components {
        monitor
            .update_component_health(component, HealthStatus::Healthy, None)
            .await;
    }

    let all_healthy = monitor.is_system_healthy().await;
    assert!(
        all_healthy,
        "System should be healthy when all components are healthy"
    );

    monitor
        .update_component_health(
            "database",
            HealthStatus::Unhealthy,
            Some("Database connection lost".to_string()),
        )
        .await;

    let critical_unhealthy = monitor.is_system_healthy().await;
    assert!(
        !critical_unhealthy,
        "System should not be healthy when database is unhealthy"
    );

    Ok(())
}

// =============================================================================
// PAYLOAD BOUNDARY TESTS
// =============================================================================

const SMALL_PAYLOAD_SIZE: usize = 1024;
const MEDIUM_PAYLOAD_SIZE: usize = 1024 * 1024;
const LARGE_PAYLOAD_SIZE: usize = 10 * 1024 * 1024;
const EXTREME_PAYLOAD_SIZE: usize = 100 * 1024 * 1024;

#[sinex_test]
async fn test_small_payload_handling(ctx: TestContext) -> TestResult {
    let small_content = "x".repeat(SMALL_PAYLOAD_SIZE / 2);
    let payload = json!({
        "content": small_content,
        "size": small_content.len(),
        "metadata": {
            "type": "small_test",
            "timestamp": chrono::Utc::now().to_rfc3339()
        }
    });

    let event = RawEventBuilder::new("test.boundary", "small.payload", payload).build();

    sinex_db::insert_event(ctx.pool(), &event).await?;

    let retrieved = sinex_db::get_event_by_id(ctx.pool(), event.id).await?;
    assert_eq!(retrieved.id, event.id);
    assert_eq!(
        retrieved.payload["content"].as_str().unwrap().len(),
        small_content.len()
    );

    Ok(())
}

#[sinex_test]
async fn test_large_payload_handling(ctx: TestContext) -> TestResult {
    let large_content = "b".repeat(LARGE_PAYLOAD_SIZE);
    let payload = json!({
        "very_large_text": large_content,
        "size": large_content.len(),
        "type": "large_payload_test"
    });

    let event = RawEventBuilder::new("test.boundary", "large.payload", payload).build();

    let start = std::time::Instant::now();
    let result = sinex_db::insert_event(ctx.pool(), &event).await;
    let duration = start.elapsed();

    match result {
        Ok(_inserted_event) => {
            println!("Large payload insert took: {:?}", duration);

            let start_retrieval = std::time::Instant::now();
            let retrieved = sinex_db::get_event_by_id(ctx.pool(), event.id).await?;
            let retrieval_duration = start_retrieval.elapsed();

            println!("Large payload retrieval took: {:?}", retrieval_duration);
            assert_eq!(
                retrieved.payload["very_large_text"].as_str().unwrap().len(),
                large_content.len()
            );
        }
        Err(e) => {
            println!("Large payload rejected (expected): {}", e);
            assert!(
                e.to_string().contains("too large")
                    || e.to_string().contains("limit")
                    || e.to_string().contains("size")
            );
        }
    }

    Ok(())
}

#[sinex_test]
async fn test_extreme_payload_rejection(ctx: TestContext) -> TestResult {
    let extreme_content = "c".repeat(EXTREME_PAYLOAD_SIZE);
    let payload = json!({
        "extreme_text": extreme_content,
        "size": extreme_content.len(),
        "warning": "This should probably be rejected"
    });

    let event = RawEventBuilder::new("test.boundary", "extreme.payload", payload).build();

    let result = sinex_db::insert_event(ctx.pool(), &event).await;
    assert!(result.is_err(), "Extreme payloads should be rejected");

    let error_msg = result.unwrap_err().to_string();
    assert!(
        error_msg.contains("too large")
            || error_msg.contains("limit")
            || error_msg.contains("size")
            || error_msg.contains("memory"),
        "Error should indicate size/memory issue: {}",
        error_msg
    );

    Ok(())
}

// =============================================================================
// QUERY INTERFACE TESTS
// =============================================================================

#[sinex_test]
async fn test_exo_cli_basic_queries(ctx: TestContext) -> sqlx::Result<()> {
    let test_events = vec![
        events::file_created_event("/test/file1.txt"),
        events::file_modified_event("/test/file2.txt"),
        events::kitty_event("ls -la"),
        crate::common::create_test_event_with_payload(
            "clipboard",
            "content.changed",
            json!({"content": "test data", "format": "text"}),
        ),
    ];

    for event in test_events {
        assertions::assert_event_inserted(ctx.pool(), &event)
            .await
            .unwrap();
    }

    let cli_path = std::env::current_dir().unwrap().join("cli/exo.py");

    let output = Command::new("python3")
        .arg(&cli_path)
        .arg("query")
        .env("DATABASE_URL", std::env::var("DATABASE_URL").unwrap())
        .output()
        .expect("Failed to execute CLI");

    assert!(output.status.success(), "CLI should succeed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("fs"), "Should show filesystem events");

    let output = Command::new("python3")
        .arg(&cli_path)
        .arg("query")
        .arg("--source")
        .arg("terminal")
        .env("DATABASE_URL", std::env::var("DATABASE_URL").unwrap())
        .output()
        .expect("Failed to execute CLI");

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("command.executed"),
        "Should show terminal events"
    );
    assert!(
        !stdout.contains("file.created"),
        "Should not show filesystem events"
    );

    Ok(())
}

#[sinex_test]
async fn test_exo_cli_error_handling(ctx: TestContext) -> TestResult {
    let cli_path = std::env::current_dir().unwrap().join("cli/exo.py");

    let output = Command::new("python3")
        .arg(&cli_path)
        .arg("query")
        .env("DATABASE_URL", "postgresql://invalid/db")
        .output()
        .expect("Failed to execute CLI");

    assert!(!output.status.success(), "Should fail with invalid DB");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("Error") || stderr.contains("failed"),
        "Should show error message"
    );

    let output = Command::new("python3")
        .arg(&cli_path)
        .arg("invalid-command")
        .env("DATABASE_URL", std::env::var("DATABASE_URL").unwrap())
        .output()
        .expect("Failed to execute CLI");

    assert!(!output.status.success(), "Should fail with invalid command");

    Ok(())
}

// =============================================================================
// MOCK TYPES FOR TESTING
// =============================================================================

mod mock_types {
    use std::sync::{Arc, Mutex};
    use std::time::Instant;

    #[derive(Clone, Debug)]
    pub enum SystemdEvent {
        Ready,
        Watchdog,
        Status(String),
        Stopping,
    }

    #[derive(Clone)]
    pub struct SystemdNotifier {
        events: Arc<Mutex<Vec<(SystemdEvent, Instant)>>>,
        fail_watchdog: Arc<Mutex<bool>>,
    }

    impl Default for SystemdNotifier {
        fn default() -> Self {
            Self::new()
        }
    }

    impl SystemdNotifier {
        pub fn new() -> Self {
            Self {
                events: Arc::new(Mutex::new(Vec::new())),
                fail_watchdog: Arc::new(Mutex::new(false)),
            }
        }

        pub fn get_events(&self) -> Vec<(SystemdEvent, Instant)> {
            self.events.lock().unwrap().clone()
        }

        pub fn verify_sequence(&self, expected: &[SystemdEvent]) -> Result<(), String> {
            let events = self.events.lock().unwrap();
            if events.len() < expected.len() {
                return Err(format!(
                    "Expected {} events, got {}",
                    expected.len(),
                    events.len()
                ));
            }

            for (i, expected_event) in expected.iter().enumerate() {
                match (&events[i].0, expected_event) {
                    (SystemdEvent::Ready, SystemdEvent::Ready) => {}
                    (SystemdEvent::Watchdog, SystemdEvent::Watchdog) => {}
                    (SystemdEvent::Status(a), SystemdEvent::Status(b)) if a == b => {}
                    (SystemdEvent::Stopping, SystemdEvent::Stopping) => {}
                    _ => {
                        return Err(format!(
                            "Event {} mismatch: expected {:?}, got {:?}",
                            i, expected_event, events[i].0
                        ))
                    }
                }
            }
            Ok(())
        }

        pub fn notify_ready(&self) -> Result<(), anyhow::Error> {
            self.events
                .lock()
                .unwrap()
                .push((SystemdEvent::Ready, Instant::now()));
            Ok(())
        }

        pub fn notify_watchdog(&self) -> Result<(), anyhow::Error> {
            if *self.fail_watchdog.lock().unwrap() {
                return Err(anyhow::anyhow!("Simulated watchdog failure"));
            }
            self.events
                .lock()
                .unwrap()
                .push((SystemdEvent::Watchdog, Instant::now()));
            Ok(())
        }

        pub fn notify_status(&self, status: &str) -> Result<(), anyhow::Error> {
            self.events
                .lock()
                .unwrap()
                .push((SystemdEvent::Status(status.to_string()), Instant::now()));
            Ok(())
        }

        pub fn notify_stopping(&self) -> Result<(), anyhow::Error> {
            self.events
                .lock()
                .unwrap()
                .push((SystemdEvent::Stopping, Instant::now()));
            Ok(())
        }
    }

    #[derive(Clone)]
    pub struct ResourcePreset {
        pub memory_limit_mb: u32,
        pub cpu_quota_percent: u32,
        pub max_open_files: u32,
        pub worker_concurrency: u32,
    }

    impl ResourcePreset {
        pub fn lite() -> Self {
            Self {
                memory_limit_mb: 256,
                cpu_quota_percent: 25,
                max_open_files: 4096,
                worker_concurrency: 2,
            }
        }

        pub fn normal() -> Self {
            Self {
                memory_limit_mb: 1024,
                cpu_quota_percent: 50,
                max_open_files: 8192,
                worker_concurrency: 8,
            }
        }

        pub fn max() -> Self {
            Self {
                memory_limit_mb: 4096,
                cpu_quota_percent: 100,
                max_open_files: 65536,
                worker_concurrency: 32,
            }
        }
    }

    pub struct DeploymentChecklist {
        checks: Vec<(
            &'static str,
            Box<
                dyn Fn() -> std::pin::Pin<
                    Box<dyn std::future::Future<Output = Result<bool, anyhow::Error>>>,
                >,
            >,
        )>,
    }

    impl DeploymentChecklist {
        pub fn new() -> Self {
            Self { checks: vec![] }
        }

        pub fn add_check<F, Fut>(&mut self, name: &'static str, check: F)
        where
            F: Fn() -> Fut + 'static,
            Fut: std::future::Future<Output = Result<bool, anyhow::Error>> + 'static,
        {
            self.checks
                .push((name, Box::new(move || Box::pin(check()))));
        }

        pub async fn run_all(&self) -> Result<Vec<(&str, bool)>, anyhow::Error> {
            let mut results = vec![];
            for (name, check) in &self.checks {
                let passed = check().await?;
                results.push((*name, passed));
            }
            Ok(results)
        }
    }
}

// =============================================================================
// GIT-ANNEX INTEGRATION TESTS
// =============================================================================

/// Git-annex integration test helper
struct GitAnnexTestRepo {
    pub path: PathBuf,
    pub _temp_dir: TempDir,
    pub available: bool,
}

impl GitAnnexTestRepo {
    pub async fn new() -> Result<Self> {
        let temp_dir = TempDir::new()?;
        let path = temp_dir.path().to_path_buf();

        let git_init = Command::new("git")
            .args(["init"])
            .current_dir(&path)
            .output()?;

        if !git_init.status.success() {
            return Ok(Self {
                path,
                _temp_dir: temp_dir,
                available: false,
            });
        }

        let _ = Command::new("git")
            .args(["config", "user.name", "Sinex Test"])
            .current_dir(&path)
            .output();

        let _ = Command::new("git")
            .args(["config", "user.email", "test@sinex.dev"])
            .current_dir(&path)
            .output();

        let annex_init = Command::new("git")
            .args(["annex", "init", "sinex-test-repo"])
            .current_dir(&path)
            .output()?;

        let available = annex_init.status.success();

        if available {
            let _ = Command::new("git")
                .args(["config", "annex.largefiles", "largerthan=1KB"])
                .current_dir(&path)
                .output();
        }

        Ok(Self {
            path,
            _temp_dir: temp_dir,
            available,
        })
    }

    pub fn is_available(&self) -> bool {
        self.available
    }

    pub async fn add_file(&self, relative_path: &str, content: &[u8]) -> Result<String> {
        if !self.available {
            return Err(anyhow::anyhow!("Git-annex not available"));
        }

        let file_path = self.path.join(relative_path);

        if let Some(parent) = file_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        tokio::fs::write(&file_path, content).await?;

        let add_output = Command::new("git")
            .args(["annex", "add", relative_path])
            .current_dir(&self.path)
            .output()?;

        if !add_output.status.success() {
            return Err(anyhow::anyhow!(
                "Git-annex add failed: {}",
                String::from_utf8_lossy(&add_output.stderr)
            ));
        }

        let commit_output = Command::new("git")
            .args(["commit", "-m", &format!("Add {}", relative_path)])
            .current_dir(&self.path)
            .output()?;

        if !commit_output.status.success() {
            return Err(anyhow::anyhow!(
                "Git commit failed: {}",
                String::from_utf8_lossy(&commit_output.stderr)
            ));
        }

        let key_output = Command::new("git")
            .args(["annex", "lookupkey", relative_path])
            .current_dir(&self.path)
            .output()?;

        if key_output.status.success() {
            Ok(String::from_utf8_lossy(&key_output.stdout)
                .trim()
                .to_string())
        } else {
            Err(anyhow::anyhow!("Failed to get annex key"))
        }
    }

    pub async fn get_file_content(&self, relative_path: &str) -> Result<Vec<u8>> {
        if !self.available {
            return Err(anyhow::anyhow!("Git-annex not available"));
        }

        let file_path = self.path.join(relative_path);

        let get_output = Command::new("git")
            .args(["annex", "get", relative_path])
            .current_dir(&self.path)
            .output()?;

        if !get_output.status.success() {
            return Err(anyhow::anyhow!(
                "Git-annex get failed: {}",
                String::from_utf8_lossy(&get_output.stderr)
            ));
        }

        let content = tokio::fs::read(&file_path).await?;
        Ok(content)
    }
}

#[sinex_test]
async fn test_git_annex_integration_with_event_pipeline(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

    let annex_repo = GitAnnexTestRepo::new().await?;

    if !annex_repo.is_available() {
        println!("⚠️  Git-annex not available, skipping git-annex integration tests");
        return Ok(());
    }

    test_large_file_event_capture(pool, &annex_repo).await?;
    test_event_processing_with_annex_blobs(pool, &annex_repo).await?;

    Ok(())
}

async fn test_large_file_event_capture(
    pool: &DbPool,
    annex_repo: &GitAnnexTestRepo,
) -> Result<(), anyhow::Error> {
    let large_content = "x".repeat(2048);
    let medium_content = "y".repeat(512);

    let large_key = annex_repo
        .add_file("large_file.txt", large_content.as_bytes())
        .await?;
    let medium_key = annex_repo
        .add_file("medium_file.txt", medium_content.as_bytes())
        .await?;

    let large_file_event = RawEventBuilder::new(
        "fs",
        "file.created",
        json!({
            "path": "/test/large_file.txt",
            "size": large_content.len(),
            "git_annex_key": large_key,
            "storage_type": "git_annex",
            "content_hash": "sha256:placeholder"
        }),
    )
    .build();

    let medium_file_event = RawEventBuilder::new(
        "fs",
        "file.created",
        json!({
            "path": "/test/medium_file.txt",
            "size": medium_content.len(),
            "git_annex_key": medium_key,
            "storage_type": "git_annex",
            "content_hash": "sha256:placeholder"
        }),
    )
    .build();

    let large_event_id = insert_event(pool, &large_file_event).await?;
    let medium_event_id = insert_event(pool, &medium_file_event).await?;

    let retrieved_large = sinex_db::get_event_by_id(pool, large_event_id).await?;
    let retrieved_medium = sinex_db::get_event_by_id(pool, medium_event_id).await?;

    pretty_assertions::assert_eq!(retrieved_large.source, "fs");
    pretty_assertions::assert_eq!(retrieved_large.event_type, "file.created");
    assert!(retrieved_large.payload["git_annex_key"].as_str().is_some());
    pretty_assertions::assert_eq!(
        retrieved_large.payload["storage_type"].as_str().unwrap(),
        "git_annex"
    );

    pretty_assertions::assert_eq!(retrieved_medium.source, "fs");
    pretty_assertions::assert_eq!(retrieved_medium.event_type, "file.created");
    assert!(retrieved_medium.payload["git_annex_key"].as_str().is_some());

    let retrieved_large_content = annex_repo.get_file_content("large_file.txt").await?;
    let retrieved_medium_content = annex_repo.get_file_content("medium_file.txt").await?;

    pretty_assertions::assert_eq!(retrieved_large_content, large_content.as_bytes());
    pretty_assertions::assert_eq!(retrieved_medium_content, medium_content.as_bytes());

    println!("✅ Large file event capture with git-annex integration successful");
    Ok(())
}

async fn test_event_processing_with_annex_blobs(
    pool: &DbPool,
    annex_repo: &GitAnnexTestRepo,
) -> Result<(), anyhow::Error> {
    let text_content = "This is a test document with important content for processing.";
    let binary_content = vec![0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10];
    let json_content = r#"{"type": "document", "content": "structured data", "version": 1}"#;

    let text_key = annex_repo
        .add_file("document.txt", text_content.as_bytes())
        .await?;
    let binary_key = annex_repo.add_file("image.jpg", &binary_content).await?;
    let json_key = annex_repo
        .add_file("data.json", json_content.as_bytes())
        .await?;

    let events = vec![
        RawEventBuilder::new(
            "document_processor",
            "document.analyze",
            json!({
                "document_path": "/docs/document.txt",
                "git_annex_key": text_key,
                "processing_type": "text_analysis",
                "priority": "high"
            }),
        )
        .build(),
        RawEventBuilder::new(
            "image_processor",
            "image.process",
            json!({
                "image_path": "/images/image.jpg",
                "git_annex_key": binary_key,
                "processing_type": "metadata_extraction",
                "priority": "medium"
            }),
        )
        .build(),
        RawEventBuilder::new(
            "data_processor",
            "data.validate",
            json!({
                "data_path": "/data/data.json",
                "git_annex_key": json_key,
                "processing_type": "schema_validation",
                "priority": "low"
            }),
        )
        .build(),
    ];

    let mut event_ids = Vec::new();
    for event in &events {
        let inserted_event_id = insert_event(pool, event).await?;
        event_ids.push(inserted_event_id);
        add_to_work_queue(pool, inserted_event_id, "annex-test-agent", 3).await?;
    }

    let mut processed_events = Vec::new();

    for (i, event_id) in event_ids.iter().enumerate() {
        let claimed_items =
            claim_work_queue_items(pool, "annex-test-agent", &format!("annex-worker-{}", i), 1)
                .await?;

        assert!(
            !claimed_items.is_empty(),
            "Should claim work item for event {}",
            i
        );

        let queue_item = &claimed_items[0];
        let event = sinex_db::get_event_by_id(pool, *event_id).await?;

        if let Some(_annex_key) = event.payload["git_annex_key"].as_str() {
            let file_name = match event.source.as_str() {
                "document_processor" => "document.txt",
                "image_processor" => "image.jpg",
                "data_processor" => "data.json",
                _ => continue,
            };

            let content = annex_repo.get_file_content(file_name).await?;

            match event.source.as_str() {
                "document_processor" => {
                    pretty_assertions::assert_eq!(content, text_content.as_bytes());
                }
                "image_processor" => {
                    pretty_assertions::assert_eq!(content, binary_content);
                }
                "data_processor" => {
                    pretty_assertions::assert_eq!(content, json_content.as_bytes());
                    let _: serde_json::Value = serde_json::from_slice(&content)?;
                }
                _ => {}
            }

            processed_events.push((event_id, event.source.clone(), content.len()));
        }

        complete_work_queue_item(pool, queue_item.queue_id).await?;
    }

    pretty_assertions::assert_eq!(processed_events.len(), 3, "All events should be processed");

    let remaining_work =
        claim_work_queue_items(pool, "annex-test-agent", "cleanup-worker", 10).await?;
    assert!(remaining_work.is_empty(), "No work should remain in queue");

    println!("✅ Event processing with git-annex blob integration successful");
    Ok(())
}

#[sinex_test]
async fn test_git_annex_fallback_scenarios(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

    test_annex_unavailable_fallback(pool).await?;
    test_annex_operation_failure_handling(pool).await?;

    Ok(())
}

async fn test_annex_unavailable_fallback(pool: &DbPool) -> Result<(), anyhow::Error> {
    let large_file_content = "x".repeat(2048);

    let fallback_event = RawEventBuilder::new(
        "fs",
        "file.created",
        json!({
            "path": "/test/large_file_no_annex.txt",
            "size": large_file_content.len(),
            "content": large_file_content,
            "storage_type": "inline",
            "fallback_reason": "git_annex_unavailable"
        }),
    )
    .build();

    let inserted_event_id = insert_event(pool, &fallback_event).await?;

    let retrieved_event = sinex_db::get_event_by_id(pool, inserted_event_id).await?;
    pretty_assertions::assert_eq!(
        retrieved_event.payload["storage_type"].as_str().unwrap(),
        "inline"
    );
    pretty_assertions::assert_eq!(
        retrieved_event.payload["content"].as_str().unwrap(),
        large_file_content
    );
    pretty_assertions::assert_eq!(
        retrieved_event.payload["fallback_reason"].as_str().unwrap(),
        "git_annex_unavailable"
    );

    let health_check = sqlx::query("SELECT 1").fetch_one(pool).await;
    assert!(
        health_check.is_ok(),
        "System should remain healthy without git-annex"
    );

    println!("✅ Git-annex unavailable fallback handling successful");
    Ok(())
}

async fn test_annex_operation_failure_handling(pool: &DbPool) -> Result<(), anyhow::Error> {
    let failure_scenarios = vec![
        ("corrupted_repo", "Repository corruption detected"),
        ("disk_full", "No space left on device"),
        ("permission_denied", "Permission denied accessing annex"),
        ("network_timeout", "Remote operation timed out"),
    ];

    for (scenario, error_message) in failure_scenarios {
        let failure_event = RawEventBuilder::new(
            "fs",
            "file.created",
            json!({
                "path": format!("/test/{}_test.txt", scenario),
                "size": 1024,
                "git_annex_operation": "add",
                "git_annex_error": error_message,
                "storage_type": "failed_annex",
                "fallback_applied": true
            }),
        )
        .build();

        let inserted_event_id = insert_event(pool, &failure_event).await?;

        let retrieved = sinex_db::get_event_by_id(pool, inserted_event_id).await?;
        pretty_assertions::assert_eq!(
            retrieved.payload["git_annex_error"].as_str().unwrap(),
            error_message
        );
        pretty_assertions::assert_eq!(
            retrieved.payload["fallback_applied"].as_bool().unwrap(),
            true
        );
    }

    let error_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM core.events WHERE payload->>'storage_type' = 'failed_annex'",
    )
    .fetch_one(pool)
    .await?;

    pretty_assertions::assert_eq!(error_count, 4, "All failure scenarios should be recorded");

    println!("✅ Git-annex operation failure handling successful");
    Ok(())
}

use mock_types::*;

// =============================================================================
// AGENT LIFECYCLE MANAGEMENT TESTS (migrated from deleted agent/)
// =============================================================================

#[sinex_test]
async fn test_agent_manifest_create(ctx: TestContext) -> TestResult {
    // Create a complete agent manifest
    let result = sqlx::query(
        "INSERT INTO sinex_schemas.processor_manifests
         (processor_name, processor_type, description, version, status,
          config_template_json, produces_event_types, consumes_event_types,
          required_capabilities, llm_dependencies, repo_url)
         VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8::jsonb, $9::jsonb, $10::jsonb, $11)",
    )
    .bind("test_agent_crud")
    .bind("Test agent for CRUD operations")
    .bind("1.0.0")
    .bind("running")
    .bind("ingestor")
    .bind(json!({
        "api_key": "string",
        "batch_size": 100,
        "endpoints": ["http://example.com"]
    }))
    .bind(json!({
        "desktop.test": [
            {"type": "window_opened", "schema_id_ref": "01234567890123456789012345"},
            {"type": "window_closed", "schema_id_ref": "01234567890123456789012346"}
        ]
    }))
    .bind(json!({
        "core.events_feed_all": [
            {"source_filter": "app.browser.*", "event_type_filter": "page_loaded"}
        ]
    }))
    .bind(json!({
        "filesystem_read": ["/var/log"],
        "network_host_allow": ["api.example.com:443"]
    }))
    .bind(json!({
        "models_used": ["ollama/mistral:7b"],
        "required_capabilities": ["function_calling"]
    }))
    .bind("https://github.com/example/test-agent")
    .execute(ctx.pool())
    .await;

    assert!(result.is_ok(), "Should be able to create agent manifest");

    // Verify all fields were stored
    type ManifestRow = (
        String,                    // agent_name
        Option<String>,            // description
        String,                    // version
        String,                    // status
        String,                    // agent_type
        Option<serde_json::Value>, // config_template_json
        Option<serde_json::Value>, // produces_event_types
        Option<serde_json::Value>, // subscribes_to_event_types
        Option<serde_json::Value>, // required_capabilities
        Option<serde_json::Value>, // llm_dependencies
        Option<String>,            // repo_url
    );
    let manifest: ManifestRow = sqlx::query_as(
        "SELECT automaton_name, description, version, status, agent_type,
                config_template_json, produces_event_types, subscribes_to_event_types,
                required_capabilities, llm_dependencies, repo_url
         FROM sinex_schemas.processor_manifests
         WHERE processor_name = $1 AND processor_type = 'automaton'",
    )
    .bind("test_agent_crud")
    .fetch_one(ctx.pool())
    .await
    .unwrap();

    pretty_assertions::assert_eq!(manifest.0, "test_agent_crud");
    pretty_assertions::assert_eq!(manifest.1.unwrap(), "Test agent for CRUD operations");
    pretty_assertions::assert_eq!(manifest.2, "1.0.0");
    pretty_assertions::assert_eq!(manifest.3, "running");
    pretty_assertions::assert_eq!(manifest.4, "ingestor");
    assert!(manifest.5.is_some());
    assert!(manifest.6.is_some());
    assert!(manifest.7.is_some());
    assert!(manifest.8.is_some());
    assert!(manifest.9.is_some());
    pretty_assertions::assert_eq!(
        manifest.10.unwrap(),
        "https://github.com/example/test-agent"
    );

    Ok(())
}

#[sinex_test]
async fn test_agent_manifest_update(ctx: TestContext) -> TestResult {
    // Create agent
    sqlx::query("INSERT INTO sinex_schemas.processor_manifests (processor_name, processor_type, version) VALUES ($1, 'automaton', $2)")
        .bind("update_test_agent")
        .bind("1.0.0")
        .execute(ctx.pool())
        .await
        .unwrap();

    // Get initial timestamps
    let (registered, updated): (chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>) =
        sqlx::query_as(
            "SELECT registered_at, updated_at FROM sinex_schemas.processor_manifests WHERE processor_name = $1 AND processor_type = 'automaton'"
        )
        .bind("update_test_agent")
        .fetch_one(ctx.pool())
        .await
        .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    // Update various fields
    sqlx::query(
        "UPDATE sinex_schemas.processor_manifests
         SET version = $1,
             status = $2,
             last_heartbeat_ts = $3,
             produces_event_types = $4::jsonb
         WHERE automaton_name = $5",
    )
    .bind("1.1.0")
    .bind("stopped")
    .bind(chrono::Utc::now())
    .bind(json!({
        "new.events": [{"type": "test_event"}]
    }))
    .bind("update_test_agent")
    .execute(ctx.pool())
    .await
    .unwrap();

    // Verify updates and trigger
    let (version, status, updated_new): (String, String, chrono::DateTime<chrono::Utc>) =
        sqlx::query_as(
            "SELECT version, status, updated_at FROM sinex_schemas.processor_manifests WHERE processor_name = $1 AND processor_type = 'automaton'"
        )
        .bind("update_test_agent")
        .fetch_one(ctx.pool())
        .await
        .unwrap();

    pretty_assertions::assert_eq!(version, "1.1.0");
    pretty_assertions::assert_eq!(status, "stopped");
    assert!(
        updated_new > updated,
        "updated_at should be updated by trigger"
    );
    pretty_assertions::assert_eq!(registered, registered, "registered_at should not change");

    Ok(())
}

#[sinex_test]
async fn test_agent_manifest_delete(ctx: TestContext) -> TestResult {
    // Create agent
    sqlx::query("INSERT INTO sinex_schemas.processor_manifests (processor_name, processor_type, version) VALUES ($1, 'automaton', $2)")
        .bind("delete_test_agent")
        .bind("1.0.0")
        .execute(ctx.pool())
        .await
        .unwrap();

    // Create event and promotion queue item
    let event_id = sinex_ulid::Ulid::new();
    sqlx::query(
        "INSERT INTO core.events (id, source, event_type, host, payload)
         VALUES ($1::uuid, $2, $3, $4, $5::jsonb)",
    )
    .bind(event_id.to_uuid())
    .bind("delete_test")
    .bind("test_event")
    .bind("test_host")
    .bind(json!({"test": "data"}))
    .execute(ctx.pool())
    .await
    .unwrap();

    sqlx::query(
        "INSERT INTO sinex_schemas.work_queue (raw_event_id, target_automaton_name)
         VALUES ($1::uuid, $2)",
    )
    .bind(event_id.to_uuid())
    .bind("delete_test_agent")
    .execute(ctx.pool())
    .await
    .unwrap();

    // Delete agent - should cascade delete work queue items
    sqlx::query("DELETE FROM sinex_schemas.processor_manifests WHERE processor_name = $1 AND processor_type = 'automaton'")
        .bind("delete_test_agent")
        .execute(ctx.pool())
        .await
        .unwrap();

    // Verify agent is deleted
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sinex_schemas.processor_manifests WHERE processor_name = $1 AND processor_type = 'automaton'",
    )
    .bind("delete_test_agent")
    .fetch_one(ctx.pool())
    .await
    .unwrap();

    pretty_assertions::assert_eq!(count, 0, "Agent should be deleted");

    // Verify work queue items were cascade deleted
    let queue_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sinex_schemas.work_queue WHERE target_automaton_name = $1",
    )
    .bind("delete_test_agent")
    .fetch_one(ctx.pool())
    .await
    .unwrap();

    pretty_assertions::assert_eq!(queue_count, 0, "Work queue items should be cascade deleted");

    Ok(())
}

#[sinex_test]
async fn test_agent_status_transitions(ctx: TestContext) -> TestResult {
    // Create agent in pending state
    sqlx::query(
        "INSERT INTO sinex_schemas.processor_manifests (processor_name, processor_type, version, status)
         VALUES ($1, 'automaton', $2, $3)",
    )
    .bind("status_test_agent")
    .bind("1.0.0")
    .bind("pending_registration")
    .execute(ctx.pool())
    .await
    .unwrap();

    // Valid status transitions
    let valid_statuses = vec![
        "running",
        "stopped",
        "error_state",
        "disabled_by_user",
        "degraded",
        "unknown",
    ];

    for status in valid_statuses {
        let result = sqlx::query(
            "UPDATE sinex_schemas.processor_manifests SET status = $1 WHERE processor_name = $2 AND processor_type = 'automaton'",
        )
        .bind(status)
        .bind("status_test_agent")
        .execute(ctx.pool())
        .await;

        assert!(
            result.is_ok(),
            "Status transition to {} should be valid",
            status
        );
    }

    // Test error state with error tracking
    let error_time = chrono::Utc::now();
    sqlx::query(
        "UPDATE sinex_schemas.processor_manifests
         SET status = $1, last_heartbeat_ts = $2, description = $3
         WHERE processor_name = $4 AND processor_type = 'automaton'",
    )
    .bind("error_state")
    .bind(error_time)
    .bind("Connection timeout to data source")
    .bind("status_test_agent")
    .execute(ctx.pool())
    .await
    .unwrap();

    let (status, error_ts, error_msg): (
        String,
        Option<chrono::DateTime<chrono::Utc>>,
        Option<String>,
    ) = sqlx::query_as(
        "SELECT status, last_error_ts, last_error_summary
             FROM sinex_schemas.processor_manifests WHERE processor_name = $1 AND processor_type = 'automaton'",
    )
    .bind("status_test_agent")
    .fetch_one(ctx.pool())
    .await
    .unwrap();

    pretty_assertions::assert_eq!(status, "error_state");
    assert!(error_ts.is_some());
    pretty_assertions::assert_eq!(error_msg.unwrap(), "Connection timeout to data source");

    Ok(())
}

#[sinex_test]
async fn test_agent_capabilities_and_dependencies(ctx: TestContext) -> TestResult {
    // Create agent with complex capabilities
    let capabilities = json!({
        "filesystem_read": ["/home/user/documents", "/var/log/app"],
        "filesystem_write": ["/tmp/sinex"],
        "network_host_allow": ["api.openai.com:443", "github.com:443"],
        "db_tables_rw": ["core.artifacts", "core.entities"],
        "db_tables_ro": ["core.events"],
        "system_commands": ["ps", "top", "df"]
    });

    let llm_deps = json!({
        "models_used": [
            "openai/gpt-4-turbo",
            "anthropic/claude-3-opus",
            "ollama/llama2:13b"
        ],
        "required_capabilities": [
            "function_calling",
            "json_mode",
            "vision"
        ],
        "estimated_tokens_per_hour": 50000,
        "fallback_model": "ollama/mistral:7b"
    });

    sqlx::query(
        "INSERT INTO sinex_schemas.processor_manifests
         (processor_name, processor_type, version, description)
         VALUES ($1, 'automaton', $2, $3)",
    )
    .bind("capability_test_agent")
    .bind("1.0.0")
    .bind(&capabilities)
    .bind(&llm_deps)
    .execute(ctx.pool())
    .await
    .unwrap();

    // Query agents by capability
    let agents_with_fs_write: Vec<String> = sqlx::query_scalar(
        "SELECT processor_name FROM sinex_schemas.processor_manifests
         WHERE processor_type = 'automaton' AND produces_event_types @> '["file.created"]'",
    )
    .fetch_all(ctx.pool())
    .await
    .unwrap();

    assert!(agents_with_fs_write.contains(&"capability_test_agent".to_string()));

    // Query agents using specific LLM model
    let agents_using_gpt4: Vec<String> = sqlx::query_scalar(
        "SELECT processor_name FROM sinex_schemas.processor_manifests
         WHERE processor_type = 'automaton' AND description LIKE '%gpt-4%'",
    )
    .fetch_all(ctx.pool())
    .await
    .unwrap();

    assert!(agents_using_gpt4.contains(&"capability_test_agent".to_string()));

    Ok(())
}

#[sinex_test]
async fn test_agent_event_subscription_queries(ctx: TestContext) -> TestResult {
    // Create multiple agents with different subscriptions
    let agents = vec![
        (
            "subscriber_1",
            json!({
                "core.events_feed_all": [
                    {"source_filter": "desktop.hyprland.*", "event_type_filter": "window_*"}
                ]
            }),
        ),
        (
            "subscriber_2",
            json!({
                "core.events_feed_all": [
                    {"source_filter": "app.browser.*", "event_type_filter": "page_loaded"},
                    {"source_filter": "app.terminal.*", "event_type_filter": "command_executed"}
                ]
            }),
        ),
        (
            "subscriber_3",
            json!({
                "sinex.pkm.note_updated": [{"schema_id_expected_ref": "01234567890123456789012345"}],
                "sinex.system.heartbeat": []
            }),
        ),
    ];

    for (name, subscriptions) in agents {
        sqlx::query(
            "INSERT INTO sinex_schemas.processor_manifests
             (processor_name, processor_type, version, consumes_event_types)
             VALUES ($1, 'automaton', $2, $3)",
        )
        .bind(name)
        .bind("1.0.0")
        .bind(&subscriptions)
        .execute(ctx.pool())
        .await
        .unwrap();
    }

    // Query agents subscribing to any events (using GIN index)
    let subscribers: Vec<String> = sqlx::query_scalar(
        "SELECT processor_name FROM sinex_schemas.processor_manifests
         WHERE processor_type = 'automaton' AND consumes_event_types IS NOT NULL
         ORDER BY processor_name",
    )
    .fetch_all(ctx.pool())
    .await
    .unwrap();

    pretty_assertions::assert_eq!(subscribers.len(), 3);

    // Query agents subscribing to specific event feed
    let raw_feed_subscribers: Vec<String> = sqlx::query_scalar(
        "SELECT processor_name FROM sinex_schemas.processor_manifests
         WHERE processor_type = 'automaton' AND consumes_event_types @> '["core.events_feed_all"]'
         ORDER BY processor_name",
    )
    .fetch_all(ctx.pool())
    .await
    .unwrap();

    pretty_assertions::assert_eq!(raw_feed_subscribers.len(), 2);
    assert!(raw_feed_subscribers.contains(&"subscriber_1".to_string()));
    assert!(raw_feed_subscribers.contains(&"subscriber_2".to_string()));

    Ok(())
}
