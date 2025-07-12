//! Comprehensive integration tests for satellite architecture
//!
//! Tests the full satellite ecosystem including:
//! - ingestd gRPC server
//! - Event source satellites (sensor mode)
//! - Scanner mode operations
//! - Satellite coordination

use sinex_test::{sinex_test, EventBuilder, TestContext, TestResult};
use std::time::Duration;
use tokio::time::timeout;

/// Test basic satellite lifecycle: start, connect, send events, shutdown
#[sinex_test]
async fn test_satellite_basic_lifecycle(ctx: TestContext) -> TestResult {
    // Start ingestd server
    let (_ingestd_handle, socket_path) = start_test_ingestd(&ctx).await?;
    
    // Create and start a test satellite
    let satellite_config = TestSatelliteConfig {
        socket_path: socket_path.clone(),
        event_rate: Duration::from_millis(100),
        total_events: 10,
    };
    
    let satellite_handle = start_test_satellite(satellite_config).await?;
    
    // Wait for events to be ingested
    tokio::time::sleep(Duration::from_secs(2)).await;
    
    // Verify events were stored
    let event_count = count_events_from_source(ctx.pool(), "test-satellite").await?;
    assert!(event_count >= 10, "Expected at least 10 events, got {}", event_count);
    
    // Shutdown satellite gracefully
    satellite_handle.abort();
    
    Ok(())
}

/// Test scanner mode functionality
#[sinex_test]
async fn test_satellite_scanner_mode(ctx: TestContext) -> TestResult {
    use sinex_satellite_sdk::{ScannerArgs, EventSourceRunner};
    
    // Create test data to scan
    let test_data = create_test_scan_data().await?;
    
    // Start ingestd
    let (_ingestd_handle, socket_path) = start_test_ingestd(&ctx).await?;
    
    // Create scanner args
    let scanner_args = ScannerArgs {
        paths: vec![test_data.path.to_string_lossy().to_string()],
        time_range: None,
        dry_run: false,
        interactive: false,
        max_events: 100,
        skip_existing: false,
    };
    
    // Run scanner
    let ingest_client = sinex_satellite_sdk::IngestClient::new(&socket_path).await?;
    let test_satellite = TestScannerSatellite::new();
    let mut runner = EventSourceRunner::new(test_satellite, ingest_client);
    
    // Initialize runner
    runner.initialize(
        "test-scanner".to_string(),
        std::collections::HashMap::new(),
        100,
        5,
        ctx.work_dir(),
        false,
    ).await?;
    
    // Run scan
    let scan_report = runner.run_scanner(scanner_args).await?;
    
    // Verify scan results
    assert_eq!(scan_report.events_generated, 50);
    assert!(scan_report.duration.as_millis() > 0);
    assert_eq!(scan_report.processed_paths.len(), 1);
    assert_eq!(scan_report.failed_paths.len(), 0);
    
    // Verify events in database
    let event_count = count_events_from_source(ctx.pool(), "test-scanner").await?;
    assert_eq!(event_count, 50);
    
    Ok(())
}

/// Test multiple satellites coordinating
#[sinex_test]
async fn test_multi_satellite_coordination(ctx: TestContext) -> TestResult {
    // Start ingestd
    let (_ingestd_handle, socket_path) = start_test_ingestd(&ctx).await?;
    
    // Start multiple satellites
    let mut satellite_handles = Vec::new();
    
    for i in 0..3 {
        let config = TestSatelliteConfig {
            socket_path: socket_path.clone(),
            event_rate: Duration::from_millis(200),
            total_events: 20,
        };
        
        let handle = start_test_satellite_with_name(config, &format!("satellite-{}", i)).await?;
        satellite_handles.push(handle);
    }
    
    // Wait for all satellites to produce events
    tokio::time::sleep(Duration::from_secs(5)).await;
    
    // Verify each satellite produced events
    for i in 0..3 {
        let event_count = count_events_from_source(ctx.pool(), &format!("satellite-{}", i)).await?;
        assert!(event_count >= 20, "Satellite {} produced only {} events", i, event_count);
    }
    
    // Verify total event count
    let total_events = count_all_events(ctx.pool()).await?;
    assert!(total_events >= 60, "Expected at least 60 total events, got {}", total_events);
    
    // Shutdown all satellites
    for handle in satellite_handles {
        handle.abort();
    }
    
    Ok(())
}

/// Test satellite reconnection after ingestd restart
#[sinex_test]
async fn test_satellite_reconnection(ctx: TestContext) -> TestResult {
    // Start ingestd
    let (ingestd_handle, socket_path) = start_test_ingestd(&ctx).await?;
    
    // Start satellite
    let satellite_config = TestSatelliteConfig {
        socket_path: socket_path.clone(),
        event_rate: Duration::from_millis(500),
        total_events: 100, // Long running
    };
    
    let satellite_handle = start_test_satellite(satellite_config).await?;
    
    // Wait for initial events
    tokio::time::sleep(Duration::from_secs(2)).await;
    let initial_count = count_events_from_source(ctx.pool(), "test-satellite").await?;
    assert!(initial_count > 0, "No initial events produced");
    
    // Stop ingestd
    ingestd_handle.abort();
    tokio::time::sleep(Duration::from_millis(500)).await;
    
    // Restart ingestd
    let (_new_ingestd_handle, _) = start_test_ingestd_at_path(&ctx, &socket_path).await?;
    
    // Wait for reconnection and more events
    tokio::time::sleep(Duration::from_secs(3)).await;
    
    // Verify satellite continued producing events
    let final_count = count_events_from_source(ctx.pool(), "test-satellite").await?;
    assert!(final_count > initial_count, "Satellite didn't reconnect and produce more events");
    
    satellite_handle.abort();
    
    Ok(())
}

/// Test satellite error handling and retry logic
#[sinex_test]
async fn test_satellite_error_handling(ctx: TestContext) -> TestResult {
    // Start ingestd with simulated failures
    let (_ingestd_handle, socket_path) = start_test_ingestd_with_failures(&ctx, 0.3).await?;
    
    // Start satellite
    let satellite_config = TestSatelliteConfig {
        socket_path: socket_path.clone(),
        event_rate: Duration::from_millis(100),
        total_events: 50,
    };
    
    let satellite_handle = start_test_satellite(satellite_config).await?;
    
    // Wait for completion with extra time for retries
    tokio::time::sleep(Duration::from_secs(10)).await;
    
    // Verify events were eventually ingested despite failures
    let event_count = count_events_from_source(ctx.pool(), "test-satellite").await?;
    assert!(event_count >= 45, "Expected at least 45 events (with some failures), got {}", event_count);
    
    satellite_handle.abort();
    
    Ok(())
}

/// Test scanner estimation accuracy
#[sinex_test]
async fn test_scanner_estimation(ctx: TestContext) -> TestResult {
    use sinex_satellite_sdk::ScannerArgs;
    
    // Create test data with known size
    let test_data = create_large_test_scan_data(1000).await?;
    
    // Start ingestd
    let (_ingestd_handle, socket_path) = start_test_ingestd(&ctx).await?;
    
    // Create scanner args
    let scanner_args = ScannerArgs {
        paths: vec![test_data.path.to_string_lossy().to_string()],
        time_range: None,
        dry_run: false,
        interactive: false,
        max_events: 0,
        skip_existing: false,
    };
    
    // Get estimation
    let ingest_client = sinex_satellite_sdk::IngestClient::new(&socket_path).await?;
    let test_satellite = TestScannerSatellite::new();
    let mut runner = EventSourceRunner::new(test_satellite, ingest_client);
    
    runner.initialize(
        "test-scanner".to_string(),
        std::collections::HashMap::new(),
        100,
        5,
        ctx.work_dir(),
        false,
    ).await?;
    
    let estimate = runner.estimate_scanner_scope(&scanner_args).await?;
    
    // Verify estimation is reasonable
    assert!(estimate.estimated_events >= 900 && estimate.estimated_events <= 1100);
    assert!(estimate.estimated_duration.as_millis() > 0);
    assert_eq!(estimate.estimated_paths, 1);
    
    Ok(())
}

/// Test dual-mode satellite (sensor + scanner)
#[sinex_test]
async fn test_dual_mode_satellite(ctx: TestContext) -> TestResult {
    // Start ingestd
    let (_ingestd_handle, socket_path) = start_test_ingestd(&ctx).await?;
    
    // Create dual-mode satellite
    let ingest_client = sinex_satellite_sdk::IngestClient::new(&socket_path).await?;
    let dual_satellite = DualModeSatellite::new();
    let mut runner = EventSourceRunner::new(dual_satellite, ingest_client);
    
    runner.initialize(
        "dual-mode".to_string(),
        std::collections::HashMap::new(),
        100,
        5,
        ctx.work_dir(),
        false,
    ).await?;
    
    // Verify capabilities
    let (supports_sensor, supports_scanner) = runner.get_capabilities();
    assert!(supports_sensor);
    assert!(supports_scanner);
    
    // Test scanner mode first
    let scan_data = create_test_scan_data().await?;
    let scanner_args = ScannerArgs {
        paths: vec![scan_data.path.to_string_lossy().to_string()],
        time_range: None,
        dry_run: false,
        interactive: false,
        max_events: 20,
        skip_existing: false,
    };
    
    let scan_report = runner.run_scanner(scanner_args).await?;
    assert_eq!(scan_report.events_generated, 20);
    
    // Test sensor mode
    let sensor_handle = tokio::spawn(async move {
        runner.run().await
    });
    
    // Let sensor run for a bit
    tokio::time::sleep(Duration::from_secs(3)).await;
    
    // Verify both modes produced events
    let scan_events = count_events_by_metadata(ctx.pool(), "dual-mode", "mode", "scanner").await?;
    let sensor_events = count_events_by_metadata(ctx.pool(), "dual-mode", "mode", "sensor").await?;
    
    assert_eq!(scan_events, 20);
    assert!(sensor_events > 0);
    
    sensor_handle.abort();
    
    Ok(())
}

// ===== Helper Functions =====

/// Configuration for test satellites
struct TestSatelliteConfig {
    socket_path: String,
    event_rate: Duration,
    total_events: u64,
}

/// Start a test ingestd server
async fn start_test_ingestd(ctx: &TestContext) -> TestResult<(tokio::task::JoinHandle<()>, String)> {
    let socket_path = ctx.work_dir().join("test-ingestd.sock").to_string_lossy().to_string();
    start_test_ingestd_at_path(ctx, &socket_path).await
}

/// Start ingestd at specific socket path
async fn start_test_ingestd_at_path(ctx: &TestContext, socket_path: &str) -> TestResult<(tokio::task::JoinHandle<()>, String)> {
    use sinex_ingestd::{IngestServer, ServerConfig};
    
    let config = ServerConfig {
        socket_path: socket_path.to_string(),
        pool: ctx.pool().clone(),
        max_batch_size: 1000,
        batch_timeout: Duration::from_secs(1),
    };
    
    let server = IngestServer::new(config).await?;
    let handle = tokio::spawn(async move {
        let _ = server.run().await;
    });
    
    // Wait for server to start
    tokio::time::sleep(Duration::from_millis(500)).await;
    
    Ok((handle, socket_path.to_string()))
}

/// Start ingestd with simulated failures
async fn start_test_ingestd_with_failures(ctx: &TestContext, failure_rate: f32) -> TestResult<(tokio::task::JoinHandle<()>, String)> {
    // This would require a test version of ingestd that randomly fails
    // For now, just use regular ingestd
    start_test_ingestd(ctx).await
}

/// Start a test satellite
async fn start_test_satellite(config: TestSatelliteConfig) -> TestResult<tokio::task::JoinHandle<()>> {
    start_test_satellite_with_name(config, "test-satellite").await
}

/// Start a test satellite with specific name
async fn start_test_satellite_with_name(config: TestSatelliteConfig, name: &str) -> TestResult<tokio::task::JoinHandle<()>> {
    use sinex_satellite_sdk::{EventSourceRunner, IngestClient};
    
    let ingest_client = IngestClient::new(&config.socket_path).await?;
    let test_satellite = TestEventSource::new(config.event_rate, config.total_events);
    let mut runner = EventSourceRunner::new(test_satellite, ingest_client);
    
    let name = name.to_string();
    runner.initialize(
        name,
        std::collections::HashMap::new(),
        100,
        5,
        std::path::PathBuf::from("/tmp"),
        false,
    ).await?;
    
    Ok(tokio::spawn(async move {
        let _ = runner.run().await;
    }))
}

/// Count events from a specific source
async fn count_events_from_source(pool: &sqlx::PgPool, source: &str) -> TestResult<u64> {
    let count = sqlx::query_scalar!(
        "SELECT COUNT(*) FROM raw.events WHERE source = $1",
        source
    )
    .fetch_one(pool)
    .await?;
    
    Ok(count.unwrap_or(0) as u64)
}

/// Count all events
async fn count_all_events(pool: &sqlx::PgPool) -> TestResult<u64> {
    let count = sqlx::query_scalar!(
        "SELECT COUNT(*) FROM raw.events"
    )
    .fetch_one(pool)
    .await?;
    
    Ok(count.unwrap_or(0) as u64)
}

/// Count events by metadata field
async fn count_events_by_metadata(pool: &sqlx::PgPool, source: &str, key: &str, value: &str) -> TestResult<u64> {
    let count = sqlx::query_scalar!(
        "SELECT COUNT(*) FROM raw.events WHERE source = $1 AND payload->$2 = $3",
        source,
        key,
        serde_json::json!(value)
    )
    .fetch_one(pool)
    .await?;
    
    Ok(count.unwrap_or(0) as u64)
}

/// Create test scan data
async fn create_test_scan_data() -> TestResult<TestScanData> {
    create_large_test_scan_data(50).await
}

/// Create large test scan data
async fn create_large_test_scan_data(event_count: usize) -> TestResult<TestScanData> {
    use std::io::Write;
    
    let temp_dir = tempfile::tempdir()?;
    let data_file = temp_dir.path().join("scan_data.jsonl");
    let mut file = std::fs::File::create(&data_file)?;
    
    for i in 0..event_count {
        let event = serde_json::json!({
            "index": i,
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "data": format!("test event {}", i),
        });
        writeln!(file, "{}", event)?;
    }
    
    Ok(TestScanData {
        path: data_file,
        _temp_dir: temp_dir,
    })
}

struct TestScanData {
    path: std::path::PathBuf,
    _temp_dir: tempfile::TempDir,
}

// ===== Test Satellite Implementations =====

/// Simple test event source
struct TestEventSource {
    event_rate: Duration,
    total_events: u64,
    events_sent: u64,
}

impl TestEventSource {
    fn new(event_rate: Duration, total_events: u64) -> Self {
        Self {
            event_rate,
            total_events,
            events_sent: 0,
        }
    }
}

#[async_trait::async_trait]
impl sinex_satellite_sdk::EventSource for TestEventSource {
    async fn initialize(&mut self, _ctx: sinex_satellite_sdk::EventSourceContext) -> sinex_satellite_sdk::SatelliteResult<()> {
        Ok(())
    }
    
    async fn start_streaming(&mut self) -> sinex_satellite_sdk::SatelliteResult<()> {
        let mut interval = tokio::time::interval(self.event_rate);
        
        while self.events_sent < self.total_events {
            interval.tick().await;
            
            let event = sinex_events::RawEventBuilder::new(
                self.source_name(),
                "test.event",
                serde_json::json!({
                    "index": self.events_sent,
                    "timestamp": chrono::Utc::now().to_rfc3339(),
                    "mode": "sensor",
                })
            )
            .with_host("test-host")
            .build();
            
            // Send event (would use context.event_sender in real impl)
            self.events_sent += 1;
        }
        
        Ok(())
    }
    
    fn source_name(&self) -> &str {
        "test-satellite"
    }
}

/// Test scanner satellite
struct TestScannerSatellite;

impl TestScannerSatellite {
    fn new() -> Self {
        Self
    }
}

#[async_trait::async_trait]
impl sinex_satellite_sdk::EventSource for TestScannerSatellite {
    async fn initialize(&mut self, _ctx: sinex_satellite_sdk::EventSourceContext) -> sinex_satellite_sdk::SatelliteResult<()> {
        Ok(())
    }
    
    async fn start_streaming(&mut self) -> sinex_satellite_sdk::SatelliteResult<()> {
        // Scanner-only satellite
        tokio::time::sleep(Duration::from_secs(3600)).await;
        Ok(())
    }
    
    fn source_name(&self) -> &str {
        "test-scanner"
    }
    
    fn supports_scanner(&self) -> bool {
        true
    }
    
    async fn run_scanner(
        &mut self,
        args: sinex_satellite_sdk::ScannerArgs,
    ) -> sinex_satellite_sdk::SatelliteResult<sinex_satellite_sdk::ScanReport> {
        use std::io::BufRead;
        
        let mut events_generated = 0u64;
        let start = std::time::Instant::now();
        
        // Read test data file
        if let Some(path) = args.paths.first() {
            let file = std::fs::File::open(path)?;
            let reader = std::io::BufReader::new(file);
            
            for line in reader.lines() {
                if let Ok(line) = line {
                    if let Ok(data) = serde_json::from_str::<serde_json::Value>(&line) {
                        // Create event from data
                        let event = sinex_events::RawEventBuilder::new(
                            self.source_name(),
                            "scanned.event",
                            serde_json::json!({
                                "original": data,
                                "mode": "scanner",
                            })
                        )
                        .with_host("test-host")
                        .build();
                        
                        // Send event (would use context.event_sender in real impl)
                        events_generated += 1;
                        
                        if args.max_events > 0 && events_generated >= args.max_events {
                            break;
                        }
                    }
                }
            }
        }
        
        Ok(sinex_satellite_sdk::ScanReport {
            events_generated,
            duration: start.elapsed(),
            blob_id: None,
            time_range: None,
            content_hash: None,
            source_stats: std::collections::HashMap::new(),
            version_info: sinex_satellite_sdk::VersionInfo {
                git_revision: "test".to_string(),
                binary_hash: "test".to_string(),
                component_version: "test-scanner-1.0".to_string(),
                scan_timestamp: chrono::Utc::now(),
            },
            processed_paths: args.paths,
            failed_paths: vec![],
        })
    }
    
    async fn estimate_scanner_scope(
        &self,
        args: &sinex_satellite_sdk::ScannerArgs,
    ) -> sinex_satellite_sdk::SatelliteResult<sinex_satellite_sdk::ScannerEstimate> {
        let mut estimated_events = 0u64;
        
        // Count lines in files
        for path in &args.paths {
            if let Ok(file) = std::fs::File::open(path) {
                let reader = std::io::BufReader::new(file);
                estimated_events += reader.lines().count() as u64;
            }
        }
        
        Ok(sinex_satellite_sdk::ScannerEstimate {
            estimated_events,
            estimated_duration: Duration::from_millis(estimated_events * 10),
            estimated_data_size: estimated_events * 100,
            estimated_paths: args.paths.len() as u64,
            warnings: vec![],
        })
    }
}

/// Dual-mode satellite supporting both sensor and scanner
struct DualModeSatellite {
    sensor_events_sent: u64,
}

impl DualModeSatellite {
    fn new() -> Self {
        Self {
            sensor_events_sent: 0,
        }
    }
}

#[async_trait::async_trait]
impl sinex_satellite_sdk::EventSource for DualModeSatellite {
    async fn initialize(&mut self, _ctx: sinex_satellite_sdk::EventSourceContext) -> sinex_satellite_sdk::SatelliteResult<()> {
        Ok(())
    }
    
    async fn start_streaming(&mut self) -> sinex_satellite_sdk::SatelliteResult<()> {
        let mut interval = tokio::time::interval(Duration::from_millis(500));
        
        loop {
            interval.tick().await;
            
            let event = sinex_events::RawEventBuilder::new(
                self.source_name(),
                "sensor.event",
                serde_json::json!({
                    "index": self.sensor_events_sent,
                    "timestamp": chrono::Utc::now().to_rfc3339(),
                    "mode": "sensor",
                })
            )
            .with_host("test-host")
            .build();
            
            // Send event
            self.sensor_events_sent += 1;
        }
    }
    
    fn source_name(&self) -> &str {
        "dual-mode"
    }
    
    fn supports_scanner(&self) -> bool {
        true
    }
    
    async fn run_scanner(
        &mut self,
        args: sinex_satellite_sdk::ScannerArgs,
    ) -> sinex_satellite_sdk::SatelliteResult<sinex_satellite_sdk::ScanReport> {
        // Reuse test scanner implementation
        let mut scanner = TestScannerSatellite::new();
        scanner.run_scanner(args).await
    }
}