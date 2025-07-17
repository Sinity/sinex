//! Satellite metrics example
//!
//! This example demonstrates how to use automatic metrics collection
//! with StatefulStreamProcessor implementations.

use async_trait::async_trait;
use sinex_macros::auto_satellite_metrics;
use sinex_metrics_lib::{export_prometheus, init_metrics};
use std::collections::HashMap;
use tokio::time::{sleep, Duration};

// Mock types for the example
#[derive(Debug, Clone)]
pub enum TimeHorizon {
    Continuous,
    Historical { end_time: std::time::SystemTime },
    Snapshot,
}

#[derive(Debug, Clone)]
pub enum Checkpoint {
    None,
    External {
        position: serde_json::Value,
        description: String,
    },
}

#[derive(Debug, Clone)]
pub struct ScanArgs {
    pub targets: Vec<String>,
    pub dry_run: bool,
    pub max_events: u64,
}

#[derive(Debug, Clone)]
pub struct ScanReport {
    pub events_processed: u64,
    pub duration: Duration,
    pub final_checkpoint: Checkpoint,
    pub successful_targets: Vec<String>,
    pub failed_targets: Vec<(String, String)>,
}

#[derive(Debug)]
pub struct SatelliteError(pub String);

impl std::fmt::Display for SatelliteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Satellite error: {}", self.0)
    }
}

impl std::error::Error for SatelliteError {}

pub type SatelliteResult<T> = Result<T, SatelliteError>;

// Mock trait for StatefulStreamProcessor
#[async_trait]
pub trait StatefulStreamProcessor: Send + Sync {
    async fn scan(
        &mut self,
        from: Checkpoint,
        until: TimeHorizon,
        args: ScanArgs,
    ) -> SatelliteResult<ScanReport>;

    fn processor_name(&self) -> &str;
    fn processor_type(&self) -> &str;

    async fn current_checkpoint(&self) -> SatelliteResult<Checkpoint>;
    async fn health_check(&self) -> SatelliteResult<bool>;
}

// Example filesystem watcher with automatic metrics
pub struct FilesystemWatcher {
    name: String,
    watch_paths: Vec<String>,
    last_scan: Option<std::time::SystemTime>,
}

impl FilesystemWatcher {
    pub fn new(name: String, watch_paths: Vec<String>) -> Self {
        Self {
            name,
            watch_paths,
            last_scan: None,
        }
    }
}

#[auto_satellite_metrics(processor_type = "ingestor", labels = ["source=filesystem"])]
#[async_trait]
impl StatefulStreamProcessor for FilesystemWatcher {
    async fn scan(
        &mut self,
        from: Checkpoint,
        until: TimeHorizon,
        args: ScanArgs,
    ) -> SatelliteResult<ScanReport> {
        let start_time = std::time::Instant::now();

        // Simulate filesystem scanning
        println!("Scanning filesystem from checkpoint: {:?}", from);
        println!("Scan horizon: {:?}", until);
        println!("Watching paths: {:?}", self.watch_paths);

        let mut events_processed = 0u64;
        let mut successful_targets = Vec::new();
        let mut failed_targets = Vec::new();

        // Simulate scanning each path
        for path in &self.watch_paths {
            if path.contains("error") {
                failed_targets.push((path.clone(), "Permission denied".to_string()));
                continue;
            }

            // Simulate finding files
            let files_found = (path.len() % 5) + 1;
            events_processed += files_found as u64;
            successful_targets.push(path.clone());

            // Simulate scan time
            sleep(Duration::from_millis(50)).await;
        }

        // Simulate processing delay
        sleep(Duration::from_millis(100)).await;

        self.last_scan = Some(std::time::SystemTime::now());

        let final_checkpoint = Checkpoint::External {
            position: serde_json::json!({
                "last_scan_time": self.last_scan.unwrap().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs(),
                "events_processed": events_processed
            }),
            description: format!(
                "Scanned {} paths, processed {} events",
                self.watch_paths.len(),
                events_processed
            ),
        };

        Ok(ScanReport {
            events_processed,
            duration: start_time.elapsed(),
            final_checkpoint,
            successful_targets,
            failed_targets,
        })
    }

    fn processor_name(&self) -> &str {
        &self.name
    }

    fn processor_type(&self) -> &str {
        "ingestor"
    }

    async fn current_checkpoint(&self) -> SatelliteResult<Checkpoint> {
        if let Some(last_scan) = self.last_scan {
            Ok(Checkpoint::External {
                position: serde_json::json!({
                    "last_scan_time": last_scan.duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()
                }),
                description: "Last filesystem scan checkpoint".to_string(),
            })
        } else {
            Ok(Checkpoint::None)
        }
    }

    async fn health_check(&self) -> SatelliteResult<bool> {
        // Simulate health check
        Ok(true)
    }
}

// Example command canonicalizer automaton
pub struct CommandCanonicalizer {
    name: String,
    patterns: Vec<String>,
}

impl CommandCanonicalizer {
    pub fn new(name: String, patterns: Vec<String>) -> Self {
        Self { name, patterns }
    }
}

#[auto_satellite_metrics(processor_type = "automaton", labels = ["source=shell"])]
#[async_trait]
impl StatefulStreamProcessor for CommandCanonicalizer {
    async fn scan(
        &mut self,
        from: Checkpoint,
        until: TimeHorizon,
        args: ScanArgs,
    ) -> SatelliteResult<ScanReport> {
        let start_time = std::time::Instant::now();

        // Simulate command canonicalization
        println!("Canonicalizing commands from checkpoint: {:?}", from);
        println!("Processing patterns: {:?}", self.patterns);

        let mut events_processed = 0u64;
        let mut successful_targets = Vec::new();
        let mut failed_targets = Vec::new();

        // Simulate processing command patterns
        for pattern in &self.patterns {
            if pattern.contains("invalid") {
                failed_targets.push((pattern.clone(), "Invalid regex pattern".to_string()));
                continue;
            }

            // Simulate matching commands
            let commands_matched = (pattern.len() % 10) + 1;
            events_processed += commands_matched as u64;
            successful_targets.push(pattern.clone());

            // Simulate processing time
            sleep(Duration::from_millis(20)).await;
        }

        // Simulate additional processing
        sleep(Duration::from_millis(80)).await;

        let final_checkpoint = Checkpoint::External {
            position: serde_json::json!({
                "patterns_processed": self.patterns.len(),
                "events_processed": events_processed,
                "timestamp": std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()
            }),
            description: format!(
                "Canonicalized {} patterns, processed {} commands",
                self.patterns.len(),
                events_processed
            ),
        };

        Ok(ScanReport {
            events_processed,
            duration: start_time.elapsed(),
            final_checkpoint,
            successful_targets,
            failed_targets,
        })
    }

    fn processor_name(&self) -> &str {
        &self.name
    }

    fn processor_type(&self) -> &str {
        "automaton"
    }

    async fn current_checkpoint(&self) -> SatelliteResult<Checkpoint> {
        Ok(Checkpoint::External {
            position: serde_json::json!({
                "patterns_count": self.patterns.len(),
                "timestamp": std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()
            }),
            description: "Command canonicalizer checkpoint".to_string(),
        })
    }

    async fn health_check(&self) -> SatelliteResult<bool> {
        // Simulate health check
        Ok(!self.patterns.is_empty())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize the metrics system
    init_metrics().await;

    println!("Starting satellite metrics example...");

    // Create filesystem watcher
    let mut fs_watcher = FilesystemWatcher::new(
        "fs-watcher".to_string(),
        vec![
            "/tmp/test1".to_string(),
            "/tmp/test2".to_string(),
            "/tmp/error".to_string(), // This will simulate an error
            "/tmp/test3".to_string(),
        ],
    );

    // Create command canonicalizer
    let mut cmd_canonicalizer = CommandCanonicalizer::new(
        "cmd-canonicalizer".to_string(),
        vec![
            "ls.*".to_string(),
            "cd.*".to_string(),
            "invalid[".to_string(), // This will simulate an error
            "grep.*".to_string(),
        ],
    );

    // Run multiple scan operations
    for i in 0..5 {
        println!("\n=== Scan iteration {} ===", i + 1);

        // Run filesystem watcher scan
        let scan_args = ScanArgs {
            targets: vec!["filesystem".to_string()],
            dry_run: false,
            max_events: 100,
        };

        match fs_watcher
            .scan(Checkpoint::None, TimeHorizon::Snapshot, scan_args.clone())
            .await
        {
            Ok(report) => {
                println!(
                    "FS Watcher scan completed: {} events processed",
                    report.events_processed
                );
                println!("Successful targets: {:?}", report.successful_targets);
                println!("Failed targets: {:?}", report.failed_targets);
            }
            Err(e) => println!("FS Watcher scan error: {}", e),
        }

        // Run command canonicalizer scan
        match cmd_canonicalizer
            .scan(Checkpoint::None, TimeHorizon::Snapshot, scan_args)
            .await
        {
            Ok(report) => {
                println!(
                    "Command Canonicalizer scan completed: {} events processed",
                    report.events_processed
                );
                println!("Successful targets: {:?}", report.successful_targets);
                println!("Failed targets: {:?}", report.failed_targets);
            }
            Err(e) => println!("Command Canonicalizer scan error: {}", e),
        }

        // Test health checks
        match fs_watcher.health_check().await {
            Ok(healthy) => println!(
                "FS Watcher health: {}",
                if healthy { "OK" } else { "FAILED" }
            ),
            Err(e) => println!("FS Watcher health check error: {}", e),
        }

        match cmd_canonicalizer.health_check().await {
            Ok(healthy) => println!(
                "Command Canonicalizer health: {}",
                if healthy { "OK" } else { "FAILED" }
            ),
            Err(e) => println!("Command Canonicalizer health check error: {}", e),
        }

        // Delay between iterations
        sleep(Duration::from_millis(500)).await;
    }

    // Wait for metrics to be collected
    sleep(Duration::from_secs(2)).await;

    println!("\n=== Satellite Metrics Export ===");

    // Export metrics in Prometheus format
    let prometheus_metrics = export_prometheus();
    println!("{}", prometheus_metrics);

    println!("\nSatellite metrics example completed!");

    Ok(())
}
