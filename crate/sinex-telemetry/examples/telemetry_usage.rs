//! Example demonstrating comprehensive telemetry usage in Sinex
//!
//! This example shows how to:
//! - Set up telemetry for different component types
//! - Record various metrics
//! - Query telemetry events from the database

use sinex_events::EventSender;
use sinex_telemetry::{
    init_metrics, set_global_telemetry, SystemTelemetryEmitter, TelemetryAccumulator,
};
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{info, Level};

/// Example satellite component with telemetry
struct ExampleSatellite {
    name: String,
    telemetry: TelemetryAccumulator,
}

impl ExampleSatellite {
    fn new(name: &str, event_sender: EventSender) -> Self {
        let telemetry = TelemetryAccumulator::new(name)
            .with_event_sender(event_sender)
            .with_interval(Duration::from_secs(30)); // Emit every 30 seconds for demo

        Self {
            name: name.to_string(),
            telemetry,
        }
    }

    async fn process_files(&self, files: Vec<&str>) {
        for file in files {
            let start = std::time::Instant::now();

            // Simulate file processing
            tokio::time::sleep(Duration::from_millis(10)).await;

            let duration_ms = start.elapsed().as_secs_f64() * 1000.0;

            // Record metrics
            if file.ends_with(".txt") {
                self.telemetry
                    .record_event_processed("file.text", duration_ms);
            } else if file.ends_with(".jpg") {
                self.telemetry
                    .record_event_processed("file.image", duration_ms);
            } else {
                self.telemetry
                    .record_event_processed("file.other", duration_ms);
            }

            info!(file = %file, duration_ms = %duration_ms, "Processed file");
        }
    }

    async fn scan_directory(&self, path: &str) -> Result<(), Box<dyn std::error::Error>> {
        let start = std::time::Instant::now();

        // Simulate directory scanning
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Simulate finding files
        let files = vec!["doc1.txt", "image.jpg", "data.csv", "readme.txt"];

        let duration_ms = start.elapsed().as_secs_f64() * 1000.0;
        self.telemetry
            .record_operation_latency("scan_directory", duration_ms);

        info!(path = %path, file_count = %files.len(), "Scanned directory");

        // Process found files
        self.process_files(files).await;

        Ok(())
    }

    fn simulate_resource_usage(&self) {
        // Simulate varying resource usage
        let memory_mb = 100.0 + (rand::random::<f64>() * 50.0);
        let cpu_percent = 10.0 + (rand::random::<f64>() * 30.0);

        self.telemetry.record_resource_usage(memory_mb, cpu_percent);
    }
}

/// Example service component (like ingestd or gateway)
struct ExampleService {
    name: String,
    telemetry: TelemetryAccumulator,
}

impl ExampleService {
    fn new(name: &str, event_sender: EventSender) -> Self {
        let telemetry = TelemetryAccumulator::new(name)
            .with_event_sender(event_sender)
            .with_interval(Duration::from_secs(60)); // Emit every minute

        Self {
            name: name.to_string(),
            telemetry,
        }
    }

    async fn handle_request(&self, request_type: &str) -> Result<(), &'static str> {
        let start = std::time::Instant::now();

        // Simulate request processing
        match request_type {
            "query" => {
                tokio::time::sleep(Duration::from_millis(20)).await;
                Ok(())
            }
            "update" => {
                tokio::time::sleep(Duration::from_millis(30)).await;
                Ok(())
            }
            "invalid" => {
                self.telemetry.record_error("validation_error");
                Err("Invalid request type")
            }
            _ => {
                self.telemetry.record_error("unknown_request");
                Err("Unknown request type")
            }
        }?;

        let duration_ms = start.elapsed().as_secs_f64() * 1000.0;
        self.telemetry
            .record_operation_latency(&format!("handle_{}", request_type), duration_ms);

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    info!("Starting telemetry example");

    // Initialize metrics system
    init_metrics().await;

    // Create event channel (in real usage, this would connect to ingestd)
    let (tx, mut rx) = mpsc::channel::<sinex_events::RawEvent>(100);

    // Set up system telemetry
    let system_emitter = SystemTelemetryEmitter::new(tx.clone());
    let _system_handle = system_emitter.spawn_emitter();

    // Create example satellite
    let satellite = ExampleSatellite::new("example-fs-watcher", tx.clone());

    // Set global telemetry for auto-metrics integration
    set_global_telemetry(satellite.telemetry.clone()).await;

    // Spawn telemetry emitter
    let _satellite_handle = satellite.telemetry.clone().spawn_emitter();

    // Create example service
    let service = ExampleService::new("example-gateway", tx.clone());
    let _service_handle = service.telemetry.clone().spawn_emitter();

    // Spawn event logger (in real usage, events would go to database)
    tokio::spawn(async move {
        while let Some(event) = rx.recv().await {
            println!("\n📊 Telemetry Event:");
            println!("  Source: {}", event.source);
            println!("  Type: {}", event.event_type);
            println!(
                "  Payload: {}",
                serde_json::to_string_pretty(&event.payload).unwrap()
            );
        }
    });

    // Simulate satellite activity
    tokio::spawn(async move {
        loop {
            // Scan directories
            if let Err(e) = satellite.scan_directory("/home/user/documents").await {
                satellite.telemetry.record_error("scan_error");
                eprintln!("Scan error: {}", e);
            }

            // Record resource usage
            satellite.simulate_resource_usage();

            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    });

    // Simulate service activity
    tokio::spawn(async move {
        let request_types = ["query", "query", "update", "query", "invalid", "query"];
        let mut i = 0;

        loop {
            let request_type = request_types[i % request_types.len()];

            if let Err(e) = service.handle_request(request_type).await {
                eprintln!("Request error: {}", e);
            }

            i += 1;
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    });

    // Example function with auto-metrics
    #[sinex_macros::auto_metrics]
    async fn process_data(data: &str) -> Result<String, std::io::Error> {
        // Simulate processing
        tokio::time::sleep(Duration::from_millis(15)).await;

        if data.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Empty data",
            ));
        }

        Ok(data.to_uppercase())
    }

    // Use the auto-instrumented function
    tokio::spawn(async {
        loop {
            match process_data("test data").await {
                Ok(result) => info!("Processed: {}", result),
                Err(e) => eprintln!("Processing error: {}", e),
            }

            tokio::time::sleep(Duration::from_secs(3)).await;
        }
    });

    // Run for a while to see telemetry events
    info!("Running example... Press Ctrl+C to stop");
    tokio::time::sleep(Duration::from_secs(120)).await;

    // Example SQL queries for telemetry data
    println!("\n📊 Example Telemetry Queries:\n");

    println!("-- Event throughput by component and type:");
    println!(
        r#"
SELECT 
    payload->>'component' as component,
    jsonb_object_keys(payload->'by_type') as event_type,
    SUM((payload->'by_type'->>jsonb_object_keys(payload->'by_type'))::int) as total_count
FROM core.events
WHERE source = 'sinex.telemetry' 
  AND event_type = 'events.processed'
  AND ts_ingest > NOW() - INTERVAL '1 hour'
GROUP BY component, event_type
ORDER BY component, total_count DESC;
"#
    );

    println!("\n-- Operation performance percentiles:");
    println!(
        r#"
SELECT 
    payload->>'component' as component,
    payload->>'operation' as operation,
    payload->'duration_ms'->>'p50' as p50_ms,
    payload->'duration_ms'->>'p95' as p95_ms,
    payload->'duration_ms'->>'p99' as p99_ms
FROM core.events
WHERE source = 'sinex.telemetry' 
  AND event_type = 'operation.performance'
  AND ts_ingest > NOW() - INTERVAL '1 hour'
ORDER BY component, operation;
"#
    );

    println!("\n-- Resource usage by component:");
    println!(
        r#"
SELECT 
    payload->>'component' as component,
    AVG((payload->'memory_mb'->>'avg')::float) as avg_memory_mb,
    MAX((payload->'memory_mb'->>'peak')::float) as peak_memory_mb,
    AVG((payload->'cpu_percent'->>'avg')::float) as avg_cpu_percent
FROM core.events
WHERE source = 'sinex.telemetry'
  AND event_type = 'resource.usage'
  AND ts_ingest > NOW() - INTERVAL '1 hour'
GROUP BY component
ORDER BY avg_memory_mb DESC;
"#
    );

    println!("\n-- Error summary by component:");
    println!(
        r#"
SELECT 
    payload->>'component' as component,
    jsonb_object_keys(payload->'by_type') as error_type,
    SUM((payload->'by_type'->>jsonb_object_keys(payload->'by_type'))::int) as error_count
FROM core.events
WHERE source = 'sinex.telemetry'
  AND event_type = 'errors.summary'
  AND ts_ingest > NOW() - INTERVAL '24 hours'
GROUP BY component, error_type
ORDER BY error_count DESC;
"#
    );

    Ok(())
}

// Add rand dependency for demo
use rand;
