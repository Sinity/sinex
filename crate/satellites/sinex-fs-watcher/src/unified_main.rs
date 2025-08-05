//! Unified main.rs for filesystem processor using StatefulStreamProcessor architecture
//!
//! This demonstrates the new CLI structure with service/scan/explore subcommands.

use clap::Parser;
use sinex_fs_watcher::{FilesystemProcessor, FilesystemConfig};
use sinex_satellite_sdk::{
    cli::{ProcessorCli, ProcessorCommand},
    stream_processor::{Checkpoint, StreamProcessorRunner, TimeHorizon},
    grpc_client::IngestClient,
    SatelliteResult,
};
use sinex_db::SqlxPgPool;
use std::collections::HashMap;
use camino::Utf8PathBuf;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = ProcessorCli::parse();

    // Initialize logging based on verbosity
    let log_level = match args.verbose {
        0 => "info",
        1 => "debug",
        _ => "trace",
    };
    
    tracing_subscriber::fmt()
        .with_env_filter(format!("sinex={}", log_level))
        .init();

    // Parse processor configuration
    let processor_config: HashMap<String, serde_json::Value> = if let Some(config_str) = args.processor_config {
        serde_json::from_str(&config_str)?
    } else {
        HashMap::new()
    };

    match args.command {
        ProcessorCommand::Service { dry_run, consumer_group: _ } => {
            run_service_mode(args, processor_config, dry_run).await?;
        }

        ProcessorCommand::Scan {
            from,
            until,
            targets,
            dry_run,
            interactive,
            max_events,
            no_skip_duplicates,
            estimate,
        } => {
            run_scan_mode(
                args,
                processor_config,
                from,
                until,
                targets,
                dry_run,
                interactive,
                max_events,
                !no_skip_duplicates,
                estimate,
            ).await?;
        }

        ProcessorCommand::Explore {
            source_state,
            ingestion_history,
            coverage_analysis,
            limit,
            export_to,
        } => {
            run_explore_mode(
                args,
                processor_config,
                source_state,
                ingestion_history,
                coverage_analysis,
                limit,
                export_to,
            ).await?;
        }
    }

    Ok(())
}

async fn run_service_mode(
    args: ProcessorCli,
    processor_config: HashMap<String, serde_json::Value>,
    dry_run: bool,
) -> SatelliteResult<()> {
    info!("Starting filesystem processor in service mode");

    // Create processor
    let processor = create_configured_processor(&processor_config).await?;
    
    // Create runner
    let mut runner = StreamProcessorRunner::new(processor);
    
    // Set up dependencies
    let service_name = args.service_name.unwrap_or_else(|| "sinex-fs-processor".to_string());
    let work_dir = args.work_dir.unwrap_or_else(|| Utf8PathBuf::from("/tmp/sinex/fs-processor"));
    
    // Create database pool if needed
    let db_pool = if let Some(db_url) = args.database_url {
        SqlxPgPool::connect(&db_url).await
            .map_err(|e| sinex_satellite_sdk::SatelliteError::Database(format!("Failed to connect to database: {}", e)))?
    } else {
        // Use environment variable
        let db_url = std::env::var("DATABASE_URL")
            .map_err(|_| sinex_satellite_sdk::SatelliteError::Config("DATABASE_URL not set".to_string()))?;
        SqlxPgPool::connect(&db_url).await
            .map_err(|e| sinex_satellite_sdk::SatelliteError::Database(format!("Failed to connect to database: {}", e)))?
    };
    
    // Create ingest client
    let ingest_client = IngestClient::new(&args.ingest_socket_path).await?;
    
    // Initialize runner
    runner.initialize(
        service_name,
        processor_config,
        db_pool,
        ingest_client,
        work_dir,
        dry_run,
    ).await?;
    
    // Run service with startup sequence
    runner.run_service().await?;
    
    Ok(())
}

async fn run_scan_mode(
    args: ProcessorCli,
    processor_config: HashMap<String, serde_json::Value>,
    from: String,
    until: String,
    targets: Vec<String>,
    dry_run: bool,
    interactive: bool,
    max_events: u64,
    skip_duplicates: bool,
    estimate: bool,
) -> SatelliteResult<()> {
    info!("Starting filesystem processor in scan mode");

    // Parse checkpoint and time horizon
    let checkpoint = sinex_satellite_sdk::cli::parse_checkpoint(&from)?;
    let time_horizon = sinex_satellite_sdk::cli::parse_time_horizon(&until)?;
    
    // Create processor
    let processor = create_configured_processor(&processor_config).await?;
    
    // Create runner
    let mut runner = StreamProcessorRunner::new(processor);
    
    // Set up dependencies (minimal for scan mode)
    let service_name = args.service_name.unwrap_or_else(|| "sinex-fs-processor".to_string());
    let work_dir = args.work_dir.unwrap_or_else(|| Utf8PathBuf::from("/tmp/sinex/fs-processor"));
    
    // For scan mode, we need minimal setup
    let db_pool = if let Some(db_url) = args.database_url {
        SqlxPgPool::connect(&db_url).await
            .map_err(|e| sinex_satellite_sdk::SatelliteError::Database(format!("Failed to connect to database: {}", e)))?
    } else {
        // Use environment variable or create dummy pool for dry runs
        if dry_run {
            // For dry runs, we can use a dummy connection string
            SqlxPgPool::connect("postgresql://localhost/dummy").await
                .unwrap_or_else(|_| {
                    // If even dummy connection fails, we'll need to handle this gracefully in the processor
                    panic!("Could not create database pool for scan mode")
                })
        } else {
            let db_url = std::env::var("DATABASE_URL")
                .map_err(|_| sinex_satellite_sdk::SatelliteError::Config("DATABASE_URL not set".to_string()))?;
            SqlxPgPool::connect(&db_url).await
                .map_err(|e| sinex_satellite_sdk::SatelliteError::Database(format!("Failed to connect to database: {}", e)))?
        }
    };
    
    let ingest_client = IngestClient::new(&args.ingest_socket_path).await?;
    
    // Initialize runner
    runner.initialize(
        service_name,
        processor_config,
        db_pool,
        ingest_client,
        work_dir,
        dry_run,
    ).await?;
    
    // Create scan args
    let scan_args = sinex_satellite_sdk::stream_processor::ScanArgs {
        targets,
        dry_run,
        interactive,
        max_events,
        skip_duplicates,
        config: HashMap::new(),
    };
    
    // Run estimation if requested
    if estimate {
        let estimate_result = runner.estimate_scan_scope(&checkpoint, &time_horizon, &scan_args).await?;
        println!("Scan Estimation:");
        println!("  Estimated events: {}", estimate_result.estimated_events);
        println!("  Estimated duration: {:?}", estimate_result.estimated_duration);
        println!("  Estimated data size: {} bytes", estimate_result.estimated_data_size);
        println!("  Estimated targets: {}", estimate_result.estimated_targets);
        println!("  Confidence: {:.1}%", estimate_result.confidence * 100.0);
        if !estimate_result.warnings.is_empty() {
            println!("  Warnings:");
            for warning in &estimate_result.warnings {
                println!("    - {}", warning);
            }
        }
        println!();
        
        if interactive {
            println!("Proceed with scan? [y/N]");
            let mut input = String::new();
            std::io::stdin().read_line(&mut input)?;
            if !input.trim().to_lowercase().starts_with('y') {
                println!("Scan cancelled");
                return Ok(());
            }
        }
    }
    
    // Run scan
    let report = runner.run_scan(checkpoint, time_horizon, scan_args).await?;
    
    // Display results
    println!("Scan Results:");
    println!("  Events processed: {}", report.events_processed);
    println!("  Duration: {:?}", report.duration);
    println!("  Final checkpoint: {}", report.final_checkpoint.description());
    
    if let Some((start, end)) = report.time_range {
        println!("  Time range: {} to {}", start.format("%Y-%m-%d %H:%M:%S"), end.format("%Y-%m-%d %H:%M:%S"));
    }
    
    if !report.processor_stats.is_empty() {
        println!("  Processor stats:");
        for (key, value) in &report.processor_stats {
            println!("    {}: {}", key, value);
        }
    }
    
    if !report.successful_targets.is_empty() {
        println!("  Successful targets: {}", report.successful_targets.len());
        for target in &report.successful_targets {
            println!("    - {}", target);
        }
    }
    
    if !report.failed_targets.is_empty() {
        println!("  Failed targets:");
        for (target, error) in &report.failed_targets {
            println!("    - {}: {}", target, error);
        }
    }
    
    if !report.warnings.is_empty() {
        println!("  Warnings:");
        for warning in &report.warnings {
            println!("    - {}", warning);
        }
    }
    
    Ok(())
}

async fn run_explore_mode(
    args: ProcessorCli,
    processor_config: HashMap<String, serde_json::Value>,
    source_state: bool,
    ingestion_history: bool,
    coverage_analysis: bool,
    limit: u64,
    export_to: Option<Utf8PathBuf>,
) -> SatelliteResult<()> {
    info!("Starting filesystem processor in explore mode");

    // Create processor
    let processor = create_configured_processor(&processor_config).await?;
    
    // For exploration, we can work with the processor directly
    use sinex_satellite_sdk::cli::ExplorationProvider;
    
    if source_state {
        match processor.get_source_state() {
            Ok(state) => {
                println!("Source State:");
                println!("  Description: {}", state.description);
                println!("  Last updated: {}", state.last_updated.format("%Y-%m-%d %H:%M:%S"));
                if let Some(total) = state.total_items {
                    println!("  Total items: {}", total);
                }
                println!("  Healthy: {}", state.healthy);
                
                if !state.recent_activity.is_empty() {
                    println!("  Recent activity:");
                    for activity in &state.recent_activity {
                        println!("    - {}: {}", activity.timestamp.format("%H:%M:%S"), activity.description);
                    }
                }
                
                if !state.metadata.is_empty() {
                    println!("  Metadata:");
                    for (key, value) in &state.metadata {
                        println!("    {}: {}", key, value);
                    }
                }
            }
            Err(e) => {
                eprintln!("Failed to get source state: {}", e);
            }
        }
        println!();
    }
    
    if ingestion_history {
        match processor.get_ingestion_history(limit) {
            Ok(history) => {
                println!("Ingestion History ({} entries):", history.len());
                for entry in &history {
                    println!("  ID: {}", entry.id);
                    println!("    Started: {}", entry.started_at.format("%Y-%m-%d %H:%M:%S"));
                    if let Some(completed) = entry.completed_at {
                        println!("    Completed: {}", completed.format("%Y-%m-%d %H:%M:%S"));
                    }
                    println!("    Events: {}", entry.events_generated);
                    if let Some(error) = &entry.error {
                        println!("    Error: {}", error);
                    }
                }
            }
            Err(e) => {
                eprintln!("Failed to get ingestion history: {}", e);
            }
        }
        println!();
    }
    
    if coverage_analysis {
        match processor.get_coverage_analysis(None) {
            Ok(analysis) => {
                println!("Coverage Analysis:");
                println!("  Time range: {} to {}", 
                         analysis.time_range.0.format("%Y-%m-%d %H:%M:%S"),
                         analysis.time_range.1.format("%Y-%m-%d %H:%M:%S"));
                println!("  Source total: {}", analysis.source_total);
                println!("  Sinex total: {}", analysis.sinex_total);
                println!("  Coverage: {:.1}%", analysis.coverage_percentage);
                println!("  Missing: {}", analysis.missing_count);
                println!("  Duplicates: {}", analysis.duplicate_count);
                
                if !analysis.missing_samples.is_empty() {
                    println!("  Missing samples:");
                    for sample in &analysis.missing_samples {
                        println!("    - {}: {} ({})", sample.source_id, sample.description, 
                                sample.missing_reason.as_deref().unwrap_or("Unknown"));
                    }
                }
                
                if !analysis.recommendations.is_empty() {
                    println!("  Recommendations:");
                    for rec in &analysis.recommendations {
                        println!("    - {}", rec);
                    }
                }
            }
            Err(e) => {
                eprintln!("Failed to get coverage analysis: {}", e);
            }
        }
        println!();
    }
    
    if let Some(export_path) = export_to {
        let format = match export_path.extension().and_then(|s| s.to_str()) {
            Some("json") => sinex_satellite_sdk::cli::ExportFormat::Json,
            Some("csv") => sinex_satellite_sdk::cli::ExportFormat::Csv,
            _ => sinex_satellite_sdk::cli::ExportFormat::Raw,
        };
        
        match processor.export_data(&export_path, format) {
            Ok(_) => {
                println!("Data exported to: {}", export_path.as_str());
            }
            Err(e) => {
                eprintln!("Failed to export data: {}", e);
            }
        }
    }
    
    Ok(())
}

async fn create_configured_processor(
    processor_config: &HashMap<String, serde_json::Value>,
) -> SatelliteResult<FilesystemProcessor> {
    // Parse filesystem-specific configuration
    let config = if let Some(fs_config) = processor_config.get("filesystem") {
        serde_json::from_value::<FilesystemConfig>(fs_config.clone())
            .unwrap_or_default()
    } else {
        // Build config from individual values
        let mut config = FilesystemConfig::default();
        
        if let Some(patterns) = processor_config.get("watch_patterns") {
            if let Ok(patterns) = serde_json::from_value::<Vec<String>>(patterns.clone()) {
                config.watch_patterns = patterns;
            }
        }
        
        if let Some(ignore) = processor_config.get("ignore_patterns") {
            if let Ok(patterns) = serde_json::from_value::<Vec<String>>(ignore.clone()) {
                config.ignore_patterns = patterns;
            }
        }
        
        if let Some(debounce) = processor_config.get("debounce_ms") {
            if let Ok(ms) = serde_json::from_value::<u64>(debounce.clone()) {
                config.debounce_ms = ms;
            }
        }
        
        if let Some(depth) = processor_config.get("max_depth") {
            if let Ok(depth) = serde_json::from_value::<Option<usize>>(depth.clone()) {
                config.max_depth = depth;
            }
        }
        
        config
    };
    
    Ok(FilesystemProcessor::with_config(config))
}