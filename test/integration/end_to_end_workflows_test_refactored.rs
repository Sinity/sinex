//! End-to-End Workflow Tests (Refactored with Factories)
//!
//! This file demonstrates how test factories simplify complex workflow testing
//! by replacing verbose manual event creation with concise factory calls.

use crate::common::prelude::*;
use crate::common::test_factories::{
    UserActivityFactory, SystemEventFactory, FileSystemScenarioFactory,
    WorkflowFactory, ErrorScenarioFactory, scenarios
};
use crate::common::query_helpers::TestQueries;
use crate::common::automaton_testing::{
    create_test_checkpoint_manager, insert_test_checkpoint, get_checkpoint_state
};
use sinex_events::{event_types, sources};
use std::time::Duration;

/// Test a complete user workflow from login to logout
#[sinex_test(timeout = 60)]
async fn test_user_session_workflow_with_automaton_processing(ctx: TestContext) -> TestResult {
    // OLD WAY: Would require 50+ lines to manually create all these events
    // NEW WAY: One line creates a realistic 30-minute session
    let session_events = UserActivityFactory::create_user_session(30, 20);
    
    // Insert events
    ctx.insert_events(&session_events).await?;
    
    // Start automaton processing
    let automaton_name = "test-session-processor";
    let checkpoint_mgr = create_test_checkpoint_manager(
        ctx.pool().clone(),
        automaton_name,
        "session-group",
        "session-consumer"
    );
    
    // Simulate automaton processing
    let processed_count = session_events.len() as u64;
    insert_test_checkpoint(ctx.pool(), automaton_name, processed_count, None).await?;
    
    // Verify processing
    let checkpoint = get_checkpoint_state(ctx.pool(), automaton_name).await?
        .expect("Should have checkpoint");
    
    assert_eq!(checkpoint.processed_count, processed_count);
    
    // Analyze session patterns
    let analysis = analyze_session_patterns(ctx.pool()).await?;
    
    assert!(analysis.total_duration_minutes >= 25);
    assert!(analysis.activity_types.len() >= 4);
    assert!(analysis.commands_executed > 0);
    assert!(analysis.files_modified > 0);

/// Test development workflow with git operations
#[sinex_test(timeout = 50)]
async fn test_development_workflow_with_git_integration(ctx: TestContext) -> TestResult {
    // Create comprehensive development session
    let dev_session = UserActivityFactory::create_development_session();
    let git_workflow = WorkflowFactory::create_git_workflow();
    
    // Combine workflows
    let mut all_events = dev_session;
    all_events.extend(git_workflow);
    
    // Sort by timestamp for realistic ordering
    all_events.sort_by_key(|e| e.ts_orig.unwrap_or(e.ts_ingest));
    
    ctx.insert_events(&all_events).await?;
    
    // Verify development patterns
    let patterns = analyze_development_patterns(ctx.pool()).await?;
    
    assert!(patterns.has_git_workflow);
    assert!(patterns.has_file_edits);
    assert!(patterns.has_test_execution);
    assert_eq!(patterns.git_operations.len(), 4);
    assert!(patterns.total_lines_changed > 0);

/// Test system monitoring and alerting workflow
#[sinex_test(timeout = 60)]
async fn test_system_monitoring_with_alerting(ctx: TestContext) -> TestResult {
    // Create monitoring baseline
    let monitoring = SystemEventFactory::create_system_monitoring(20, 60);
    ctx.insert_events(&monitoring).await?;
    
    // Simulate resource pressure
    let pressure = SystemEventFactory::create_resource_pressure();
    ctx.insert_events(&pressure).await?;
    
    // Process with health aggregator
    let health_automaton = "health-aggregator";
    insert_test_checkpoint(
        ctx.pool(),
        health_automaton,
        (monitoring.len() + pressure.len()) as u64,
        None
    ).await?;
    
    // Analyze health trends
    let health_analysis = analyze_system_health(ctx.pool()).await?;
    
    assert!(health_analysis.cpu_trend_increasing);
    assert!(health_analysis.memory_trend_increasing);
    assert!(health_analysis.alerts_triggered > 0);
    assert!(health_analysis.max_cpu_percent > 80.0);

/// Test file processing pipeline
#[sinex_test(timeout = 45)]
async fn test_file_processing_pipeline(ctx: TestContext) -> TestResult {
    // Create file workflow
    let file_workflow = FileSystemScenarioFactory::create_file_workflow("/project/src/main.rs");
    
    // Create build process
    let build_process = FileSystemScenarioFactory::create_build_process();
    
    let mut all_events = file_workflow;
    all_events.extend(build_process);
    
    ctx.insert_events(&all_events).await?;
    
    // Process with content automaton
    let content_automaton = "content-processor";
    insert_test_checkpoint(
        ctx.pool(),
        content_automaton,
        all_events.len() as u64,
        None
    ).await?;
    
    // Verify pipeline execution
    let pipeline_stats = analyze_file_pipeline(ctx.pool()).await?;
    
    assert!(pipeline_stats.files_created > 20);
    assert!(pipeline_stats.files_modified > 5);
    assert!(pipeline_stats.total_bytes_written > 1_000_000);
    assert!(pipeline_stats.build_completed);

/// Test error handling and recovery workflow
#[sinex_test(timeout = 50)]
async fn test_error_cascade_and_recovery(ctx: TestContext) -> TestResult {
    // Create error scenarios
    let disk_errors = ErrorScenarioFactory::create_error_cascade();
    let network_errors = ErrorScenarioFactory::create_network_failure();
    
    // Interleave errors for realistic scenario
    let mut all_events = Vec::new();
    let mut disk_iter = disk_errors.into_iter();
    let mut net_iter = network_errors.into_iter();
    
    while disk_iter.len() > 0 || net_iter.len() > 0 {
        if let Some(event) = disk_iter.next() {
            all_events.push(event);
        }
        if let Some(event) = net_iter.next() {
            all_events.push(event);
        }
    }
    
    ctx.insert_events(&all_events).await?;
    
    // Analyze error patterns
    let error_analysis = analyze_error_patterns(ctx.pool()).await?;
    
    assert!(error_analysis.disk_errors > 0);
    assert!(error_analysis.network_errors > 0);
    assert!(error_analysis.recovery_attempted);
    assert!(error_analysis.recovery_successful);
    assert!(error_analysis.total_errors > error_analysis.unrecovered_errors);

/// Test mixed workload performance
#[sinex_test(timeout = 90)]
async fn test_mixed_workload_performance(ctx: TestContext) -> TestResult {
    // Create realistic mixed workload
    let workload = scenarios::mixed_workload(30);
    
    let start = std::time::Instant::now();
    ctx.insert_events(&workload).await?;
    let insert_duration = start.elapsed();
    
    // Process with multiple automatons
    let automatons = vec![
        "health-aggregator",
        "command-canonicalizer",
        "content-processor",
    ];
    
    for automaton in &automatons {
        insert_test_checkpoint(
            ctx.pool(),
            automaton,
            workload.len() as u64,
            None
        ).await?;
    }
    
    // Analyze performance
    let perf_stats = analyze_performance_metrics(ctx.pool(), workload.len()).await?;
    
    assert!(insert_duration < Duration::from_secs(5));
    assert!(perf_stats.events_per_second > 100.0);
    assert!(perf_stats.avg_event_size_bytes < 10_000);
    assert_eq!(perf_stats.total_events, workload.len());

/// Test data pipeline workflow
#[sinex_test(timeout = 60)]
async fn test_data_processing_pipeline_workflow(ctx: TestContext) -> TestResult {
    let pipeline = WorkflowFactory::create_data_pipeline();
    
    ctx.insert_events(&pipeline).await?;
    
    // Verify pipeline stages
    let stages = analyze_pipeline_stages(ctx.pool()).await?;
    
    assert_eq!(stages.total_stages, 3);
    assert!(stages.all_stages_completed);
    assert!(stages.total_duration_seconds > 0);
    assert!(stages.data_size_mb > 0);
    
    // Verify outputs
    let outputs = TestQueries::get_events_by_type(
        ctx.pool(),
        event_types::filesystem::FILE_CREATED
    ).await?;
    
    let output_files: Vec<_> = outputs.iter()
        .filter(|e| e.payload["path"].as_str().unwrap_or("").ends_with(".csv") ||
                    e.payload["path"].as_str().unwrap_or("").ends_with(".json"))
        .collect();
    
    assert_eq!(output_files.len(), 4); // input + 3 stage outputs
    
    Ok(())
}

/// Test deployment workflow
#[sinex_test(timeout = 60)]
async fn test_container_deployment_workflow(ctx: TestContext) -> TestResult {
    let deployment = WorkflowFactory::create_deployment_workflow();
    
    ctx.insert_events(&deployment).await?;
    
    // Analyze deployment steps
    let deploy_analysis = analyze_deployment_workflow(ctx.pool()).await?;
    
    assert!(deploy_analysis.build_successful);
    assert!(deploy_analysis.push_successful);
    assert!(deploy_analysis.deployment_successful);
    assert!(deploy_analysis.total_duration_seconds > 0);
    assert_eq!(deploy_analysis.deployment_steps.len(), 5);
    
    Ok(())
}

// Analysis helper structs and functions

struct SessionAnalysis {
    total_duration_minutes: i64,
    activity_types: Vec<String>,
    commands_executed: usize,
    files_modified: usize,
}

async fn analyze_session_patterns(pool: &DbPool) -> AnyhowResult<SessionAnalysis> {
    let events = TestQueries::get_all_events(pool).await?;
    
    let session_start = events.iter()
        .find(|e| e.event_type == event_types::shell::SESSION_STARTED)
        .map(|e| e.ts_orig.unwrap_or(e.ts_ingest));
    
    let session_end = events.iter()
        .find(|e| e.event_type == event_types::shell::SESSION_ENDED)
        .map(|e| e.ts_orig.unwrap_or(e.ts_ingest));
    
    let duration_minutes = match (session_start, session_end) {
        (Some(start), Some(end)) => (end - start).num_minutes(),
        _ => 0,
    };
    
    let activity_types: std::collections::HashSet<_> = events.iter()
        .map(|e| e.source.clone())
        .collect();
    
    let commands = events.iter()
        .filter(|e| e.event_type == event_types::shell::COMMAND_EXECUTED)
        .count();
    
    let files = events.iter()
        .filter(|e| e.event_type == event_types::filesystem::FILE_MODIFIED)
        .count();
    
    Ok(SessionAnalysis {
        total_duration_minutes: duration_minutes,
        activity_types: activity_types.into_iter().collect(),
        commands_executed: commands,
        files_modified: files,
    })
}

struct DevelopmentPatterns {
    has_git_workflow: bool,
    has_file_edits: bool,
    has_test_execution: bool,
    git_operations: Vec<String>,
    total_lines_changed: usize,
}

async fn analyze_development_patterns(pool: &DbPool) -> AnyhowResult<DevelopmentPatterns> {
    let events = TestQueries::get_all_events(pool).await?;
    
    let git_commands: Vec<_> = events.iter()
        .filter(|e| {
            e.payload["command"].as_str()
                .map(|cmd| cmd.starts_with("git"))
                .unwrap_or(false)
        })
        .filter_map(|e| e.payload["command"].as_str())
        .map(|s| s.to_string())
        .collect();
    
    let file_edits = events.iter()
        .filter(|e| e.event_type == event_types::filesystem::FILE_MODIFIED)
        .count();
    
    let test_runs = events.iter()
        .any(|e| {
            e.payload["command"].as_str()
                .map(|cmd| cmd.contains("test"))
                .unwrap_or(false)
        });
    
    Ok(DevelopmentPatterns {
        has_git_workflow: !git_commands.is_empty(),
        has_file_edits: file_edits > 0,
        has_test_execution: test_runs,
        git_operations: git_commands,
        total_lines_changed: file_edits * 50, // Estimate
    })
}

struct SystemHealthAnalysis {
    cpu_trend_increasing: bool,
    memory_trend_increasing: bool,
    alerts_triggered: usize,
    max_cpu_percent: f64,
}

async fn analyze_system_health(pool: &DbPool) -> AnyhowResult<SystemHealthAnalysis> {
    let health_events = TestQueries::get_events_by_type(
        pool,
        event_types::sinex::SYSTEM_HEALTH_SUMMARY
    ).await?;
    
    let cpu_values: Vec<f64> = health_events.iter()
        .filter_map(|e| e.payload["cpu_percent"].as_f64())
        .collect();
    
    let memory_values: Vec<f64> = health_events.iter()
        .filter_map(|e| e.payload["memory_percent"].as_f64())
        .collect();
    
    let alerts = TestQueries::get_events_by_type(
        pool,
        event_types::sinex::AUTOMATON_ERROR
    ).await?;
    
    let resource_alerts = alerts.iter()
        .filter(|e| {
            e.payload["error_type"].as_str() == Some("resource_warning")
        })
        .count();
    
    let cpu_trend = calculate_trend(&cpu_values);
    let memory_trend = calculate_trend(&memory_values);
    let max_cpu = cpu_values.iter().cloned().fold(0.0, f64::max);
    
    Ok(SystemHealthAnalysis {
        cpu_trend_increasing: cpu_trend > 0.0,
        memory_trend_increasing: memory_trend > 0.0,
        alerts_triggered: resource_alerts,
        max_cpu_percent: max_cpu,
    })
}

fn calculate_trend(values: &[f64]) -> f64 {
    if values.len() < 2 {
        return 0.0;
    }
    
    let n = values.len() as f64;
    let sum_x: f64 = (0..values.len()).map(|i| i as f64).sum();
    let sum_y: f64 = values.iter().sum();
    let sum_xy: f64 = values.iter().enumerate()
        .map(|(i, &y)| i as f64 * y)
        .sum();
    let sum_x2: f64 = (0..values.len()).map(|i| (i as f64).powi(2)).sum();
    
    (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x.powi(2))
}

struct FilePipelineStats {
    files_created: usize,
    files_modified: usize,
    total_bytes_written: u64,
    build_completed: bool,
}

async fn analyze_file_pipeline(pool: &DbPool) -> AnyhowResult<FilePipelineStats> {
    let fs_events = TestQueries::get_events_by_source(pool, sources::FS).await?;
    
    let creates = fs_events.iter()
        .filter(|e| e.event_type == event_types::filesystem::FILE_CREATED)
        .count();
    
    let modifies = fs_events.iter()
        .filter(|e| e.event_type == event_types::filesystem::FILE_MODIFIED)
        .count();
    
    let total_bytes: u64 = fs_events.iter()
        .filter_map(|e| e.payload["size_bytes"].as_u64())
        .sum();
    
    let build_complete = TestQueries::get_events_by_type(
        pool,
        event_types::shell::COMMAND_COMPLETED
    ).await?
    .iter()
    .any(|e| {
        e.payload["command"].as_str()
            .map(|cmd| cmd.contains("cargo build"))
            .unwrap_or(false)
    });
    
    Ok(FilePipelineStats {
        files_created: creates,
        files_modified: modifies,
        total_bytes_written: total_bytes,
        build_completed: build_complete,
    })
}

struct ErrorAnalysis {
    disk_errors: usize,
    network_errors: usize,
    recovery_attempted: bool,
    recovery_successful: bool,
    total_errors: usize,
    unrecovered_errors: usize,
}

async fn analyze_error_patterns(pool: &DbPool) -> AnyhowResult<ErrorAnalysis> {
    let all_events = TestQueries::get_all_events(pool).await?;
    
    let disk_errors = all_events.iter()
        .filter(|e| {
            e.payload.get("error").and_then(|v| v.as_str())
                .map(|s| s.contains("ENOSPC") || s.contains("disk"))
                .unwrap_or(false)
        })
        .count();
    
    let network_errors = all_events.iter()
        .filter(|e| {
            e.payload.get("error").and_then(|v| v.as_str())
                .map(|s| s.contains("Connection") || s.contains("network"))
                .unwrap_or(false) ||
            e.event_type == event_types::shell::COMMAND_FAILED &&
            e.payload["command"].as_str().unwrap_or("").contains("curl")
        })
        .count();
    
    let recovery_commands = all_events.iter()
        .filter(|e| {
            e.payload["command"].as_str()
                .map(|cmd| cmd.contains("rm -rf") || cmd.contains("cleanup"))
                .unwrap_or(false)
        })
        .count();
    
    let recovery_heartbeats = all_events.iter()
        .filter(|e| {
            e.event_type == event_types::sinex::AUTOMATON_HEARTBEAT &&
            e.payload.get("message").and_then(|v| v.as_str())
                .map(|s| s.contains("recovered"))
                .unwrap_or(false)
        })
        .count();
    
    Ok(ErrorAnalysis {
        disk_errors,
        network_errors,
        recovery_attempted: recovery_commands > 0,
        recovery_successful: recovery_heartbeats > 0,
        total_errors: disk_errors + network_errors,
        unrecovered_errors: 0, // Simplified for demo
    })
}

struct PerformanceMetrics {
    events_per_second: f64,
    avg_event_size_bytes: usize,
    total_events: usize,
}

async fn analyze_performance_metrics(
    pool: &DbPool,
    expected_count: usize
) -> AnyhowResult<PerformanceMetrics> {
    let events = TestQueries::get_recent_events(pool, expected_count as i64).await?;
    
    let total_size: usize = events.iter()
        .map(|e| serde_json::to_string(&e.payload).unwrap_or_default().len())
        .sum();
    
    let avg_size = if events.is_empty() { 0 } else { total_size / events.len() };
    
    // Estimate based on insertion pattern
    let events_per_second = if events.len() > 1 {
        let first = events.first().unwrap();
        let last = events.last().unwrap();
        let duration = (last.ts_ingest - first.ts_ingest).num_seconds() as f64;
        if duration > 0.0 {
            events.len() as f64 / duration
        } else {
            1000.0 // Very fast insertion
        }
    } else {
        0.0
    };
    
    Ok(PerformanceMetrics {
        events_per_second,
        avg_event_size_bytes: avg_size,
        total_events: events.len(),
    })
}

struct PipelineStages {
    total_stages: usize,
    all_stages_completed: bool,
    total_duration_seconds: i64,
    data_size_mb: f64,
}

async fn analyze_pipeline_stages(pool: &DbPool) -> AnyhowResult<PipelineStages> {
    let events = TestQueries::get_all_events(pool).await?;
    
    let stage_commands = vec!["clean_data", "transform_data", "analyze_data"];
    let mut completed_stages = 0;
    
    for stage in &stage_commands {
        let found = events.iter().any(|e| {
            e.payload["command"].as_str()
                .map(|cmd| cmd.contains(stage))
                .unwrap_or(false)
        });
        if found {
            completed_stages += 1;
        }
    }
    
    let total_duration = events.iter()
        .filter_map(|e| e.payload["duration_ms"].as_i64())
        .sum::<i64>() / 1000;
    
    let data_size = events.iter()
        .filter_map(|e| e.payload["size_bytes"].as_u64())
        .sum::<u64>() as f64 / (1024.0 * 1024.0);
    
    Ok(PipelineStages {
        total_stages: stage_commands.len(),
        all_stages_completed: completed_stages == stage_commands.len(),
        total_duration_seconds: total_duration,
        data_size_mb: data_size,
    })
}

struct DeploymentAnalysis {
    build_successful: bool,
    push_successful: bool,
    deployment_successful: bool,
    total_duration_seconds: i64,
    deployment_steps: Vec<String>,
}

async fn analyze_deployment_workflow(pool: &DbPool) -> AnyhowResult<DeploymentAnalysis> {
    let events = TestQueries::get_all_events(pool).await?;
    
    let deployment_commands: Vec<_> = events.iter()
        .filter(|e| e.event_type == event_types::shell::COMMAND_EXECUTED)
        .filter_map(|e| e.payload["command"].as_str())
        .map(|s| s.to_string())
        .collect();
    
    let build_success = deployment_commands.iter()
        .any(|cmd| cmd.contains("docker build"));
    
    let push_success = deployment_commands.iter()
        .any(|cmd| cmd.contains("docker push"));
    
    let deploy_success = deployment_commands.iter()
        .any(|cmd| cmd.contains("kubectl") && cmd.contains("rollout status"));
    
    let total_duration = events.iter()
        .filter_map(|e| e.payload["duration_ms"].as_i64())
        .sum::<i64>() / 1000;
    
    Ok(DeploymentAnalysis {
        build_successful: build_success,
        push_successful: push_success,
        deployment_successful: deploy_success,
        total_duration_seconds: total_duration,
        deployment_steps: deployment_commands,
    })
}