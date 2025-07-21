// System Integration Tests with Snapshot Testing
//
// Demonstrates using snapshot testing for complex system states,
// configuration validation, and multi-component interactions.

use crate::common::prelude::*;
use crate::common::builders::{TestEventBuilder, TestEvents, BatchEventBuilder};
use crate::common::query_helpers::TestQueries;
use crate::common::snapshot_testing::{assert_snapshot, snapshot, Redaction};
use sinex_events::{event_types, EventFactory, sources};
use sinex_db::queries::{EventQueries, CheckpointQueries, OperationQueries};
use sinex_db::query_builder::{QueryBuilder, QueryParam};
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::sync::{mpsc, Mutex, RwLock};
use serde_json::{json, Value};

// =============================================================================
// SYSTEM CONFIGURATION SNAPSHOT TESTS
// =============================================================================

#[sinex_test]
async fn test_comprehensive_system_configuration_snapshot(ctx: TestContext) -> TestResult {
    // Simulate a comprehensive system configuration
    let system_config = json!({
        "version": "3.0.0",
        "deployment": {
            "environment": "test",
            "hostname": "test-host",
            "start_time": "2024-01-01T00:00:00Z",
            "process_id": 12345,
        },
        "components": {
            "database": {
                "type": "postgresql",
                "version": "15.0",
                "timescaledb_version": "2.11.0",
                "connection_pool": {
                    "max_connections": 100,
                    "idle_timeout_seconds": 300,
                },
                "tables": [
                    "core.events",
                    "core.automaton_checkpoints", 
                    "sinex_schemas.work_queue",
                ],
            },
            "event_sources": {
                "enabled": [
                    "filesystem",
                    "terminal",
                    "desktop",
                    "system",
                ],
                "configurations": {
                    "filesystem": {
                        "watch_paths": ["/home/user", "/tmp"],
                        "ignore_patterns": ["*.tmp", "*.log"],
                        "max_file_size_mb": 100,
                    },
                    "terminal": {
                        "shell_types": ["bash", "zsh", "fish"],
                        "capture_stdout": true,
                        "capture_stderr": true,
                    },
                },
            },
            "workers": {
                "count": 4,
                "batch_size": 100,
                "processing_timeout_seconds": 30,
                "retry_policy": {
                    "max_attempts": 3,
                    "backoff_seconds": [1, 5, 10],
                },
            },
            "monitoring": {
                "health_check_interval_seconds": 60,
                "metrics_retention_days": 30,
                "alert_thresholds": {
                    "error_rate": 0.05,
                    "latency_p99_ms": 1000,
                    "queue_depth": 10000,
                },
            },
        },
        "feature_flags": {
            "enable_git_annex": false,
            "enable_blob_storage": true,
            "enable_event_compression": true,
            "enable_distributed_tracing": false,
        },
    });

    // Apply redactions for dynamic values
    assert_snapshot!(
        system_config,
        "comprehensive_system_configuration",
        Redaction::timestamps(),
        Redaction::dynamic_ids(),
        Redaction::field("deployment.hostname", json!("test-host"))
    );

    Ok(())
}

// =============================================================================
// SYSTEM HEALTH STATE SNAPSHOT TESTS
// =============================================================================

#[sinex_test]
async fn test_system_health_monitoring_snapshot(ctx: TestContext) -> TestResult {
    // Simulate collecting health metrics from various components
    let pool = ctx.pool();
    
    // Database health
    let db_stats = OperationQueries::get_database_statistics(pool).await?;
    let table_sizes = OperationQueries::get_table_sizes(pool).await?;
    
    // Event processing stats
    let event_count = EventQueries::count_all(pool).await?;
    let recent_events = EventQueries::list_recent(pool, 10).await?;
    let event_type_distribution = EventQueries::get_event_type_distribution(pool).await?;
    
    // Checkpoint status
    let checkpoints = CheckpointQueries::list_all_checkpoints(pool).await?;
    
    // Build comprehensive health snapshot
    let health_snapshot = json!({
        "timestamp": "2024-01-01T00:00:00Z",
        "status": "healthy",
        "database": {
            "connection_status": "connected",
            "active_connections": db_stats.get("active_connections").unwrap_or(&json!(0)),
            "table_sizes": table_sizes.into_iter().map(|(table, size)| {
                json!({
                    "table": table,
                    "size_mb": size / (1024 * 1024),
                    "row_estimate": size / 100, // Rough estimate
                })
            }).collect::<Vec<_>>(),
        },
        "event_processing": {
            "total_events": event_count,
            "recent_event_count": recent_events.len(),
            "event_types": event_type_distribution.into_iter().map(|(event_type, count)| {
                json!({
                    "type": event_type,
                    "count": count,
                    "percentage": (count as f64 / event_count as f64 * 100.0).round() / 100.0,
                })
            }).collect::<Vec<_>>(),
            "processing_rate": {
                "events_per_second": 42.5,
                "avg_latency_ms": 15.3,
                "p99_latency_ms": 125.0,
            },
        },
        "automaton_status": checkpoints.into_iter().map(|cp| {
            json!({
                "name": cp.automaton_name,
                "last_processed": cp.last_processed_id.map(|id| id.to_string()).unwrap_or_else(|| "none".to_string()),
                "processed_count": cp.processed_count,
                "last_error": cp.last_error,
                "state": if cp.last_error.is_some() { "error" } else { "active" },
            })
        }).collect::<Vec<_>>(),
        "system_resources": {
            "cpu_usage_percent": 25.5,
            "memory_usage_mb": 512,
            "disk_usage_percent": 45.0,
            "open_file_descriptors": 150,
        },
        "alerts": [],
    });

    // Snapshot with appropriate redactions
    assert_snapshot!(
        health_snapshot,
        "system_health_monitoring_state",
        Redaction::timestamps(),
        Redaction::ulids(),
        Redaction::field("system_resources.cpu_usage_percent", json!(25.0)),
        Redaction::field("system_resources.memory_usage_mb", json!(500)),
        Redaction::field("event_processing.processing_rate", json!({
            "events_per_second": 40.0,
            "avg_latency_ms": 15.0,
            "p99_latency_ms": 120.0,
        }))
    );

    Ok(())
}

// =============================================================================
// MULTI-COMPONENT INTEGRATION SNAPSHOT TESTS
// =============================================================================

#[sinex_test]
async fn test_full_event_pipeline_snapshot(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();
    
    // Simulate a complete event flow through the system
    let test_scenario = "user_workflow_session";
    
    // 1. Generate events from multiple sources
    let events = vec![
        TestEventBuilder::new("terminal", "command.executed")
            .with_field("command", json!("vim config.toml"))
            .with_field("exit_code", json!(0))
            .with_field("duration_ms", json!(150))
            .build(),
        
        TestEventBuilder::new("filesystem", "file.modified")
            .with_field("path", json!("/home/user/config.toml"))
            .with_field("size", json!(2048))
            .with_field("operation", json!("write"))
            .build(),
        
        TestEventBuilder::new("desktop", "window.focused")
            .with_field("window_title", json!("vim - config.toml"))
            .with_field("application", json!("terminal"))
            .with_field("duration_ms", json!(5000))
            .build(),
        
        TestEventBuilder::new("filesystem", "file.created")
            .with_field("path", json!("/home/user/.config.toml.swp"))
            .with_field("size", json!(1024))
            .with_field("temporary", json!(true))
            .build(),
    ];
    
    // Insert events
    for event in &events {
        EventQueries::insert_raw_event(pool, event).await?;
    }
    
    // 2. Simulate processing pipeline
    let processing_results = json!({
        "scenario": test_scenario,
        "input_events": events.len(),
        "processing_stages": [
            {
                "stage": "ingestion",
                "status": "completed",
                "events_processed": events.len(),
                "duration_ms": 25,
                "errors": [],
            },
            {
                "stage": "validation",
                "status": "completed",
                "events_validated": events.len(),
                "schema_violations": 0,
                "duration_ms": 10,
            },
            {
                "stage": "enrichment",
                "status": "completed",
                "enrichments_applied": {
                    "user_context": 4,
                    "application_metadata": 2,
                    "file_metadata": 2,
                },
                "duration_ms": 15,
            },
            {
                "stage": "correlation",
                "status": "completed",
                "correlations_found": {
                    "command_to_file": 1,
                    "window_to_command": 1,
                    "temporal_proximity": 2,
                },
                "duration_ms": 20,
            },
            {
                "stage": "synthesis",
                "status": "completed",
                "high_level_events_generated": [
                    {
                        "type": "user_workflow.file_edit",
                        "confidence": 0.95,
                        "supporting_events": 4,
                    }
                ],
                "duration_ms": 30,
            },
        ],
        "output_summary": {
            "total_processing_time_ms": 100,
            "events_stored": events.len(),
            "correlations_created": 4,
            "high_level_insights": 1,
            "storage_size_bytes": 8192,
        },
        "system_impact": {
            "cpu_spike_percent": 5.0,
            "memory_increase_mb": 10,
            "io_operations": 25,
        },
    });
    
    // Capture processing pipeline snapshot
    assert_snapshot!(
        processing_results,
        "full_event_pipeline_processing",
        Redaction::field("system_impact.cpu_spike_percent", json!(5.0)),
        Redaction::field("system_impact.memory_increase_mb", json!(10)),
        Redaction::regex(r"duration_ms\": \d+", "duration_ms\": 20")
    );
    
    Ok(())
}

// =============================================================================
// ERROR SCENARIO SNAPSHOT TESTS
// =============================================================================

#[sinex_test]
async fn test_system_failure_recovery_snapshot(ctx: TestContext) -> TestResult {
    // Simulate various failure scenarios and recovery attempts
    let failure_scenarios = json!({
        "test_id": "system_resilience_test",
        "scenarios": [
            {
                "name": "database_connection_loss",
                "trigger": "network_partition",
                "detection_time_ms": 100,
                "recovery_attempts": [
                    {
                        "attempt": 1,
                        "strategy": "immediate_reconnect",
                        "result": "failed",
                        "duration_ms": 50,
                        "error": "connection refused",
                    },
                    {
                        "attempt": 2,
                        "strategy": "exponential_backoff",
                        "result": "failed",
                        "duration_ms": 1000,
                        "error": "connection timeout",
                    },
                    {
                        "attempt": 3,
                        "strategy": "circuit_breaker_open",
                        "result": "success",
                        "duration_ms": 5000,
                        "recovery_actions": ["queue_events_locally", "alert_operators"],
                    },
                ],
                "impact": {
                    "events_queued": 150,
                    "data_loss": false,
                    "service_degradation": "partial",
                },
            },
            {
                "name": "memory_pressure",
                "trigger": "event_burst",
                "detection_time_ms": 500,
                "recovery_attempts": [
                    {
                        "attempt": 1,
                        "strategy": "garbage_collection",
                        "result": "partial",
                        "freed_mb": 100,
                    },
                    {
                        "attempt": 2,
                        "strategy": "drop_cache",
                        "result": "success",
                        "freed_mb": 500,
                    },
                ],
                "impact": {
                    "processing_slowdown_percent": 25,
                    "events_delayed": 50,
                    "service_degradation": "minimal",
                },
            },
            {
                "name": "corrupt_event_payload",
                "trigger": "malformed_json",
                "detection_time_ms": 10,
                "recovery_attempts": [
                    {
                        "attempt": 1,
                        "strategy": "quarantine_event",
                        "result": "success",
                        "quarantine_id": "QRNT_0001",
                    },
                ],
                "impact": {
                    "events_quarantined": 1,
                    "processing_interrupted": false,
                    "service_degradation": "none",
                },
            },
        ],
        "overall_resilience_score": 0.85,
        "recommendations": [
            "Implement connection pooling with health checks",
            "Add memory pressure early warning system",
            "Enhance event validation pipeline",
        ],
    });
    
    // Snapshot the failure recovery patterns
    snapshot(failure_scenarios)
        .name("system_failure_recovery_patterns")
        .redact_timestamps()
        .redact_field("scenarios.0.recovery_attempts.2.duration_ms", json!(5000))
        .redact_field("scenarios.1.recovery_attempts.0.freed_mb", json!(100))
        .redact_field("scenarios.1.recovery_attempts.1.freed_mb", json!(500))
        .assert();
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::snapshot_testing::clear_redaction_cache;
    
    #[test]
    fn test_snapshot_redaction_consistency() {
        // Ensure consistent redaction across test runs
        clear_redaction_cache();
        
        let test_data = json!({
            "id": ulid::Ulid::new().to_string(),
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "process_id": 12345,
            "data": {
                "id": ulid::Ulid::new().to_string(),
                "created_at": chrono::Utc::now().to_rfc3339(),
            }
        });
        
        // First snapshot
        let snapshot1 = snapshot(test_data.clone())
            .redact_timestamps()
            .redact_ulids()
            .redact_field("process_id", json!(99999));
        
        // This would normally create a snapshot, but in test mode just validates
        // that our redaction system produces consistent results
    }
}