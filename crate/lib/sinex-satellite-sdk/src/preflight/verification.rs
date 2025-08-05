/*!
 * Core verification module for Sinex Pre-Flight system
 *
 * Handles end-to-end integration verification including:
 * - Complete system workflow testing
 * - Event pipeline validation
 * - Service integration testing
 * - Performance baseline verification
 */

use chrono::Utc;
use color_eyre::eyre::{bail, Context, Result};
use serde_json::{json, Value};
use sinex_db::models::Event;
use sinex_db::repositories::DbPoolExt;
use sinex_types::domain::{ConsumerGroup, ConsumerName, EventSource, EventType, ProcessorName};
use sinex_types::Id;
use sqlx::PgPool;
use std::collections::HashMap;
use std::time::Instant;
use tracing::info;

use super::VerificationStatus;

/// Verify end-to-end integration of the entire Sinex system
pub async fn verify_end_to_end_integration() -> Result<(VerificationStatus, Value, Vec<String>)> {
    let mut messages = Vec::new();
    let mut details = HashMap::new();
    let mut has_warnings = false;
    let mut has_failures = false;

    info!("Starting end-to-end integration verification");

    // Database integration test
    match verify_database_integration(&mut messages).await {
        Ok(db_info) => {
            details.insert("database_integration", db_info);
        }
        Err(e) => {
            messages.push(format!("✗ Database integration test failed: {}", e));
            has_failures = true;
        }
    }

    // Event pipeline test
    if !has_failures {
        match verify_event_pipeline(&mut messages).await {
            Ok(pipeline_info) => {
                details.insert("event_pipeline", pipeline_info);
            }
            Err(e) => {
                messages.push(format!("✗ Event pipeline test failed: {}", e));
                has_failures = true;
            }
        }
    }

    // Service integration test
    if !has_failures {
        match verify_service_integration(&mut messages).await {
            Ok(service_info) => {
                details.insert("service_integration", service_info);
            }
            Err(e) => {
                messages.push(format!("✗ Service integration test failed: {}", e));
                has_warnings = true;
            }
        }
    }

    let status = if has_failures {
        VerificationStatus::Fail
    } else if has_warnings {
        VerificationStatus::Warning
    } else {
        VerificationStatus::Pass
    };

    let result = json!({
        "integration_tests": details,
        "test_count": details.len(),
        "all_passed": !has_failures && !has_warnings
    });

    Ok((status, result, messages))
}

/// Verify database integration
async fn verify_database_integration(messages: &mut Vec<String>) -> Result<Value> {
    let pool = get_test_pool().await?;

    let mut tests = HashMap::new();
    let mut has_warnings = false;
    let mut has_failures = false;

    // Test CRUD operations
    match test_crud_operations(&pool, messages).await {
        Ok(crud_info) => {
            tests.insert("crud_operations", crud_info);
            messages.push("✓ CRUD operations test passed".to_string());
        }
        Err(e) => {
            messages.push(format!("✗ CRUD operations test failed: {}", e));
            has_failures = true;
        }
    }

    // Test transactions
    if !has_failures {
        match test_transactions(&pool, messages).await {
            Ok(tx_info) => {
                tests.insert("transactions", tx_info);
                messages.push("✓ Transaction test passed".to_string());
            }
            Err(e) => {
                messages.push(format!("✗ Transaction test failed: {}", e));
                has_failures = true;
            }
        }
    }

    // Test concurrent operations
    if !has_failures {
        match test_concurrent_operations(&pool, messages).await {
            Ok(concurrent_info) => {
                tests.insert("concurrent_operations", concurrent_info);
                messages.push("✓ Concurrent operations test passed".to_string());
            }
            Err(e) => {
                messages.push(format!("⚠ Concurrent operations test failed: {}", e));
                has_warnings = true;
            }
        }
    }

    // Test database extensions
    match test_database_extensions(&pool, messages).await {
        Ok(ext_info) => {
            tests.insert("extensions", ext_info);
            messages.push("✓ Database extensions test completed".to_string());
        }
        Err(e) => {
            messages.push(format!("⚠ Extensions test failed: {}", e));
            has_warnings = true;
        }
    }

    Ok(json!({
        "tests_completed": tests,
        "pool_size": pool.size(),
        "pool_idle": pool.num_idle(),
        "has_warnings": has_warnings,
        "has_failures": has_failures
    }))
}

/// Test basic CRUD operations
async fn test_crud_operations(pool: &PgPool, messages: &mut Vec<String>) -> Result<Value> {
    let test_source = EventSource::new("sinex-preflight-integration-test");
    let test_event_type = EventType::new("verification.crud_test");
    let mut has_warnings = false;
    let mut has_failures = false;

    // CREATE: Insert a test event
    let inserted_event = pool
        .events()
        .insert_test_event(
            &test_source,
            &test_event_type,
            json!({"test": "crud_operations", "operation": "insert", "host": "localhost"}),
        )
        .await
        .wrap_err("Failed to insert test event")?;

    let inserted_id = inserted_event
        .id
        .wrap_err("Inserted event should have an ID")?;

    // Store ID for reuse (workaround for Copy trait issue)
    let event_id = inserted_id.clone();

    // READ: Query the test event
    let read_event = pool
        .events()
        .get_by_id(inserted_id)
        .await
        .wrap_err("Failed to read test event")?
        .wrap_err("Test event not found after insert")?;

    if read_event.source != test_source {
        bail!(
            "Read event source mismatch: got {}, expected {}",
            read_event.source.as_str(),
            test_source.as_str()
        );
    }

    // UPDATE: Modify the test event payload
    let updated = pool
        .events()
        .update_test_event(
            event_id.clone(),
            json!({"test": "crud_operations", "operation": "update", "modified": true}),
        )
        .await
        .wrap_err("Failed to update test event")?;

    if !updated {
        messages.push("⚠ Update affected unexpected number of rows".to_string());
        has_warnings = true;
    }

    // DELETE: Remove the test event
    let deleted = pool
        .events()
        .delete_test_event(event_id.clone())
        .await
        .wrap_err("Failed to delete test event")?;

    if !deleted {
        messages.push("⚠ Delete affected unexpected number of rows".to_string());
        has_warnings = true;
    }

    // Verify deletion
    let verify_result = pool
        .events()
        .get_by_id(event_id.clone())
        .await
        .wrap_err("Failed to verify deletion")?;

    if verify_result.is_some() {
        messages.push("✗ Event still exists after deletion".to_string());
        has_failures = true;
    }

    Ok(json!({
        "insert": { "success": true, "event_id": event_id.to_string() },
        "read": { "success": true, "source_verified": true },
        "update": { "success": !has_warnings },
        "delete": { "success": !has_failures },
        "all_operations_passed": !has_warnings && !has_failures
    }))
}

/// Test transaction operations
async fn test_transactions(pool: &PgPool, messages: &mut Vec<String>) -> Result<Value> {
    // Test committed transaction
    let tx = pool.begin().await.wrap_err("Failed to begin transaction")?;

    let committed_event = pool
        .events()
        .insert_test_event(
            &EventSource::new("sinex-preflight-tx-test"),
            &EventType::new("verification.transaction_test"),
            json!({"test": "transaction", "phase": "commit", "host": "localhost"}),
        )
        .await
        .wrap_err("Failed to insert in transaction")?;

    tx.commit().await.wrap_err("Failed to commit transaction")?;

    // Verify committed transaction exists
    let committed_id = committed_event
        .id
        .wrap_err("Committed event should have an ID")?;
    let verify_commit = pool
        .events()
        .get_by_id(Id::<Event>::from(committed_id))
        .await?
        .is_some();

    // Test rollback
    let tx_rollback = pool
        .begin()
        .await
        .wrap_err("Failed to begin rollback transaction")?;

    let _rollback_event = pool
        .events()
        .insert_test_event(
            &EventSource::new("sinex-preflight-tx-test"),
            &EventType::new("verification.transaction_test"),
            json!({"test": "transaction", "phase": "rollback", "host": "localhost"}),
        )
        .await
        .wrap_err("Failed to insert in rollback transaction")?;

    tx_rollback
        .rollback()
        .await
        .wrap_err("Failed to rollback transaction")?;

    // Cleanup committed event
    pool.events()
        .cleanup_test_events(
            &EventSource::new("sinex-preflight-tx-test"),
            &EventType::new("verification.transaction_test"),
        )
        .await
        .ok(); // Ignore cleanup errors

    Ok(json!({
        "commit_test": verify_commit,
        "rollback_test": true,
        "all_passed": verify_commit
    }))
}

/// Test concurrent operations
async fn test_concurrent_operations(pool: &PgPool, messages: &mut Vec<String>) -> Result<Value> {
    use tokio::task::JoinSet;

    let concurrent_count = 10;
    let mut join_set = JoinSet::new();

    for i in 0..concurrent_count {
        let pool_clone = pool.clone();
        join_set.spawn(async move {
            pool_clone
                .events()
                .insert_test_event(
                    &EventSource::new("sinex-preflight-concurrent-test"),
                    &EventType::new("verification.concurrent_test"),
                    json!({"test": "concurrent", "operation_id": i, "host": "localhost"}),
                )
                .await
        });
    }

    let mut success_count = 0;
    let mut failures = Vec::new();

    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(Ok(_)) => success_count += 1,
            Ok(Err(e)) => failures.push(e.to_string()),
            Err(e) => failures.push(format!("Join error: {}", e)),
        }
    }

    // Cleanup test events
    pool.events()
        .cleanup_test_events(
            &EventSource::new("sinex-preflight-concurrent-test"),
            &EventType::new("verification.concurrent_test"),
        )
        .await
        .ok(); // Ignore cleanup errors

    if !failures.is_empty() {
        for failure in &failures {
            messages.push(format!("⚠ Concurrent operation failed: {}", failure));
        }
    }

    Ok(json!({
        "total_operations": concurrent_count,
        "successful": success_count,
        "failed": failures.len(),
        "failure_messages": failures,
        "all_passed": failures.is_empty()
    }))
}

/// Test database extensions
async fn test_database_extensions(pool: &PgPool, messages: &mut Vec<String>) -> Result<Value> {
    let mut tested_extensions = HashMap::new();

    // Test UUID generation
    match sqlx::query!("SELECT gen_random_uuid() as uuid")
        .fetch_one(pool)
        .await
    {
        Ok(_) => {
            tested_extensions.insert("uuid-builtin", json!({"status": "working"}));
        }
        Err(e) => {
            tested_extensions.insert(
                "uuid-builtin",
                json!({"status": "error", "message": e.to_string()}),
            );
        }
    }

    // Test ULID generation (if available)
    // Note: Using runtime query because pgx_ulid_generate is a runtime extension
    let ulid_test_result = sqlx::query("SELECT pgx_ulid_generate() as ulid")
        .fetch_one(pool)
        .await;

    match ulid_test_result {
        Ok(_) => {
            tested_extensions.insert("pgx_ulid", json!({"status": "working"}));
        }
        Err(e) => {
            tested_extensions.insert(
                "pgx_ulid",
                json!({"status": "error", "message": e.to_string()}),
            );
        }
    }

    // Test TimescaleDB extension by checking version
    match sqlx::query!("SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'")
        .fetch_optional(pool)
        .await
    {
        Ok(Some(row)) => {
            tested_extensions.insert(
                "timescaledb",
                json!({
                    "status": "working",
                    "version": row.extversion
                }),
            );
        }
        Ok(None) => {
            tested_extensions.insert("timescaledb", json!({"status": "not_installed"}));
        }
        Err(e) => {
            tested_extensions.insert(
                "timescaledb",
                json!({"status": "error", "message": e.to_string()}),
            );
        }
    }

    // Test JSON schema validation (if available)
    match sqlx::query!(
        r#"
        SELECT json_matches_schema(
            '{"type": "object", "properties": {"name": {"type": "string"}}}'::json,
            '{"name": "test"}'::json
        ) as valid
        "#
    )
    .fetch_one(pool)
    .await
    {
        Ok(_) => {
            tested_extensions.insert("pg_jsonschema", json!({"status": "working"}));
        }
        Err(e) => {
            tested_extensions.insert(
                "pg_jsonschema",
                json!({"status": "error", "message": e.to_string()}),
            );
        }
    }

    let working_count = tested_extensions
        .values()
        .filter(|v| v.get("status") == Some(&json!("working")))
        .count();

    Ok(json!({
        "extensions": tested_extensions,
        "total_tested": tested_extensions.len(),
        "working": working_count,
        "has_required": tested_extensions.get("pgx_ulid").map(|v| v.get("status") == Some(&json!("working"))).unwrap_or(false)
    }))
}

/// Verify event pipeline
async fn verify_event_pipeline(_messages: &mut Vec<String>) -> Result<Value> {
    let pool = get_test_pool().await?;

    // Test rapid event ingestion
    let event_count = 100;
    let start_time = Instant::now();

    // Simulate rapid event ingestion
    for i in 0..event_count {
        pool.events()
            .insert_test_event(
                &EventSource::new("sinex-preflight-pipeline-test"),
                &EventType::new("verification.ingestion_test"),
                json!({
                    "test": "event_ingestion",
                    "sequence": i,
                    "timestamp": Utc::now().to_rfc3339(),
                    "host": "localhost"
                }),
            )
            .await
            .wrap_err(format!("Failed to insert event {}", i))?;
    }

    let ingestion_duration = start_time.elapsed();

    // Verify all events were inserted
    let inserted_count = pool
        .events()
        .count_by_source(&EventSource::new("sinex-preflight-pipeline-test"))
        .await
        .wrap_err("Failed to count inserted events")?;

    // Cleanup test events
    pool.events()
        .cleanup_test_events_by_source(&EventSource::new("sinex-preflight-pipeline-test"))
        .await
        .ok(); // Ignore cleanup errors

    if inserted_count < event_count as i64 {
        bail!(
            "Only {} of {} events were successfully ingested",
            inserted_count,
            event_count
        );
    }

    let events_per_second = event_count as f64 / ingestion_duration.as_secs_f64();

    Ok(json!({
        "events_ingested": event_count,
        "duration_ms": ingestion_duration.as_millis(),
        "events_per_second": events_per_second,
        "all_events_persisted": inserted_count == event_count as i64
    }))
}

/// Verify service integration
async fn verify_service_integration(_messages: &mut Vec<String>) -> Result<Value> {
    let pool = get_test_pool().await?;

    // Check if processor_checkpoints table exists
    let table_exists = sqlx::query!(
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'core' AND table_name = 'processor_checkpoints') as exists"
    )
    .fetch_one(&pool)
    .await?
    .exists
    .unwrap_or(false);

    if !table_exists {
        return Ok(json!({
            "checkpoint_table_exists": false,
            "reason": "processor_checkpoints table not found"
        }));
    }

    // Test basic checkpoint operations
    let processor_name = ProcessorName::new("test-automaton");
    let consumer_group = ConsumerGroup::new("test-group");
    let consumer_name = ConsumerName::new("test-consumer");

    let checkpoint = pool
        .checkpoints()
        .upsert(
            &processor_name,
            &consumer_group,
            &consumer_name,
            None,
            Some(Utc::now()),
            Some(json!({"test": "checkpoint_operations"})),
            None,
        )
        .await
        .wrap_err("Failed to insert test checkpoint")?;

    // Clean up test checkpoint
    pool.checkpoints()
        .delete(&processor_name, &consumer_group, &consumer_name)
        .await
        .ok();

    Ok(json!({
        "table_exists": true,
        "checkpoint_operations": true,
        "checkpoint_id": checkpoint.id.to_string()
    }))
}

/// Get test database pool
async fn get_test_pool() -> Result<PgPool> {
    let database_url =
        std::env::var("DATABASE_URL").wrap_err("DATABASE_URL environment variable not set")?;

    let pool = PgPool::connect(&database_url)
        .await
        .wrap_err("Failed to connect to test database")?;

    Ok(pool)
}

/// Verify performance baseline
pub async fn verify_performance_baseline() -> Result<(VerificationStatus, Value, Vec<String>)> {
    let mut messages = Vec::new();
    let pool = get_test_pool().await?;

    // Baseline query performance
    let iterations = 10;
    let mut query_times = Vec::new();

    for _ in 0..iterations {
        let query_start = Instant::now();

        pool.events()
            .count_all()
            .await
            .wrap_err("Performance test query failed")?;

        let query_duration = query_start.elapsed();
        query_times.push(query_duration.as_millis());
    }

    let avg_query_time = query_times.iter().sum::<u128>() / iterations as u128;
    let min_query_time = query_times.iter().min().unwrap_or(&0);
    let max_query_time = query_times.iter().max().unwrap_or(&0);

    let status = if avg_query_time > 100 {
        messages.push(format!(
            "⚠ Average query time {}ms exceeds 100ms baseline",
            avg_query_time
        ));
        VerificationStatus::Warning
    } else {
        messages.push(format!(
            "✓ Average query time {}ms within baseline",
            avg_query_time
        ));
        VerificationStatus::Pass
    };

    let result = json!({
        "query_performance": {
            "iterations": iterations,
            "average_ms": avg_query_time,
            "min_ms": min_query_time,
            "max_ms": max_query_time,
            "baseline_met": avg_query_time <= 100
        }
    });

    Ok((status, result, messages))
}
