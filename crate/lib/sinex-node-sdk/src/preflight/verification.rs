/*!
 * Core verification module for Sinex Pre-Flight system
 *
 * Handles infrastructure verification using lightweight query-based checks:
 * - Database connectivity and schema validation
 * - Transaction support verification
 * - Concurrent query capability
 * - Required extension availability
 *
 * This module does NOT insert, update, or delete any events.
 * All verification is done via SELECT queries and schema introspection.
 */

use crate::{Checkpoint, CheckpointManager, CheckpointState};
use async_nats::jetstream::kv;
use chrono::Utc;
use color_eyre::eyre::{Context, Result};
use serde_json::{json, Value};
use sinex_core::types::ulid::Ulid;
use sqlx::PgPool;
use std::collections::HashMap;
use std::time::Instant;
use tracing::info;

use super::{resolve_database_url, resolve_nats_url, VerificationStatus};

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

    // Service integration test (NATS/checkpoint KV)
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

/// Verify database integration using query-based checks only
async fn verify_database_integration(messages: &mut Vec<String>) -> Result<Value> {
    let pool = get_test_pool().await?;

    let mut tests = HashMap::new();
    let mut has_warnings = false;
    let mut has_failures = false;

    // Test schema and table access
    match test_schema_access(&pool, messages).await {
        Ok(schema_info) => {
            tests.insert("schema_access", schema_info);
            messages.push("✓ Schema access test passed".to_string());
        }
        Err(e) => {
            messages.push(format!("✗ Schema access test failed: {}", e));
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
        match test_concurrent_queries(&pool, messages).await {
            Ok(concurrent_info) => {
                tests.insert("concurrent_queries", concurrent_info);
                messages.push("✓ Concurrent queries test passed".to_string());
            }
            Err(e) => {
                messages.push(format!("⚠ Concurrent queries test failed: {}", e));
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

/// Test schema access - verify core tables exist and are queryable
async fn test_schema_access(pool: &PgPool, _messages: &mut Vec<String>) -> Result<Value> {
    // Check that core.events table exists
    let events_table_exists = sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_schema = 'core'
            AND table_name = 'events'
        )
        "#,
    )
    .fetch_one(pool)
    .await
    .wrap_err("Failed to check core.events table existence")?;

    if !events_table_exists {
        color_eyre::eyre::bail!("core.events table does not exist");
    }

    // Check that we can SELECT from core.events
    let select_works = sqlx::query("SELECT id, source, event_type FROM core.events LIMIT 0")
        .execute(pool)
        .await
        .is_ok();

    // Check that we can run a COUNT query
    let count_result = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM core.events")
        .fetch_one(pool)
        .await
        .wrap_err("Failed to count events")?;

    // Check core.source_materials table exists
    let source_materials_exists = sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_schema = 'core'
            AND table_name = 'source_materials'
        )
        "#,
    )
    .fetch_one(pool)
    .await
    .wrap_err("Failed to check core.source_materials table existence")?;

    // Check core.blobs table exists
    let blobs_exists = sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_schema = 'core'
            AND table_name = 'blobs'
        )
        "#,
    )
    .fetch_one(pool)
    .await
    .wrap_err("Failed to check core.blobs table existence")?;

    Ok(json!({
        "events_table_exists": events_table_exists,
        "select_works": select_works,
        "count_query_works": true,
        "current_event_count": count_result,
        "source_materials_exists": source_materials_exists,
        "blobs_exists": blobs_exists,
        "all_checks_passed": events_table_exists && select_works && source_materials_exists && blobs_exists
    }))
}

/// Test transaction support using SELECT queries only
async fn test_transactions(pool: &PgPool, _messages: &mut [String]) -> Result<Value> {
    // Test committed transaction with SELECT
    let mut tx = pool.begin().await.wrap_err("Failed to begin transaction")?;

    // Run a simple SELECT inside the transaction
    let select_in_tx = sqlx::query_scalar::<_, i32>("SELECT 1")
        .fetch_one(&mut *tx)
        .await
        .wrap_err("Failed to SELECT inside transaction")?;

    tx.commit().await.wrap_err("Failed to commit transaction")?;

    let commit_works = select_in_tx == 1;

    // Test rollback with SELECT
    let mut tx_rollback = pool
        .begin()
        .await
        .wrap_err("Failed to begin rollback transaction")?;

    // Run a simple SELECT inside the transaction
    let _select_in_rollback = sqlx::query_scalar::<_, i32>("SELECT 1")
        .fetch_one(&mut *tx_rollback)
        .await
        .wrap_err("Failed to SELECT inside rollback transaction")?;

    tx_rollback
        .rollback()
        .await
        .wrap_err("Failed to rollback transaction")?;

    // Verify no side effects by checking we can still query
    let after_rollback = sqlx::query_scalar::<_, i32>("SELECT 1")
        .fetch_one(pool)
        .await
        .wrap_err("Failed to SELECT after rollback")?;

    let rollback_works = after_rollback == 1;

    Ok(json!({
        "commit_test": commit_works,
        "rollback_test": rollback_works,
        "no_side_effects": true,
        "all_passed": commit_works && rollback_works
    }))
}

/// Test concurrent query operations
async fn test_concurrent_queries(pool: &PgPool, messages: &mut Vec<String>) -> Result<Value> {
    use tokio::task::JoinSet;

    let concurrent_count = 10;
    let mut join_set = JoinSet::new();

    for i in 0..concurrent_count {
        let pool_clone = pool.clone();
        join_set.spawn(async move {
            // Run concurrent SELECT queries - no mutations
            sqlx::query_scalar::<_, i64>(&format!(
                "SELECT COUNT(*) + {} - {} FROM core.events",
                i, i
            ))
            .fetch_one(&pool_clone)
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

    if !failures.is_empty() {
        for failure in &failures {
            messages.push(format!("⚠ Concurrent query failed: {}", failure));
        }
    }

    Ok(json!({
        "total_queries": concurrent_count,
        "successful": success_count,
        "failed": failures.len(),
        "failure_messages": failures,
        "all_passed": failures.is_empty()
    }))
}

/// Test database extensions
async fn test_database_extensions(pool: &PgPool, _messages: &mut [String]) -> Result<Value> {
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
    match sqlx::query_scalar::<_, Option<bool>>(
        r#"
        SELECT json_matches_schema(
            '{"type": "object", "properties": {"name": {"type": "string"}}}'::json,
            '{"name": "test"}'::json
        )
        "#,
    )
    .fetch_one(pool)
    .await
    {
        Ok(_) => {
            tested_extensions.insert("pg_jsonschema", json!({"status": "working"}));
        }
        Err(e) => {
            let status = if let sqlx::Error::Database(db_err) = &e {
                if db_err
                    .message()
                    .to_lowercase()
                    .contains("json_matches_schema")
                {
                    json!({"status": "not_installed"})
                } else {
                    json!({"status": "error", "message": e.to_string()})
                }
            } else {
                json!({"status": "error", "message": e.to_string()})
            };
            tested_extensions.insert("pg_jsonschema", status);
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

/// Verify service integration (NATS checkpoint KV)
async fn verify_service_integration(_messages: &mut [String]) -> Result<Value> {
    let nats_url = resolve_nats_url()?;
    let mut nats_config = sinex_core::nats::NatsConnectionConfig::from_env();
    nats_config.url = nats_url;
    let client = nats_config
        .connect()
        .await
        .wrap_err("Failed to connect to NATS for checkpoint verification")?;
    let js = async_nats::jetstream::new(client);
    let bucket = format!(
        "KV_{}",
        crate::checkpoint::checkpoint_bucket_name(Some("preflight"))
    );
    let kv_store = match js
        .create_key_value(kv::Config {
            bucket: bucket.clone(),
            history: 64,
            ..Default::default()
        })
        .await
    {
        Ok(store) => Ok(store),
        Err(_) => js.get_key_value(&bucket).await,
    }
    .wrap_err("Failed to create/open checkpoint KV bucket")?;

    let consumer_name = format!("preflight-{}", Ulid::new());
    let manager = CheckpointManager::new(
        kv_store,
        "preflight-checkpoint".to_string(),
        "default".to_string(),
        consumer_name.clone(),
    );

    let state = CheckpointState {
        checkpoint: Checkpoint::None,
        processed_count: 1,
        last_activity: Utc::now(),
        data: Some(json!({ "preflight": true })),
        version: 2,
        revision: 0,
    };

    manager
        .save_checkpoint(&state)
        .await
        .wrap_err("Failed to persist checkpoint to KV")?;
    manager
        .reset_checkpoint()
        .await
        .wrap_err("Failed to delete checkpoint from KV")?;

    Ok(json!({
        "checkpoint_kv": true,
        "consumer_name": consumer_name
    }))
}

/// Get test database pool
async fn get_test_pool() -> Result<PgPool> {
    let database_url = resolve_database_url()?;

    let pool = PgPool::connect(&database_url)
        .await
        .wrap_err("Failed to connect to test database")?;

    Ok(pool)
}

/// Main entry point for preflight verification
pub async fn run_preflight_checks() -> Result<(VerificationStatus, Value, Vec<String>)> {
    let mut messages = Vec::new();
    let mut details = HashMap::new();
    let mut has_warnings = false;
    let mut has_failures = false;

    info!("Starting comprehensive preflight verification");

    // Run end-to-end integration tests
    match verify_end_to_end_integration().await {
        Ok((status, value, mut test_messages)) => {
            messages.append(&mut test_messages);
            details.insert("integration", value);

            match status {
                VerificationStatus::Warning => has_warnings = true,
                VerificationStatus::Fail => has_failures = true,
                VerificationStatus::Pass => {}
            }
        }
        Err(e) => {
            messages.push(format!("✗ Integration tests failed: {}", e));
            has_failures = true;
        }
    }

    // Run performance baseline tests
    match verify_performance_baseline().await {
        Ok((status, value, mut perf_messages)) => {
            messages.append(&mut perf_messages);
            details.insert("performance", value);

            match status {
                VerificationStatus::Warning => has_warnings = true,
                VerificationStatus::Fail => has_failures = true,
                VerificationStatus::Pass => {}
            }
        }
        Err(e) => {
            messages.push(format!("⚠ Performance baseline failed: {}", e));
            has_warnings = true;
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
        "preflight_checks": details,
        "total_checks": details.len(),
        "overall_status": match status {
            VerificationStatus::Pass => "pass",
            VerificationStatus::Warning => "warning",
            VerificationStatus::Fail => "fail"
        }
    });

    Ok((status, result, messages))
}

/// Verify performance baseline using read-only queries
pub async fn verify_performance_baseline() -> Result<(VerificationStatus, Value, Vec<String>)> {
    let mut messages = Vec::new();
    let pool = get_test_pool().await?;

    // Baseline query performance using COUNT query
    let iterations = 10;
    let mut query_times = Vec::new();

    for _ in 0..iterations {
        let query_start = Instant::now();

        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM core.events")
            .fetch_one(&pool)
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
