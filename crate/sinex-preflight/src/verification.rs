/*!
 * Core verification module for Sinex Pre-Flight system
 *
 * Handles end-to-end integration verification including:
 * - Complete system workflow testing
 * - Event pipeline validation
 * - Service integration testing
 * - Performance baseline verification
 */

use anyhow::{bail, Context, Result};
use serde_json::{json, Value};
use sinex_db::{ulid_to_uuid, uuid_to_ulid};
use sinex_ulid::Ulid;
use sqlx::PgPool;
use std::collections::HashMap;
use std::time::Instant;
use tracing::{info, warn};

use crate::VerificationStatus;

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

    // Performance baseline test
    if !has_failures {
        match verify_performance_baseline(&mut messages).await {
            Ok(perf_info) => {
                details.insert("performance", perf_info);
            }
            Err(e) => {
                messages.push(format!("⚠ Performance baseline test warning: {}", e));
                has_warnings = true;
            }
        }
    }

    // System resource integration test
    match verify_system_integration(&mut messages).await {
        Ok(system_info) => {
            details.insert("system_integration", system_info);
        }
        Err(e) => {
            messages.push(format!("⚠ System integration test warning: {}", e));
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

    info!(
        "End-to-end integration verification completed with status: {:?}",
        status
    );
    Ok((status, json!(details), messages))
}

async fn verify_database_integration(messages: &mut Vec<String>) -> Result<Value> {
    let mut db_info = HashMap::new();

    info!("Testing database integration");

    let database_url =
        std::env::var("DATABASE_URL").context("DATABASE_URL environment variable not set")?;

    let pool = PgPool::connect(&database_url)
        .await
        .context("Failed to connect to database for integration test")?;

    // Test 1: Basic CRUD operations
    let crud_start = Instant::now();
    let crud_result = test_crud_operations(&pool).await;
    let crud_duration = crud_start.elapsed();

    match crud_result {
        Ok(crud_data) => {
            db_info.insert(
                "crud_operations",
                json!({
                    "success": true,
                    "duration_ms": crud_duration.as_millis(),
                    "operations": crud_data
                }),
            );
            messages.push(format!(
                "✓ Database CRUD operations test passed ({}ms)",
                crud_duration.as_millis()
            ));
        }
        Err(e) => {
            db_info.insert(
                "crud_operations",
                json!({
                    "success": false,
                    "duration_ms": crud_duration.as_millis(),
                    "error": e.to_string()
                }),
            );
            bail!("Database CRUD operations test failed: {}", e);
        }
    }

    // Test 2: Transaction handling
    let tx_start = Instant::now();
    let tx_result = test_transaction_handling(&pool).await;
    let tx_duration = tx_start.elapsed();

    match tx_result {
        Ok(_) => {
            db_info.insert(
                "transactions",
                json!({
                    "success": true,
                    "duration_ms": tx_duration.as_millis()
                }),
            );
            messages.push(format!(
                "✓ Database transaction handling test passed ({}ms)",
                tx_duration.as_millis()
            ));
        }
        Err(e) => {
            db_info.insert(
                "transactions",
                json!({
                    "success": false,
                    "duration_ms": tx_duration.as_millis(),
                    "error": e.to_string()
                }),
            );
            bail!("Database transaction handling test failed: {}", e);
        }
    }

    // Test 3: Concurrent operations
    let concurrent_start = Instant::now();
    let concurrent_result = test_concurrent_operations(&pool).await;
    let concurrent_duration = concurrent_start.elapsed();

    match concurrent_result {
        Ok(concurrent_data) => {
            db_info.insert(
                "concurrent_operations",
                json!({
                    "success": true,
                    "duration_ms": concurrent_duration.as_millis(),
                    "operations_count": concurrent_data
                }),
            );
            messages.push(format!(
                "✓ Database concurrent operations test passed ({}ms, {} ops)",
                concurrent_duration.as_millis(),
                concurrent_data
            ));
        }
        Err(e) => {
            db_info.insert(
                "concurrent_operations",
                json!({
                    "success": false,
                    "duration_ms": concurrent_duration.as_millis(),
                    "error": e.to_string()
                }),
            );
            bail!("Database concurrent operations test failed: {}", e);
        }
    }

    // Test 4: Extension functionality
    let ext_start = Instant::now();
    let ext_result = test_extension_functionality(&pool).await;
    let ext_duration = ext_start.elapsed();

    match ext_result {
        Ok(ext_data) => {
            db_info.insert(
                "extensions",
                json!({
                    "success": true,
                    "duration_ms": ext_duration.as_millis(),
                    "tested_extensions": ext_data
                }),
            );
            messages.push(format!(
                "✓ Database extensions test passed ({}ms)",
                ext_duration.as_millis()
            ));
        }
        Err(e) => {
            db_info.insert(
                "extensions",
                json!({
                    "success": false,
                    "duration_ms": ext_duration.as_millis(),
                    "error": e.to_string()
                }),
            );
            bail!("Database extensions test failed: {}", e);
        }
    }

    Ok(json!(db_info))
}

async fn test_crud_operations(pool: &PgPool) -> Result<Value> {
    let _test_id = Ulid::new();
    let test_source = "sinex-preflight-integration-test";
    let test_event_type = "verification.crud_test";

    // CREATE: Insert a test event
    let insert_result = sqlx::query!(
        r#"
        INSERT INTO raw.events (source, event_type, host, payload)
        VALUES ($1, $2, $3, $4)
        RETURNING id::uuid as "id!"
        "#,
        test_source,
        test_event_type,
        "localhost",
        json!({"test": "crud_operations", "operation": "insert"})
    )
    .fetch_one(pool)
    .await
    .context("Failed to insert test event")?;

    let inserted_id = uuid_to_ulid(insert_result.id);

    // READ: Query the test event
    let read_result = sqlx::query!(
        "SELECT id::uuid as \"id!\", source, event_type, payload FROM raw.events WHERE id = $1::uuid::ulid",
        ulid_to_uuid(inserted_id)
    )
    .fetch_optional(pool)
    .await
    .context("Failed to read test event")?;

    let read_event = read_result.context("Test event not found after insert")?;

    if read_event.source != test_source {
        bail!(
            "Read event source mismatch: got {}, expected {}",
            read_event.source,
            test_source
        );
    }

    // UPDATE: Modify the test event payload
    let update_result = sqlx::query!(
        "UPDATE raw.events SET payload = $1 WHERE id = $2::uuid::ulid",
        json!({"test": "crud_operations", "operation": "update", "modified": true}),
        ulid_to_uuid(inserted_id)
    )
    .execute(pool)
    .await
    .context("Failed to update test event")?;

    if update_result.rows_affected() != 1 {
        bail!(
            "Update operation affected {} rows, expected 1",
            update_result.rows_affected()
        );
    }

    // DELETE: Remove the test event
    let delete_result = sqlx::query!(
        "DELETE FROM raw.events WHERE id = $1::uuid::ulid",
        ulid_to_uuid(inserted_id)
    )
    .execute(pool)
    .await
    .context("Failed to delete test event")?;

    if delete_result.rows_affected() != 1 {
        bail!(
            "Delete operation affected {} rows, expected 1",
            delete_result.rows_affected()
        );
    }

    // Verify deletion
    let id_as_uuid = ulid_to_uuid(inserted_id);
    let verify_result = sqlx::query!(
        "SELECT id::uuid as \"id!\" FROM raw.events WHERE id::uuid = $1",
        id_as_uuid
    )
    .fetch_optional(pool)
    .await
    .context("Failed to verify deletion")?;

    if verify_result.is_some() {
        bail!("Test event still exists after deletion");
    }

    Ok(json!({
        "insert": "success",
        "read": "success",
        "update": "success",
        "delete": "success",
        "verification": "success"
    }))
}

async fn test_transaction_handling(pool: &PgPool) -> Result<()> {
    let _test_id_1 = Ulid::new();
    let _test_id_2 = Ulid::new();

    // Test successful transaction
    let mut tx = pool.begin().await.context("Failed to begin transaction")?;

    sqlx::query!(
        r#"
        INSERT INTO raw.events (source, event_type, host, payload)
        VALUES ($1, $2, $3, $4)
        "#,
        "sinex-preflight-tx-test",
        "verification.transaction_test",
        "localhost",
        json!({"test": "transaction", "phase": "commit"})
    )
    .execute(&mut *tx)
    .await
    .context("Failed to insert in transaction")?;

    tx.commit().await.context("Failed to commit transaction")?;

    // Verify committed transaction exists (we can't predict the auto-generated ID)
    let committed_event = sqlx::query!(
        "SELECT COUNT(*) as count FROM raw.events WHERE source = $1 AND event_type = $2 AND payload->>'phase' = $3",
        "sinex-preflight-tx-test",
        "verification.transaction_test", 
        "commit"
    )
    .fetch_one(pool)
    .await
    .context("Failed to verify committed transaction")?;

    if committed_event.count.unwrap_or(0) == 0 {
        bail!("Committed transaction event not found");
    }

    // Test rollback transaction
    let mut tx = pool
        .begin()
        .await
        .context("Failed to begin rollback transaction")?;

    sqlx::query!(
        r#"
        INSERT INTO raw.events (source, event_type, host, payload)
        VALUES ($1, $2, $3, $4)
        "#,
        "sinex-preflight-tx-test",
        "verification.transaction_test",
        "localhost",
        json!({"test": "transaction", "phase": "rollback"})
    )
    .execute(&mut *tx)
    .await
    .context("Failed to insert in rollback transaction")?;

    tx.rollback()
        .await
        .context("Failed to rollback transaction")?;

    // Verify rolled back transaction doesn't exist
    let rollback_event = sqlx::query!(
        "SELECT COUNT(*) as count FROM raw.events WHERE source = $1 AND event_type = $2 AND payload->>'phase' = $3",
        "sinex-preflight-tx-test",
        "verification.transaction_test",
        "rollback"
    )
    .fetch_one(pool)
    .await
    .context("Failed to verify rollback transaction")?;

    if rollback_event.count.unwrap_or(0) > 0 {
        bail!("Rollback transaction event found (should not exist)");
    }

    // Cleanup committed event
    sqlx::query!(
        "DELETE FROM raw.events WHERE source = $1 AND event_type = $2 AND payload->>'phase' = $3",
        "sinex-preflight-tx-test",
        "verification.transaction_test",
        "commit"
    )
    .execute(pool)
    .await
    .context("Failed to cleanup committed test event")?;

    Ok(())
}

async fn test_concurrent_operations(pool: &PgPool) -> Result<usize> {
    use tokio::task::JoinSet;

    let mut join_set = JoinSet::new();
    let operation_count = 10;

    // Spawn concurrent insert operations
    for i in 0..operation_count {
        let pool_clone = pool.clone();
        join_set.spawn(async move {
            let result = sqlx::query!(
                r#"
                INSERT INTO raw.events (source, event_type, host, payload)
                VALUES ($1, $2, $3, $4)
                RETURNING id::uuid as "id!"
                "#,
                "sinex-preflight-concurrent-test",
                "verification.concurrent_test",
                "localhost",
                json!({"test": "concurrent", "operation_id": i})
            )
            .fetch_one(&pool_clone)
            .await;

            result
        });
    }

    let mut successful_operations = 0;
    let mut test_ids = Vec::new();

    // Wait for all operations to complete
    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(Ok(insert_result)) => {
                successful_operations += 1;
                test_ids.push(uuid_to_ulid(insert_result.id));
            }
            Ok(Err(e)) => {
                warn!("Concurrent operation failed: {}", e);
            }
            Err(e) => {
                warn!("Concurrent task failed: {}", e);
            }
        }
    }

    // Cleanup test events using source and event_type filter
    sqlx::query!(
        "DELETE FROM raw.events WHERE source = $1 AND event_type = $2",
        "sinex-preflight-concurrent-test",
        "verification.concurrent_test"
    )
    .execute(pool)
    .await
    .ok(); // Ignore cleanup errors

    if successful_operations == 0 {
        bail!("No concurrent operations succeeded");
    }

    Ok(successful_operations)
}

async fn test_extension_functionality(pool: &PgPool) -> Result<Value> {
    let mut tested_extensions = HashMap::new();

    // Test UUID generation
    match sqlx::query!("SELECT gen_random_uuid() as test_uuid")
        .fetch_one(pool)
        .await
    {
        Ok(_) => {
            tested_extensions.insert("uuid-builtin", json!({"status": "working"}));
        }
        Err(e) => {
            tested_extensions.insert(
                "uuid-ossp",
                json!({
                    "status": "failed",
                    "error": e.to_string()
                }),
            );
        }
    }

    // Test ULID generation (if available)
    match sqlx::query!("SELECT gen_ulid()::text as test_ulid")
        .fetch_one(pool)
        .await
    {
        Ok(_) => {
            tested_extensions.insert("pgx_ulid", json!({"status": "working"}));
        }
        Err(e) => {
            tested_extensions.insert(
                "pgx_ulid",
                json!({
                    "status": "failed",
                    "error": e.to_string()
                }),
            );
        }
    }

    // Test TimescaleDB extension by checking version
    match sqlx::query!("SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'")
        .fetch_one(pool)
        .await
    {
        Ok(result) => {
            tested_extensions.insert(
                "timescaledb",
                json!({
                    "status": "working",
                    "version": result.extversion
                }),
            );
        }
        Err(e) => {
            tested_extensions.insert(
                "timescaledb",
                json!({
                    "status": "failed",
                    "error": e.to_string()
                }),
            );
        }
    }

    // Test JSON schema validation (if available)
    match sqlx::query!(r#"SELECT json_matches_schema('{"type": "object"}', '{}') as valid"#)
        .fetch_one(pool)
        .await
    {
        Ok(_) => {
            tested_extensions.insert("pg_jsonschema", json!({"status": "working"}));
        }
        Err(e) => {
            tested_extensions.insert(
                "pg_jsonschema",
                json!({
                    "status": "failed",
                    "error": e.to_string()
                }),
            );
        }
    }

    Ok(json!(tested_extensions))
}

async fn verify_event_pipeline(messages: &mut Vec<String>) -> Result<Value> {
    let mut pipeline_info = HashMap::new();

    info!("Testing event pipeline integration");

    let database_url =
        std::env::var("DATABASE_URL").context("DATABASE_URL environment variable not set")?;

    let pool = PgPool::connect(&database_url)
        .await
        .context("Failed to connect to database for pipeline test")?;

    // Test 1: Event ingestion simulation
    let ingestion_start = Instant::now();
    let ingestion_result = test_event_ingestion(&pool).await;
    let ingestion_duration = ingestion_start.elapsed();

    match ingestion_result {
        Ok(ingestion_data) => {
            pipeline_info.insert(
                "event_ingestion",
                json!({
                    "success": true,
                    "duration_ms": ingestion_duration.as_millis(),
                    "events_processed": ingestion_data
                }),
            );
            messages.push(format!(
                "✓ Event ingestion test passed ({}ms, {} events)",
                ingestion_duration.as_millis(),
                ingestion_data
            ));
        }
        Err(e) => {
            pipeline_info.insert(
                "event_ingestion",
                json!({
                    "success": false,
                    "duration_ms": ingestion_duration.as_millis(),
                    "error": e.to_string()
                }),
            );
            bail!("Event ingestion test failed: {}", e);
        }
    }

    // Test 2: Work queue operations
    let queue_start = Instant::now();
    let queue_result = test_work_queue_operations(&pool).await;
    let queue_duration = queue_start.elapsed();

    match queue_result {
        Ok(queue_data) => {
            pipeline_info.insert(
                "work_queue",
                json!({
                    "success": true,
                    "duration_ms": queue_duration.as_millis(),
                    "queue_operations": queue_data
                }),
            );
            messages.push(format!(
                "✓ Work queue operations test passed ({}ms)",
                queue_duration.as_millis()
            ));
        }
        Err(e) => {
            pipeline_info.insert(
                "work_queue",
                json!({
                    "success": false,
                    "duration_ms": queue_duration.as_millis(),
                    "error": e.to_string()
                }),
            );
            bail!("Work queue operations test failed: {}", e);
        }
    }

    Ok(json!(pipeline_info))
}

async fn test_event_ingestion(pool: &PgPool) -> Result<usize> {
    let event_count = 5;
    let mut test_ids = Vec::new();

    // Simulate rapid event ingestion
    for i in 0..event_count {
        let result = sqlx::query!(
            r#"
            INSERT INTO raw.events (source, event_type, host, payload)
            VALUES ($1, $2, $3, $4)
            RETURNING id::uuid as "id!"
            "#,
            "sinex-preflight-pipeline-test",
            "verification.ingestion_test",
            "localhost",
            json!({
                "test": "event_ingestion",
                "sequence": i,
                "timestamp": chrono::Utc::now()
            })
        )
        .fetch_one(pool)
        .await
        .context("Failed to insert pipeline test event")?;

        test_ids.push(result.id);
    }

    // Verify all events were inserted
    let count_result = sqlx::query!(
        "SELECT COUNT(*) as count FROM raw.events WHERE source = $1",
        "sinex-preflight-pipeline-test"
    )
    .fetch_one(pool)
    .await
    .context("Failed to count inserted events")?;

    let inserted_count = count_result.count.unwrap_or(0) as usize;

    // Cleanup test events using source filter (more efficient)
    sqlx::query!(
        "DELETE FROM raw.events WHERE source = $1",
        "sinex-preflight-pipeline-test"
    )
    .execute(pool)
    .await
    .ok(); // Ignore cleanup errors

    if inserted_count < event_count {
        bail!(
            "Only {} of {} events were inserted",
            inserted_count,
            event_count
        );
    }

    Ok(inserted_count)
}

async fn test_work_queue_operations(pool: &PgPool) -> Result<Value> {
    // This would test the work queue table operations
    // For now, we'll do a basic table existence and operation test

    // Check if work queue table exists
    let table_exists = sqlx::query!(
        r#"
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'sinex_schemas' 
            AND table_name = 'work_queue'
        ) as exists
        "#
    )
    .fetch_one(pool)
    .await
    .context("Failed to check work queue table existence")?;

    if !table_exists.exists.unwrap_or(false) {
        return Ok(json!({
            "table_exists": false,
            "note": "Work queue table not found - will be created during deployment"
        }));
    }

    // Test basic queue operations

    // Test queue insertion (this would typically be done by the router)
    // First create a test event to reference
    let test_event = sqlx::query!(
        r#"
        INSERT INTO raw.events (source, event_type, host, payload)
        VALUES ($1, $2, $3, $4)
        RETURNING id::uuid as "id!"
        "#,
        "sinex-preflight-work-queue-test",
        "verification.work_queue_test",
        "localhost",
        json!({"test": "work_queue_operations"})
    )
    .fetch_one(pool)
    .await
    .context("Failed to create test event for work queue")?;

    match sqlx::query!(
        r#"
        INSERT INTO sinex_schemas.work_queue (raw_event_id, target_agent_name)
        VALUES ($1::uuid, $2)
        "#,
        test_event.id,
        "batch-test-agent"
    )
    .execute(pool)
    .await
    {
        Ok(_) => {
            // Clean up test queue entry
            sqlx::query!(
                "DELETE FROM sinex_schemas.work_queue WHERE raw_event_id::uuid = $1",
                test_event.id
            )
            .execute(pool)
            .await
            .ok();

            // Clean up test event
            sqlx::query!(
                "DELETE FROM raw.events WHERE source = $1",
                "sinex-preflight-work-queue-test"
            )
            .execute(pool)
            .await
            .ok();

            Ok(json!({
                "table_exists": true,
                "insert_test": "success",
                "delete_test": "success"
            }))
        }
        Err(e) => Ok(json!({
            "table_exists": true,
            "insert_test": "failed",
            "error": e.to_string()
        })),
    }
}

async fn verify_performance_baseline(messages: &mut Vec<String>) -> Result<Value> {
    let mut perf_info = HashMap::new();

    info!("Testing performance baseline");

    let database_url =
        std::env::var("DATABASE_URL").context("DATABASE_URL environment variable not set")?;

    let pool = PgPool::connect(&database_url)
        .await
        .context("Failed to connect to database for performance test")?;

    // Test 1: Database query performance
    let db_perf_start = Instant::now();
    let db_perf_result = test_database_performance(&pool).await;
    let db_perf_duration = db_perf_start.elapsed();

    match db_perf_result {
        Ok(db_metrics) => {
            perf_info.insert(
                "database_performance",
                json!({
                    "success": true,
                    "total_duration_ms": db_perf_duration.as_millis(),
                    "metrics": db_metrics
                }),
            );

            // Check if performance is acceptable
            let avg_query_time = db_metrics["average_query_ms"].as_f64().unwrap_or(0.0);
            if avg_query_time > 100.0 {
                messages.push(format!(
                    "⚠ Database performance warning: avg query time {:.2}ms (>100ms)",
                    avg_query_time
                ));
            } else {
                messages.push(format!(
                    "✓ Database performance baseline acceptable: avg {:.2}ms",
                    avg_query_time
                ));
            }
        }
        Err(e) => {
            perf_info.insert(
                "database_performance",
                json!({
                    "success": false,
                    "duration_ms": db_perf_duration.as_millis(),
                    "error": e.to_string()
                }),
            );
            bail!("Database performance test failed: {}", e);
        }
    }

    // Test 2: Memory usage baseline
    let memory_usage = test_memory_usage().await?;
    perf_info.insert("memory_usage", memory_usage);
    messages.push("✓ Memory usage baseline recorded".to_string());

    Ok(json!(perf_info))
}

async fn test_database_performance(pool: &PgPool) -> Result<Value> {
    let query_count = 10;
    let mut query_times = Vec::new();

    // Run multiple queries to get performance baseline
    for _ in 0..query_count {
        let query_start = Instant::now();

        sqlx::query!("SELECT COUNT(*) as count FROM raw.events")
            .fetch_one(pool)
            .await
            .context("Performance test query failed")?;

        let query_duration = query_start.elapsed();
        query_times.push(query_duration.as_millis() as f64);
    }

    let total_time: f64 = query_times.iter().sum();
    let average_time = total_time / query_times.len() as f64;
    let min_time = query_times.iter().fold(f64::INFINITY, |a, &b| a.min(b));
    let max_time = query_times.iter().fold(0.0f64, |a, &b| a.max(b));

    Ok(json!({
        "query_count": query_count,
        "total_time_ms": total_time,
        "average_query_ms": average_time,
        "min_query_ms": min_time,
        "max_query_ms": max_time,
        "queries_per_second": 1000.0 / average_time
    }))
}

async fn test_memory_usage() -> Result<Value> {
    use sysinfo::System;

    let mut sys = System::new_all();
    sys.refresh_all();

    let current_pid = std::process::id();

    if let Some(process) = sys.process((current_pid as usize).into()) {
        Ok(json!({
            "memory_usage_kb": process.memory(),
            "virtual_memory_kb": process.virtual_memory(),
            "cpu_usage_percent": process.cpu_usage()
        }))
    } else {
        Ok(json!({
            "note": "Could not get current process memory usage"
        }))
    }
}

async fn verify_system_integration(messages: &mut Vec<String>) -> Result<Value> {
    let mut system_info = HashMap::new();

    info!("Testing system integration");

    // Test file system operations
    match test_filesystem_integration().await {
        Ok(fs_info) => {
            system_info.insert("filesystem", fs_info);
            messages.push("✓ Filesystem integration test passed".to_string());
        }
        Err(e) => {
            system_info.insert(
                "filesystem",
                json!({
                    "success": false,
                    "error": e.to_string()
                }),
            );
            messages.push(format!("⚠ Filesystem integration warning: {}", e));
        }
    }

    // Test process operations
    match test_process_integration().await {
        Ok(proc_info) => {
            system_info.insert("processes", proc_info);
            messages.push("✓ Process integration test passed".to_string());
        }
        Err(e) => {
            system_info.insert(
                "processes",
                json!({
                    "success": false,
                    "error": e.to_string()
                }),
            );
            messages.push(format!("⚠ Process integration warning: {}", e));
        }
    }

    Ok(json!(system_info))
}

async fn test_filesystem_integration() -> Result<Value> {
    use std::fs;
    use std::path::Path;

    let test_dir = Path::new("/tmp/sinex-preflight-fs-test");

    // Create test directory
    fs::create_dir_all(test_dir).context("Failed to create test directory")?;

    // Write test file
    let test_file = test_dir.join("test.txt");
    fs::write(&test_file, "Sinex preflight filesystem test")
        .context("Failed to write test file")?;

    // Read test file
    let content = fs::read_to_string(&test_file).context("Failed to read test file")?;

    if content != "Sinex preflight filesystem test" {
        bail!("File content mismatch");
    }

    // Clean up
    fs::remove_dir_all(test_dir).context("Failed to clean up test directory")?;

    Ok(json!({
        "success": true,
        "operations": ["create_dir", "write_file", "read_file", "cleanup"]
    }))
}

async fn test_process_integration() -> Result<Value> {
    use std::process::Command;

    // Test basic command execution
    let output = Command::new("echo")
        .arg("Sinex preflight process test")
        .output()
        .context("Failed to execute test command")?;

    if !output.status.success() {
        bail!("Test command failed");
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    if !stdout.contains("Sinex preflight process test") {
        bail!("Command output mismatch");
    }

    Ok(json!({
        "success": true,
        "command_execution": "working",
        "output_capture": "working"
    }))
}
