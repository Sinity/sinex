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

use crate::{NodeResult, SinexError};
use serde_json::{Value, json};
use sqlx::PgPool;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tracing::info;

use super::{
    PREFLIGHT_DB_MAX_CONNECTIONS, VerificationStatus, connect_preflight_database_pool,
    resolve_database_url, resolve_nats_url, runtime_database_expected,
};

const SQLSTATE_UNDEFINED_FUNCTION: &str = "42883";
const EVENTS_ACCESS_PROBE_SQL: &str = "SELECT id, source, event_type FROM core.events LIMIT 0";
const PREFLIGHT_NATS_OPERATION_TIMEOUT: Duration = Duration::from_secs(5);

/// Verify end-to-end integration of the entire Sinex system
pub async fn verify_end_to_end_integration() -> NodeResult<(VerificationStatus, Value, Vec<String>)>
{
    let mut messages = Vec::new();
    let mut details = HashMap::new();
    let mut has_warnings = false;
    let mut has_failures = false;
    let database_expected = runtime_database_expected()?;

    info!("Starting end-to-end integration verification");

    if database_expected {
        // Database integration test
        match verify_database_integration(&mut messages).await {
            Ok(db_info) => {
                details.insert("database_integration", db_info);
            }
            Err(e) => {
                messages.push(format!("✗ Database integration test failed: {e}"));
                has_failures = true;
            }
        }
    } else {
        messages.push(
            "ℹ Database integration skipped: deployment is running in edge mode or does not expect a runtime PostgreSQL dependency"
                .to_string(),
        );
        details.insert(
            "database_integration",
            json!({
                "skipped": true,
                "reason": "runtime_database_not_expected",
            }),
        );
    }

    // Service integration test (NATS/checkpoint KV)
    if !has_failures {
        match verify_service_integration(&mut messages).await {
            Ok(service_info) => {
                details.insert("service_integration", service_info);
            }
            Err(e) => {
                messages.push(format!("✗ Service integration test failed: {e}"));
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
async fn verify_database_integration(messages: &mut Vec<String>) -> NodeResult<Value> {
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
            messages.push(format!("✗ Schema access test failed: {e}"));
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
                messages.push(format!("✗ Transaction test failed: {e}"));
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
                messages.push(format!("⚠ Concurrent queries test failed: {e}"));
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
            messages.push(format!("⚠ Extensions test failed: {e}"));
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
async fn test_schema_access(pool: &PgPool, _messages: &mut Vec<String>) -> NodeResult<Value> {
    let events_table_exists = relation_exists(pool, "core", "events").await?;

    if !events_table_exists {
        return Err(SinexError::processing(
            "core.events table does not exist; schema apply has not completed for the effective runtime database".to_string(),
        ));
    }

    // Check that we can SELECT from core.events without reading table data.
    let select_works = sqlx::query(EVENTS_ACCESS_PROBE_SQL)
        .execute(pool)
        .await
        .is_ok();
    if !select_works {
        return Err(SinexError::processing(
            "core.events exists but is not queryable from the effective runtime database"
                .to_string(),
        ));
    }

    let source_material_registry_exists =
        relation_exists(pool, "raw", "source_material_registry").await?;
    if !source_material_registry_exists {
        return Err(SinexError::processing(
            "raw.source_material_registry does not exist; source-material provenance storage is unavailable"
                .to_string(),
        ));
    }

    let source_material_registry_select_works =
        sqlx::query("SELECT 1 FROM raw.source_material_registry LIMIT 0")
            .execute(pool)
            .await
            .is_ok();
    if !source_material_registry_select_works {
        return Err(SinexError::processing(
            "raw.source_material_registry exists but is not queryable from the effective runtime database"
                .to_string(),
        ));
    }

    let blobs_exists = relation_exists(pool, "core", "blobs").await?;
    if !blobs_exists {
        return Err(SinexError::processing(
            "core.blobs table does not exist; blob metadata storage is unavailable".to_string(),
        ));
    }

    let blobs_select_works = sqlx::query("SELECT 1 FROM core.blobs LIMIT 0")
        .execute(pool)
        .await
        .is_ok();
    if !blobs_select_works {
        return Err(SinexError::processing(
            "core.blobs exists but is not queryable from the effective runtime database"
                .to_string(),
        ));
    }

    Ok(json!({
        "events_table_exists": events_table_exists,
        "select_works": select_works,
        "events_access_probe": "limit_0",
        "unbounded_event_count_skipped": true,
        "source_material_registry_exists": source_material_registry_exists,
        "source_material_registry_select_works": source_material_registry_select_works,
        "blobs_exists": blobs_exists,
        "blobs_select_works": blobs_select_works,
        "all_checks_passed": events_table_exists
            && select_works
            && source_material_registry_exists
            && source_material_registry_select_works
            && blobs_exists
            && blobs_select_works
    }))
}

async fn relation_exists(pool: &PgPool, schema: &str, table: &str) -> NodeResult<bool> {
    sqlx::query_scalar::<_, bool>(
        r"
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_schema = $1
            AND table_name = $2
        )
        ",
    )
    .bind(schema)
    .bind(table)
    .fetch_one(pool)
    .await
    .map_err(SinexError::from)
}

/// Test transaction support using SELECT queries only
async fn test_transactions(pool: &PgPool, _messages: &mut [String]) -> NodeResult<Value> {
    // Test committed transaction with SELECT
    let mut tx = pool.begin().await.map_err(SinexError::from)?;

    // Run a simple SELECT inside the transaction
    let select_in_tx = sqlx::query_scalar::<_, i32>("SELECT 1")
        .fetch_one(&mut *tx)
        .await
        .map_err(SinexError::from)?;

    tx.commit().await.map_err(SinexError::from)?;

    let commit_works = select_in_tx == 1;

    // Test rollback with SELECT
    let mut tx_rollback = pool.begin().await.map_err(SinexError::from)?;

    // Run a simple SELECT inside the transaction
    let _select_in_rollback = sqlx::query_scalar::<_, i32>("SELECT 1")
        .fetch_one(&mut *tx_rollback)
        .await
        .map_err(SinexError::from)?;

    tx_rollback.rollback().await.map_err(SinexError::from)?;

    // Verify no side effects by checking we can still query
    let after_rollback = sqlx::query_scalar::<_, i32>("SELECT 1")
        .fetch_one(pool)
        .await
        .map_err(SinexError::from)?;

    let rollback_works = after_rollback == 1;

    Ok(json!({
        "commit_test": commit_works,
        "rollback_test": rollback_works,
        "no_side_effects": true,
        "all_passed": commit_works && rollback_works
    }))
}

/// Test concurrent query operations
async fn test_concurrent_queries(pool: &PgPool, messages: &mut Vec<String>) -> NodeResult<Value> {
    use tokio::task::JoinSet;

    let concurrent_count = PREFLIGHT_DB_MAX_CONNECTIONS;
    let mut join_set = JoinSet::new();

    for i in 0..concurrent_count {
        let pool_clone = pool.clone();
        join_set.spawn(async move {
            // Run concurrent SELECT queries - no mutations
            sqlx::query_scalar::<_, i64>(&format!("SELECT {i}::bigint"))
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
            Err(e) => failures.push(format!("Join error: {e}")),
        }
    }

    if !failures.is_empty() {
        for failure in &failures {
            messages.push(format!("⚠ Concurrent query failed: {failure}"));
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
async fn test_database_extensions(pool: &PgPool, _messages: &mut [String]) -> NodeResult<Value> {
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

    // Test UUIDv7 generation used by canonical schema defaults.
    let uuid_test_result = sqlx::query("SELECT uuidv7() as uuid").fetch_one(pool).await;

    match uuid_test_result {
        Ok(_) => {
            tested_extensions.insert("uuidv7", json!({"status": "working"}));
        }
        Err(e) => {
            tested_extensions.insert(
                "uuidv7",
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
                    .code()
                    .as_deref()
                    .is_some_and(|code| code == SQLSTATE_UNDEFINED_FUNCTION)
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
        "has_required": tested_extensions.get("uuidv7").is_some_and(|v| v.get("status") == Some(&json!("working")))
    }))
}

/// Verify service integration (NATS checkpoint KV)
async fn verify_service_integration(_messages: &mut [String]) -> NodeResult<Value> {
    let env = sinex_primitives::environment::SinexEnvironment::current().map_err(|error| {
        SinexError::processing(format!(
            "Failed to resolve SINEX_ENVIRONMENT for service integration verification: {error}"
        ))
    })?;
    let nats_url = resolve_nats_url()?;
    let mut nats_config = sinex_primitives::nats::NatsConnectionConfig::from_env();
    nats_config.url = nats_url;
    let client = nats_config.connect().await.map_err(|e| {
        SinexError::processing(format!(
            "Failed to connect to NATS for checkpoint verification: {e}"
        ))
    })?;
    let js = async_nats::jetstream::new(client);
    let bucket = crate::checkpoint::checkpoint_bucket_name(None);
    js.get_key_value(&bucket).await.map_err(|e| {
        SinexError::processing(format!(
            "Failed to open checkpoint KV bucket '{bucket}': {e}"
        ))
    })?;

    let required_streams = [
        env.nats_stream_name("SINEX_RAW_EVENTS"),
        env.nats_stream_name("SINEX_RAW_EVENTS_CONFIRMATIONS"),
        env.nats_stream_name(crate::SOURCE_MATERIAL_STREAM),
    ];
    let mut checked_streams = Vec::with_capacity(required_streams.len());
    let mut missing_streams = Vec::new();

    for stream in &required_streams {
        match tokio::time::timeout(PREFLIGHT_NATS_OPERATION_TIMEOUT, js.get_stream(stream)).await {
            Ok(Ok(_stream)) => checked_streams.push(stream.clone()),
            Ok(Err(error)) => missing_streams.push(format!("{stream} ({error})")),
            Err(_) => missing_streams.push(format!(
                "{stream} (timed out after {:?})",
                PREFLIGHT_NATS_OPERATION_TIMEOUT
            )),
        }
    }

    if !missing_streams.is_empty() {
        return Err(SinexError::processing(format!(
            "Missing required JetStream streams: {}; checked: {}",
            missing_streams.join(", "),
            if checked_streams.is_empty() {
                "<none>".to_string()
            } else {
                checked_streams.join(", ")
            }
        )));
    }

    Ok(json!({
        "checkpoint_kv": true,
        "checkpoint_bucket": bucket,
        "required_streams": required_streams,
        "stream_probe": "direct_required_streams"
    }))
}

/// Get test database pool
async fn get_test_pool() -> NodeResult<PgPool> {
    let database_url = resolve_database_url()?;
    connect_preflight_database_pool(&database_url).await
}

/// Verify performance baseline using read-only queries
pub async fn verify_performance_baseline() -> NodeResult<(VerificationStatus, Value, Vec<String>)> {
    let mut messages = Vec::new();
    if !runtime_database_expected()? {
        messages.push(
            "ℹ Performance baseline skipped: deployment is running without a runtime PostgreSQL dependency"
                .to_string(),
        );
        return Ok((
            VerificationStatus::Pass,
            json!({
                "skipped": true,
                "reason": "runtime_database_not_expected",
            }),
            messages,
        ));
    }
    let pool = get_test_pool().await?;

    // Baseline query performance using metadata-only table access. Startup
    // preflight must not scan core.events or ask PostgreSQL for parallel workers.
    let iterations = 10;
    let mut query_times = Vec::new();

    for _ in 0..iterations {
        let query_start = Instant::now();

        sqlx::query(EVENTS_ACCESS_PROBE_SQL)
            .execute(&pool)
            .await
            .map_err(SinexError::from)?;

        let query_duration = query_start.elapsed();
        query_times.push(query_duration.as_millis());
    }

    let avg_query_time = query_times.iter().sum::<u128>() / iterations as u128;
    let min_query_time = query_times.iter().min().unwrap_or(&0);
    let max_query_time = query_times.iter().max().unwrap_or(&0);

    let status = if avg_query_time > 100 {
        messages.push(format!(
            "⚠ Average query time {avg_query_time}ms exceeds 100ms baseline"
        ));
        VerificationStatus::Warning
    } else {
        messages.push(format!(
            "✓ Average query time {avg_query_time}ms within baseline"
        ));
        VerificationStatus::Pass
    };

    let result = json!({
        "query_performance": {
            "probe": "events_limit_0",
            "iterations": iterations,
            "average_ms": avg_query_time,
            "min_ms": min_query_time,
            "max_ms": max_query_time,
            "unbounded_scans": false,
            "parallel_workers_disabled": true,
            "baseline_met": avg_query_time <= 100
        }
    });

    Ok((status, result, messages))
}

#[cfg(test)]
mod tests {
    use super::{EVENTS_ACCESS_PROBE_SQL, test_schema_access};
    use crate::preflight::{
        PREFLIGHT_MAX_PARALLEL_WORKERS_PER_GATHER, configure_preflight_database_session,
    };
    use serde_json::Value;
    use xtask::sandbox::prelude::*;

    #[test]
    fn event_access_probe_is_metadata_only() {
        assert!(EVENTS_ACCESS_PROBE_SQL.contains("LIMIT 0"));
        assert!(!EVENTS_ACCESS_PROBE_SQL.contains("COUNT(*)"));
    }

    #[sinex_test]
    async fn test_schema_access_uses_source_material_registry_contract(
        ctx: TestContext,
    ) -> TestResult<()> {
        let mut messages = Vec::new();
        let details = test_schema_access(ctx.pool(), &mut messages).await?;

        assert_eq!(
            details.get("source_material_registry_exists"),
            Some(&Value::Bool(true))
        );
        assert_eq!(
            details.get("source_material_registry_select_works"),
            Some(&Value::Bool(true))
        );
        assert_eq!(
            details.get("unbounded_event_count_skipped"),
            Some(&Value::Bool(true))
        );
        assert_eq!(details.get("current_event_count"), None);
        assert_eq!(details.get("source_materials_exists"), None);
        assert_eq!(details.get("blobs_exists"), Some(&Value::Bool(true)));
        assert_eq!(details.get("blobs_select_works"), Some(&Value::Bool(true)));
        assert_eq!(details.get("all_checks_passed"), Some(&Value::Bool(true)));

        Ok(())
    }

    #[sinex_test]
    async fn preflight_database_session_disables_parallel_workers(
        ctx: TestContext,
    ) -> TestResult<()> {
        let mut conn = ctx.pool().acquire().await?;
        configure_preflight_database_session(&mut *conn).await?;

        let parallel_workers =
            sqlx::query_scalar::<_, String>("SHOW max_parallel_workers_per_gather")
                .fetch_one(&mut *conn)
                .await?;
        let statement_timeout = sqlx::query_scalar::<_, String>("SHOW statement_timeout")
            .fetch_one(&mut *conn)
            .await?;
        let lock_timeout = sqlx::query_scalar::<_, String>("SHOW lock_timeout")
            .fetch_one(&mut *conn)
            .await?;

        assert_eq!(parallel_workers, PREFLIGHT_MAX_PARALLEL_WORKERS_PER_GATHER);
        assert_eq!(statement_timeout, "5s");
        assert_eq!(lock_timeout, "1s");

        Ok(())
    }
}
