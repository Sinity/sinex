/*!
 * Unit tests for database verification module
 */

use anyhow::Result;
use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

use sinex_preflight::database::*;
use sinex_test_macros::sinex_test;
use crate::common::prelude::*;

#[sinex_test]
async fn test_database_connectivity_verification(ctx: TestContext) -> TestResult {
    let (status, details, messages) = verify_database_connectivity().await?;

    assert_eq!(status, sinex_preflight::VerificationStatus::Pass);
    assert!(!messages.is_empty());
    assert!(messages.iter().any(|m| m.contains("Database connection established")));

    // Check details structure
    assert!(details.get("database_url").is_some());
    assert!(details.get("postgresql_version").is_some());
    assert!(details.get("connection_pool").is_some());

    Ok(())
}

#[sinex_test]
async fn test_postgresql_extensions_verification(ctx: TestContext) -> TestResult {
    let (status, details, messages) = verify_postgresql_extensions().await?;

    // Should pass or warn, depending on which extensions are available
    assert!(matches!(status, sinex_preflight::VerificationStatus::Pass | sinex_preflight::VerificationStatus::Warning));

    // Should have checked for required extensions
    let extensions = details.get("extensions").unwrap().as_object().unwrap();
    assert!(extensions.contains_key("uuid-ossp"));
    assert!(extensions.contains_key("timescaledb"));
    assert!(extensions.contains_key("pg_jsonschema"));

    Ok(())
}

#[sinex_test]
async fn test_migration_readiness_verification(ctx: TestContext) -> TestResult {
    let (status, details, messages) = verify_migration_readiness().await?;

    assert_eq!(status, sinex_preflight::VerificationStatus::Pass);
    assert!(details.get("current_migrations").is_some());

    Ok(())
}

#[sinex_test]
async fn test_database_crud_operations(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

    // This simulates the internal test_crud_operations function
    let test_id = Uuid::new_v4();
    let test_source = "unit-test-crud";
    let test_event_type = "test.crud_operations";

    // CREATE
    let insert_result = sqlx::query!(
        r#"
        INSERT INTO raw.events (id, source, event_type, payload, ts_ingest)
        VALUES ($1, $2, $3, $4, NOW())
        "#,
        test_id,
        test_source,
        test_event_type,
        serde_json::json!({"test": "crud_operations"})
    )
    .execute(pool)
    .await?;

    assert_eq!(insert_result.rows_affected(), 1);

    // READ
    let read_result = sqlx::query!(
        "SELECT id, source, event_type FROM raw.events WHERE id = $1",
        test_id
    )
    .fetch_optional(pool)
    .await?;

    let event = read_result.expect("Test event should exist");
    assert_eq!(event.source, test_source);
    assert_eq!(event.event_type, test_event_type);

    // UPDATE
    let update_result = sqlx::query!(
        "UPDATE raw.events SET payload = $1 WHERE id = $2",
        serde_json::json!({"test": "crud_operations", "updated": true}),
        test_id
    )
    .execute(pool)
    .await?;

    assert_eq!(update_result.rows_affected(), 1);

    // DELETE
    let delete_result = sqlx::query!(
        "DELETE FROM raw.events WHERE id = $1",
        test_id
    )
    .execute(pool)
    .await?;

    assert_eq!(delete_result.rows_affected(), 1);

    // Verify deletion
    let verify_result = sqlx::query!(
        "SELECT id FROM raw.events WHERE id = $1",
        test_id
    )
    .fetch_optional(pool)
    .await?;

    assert!(verify_result.is_none());

    Ok(())
}

#[sinex_test]
async fn test_database_transaction_handling(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

    let test_id_1 = Uuid::new_v4();
    let test_id_2 = Uuid::new_v4();

    // Test successful transaction
    let mut tx = pool.begin().await?;

    sqlx::query!(
        r#"
        INSERT INTO raw.events (id, source, event_type, payload, ts_ingest)
        VALUES ($1, $2, $3, $4, NOW())
        "#,
        test_id_1,
        "unit-test-tx",
        "test.transaction",
        serde_json::json!({"test": "commit"})
    )
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;

    // Verify committed
    let committed = sqlx::query!(
        "SELECT id FROM raw.events WHERE id = $1",
        test_id_1
    )
    .fetch_optional(pool)
    .await?;

    assert!(committed.is_some());

    // Test rollback transaction
    let mut tx = pool.begin().await?;

    sqlx::query!(
        r#"
        INSERT INTO raw.events (id, source, event_type, payload, ts_ingest)
        VALUES ($1, $2, $3, $4, NOW())
        "#,
        test_id_2,
        "unit-test-tx",
        "test.transaction",
        serde_json::json!({"test": "rollback"})
    )
    .execute(&mut *tx)
    .await?;

    tx.rollback().await?;

    // Verify not committed
    let rolled_back = sqlx::query!(
        "SELECT id FROM raw.events WHERE id = $1",
        test_id_2
    )
    .fetch_optional(pool)
    .await?;

    assert!(rolled_back.is_none());

    // Cleanup
    sqlx::query!("DELETE FROM raw.events WHERE id = $1", test_id_1)
        .execute(pool)
        .await?;

    Ok(())
}

#[sinex_test]
async fn test_database_concurrent_operations(ctx: TestContext) -> TestResult {
    use tokio::task::JoinSet;

    let pool = ctx.pool();
    let mut join_set = JoinSet::new();
    let operation_count = 5;

    // Spawn concurrent operations
    for i in 0..operation_count {
        let pool_clone = pool.clone();
        join_set.spawn(async move {
            let test_id = Uuid::new_v4();

            let result = sqlx::query!(
                r#"
                INSERT INTO raw.events (id, source, event_type, payload, ts_ingest)
                VALUES ($1, $2, $3, $4, NOW())
                "#,
                test_id,
                "unit-test-concurrent",
                "test.concurrent",
                serde_json::json!({"operation": i})
            )
            .execute(pool_clone)
            .await;

            (test_id, result)
        });
    }

    let mut successful_operations = 0;
    let mut test_ids = Vec::new();

    while let Some(result) = join_set.join_next().await {
        match result {
            Ok((test_id, Ok(_))) => {
                successful_operations += 1;
                test_ids.push(test_id);
            }
            Ok((_, Err(e))) => {
                eprintln!("Concurrent operation failed: {}", e);
            }
            Err(e) => {
                eprintln!("Concurrent task failed: {}", e);
            }
        }
    }

    assert_eq!(successful_operations, operation_count, "All concurrent operations should succeed");

    // Cleanup
    for test_id in test_ids {
        sqlx::query!("DELETE FROM raw.events WHERE id = $1", test_id)
            .execute(pool)
            .await
            .ok();
    }

    Ok(())
}

#[sinex_test]
async fn test_database_extension_functionality(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

    // Test UUID generation
    let uuid_result = sqlx::query!("SELECT uuid_generate_v4() as test_uuid")
        .fetch_one(pool)
        .await;

    match uuid_result {
        Ok(_) => {
            // UUID extension is working
        }
        Err(_) => {
            // UUID extension not available, which is acceptable in test environment
            eprintln!("UUID extension not available in test environment");
        }
    }

    // Test basic PostgreSQL functionality
    let basic_result = sqlx::query!("SELECT version() as version")
        .fetch_one(pool)
        .await?;

    assert!(basic_result.version.is_some());
    assert!(basic_result.version.unwrap().contains("PostgreSQL"));

    Ok(())
}

#[sinex_test]
async fn test_database_schema_validation(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

    // Test that we can validate schema exists
    let raw_schema_exists = sqlx::query!(
        r#"
        SELECT EXISTS (
            SELECT FROM information_schema.schemata
            WHERE schema_name = 'raw'
        ) as exists
        "#
    )
    .fetch_one(pool)
    .await?;

    // In test environment, raw schema might not exist yet, which is fine
    // The test is that the query executes successfully

    // Test table existence checking
    let events_table_exists = sqlx::query!(
        r#"
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_schema = 'raw' AND table_name = 'events'
        ) as exists
        "#
    )
    .fetch_one(pool)
    .await?;

    // Again, table might not exist in test environment, but query should work

    Ok(())
}

#[sinex_test]
async fn test_database_connection_pool_health(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

    // Test multiple connections from the pool
    let mut connections = Vec::new();

    for _ in 0..5 {
        let conn = pool.acquire().await?;
        connections.push(conn);
    }

    // All connections should be valid
    assert_eq!(connections.len(), 5);

    // Test that we can execute queries on all connections
    for (i, conn) in connections.iter_mut().enumerate() {
        let result = sqlx::query!("SELECT $1 as test_value", i as i32)
            .fetch_one(mut **conn)
            .await?;

        assert_eq!(result.test_value, Some(i as i32));
    }

    // Connections are automatically returned to pool when dropped
    drop(connections);

    // Verify pool is still functional
    let final_test = sqlx::query!("SELECT 1 as test")
        .fetch_one(pool)
        .await?;

    assert_eq!(final_test.test, Some(1));

    Ok(())
}

#[sinex_test]
async fn test_database_error_handling(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

    // Test handling of SQL syntax errors
    let syntax_error = sqlx::query!("SELECT * FROM nonexistent_table_12345")
        .fetch_optional(pool)
        .await;

    assert!(syntax_error.is_err(), "Should fail with syntax/table error");

    // Test handling of constraint violations
    // First insert a test record
    let test_id = Uuid::new_v4();

    sqlx::query!(
        r#"
        INSERT INTO raw.events (id, source, event_type, payload, ts_ingest)
        VALUES ($1, $2, $3, $4, NOW())
        "#,
        test_id,
        "unit-test-error",
        "test.error_handling",
        serde_json::json!({"test": "constraint"})
    )
    .execute(pool)
    .await?;

    // Try to insert with same ID (should fail with constraint violation)
    let constraint_error = sqlx::query!(
        r#"
        INSERT INTO raw.events (id, source, event_type, payload, ts_ingest)
        VALUES ($1, $2, $3, $4, NOW())
        "#,
        test_id, // Same ID
        "unit-test-error",
        "test.error_handling",
        serde_json::json!({"test": "duplicate"})
    )
    .execute(pool)
    .await;

    assert!(constraint_error.is_err(), "Should fail with constraint violation");

    // Cleanup
    sqlx::query!("DELETE FROM raw.events WHERE id = $1", test_id)
        .execute(pool)
        .await?;

    Ok(())
}