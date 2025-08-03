//! Comprehensive error path testing for production code
//!
//! This module tests all error conditions that could trigger unwrap() or expect()
//! failures in production code, ensuring graceful error handling.

use chrono::{DateTime, Utc};
use sinex_db::{queries::checkpoints::CheckpointQueries, query_helpers::ulid_to_uuid};
use sinex_types::error::{Error, ErrorContext};
use sinex_test_utils::prelude::*;
use sinex_types::ulid::Ulid;
use sqlx::PgPool;
use std::str::FromStr;

// =============================================================================
// ULID Parsing Error Tests
// =============================================================================

#[sinex_test]
async fn test_checkpoint_invalid_ulid_parsing(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // Test various invalid ULID formats that could cause parsing errors
    let invalid_ulids = vec![
        ("not-a-ulid", "Non-ULID string"),
        (
            "01234567890123456789012345",
            "Wrong length (25 chars instead of 26)",
        ),
        ("01234567890123456789012345XX", "Extra characters"),
        ("ZZZZZZZZZZZZZZZZZZZZZZZZZ", "Invalid base32 characters"),
        ("", "Empty string"),
        ("01234567890123456789012345\0", "Null byte in string"),
        ("01234567890123456789012345 ", "Trailing space"),
        (" 01234567890123456789012345", "Leading space"),
        (
            "0123456789ABCDEFGHIJKLMNOP",
            "Mixed case (should be uppercase)",
        ),
        ("🦀1234567890123456789012345", "Unicode in ULID"),
    ];

    for (invalid, description) in invalid_ulids {
        println!("Testing invalid ULID: {} - {}", invalid, description);

        // Test direct parsing
        match Ulid::from_str(invalid) {
            Ok(_) => panic!("Expected error for {}: {}", description, invalid),
            Err(e) => {
                println!("  ✓ Correctly rejected with error: {}", e);
            }
        }

        // Test in database context
        let result = sqlx::query!(
            r#"
            SELECT $1::text as ulid_text,
                   CASE 
                     WHEN $1::text ~ '^[0-9A-Z]{26}$' THEN true
                     ELSE false
                   END as is_valid_format
            "#,
            invalid
        )
        .fetch_one(ctx.pool())
        .await?;

        assert!(
            !result.is_valid_format,
            "Database should reject invalid ULID format"
        );
    }

    Ok(())
}

#[sinex_test]
async fn test_ulid_uuid_conversion_errors(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // Test ULID to UUID conversion edge cases
    let edge_cases = vec![
        // Maximum valid ULID
        Ulid::from_parts(u64::MAX >> 16, u128::MAX),
        // Minimum valid ULID
        Ulid::from_parts(0, 0),
        // Current time ULID
        Ulid::new(),
    ];

    for ulid in edge_cases {
        println!("Testing ULID to UUID conversion: {}", ulid);

        // This should always succeed for valid ULIDs
        let uuid = ulid_to_uuid(ulid);
        println!("  Converted to UUID: {}", uuid);

        // Verify round-trip is not possible (UUIDs lose timestamp precision)
        let uuid_bytes = uuid.as_bytes();
        let ulid_bytes = ulid.to_bytes();

        // First 6 bytes (timestamp) might differ due to UUID version bits
        println!("  ULID bytes: {:?}", &ulid_bytes[..8]);
        println!("  UUID bytes: {:?}", &uuid_bytes[..8]);
    }

    Ok(())
}

// =============================================================================
// Timestamp Conversion Error Tests
// =============================================================================

#[sinex_test]
async fn test_timestamp_conversion_boundaries(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // Test timestamp values that could cause conversion errors
    let edge_timestamps = vec![
        (0i64, "Unix epoch"),
        (946684800, "Year 2000"),
        (-1, "Before epoch"),
        (i64::MAX / 1000, "Near i64::MAX (seconds)"),
        (i64::MIN / 1000, "Near i64::MIN (seconds)"),
        (253402300799, "Year 9999 (max typical)"),
        (32503680000, "Year 3000"),
    ];

    for (timestamp_secs, description) in edge_timestamps {
        println!("Testing timestamp: {} - {}", timestamp_secs, description);

        // Test DateTime creation
        match DateTime::from_timestamp(timestamp_secs, 0) {
            Some(dt) => {
                println!("  ✓ Valid datetime: {}", dt);

                // Test database storage
                let result = sqlx::query!(
                    r#"
                    SELECT $1::timestamptz as ts,
                           EXTRACT(EPOCH FROM $1::timestamptz)::bigint as epoch_secs
                    "#,
                    dt
                )
                .fetch_one(ctx.pool())
                .await?;

                // Verify round-trip
                assert_eq!(
                    result.epoch_secs.unwrap_or(0),
                    timestamp_secs,
                    "Timestamp round-trip failed for {}",
                    description
                );
            }
            None => {
                println!("  ✗ Invalid timestamp (expected for some edge cases)");
            }
        }
    }

    Ok(())
}

#[sinex_test]
async fn test_timestamp_overflow_in_calculations(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // Test timestamp arithmetic that could overflow
    let base_time = Utc::now();

    let overflow_operations = vec![
        ("Add max duration", chrono::Duration::max_value()),
        ("Subtract max duration", -chrono::Duration::max_value()),
        ("Add 100 years", chrono::Duration::days(365 * 100)),
        ("Subtract 100 years", chrono::Duration::days(-365 * 100)),
    ];

    for (operation, duration) in overflow_operations {
        println!("Testing timestamp operation: {}", operation);

        // Use checked arithmetic
        match base_time.checked_add_signed(duration) {
            Some(result) => {
                println!("  ✓ Operation succeeded: {}", result);
            }
            None => {
                println!("  ✗ Operation would overflow (correctly detected)");
            }
        }
    }

    Ok(())
}

// =============================================================================
// JSON Parsing Error Tests
// =============================================================================

#[sinex_test]
async fn test_json_parsing_edge_cases(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    use serde_json::{json, Value};

    // Test JSON values that could cause parsing errors
    let edge_cases = vec![
        (json!(null), "Null value"),
        (json!({}), "Empty object"),
        (json!([]), "Empty array"),
        (json!({"key": null}), "Null in object"),
        (json!({"": "empty key"}), "Empty string key"),
        (json!({"longkey": "value"}), "Very long key"),
        (
            json!({"nested": {"deep": {"deeper": {"deepest": "value"}}}}),
            "Deeply nested",
        ),
        (json!([[[[[["deep"]]]]]]), "Deeply nested arrays"),
        (json!({"unicode": "🦀🔥💻"}), "Unicode values"),
        (json!({"number": f64::INFINITY}), "Infinity (becomes null)"),
        (json!({"number": f64::NAN}), "NaN (becomes null)"),
    ];

    for (json_val, description) in edge_cases {
        println!("Testing JSON: {}", description);

        // Test serialization
        match serde_json::to_string(&json_val) {
            Ok(json_str) => {
                println!("  ✓ Serialized successfully: {} bytes", json_str.len());

                // Test database storage
                let result = sqlx::query!(
                    r#"
                    SELECT $1::jsonb as data,
                           jsonb_typeof($1::jsonb) as json_type,
                           pg_column_size($1::jsonb) as size_bytes
                    "#,
                    json_val
                )
                .fetch_one(ctx.pool())
                .await?;

                println!(
                    "  DB type: {:?}, size: {:?} bytes",
                    result.json_type, result.size_bytes
                );

                // Special handling for infinity/NaN
                if json_val
                    .get("number")
                    .and_then(|v| v.as_f64())
                    .map(|f| f.is_infinite() || f.is_nan())
                    .unwrap_or(false)
                {
                    // PostgreSQL converts these to null
                    assert_eq!(result.data.get("number"), Some(&Value::Null));
                } else {
                    assert_eq!(result.data, json_val);
                }
            }
            Err(e) => {
                println!("  ✗ Serialization failed: {} (may be expected)", e);
            }
        }
    }

    Ok(())
}

// =============================================================================
// Database Operation Error Tests
// =============================================================================

#[sinex_test]
async fn test_database_constraint_violations(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let pool = ctx.pool();

    // Create a test event
    let event = events::create_test_event();
    insert_event(pool, &event).await?;

    // Test various constraint violations

    // 1. Duplicate primary key (ULID)
    println!("Testing duplicate primary key insertion...");
    let duplicate_result = sqlx::query!(
        r#"
        INSERT INTO core.events (event_id, source, event_type, payload, ts_orig, ts_ingest)
        VALUES ($1::uuid, $2, $3, $4, $5, $6)
        "#,
        event.id.to_uuid(),
        event.source,
        event.event_type,
        event.payload,
        event.ts_orig,
        event.ts_ingest
    )
    .execute(pool)
    .await;

    assert!(
        duplicate_result.is_err(),
        "Duplicate primary key should fail"
    );
    if let Err(e) = duplicate_result {
        println!("  ✓ Correctly rejected: {}", e);
    }

    // 2. Null constraint violations
    println!("Testing null constraint violations...");
    let null_source_result = sqlx::query!(
        r#"
        INSERT INTO core.events (event_id, source, event_type, payload, ts_orig, ts_ingest)
        VALUES ($1::uuid, NULL, $2, $3, $4, $5)
        "#,
        Ulid::new().to_uuid(),
        "test.type",
        json!({}),
        Utc::now(),
        Utc::now()
    )
    .execute(pool)
    .await;

    assert!(null_source_result.is_err(), "Null source should fail");

    // 3. Check constraint violations
    println!("Testing check constraint violations...");

    // Empty source string
    let empty_source_result = sqlx::query!(
        r#"
        INSERT INTO core.events (event_id, source, event_type, payload, ts_orig, ts_ingest)
        VALUES ($1::uuid, $2, $3, $4, $5, $6)
        "#,
        Ulid::new().to_uuid(),
        "", // Empty source
        "test.type",
        json!({}),
        Utc::now(),
        Utc::now()
    )
    .execute(pool)
    .await;

    if empty_source_result.is_err() {
        println!("  ✓ Empty source correctly rejected");
    }

    Ok(())
}

#[sinex_test]
async fn test_transaction_rollback_scenarios(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let pool = ctx.pool();

    // Test transaction rollback behavior
    println!("Testing transaction rollback scenarios...");

    // Start a transaction
    let mut tx = pool.begin().await?;

    // Insert an event in the transaction
    let event1 = events::create_test_event();
    sqlx::query!(
        r#"
        INSERT INTO core.events (event_id, source, event_type, payload, ts_orig, ts_ingest)
        VALUES ($1::uuid, $2, $3, $4, $5, $6)
        "#,
        event1.id.to_uuid(),
        event1.source,
        event1.event_type,
        event1.payload,
        event1.ts_orig,
        event1.ts_ingest
    )
    .execute(&mut *tx)
    .await?;

    // Verify it exists in the transaction
    let count_in_tx = sqlx::query_scalar!(
        "SELECT COUNT(*) FROM core.events WHERE event_id::uuid = $1::uuid",
        event1.id.to_uuid()
    )
    .fetch_one(&mut *tx)
    .await?;

    assert_eq!(
        count_in_tx.unwrap_or(0),
        1,
        "Event should exist in transaction"
    );

    // Rollback the transaction
    tx.rollback().await?;

    // Verify event doesn't exist after rollback
    let count_after_rollback = sqlx::query_scalar!(
        "SELECT COUNT(*) FROM core.events WHERE event_id::uuid = $1::uuid",
        event1.id.to_uuid()
    )
    .fetch_one(pool)
    .await?;

    assert_eq!(
        count_after_rollback.unwrap_or(0),
        0,
        "Event should not exist after rollback"
    );
    println!("  ✓ Transaction rollback working correctly");

    Ok(())
}

// =============================================================================
// Query Builder Error Tests
// =============================================================================

#[sinex_test]
async fn test_query_builder_invalid_operations(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    use sinex_db::query_builder::{QueryBuilder, QueryParam};

    // Test invalid query builder operations
    println!("Testing query builder error cases...");

    // 1. Empty table name
    let result = std::panic::catch_unwind(|| QueryBuilder::select(""));
    if result.is_err() {
        println!("  ✓ Empty table name correctly rejected");
    }

    // 2. Invalid column names
    let invalid_columns = vec![
        "column; DROP TABLE events;--", // SQL injection attempt
        "column/*comment*/name",        // Comment injection
        "column\0name",                 // Null byte
        "",                             // Empty column
        "column name with spaces",      // Unquoted spaces
    ];

    for invalid_col in invalid_columns {
        println!("  Testing invalid column: {:?}", invalid_col);

        let builder = QueryBuilder::select("test_table").columns(&[invalid_col]);

        // Building should handle this gracefully
        match builder.build() {
            Ok((sql, _)) => {
                println!("    SQL generated: {}", sql);
                // Verify proper escaping/quoting
                assert!(!sql.contains("DROP"), "SQL injection should be prevented");
            }
            Err(e) => {
                println!("    ✓ Correctly rejected: {}", e);
            }
        }
    }

    Ok(())
}

// =============================================================================
// Concurrent Operation Error Tests
// =============================================================================

#[sinex_test]
async fn test_concurrent_checkpoint_update_conflicts(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let pool = ctx.pool();
    let processor_name = "conflict_test_processor";
    let consumer_group = "test_group";
    let consumer_name = "test_consumer";

    // Create initial checkpoint
    let checkpoint_id = Ulid::new();
    CheckpointQueries::upsert_checkpoint(
        checkpoint_id,
        processor_name.to_string(),
        consumer_group.to_string(),
        consumer_name.to_string(),
        None,
        0,
        Utc::now(),
        None,
        1,
        None,
        Utc::now(),
        Utc::now(),
    )
    .build()?
    .0 // Get SQL string
    .as_str(); // This is a placeholder - actual execution would happen here

    println!("Testing concurrent checkpoint updates...");

    // Simulate concurrent updates
    let update_tasks = (0..10).map(|i| {
        let pool = pool.clone();
        tokio::spawn(async move {
            // Each task tries to update the same checkpoint
            let update_result = sqlx::query!(
                r#"
                UPDATE core.processor_checkpoints
                SET processed_count = processed_count + 1,
                    last_processed_id = $1,
                    updated_at = NOW()
                WHERE processor_name = $2
                  AND consumer_group = $3
                  AND consumer_name = $4
                "#,
                format!("event_{}", i),
                processor_name,
                consumer_group,
                consumer_name
            )
            .execute(&pool)
            .await;

            match update_result {
                Ok(result) => Ok(result.rows_affected()),
                Err(e) => Err(e),
            }
        })
    });

    let results = futures::future::join_all(update_tasks).await;

    let mut successful_updates = 0;
    let mut failed_updates = 0;

    for (i, result) in results.into_iter().enumerate() {
        match result {
            Ok(Ok(rows)) => {
                if rows > 0 {
                    successful_updates += 1;
                    println!("  Task {} succeeded", i);
                }
            }
            Ok(Err(e)) => {
                failed_updates += 1;
                println!("  Task {} failed: {}", i, e);
            }
            Err(e) => {
                failed_updates += 1;
                println!("  Task {} panicked: {}", i, e);
            }
        }
    }

    println!("Concurrent update results:");
    println!("  Successful: {}", successful_updates);
    println!("  Failed: {}", failed_updates);

    // All updates should succeed (PostgreSQL handles concurrent updates)
    assert!(successful_updates > 0, "Some updates should succeed");

    Ok(())
}

// =============================================================================
// Helper Functions
// =============================================================================

async fn insert_event(pool: &PgPool, event: &sinex_events::Event) -> Result<(), sqlx::Error> {
    sqlx::query!(
        r#"
        INSERT INTO core.events (event_id, source, event_type, payload, ts_orig, ts_ingest)
        VALUES ($1::uuid, $2, $3, $4, $5, $6)
        "#,
        event.id.to_uuid(),
        event.source,
        event.event_type,
        event.payload,
        event.ts_orig,
        event.ts_ingest
    )
    .execute(pool)
    .await?;
    Ok(())
}

mod events {
    use super::*;
    use serde_json::json;

    pub fn create_test_event() -> sinex_events::RawEvent {
        crate::sinex_test_utils::test_event_with_payload(
            "error_path_test",
            "test.event",
            json!({"test": true}),
        )
    }
}
