//! Large payload performance testing
//!
//! This module tests system behavior with progressively larger payloads:
//! - JSON payload size limits (approaching PostgreSQL's 1GB JSONB limit)
//! - Deeply nested JSON structures
//! - Large array and object handling
//! - Memory usage and performance characteristics

use serde_json::{json, Value};
use sinex_test_utils::prelude::*;
use std::time::Instant;
use tokio::time::timeout;

// =============================================================================
// Progressive Payload Size Tests
// =============================================================================

#[sinex_test(timeout = 300)] // 5 minute timeout for large operations
async fn test_progressive_payload_sizes(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    println!("Testing progressive payload sizes...");

    let pool = ctx.pool();

    // Test sizes from 1MB to 500MB
    let test_sizes = vec![
        (1_000_000, "1MB"),
        (5_000_000, "5MB"),
        (10_000_000, "10MB"),
        (25_000_000, "25MB"),
        (50_000_000, "50MB"),
        (100_000_000, "100MB"),
        (250_000_000, "250MB"),
        (500_000_000, "500MB"),
    ];

    let mut results = Vec::new();

    for (size, label) in test_sizes {
        println!(
            "\n{} Testing {} payload",
            chrono::Utc::now().format("%H:%M:%S"),
            label
        );

        // Generate large JSON payload
        let large_data = generate_large_json_payload(size);
        let payload_size = serde_json::to_string(&large_data)?.len();
        println!("  Actual JSON size: {} bytes", payload_size);

        let event = EventBuilder::new()
            .source("large_payload_test")
            .event_type("size.test")
            .payload(large_data)
            .build();

        // Measure insertion time
        let insert_start = Instant::now();
        let insert_result = timeout(Duration::from_secs(60), insert_event(pool, &event)).await;

        match insert_result {
            Ok(Ok(_)) => {
                let insert_duration = insert_start.elapsed();
                println!("  ✓ Insert successful in {:?}", insert_duration);

                // Measure retrieval time
                let retrieve_start = Instant::now();
                let retrieve_result = timeout(
                    Duration::from_secs(60),
                    sqlx::query!(
                        r#"
                        SELECT
                            event_id::text as "id!",
                            source,
                            event_type,
                            payload,
                            pg_column_size(payload) as "payload_size!"
                        FROM core.events
                        WHERE event_id::uuid = $1::uuid
                        "#,
                        event.id.to_uuid()
                    )
                    .fetch_one(pool),
                )
                .await;

                match retrieve_result {
                    Ok(Ok(row)) => {
                        let retrieve_duration = retrieve_start.elapsed();
                        println!("  ✓ Retrieve successful in {:?}", retrieve_duration);
                        println!("  Storage size: {} bytes", row.payload_size);

                        results.push((
                            label,
                            payload_size,
                            row.payload_size as usize,
                            insert_duration,
                            retrieve_duration,
                            true,
                        ));
                    }
                    Ok(Err(e)) => {
                        println!("  ✗ Retrieve failed: {}", e);
                        results.push((
                            label,
                            payload_size,
                            0,
                            insert_duration,
                            Duration::ZERO,
                            false,
                        ));
                    }
                    Err(_) => {
                        println!("  ✗ Retrieve timeout (>60s)");
                        results.push((
                            label,
                            payload_size,
                            0,
                            insert_duration,
                            Duration::ZERO,
                            false,
                        ));
                    }
                }
            }
            Ok(Err(e)) => {
                println!("  ✗ Insert failed: {}", e);
                results.push((
                    label,
                    payload_size,
                    0,
                    Duration::ZERO,
                    Duration::ZERO,
                    false,
                ));

                // Check if it's a size limit error
                if e.to_string().contains("row too big") || e.to_string().contains("out of memory")
                {
                    println!("  → Hit database size limit at {}", label);
                    break;
                }
            }
            Err(_) => {
                println!("  ✗ Insert timeout (>60s)");
                results.push((
                    label,
                    payload_size,
                    0,
                    Duration::ZERO,
                    Duration::ZERO,
                    false,
                ));
            }
        }

        // Small delay between tests
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // Print summary
    println!("\n=== Payload Size Test Results ===");
    println!("Size     | JSON Size  | DB Size    | Insert Time | Retrieve Time | Success");
    println!("---------|------------|------------|-------------|---------------|--------");
    for (label, json_size, db_size, insert_time, retrieve_time, success) in results {
        println!(
            "{:8} | {:10} | {:10} | {:11.2?} | {:13.2?} | {}",
            label,
            format_bytes(json_size),
            format_bytes(db_size),
            insert_time,
            retrieve_time,
            if success { "✓" } else { "✗" }
        );
    }

    Ok(())
}

// =============================================================================
// Deeply Nested JSON Tests
// =============================================================================

#[sinex_bench]
async fn test_deeply_nested_json_structures(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    println!("Testing deeply nested JSON structures...");

    let pool = ctx.pool();

    // Test various nesting depths
    let depths = vec![
        (10, "shallow"),
        (50, "moderate"),
        (100, "deep"),
        (500, "very deep"),
        (1000, "extreme"),
        (5000, "pathological"),
    ];

    for (depth, description) in depths {
        println!("\nTesting {} nesting (depth={})", description, depth);

        // Create deeply nested object
        let nested = create_nested_json(depth, "object");
        let json_str = serde_json::to_string(&nested)?;
        println!("  JSON size: {} bytes", json_str.len());

        let event = EventBuilder::new()
            .source("nested_json_test")
            .event_type("nesting.test")
            .payload(json!({
                "depth": depth,
                "type": "object",
                "nested": nested
            }))
            .build();

        // Try to insert
        let insert_start = Instant::now();
        match insert_event(pool, &event).await {
            Ok(_) => {
                let duration = insert_start.elapsed();
                println!("  ✓ Insert successful in {:?}", duration);

                // Test querying nested data
                let query_result = sqlx::query!(
                    r#"
                    SELECT
                        jsonb_typeof(payload->'nested') as json_type,
                        jsonb_depth(payload->'nested') as nesting_depth
                    FROM core.events
                    WHERE event_id::uuid = $1::uuid
                    "#,
                    event.id.to_uuid()
                )
                .fetch_optional(pool)
                .await;

                match query_result {
                    Ok(Some(row)) => {
                        println!(
                            "  Type: {:?}, Depth measurement: {:?}",
                            row.json_type, row.nesting_depth
                        );
                    }
                    Ok(None) => println!("  ⚠️  Query returned no results"),
                    Err(e) => println!("  ⚠️  Query failed: {}", e),
                }
            }
            Err(e) => {
                println!("  ✗ Insert failed: {}", e);

                // Check if it's a nesting limit
                if e.to_string().contains("nested too deeply")
                    || e.to_string().contains("stack depth limit")
                {
                    println!("  → Hit nesting depth limit at depth {}", depth);
                    break;
                }
            }
        }

        // Also test deeply nested arrays
        println!("  Testing array nesting at depth {}...", depth);
        let nested_array = create_nested_json(depth, "array");

        let array_event = EventBuilder::new()
            .source("nested_json_test")
            .event_type("nesting.array")
            .payload(json!({
                "depth": depth,
                "type": "array",
                "nested": nested_array
            }))
            .build();

        match insert_event(pool, &array_event).await {
            Ok(_) => println!("  ✓ Array nesting successful"),
            Err(e) => println!("  ✗ Array nesting failed: {}", e),
        }
    }

    Ok(())
}

// =============================================================================
// Large Array and Object Tests
// =============================================================================

#[sinex_test(timeout = 120)]
async fn test_large_arrays_and_objects(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    println!("Testing large arrays and objects...");

    let pool = ctx.pool();

    // Test large arrays
    let array_sizes = vec![
        (1_000, "1K elements"),
        (10_000, "10K elements"),
        (100_000, "100K elements"),
        (1_000_000, "1M elements"),
    ];

    println!("\n--- Testing Large Arrays ---");
    for (size, description) in array_sizes {
        println!("\nTesting array with {}", description);

        // Create array with mixed types
        let large_array: Vec<Value> = (0..size)
            .map(|i| match i % 4 {
                0 => json!(i),
                1 => json!(format!("string_{}", i)),
                2 => json!({"index": i, "data": "test"}),
                _ => json!([i, i + 1, i + 2]),
            })
            .collect();

        let payload = json!({
            "array_size": size,
            "data": large_array
        });

        let json_size = serde_json::to_string(&payload)?.len();
        println!("  JSON size: {}", format_bytes(json_size));

        let event = EventBuilder::new()
            .source("large_array_test")
            .event_type("array.size")
            .payload(payload)
            .build();

        let start = Instant::now();
        match timeout(Duration::from_secs(30), insert_event(pool, &event)).await {
            Ok(Ok(_)) => {
                let duration = start.elapsed();
                println!("  ✓ Insert successful in {:?}", duration);

                // Test array operations
                let ops_result = sqlx::query!(
                    r#"
                    SELECT
                        jsonb_array_length(payload->'data') as array_length,
                        pg_column_size(payload->'data') as data_size
                    FROM core.events
                    WHERE event_id::uuid = $1::uuid
                    "#,
                    event.id.to_uuid()
                )
                .fetch_one(pool)
                .await;

                match ops_result {
                    Ok(row) => {
                        println!(
                            "  Array length: {:?}, Storage size: {} bytes",
                            row.array_length,
                            row.data_size.unwrap_or(0)
                        );
                    }
                    Err(e) => println!("  Array operations failed: {}", e),
                }
            }
            Ok(Err(e)) => println!("  ✗ Insert failed: {}", e),
            Err(_) => println!("  ✗ Insert timeout (>30s)"),
        }
    }

    // Test large objects
    println!("\n--- Testing Large Objects ---");
    let object_sizes = vec![
        (1_000, "1K keys"),
        (10_000, "10K keys"),
        (100_000, "100K keys"),
    ];

    for (size, description) in object_sizes {
        println!("\nTesting object with {}", description);

        // Create object with many keys
        let mut large_object = serde_json::Map::new();
        for i in 0..size {
            let key = format!("key_{:06}", i);
            let value = json!({
                "index": i,
                "data": format!("value_{}", i),
                "timestamp": chrono::Utc::now().timestamp_millis()
            });
            large_object.insert(key, value);
        }

        let payload = json!({
            "object_size": size,
            "data": large_object
        });

        let json_size = serde_json::to_string(&payload)?.len();
        println!("  JSON size: {}", format_bytes(json_size));

        let event = EventBuilder::new()
            .source("large_object_test")
            .event_type("object.size")
            .payload(payload)
            .build();

        let start = Instant::now();
        match timeout(Duration::from_secs(30), insert_event(pool, &event)).await {
            Ok(Ok(_)) => {
                let duration = start.elapsed();
                println!("  ✓ Insert successful in {:?}", duration);
            }
            Ok(Err(e)) => println!("  ✗ Insert failed: {}", e),
            Err(_) => println!("  ✗ Insert timeout (>30s)"),
        }
    }

    Ok(())
}

// =============================================================================
// Memory Pressure Tests
// =============================================================================

#[sinex_bench]
async fn test_concurrent_large_payload_memory_pressure(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    println!("Testing concurrent large payload memory pressure...");

    let pool = ctx.pool();
    let concurrent_tasks = 10;
    let payload_size = 10_000_000; // 10MB per payload

    println!(
        "Spawning {} concurrent tasks with {}MB payloads each",
        concurrent_tasks,
        payload_size / 1_000_000
    );

    let start = Instant::now();
    let mut handles = vec![];

    for task_id in 0..concurrent_tasks {
        let pool = pool.clone();

        let handle = tokio::spawn(async move {
            // Generate large payload
            let large_data = generate_large_json_payload(payload_size);

            let event = EventBuilder::new()
                .source("memory_pressure_test")
                .event_type("concurrent.large")
                .payload(json!({
                    "task_id": task_id,
                    "size": payload_size,
                    "data": large_data
                }))
                .build();

            let task_start = Instant::now();
            let result = insert_event(&pool, &event).await;
            let duration = task_start.elapsed();

            match result {
                Ok(_) => Ok((task_id, duration)),
                Err(e) => Err((task_id, e.to_string())),
            }
        });

        handles.push(handle);
    }

    // Wait for all tasks
    let results = futures::future::join_all(handles).await;
    let total_duration = start.elapsed();

    let mut successful = 0;
    let mut failed = 0;

    println!("\nResults:");
    for result in results {
        match result {
            Ok(Ok((id, duration))) => {
                successful += 1;
                println!("  Task {} succeeded in {:?}", id, duration);
            }
            Ok(Err((id, error))) => {
                failed += 1;
                println!("  Task {} failed: {}", id, error);
            }
            Err(e) => {
                failed += 1;
                println!("  Task panicked: {}", e);
            }
        }
    }

    println!("\nSummary:");
    println!("  Total time: {:?}", total_duration);
    println!("  Successful: {}/{}", successful, concurrent_tasks);
    println!("  Failed: {}/{}", failed, concurrent_tasks);
    println!(
        "  Total data processed: {}MB",
        (successful * payload_size) / 1_000_000
    );

    // Most tasks should succeed
    assert!(
        successful > concurrent_tasks / 2,
        "At least half of concurrent large payload insertions should succeed"
    );

    Ok(())
}

// =============================================================================
// Helper Functions
// =============================================================================

fn generate_large_json_payload(target_size: usize) -> Value {
    // Create a JSON structure that results in approximately target_size bytes
    let mut data = serde_json::Map::new();

    // Add metadata
    data.insert(
        "generated_at".to_string(),
        json!(chrono::Utc::now().to_rfc3339()),
    );
    data.insert("target_size".to_string(), json!(target_size));

    // Calculate how much data we need to add
    let overhead = serde_json::to_string(&data).unwrap().len();
    let remaining = target_size.saturating_sub(overhead);

    // Generate string data to fill the remaining space
    // Account for JSON escaping overhead (roughly 10%)
    let string_size = (remaining as f64 * 0.9) as usize;

    // Create data in chunks to avoid massive string allocations
    let chunk_size = 1_000_000; // 1MB chunks
    let mut chunks = Vec::new();
    let mut total_size = 0;

    while total_size < string_size {
        let size = (string_size - total_size).min(chunk_size);
        let chunk: String = (0..size)
            .map(|i| {
                match i % 100 {
                    0..=60 => 'a',  // 60% 'a'
                    61..=80 => 'b', // 20% 'b'
                    81..=90 => '1', // 10% '1'
                    _ => '\n',      // 10% newlines
                }
            })
            .collect();
        chunks.push(chunk);
        total_size += size;
    }

    data.insert("data".to_string(), json!(chunks.join("")));

    // Add some structured data
    data.insert(
        "metadata".to_string(),
        json!({
            "type": "large_payload_test",
            "chunks": chunks.len(),
            "actual_size": total_size
        }),
    );

    Value::Object(data)
}

fn create_nested_json(depth: usize, structure_type: &str) -> Value {
    if depth == 0 {
        return json!({"leaf": true, "value": "end"});
    }

    match structure_type {
        "array" => {
            json!([create_nested_json(depth - 1, structure_type)])
        }
        "object" => {
            json!({
                "level": depth,
                "nested": create_nested_json(depth - 1, structure_type)
            })
        }
        "mixed" => {
            if depth % 2 == 0 {
                json!([create_nested_json(depth - 1, "mixed")])
            } else {
                json!({"nested": create_nested_json(depth - 1, "mixed")})
            }
        }
        _ => json!(null),
    }
}

fn format_bytes(bytes: usize) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB"];
    let mut size = bytes as f64;
    let mut unit_index = 0;

    while size >= 1024.0 && unit_index < UNITS.len() - 1 {
        size /= 1024.0;
        unit_index += 1;
    }

    format!("{:.2} {}", size, UNITS[unit_index])
}

async fn insert_event(pool: &PgPool, event: &Event) -> Result<(), Error> {
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
    .await
    .wrap_err("Failed to insert event")?;
    Ok(())
}

// Custom queries for PostgreSQL-specific JSON functions
// Note: These are wrapped in conditional compilation for PostgreSQL
#[cfg(feature = "postgres")]
mod pg_json_functions {
    use sqlx::postgres::PgQueryAs;

    pub async fn jsonb_depth(pool: &PgPool, event_id: Ulid) -> Result<Option<i32>, Error> {
        // PostgreSQL doesn't have built-in jsonb_depth, we'd need a custom function
        // For now, return None
        Ok(None)
    }
}
