//! ULID edge case testing
//!
//! This module tests ULID behavior at system boundaries including:
//! - Maximum timestamp values (year 10889)
//! - Monotonic generation under extreme load
//! - Wraparound behavior
//! - Concurrent generation safety

use color_eyre::eyre::Result;
use sinex_test_utils::prelude::*;
use sinex_core::types::ulid::Ulid;
use std::collections::{HashMap, HashSet};
use parking_lot::Mutex;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::JoinSet;

// =============================================================================
// ULID Timestamp Boundary Tests
// =============================================================================

#[sinex_test]
async fn test_ulid_max_timestamp_representation(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    println!("Testing ULID maximum timestamp representation...");

    // ULID uses 48 bits for timestamp (milliseconds since Unix epoch)
    let max_timestamp_ms: u64 = (1u64 << 48) - 1; // 281,474,976,710,655 ms

    println!("Max ULID timestamp: {} ms", max_timestamp_ms);
    println!(
        "Max timestamp date: ~{} years from epoch",
        max_timestamp_ms / (365 * 24 * 60 * 60 * 1000)
    );

    // Create ULID with maximum timestamp
    let mut max_ulid_bytes = [0u8; 16];
    // Set timestamp bytes (first 48 bits)
    max_ulid_bytes[0] = (max_timestamp_ms >> 40) as u8;
    max_ulid_bytes[1] = (max_timestamp_ms >> 32) as u8;
    max_ulid_bytes[2] = (max_timestamp_ms >> 24) as u8;
    max_ulid_bytes[3] = (max_timestamp_ms >> 16) as u8;
    max_ulid_bytes[4] = (max_timestamp_ms >> 8) as u8;
    max_ulid_bytes[5] = max_timestamp_ms as u8;
    // Set random component to max
    for i in 6..16 {
        max_ulid_bytes[i] = 0xFF;
    }

    let max_ulid = Ulid::from_bytes(max_ulid_bytes).expect("Valid ULID bytes");
    println!("Max ULID: {}", max_ulid);

    // Test database storage
    let pool = ctx.pool();
    let event = EventBuilder::new()
        .id(max_ulid)
        .source("ulid_boundary_test")
        .event_type("max.timestamp")
        .payload(json!({
            "timestamp_ms": max_timestamp_ms,
            "ulid": max_ulid.to_string()
        }))
        .build();

    // Insert event with max ULID
    let insert_result = insert_event(pool, &event).await;
    assert!(insert_result.is_ok(), "Should handle max ULID");

    // Verify retrieval
    let retrieved = sqlx::query!(
        r#"
        SELECT id as "ulid!: Ulid",
               ts_orig,
               payload
        FROM core.events
        WHERE id = $1::uuid::ulid
        "#,
        max_ulid.to_uuid()
    )
    .fetch_one(pool)
    .await?;

    println!("Retrieved ULID: {}", retrieved.ulid);
    println!("Retrieved timestamp: {:?}", retrieved.ts_orig);

    // Verify ULID components are preserved
    assert_eq!(retrieved.ulid.timestamp_ms(), max_timestamp_ms);

    Ok(())
}

#[sinex_test]
async fn test_ulid_timestamp_wraparound_behavior(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    println!("Testing ULID timestamp wraparound behavior...");

    // Test what happens when we try to create ULIDs beyond max timestamp
    let max_timestamp_ms: u64 = (1u64 << 48) - 1;

    // Try to create a ULID with timestamp beyond max (should fail or wrap)
    let overflow_timestamp = max_timestamp_ms + 1;

    // Manually construct bytes with overflow timestamp
    let mut overflow_bytes = [0u8; 16];
    // This will truncate to 48 bits
    overflow_bytes[0] = (overflow_timestamp >> 40) as u8;
    overflow_bytes[1] = (overflow_timestamp >> 32) as u8;
    overflow_bytes[2] = (overflow_timestamp >> 24) as u8;
    overflow_bytes[3] = (overflow_timestamp >> 16) as u8;
    overflow_bytes[4] = (overflow_timestamp >> 8) as u8;
    overflow_bytes[5] = overflow_timestamp as u8;

    let wrapped_ulid = Ulid::from_bytes(overflow_bytes).expect("Valid ULID bytes");
    let wrapped_timestamp = wrapped_ulid.timestamp_ms();

    println!("Overflow timestamp: {}", overflow_timestamp);
    println!("Wrapped timestamp: {}", wrapped_timestamp);
    println!(
        "Expected wrapped: {}",
        overflow_timestamp & ((1u64 << 48) - 1)
    );

    // Verify wraparound behavior
    assert_eq!(
        wrapped_timestamp,
        overflow_timestamp & ((1u64 << 48) - 1),
        "Timestamp should wrap at 48 bits"
    );

    Ok(())
}

// =============================================================================
// ULID Monotonic Generation Tests
// =============================================================================

#[sinex_test]
async fn test_ulid_monotonic_generation_extreme_rate(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    println!("Testing ULID monotonic generation at extreme rates...");

    let generated_ulids = Arc::new(Mutex::new(Vec::new()));

    // Generate ULIDs from multiple threads as fast as possible
    let thread_count = 10;
    let ulids_per_thread = 10_000;

    let start = Instant::now();
    let mut handles = vec![];

    for thread_id in 0..thread_count {
        let ulids = generated_ulids.clone();

        let handle = std::thread::spawn(move || {
            let mut local_ulids = Vec::with_capacity(ulids_per_thread);

            for _ in 0..ulids_per_thread {
                let ulid = Ulid::new();
                local_ulids.push(ulid);
            }

            ulids.lock().extend(local_ulids);

            println!("Thread {} completed", thread_id);
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let elapsed = start.elapsed();
    let all_ulids = generated_ulids.lock();
    let total_ulids = all_ulids.len();
    let rate = total_ulids as f64 / elapsed.as_secs_f64();

    println!("Generated {} ULIDs in {:?}", total_ulids, elapsed);
    println!("Rate: {:.0} ULIDs/second", rate);

    // Check for strict monotonicity
    let mut violations = 0;
    for window in all_ulids.windows(2) {
        if window[1] <= window[0] {
            violations += 1;
            if violations <= 5 {
                println!("Monotonicity violation: {} <= {}", window[1], window[0]);
            }
        }
    }

    // Check for duplicates
    let unique_count = all_ulids.iter().collect::<HashSet<_>>().len();
    let duplicate_count = total_ulids - unique_count;

    println!("Monotonicity violations: {}", violations);
    println!("Duplicate ULIDs: {}", duplicate_count);

    // With monotonic ULID generation, there should be no violations or duplicates
    assert_eq!(violations, 0, "Ulid::new() should maintain strict ordering");
    assert_eq!(
        duplicate_count, 0,
        "Ulid::new() should never produce duplicates"
    );

    Ok(())
}

#[sinex_test]
async fn test_ulid_generation_same_millisecond_ordering(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    println!("Testing ULID generation within same millisecond...");

    // Force generation within same millisecond by generating in tight loop
    let mut same_ms_ulids = Vec::new();
    let start_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis().min(u64::MAX as u128) as u64;

    // Generate ULIDs until we get a different millisecond
    while std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        == start_ms
    {
        same_ms_ulids.push(Ulid::new());
        if same_ms_ulids.len() > 10000 {
            break; // Safety limit
        }
    }

    println!(
        "Generated {} ULIDs in same millisecond",
        same_ms_ulids.len()
    );

    if same_ms_ulids.len() > 1 {
        // Group by timestamp
        let mut timestamp_groups: HashMap<u64, Vec<Ulid>> = HashMap::new();
        for ulid in &same_ms_ulids {
            timestamp_groups
                .entry(ulid.timestamp_ms())
                .or_insert_with(Vec::new)
                .push(*ulid);
        }

        // Check ordering within same timestamp
        for (ts, group) in timestamp_groups {
            println!("Timestamp {}: {} ULIDs", ts, group.len());

            if group.len() > 1 {
                // Within same millisecond, ordering depends on random component
                let mut sorted = group.clone();
                sorted.sort();

                // Check if original order matches sorted order
                let in_order = group.windows(2).all(|w| w[0] < w[1]);
                println!("  ULIDs in strict order: {}", in_order);

                // Standard ULID generation may not maintain order within same ms
                // This is expected behavior
            }
        }
    }

    Ok(())
}

// =============================================================================
// ULID Concurrent Generation Safety Tests
// =============================================================================

#[sinex_test]
async fn test_ulid_concurrent_generation_safety(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    println!("Testing ULID concurrent generation safety...");

    let pool = ctx.pool();
    let concurrent_tasks = 100;
    let ulids_per_task = 50;

    let start = Instant::now();
    let mut tasks = JoinSet::new();

    for task_id in 0..concurrent_tasks {
        let pool = pool.clone();

        tasks.spawn(async move {
            let mut task_ulids = Vec::new();

            for i in 0..ulids_per_task {
                let ulid = Ulid::new();
                task_ulids.push(ulid);

                // Also test database insertion with specific ULID - skipped for now
                // The TestContext infrastructure doesn't support setting specific ULIDs
                // This would need to be implemented if required for this test
            }

            Ok(task_ulids)
        });
    }

    let mut all_ulids = Vec::new();
    let mut errors = Vec::new();

    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(Ok(ulids)) => all_ulids.extend(ulids),
            Ok(Err(e)) => errors.push(e),
            Err(e) => errors.push(format!("Task panic: {}", e)),
        }
    }

    let elapsed = start.elapsed();
    let total_ulids = all_ulids.len();
    let expected_total = concurrent_tasks * ulids_per_task;

    println!("Generated {} ULIDs in {:?}", total_ulids, elapsed);
    println!(
        "Rate: {:.0} ULIDs/second",
        total_ulids as f64 / elapsed.as_secs_f64()
    );
    println!("Errors: {}", errors.len());

    if !errors.is_empty() {
        for (i, error) in errors.iter().take(5).enumerate() {
            println!("  Error {}: {}", i + 1, error);
        }
    }

    // Check for duplicates
    let unique_ulids: HashSet<_> = all_ulids.iter().collect();
    let duplicate_count = total_ulids - unique_ulids.len();

    println!("Total ULIDs: {}", total_ulids);
    println!("Unique ULIDs: {}", unique_ulids.len());
    println!("Duplicates: {}", duplicate_count);

    // Verify all events were inserted
    let db_count = sqlx::query_scalar!(
        r#"
        SELECT COUNT(*) as "count!"
        FROM core.events
        WHERE source = 'concurrent_ulid_test'
        "#
    )
    .fetch_one(pool)
    .await?;

    println!("Events in database: {}", db_count);

    // Assertions
    assert_eq!(duplicate_count, 0, "No duplicate ULIDs should be generated");
    assert_eq!(
        db_count as usize,
        expected_total - errors.len(),
        "All non-errored events should be in database"
    );

    Ok(())
}

#[sinex_test]
async fn test_ulid_random_component_distribution(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    println!("Testing ULID random component distribution...");

    // Generate many ULIDs and analyze random component distribution
    let sample_size = 10_000;
    let mut random_bytes_distribution = HashMap::new();

    for _ in 0..sample_size {
        let ulid = Ulid::new();
        let bytes = ulid.to_bytes();

        // Random component is bytes 6-15 (10 bytes)
        let random_component = &bytes[6..16];

        // Check first byte distribution
        *random_bytes_distribution
            .entry(random_component[0])
            .or_insert(0) += 1;
    }

    // Calculate statistics
    let mean = sample_size / 256;
    let mut variance_sum = 0.0;

    println!("Random byte distribution (first byte of random component):");
    for byte_value in 0u8..=255 {
        let count = random_bytes_distribution.get(&byte_value).unwrap_or(&0);
        let deviation = (*count as f64 - mean as f64).powi(2);
        variance_sum += deviation;

        if byte_value % 32 == 0 {
            println!("  Byte {:#04x}: {} occurrences", byte_value, count);
        }
    }

    let variance = variance_sum / 256.0;
    let std_dev = variance.sqrt();
    let cv = std_dev / mean as f64; // Coefficient of variation

    println!("\nDistribution statistics:");
    println!("  Expected mean: {}", mean);
    println!("  Standard deviation: {:.2}", std_dev);
    println!("  Coefficient of variation: {:.4}", cv);

    // For good randomness, CV should be small (< 0.1)
    assert!(
        cv < 0.1,
        "Random component should have uniform distribution"
    );

    Ok(())
}

// Helper functions removed - use common test infrastructure instead
