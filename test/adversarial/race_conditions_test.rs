use crate::common::prelude::*;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Instant;

#[sinex_test]
async fn test_worker_claim_exact_same_microsecond(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

    // Insert event to be claimed
    let event = events::race_test_event("race");

    let inserted = queries::insert_event(pool, &event).await?;
    let event_id = inserted.id;

    // Create high-precision synchronization
    let barrier = Arc::new(Barrier::new(2));
    let claims = Arc::new(AtomicU64::new(0));

    let pool1 = pool.clone();
    let pool2 = pool.clone();
    let barrier1 = barrier.clone();
    let barrier2 = barrier.clone();
    let claims1 = claims.clone();
    let claims2 = claims.clone();

    let handle1 = tokio::spawn(async move {
        barrier1.wait();

        // Try to claim with SELECT FOR UPDATE
        let result = sqlx::query!(
            r#"
                UPDATE raw.events
                SET payload = payload || '{"claimed_by": 1}'::jsonb
                WHERE id::uuid = $1::uuid
                AND NOT (payload ? 'claimed_by')
                "#,
            event_id.to_uuid()
        )
        .execute(&pool1)
        .await;

        if let Ok(result) = result {
            if result.rows_affected() > 0 {
                claims1.fetch_add(1, Ordering::SeqCst);
            }
        }
    });

    let handle2 = tokio::spawn(async move {
        barrier2.wait();

        // Try to claim at exact same time
        let result = sqlx::query!(
            r#"
                UPDATE raw.events
                SET payload = payload || '{"claimed_by": 2}'::jsonb
                WHERE id::uuid = $1::uuid
                AND NOT (payload ? 'claimed_by')
                "#,
            event_id.to_uuid()
        )
        .execute(&pool2)
        .await;

        if let Ok(result) = result {
            if result.rows_affected() > 0 {
                claims2.fetch_add(1, Ordering::SeqCst);
            }
        }
    });

    let _ = tokio::join!(handle1, handle2);

    let total_claims = claims.load(Ordering::SeqCst);
    println!("Total successful claims: {}", total_claims);

    // Check final state
    let final_state = sqlx::query!(
        "SELECT payload FROM raw.events WHERE id::uuid = $1::uuid",
        event_id.to_uuid()
    )
    .fetch_one(pool)
    .await?;

    println!("Final payload: {}", final_state.payload);

    // Both workers might claim if there's a race condition
    pretty_assertions::assert_eq!(total_claims, 1, "Multiple workers claimed same event!");

    Ok(())
}

#[sinex_test]
async fn test_event_causality_violation(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();
    let order_violations = Arc::new(AtomicU64::new(0));

    // Simulate dependent events processed out of order
    for _ in 0..100 {
        let pool1 = pool.clone();
        let pool2 = pool.clone();
        let violations = order_violations.clone();

        // Event A: Create file
        let event_a = events::file_created_event("/tmp/test.txt");

        // Event B: Modify file (depends on A)
        let event_b = events::file_modified_event("/tmp/test.txt");

        // Use deterministic barrier for precise race condition timing
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let barrier_a = barrier.clone();
        let barrier_b = barrier.clone();

        // Insert B first (race condition)
        let handle_b = tokio::spawn(async move {
            barrier_b.wait().await;
            queries::insert_event(&pool2, &event_b).await
        });

        // Insert A second - both will start simultaneously at barrier
        let handle_a = tokio::spawn(async move {
            barrier_a.wait().await;
            queries::insert_event(&pool1, &event_a).await
        });

        let (res_b, res_a) = tokio::join!(handle_b, handle_a);

        if let (Ok(Ok(inserted_b)), Ok(Ok(inserted_a))) = (res_b, res_a) {
            // Check if B was inserted before A (causality violation)
            let b_id = inserted_b.id;
            let a_id = inserted_a.id;

            if b_id < a_id {
                violations.fetch_add(1, Ordering::SeqCst);
            }
        }
    }

    let total_violations = order_violations.load(Ordering::SeqCst);
    println!("Causality violations detected: {}/100", total_violations);

    // This likely shows violations due to concurrent inserts
    Ok(())
}

#[sinex_test]
async fn test_work_queue_thundering_herd(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

    // Insert single event
    let event = events::adversarial_test_event("herd.test", serde_json::json!({"value": "prize"}));

    queries::insert_event(pool, &event).await?;

    // Simulate 100 workers waking simultaneously
    let start = Instant::now();
    let mut handles = vec![];
    let successful_claims = Arc::new(AtomicU64::new(0));

    for i in 0..100 {
        let pool = pool.clone();
        let claims = successful_claims.clone();

        let handle = tokio::spawn(async move {
            // All workers try to claim work simultaneously
            let result = sqlx::query!(
                r#"
                    SELECT id::uuid as id
                    FROM raw.events
                    WHERE event_type = 'herd.test'
                    AND NOT (payload ? 'claimed')
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                    "#
            )
            .fetch_optional(&pool)
            .await;

            if let Ok(Some(_)) = result {
                claims.fetch_add(1, Ordering::SeqCst);
                println!("Worker {} claimed the event", i);
            }
        });

        handles.push(handle);
    }

    futures::future::join_all(handles).await;

    let elapsed = start.elapsed();
    let claims = successful_claims.load(Ordering::SeqCst);

    println!("Thundering herd results:");
    println!("- Time taken: {:?}", elapsed);
    println!("- Successful claims: {}", claims);
    println!("- Database connections stressed: 100");

    // Only 1 should succeed, but timing shows stress
    pretty_assertions::assert_eq!(claims, 1, "Multiple workers claimed single event");

    Ok(())
}

#[sinex_test]
async fn test_concurrent_metadata_lost_update(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

    // Insert test event
    let event = events::adversarial_test_event(
        "metadata.test",
        serde_json::json!({
            "counter": 0,
            "updates": []
        }),
    );

    let inserted = queries::insert_event(pool, &event).await?;
    let event_id = inserted.id;

    // 10 concurrent updates
    let mut handles = vec![];

    for i in 0..10 {
        let pool = pool.clone();
        let id = event_id.clone();

        let handle = tokio::spawn(async move {
            // Read current value
            let current = sqlx::query!(
                "SELECT payload FROM raw.events WHERE id::uuid = $1::uuid",
                id.to_uuid()
            )
            .fetch_one(&pool)
            .await
            .expect("Database query failed");

            // Simulate processing time
            tokio::task::yield_now().await;

            // Update based on read value (classic lost update)
            let mut payload = current.payload;
            payload["counter"] = serde_json::json!(payload["counter"].as_i64().unwrap_or(0) + 1);
            payload["updates"]
                .as_array_mut()
                .unwrap()
                .push(serde_json::json!(i));

            sqlx::query!(
                "UPDATE raw.events SET payload = $2 WHERE id::uuid = $1::uuid",
                id.to_uuid(),
                payload
            )
            .execute(&pool)
            .await
            .expect("Database update failed");
        });

        handles.push(handle);
    }

    futures::future::join_all(handles).await;

    // Check final state
    let final_state = sqlx::query!(
        "SELECT payload FROM raw.events WHERE id::uuid = $1::uuid",
        event_id.to_uuid()
    )
    .fetch_one(pool)
    .await?;

    let counter = final_state.payload["counter"].as_i64().unwrap_or(0);
    let updates = final_state.payload["updates"].as_array().unwrap().len();

    println!("Final counter: {} (expected: 10)", counter);
    println!("Update array length: {} (expected: 10)", updates);

    // Lost updates likely occurred
    pretty_assertions::assert_eq!(counter, 10, "Lost updates detected!");
    pretty_assertions::assert_eq!(updates, 10, "Lost update records!");

    Ok(())
}
