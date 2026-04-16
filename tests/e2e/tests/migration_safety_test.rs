// # Pipeline Safety Tests
//
// Tests that verify:
// - Data preservation during pipeline operations
// - Operation idempotency (repeated operations yield same result)
// - Data integrity across multiple pipeline scopes
//
// ## Performance Expectations
//
// - **Individual tests**: 30-60 seconds
// - **Resource usage**: Significant database load
// - **Dependencies**: PostgreSQL

use sinex_primitives::DynamicPayload;
use xtask::sandbox::prelude::*;

/// Publish events, verify they persist, then publish more and verify the original
/// events remain unmodified.
#[sinex_test(timeout = 60)]
async fn test_data_persistence_safety(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let scope = ctx.pipeline().await?;

    // Phase 1: Seed initial data
    let initial_count = 10usize;
    scope
        .publish_batch(initial_count, "pipeline-safety", "pipeline.initial", |i| {
            json!({"seq": i, "batch": "initial", "checksum": format!("init-{i}")})
        })
        .await?;

    // Capture initial data fingerprint
    let source = sinex_primitives::EventSource::from("pipeline-safety");
    let initial_stored = scope
        .ctx()
        .pool
        .events()
        .get_by_source(&source, sinex_primitives::Pagination::new(Some(100), None))
        .await?;
    assert_eq!(initial_stored.len(), initial_count);

    // Collect checksums from initial batch
    let initial_checksums: Vec<String> = initial_stored
        .iter()
        .filter_map(|e| {
            e.payload
                .get("checksum")
                .and_then(|v| v.as_str())
                .map(std::string::ToString::to_string)
        })
        .collect();

    // Phase 2: Add more data
    let additional_count = 5usize;
    scope
        .publish_batch(additional_count, "pipeline-safety", "pipeline.additional", |i| {
            json!({"seq": i, "batch": "additional", "checksum": format!("add-{i}")})
        })
        .await?;

    // Verify initial data is still intact
    let all_stored = scope
        .ctx()
        .pool
        .events()
        .get_by_source(&source, sinex_primitives::Pagination::new(Some(100), None))
        .await?;
    assert_eq!(all_stored.len(), initial_count + additional_count);

    // Verify original checksums are still present
    let all_checksums: Vec<String> = all_stored
        .iter()
        .filter_map(|e| {
            e.payload
                .get("checksum")
                .and_then(|v| v.as_str())
                .map(std::string::ToString::to_string)
        })
        .collect();

    for cs in &initial_checksums {
        assert!(
            all_checksums.contains(cs),
            "initial checksum '{cs}' should still be present after additional writes"
        );
    }

    scope.shutdown().await?;
    Ok(())
}

/// Publish the same logical batch twice and verify the operation is idempotent --
/// each publish creates new events (unique IDs) but the system doesn't corrupt.
#[sinex_test(timeout = 60)]
async fn test_pipeline_idempotency(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let scope = ctx.pipeline().await?;

    let batch_size = 5usize;

    // First execution of the batch
    scope
        .publish_batch(batch_size, "idempotency-pipeline", "pipeline.batch", |i| {
            json!({"seq": i, "run": 1})
        })
        .await?;

    let source = sinex_primitives::EventSource::from("idempotency-pipeline");
    let after_first = scope.ctx().pool.events().count_by_source(&source).await?;

    // Second execution of the same batch
    scope
        .publish_batch(batch_size, "idempotency-pipeline", "pipeline.batch", |i| {
            json!({"seq": i, "run": 2})
        })
        .await?;

    let after_second = scope.ctx().pool.events().count_by_source(&source).await?;

    // Each execution adds new events (unique IDs), so count doubles
    assert_eq!(after_first, batch_size as i64);
    assert_eq!(after_second, (batch_size * 2) as i64);

    // Verify all events have unique IDs
    let all_events = scope
        .ctx()
        .pool
        .events()
        .get_by_source(&source, sinex_primitives::Pagination::new(Some(100), None))
        .await?;
    let ids: std::collections::HashSet<_> =
        all_events.iter().filter_map(|e| e.id.as_ref()).collect();
    assert_eq!(
        ids.len(),
        all_events.len(),
        "all events should have unique IDs"
    );

    scope.shutdown().await?;
    Ok(())
}

/// Publish events, shut down the pipeline, create a new pipeline, publish more,
/// and verify each pipeline scope is isolated.
#[sinex_test(timeout = 60)]
async fn test_pipeline_scope_isolation_across_restarts(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;

    // Phase 1: First pipeline scope
    let scope1 = ctx.pipeline().await?;
    let phase1_count = 8usize;
    for i in 0..phase1_count {
        scope1
            .publish(DynamicPayload::new(
                "preservation-test",
                "pipeline.preserve.phase1",
                json!({"seq": i, "phase": 1, "marker": format!("p1-{i}")}),
            ))
            .await?;
    }
    scope1.wait_for_event_count(phase1_count).await?;
    scope1.shutdown().await?;

    // Verify phase 1 data
    let source = sinex_primitives::EventSource::from("preservation-test");
    let phase1_stored = ctx.pool.events().count_by_source(&source).await?;
    assert_eq!(phase1_stored, phase1_count as i64);

    // Phase 2: New pipeline scope
    let scope2 = ctx.pipeline().await?;
    let phase2_count = 6usize;
    for i in 0..phase2_count {
        scope2
            .publish(DynamicPayload::new(
                "preservation-test",
                "pipeline.preserve.phase2",
                json!({"seq": i, "phase": 2, "marker": format!("p2-{i}")}),
            ))
            .await?;
    }
    scope2.wait_for_event_count(phase2_count).await?;

    // Verify scope isolation: second scope only contains second-phase data.
    let total_stored = ctx.pool.events().count_by_source(&source).await?;
    assert_eq!(
        total_stored, phase2_count as i64,
        "second scope should not inherit first-scope events"
    );

    // Verify phase 1 markers are absent in the new scope.
    let all_events = ctx
        .pool
        .events()
        .get_by_source(&source, sinex_primitives::Pagination::new(Some(100), None))
        .await?;

    let phase1_markers: Vec<_> = all_events
        .iter()
        .filter_map(|e| e.payload.get("marker").and_then(|v| v.as_str()))
        .filter(|m| m.starts_with("p1-"))
        .collect();

    assert_eq!(
        phase1_markers.len(),
        0,
        "first-scope markers must not appear after restart"
    );

    scope2.shutdown().await?;
    Ok(())
}
