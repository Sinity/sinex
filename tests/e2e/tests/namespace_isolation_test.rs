//! Namespace isolation tests.
//!
//! Verifies that events published through one pipeline scope's namespace
//! do not leak into another scope's view.

use sinex_primitives::DynamicPayload;
use xtask::sandbox::prelude::*;

/// Two independent pipeline scopes should not see each other's events.
/// Each scope gets its own NATS namespace and ingestd consumer, so event
/// counts observed via `wait_for_event_count` must be scope-local.
#[sinex_test]
#[ignore = "requires multi-namespace infrastructure"]
async fn pipeline_namespace_subjects_are_isolated(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;

    // Create two independent pipeline scopes
    let scope_a = ctx.pipeline().await?;
    let scope_b = ctx.pipeline().await?;

    // Publish 5 events into scope A
    let a_count = 5usize;
    for i in 0..a_count {
        scope_a
            .publish(DynamicPayload::new(
                "namespace-a",
                "isolation.scope_a",
                json!({"seq": i, "scope": "a"}),
            ))
            .await?;
    }

    // Publish 3 events into scope B
    let b_count = 3usize;
    for i in 0..b_count {
        scope_b
            .publish(DynamicPayload::new(
                "namespace-b",
                "isolation.scope_b",
                json!({"seq": i, "scope": "b"}),
            ))
            .await?;
    }

    // Each scope should see only its own events
    scope_a.wait_for_event_count(a_count).await?;
    scope_b.wait_for_event_count(b_count).await?;

    // Verify via database queries -- events are actually separate
    let source_a = sinex_primitives::EventSource::from("namespace-a");
    let source_b = sinex_primitives::EventSource::from("namespace-b");

    let count_a = ctx.pool.events().count_by_source(&source_a).await?;
    let count_b = ctx.pool.events().count_by_source(&source_b).await?;

    assert_eq!(
        count_a, a_count as i64,
        "scope A should have {a_count} events"
    );
    assert_eq!(
        count_b, b_count as i64,
        "scope B should have {b_count} events"
    );

    scope_a.shutdown().await?;
    scope_b.shutdown().await?;
    Ok(())
}
