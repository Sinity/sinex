//! Self-tests for `TestCoreStack` — verifies the composite test fixture itself.
//!
//! These tests prove:
//! 1. Stack startup (NATS + ingestd + gateway) completes without error
//! 2. Gateway is actually accepting TCP connections
//! 3. Material/ledger seeding helpers produce valid, queryable data
//! 4. Events seeded through the stack have correct provenance

use sinex_db::DbPoolExt;
use xtask::sandbox::prelude::*;

/// Verify that `TestCoreStack::new` starts all three services and exposes
/// a reachable gateway endpoint.
#[sinex_test(timeout = 120)]
async fn core_stack_starts_and_gateway_accepts_tcp(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let stack = TestCoreStack::new(&ctx).await?;

    // Gateway should be listening — verify by TCP connect
    let addr = stack.gateway_addr();
    let stream = tokio::net::TcpStream::connect(addr).await?;
    drop(stream);

    // RPC URL should be well-formed
    let url = stack.rpc_url();
    assert!(
        url.starts_with("https://127.0.0.1:"),
        "unexpected RPC URL: {url}"
    );
    assert!(url.ends_with("/rpc"), "RPC URL should end with /rpc: {url}");

    // Token should match default
    assert_eq!(stack.rpc_token(), TEST_RPC_TOKEN);

    stack.shutdown().await?;
    Ok(())
}

/// Verify that `seed_material_with_ledger` creates both the source material
/// registry entry and the temporal ledger row.
#[sinex_test(timeout = 120)]
async fn seed_material_with_ledger_creates_valid_records(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let stack = TestCoreStack::new(&ctx).await?;

    let material_id = stack
        .seed_material_with_ledger("test-source", "realtime_capture", (0, 1000))
        .await?;

    // Verify source material exists
    let row = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM raw.source_material_registry WHERE id = $1",
    )
    .bind(material_id.to_uuid())
    .fetch_one(stack.pool())
    .await?;
    assert_eq!(row, 1, "source material should exist");

    // Verify temporal ledger row
    let ledger_row = sqlx::query_as::<_, (i64, i64, String)>(
        "SELECT offset_start, offset_end, source_type FROM raw.temporal_ledger WHERE source_material_id = $1",
    )
    .bind(material_id.to_uuid())
    .fetch_one(stack.pool())
    .await?;
    assert_eq!(ledger_row.0, 0, "offset_start");
    assert_eq!(ledger_row.1, 1000, "offset_end");
    assert_eq!(ledger_row.2, "realtime_capture", "source_type");

    stack.shutdown().await?;
    Ok(())
}

/// Verify that `seed_material_with_events` creates events with correct provenance
/// that points back to the seeded material.
#[sinex_test(timeout = 120)]
async fn seed_material_with_events_creates_provenance_chain(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let stack = TestCoreStack::new(&ctx).await?;

    let (material_id, event_ids) = stack
        .seed_material_with_events("test-seeder", "test.seeded", 3)
        .await?;

    assert_eq!(event_ids.len(), 3);

    // Verify events exist and have correct source/type
    let typed_ids: Vec<Id<Event>> = event_ids.clone();
    let events = stack.pool().events().get_by_ids(&typed_ids).await?;
    assert_eq!(events.len(), 3, "all seeded events should be persisted");

    let expected_source = EventSource::new("test-seeder")?;
    let expected_type = EventType::new("test.seeded")?;
    for event in &events {
        assert_eq!(event.source, expected_source);
        assert_eq!(event.event_type, expected_type);
        // scope_key and equivalence_key should be set
        assert!(
            event.scope_key.is_some(),
            "seeded events should have scope_key"
        );
        assert!(
            event.equivalence_key.is_some(),
            "seeded events should have equivalence_key"
        );
    }

    // Verify temporal ledger exists for the material
    let ledger_count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM raw.temporal_ledger WHERE source_material_id = $1",
    )
    .bind(material_id.to_uuid())
    .fetch_one(stack.pool())
    .await?;
    assert_eq!(
        ledger_count, 1,
        "material should have exactly one ledger entry"
    );

    stack.shutdown().await?;
    Ok(())
}
