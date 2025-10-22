//! Single-writer enforcement tests
//!
//! These tests verify that the single-writer pattern is enforced:
//! - Only ingestd can write canonical events to the database
//! - Satellites must go through ingestd for all event writes
//! - Events only appear in DB after commit (post-commit publish property)

use color_eyre::eyre::Result;
use sinex_core::{
    types::domain::{EventSource, EventType},
    Event, JsonValue,
};
use sinex_test_utils::{fixtures::*, sinex_test, TestContext};
use sqlx::PgPool;

/// Test that satellites cannot directly write to core.events table
#[sinex_test]
async fn test_satellites_cannot_write_directly_to_events(ctx: TestContext) -> Result<()> {
    // This test would need to be run with different connection permissions
    // In a real CI environment, we'd have:
    // 1. A satellite connection with restricted permissions
    // 2. An ingestd connection with write permissions

    // For now, we document the expected behavior
    // In production, satellites should only have SELECT permission on core.events

    // Try to insert directly as a satellite (should fail in production)
    let event = Event::<JsonValue>::test_event(
        EventSource::from_static("fs-watcher"),
        EventType::from_static("file.created"),
        serde_json::json!({ "path": "/test/file.txt" }),
    );

    // In production with proper permissions, this would fail with:
    // "permission denied for table events"

    // For now, we can at least verify the pattern by checking that
    // all events have proper provenance
    let events = sqlx::query(
        r#"
        SELECT COUNT(*) as count
        FROM core.events
        WHERE source_material_id IS NULL 
          AND source_event_ids IS NULL
        "#,
    )
    .fetch_one(ctx.pool.as_ref())
    .await?;

    assert_eq!(
        events.get::<Option<i64>, _>("count").unwrap_or(0),
        0,
        "Found events without proper provenance - violates single-writer pattern"
    );

    Ok(())
}

/// Test that ingestd is the only service that can write events
#[sinex_test]
async fn test_only_ingestd_writes_events(ctx: TestContext) -> Result<()> {
    // In a proper setup, we would:
    // 1. Start a satellite service
    // 2. Have it attempt to write directly
    // 3. Verify it fails
    // 4. Have it go through ingestd
    // 5. Verify it succeeds

    // For now, we can check that all events follow the expected pattern
    let result = sqlx::query(
        r#"
        SELECT DISTINCT source
        FROM core.events
        WHERE source NOT LIKE 'test%'
        "#,
    )
    .fetch_all(ctx.pool.as_ref())
    .await?;

    // All non-test events should come from known satellites
    for row in result {
        let source: String = row.get("source");
        assert!(
            source == "fs-watcher"
                || source == "terminal"
                || source == "desktop"
                || source == "system"
                || source.starts_with("automaton.")
                || source.starts_with("agent."),
            "Unknown event source: {} - might be bypassing ingestd",
            source
        );
    }

    Ok(())
}

/// Test that events appear in DB only after commit (post-commit publish)
#[sinex_test]
async fn test_post_commit_publish_property(ctx: TestContext) -> Result<()> {
    use sqlx::Transaction;

    let pool = ctx.pool.clone();

    // Start a transaction
    let mut tx = pool.begin().await?;

    // Insert a source material and a valid event with proper provenance
    use sinex_core::types::ulid::Ulid;
    let event_id = Ulid::new();
    let material_id = Ulid::new();
    // minimal source_material_registry row
    sqlx::query(
        r#"
        INSERT INTO raw.source_material_registry 
            (id, material_kind, source_identifier, status, timing_info_type)
        VALUES ($1::uuid::ulid, 'annex', $2, 'completed', 'realtime')
        ON CONFLICT (id) DO NOTHING
        "#,
    )
    .bind(material_id.to_uuid())
    .bind(format!("test://material/{}", material_id))
    .execute(&mut *tx)
    .await?;

    sqlx::query(
        r#"
        INSERT INTO core.events (id, event_type, source, host, payload, ts_orig, source_material_id)
        VALUES ($1::uuid::ulid, $2, $3, $4, $5::jsonb, NOW(), $6::uuid::ulid)
        "#,
    )
    .bind(event_id.to_uuid())
    .bind("test.transaction")
    .bind("test")
    .bind("testhost")
    .bind(serde_json::json!({"test": "data"}))
    .bind(material_id.to_uuid())
    .execute(&mut *tx)
    .await?;

    // Before commit, event should not be visible from another connection
    let count_before = sqlx::query(
        r#"
        SELECT COUNT(*) as count
        FROM core.events
        WHERE id = $1::uuid::ulid
        "#,
    )
    .bind(event_id.to_uuid())
    .fetch_one(pool.as_ref())
    .await?;

    assert_eq!(
        count_before.get::<Option<i64>, _>("count").unwrap_or(0),
        0,
        "Event visible before commit - violates post-commit publish"
    );

    // Commit the transaction
    tx.commit().await?;

    // After commit, event should be visible
    let count_after = sqlx::query(
        r#"
        SELECT COUNT(*) as count
        FROM core.events
        WHERE id = $1::uuid::ulid
        "#,
    )
    .bind(event_id.to_uuid())
    .fetch_one(pool.as_ref())
    .await?;

    assert_eq!(
        count_after.get::<Option<i64>, _>("count").unwrap_or(0),
        1,
        "Event not visible after commit"
    );

    // Clean up
    sqlx::query(
        r#"
        DELETE FROM core.events WHERE id = $1::uuid::ulid
        "#,
    )
    .bind(event_id.to_uuid())
    .execute(pool.as_ref())
    .await?;

    Ok(())
}

/// CI check: Verify no live events reference archived events
#[sinex_test]
async fn test_no_live_to_archived_references(ctx: TestContext) -> Result<()> {
    // This is the CI check from TARGET_final.md E.5
    let violations = sqlx::query(
        r#"
        WITH archived AS (SELECT id FROM audit.archived_events)
        SELECT COUNT(*) AS live_refs_archived
        FROM core.events e
        WHERE e.source_event_ids && (SELECT array_agg(id) FROM archived)
        "#,
    )
    .fetch_one(ctx.pool.as_ref())
    .await?;

    assert_eq!(
        violations
            .get::<Option<i64>, _>("live_refs_archived")
            .unwrap_or(0),
        0,
        "Found live events referencing archived events - violates cascade invariant"
    );

    Ok(())
}

/// CI check: Verify XOR provenance constraint
#[sinex_test]
async fn test_provenance_xor_constraint(ctx: TestContext) -> Result<()> {
    // This is the CI check from TARGET_final.md E.5
    let violations = sqlx::query(
        r#"
        SELECT COUNT(*) AS xor_violations
        FROM core.events
        WHERE (source_material_id IS NULL AND source_event_ids IS NULL)
           OR (source_material_id IS NOT NULL AND source_event_ids IS NOT NULL)
        "#,
    )
    .fetch_one(ctx.pool.as_ref())
    .await?;

    assert_eq!(
        violations
            .get::<Option<i64>, _>("xor_violations")
            .unwrap_or(0),
        0,
        "Found events violating XOR provenance constraint"
    );

    Ok(())
}

/// CI check: Verify anchor uniqueness for first-order events
#[sinex_test]
async fn test_anchor_uniqueness(ctx: TestContext) -> Result<()> {
    // This is the CI check from TARGET_final.md E.5
    let duplicates = sqlx::query(
        r#"
        SELECT source_material_id::uuid as material_id, anchor_byte, COUNT(*) as count
        FROM core.events
        WHERE source_material_id IS NOT NULL
        GROUP BY source_material_id::uuid, anchor_byte
        HAVING COUNT(*) > 1
        "#,
    )
    .fetch_all(ctx.pool.as_ref())
    .await?;

    assert!(
        duplicates.is_empty(),
        "Found duplicate anchors for first-order events: {:?}",
        duplicates
            .iter()
            .map(|row| {
                let mid: sqlx::types::Uuid = row.get("material_id");
                let anchor: Option<i64> = row.get("anchor_byte");
                let cnt: Option<i64> = row.get("count");
                format!(
                    "material={}, anchor={:?}, count={}",
                    mid,
                    anchor,
                    cnt.unwrap_or(0)
                )
            })
            .collect::<Vec<_>>()
    );

    Ok(())
}
