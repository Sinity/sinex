//! sinex-k22c regression coverage: `core.tagged_items` has no FK to
//! `core.events` (its `item_id` column is polymorphic), so it is NOT covered
//! by `core.fn_archive_before_delete` the way annotations/embeddings are.
//! `execute_cascade_archive`/`execute_cascade_archive_in_tx` explicitly copy
//! tagged rows into `audit.archived_tagged_items` and delete them from
//! `core.tagged_items` before the event delete. This is exactly the
//! mechanism `crate/sinexd/src/api/lifecycle_ttl.rs`'s TTL sweep now routes
//! through (sinex-k22c) instead of a raw `DELETE FROM core.events` that
//! bypassed it and left dangling `tagged_items` rows.
//!
//! Production dependency exercised: `EventRepository::execute_cascade_archive`
//! (`crate/sinex-db/src/repositories/events/persistence.rs`). Reverting the
//! tagged_items copy-then-delete block in `execute_cascade_archive_in_tx` (or
//! reverting `lifecycle_ttl.rs`'s TTL sweep back to a raw event DELETE that
//! bypasses this repository method entirely) makes this test fail: the
//! `audit.archived_tagged_items` row would never appear, and/or the
//! `core.tagged_items` row would survive the archive as a dangling reference
//! to an event no longer in `core.events`.

use sinex_db::DbPoolExt;
use sinex_primitives::Timestamp;
use sinex_primitives::domain::RecordedPath;
use sinex_primitives::events::{EventPayload, payloads::FileCreatedPayload};
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn cascade_archive_preserves_and_removes_tagged_items(ctx: TestContext) -> TestResult<()> {
    let material_id = ctx
        .create_source_material(Some("k22c-tagged-items-material"))
        .await?;

    let event = ctx
        .pool()
        .events()
        .insert(
            FileCreatedPayload {
                path: RecordedPath::from_observed("/tmp/k22c-tagged-items.txt")
                    .map_err(|e| color_eyre::eyre::eyre!(e))?,
                size: 42,
                created_at: Timestamp::now(),
                permissions: None,
            }
            .from_material(material_id)
            .build()?,
        )
        .await?;
    let event_id = event.id.expect("inserted event should have an id");

    // Create a tag and attach it to the event -- this is the exact shape
    // core.tagged_items rows take in production (item_type = "event").
    let tag_id: sinex_primitives::Uuid = sqlx::query_scalar(
        "INSERT INTO core.tags (name) VALUES ($1) ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name RETURNING id",
    )
    .bind(format!("k22c-test-tag-{event_id}"))
    .fetch_one(ctx.pool())
    .await?;

    sqlx::query(
        "INSERT INTO core.tagged_items (tag_id, item_id, item_type) VALUES ($1, $2, 'event')",
    )
    .bind(tag_id)
    .bind(*event_id.as_uuid())
    .execute(ctx.pool())
    .await?;

    // Sanity: the tagged_items row exists before archive.
    let live_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM core.tagged_items WHERE tag_id = $1 AND item_id = $2",
    )
    .bind(tag_id)
    .bind(*event_id.as_uuid())
    .fetch_one(ctx.pool())
    .await?;
    assert_eq!(live_count, 1, "fixture setup: tagged_items row must exist pre-archive");

    let archive_operation_id = sinex_primitives::Uuid::now_v7().to_string();
    ctx.pool()
        .events()
        .execute_cascade_archive(
            &[*event_id.as_uuid()],
            "sinex-k22c tagged_items regression test",
            &archive_operation_id,
            "test",
        )
        .await?;

    // The live tagged_items row must be gone -- it has no FK to core.events,
    // so nothing else would clean it up, and a dangling row here is exactly
    // what the raw-DELETE bypass this bead fixes used to leave behind.
    let live_after: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM core.tagged_items WHERE tag_id = $1 AND item_id = $2",
    )
    .bind(tag_id)
    .bind(*event_id.as_uuid())
    .fetch_one(ctx.pool())
    .await?;
    assert_eq!(
        live_after, 0,
        "tagged_items row must be removed from the live table by cascade archive"
    );

    // The archived copy must exist for restore to read back.
    let archived_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM audit.archived_tagged_items WHERE tag_id = $1 AND item_id = $2",
    )
    .bind(tag_id)
    .bind(*event_id.as_uuid())
    .fetch_one(ctx.pool())
    .await?;
    assert_eq!(
        archived_count, 1,
        "tagged_items row must be copied into audit.archived_tagged_items before removal, \
         so core.execute_cascade_restore can read it back on restore"
    );

    Ok(())
}
