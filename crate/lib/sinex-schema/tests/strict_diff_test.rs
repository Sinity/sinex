//! Integration tests for the strict-drift detector (issue #556).
//!
//! These tests apply the schema, then deliberately mutate the live DB to
//! introduce drift in each detected category, and verify the strict diff
//! catches it. The mutations are reverted at test exit by the per-test
//! database isolation in `xtask::sandbox`.

use sinex_schema::strict_diff::{DriftCategory, check_strict};
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn check_strict_returns_empty_after_apply(ctx: TestContext) -> TestResult<()> {
    sinex_schema::apply::apply(&ctx.pool).await?;

    let drifts = check_strict(&ctx.pool).await?;
    assert!(
        drifts.is_empty(),
        "strict drift must be empty on a freshly applied schema: {drifts:?}"
    );

    Ok(())
}

#[sinex_test]
async fn detects_dropped_default_on_existing_column(ctx: TestContext) -> TestResult<()> {
    sinex_schema::apply::apply(&ctx.pool).await?;

    // Drop the DEFAULT on `core.events.ts_persisted` — convergence does not
    // re-add it because `ADD COLUMN IF NOT EXISTS` is a no-op for existing
    // columns. The strict diff must catch this.
    sqlx::query("ALTER TABLE core.events ALTER COLUMN ts_persisted DROP DEFAULT")
        .execute(&ctx.pool)
        .await?;

    let drifts = check_strict(&ctx.pool).await?;
    let matched: Vec<_> = drifts
        .iter()
        .filter(|d| {
            d.category == DriftCategory::ColumnDefault
                && d.location == "core.events.ts_persisted"
        })
        .collect();

    assert_eq!(
        matched.len(),
        1,
        "expected exactly one column_default drift on ts_persisted, got: {drifts:?}"
    );
    assert!(
        matched[0].observed_summary.contains("no DEFAULT")
            || matched[0].observed_summary.is_empty(),
        "observed summary should reflect the dropped default: {}",
        matched[0].observed_summary
    );

    Ok(())
}

#[sinex_test]
async fn detects_replaced_default_on_existing_column(ctx: TestContext) -> TestResult<()> {
    sinex_schema::apply::apply(&ctx.pool).await?;

    // Replace the declared `CURRENT_TIMESTAMP` default with a constant;
    // the marker check should fire because the live expression no longer
    // contains `CURRENT_TIMESTAMP`.
    sqlx::query(
        "ALTER TABLE core.events
            ALTER COLUMN ts_persisted SET DEFAULT '2000-01-01T00:00:00Z'::timestamptz",
    )
    .execute(&ctx.pool)
    .await?;

    let drifts = check_strict(&ctx.pool).await?;
    let matched: Vec<_> = drifts
        .iter()
        .filter(|d| {
            d.category == DriftCategory::ColumnDefault
                && d.location == "core.events.ts_persisted"
        })
        .collect();

    assert_eq!(
        matched.len(),
        1,
        "expected exactly one column_default drift on ts_persisted, got: {drifts:?}"
    );
    assert!(
        matched[0].observed_summary.contains("2000-01-01"),
        "observed summary should expose the replaced literal default: {}",
        matched[0].observed_summary
    );

    // Restore the original DEFAULT so this slot is not contaminated for the
    // `check_strict_returns_empty_after_apply` test, which runs in the same
    // sandbox DB pool.  DDL mutations persist across the pool's data-cleaning
    // pass, so we must undo the structural change explicitly here.
    sqlx::query(
        "ALTER TABLE core.events ALTER COLUMN ts_persisted SET DEFAULT CURRENT_TIMESTAMP",
    )
    .execute(&ctx.pool)
    .await?;

    Ok(())
}

#[sinex_test]
async fn detects_manual_edit_to_trigger_function_body(
    ctx: TestContext,
) -> TestResult<()> {
    sinex_schema::apply::apply(&ctx.pool).await?;

    // Replace `core.expand_cascade` with a stub that drops the
    // RAISE EXCEPTION refusal. This is exactly the silent-edit failure
    // mode that motivated #556 — convergence won't notice on the next
    // apply (it's a CREATE OR REPLACE that would silently overwrite
    // back), but strict diff catches the drift now.
    sqlx::query(
        r"
        CREATE OR REPLACE FUNCTION core.expand_cascade(temp_table TEXT, max_depth INTEGER)
        RETURNS INTEGER AS $$
        BEGIN
            RETURN 0;
        END;
        $$ LANGUAGE plpgsql
        ",
    )
    .execute(&ctx.pool)
    .await?;

    let drifts = check_strict(&ctx.pool).await?;
    let matched: Vec<_> = drifts
        .iter()
        .filter(|d| {
            d.category == DriftCategory::TriggerBody
                && d.location == "core.expand_cascade"
        })
        .collect();

    assert_eq!(
        matched.len(),
        1,
        "expected exactly one trigger_body drift on expand_cascade, got: {drifts:?}"
    );
    assert!(
        matched[0].observed_summary.contains("RAISE EXCEPTION")
            || matched[0].observed_summary.contains("max depth"),
        "observed summary should name the missing markers: {}",
        matched[0].observed_summary
    );

    Ok(())
}

#[sinex_test]
async fn detects_dropped_inline_check_on_events(ctx: TestContext) -> TestResult<()> {
    sinex_schema::apply::apply(&ctx.pool).await?;

    // Find and drop the XOR provenance CHECK. There is no stable name —
    // discover it via pg_constraint, then drop it. This simulates an
    // operator running ALTER TABLE ... DROP CONSTRAINT manually.
    let constraint_name: String = sqlx::query_scalar(
        r"
        SELECT c.conname::text
        FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = 'core'
          AND t.relname = 'events'
          AND c.contype = 'c'
          AND pg_get_constraintdef(c.oid) LIKE '%source_material_id IS NOT NULL%'
          AND pg_get_constraintdef(c.oid) LIKE '%source_event_ids IS NULL%'
        LIMIT 1
        ",
    )
    .fetch_one(&ctx.pool)
    .await?;

    sqlx::query(&format!(
        "ALTER TABLE core.events DROP CONSTRAINT {constraint_name}"
    ))
    .execute(&ctx.pool)
    .await?;

    let drifts = check_strict(&ctx.pool).await?;
    let matched: Vec<_> = drifts
        .iter()
        .filter(|d| {
            d.category == DriftCategory::InlineCheckExpr
                && d.location == "core.events::xor_provenance"
        })
        .collect();

    assert_eq!(
        matched.len(),
        1,
        "expected exactly one inline_check_expr drift on xor_provenance, got: {drifts:?}"
    );

    Ok(())
}

#[sinex_test]
async fn detects_changed_foreign_key_action(ctx: TestContext) -> TestResult<()> {
    sinex_schema::apply::apply(&ctx.pool).await?;

    // The TaggedItems(tag_id) FK declares ON DELETE CASCADE. Drop it and
    // re-add with NO ACTION to simulate manual drift.
    let constraint_name: String = sqlx::query_scalar(
        r"
        SELECT c.conname::text
        FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = 'core'
          AND t.relname = 'tagged_items'
          AND c.contype = 'f'
          AND pg_get_constraintdef(c.oid) LIKE '%FOREIGN KEY (tag_id)%'
        LIMIT 1
        ",
    )
    .fetch_one(&ctx.pool)
    .await?;

    sqlx::query(&format!(
        "ALTER TABLE core.tagged_items DROP CONSTRAINT {constraint_name}"
    ))
    .execute(&ctx.pool)
    .await?;

    sqlx::query(
        "ALTER TABLE core.tagged_items
            ADD CONSTRAINT tagged_items_tag_id_drift_fkey
            FOREIGN KEY (tag_id) REFERENCES core.tags(id) ON DELETE NO ACTION",
    )
    .execute(&ctx.pool)
    .await?;

    let drifts = check_strict(&ctx.pool).await?;
    let matched: Vec<_> = drifts
        .iter()
        .filter(|d| {
            d.category == DriftCategory::ForeignKeyAction
                && d.location.contains("tagged_items")
                && d.location.contains("tag_id")
        })
        .collect();

    assert_eq!(
        matched.len(),
        1,
        "expected exactly one foreign_key_action drift on tagged_items(tag_id), got: {drifts:?}"
    );
    // Postgres normalizes `ON DELETE NO ACTION` to no clause at all in
    // pg_get_constraintdef, since NO ACTION is the SQL default. The drift
    // shows up as the CASCADE marker missing from the observed summary.
    assert!(
        !matched[0].observed_summary.contains("ON DELETE CASCADE"),
        "observed summary should no longer contain CASCADE: {}",
        matched[0].observed_summary
    );

    Ok(())
}

#[sinex_test]
async fn detects_changed_tags_parent_foreign_key_action(ctx: TestContext) -> TestResult<()> {
    sinex_schema::apply::apply(&ctx.pool).await?;

    // The tags(parent_tag_id) self-FK is installed by a raw schema fixup
    // because the sea-query self-reference path used to emit CASCADE
    // instead of SET NULL. Simulate that regression.
    sqlx::query("ALTER TABLE core.tags DROP CONSTRAINT tags_parent_tag_id_fkey")
        .execute(&ctx.pool)
        .await?;

    sqlx::query(
        "ALTER TABLE core.tags
            ADD CONSTRAINT tags_parent_tag_id_fkey
            FOREIGN KEY (parent_tag_id) REFERENCES core.tags(id) ON DELETE CASCADE",
    )
    .execute(&ctx.pool)
    .await?;

    let drifts = check_strict(&ctx.pool).await?;
    let matched: Vec<_> = drifts
        .iter()
        .filter(|d| {
            d.category == DriftCategory::ForeignKeyAction
                && d.location.contains("tags")
                && d.location.contains("parent_tag_id")
        })
        .collect();

    assert_eq!(
        matched.len(),
        1,
        "expected exactly one foreign_key_action drift on tags(parent_tag_id), got: {drifts:?}"
    );
    assert!(
        matched[0].declared_summary.contains("ON DELETE SET NULL"),
        "declared summary should pin SET NULL: {}",
        matched[0].declared_summary
    );
    assert!(
        matched[0].observed_summary.contains("ON DELETE CASCADE"),
        "observed summary should show the drifted CASCADE action: {}",
        matched[0].observed_summary
    );

    Ok(())
}

#[sinex_test]
async fn detects_changed_hypertable_chunk_interval(ctx: TestContext) -> TestResult<()> {
    sinex_schema::apply::apply(&ctx.pool).await?;

    // Change the chunk interval to 1 day. TimescaleDB exposes
    // `set_chunk_time_interval` for exactly this — operator-facing
    // mutation that strict diff must catch.
    sqlx::query("SELECT set_chunk_time_interval('core.events', INTERVAL '1 day')")
        .execute(&ctx.pool)
        .await?;

    let drifts = check_strict(&ctx.pool).await?;
    let matched: Vec<_> = drifts
        .iter()
        .filter(|d| {
            d.category == DriftCategory::HypertableSetting
                && d.location == "core.events::chunk_interval"
        })
        .collect();

    assert_eq!(
        matched.len(),
        1,
        "expected exactly one hypertable_setting drift on chunk_interval, got: {drifts:?}"
    );

    Ok(())
}
