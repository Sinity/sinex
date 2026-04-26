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
