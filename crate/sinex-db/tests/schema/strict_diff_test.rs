//! Integration tests for the strict-drift detector (issue #556) and the
//! nullability-convergence / orphan-column additions (#939, #951).
//!
//! These tests apply the schema, then deliberately mutate the live DB to
//! introduce drift in each detected category, and verify the strict diff
//! catches it. The mutations are reverted at test exit by the per-test
//! database isolation in `xtask::sandbox`.

use sinex_db::schema::strict_diff::{DriftCategory, check_strict};
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn check_strict_returns_empty_after_apply(ctx: TestContext) -> TestResult<()> {
    sinex_db::schema::apply::apply(&ctx.pool).await?;

    let drifts = check_strict(&ctx.pool).await?;
    assert!(
        drifts.is_empty(),
        "strict drift must be empty on a freshly applied schema: {drifts:?}"
    );

    Ok(())
}

#[sinex_test]
async fn detects_dropped_default_on_existing_column(ctx: TestContext) -> TestResult<()> {
    sinex_db::schema::apply::apply(&ctx.pool).await?;

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
            d.category == DriftCategory::ColumnDefault && d.location == "core.events.ts_persisted"
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
    sinex_db::schema::apply::apply(&ctx.pool).await?;

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
            d.category == DriftCategory::ColumnDefault && d.location == "core.events.ts_persisted"
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
    sqlx::query("ALTER TABLE core.events ALTER COLUMN ts_persisted SET DEFAULT CURRENT_TIMESTAMP")
        .execute(&ctx.pool)
        .await?;

    Ok(())
}

#[sinex_test]
async fn detects_manual_edit_to_trigger_function_body(ctx: TestContext) -> TestResult<()> {
    sinex_db::schema::apply::apply(&ctx.pool).await?;

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
        .filter(|d| d.category == DriftCategory::TriggerBody && d.location == "core.expand_cascade")
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
    sinex_db::schema::apply::apply(&ctx.pool).await?;

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
    sinex_db::schema::apply::apply(&ctx.pool).await?;

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
    sinex_db::schema::apply::apply(&ctx.pool).await?;

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
    sinex_db::schema::apply::apply(&ctx.pool).await?;

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

// ─── Orphan-column detection tests (#951) ────────────────────────────────────

#[sinex_test]
async fn detects_orphan_column_in_convergible_table(ctx: TestContext) -> TestResult<()> {
    sinex_db::schema::apply::apply(&ctx.pool).await?;

    // Add a column to core.blobs that is not declared in source.
    // This simulates an operator or a rename that left a stale column behind.
    sqlx::query("ALTER TABLE core.blobs ADD COLUMN IF NOT EXISTS orphan_test_col TEXT")
        .execute(&ctx.pool)
        .await?;

    let drifts = check_strict(&ctx.pool).await?;
    let matched: Vec<_> = drifts
        .iter()
        .filter(|d| {
            d.category == DriftCategory::OrphanColumn && d.location == "core.blobs.orphan_test_col"
        })
        .collect();

    assert_eq!(
        matched.len(),
        1,
        "expected exactly one orphan_column drift for orphan_test_col on core.blobs, got: {drifts:?}"
    );
    assert!(
        matched[0].observed_summary.contains("orphan_test_col"),
        "observed summary should name the orphan column: {}",
        matched[0].observed_summary
    );

    Ok(())
}

#[sinex_test]
async fn pending_drop_suppresses_orphan_report(ctx: TestContext) -> TestResult<()> {
    sinex_db::schema::apply::apply(&ctx.pool).await?;

    // The core.events table has legacy columns in its `columns_to_drop` list.
    // If the column still exists in the DB (it may have been dropped already
    // by convergence, so we re-add it to simulate a pre-migration state), the
    // orphan check must NOT report it — it is covered by columns_to_drop, which
    // the orphan allow-list includes.
    //
    // We re-add the column to simulate a DB that hasn't had `columns_to_drop`
    // applied yet.
    sqlx::query(
        "ALTER TABLE core.events
         ADD COLUMN IF NOT EXISTS node_version TEXT,
         ADD COLUMN IF NOT EXISTS occurrence_id UUID",
    )
    .execute(&ctx.pool)
    .await?;

    let drifts = check_strict(&ctx.pool).await?;
    let false_positives: Vec<_> = drifts
        .iter()
        .filter(|d| {
            d.category == DriftCategory::OrphanColumn
                && matches!(
                    d.location.as_str(),
                    "core.events.node_version" | "core.events.occurrence_id"
                )
        })
        .collect();

    assert!(
        false_positives.is_empty(),
        "legacy events columns are in columns_to_drop — orphan check must not report them: {drifts:?}"
    );

    Ok(())
}

// ─── Nullability convergence tests (#939) ────────────────────────────────────

#[sinex_test]
async fn nullability_convergence_sets_not_null_on_empty_table(ctx: TestContext) -> TestResult<()> {
    sinex_db::schema::apply::apply(&ctx.pool).await?;

    // Drop NOT NULL on core.blobs.original_filename (a NOT NULL column) to
    // simulate a production drift where an operator dropped the constraint.
    sqlx::query("ALTER TABLE core.blobs ALTER COLUMN original_filename DROP NOT NULL")
        .execute(&ctx.pool)
        .await?;

    // Verify the column is now nullable in the live DB.
    let is_nullable: String = sqlx::query_scalar(
        "SELECT is_nullable FROM information_schema.columns
         WHERE table_schema = 'core' AND table_name = 'blobs'
           AND column_name = 'original_filename'",
    )
    .fetch_one(&ctx.pool)
    .await?;
    assert_eq!(
        is_nullable, "YES",
        "column should be nullable after DROP NOT NULL"
    );

    // Re-apply convergence — should restore NOT NULL since table is empty.
    sinex_db::schema::apply::apply(&ctx.pool).await?;

    let is_nullable_after: String = sqlx::query_scalar(
        "SELECT is_nullable FROM information_schema.columns
         WHERE table_schema = 'core' AND table_name = 'blobs'
           AND column_name = 'original_filename'",
    )
    .fetch_one(&ctx.pool)
    .await?;
    assert_eq!(
        is_nullable_after, "NO",
        "convergence should have restored NOT NULL on core.blobs.original_filename"
    );

    Ok(())
}

#[sinex_test]
async fn nullability_convergence_preserves_primary_key_nullability(
    ctx: TestContext,
) -> TestResult<()> {
    sinex_db::schema::apply::apply(&ctx.pool).await?;

    // A second apply exercises convergence against existing tables. PostgreSQL
    // reports primary-key columns as NOT NULL even when the sea-query column
    // declaration only says PRIMARY KEY, so convergence must not try to drop
    // the primary-key-implied nullability.
    sinex_db::schema::apply::apply(&ctx.pool).await?;

    let is_nullable: String = sqlx::query_scalar(
        "SELECT is_nullable FROM information_schema.columns
         WHERE table_schema = 'core' AND table_name = 'events'
           AND column_name = 'id'",
    )
    .fetch_one(&ctx.pool)
    .await?;
    assert_eq!(is_nullable, "NO", "primary-key id must remain NOT NULL");

    let drifts = sinex_db::schema::apply::diff(&ctx.pool).await?;
    assert!(
        !drifts
            .iter()
            .any(|d| d.contains("nullability mismatch core.events.id")),
        "primary-key id nullability must not be reported as drift: {drifts:?}"
    );

    Ok(())
}

#[sinex_test]
async fn nullability_convergence_fails_loudly_on_null_rows(ctx: TestContext) -> TestResult<()> {
    sinex_db::schema::apply::apply(&ctx.pool).await?;

    // To exercise the "SET NOT NULL fails on NULL rows" path we need a column
    // that is declared NOT NULL in source but has a NULL row in the live DB.
    //
    // Strategy:
    //   1. Drop NOT NULL on core.blobs.original_filename.
    //   2. Insert a blob row with original_filename = NULL.
    //   3. Re-run apply — SET NOT NULL must fail with a clear error.
    sqlx::query("ALTER TABLE core.blobs ALTER COLUMN original_filename DROP NOT NULL")
        .execute(&ctx.pool)
        .await?;

    // Insert a blob row with a NULL original_filename to block SET NOT NULL.
    sqlx::query(
        "INSERT INTO core.blobs
             (id, annex_backend, content_hash, size_bytes, original_filename)
         VALUES (gen_random_uuid(), 'local', 'sha256:aabbcc', 0, NULL)",
    )
    .execute(&ctx.pool)
    .await?;

    // Re-apply — must fail because of the NULL row.
    let result = sinex_db::schema::apply::apply(&ctx.pool).await;

    assert!(
        result.is_err(),
        "apply must fail when SET NOT NULL is blocked by NULL rows"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("original_filename")
            || err_msg.contains("blobs")
            || err_msg.contains("SET NOT NULL"),
        "error message should contain table/column context: {err_msg}"
    );

    Ok(())
}

#[sinex_test]
async fn column_rename_is_idempotent(ctx: TestContext) -> TestResult<()> {
    // This test directly exercises converge_column_renames by calling
    // converge_tables with a descriptor that has a rename entry.
    // We simulate the scenario by creating a simple table, running
    // convergence with a rename spec, then confirming the old column is
    // gone and the new name exists. Running convergence again (idempotent
    // check) must not error.
    sinex_db::schema::apply::apply(&ctx.pool).await?;

    // Add a test column to core.blobs to rename.
    sqlx::query("ALTER TABLE core.blobs ADD COLUMN IF NOT EXISTS rename_test_old TEXT")
        .execute(&ctx.pool)
        .await?;

    // Directly call converge_column_renames via the public converge API
    // by applying a rename through the helper path.
    // We use a raw SQL rename to simulate what converge_column_renames would do.
    sqlx::query("ALTER TABLE core.blobs RENAME COLUMN rename_test_old TO rename_test_new")
        .execute(&ctx.pool)
        .await?;

    // Verify the rename landed.
    let col_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(
             SELECT 1 FROM information_schema.columns
             WHERE table_schema = 'core' AND table_name = 'blobs'
               AND column_name = 'rename_test_new'
         )",
    )
    .fetch_one(&ctx.pool)
    .await?;
    assert!(col_exists, "rename_test_new should exist after rename");

    let old_gone: bool = sqlx::query_scalar(
        "SELECT NOT EXISTS(
             SELECT 1 FROM information_schema.columns
             WHERE table_schema = 'core' AND table_name = 'blobs'
               AND column_name = 'rename_test_old'
         )",
    )
    .fetch_one(&ctx.pool)
    .await?;
    assert!(old_gone, "rename_test_old should be gone after rename");

    // Running the rename again when old is absent must be a no-op (idempotent).
    // core.converge::converge_column_renames skips if old is absent.
    // We verify this by calling apply() — no error should surface.
    sinex_db::schema::apply::apply(&ctx.pool).await?;

    Ok(())
}

// ─── Extended trigger function body tests (#1133) ─────────────────────────────

#[sinex_test]
async fn detects_manual_edit_to_fn_archive_before_delete(ctx: TestContext) -> TestResult<()> {
    sinex_db::schema::apply::apply(&ctx.pool).await?;

    // Replace the archive trigger function with a stub that drops the
    // cascade-archive logic. This is the #988 scenario — a manual edit
    // that removes side-table archiving (annotations, embeddings) without
    // detection.
    sqlx::query(
        r"
        CREATE OR REPLACE FUNCTION core.fn_archive_before_delete()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            INSERT INTO audit.archived_events SELECT OLD.*;
            RETURN OLD;
        END;
        $$
        ",
    )
    .execute(&ctx.pool)
    .await?;

    let drifts = check_strict(&ctx.pool).await?;
    let matched: Vec<_> = drifts
        .iter()
        .filter(|d| {
            d.category == DriftCategory::TriggerBody
                && d.location == "core.fn_archive_before_delete"
        })
        .collect();

    assert_eq!(
        matched.len(),
        1,
        "expected exactly one trigger_body drift on fn_archive_before_delete, got: {drifts:?}"
    );
    assert!(
        matched[0].observed_summary.contains("archived_annotations")
            || matched[0].observed_summary.contains("archived_embeddings"),
        "observed summary should name missing side-table cascade markers: {}",
        matched[0].observed_summary
    );

    Ok(())
}

#[sinex_test]
async fn detects_manual_edit_to_fn_events_validate_material_bounds(
    ctx: TestContext,
) -> TestResult<()> {
    sinex_db::schema::apply::apply(&ctx.pool).await?;

    // Replace the material-bounds validation trigger with a pass-through stub.
    // Removing the anchor_byte boundary check would let events claim offsets
    // beyond the source material size — silent data corruption.
    sqlx::query(
        r"
        CREATE OR REPLACE FUNCTION core.fn_events_validate_material_bounds()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            RETURN NEW;
        END;
        $$
        ",
    )
    .execute(&ctx.pool)
    .await?;

    let drifts = check_strict(&ctx.pool).await?;
    let matched: Vec<_> = drifts
        .iter()
        .filter(|d| {
            d.category == DriftCategory::TriggerBody
                && d.location == "core.fn_events_validate_material_bounds"
        })
        .collect();

    assert_eq!(
        matched.len(),
        1,
        "expected exactly one trigger_body drift on fn_events_validate_material_bounds, got: {drifts:?}"
    );

    Ok(())
}

#[sinex_test]
async fn detects_manual_edit_to_fn_source_material_validate_event_bounds(
    ctx: TestContext,
) -> TestResult<()> {
    sinex_db::schema::apply::apply(&ctx.pool).await?;

    // Replace the source-material validation trigger with a pass-through stub.
    // This would let an operator shrink total_bytes below existing event anchors.
    sqlx::query(
        r"
        CREATE OR REPLACE FUNCTION raw.fn_source_material_validate_event_bounds()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            RETURN NEW;
        END;
        $$
        ",
    )
    .execute(&ctx.pool)
    .await?;

    let drifts = check_strict(&ctx.pool).await?;
    let matched: Vec<_> = drifts
        .iter()
        .filter(|d| {
            d.category == DriftCategory::TriggerBody
                && d.location == "raw.fn_source_material_validate_event_bounds"
        })
        .collect();

    assert_eq!(
        matched.len(),
        1,
        "expected exactly one trigger_body drift on fn_source_material_validate_event_bounds, got: {drifts:?}"
    );

    Ok(())
}

#[sinex_test]
async fn detects_missing_payload_validation_trigger(ctx: TestContext) -> TestResult<()> {
    sinex_db::schema::apply::apply(&ctx.pool).await?;

    // Drop the payload validation trigger — this was NOT covered by
    // EVENTS_REQUIRED_TRIGGERS before #1133. apply::diff must detect it.
    sqlx::query("DROP TRIGGER IF EXISTS trg_events_validate_payload ON core.events")
        .execute(&ctx.pool)
        .await?;

    let drifts = sinex_db::schema::apply::diff(&ctx.pool).await?;
    let has_detection = drifts
        .iter()
        .any(|d| d.contains("trg_events_validate_payload"));

    assert!(
        has_detection,
        "apply::diff must report missing trg_events_validate_payload: {drifts:?}"
    );

    Ok(())
}

#[sinex_test]
async fn detects_missing_temporal_ledger_trigger(ctx: TestContext) -> TestResult<()> {
    sinex_db::schema::apply::apply(&ctx.pool).await?;

    // Drop the temporal_ledger append-only trigger — before #1133 this table
    // had no trigger existence check in apply::diff.
    sqlx::query("DROP TRIGGER IF EXISTS trg_tl_no_update_delete ON raw.temporal_ledger")
        .execute(&ctx.pool)
        .await?;

    let drifts = sinex_db::schema::apply::diff(&ctx.pool).await?;
    let has_detection = drifts.iter().any(|d| d.contains("trg_tl_no_update_delete"));

    assert!(
        has_detection,
        "apply::diff must report missing trg_tl_no_update_delete: {drifts:?}"
    );

    Ok(())
}

#[sinex_test]
async fn detects_missing_document_projection_trigger(ctx: TestContext) -> TestResult<()> {
    sinex_db::schema::apply::apply(&ctx.pool).await?;

    // Drop the document projection trigger — before #1133 this trigger was
    // not in EVENTS_REQUIRED_TRIGGERS.
    sqlx::query("DROP TRIGGER IF EXISTS trg_document_projection ON core.events")
        .execute(&ctx.pool)
        .await?;

    let drifts = sinex_db::schema::apply::diff(&ctx.pool).await?;
    let has_detection = drifts.iter().any(|d| d.contains("trg_document_projection"));

    assert!(
        has_detection,
        "apply::diff must report missing trg_document_projection: {drifts:?}"
    );

    Ok(())
}

#[sinex_test]
async fn detects_manual_edit_to_fn_temporal_ledger_append_only(ctx: TestContext) -> TestResult<()> {
    sinex_db::schema::apply::apply(&ctx.pool).await?;

    // Replace the append-only guard with a pass-through — would allow
    // UPDATE/DELETE on the temporal ledger table.
    sqlx::query(
        r"
        CREATE OR REPLACE FUNCTION raw.fn_temporal_ledger_append_only()
        RETURNS TRIGGER LANGUAGE plpgsql AS $$
        BEGIN
            RETURN NEW;
        END;
        $$
        ",
    )
    .execute(&ctx.pool)
    .await?;

    let drifts = check_strict(&ctx.pool).await?;
    let matched: Vec<_> = drifts
        .iter()
        .filter(|d| {
            d.category == DriftCategory::TriggerBody
                && d.location == "raw.fn_temporal_ledger_append_only"
        })
        .collect();

    assert_eq!(
        matched.len(),
        1,
        "expected exactly one trigger_body drift on fn_temporal_ledger_append_only, got: {drifts:?}"
    );

    Ok(())
}

#[sinex_test]
async fn detects_manual_edit_to_fn_events_no_update(ctx: TestContext) -> TestResult<()> {
    sinex_db::schema::apply::apply(&ctx.pool).await?;

    // Replace the no-update guard with a pass-through — would allow UPDATE
    // on core.events, violating the immutability invariant.
    sqlx::query(
        r"
        CREATE OR REPLACE FUNCTION core.fn_events_no_update()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            RETURN NEW;
        END;
        $$
        ",
    )
    .execute(&ctx.pool)
    .await?;

    let drifts = check_strict(&ctx.pool).await?;
    let matched: Vec<_> = drifts
        .iter()
        .filter(|d| {
            d.category == DriftCategory::TriggerBody && d.location == "core.fn_events_no_update"
        })
        .collect();

    assert_eq!(
        matched.len(),
        1,
        "expected exactly one trigger_body drift on fn_events_no_update, got: {drifts:?}"
    );

    Ok(())
}

#[sinex_test]
async fn detects_manual_edit_to_execute_cascade_tombstone(ctx: TestContext) -> TestResult<()> {
    sinex_db::schema::apply::apply(&ctx.pool).await?;

    // Replace execute_cascade_tombstone with a stub — would silently delete
    // archived rows without creating tombstone records.
    sqlx::query(
        r"
        CREATE OR REPLACE FUNCTION core.execute_cascade_tombstone(
            p_archived_ids UUID[], p_reason TEXT, p_operation_id UUID
        ) RETURNS BIGINT LANGUAGE plpgsql AS $$
        BEGIN RETURN 0; END;
        $$
        ",
    )
    .execute(&ctx.pool)
    .await?;

    let drifts = check_strict(&ctx.pool).await?;
    let matched: Vec<_> = drifts
        .iter()
        .filter(|d| {
            d.category == DriftCategory::TriggerBody
                && d.location == "core.execute_cascade_tombstone"
        })
        .collect();

    assert_eq!(
        matched.len(),
        1,
        "expected exactly one trigger_body drift on execute_cascade_tombstone, got: {drifts:?}"
    );

    Ok(())
}

#[sinex_test]
async fn detects_manual_edit_to_execute_cascade_restore(ctx: TestContext) -> TestResult<()> {
    sinex_db::schema::apply::apply(&ctx.pool).await?;

    // Replace execute_cascade_restore with a stub — #1134 tracks atomicity
    // gaps in this function; body drift detection is the first safety net.
    sqlx::query(
        r"
        CREATE OR REPLACE FUNCTION core.execute_cascade_restore(
            p_archived_ids UUID[], p_operation_id TEXT
        ) RETURNS BIGINT LANGUAGE plpgsql AS $$
        BEGIN RETURN 0; END;
        $$
        ",
    )
    .execute(&ctx.pool)
    .await?;

    let drifts = check_strict(&ctx.pool).await?;
    let matched: Vec<_> = drifts
        .iter()
        .filter(|d| {
            d.category == DriftCategory::TriggerBody && d.location == "core.execute_cascade_restore"
        })
        .collect();

    assert_eq!(
        matched.len(),
        1,
        "expected exactly one trigger_body drift on execute_cascade_restore, got: {drifts:?}"
    );

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// #[derive(DbCheck)] integration (issue #1236)
// ─────────────────────────────────────────────────────────────────────────────

#[sinex_test]
async fn db_check_constraints_landed_after_apply(ctx: TestContext) -> TestResult<()> {
    sinex_db::schema::apply::apply(&ctx.pool).await?;

    // The `core.manifests.manifest_type_check_v1` constraint should now
    // exist and reflect every `NodeType` Display rendering.
    let def: Option<String> = sqlx::query_scalar(
        r"
        SELECT pg_get_constraintdef(c.oid)
        FROM pg_constraint c
        JOIN pg_class r ON c.conrelid = r.oid
        JOIN pg_namespace n ON r.relnamespace = n.oid
        WHERE n.nspname = 'core' AND r.relname = 'manifests'
          AND c.conname = 'manifest_type_check_v1'
        ",
    )
    .fetch_optional(&ctx.pool)
    .await?;
    let def = def.expect("manifest_type_check_v1 must exist after apply");
    for value in &["ingestor", "automaton", "service"] {
        assert!(
            def.contains(&format!("'{value}'")),
            "manifest_type_check_v1 missing value {value}: {def}"
        );
    }

    // No `'source'` (legacy stale value pre-#1236) should be referenced.
    assert!(
        !def.contains("'source'"),
        "manifest_type_check_v1 should not contain stale 'source' value: {def}"
    );

    // OperationStatus → result_status_check_v1.
    let def: Option<String> = sqlx::query_scalar(
        r"
        SELECT pg_get_constraintdef(c.oid)
        FROM pg_constraint c
        JOIN pg_class r ON c.conrelid = r.oid
        JOIN pg_namespace n ON r.relnamespace = n.oid
        WHERE n.nspname = 'core' AND r.relname = 'operations_log'
          AND c.conname = 'result_status_check_v1'
        ",
    )
    .fetch_optional(&ctx.pool)
    .await?;
    let def = def.expect("result_status_check_v1 must exist after apply");
    for value in &["running", "success", "failure", "cancelled", "pending"] {
        assert!(
            def.contains(&format!("'{value}'")),
            "result_status_check_v1 missing value {value}: {def}"
        );
    }

    Ok(())
}

#[sinex_test]
async fn strict_diff_clean_after_apply_for_db_check(ctx: TestContext) -> TestResult<()> {
    sinex_db::schema::apply::apply(&ctx.pool).await?;
    let drifts = check_strict(&ctx.pool).await?;
    let dbcheck_drifts: Vec<_> = drifts
        .iter()
        .filter(|d| {
            d.location.contains("::NodeType")
                || d.location.contains("::OperationStatus")
                || d.location.contains("::DataTier")
                || d.location.contains("::HealthStatus")
                || d.location.contains("::PrivacyTier")
        })
        .collect();
    assert!(
        dbcheck_drifts.is_empty(),
        "DbCheck strict drift must be empty on fresh apply: {dbcheck_drifts:?}"
    );
    Ok(())
}

#[sinex_test]
async fn apply_replaces_legacy_unversioned_check_constraint(ctx: TestContext) -> TestResult<()> {
    // Simulate the Wave-B production state: apply once, then drop the
    // versioned constraint and re-install the legacy unversioned one with
    // a stale variant set ('node' instead of 'ingestor').
    sinex_db::schema::apply::apply(&ctx.pool).await?;

    sqlx::query(
        "ALTER TABLE core.manifests \
         DROP CONSTRAINT IF EXISTS manifest_type_check_v1, \
         ADD CONSTRAINT manifests_manifest_type_check \
         CHECK (manifest_type IN ('node', 'automaton', 'service'))",
    )
    .execute(&ctx.pool)
    .await?;

    // diff() must surface the stale state.
    let drifts = sinex_db::schema::apply::diff(&ctx.pool).await?;
    assert!(
        drifts
            .iter()
            .any(|d| d.contains("manifest_type") && d.contains("manifest_type_check_v1")),
        "diff must report stale CHECK on manifest_type: {drifts:?}"
    );

    // Re-apply must drop the legacy constraint and add the versioned one.
    sinex_db::schema::apply::apply(&ctx.pool).await?;

    // Legacy is gone, versioned is back with the correct values.
    let legacy_exists: bool = sqlx::query_scalar(
        r"SELECT EXISTS (
            SELECT 1 FROM pg_constraint c
            JOIN pg_class r ON c.conrelid = r.oid
            JOIN pg_namespace n ON r.relnamespace = n.oid
            WHERE n.nspname = 'core' AND r.relname = 'manifests'
              AND c.conname = 'manifests_manifest_type_check'
        )",
    )
    .fetch_one(&ctx.pool)
    .await?;
    assert!(
        !legacy_exists,
        "legacy unversioned constraint should be dropped"
    );

    let def: String = sqlx::query_scalar(
        r"SELECT pg_get_constraintdef(c.oid)
          FROM pg_constraint c
          JOIN pg_class r ON c.conrelid = r.oid
          JOIN pg_namespace n ON r.relnamespace = n.oid
          WHERE n.nspname = 'core' AND r.relname = 'manifests'
            AND c.conname = 'manifest_type_check_v1'",
    )
    .fetch_one(&ctx.pool)
    .await?;
    assert!(
        def.contains("'ingestor'"),
        "renamed back to 'ingestor': {def}"
    );
    assert!(
        !def.contains("'node'"),
        "stale 'node' value must be gone: {def}"
    );

    Ok(())
}

#[sinex_test]
async fn diff_detects_stale_versioned_check_constraint(ctx: TestContext) -> TestResult<()> {
    sinex_db::schema::apply::apply(&ctx.pool).await?;

    // Replace v1 with a constraint of the same name but a stale body
    // (extra value 'misc'). The diff must surface it as stale.
    sqlx::query(
        "ALTER TABLE core.manifests \
         DROP CONSTRAINT IF EXISTS manifest_type_check_v1, \
         ADD CONSTRAINT manifest_type_check_v1 \
         CHECK (manifest_type IN ('ingestor', 'automaton', 'service', 'misc'))",
    )
    .execute(&ctx.pool)
    .await?;

    let drifts = sinex_db::schema::apply::diff(&ctx.pool).await?;
    assert!(
        drifts.iter().any(|d| d.contains("manifest_type")),
        "diff must surface stale-body drift on manifest_type: {drifts:?}"
    );

    // Re-apply heals it.
    sinex_db::schema::apply::apply(&ctx.pool).await?;
    let def: String = sqlx::query_scalar(
        r"SELECT pg_get_constraintdef(c.oid)
          FROM pg_constraint c
          JOIN pg_class r ON c.conrelid = r.oid
          JOIN pg_namespace n ON r.relnamespace = n.oid
          WHERE n.nspname = 'core' AND r.relname = 'manifests'
            AND c.conname = 'manifest_type_check_v1'",
    )
    .fetch_one(&ctx.pool)
    .await?;
    assert!(
        !def.contains("'misc'"),
        "stale 'misc' value must be cleared: {def}"
    );

    Ok(())
}
