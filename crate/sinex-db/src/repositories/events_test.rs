use std::collections::HashSet;
use xtask::sandbox::sinex_test;
// event_select_columns! is available in scope from the parent module

/// Number of physical columns in `core.events` (24 columns).
///
/// This must equal: `sinex::schema::Events` variant count excluding `Table` (24).
/// When adding or removing columns in `core.events`:
/// 1. Update `sinex::schema::Events` enum + `create_table_statement()`
/// 2. Update the `EventRecord` struct in both schema + sinex-db conversions.rs
/// 3. Update the `event_select_columns!` macro above
/// 4. Update this constant
const EXPECTED_COLUMN_COUNT: usize = 33;

/// Load-bearing column names that MUST appear in `event_select_columns!`.
/// Every column that appears in the SELECT list should appear here so that
/// renames and reorderings are caught.
const EXPECTED_COLUMNS: &[&str] = &[
    "id",
    "source",
    "event_type",
    "host",
    "payload",
    "ts_orig",
    "ts_orig_subnano",
    "ts_coided",
    "ts_persisted",
    "source_material_id",
    "anchor_byte",
    "offset_start",
    "offset_end",
    "offset_kind",
    "source_event_ids",
    "anchor_payload_hash",
    "associated_blob_ids",
    "payload_schema_id",
    "module_run_id",
    "temporal_policy",
    "semantics_version",
    "scope_key",
    "equivalence_key",
    "created_by_operation_id",
    "automaton_model",
    "ts_quality",
    // Derivation control plane (sinex-0vx.4 / sinex-8cr.2) — pre-existing
    // drift found and closed alongside sinex-w1w7 (this list had not been
    // updated when these columns landed).
    "product_class",
    "claim_support",
    "derivation_declaration_id",
    "derivation_epoch_id",
    "derivation_lane_id",
    "adjudication_event_id",
    // sinex-w1w7: admission-time content hash.
    "content_hash",
];

#[sinex_test]
async fn column_count_matches_schema() -> TestResult<()> {
    let cols: &str = event_select_columns!();
    let count = cols.split(',').count();
    assert_eq!(
        count, EXPECTED_COLUMN_COUNT,
        "event_select_columns! column count ({count}) != expected ({EXPECTED_COLUMN_COUNT}). \
         Either the schema changed or the macro drifted — update both, then update \
         EXPECTED_COLUMN_COUNT in this test."
    );
    Ok(())
}

#[sinex_test]
async fn all_declared_columns_present() -> TestResult<()> {
    let cols: &str = event_select_columns!();
    for expected in EXPECTED_COLUMNS {
        assert!(
            cols.contains(expected),
            "event_select_columns! is missing column '{expected}'. \
             Schema may have drifted — update the macro above and EXPECTED_COLUMNS in this test."
        );
    }
    Ok(())
}

#[sinex_test]
async fn no_extraneous_columns() -> TestResult<()> {
    // Count must equal the declared list length. Combined with
    // `all_declared_columns_present`, this guarantees the macro outputs
    // exactly the declared set — no extras, no missing entries.
    let cols: &str = event_select_columns!();
    let count = cols.split(',').count();
    assert_eq!(
        count,
        EXPECTED_COLUMNS.len(),
        "event_select_columns! column count ({count}) != declared count ({}). \
         Update EXPECTED_COLUMNS to match the macro.",
        EXPECTED_COLUMNS.len()
    );
    Ok(())
}

/// The other three tests in this file only compare `event_select_columns!()`
/// against a second hardcoded list in this SAME file (sinex-0t6w) — a
/// schema change that never touches this file (e.g. a bare `xtask schema
/// apply` DDL edit) can silently desync the macro from the real
/// `core.events` table with all three "drift guards" still green, since
/// they never look at the database. This test is the one that actually
/// looks: it queries `information_schema.columns` for the live table and
/// asserts the macro's column set is exactly the real one.
#[sinex_test]
async fn event_select_columns_matches_live_core_events_schema(
    ctx: xtask::sandbox::TestContext,
) -> TestResult<()> {
    let pool = ctx.pool.clone();
    let rows = sqlx::query!(
        r#"
        SELECT column_name as "column_name!"
        FROM information_schema.columns
        WHERE table_schema = 'core' AND table_name = 'events'
        "#
    )
    .fetch_all(&pool)
    .await?;
    let live_columns: HashSet<String> = rows.into_iter().map(|r| r.column_name).collect();

    // event_select_columns! entries are either a bare column name or a cast
    // expression like `source_material_id::uuid as source_material_id` — the
    // real output column name is whatever comes after `as`, if present.
    let cols: &str = event_select_columns!();
    let macro_columns: HashSet<String> = cols
        .split(',')
        .map(|c| {
            let c = c.trim();
            match c.rsplit_once(" as ") {
                Some((_, alias)) => alias.trim().to_string(),
                None => c.to_string(),
            }
        })
        .collect();

    let missing_from_macro: Vec<&String> = live_columns.difference(&macro_columns).collect();
    let missing_from_db: Vec<&String> = macro_columns.difference(&live_columns).collect();
    assert!(
        missing_from_macro.is_empty() && missing_from_db.is_empty(),
        "event_select_columns! has drifted from the live core.events schema — \
         in DB but not macro: {missing_from_macro:?}; in macro but not DB: {missing_from_db:?}"
    );
    Ok(())
}
