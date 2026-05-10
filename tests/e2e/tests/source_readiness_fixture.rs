//! Tri-state readiness fixture for source materials (#1174 Phase 5.3).
//!
//! Stages three sources representing the three operator-relevant readiness
//! states for `sinexctl sources readiness` and asserts that the readiness
//! repository classifies each one correctly:
//!
//!   - `missing`         — no row in `raw.source_material_registry`. The
//!                         readiness API returns `None` for the canonical
//!                         identifier; it is also absent from the list view.
//!   - `staged_unparsed` — one material registered with `status='sensing'`
//!                         (no parsed events). Readiness must report
//!                         `Partial` with the `material.staged_unparsed`
//!                         caveat.
//!   - `available`       — one material registered with `status='completed'`
//!                         and at least one event referencing it. Readiness
//!                         must report `Available` with no degraded caveats.
//!
//! Each source uses a unique synthetic identifier so the fixture composes
//! against any DB state — including a database with prior materials —
//! without aliasing onto existing readiness rows.
//!
//! The suite registers under the `readiness::tri_state_fixture` scenario tag
//! so it surfaces in `xtask test --list-scenarios` and the CI scenario lanes.

use sinex_db::DbPoolExt;
use sinex_primitives::rpc::sources::SourceReadinessStatus;
use sqlx::types::Uuid;
use xtask::sandbox::prelude::*;

/// Insert a registry row at a fixed status with no temporal metadata. Used
/// for the `staged_unparsed` (status=sensing, total_bytes NULL) shape.
async fn insert_registry_row_unparsed(pool: &DbPool, source_identifier: &str) -> TestResult<Uuid> {
    let id = Uuid::now_v7();
    sqlx::query(
        r"
        INSERT INTO raw.source_material_registry
            (id, material_kind, source_identifier, status, timing_info_type)
        VALUES ($1::uuid, 'annex', $2, 'sensing', 'intrinsic')
        ",
    )
    .bind(id)
    .bind(source_identifier)
    .execute(pool)
    .await?;
    Ok(id)
}

/// Insert a registry row at `status='completed'` with finalized total_bytes
/// so the readiness query treats it as a successful staging.
async fn insert_registry_row_completed(pool: &DbPool, source_identifier: &str) -> TestResult<Uuid> {
    let id = Uuid::now_v7();
    sqlx::query(
        r"
        INSERT INTO raw.source_material_registry
            (id, material_kind, source_identifier, status, timing_info_type, total_bytes)
        VALUES ($1::uuid, 'annex', $2, 'completed', 'intrinsic', 1024)
        ",
    )
    .bind(id)
    .bind(source_identifier)
    .execute(pool)
    .await?;
    Ok(id)
}

/// Insert a parsed event referencing the given material so the readiness
/// `parsed_event_count` query observes a non-zero count for it.
async fn seed_event(pool: &DbPool, source: &str, material_id: Uuid) -> TestResult<()> {
    let event_id = Uuid::now_v7();
    sqlx::query(
        r"
        INSERT INTO core.events
            (id, source, event_type, payload, ts_orig, host, source_material_id, anchor_byte)
        VALUES ($1::uuid, $2, 'shell.command', '{}'::jsonb, NOW(), 'test-host', $3::uuid, 0)
        ",
    )
    .bind(event_id)
    .bind(source)
    .bind(material_id)
    .execute(pool)
    .await?;
    Ok(())
}

#[sinex_test(
    scenario = "readiness::tri_state_fixture::missing",
    category = "source_material",
    lane = "fast",
    tags = "readiness,tri_state_fixture",
    subjects = "issue:1174,issue:1099,explore:readiness",
    claims = "assertion:readiness.missing_returns_none"
)]
async fn readiness_missing_source_returns_none(ctx: TestContext) -> TestResult<()> {
    // No registry row, no events. The readiness API must report `None`
    // for the targeted source identifier rather than fabricating a row.
    let identifier = format!("readiness-missing-{}", Uuid::now_v7());

    let result = ctx
        .pool()
        .source_materials()
        .get_source_readiness(&identifier, None, None)
        .await?;

    assert!(
        result.is_none(),
        "missing source must return None; got: {:?}",
        result.as_ref().map(|r| (r.status, r.material_count))
    );

    // List view must also exclude an unstaged identifier — we filter the
    // returned vector by our synthetic prefix to avoid coupling to
    // unrelated DB rows.
    let list = ctx
        .pool()
        .source_materials()
        .list_source_readiness(None, None)
        .await?;
    assert!(
        list.iter().all(|r| r.source_identifier != identifier),
        "missing source must not appear in list_source_readiness"
    );
    Ok(())
}

#[sinex_test(
    scenario = "readiness::tri_state_fixture::staged_unparsed",
    category = "source_material",
    lane = "fast",
    tags = "readiness,tri_state_fixture",
    subjects = "issue:1174,issue:1099,explore:readiness",
    claims = "assertion:readiness.staged_unparsed_partial"
)]
async fn readiness_staged_unparsed_reports_partial(ctx: TestContext) -> TestResult<()> {
    // Material registered but never finalized. The readiness API must
    // report `Partial` with the `material.staged_unparsed` caveat — the
    // operator-relevant signal is "you staged something but no parsed
    // events have referenced it yet".
    let identifier = format!("readiness-staged-{}", Uuid::now_v7());
    let _material_id = insert_registry_row_unparsed(ctx.pool(), &identifier).await?;

    let report = ctx
        .pool()
        .source_materials()
        .get_source_readiness(&identifier, None, None)
        .await?
        .expect("staged source must produce a readiness row");

    assert_eq!(
        report.status,
        SourceReadinessStatus::Partial,
        "staged-unparsed must classify as Partial; got {:?}",
        report.status
    );
    assert_eq!(report.material_count, 1, "exactly one material registered");
    assert_eq!(
        report.parsed_event_count,
        Some(0),
        "no events parsed against the staged material"
    );

    // The MATERIAL_STAGED_UNPARSED caveat is the operator-actionable signal
    // for this state. The repository emits it whenever
    // `completed_count == 0 && material_count > 0`.
    let has_unparsed = report
        .caveats
        .iter()
        .any(|c| c.code == "material.staged_unparsed");
    assert!(
        has_unparsed,
        "expected material.staged_unparsed caveat, got: {:?}",
        report.caveats.iter().map(|c| &c.code).collect::<Vec<_>>()
    );
    Ok(())
}

#[sinex_test(
    scenario = "readiness::tri_state_fixture::available",
    category = "source_material",
    lane = "fast",
    tags = "readiness,tri_state_fixture",
    subjects = "issue:1174,issue:1099,explore:readiness",
    claims = "assertion:readiness.available_completed"
)]
async fn readiness_available_completed_with_events(ctx: TestContext) -> TestResult<()> {
    // Material completed AND parsed events reference it — readiness must
    // resolve to `Available` (the green-path operator state) and must NOT
    // emit any of the degraded `material.staged_unparsed` /
    // `parser.failed_recently` caveats.
    let identifier = format!("readiness-available-{}", Uuid::now_v7());
    let material_id = insert_registry_row_completed(ctx.pool(), &identifier).await?;
    seed_event(ctx.pool(), "shellreadiness", material_id).await?;

    let report = ctx
        .pool()
        .source_materials()
        .get_source_readiness(&identifier, None, None)
        .await?
        .expect("completed source must produce a readiness row");

    assert_eq!(
        report.status,
        SourceReadinessStatus::Available,
        "completed source with events must classify as Available; got {:?} caveats={:?}",
        report.status,
        report.caveats.iter().map(|c| &c.code).collect::<Vec<_>>()
    );
    assert_eq!(report.material_count, 1);
    assert_eq!(report.parsed_event_count, Some(1));

    // Available state must not carry degraded-state caveats. Info caveats
    // (binding-evidence, parser-jobs-untracked) are expected and not
    // operator-actionable.
    let degraded: Vec<&String> = report
        .caveats
        .iter()
        .filter(|c| {
            matches!(
                c.code.as_str(),
                "material.staged_unparsed" | "parser.failed_recently"
            )
        })
        .map(|c| &c.code)
        .collect();
    assert!(
        degraded.is_empty(),
        "Available state must not carry degraded caveats; got: {degraded:?}"
    );
    Ok(())
}

#[sinex_test(
    scenario = "readiness::tri_state_fixture::distinct_in_one_run",
    category = "source_material",
    lane = "fast",
    tags = "readiness,tri_state_fixture",
    subjects = "issue:1174,issue:1099,explore:readiness",
    claims = "assertion:readiness.tri_state_distinct"
)]
async fn readiness_tri_state_distinct_in_one_run(ctx: TestContext) -> TestResult<()> {
    // The bundled assertion: stage all three states inside a single
    // database slot and confirm the readiness API distinguishes between
    // them. This is the load-bearing fixture for #1174 Phase 5.3 — the
    // operator-facing UX promise is "three sources with three distinct
    // readiness statuses".
    let missing_id = format!("tri-missing-{}", Uuid::now_v7());
    let unparsed_id = format!("tri-unparsed-{}", Uuid::now_v7());
    let available_id = format!("tri-available-{}", Uuid::now_v7());

    insert_registry_row_unparsed(ctx.pool(), &unparsed_id).await?;
    let avail_mat = insert_registry_row_completed(ctx.pool(), &available_id).await?;
    seed_event(ctx.pool(), "shelltristate", avail_mat).await?;

    let repo = ctx.pool().source_materials();

    let missing = repo.get_source_readiness(&missing_id, None, None).await?;
    let unparsed = repo
        .get_source_readiness(&unparsed_id, None, None)
        .await?
        .expect("unparsed source must produce a row");
    let available = repo
        .get_source_readiness(&available_id, None, None)
        .await?
        .expect("available source must produce a row");

    assert!(missing.is_none(), "missing source must return None");
    assert_eq!(
        unparsed.status,
        SourceReadinessStatus::Partial,
        "unparsed must be Partial"
    );
    assert_eq!(
        available.status,
        SourceReadinessStatus::Available,
        "available must be Available"
    );
    // Distinct statuses across the staged pair (the missing source is
    // already proven distinct by being absent from the registry).
    assert_ne!(unparsed.status, available.status);
    Ok(())
}
