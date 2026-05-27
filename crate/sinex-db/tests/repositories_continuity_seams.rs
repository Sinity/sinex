//! Seam-classification fixtures for [`ContinuityRepository`] (#1174 Phase 5.4).
//!
//! Each fixture stages two adjacent material chunks for a synthetic source
//! family, inserts a referencing event so the family is observable, then
//! reads the resulting continuity report and asserts that the seam between
//! the chunks is classified as the expected [`SeamKind`]. Four scenarios
//! cover every variant the production classifier emits today:
//!
//!   - `Continuation`     — back-to-back completed chunks
//!   - `Overlap`          — later chunk starts before earlier ends
//!   - `Discontinuity`    — gap > 60s with no privacy / partial markers
//!   - `RecoveredPartial` — one chunk has `status = recovered_partial`
//!
//! The suite uses ordinary test names and harness-recorded dependencies; it
//! must not rely on inert taxonomy metadata for scheduling or proof claims.

use sinex_db::repositories::DbPoolExt;
use sinex_primitives::sources::SourceFamily;
use sinex_primitives::sources::continuity::SeamKind;
use sqlx::types::Uuid;
use time::OffsetDateTime;
use xtask::sandbox::prelude::*;

/// Stage a registry row at a specific time window so the continuity query
/// can compute deterministic seams. Bypasses the repository builders so
/// `start_time` / `end_time` / `status` / `material_kind` are settable
/// directly without going through the in-flight state machine.
async fn insert_chunk(
    pool: &DbPool,
    source_identifier: &str,
    status: &str,
    timing: &str,
    start: OffsetDateTime,
    end: OffsetDateTime,
) -> TestResult<Uuid> {
    let id = Uuid::now_v7();
    sqlx::query(
        r"
        INSERT INTO raw.source_material_registry
            (id, material_kind, source_identifier, status, timing_info_type,
             start_time, end_time, total_bytes)
        VALUES ($1::uuid, 'annex', $2, $3, $4, $5, $6, 1024)
        ",
    )
    .bind(id)
    .bind(source_identifier)
    .bind(status)
    .bind(timing)
    .bind(start)
    .bind(end)
    .execute(pool)
    .await?;
    Ok(id)
}

/// Insert a single event referencing the given material so the family
/// becomes observable. The continuity query joins on
/// `core.events.source_material_id`, so no event ⇒ no family row.
///
/// Uses raw SQL rather than the publish pipeline so the test controls the
/// `(source, source_material_id)` pairing without auto-generated source
/// material being created by the publish helper.
async fn seed_event(
    pool: &DbPool,
    family: &str,
    event_type: &str,
    material_id: Uuid,
) -> TestResult<()> {
    let event_id = Uuid::now_v7();
    sqlx::query(
        r"
        INSERT INTO core.events
            (id, source, event_type, payload, ts_orig, host, source_material_id, anchor_byte)
        VALUES ($1::uuid, $2, $3, '{}'::jsonb, NOW(), 'test-host', $4::uuid, 0)
        ",
    )
    .bind(event_id)
    .bind(family)
    .bind(event_type)
    .bind(material_id)
    .execute(pool)
    .await?;
    Ok(())
}

#[sinex_test]
async fn seam_classification_emits_expected_continuation(ctx: TestContext) -> TestResult<()> {
    // Two registry rows representing the two adjacent chunks. They must
    // have distinct `source_identifier`s — `uk_sm_registry_source_identifier`
    // is UNIQUE (sinex-schema/src/schema/source_materials.rs#L252) — and
    // both must be referenced by at least one event in the same source
    // family so the continuity query (continuity.rs:170-182) picks up both
    // via path (a) (`sm.id IN (events for family)`).
    let suffix = Uuid::now_v7();
    let id_a = format!("seam-continuation-a-{suffix}");
    let id_b = format!("seam-continuation-b-{suffix}");
    let m1 = insert_chunk(
        ctx.pool(),
        &id_a,
        "completed",
        "intrinsic",
        datetime("2026-04-01T10:00:00Z"),
        datetime("2026-04-01T11:00:00Z"),
    )
    .await?;
    let m2 = insert_chunk(
        ctx.pool(),
        &id_b,
        "completed",
        "intrinsic",
        datetime("2026-04-01T11:00:00Z"),
        datetime("2026-04-01T12:00:00Z"),
    )
    .await?;
    seed_event(ctx.pool(), "shellseamcont", "shell.command", m1).await?;
    seed_event(ctx.pool(), "shellseamcont", "shell.command", m2).await?;

    let family = SourceFamily::new("shellseamcont".to_string())?;
    let report = ctx
        .pool()
        .continuity()
        .get_continuity_report(&family)
        .await?
        .expect("family observable after seeding event");

    assert_eq!(report.seams.len(), 1, "expected one adjacency");
    assert!(
        matches!(report.seams[0].kind, SeamKind::ExpectedContinuation),
        "expected ExpectedContinuation, got {:?}",
        report.seams[0].kind
    );
    Ok(())
}

#[sinex_test]
async fn seam_classification_emits_overlap(ctx: TestContext) -> TestResult<()> {
    let suffix = Uuid::now_v7();
    let id_a = format!("seam-overlap-a-{suffix}");
    let id_b = format!("seam-overlap-b-{suffix}");
    let m1 = insert_chunk(
        ctx.pool(),
        &id_a,
        "completed",
        "intrinsic",
        datetime("2026-04-02T10:00:00Z"),
        datetime("2026-04-02T11:00:00Z"),
    )
    .await?;
    // Second chunk starts 30 minutes BEFORE the first ends → overlap.
    let m2 = insert_chunk(
        ctx.pool(),
        &id_b,
        "completed",
        "intrinsic",
        datetime("2026-04-02T10:30:00Z"),
        datetime("2026-04-02T11:30:00Z"),
    )
    .await?;
    seed_event(ctx.pool(), "shellseamoverlap", "shell.command", m1).await?;
    seed_event(ctx.pool(), "shellseamoverlap", "shell.command", m2).await?;

    let family = SourceFamily::new("shellseamoverlap".to_string())?;
    let report = ctx
        .pool()
        .continuity()
        .get_continuity_report(&family)
        .await?
        .expect("family observable");

    assert!(
        report
            .seams
            .iter()
            .any(|s| matches!(s.kind, SeamKind::Overlap)),
        "expected at least one Overlap seam, got: {:?}",
        report.seams.iter().map(|s| s.kind).collect::<Vec<_>>()
    );
    Ok(())
}

#[sinex_test]
async fn seam_classification_emits_discontinuity(ctx: TestContext) -> TestResult<()> {
    let suffix = Uuid::now_v7();
    let id_a = format!("seam-discont-a-{suffix}");
    let id_b = format!("seam-discont-b-{suffix}");
    let m1 = insert_chunk(
        ctx.pool(),
        &id_a,
        "completed",
        "intrinsic",
        datetime("2026-04-03T10:00:00Z"),
        datetime("2026-04-03T10:30:00Z"),
    )
    .await?;
    // 3.5 hour gap ⇒ Discontinuity (no privacy markers, no partial state).
    let m2 = insert_chunk(
        ctx.pool(),
        &id_b,
        "completed",
        "intrinsic",
        datetime("2026-04-03T14:00:00Z"),
        datetime("2026-04-03T15:00:00Z"),
    )
    .await?;
    seed_event(ctx.pool(), "shellseamdiscont", "shell.command", m1).await?;
    seed_event(ctx.pool(), "shellseamdiscont", "shell.command", m2).await?;

    let family = SourceFamily::new("shellseamdiscont".to_string())?;
    let report = ctx
        .pool()
        .continuity()
        .get_continuity_report(&family)
        .await?
        .expect("family observable");

    assert!(
        report
            .seams
            .iter()
            .any(|s| matches!(s.kind, SeamKind::Discontinuity)),
        "expected Discontinuity seam, got: {:?}",
        report.seams.iter().map(|s| s.kind).collect::<Vec<_>>()
    );
    // A discontinuity longer than 1s also surfaces as a CoverageGap.
    assert!(
        !report.gaps.is_empty(),
        "discontinuity should produce a CoverageGap"
    );
    Ok(())
}

#[sinex_test]
async fn seam_classification_emits_recovered_partial(ctx: TestContext) -> TestResult<()> {
    let suffix = Uuid::now_v7();
    let id_a = format!("seam-recpartial-a-{suffix}");
    let id_b = format!("seam-recpartial-b-{suffix}");
    // Earlier chunk marked recovered_partial — the seam should classify
    // as RecoveredPartial regardless of gap length, because the partial
    // marker takes precedence over plain discontinuity.
    let m1 = insert_chunk(
        ctx.pool(),
        &id_a,
        "recovered_partial",
        "intrinsic",
        datetime("2026-04-04T10:00:00Z"),
        datetime("2026-04-04T10:30:00Z"),
    )
    .await?;
    let m2 = insert_chunk(
        ctx.pool(),
        &id_b,
        "completed",
        "intrinsic",
        datetime("2026-04-04T11:00:00Z"),
        datetime("2026-04-04T11:30:00Z"),
    )
    .await?;
    seed_event(ctx.pool(), "shellseamrec", "shell.command", m1).await?;
    seed_event(ctx.pool(), "shellseamrec", "shell.command", m2).await?;

    let family = SourceFamily::new("shellseamrec".to_string())?;
    let report = ctx
        .pool()
        .continuity()
        .get_continuity_report(&family)
        .await?
        .expect("family observable");

    assert!(
        report
            .seams
            .iter()
            .any(|s| matches!(s.kind, SeamKind::RecoveredPartial)),
        "expected RecoveredPartial seam, got: {:?}",
        report.seams.iter().map(|s| s.kind).collect::<Vec<_>>()
    );
    Ok(())
}

fn datetime(rfc3339: &str) -> OffsetDateTime {
    match OffsetDateTime::parse(rfc3339, &time::format_description::well_known::Rfc3339) {
        Ok(value) => value,
        Err(error) => panic!("test datetime literal must be valid RFC3339: {error}"),
    }
}
