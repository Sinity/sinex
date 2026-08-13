use super::*;
use time::macros::datetime;
use xtask::sandbox::prelude::sinex_test;

fn chunk(
    kind: &str,
    status: &str,
    start: Option<OffsetDateTime>,
    end: Option<OffsetDateTime>,
    timing: &str,
) -> Chunk {
    chunk_with_privacy(kind, status, start, end, timing, PrivacyClass::Unknown)
}

fn chunk_with_privacy(
    kind: &str,
    status: &str,
    start: Option<OffsetDateTime>,
    end: Option<OffsetDateTime>,
    timing: &str,
    privacy_class: PrivacyClass,
) -> Chunk {
    Chunk {
        material_kind: kind.into(),
        status: status.into(),
        start_time: start,
        end_time: end,
        staged_at: start.unwrap_or(datetime!(2026-01-01 0:00 UTC)),
        timing: timing.into(),
        declared_contract: DeclaredCoverageContract::default(),
        privacy_class,
    }
}

#[sinex_test]
async fn continuous_trailing_gap_is_time_bounded() -> xtask::sandbox::TestResult<()> {
    let now = datetime!(2026-08-12 12:00 UTC);

    let gap = continuous_trailing_gap(
        now - time::Duration::minutes(10),
        now,
        DEFAULT_RUNTIME_LIVENESS_STALE_AFTER_SECS,
    )
    .expect("a continuous source silent past the liveness threshold needs a trailing gap");
    assert_eq!(gap.kind, GapKind::ServiceCrash);
    assert!(
        continuous_trailing_gap(
            now - time::Duration::minutes(4),
            now,
            DEFAULT_RUNTIME_LIVENESS_STALE_AFTER_SECS,
        )
        .is_none()
    );
    Ok(())
}

fn chunk_with_declared(
    kind: &str,
    status: &str,
    start: Option<OffsetDateTime>,
    end: Option<OffsetDateTime>,
    timing: &str,
    declared: DeclaredCoverageContract,
) -> Chunk {
    Chunk {
        material_kind: kind.into(),
        status: status.into(),
        start_time: start,
        end_time: end,
        staged_at: start.unwrap_or(datetime!(2026-01-01 0:00 UTC)),
        timing: timing.into(),
        declared_contract: declared,
        privacy_class: PrivacyClass::Unknown,
    }
}

#[sinex_test]
async fn classify_overlap_when_curr_starts_before_prev_ends() -> xtask::sandbox::TestResult<()> {
    let prev = chunk(
        "annex",
        "completed",
        Some(datetime!(2026-01-01 10:00 UTC)),
        Some(datetime!(2026-01-01 11:00 UTC)),
        "intrinsic",
    );
    let curr = chunk(
        "annex",
        "completed",
        Some(datetime!(2026-01-01 10:30 UTC)),
        Some(datetime!(2026-01-01 11:30 UTC)),
        "intrinsic",
    );
    let kind = classify_seam(
        datetime!(2026-01-01 11:00 UTC),
        datetime!(2026-01-01 10:30 UTC),
        &prev,
        &curr,
    );
    assert!(matches!(kind, SeamKind::Overlap));
    Ok(())
}

#[sinex_test]
async fn classify_continuation_when_back_to_back() -> xtask::sandbox::TestResult<()> {
    let prev = chunk(
        "annex",
        "completed",
        Some(datetime!(2026-01-01 10:00 UTC)),
        Some(datetime!(2026-01-01 11:00 UTC)),
        "intrinsic",
    );
    let curr = chunk(
        "annex",
        "completed",
        Some(datetime!(2026-01-01 11:00 UTC)),
        Some(datetime!(2026-01-01 12:00 UTC)),
        "intrinsic",
    );
    let kind = classify_seam(
        datetime!(2026-01-01 11:00 UTC),
        datetime!(2026-01-01 11:00 UTC),
        &prev,
        &curr,
    );
    assert!(matches!(kind, SeamKind::ExpectedContinuation));
    Ok(())
}

#[sinex_test]
async fn classify_discontinuity_for_long_gap() -> xtask::sandbox::TestResult<()> {
    let prev = chunk(
        "annex",
        "completed",
        Some(datetime!(2026-01-01 10:00 UTC)),
        Some(datetime!(2026-01-01 10:30 UTC)),
        "intrinsic",
    );
    let curr = chunk(
        "annex",
        "completed",
        Some(datetime!(2026-01-01 14:00 UTC)),
        Some(datetime!(2026-01-01 15:00 UTC)),
        "intrinsic",
    );
    let kind = classify_seam(
        datetime!(2026-01-01 10:30 UTC),
        datetime!(2026-01-01 14:00 UTC),
        &prev,
        &curr,
    );
    assert!(matches!(kind, SeamKind::Discontinuity));
    Ok(())
}

#[sinex_test]
async fn classify_recovered_partial_when_either_chunk_marked() -> xtask::sandbox::TestResult<()> {
    let prev = chunk(
        "annex",
        "recovered_partial",
        Some(datetime!(2026-01-01 10:00 UTC)),
        Some(datetime!(2026-01-01 10:30 UTC)),
        "intrinsic",
    );
    let curr = chunk(
        "annex",
        "completed",
        Some(datetime!(2026-01-01 11:00 UTC)),
        Some(datetime!(2026-01-01 11:30 UTC)),
        "intrinsic",
    );
    let kind = classify_seam(
        datetime!(2026-01-01 10:30 UTC),
        datetime!(2026-01-01 11:00 UTC),
        &prev,
        &curr,
    );
    assert!(matches!(kind, SeamKind::RecoveredPartial));
    Ok(())
}

#[sinex_test]
async fn replayability_lists_every_dimension_weakness() -> xtask::sandbox::TestResult<()> {
    let r = build_replayability(ReplayabilityFlags {
        any_blob: false,
        good_timing: false,
        all_finalized: false,
        any_failed: true,
        any_recovered: false,
    });
    assert!(!r.raw_bytes_preserved);
    assert!(!r.timing_quality);
    assert!(!r.anchor_stability);
    // 4 reasons + the always-present parser_determinism caveat.
    assert!(r.weak_points.len() >= 4);
    Ok(())
}

#[sinex_test]
async fn coverage_contract_inferred_for_known_families() -> xtask::sandbox::TestResult<()> {
    let chunks: Vec<Chunk> = vec![];
    assert!(matches!(
        infer_coverage_contract("shell", &chunks),
        CoverageContract::Continuous
    ));
    assert!(matches!(
        infer_coverage_contract("browser", &chunks),
        CoverageContract::PeriodicDump
    ));
    assert!(matches!(
        infer_coverage_contract("import", &chunks),
        CoverageContract::FiniteOneShot
    ));
    assert!(matches!(
        infer_coverage_contract("unknown", &chunks),
        CoverageContract::OpportunisticImport
    ));
    Ok(())
}

#[sinex_test]
async fn private_mode_seam_only_fires_for_private_classes() -> xtask::sandbox::TestResult<()> {
    let prev = chunk_with_privacy(
        "annex",
        "completed",
        Some(datetime!(2026-01-01 10:00 UTC)),
        Some(datetime!(2026-01-01 10:30 UTC)),
        "intrinsic",
        PrivacyClass::Personal,
    );
    let curr = chunk_with_privacy(
        "annex",
        "completed",
        Some(datetime!(2026-01-01 12:00 UTC)),
        Some(datetime!(2026-01-01 12:30 UTC)),
        "intrinsic",
        PrivacyClass::Public,
    );
    // Personal + Public + 90 minute gap → PrivateModeGap because one
    // side is private-classed.
    let kind = classify_seam(
        datetime!(2026-01-01 10:30 UTC),
        datetime!(2026-01-01 12:00 UTC),
        &prev,
        &curr,
    );
    assert!(matches!(kind, SeamKind::PrivateModeGap));

    // Unknown + Unknown + same gap → Discontinuity (Unknown is NOT
    // treated as private).
    let prev2 = chunk(
        "annex",
        "completed",
        Some(datetime!(2026-01-01 10:00 UTC)),
        Some(datetime!(2026-01-01 10:30 UTC)),
        "intrinsic",
    );
    let kind2 = classify_seam(
        datetime!(2026-01-01 10:30 UTC),
        datetime!(2026-01-01 12:00 UTC),
        &prev2,
        &curr,
    );
    assert!(matches!(kind2, SeamKind::Discontinuity));
    Ok(())
}

#[sinex_test]
async fn declared_coverage_contract_overrides_heuristic_inference() -> xtask::sandbox::TestResult<()>
{
    let declared = DeclaredCoverageContract {
        kind: DeclaredCoverageContractKind::EphemeralStream,
        ..Default::default()
    };
    let chunks = vec![chunk_with_declared(
        "annex",
        "completed",
        Some(datetime!(2026-01-01 10:00 UTC)),
        Some(datetime!(2026-01-01 11:00 UTC)),
        "realtime",
        declared,
    )];
    // Family name `shell` would heuristically map to Continuous; the
    // declared kind takes precedence.
    let (contract, is_declared) = resolve_coverage_contract("shell", &chunks);
    assert!(matches!(contract, CoverageContract::EphemeralStream));
    assert!(is_declared);
    Ok(())
}

#[sinex_test]
async fn unknown_declared_contract_falls_back_to_heuristic() -> xtask::sandbox::TestResult<()> {
    let chunks = vec![chunk(
        "annex",
        "completed",
        Some(datetime!(2026-01-01 10:00 UTC)),
        Some(datetime!(2026-01-01 11:00 UTC)),
        "intrinsic",
    )];
    // Default declared contract is Unknown; family name decides.
    let (contract, is_declared) = resolve_coverage_contract("browser", &chunks);
    assert!(matches!(contract, CoverageContract::PeriodicDump));
    assert!(!is_declared);
    Ok(())
}

#[sinex_test]
async fn continuous_trailing_gap_requires_stale_latest_observation()
-> xtask::sandbox::TestResult<()> {
    let latest = datetime!(2026-08-12 11:54 UTC);
    let now = datetime!(2026-08-12 12:00 UTC);
    let gap = continuous_trailing_gap(latest, now, 300).expect("stale tail expected");
    assert_eq!(gap.kind, GapKind::ServiceCrash);
    assert_eq!(
        gap.attribution.as_deref(),
        Some("continuous source has no recent observed coverage")
    );
    assert!(continuous_trailing_gap(latest, now, 600).is_none());
    Ok(())
}
