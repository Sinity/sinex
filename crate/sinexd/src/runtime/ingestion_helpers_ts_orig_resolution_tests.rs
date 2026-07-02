//! #1570 Prong B — `ts_orig` quality derivation (persistence-owned tier).
//!
//! These are pure-function tests: derivation depends only on the
//! source-material timing row + sub-material ledger entries + the event's
//! anchor offset. All of those are stable across replay, so determinism
//! (same inputs → same output) is exactly the replay-stability contract.
use super::*;
use xtask::sandbox::prelude::*;

fn ts(secs: i64) -> Timestamp {
    Timestamp::from_unix_timestamp(secs).expect("valid unix timestamp")
}

/// Material-tier: a timing-bearing category with a recorded `start_time`
/// resolves to that time at the category's mapped rung.
#[sinex_test]
async fn material_timing_uses_start_time_with_category_rung() -> TestResult<()> {
    let timing = MaterialTiming {
        timing_info_type: SourceMaterialTimingInfoType::Intrinsic,
        start_time: Some(ts(1_000)),
        staged_at: ts(9_000),
    };
    assert_eq!(
        timing.resolve(),
        (ts(1_000), TemporalSourceType::IntrinsicContent)
    );
    Ok(())
}

/// Material-tier: no `start_time` falls back to the `staged_at` floor.
#[sinex_test]
async fn material_timing_falls_back_to_staged_floor() -> TestResult<()> {
    let timing = MaterialTiming {
        timing_info_type: SourceMaterialTimingInfoType::Inferred,
        start_time: None,
        staged_at: ts(9_000),
    };
    assert_eq!(timing.resolve(), (ts(9_000), TemporalSourceType::StagedAt));
    Ok(())
}

/// Material-tier: a category that itself maps to the `StagedAt` rung ignores
/// any `start_time` and uses the floor (the floor *is* the best evidence).
#[sinex_test]
async fn material_timing_staged_category_ignores_start_time() -> TestResult<()> {
    let timing = MaterialTiming {
        timing_info_type: SourceMaterialTimingInfoType::StagedAt,
        start_time: Some(ts(1_000)),
        staged_at: ts(9_000),
    };
    assert_eq!(timing.resolve(), (ts(9_000), TemporalSourceType::StagedAt));
    Ok(())
}

fn ledger_entry(
    start: i64,
    end: i64,
    ts_capture: Timestamp,
    source_type: TemporalSourceType,
) -> LedgerEntry {
    LedgerEntry {
        offset_start: start,
        offset_end: end,
        ts_capture,
        precision: TemporalPrecision::Exact,
        source_type,
    }
}

/// A sub-material ledger entry covering the event's offset (a genuine
/// wrapped-stream / per-chunk timing) takes precedence over the material tier.
#[sinex_test]
async fn derive_prefers_covering_ledger_entry() -> TestResult<()> {
    let reader = LedgerReader::new(
        Uuid::now_v7(),
        vec![ledger_entry(
            0,
            1_000,
            ts(2_000),
            TemporalSourceType::RealtimeCapture,
        )],
    );
    let timing = MaterialTiming {
        timing_info_type: SourceMaterialTimingInfoType::StagedAt,
        start_time: None,
        staged_at: ts(9_000),
    };
    assert_eq!(
        reader.derive_ts_orig(500, &timing),
        (ts(2_000), TemporalSourceType::RealtimeCapture),
        "covering ledger entry wins over the staged floor"
    );
    Ok(())
}

/// With no ledger entry covering the offset, derivation falls to the
/// material tier — and never returns an ephemeral value.
#[sinex_test]
async fn derive_falls_back_to_material_tier() -> TestResult<()> {
    let reader = LedgerReader::new(Uuid::now_v7(), Vec::new());
    let timing = MaterialTiming {
        timing_info_type: SourceMaterialTimingInfoType::Intrinsic,
        start_time: Some(ts(1_000)),
        staged_at: ts(9_000),
    };
    assert_eq!(
        reader.derive_ts_orig(500, &timing),
        (ts(1_000), TemporalSourceType::IntrinsicContent)
    );
    Ok(())
}

/// Replay stability: re-deriving from the same material yields the same
/// `(ts_orig, rung)`. Replay re-reads identical material timing + ledger, so
/// the only thing that changes is the event id (`ts_coided`), never `ts_orig`.
#[sinex_test]
async fn derive_is_replay_stable() -> TestResult<()> {
    let entries = vec![ledger_entry(
        0,
        1_000,
        ts(2_000),
        TemporalSourceType::IntrinsicContent,
    )];
    let timing = MaterialTiming {
        timing_info_type: SourceMaterialTimingInfoType::Inferred,
        start_time: Some(ts(5_000)),
        staged_at: ts(9_000),
    };
    let first = LedgerReader::new(Uuid::now_v7(), entries.clone()).derive_ts_orig(500, &timing);
    let second = LedgerReader::new(Uuid::now_v7(), entries).derive_ts_orig(500, &timing);
    assert_eq!(first, second);
    Ok(())
}
