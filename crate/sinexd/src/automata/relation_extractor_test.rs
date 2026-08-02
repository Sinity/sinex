use super::*;
use crate::runtime::automaton::AutomatonContext;
use sinex_primitives::domain::EntityTypeName;
use xtask::sandbox::sinex_test;

fn resolved(name: &str) -> EntityResolvedPayload {
    EntityResolvedPayload {
        entity_id: Uuid::new_v5(&Uuid::NAMESPACE_OID, name.as_bytes()),
        canonical_name: name.to_string(),
        entity_type: EntityTypeName::new("tool"),
        original_name: name.to_string(),
    }
}

/// sinex-audit-outoforder-pattern: `last_seen` is a watermark used to decide
/// when the co-occurrence window has gone quiet. An out-of-order arrival must
/// not move it backward, or a subsequent in-order arrival's gap computation is
/// measured against a stale earlier time and mis-fires.
///
/// Reverting the monotonic guard makes this test fail: after the out-of-order
/// entry at `t0`, `last_seen` would regress to `t0`, so the third entry at
/// `t0 + WINDOW_GAP_SECS + 1` would spuriously close the window (a gap that
/// never really elapsed against the true last arrival at `t10`).
#[sinex_test]
async fn relation_extractor_out_of_order_entry_does_not_corrupt_gap_watermark()
-> xtask::sandbox::TestResult<()> {
    let mut extractor = RelationExtractor;
    let mut state = RelationExtractorState::default();

    let t10 = Timestamp::from_unix_timestamp(1_700_000_010).expect("valid ts");
    let t0 = Timestamp::from_unix_timestamp(1_700_000_000).expect("valid ts"); // out-of-order, before t10
    let t_after = t10 + Duration::seconds(WINDOW_GAP_SECS - 1);

    extractor
        .reconcile(
            &mut state,
            CO_OCCURRENCE_SCOPE,
            resolved("alpha"),
            &AutomatonContext::timer_flush(t10)?,
        )
        .await?;
    extractor
        .reconcile(
            &mut state,
            CO_OCCURRENCE_SCOPE,
            resolved("beta"),
            &AutomatonContext::timer_flush(t0)?,
        )
        .await?;
    assert_eq!(
        state.last_seen,
        Some(t10),
        "out-of-order entry must not move the last_seen watermark backward"
    );

    // Neutralize the independent WINDOW_FORCE_EMIT_SECS age trigger (which
    // would otherwise also close the window once >60s have passed since the
    // window opened, confounding the gap-specific assertion below) by
    // pinning window_started_at to just before t_after.
    state.window_started_at = Some(t_after - Duration::seconds(1));

    // Only WINDOW_GAP_SECS-1 after the TRUE last arrival (t10) -- must not
    // trigger a gap close.
    let outputs = extractor
        .reconcile(
            &mut state,
            CO_OCCURRENCE_SCOPE,
            resolved("gamma"),
            &AutomatonContext::timer_flush(t_after)?,
        )
        .await?;
    assert!(
        outputs.is_empty(),
        "gap has not really elapsed against the true watermark; the window must still be open"
    );
    assert_eq!(
        state.window.len(),
        3,
        "all three entities remain in the open window"
    );
    Ok(())
}

/// The emitted relation's `ts_orig` must be the window's true latest arrival,
/// not whichever entry happened to be pushed last into the window.
#[sinex_test]
async fn relation_extractor_close_ts_orig_is_max_arrived_at_not_last_pushed()
-> xtask::sandbox::TestResult<()> {
    let mut extractor = RelationExtractor;
    let mut state = RelationExtractorState::default();

    let t_early = Timestamp::from_unix_timestamp(1_700_000_000).expect("valid ts");
    let t_late = Timestamp::from_unix_timestamp(1_700_000_050).expect("valid ts");

    // First entry establishes the window at the LATE time.
    extractor
        .reconcile(
            &mut state,
            CO_OCCURRENCE_SCOPE,
            resolved("alpha"),
            &AutomatonContext::timer_flush(t_late)?,
        )
        .await?;
    // Second entry arrives out-of-order (earlier) and is pushed LAST into the
    // window vector.
    extractor
        .reconcile(
            &mut state,
            CO_OCCURRENCE_SCOPE,
            resolved("beta"),
            &AutomatonContext::timer_flush(t_early)?,
        )
        .await?;

    // Force-close via capacity to inspect the emitted ts_orig directly.
    for i in 0..(MAX_WINDOW_ENTITIES - 2) {
        extractor
            .reconcile(
                &mut state,
                CO_OCCURRENCE_SCOPE,
                resolved(&format!("filler-{i}")),
                &AutomatonContext::timer_flush(t_early)?,
            )
            .await?;
    }
    let outputs = extractor
        .reconcile(
            &mut state,
            CO_OCCURRENCE_SCOPE,
            resolved("closer"),
            &AutomatonContext::timer_flush(t_early)?,
        )
        .await?;

    assert!(
        !outputs.is_empty(),
        "capacity trigger should close and emit pairs"
    );
    for output in &outputs {
        assert_eq!(
            output.ts_orig, t_late,
            "ts_orig must be the window's true max arrived_at, not the last-pushed entry's time"
        );
    }
    Ok(())
}
