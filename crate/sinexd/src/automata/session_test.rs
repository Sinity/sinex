use super::*;
use crate::runtime::automaton::AutomatonContext;
use sinex_primitives::activity::ActivitySourceKind;
use sinex_primitives::events::payloads::ActivityWindowCloseReason;
use xtask::sandbox::sinex_test;

fn activity_window(
    window_start: Timestamp,
    window_end: Timestamp,
    close_reason: ActivityWindowCloseReason,
) -> ActivityWindowSummaryPayload {
    ActivityWindowSummaryPayload {
        window_id: format!("w-{}", window_end.inner().unix_timestamp()),
        window_start,
        window_end,
        duration_secs: (window_end - window_start).whole_seconds().max(0) as u64,
        event_count: 1,
        source_count: 1,
        sources: vec!["wm.hyprland".to_string()],
        activity_sources: vec![ActivitySourceKind::Window],
        activity_source_counts: BTreeMap::from([(ActivitySourceKind::Window, 1)]),
        primary_source: ActivitySourceKind::Window,
        close_reason,
    }
}

/// sinex-audit-outoforder-pattern: `last_window_end` must never move backward.
/// An out-of-order (late-arriving) window with an earlier `window_end` must
/// not corrupt the session's tracked end time -- reverting the monotonic
/// guard makes this test fail: `last_window_end` would regress to the
/// out-of-order window's earlier end, and the final emitted `end_time` /
/// `duration_secs` would undercount the session.
#[sinex_test]
async fn session_out_of_order_window_does_not_move_end_time_backward()
-> xtask::sandbox::TestResult<()> {
    let mut detector = SessionDetector;
    let mut state = SessionState::default();

    let t0 = Timestamp::from_unix_timestamp(1_700_000_000).expect("valid ts");
    let t_far = t0 + time::Duration::seconds(120); // genuinely later window end
    let t_stale = t0 + time::Duration::seconds(10); // out-of-order: earlier than t_far

    let ctx1 = AutomatonContext::timer_flush(t0)?;
    detector
        .accumulate(
            &mut state,
            activity_window(t0, t_far, ActivityWindowCloseReason::MaxDuration),
            &ctx1,
        )
        .await?;
    assert_eq!(state.last_window_end, Some(t_far));

    // Out-of-order window: its window_end (t_stale) is BEFORE the tracked
    // watermark (t_far).
    let ctx2 = AutomatonContext::timer_flush(t_stale)?;
    detector
        .accumulate(
            &mut state,
            activity_window(t0, t_stale, ActivityWindowCloseReason::MaxDuration),
            &ctx2,
        )
        .await?;
    assert_eq!(
        state.last_window_end,
        Some(t_far),
        "out-of-order window must not move last_window_end backward"
    );
    // The window still contributes to session accounting (event/window counts).
    assert_eq!(state.window_count, 2);

    // Close the session normally.
    let close_end = t0 + time::Duration::seconds(200);
    let ctx3 = AutomatonContext::timer_flush(close_end)?;
    detector
        .accumulate(
            &mut state,
            activity_window(t_far, close_end, ActivityWindowCloseReason::Gap),
            &ctx3,
        )
        .await?;
    assert!(detector.window_complete(&state));

    let output = detector
        .emit(&mut state, &AutomatonContext::timer_flush(close_end)?)
        .await?
        .expect("gap-closed session should emit a boundary");
    assert_eq!(output.payload.end_time, close_end);
    assert_eq!(output.payload.duration_secs, 200);
    Ok(())
}

/// sinex-5s6: clock-driven closure backstop. A session whose final
/// contributing window closed for a reason OTHER than `Gap` (e.g.
/// `MaxDuration`) never sets `session_complete`, so without a wall-clock
/// backstop it would sit open forever waiting for an activity event that
/// silence never delivers (interval ends must not require future activity).
/// `flush_due` closes the gap: it must stay false before the watermark
/// crosses `last_window_end + window_gap_threshold()`, flip true exactly at
/// and beyond that boundary, and stay false once the session already
/// completed normally (avoiding a redundant double-flush) or has no windows
/// at all (nothing to flush).
#[sinex_test]
async fn session_flush_due_backstops_non_gap_trailing_close() -> xtask::sandbox::TestResult<()> {
    let mut detector = SessionDetector;
    let mut state = SessionState::default();

    let t0 = Timestamp::from_unix_timestamp(1_700_000_000).expect("valid ts");
    let window_end = t0 + time::Duration::seconds(60);
    let threshold = crate::automata::analytics::window_gap_threshold();

    // No windows accumulated yet: never due, regardless of watermark.
    assert!(
        !detector.flush_due(
            &state,
            window_end + threshold + time::Duration::seconds(3600)
        ),
        "an empty session must never be flush-due"
    );

    let ctx = AutomatonContext::timer_flush(window_end)?;
    detector
        .accumulate(
            &mut state,
            activity_window(t0, window_end, ActivityWindowCloseReason::MaxDuration),
            &ctx,
        )
        .await?;
    assert!(
        !detector.window_complete(&state),
        "a MaxDuration close (not Gap) must not mark the session complete on its own"
    );

    // Strictly before the gap threshold: not due yet.
    let just_before = window_end + threshold - time::Duration::seconds(1);
    assert!(
        !detector.flush_due(&state, just_before),
        "flush_due must not fire before the gap threshold has fully elapsed"
    );

    // Exactly at the threshold: due (the boundary is inclusive, `>=`).
    let at_threshold = window_end + threshold;
    assert!(
        detector.flush_due(&state, at_threshold),
        "flush_due must fire at the exact gap-threshold boundary, not only strictly after it"
    );

    // Well past the threshold: still due.
    let well_after = window_end + threshold + time::Duration::seconds(3600);
    assert!(detector.flush_due(&state, well_after));

    // A session that already completed normally (Gap close) must not also
    // report flush-due -- that would be a redundant second flush path for
    // the same closed session.
    let mut gap_state = SessionState::default();
    let gap_ctx = AutomatonContext::timer_flush(window_end)?;
    detector
        .accumulate(
            &mut gap_state,
            activity_window(t0, window_end, ActivityWindowCloseReason::Gap),
            &gap_ctx,
        )
        .await?;
    assert!(detector.window_complete(&gap_state));
    assert!(
        !detector.flush_due(&gap_state, well_after),
        "an already-Gap-completed session must not also be reported as flush-due"
    );
    Ok(())
}

/// A genuinely negative duration (malformed window_start/window_end, not
/// out-of-order arrival) is flagged via clamp-to-zero rather than silently
/// producing an unnoticed negative value.
#[sinex_test]
async fn session_malformed_window_clamps_negative_duration_to_zero()
-> xtask::sandbox::TestResult<()> {
    let mut detector = SessionDetector;
    let mut state = SessionState::default();

    let start = Timestamp::from_unix_timestamp(1_700_000_100).expect("valid ts");
    let malformed_end = Timestamp::from_unix_timestamp(1_700_000_000).expect("valid ts"); // < start

    let ctx = AutomatonContext::timer_flush(malformed_end)?;
    detector
        .accumulate(
            &mut state,
            activity_window(start, malformed_end, ActivityWindowCloseReason::Gap),
            &ctx,
        )
        .await?;
    assert!(detector.window_complete(&state));

    let output = detector
        .emit(&mut state, &AutomatonContext::timer_flush(malformed_end)?)
        .await?
        .expect("gap-closed session should emit a boundary");
    assert_eq!(
        output.payload.duration_secs, 0,
        "negative duration is clamped to 0"
    );
    Ok(())
}
