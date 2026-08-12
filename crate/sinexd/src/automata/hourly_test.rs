use super::*;
use crate::runtime::automaton::AutomatonContext;
use sinex_primitives::activity::ActivitySourceKind;
use sinex_primitives::events::payloads::ActivityWindowCloseReason;
use xtask::sandbox::sinex_test;

fn window(window_start: Timestamp, window_end: Timestamp) -> ActivityWindowSummaryPayload {
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
        close_reason: ActivityWindowCloseReason::Gap,
    }
}

/// sinex-audit-outoforder-pattern: a late window whose bucket is BEFORE the
/// currently-open hour must not be treated as a forward bucket transition.
/// Reverting the direction check (`bucket_start < current_bucket` back to
/// `current_bucket != bucket_start`) makes this test fail: the late window
/// would spuriously mark the window complete after only the second
/// `accumulate` call, forcing a premature flush of the still-open current
/// hour and reopening the already-elapsed earlier hour as "current".
#[sinex_test]
async fn hourly_late_window_does_not_reopen_earlier_bucket() -> xtask::sandbox::TestResult<()> {
    let mut summarizer = HourlySummarizer;
    let mut state = HourlySummaryState::default();

    let base = Timestamp::from_unix_timestamp(1_700_000_000).expect("valid ts"); // hour H
    let late = base - time::Duration::seconds(3600 * 2); // hour H-2

    let ctx1 = AutomatonContext::timer_flush(base)?;
    summarizer
        .accumulate(&mut state, window(base, base), &ctx1)
        .await?;
    assert_eq!(state.window_count, 1);
    assert!(!summarizer.window_complete(&state));

    let ctx2 = AutomatonContext::timer_flush(late)?;
    summarizer
        .accumulate(&mut state, window(late, late), &ctx2)
        .await?;

    assert_eq!(
        state.window_count, 1,
        "the late window must be dropped, not accumulated into the current hour"
    );
    assert!(
        !summarizer.window_complete(&state),
        "a late (earlier-bucket) window must not mark the current hour complete -- \
         doing so would force a premature flush and reopen an already-elapsed hour"
    );
    Ok(())
}

/// A genuinely forward transition (next bucket, later in time) still closes
/// the current hour normally.
#[sinex_test]
async fn hourly_forward_bucket_still_closes_current_hour() -> xtask::sandbox::TestResult<()> {
    let mut summarizer = HourlySummarizer;
    let mut state = HourlySummaryState::default();

    let base = Timestamp::from_unix_timestamp(1_700_000_000).expect("valid ts");
    let next = base + time::Duration::seconds(3600 * 2);

    let ctx1 = AutomatonContext::timer_flush(base)?;
    summarizer
        .accumulate(&mut state, window(base, base), &ctx1)
        .await?;

    let ctx2 = AutomatonContext::timer_flush(next)?;
    summarizer
        .accumulate(&mut state, window(next, next), &ctx2)
        .await?;

    assert!(
        summarizer.window_complete(&state),
        "a genuinely later bucket must still close the current hour"
    );
    let output = summarizer
        .emit(&mut state, &AutomatonContext::timer_flush(next)?)
        .await?
        .expect("closed hour should emit a summary");
    assert_eq!(
        output.payload.window_count, 1,
        "only the base-hour window contributes"
    );
    assert_eq!(
        state.window_count, 1,
        "the pending next-hour window is now the open bucket"
    );
    Ok(())
}

/// sinex-4wop: `accumulate`'s next-bucket branch does `state.pending_window =
/// Some(...)` -- a bare overwrite, not an accumulation. If a SECOND
/// boundary-crossing event arrives (also past the current hour) before the
/// current hour is flushed, it silently replaces the first pending event
/// instead of both contributing to the new hour once opened. Real data loss
/// on flush lag, not just a duplicate/ordering quirk.
#[sinex_test]
#[ignore = "sinex-4wop open: pending_window overwrite drops the first of two boundary-crossing events on flush lag"]
async fn hourly_pending_window_overwrite_does_not_drop_boundary_crossing_events()
-> xtask::sandbox::TestResult<()> {
    let mut summarizer = HourlySummarizer;
    let mut state = HourlySummaryState::default();

    let base = Timestamp::from_unix_timestamp(1_700_000_000).expect("valid ts"); // hour H
    let next_a = base + time::Duration::seconds(3600); // hour H+1, event A
    let next_b = next_a + time::Duration::seconds(100); // hour H+1, event B (same bucket as A)

    // Open hour H.
    summarizer
        .accumulate(&mut state, window(base, base), &AutomatonContext::timer_flush(base)?)
        .await?;
    assert_eq!(state.window_count, 1);

    // Event A crosses into H+1 before H is flushed -- becomes the pending window.
    summarizer
        .accumulate(
            &mut state,
            window(next_a, next_a),
            &AutomatonContext::timer_flush(next_a)?,
        )
        .await?;
    assert!(summarizer.window_complete(&state));

    // Event B ALSO crosses into H+1, arriving before H is ever flushed. Today
    // this overwrites the pending window instead of accumulating alongside A.
    summarizer
        .accumulate(
            &mut state,
            window(next_b, next_b),
            &AutomatonContext::timer_flush(next_b)?,
        )
        .await?;

    // Flush H, which should seed the new H+1 bucket from whatever was pending.
    let output = summarizer
        .emit(&mut state, &AutomatonContext::timer_flush(next_b)?)
        .await?
        .expect("closed hour H should emit a summary");
    assert_eq!(output.payload.window_count, 1, "only hour H's own window contributes to H's summary");

    assert_eq!(
        state.window_count, 2,
        "hour H+1 should have been seeded by BOTH boundary-crossing events (A and B), \
         but the pending_window overwrite means only the most recent one (B) survived -- \
         event A was silently dropped with no warning or durable debt record."
    );
    Ok(())
}
