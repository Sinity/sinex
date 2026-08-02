use super::*;
use crate::runtime::automaton::AutomatonContext;
use sinex_primitives::activity::ActivitySourceKind;
use xtask::sandbox::sinex_test;

fn hourly(hour_start: Timestamp) -> ActivityHourlySummaryPayload {
    ActivityHourlySummaryPayload {
        hour_id: format!("h-{}", hour_start.inner().unix_timestamp()),
        hour_start,
        hour_end: hour_start + time::Duration::seconds(3600),
        duration_secs: 3600,
        window_count: 1,
        event_count: 1,
        source_count: 1,
        sources: vec!["wm.hyprland".to_string()],
        top_sources: vec!["wm.hyprland".to_string()],
        source_window_counts: BTreeMap::from([("wm.hyprland".to_string(), 1)]),
        activity_sources: vec![ActivitySourceKind::Window],
        activity_source_counts: BTreeMap::from([(ActivitySourceKind::Window, 1)]),
        focus_time_secs_by_source: BTreeMap::from([(ActivitySourceKind::Window, 3600)]),
        primary_source: ActivitySourceKind::Window,
    }
}

/// sinex-audit-outoforder-pattern: a late hourly summary whose bucket is
/// BEFORE the currently-open day must not be treated as a forward bucket
/// transition. Reverting the direction check (`bucket_start < current_bucket`
/// back to `current_bucket != bucket_start`) makes this test fail: the late
/// hour would spuriously mark the day complete after only the second
/// `accumulate` call, forcing a premature flush of the still-open current day
/// and reopening the already-elapsed earlier day as "current".
#[sinex_test]
async fn daily_late_hour_does_not_reopen_earlier_bucket() -> xtask::sandbox::TestResult<()> {
    let mut summarizer = DailySummarizer;
    let mut state = DailySummaryState::default();

    let base = Timestamp::from_unix_timestamp(1_700_000_000).expect("valid ts"); // day D
    let late = base - time::Duration::seconds(86400 * 2); // day D-2

    let ctx1 = AutomatonContext::timer_flush(base)?;
    summarizer
        .accumulate(&mut state, hourly(base), &ctx1)
        .await?;
    assert_eq!(state.hour_count, 1);
    assert!(!summarizer.window_complete(&state));

    let ctx2 = AutomatonContext::timer_flush(late)?;
    summarizer
        .accumulate(&mut state, hourly(late), &ctx2)
        .await?;

    assert_eq!(
        state.hour_count, 1,
        "the late hour must be dropped, not accumulated into the current day"
    );
    assert!(
        !summarizer.window_complete(&state),
        "a late (earlier-bucket) hour must not mark the current day complete -- \
         doing so would force a premature flush and reopen an already-elapsed day"
    );
    Ok(())
}

/// A genuinely forward transition (next bucket, later in time) still closes
/// the current day normally.
#[sinex_test]
async fn daily_forward_bucket_still_closes_current_day() -> xtask::sandbox::TestResult<()> {
    let mut summarizer = DailySummarizer;
    let mut state = DailySummaryState::default();

    let base = Timestamp::from_unix_timestamp(1_700_000_000).expect("valid ts");
    let next = base + time::Duration::seconds(86400 * 2);

    let ctx1 = AutomatonContext::timer_flush(base)?;
    summarizer
        .accumulate(&mut state, hourly(base), &ctx1)
        .await?;

    let ctx2 = AutomatonContext::timer_flush(next)?;
    summarizer
        .accumulate(&mut state, hourly(next), &ctx2)
        .await?;

    assert!(
        summarizer.window_complete(&state),
        "a genuinely later bucket must still close the current day"
    );
    let output = summarizer
        .emit(&mut state, &AutomatonContext::timer_flush(next)?)
        .await?
        .expect("closed day should emit a summary");
    assert_eq!(
        output.payload.hour_count, 1,
        "only the base-day hour contributes"
    );
    assert_eq!(
        state.hour_count, 1,
        "the pending next-day hour is now the open bucket"
    );
    Ok(())
}
