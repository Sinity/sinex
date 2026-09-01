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

/// A late bucket is durable failure evidence instead of a successfully
/// consumed input.
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
    let error = summarizer
        .accumulate(&mut state, hourly(late), &ctx2)
        .await
        .expect_err("a late hourly summary must become durable failure evidence");

    assert_eq!(
        state.hour_count, 1,
        "the late hour must not mutate the current day"
    );
    assert!(
        !summarizer.window_complete(&state),
        "a late hour must not mark the current day complete"
    );
    assert!(error.to_string().contains("late hourly summary"));
    Ok(())
}

/// sinex-2ged integration: `flush_due` must respect the operator-local civil
/// day's REAL length across a DST transition, not a hardcoded 24h assumption.
/// A spring-forward day (Europe/Warsaw, 2024-03-31) is only 23 hours; if
/// `flush_due` ever regressed to comparing against `day_start + 24h` instead
/// of `civil::civil_day_end(day_start)`, it would wait one hour too long to
/// flush the trailing day.
#[sinex_test]
async fn daily_flush_due_respects_dst_shortened_day() -> xtask::sandbox::TestResult<()> {
    let mut summarizer = DailySummarizer;
    let mut state = DailySummaryState::default();

    // 2024-03-31 10:00 UTC == local noon (CEST, after the 02:00->03:00 jump).
    let noon = Timestamp::from_unix_timestamp(1_711_879_200).expect("valid ts");
    let day_start = crate::automata::civil::floor_to_civil_day(noon);
    let true_day_end = crate::automata::civil::civil_day_end(day_start); // 23h after day_start

    let ctx = AutomatonContext::timer_flush(noon)?;
    summarizer
        .accumulate(&mut state, hourly(noon), &ctx)
        .await?;

    // One hour before the true (23h) day end: not yet due.
    let one_hour_early = true_day_end - time::Duration::seconds(3600);
    assert!(
        !summarizer.flush_due(&state, one_hour_early),
        "must not flush before the DST-shortened day has actually elapsed"
    );

    // At the true day end (23h after start, not 24h): due.
    assert!(
        summarizer.flush_due(&state, true_day_end),
        "must flush at the real 23h civil-day boundary on a spring-forward day"
    );

    // A naive day_start + 24h boundary is one hour LATER than the true end on
    // this day -- confirm flush_due does not depend on that wrong value by
    // checking it's already true a full hour before the naive mark.
    let naive_24h_end = day_start + time::Duration::seconds(86400);
    assert!(
        true_day_end < naive_24h_end,
        "sanity: the spring-forward day is genuinely shorter than 24h"
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
