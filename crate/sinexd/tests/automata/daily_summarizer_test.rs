use sinex_primitives::activity::ActivitySourceKind;
use sinex_primitives::events::payloads::ActivityHourlySummaryPayload;
use sinex_primitives::temporal::{Duration, Timestamp};
use sinexd::automata::daily::{DailySummarizer, DailySummaryState};
use xtask::sandbox::prelude::*;

use super::summarizer_support::{
    process_windowed_input, summary_context, utc_day_start, utc_hour_start,
};

fn hourly_payload(
    hour_start: Timestamp,
    duration_secs: u64,
    window_count: u64,
    event_count: u64,
    source_window_counts: &[(&str, u64)],
    activity_source_counts: &[(ActivitySourceKind, u64)],
    focus_time_secs_by_source: &[(ActivitySourceKind, u64)],
    primary_source: ActivitySourceKind,
) -> ActivityHourlySummaryPayload {
    let hour_end = hour_start + Duration::hours(1);
    ActivityHourlySummaryPayload {
        hour_id: format!("hour-{}", hour_start.inner().unix_timestamp()),
        hour_start,
        hour_end,
        duration_secs,
        window_count,
        event_count,
        source_count: source_window_counts.len() as u64,
        sources: source_window_counts
            .iter()
            .map(|(source, _)| (*source).to_string())
            .collect(),
        top_sources: source_window_counts
            .iter()
            .map(|(source, _)| (*source).to_string())
            .collect(),
        source_window_counts: source_window_counts
            .iter()
            .map(|(source, count)| ((*source).to_string(), *count))
            .collect(),
        activity_sources: activity_source_counts
            .iter()
            .map(|(source, _)| *source)
            .collect(),
        activity_source_counts: activity_source_counts.iter().copied().collect(),
        focus_time_secs_by_source: focus_time_secs_by_source.iter().copied().collect(),
        primary_source,
    }
}

fn make_context(ts_orig: Timestamp) -> sinexd::runtime::automaton::AutomatonContext {
    summary_context::<ActivityHourlySummaryPayload>(ts_orig)
}

#[sinex_test]
async fn day_boundary_closes_summary_and_seeds_next_day() -> TestResult<()> {
    let mut summarizer = DailySummarizer;
    let mut state = DailySummaryState::default();

    let first_hour = Timestamp::from_unix_timestamp(1_700_000_000).expect("valid timestamp");
    let next_day_hour = first_hour + Duration::days(1);
    let first_ctx = make_context(first_hour + Duration::hours(1));
    let second_ctx = make_context(next_day_hour + Duration::hours(1));

    assert!(
        process_windowed_input(
            &mut summarizer,
            &mut state,
            hourly_payload(
                first_hour,
                900,
                2,
                6,
                &[("shell.kitty", 2)],
                &[(ActivitySourceKind::Terminal, 6)],
                &[(ActivitySourceKind::Terminal, 900)],
                ActivitySourceKind::Terminal,
            ),
            &first_ctx,
        )
        .await?
        .is_none()
    );

    let output = process_windowed_input(
        &mut summarizer,
        &mut state,
        hourly_payload(
            next_day_hour,
            300,
            1,
            1,
            &[("browser.history", 1)],
            &[(ActivitySourceKind::Browser, 1)],
            &[(ActivitySourceKind::Browser, 300)],
            ActivitySourceKind::Browser,
        ),
        &second_ctx,
    )
    .await?
    .expect("new UTC day should close the prior day");

    assert_eq!(output.payload.day_start, utc_day_start(first_hour));
    assert_eq!(
        output.payload.day_end,
        output.payload.day_start + Duration::days(1)
    );
    assert_eq!(output.payload.hour_count, 1);
    assert_eq!(output.payload.window_count, 2);
    assert_eq!(
        output.source_event_ids,
        vec![first_ctx.trigger_event_id.as_uuid().to_owned()]
    );
    assert_eq!(state.hour_count, 1);
    Ok(())
}

#[sinex_test]
async fn daily_summary_aggregates_hourly_rollups() -> TestResult<()> {
    let mut summarizer = DailySummarizer;
    let mut state = DailySummaryState::default();

    let day_start = Timestamp::from_unix_timestamp(1_700_000_000).expect("valid timestamp");
    let first_hour = utc_hour_start(day_start);
    let second_hour = first_hour + Duration::hours(1);
    let next_day_hour = first_hour + Duration::days(1);

    let first_ctx = make_context(first_hour + Duration::hours(1));
    let second_ctx = make_context(second_hour + Duration::hours(1));
    let third_ctx = make_context(next_day_hour + Duration::hours(1));

    assert!(
        process_windowed_input(
            &mut summarizer,
            &mut state,
            hourly_payload(
                first_hour,
                900,
                2,
                6,
                &[("shell.kitty", 2), ("wm.hyprland", 1)],
                &[
                    (ActivitySourceKind::Terminal, 5),
                    (ActivitySourceKind::Window, 1),
                ],
                &[(ActivitySourceKind::Terminal, 900)],
                ActivitySourceKind::Terminal,
            ),
            &first_ctx,
        )
        .await?
        .is_none()
    );
    assert!(
        process_windowed_input(
            &mut summarizer,
            &mut state,
            hourly_payload(
                second_hour,
                600,
                1,
                3,
                &[("shell.kitty", 1)],
                &[(ActivitySourceKind::Terminal, 3)],
                &[(ActivitySourceKind::Terminal, 600)],
                ActivitySourceKind::Terminal,
            ),
            &second_ctx,
        )
        .await?
        .is_none()
    );

    let output = process_windowed_input(
        &mut summarizer,
        &mut state,
        hourly_payload(
            next_day_hour,
            300,
            1,
            1,
            &[("browser.history", 1)],
            &[(ActivitySourceKind::Browser, 1)],
            &[(ActivitySourceKind::Browser, 300)],
            ActivitySourceKind::Browser,
        ),
        &third_ctx,
    )
    .await?
    .expect("next-day hour should roll the previous day");

    assert_eq!(output.payload.hour_count, 2);
    assert_eq!(output.payload.window_count, 3);
    assert_eq!(output.payload.event_count, 9);
    assert_eq!(output.payload.duration_secs, 1500);
    assert_eq!(
        output
            .payload
            .activity_source_counts
            .get(&ActivitySourceKind::Terminal),
        Some(&8)
    );
    assert_eq!(
        output
            .payload
            .focus_time_secs_by_source
            .get(&ActivitySourceKind::Terminal),
        Some(&1500)
    );
    assert_eq!(
        output.payload.top_sources.first().map(String::as_str),
        Some("shell.kitty")
    );
    assert_eq!(output.payload.primary_source, ActivitySourceKind::Terminal);
    Ok(())
}
