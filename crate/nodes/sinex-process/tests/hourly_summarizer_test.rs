use sinex_process::automata::hourly::{HourlySummarizer, HourlySummaryState};
use sinex_node_sdk::derived_node::{DerivedOutput, DerivedTriggerContext};
use sinex_node_sdk::{NodeLogicError, WindowedNode};
use sinex_primitives::activity::ActivitySourceKind;
use sinex_primitives::domain::{ProcessingMode, TriggerKind};
use sinex_primitives::events::payloads::{
    ActivityHourlySummaryPayload, ActivityWindowCloseReason, ActivityWindowSummaryPayload,
};
use sinex_primitives::events::{Event, EventPayload};
use sinex_primitives::temporal::{Duration, Timestamp};
use sinex_primitives::{Id, JsonValue};
use xtask::sandbox::prelude::*;

fn make_context(ts_orig: Timestamp) -> DerivedTriggerContext {
    let event_id: Id<Event<JsonValue>> = Id::new();
    DerivedTriggerContext {
        trigger_event_id: event_id,
        source: ActivityWindowSummaryPayload::SOURCE,
        event_type: ActivityWindowSummaryPayload::EVENT_TYPE,
        ts_orig: Some(ts_orig),
        ts_coided: event_id.timestamp(),
        processing_mode: ProcessingMode::Live,
        trigger_kind: TriggerKind::NewEvent,
        created_by_operation_id: None,
    }
}

fn window_payload(
    window_start: Timestamp,
    window_end: Timestamp,
    duration_secs: u64,
    event_count: u64,
    sources: &[&str],
    activity_source_counts: &[(ActivitySourceKind, u64)],
    primary_source: ActivitySourceKind,
) -> ActivityWindowSummaryPayload {
    ActivityWindowSummaryPayload {
        window_id: format!("window-{}", window_start.inner().unix_timestamp()),
        window_start,
        window_end,
        duration_secs,
        event_count,
        source_count: sources.len() as u64,
        sources: sources.iter().map(|source| (*source).to_string()).collect(),
        activity_sources: activity_source_counts
            .iter()
            .map(|(source, _)| *source)
            .collect(),
        activity_source_counts: activity_source_counts.iter().copied().collect(),
        primary_source,
        close_reason: ActivityWindowCloseReason::Gap,
    }
}

async fn process(
    summarizer: &mut HourlySummarizer,
    state: &mut HourlySummaryState,
    payload: ActivityWindowSummaryPayload,
    context: &DerivedTriggerContext,
) -> Result<Option<DerivedOutput<ActivityHourlySummaryPayload>>, NodeLogicError> {
    summarizer.accumulate(state, payload, context).await?;
    if summarizer.window_complete(state) {
        summarizer.emit(state, context).await
    } else {
        Ok(None)
    }
}

#[sinex_test]
async fn hour_boundary_closes_summary_and_seeds_next_hour() -> TestResult<()> {
    let mut summarizer = HourlySummarizer;
    let mut state = HourlySummaryState::default();

    let first_start = Timestamp::from_unix_timestamp(1_700_000_000).expect("valid timestamp");
    let first_end = first_start + Duration::minutes(10);
    let second_start = first_start + Duration::minutes(65);
    let second_end = second_start + Duration::minutes(5);

    let first_ctx = make_context(first_end);
    let second_ctx = make_context(second_end);

    assert!(
        process(
            &mut summarizer,
            &mut state,
            window_payload(
                first_start,
                first_end,
                600,
                3,
                &["shell.kitty"],
                &[(ActivitySourceKind::Terminal, 3)],
                ActivitySourceKind::Terminal,
            ),
            &first_ctx,
        )
        .await?
        .is_none()
    );

    let output = process(
        &mut summarizer,
        &mut state,
        window_payload(
            second_start,
            second_end,
            300,
            1,
            &["wm.hyprland"],
            &[(ActivitySourceKind::Window, 1)],
            ActivitySourceKind::Window,
        ),
        &second_ctx,
    )
    .await?
    .expect("new UTC hour should close the prior hour");

    assert_eq!(
        output.payload.hour_start,
        Timestamp::from(
            first_end
                .inner()
                .replace_minute(0)
                .unwrap()
                .replace_second(0)
                .unwrap()
                .replace_nanosecond(0)
                .unwrap()
        )
    );
    assert_eq!(
        output.payload.hour_end,
        output.payload.hour_start + Duration::hours(1)
    );
    assert_eq!(output.payload.window_count, 1);
    assert_eq!(output.payload.event_count, 3);
    assert_eq!(
        output.source_event_ids,
        vec![first_ctx.trigger_event_id.as_uuid().to_owned()]
    );

    assert_eq!(state.window_count, 1);
    assert_eq!(state.event_count, 1);
    Ok(())
}

#[sinex_test]
async fn hourly_summary_aggregates_focus_time_and_top_sources() -> TestResult<()> {
    let mut summarizer = HourlySummarizer;
    let mut state = HourlySummaryState::default();

    let base = Timestamp::from_unix_timestamp(1_700_000_000).expect("valid timestamp");
    let first_end = base + Duration::minutes(10);
    let second_end = base + Duration::minutes(25);
    let third_end = base + Duration::minutes(80);

    let first_ctx = make_context(first_end);
    let second_ctx = make_context(second_end);
    let third_ctx = make_context(third_end);

    assert!(
        process(
            &mut summarizer,
            &mut state,
            window_payload(
                base,
                first_end,
                600,
                4,
                &["shell.kitty", "wm.hyprland"],
                &[
                    (ActivitySourceKind::Terminal, 3),
                    (ActivitySourceKind::Window, 1),
                ],
                ActivitySourceKind::Terminal,
            ),
            &first_ctx,
        )
        .await?
        .is_none()
    );
    assert!(
        process(
            &mut summarizer,
            &mut state,
            window_payload(
                base + Duration::minutes(15),
                second_end,
                300,
                2,
                &["shell.kitty"],
                &[(ActivitySourceKind::Terminal, 2)],
                ActivitySourceKind::Terminal,
            ),
            &second_ctx,
        )
        .await?
        .is_none()
    );

    let output = process(
        &mut summarizer,
        &mut state,
        window_payload(
            base + Duration::minutes(70),
            third_end,
            300,
            1,
            &["browser.history"],
            &[(ActivitySourceKind::Browser, 1)],
            ActivitySourceKind::Browser,
        ),
        &third_ctx,
    )
    .await?
    .expect("third window should roll the previous hour");

    assert_eq!(output.payload.window_count, 2);
    assert_eq!(output.payload.event_count, 6);
    assert_eq!(output.payload.duration_secs, 900);
    assert_eq!(
        output
            .payload
            .activity_source_counts
            .get(&ActivitySourceKind::Terminal),
        Some(&5)
    );
    assert_eq!(
        output
            .payload
            .focus_time_secs_by_source
            .get(&ActivitySourceKind::Terminal),
        Some(&900)
    );
    assert_eq!(
        output.payload.top_sources.first().map(String::as_str),
        Some("shell.kitty")
    );
    assert_eq!(output.payload.primary_source, ActivitySourceKind::Terminal);
    Ok(())
}
