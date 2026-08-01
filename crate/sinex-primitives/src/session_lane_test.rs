use super::*;
use crate::activity::ActivitySourceKind;
use std::collections::BTreeMap;
use time::Duration;

fn window(
    id: &str,
    start: Timestamp,
    duration_secs: i64,
    event_count: u64,
    close_reason: ActivityWindowCloseReason,
) -> ActivityWindowSummaryPayload {
    let end = start + Duration::seconds(duration_secs.max(0));
    let mut counts = BTreeMap::new();
    counts.insert(ActivitySourceKind::Window, event_count);
    ActivityWindowSummaryPayload {
        window_id: id.to_string(),
        window_start: start,
        window_end: end,
        duration_secs: duration_secs.max(0) as u64,
        event_count,
        source_count: 1,
        sources: vec!["test-source".to_string()],
        activity_sources: vec![ActivitySourceKind::Window],
        activity_source_counts: counts,
        primary_source: ActivitySourceKind::Window,
        close_reason,
    }
}

/// Two windows closed by a `Gap` form exactly one completed session — the
/// core grouping behavior `compute_session_boundaries` must reproduce from
/// `SessionDetector`. Mutating the `Gap` check to any other close reason (or
/// dropping it) makes this red.
#[test]
fn compute_session_boundaries_groups_until_gap_close() {
    let start = Timestamp::now();
    let windows = vec![
        window(
            "w1",
            start,
            60,
            5,
            ActivityWindowCloseReason::MaxDuration,
        ),
        window(
            "w2",
            start + Duration::seconds(60),
            60,
            3,
            ActivityWindowCloseReason::Gap,
        ),
    ];

    let boundaries = compute_session_boundaries(windows);
    assert_eq!(boundaries.len(), 1);
    let session = &boundaries[0];
    assert_eq!(session.session_key, "activity-session:w1");
    assert_eq!(session.event_count, 8);
    assert_eq!(session.window_count, 2);
    assert_eq!(session.duration_secs, 120);
}

/// A trailing group with no `Gap` close is still flushed as a session (the
/// batch analog of the automaton's clock-driven `flush_due` backstop) —
/// otherwise a shadow lane over a bounded scope would silently drop the
/// final, still-open session.
#[test]
fn compute_session_boundaries_flushes_trailing_group_without_gap() {
    let start = Timestamp::now();
    let windows = vec![window(
        "w1",
        start,
        30,
        2,
        ActivityWindowCloseReason::MaxDuration,
    )];

    let boundaries = compute_session_boundaries(windows);
    assert_eq!(boundaries.len(), 1);
    assert_eq!(boundaries[0].session_key, "activity-session:w1");
    assert_eq!(boundaries[0].window_count, 1);
}

/// A `Gap` close followed by a fresh window starts a NEW session — proves
/// the accumulator resets rather than treating everything as one session.
#[test]
fn compute_session_boundaries_starts_new_session_after_gap() {
    let start = Timestamp::now();
    let windows = vec![
        window("w1", start, 30, 1, ActivityWindowCloseReason::Gap),
        window(
            "w2",
            start + Duration::seconds(600),
            30,
            1,
            ActivityWindowCloseReason::Gap,
        ),
    ];

    let boundaries = compute_session_boundaries(windows);
    assert_eq!(boundaries.len(), 2);
    assert_eq!(boundaries[0].session_key, "activity-session:w1");
    assert_eq!(boundaries[1].session_key, "activity-session:w2");
}

/// `SessionLaneOutputs::diff` correctly classifies new/missing/changed
/// sessions — the shadow-diff half of the AC (typed examples over
/// `LaneDiffReport`).
#[test]
fn session_lane_outputs_diff_classifies_changes() {
    let start = Timestamp::now();
    let unchanged = SessionBoundaryOutput {
        session_key: "activity-session:stable".to_string(),
        start_time: start,
        end_time: start + Duration::seconds(60),
        duration_secs: 60,
        event_count: 4,
        window_count: 1,
        primary_source: ActivitySourceKind::Window,
        metadata: serde_json::json!({}),
    };
    let mut changed_candidate = unchanged.clone();
    changed_candidate.session_key = "activity-session:changed".to_string();
    let mut changed_baseline = changed_candidate.clone();
    changed_baseline.duration_secs = 30;
    changed_baseline.event_count = 1;

    let baseline = SessionLaneOutputs {
        boundaries: vec![
            unchanged.clone(),
            changed_baseline,
            SessionBoundaryOutput {
                session_key: "activity-session:missing".to_string(),
                ..unchanged.clone()
            },
        ],
    };
    let candidate = SessionLaneOutputs {
        boundaries: vec![
            unchanged,
            changed_candidate,
            SessionBoundaryOutput {
                session_key: "activity-session:new".to_string(),
                start_time: start,
                end_time: start + Duration::seconds(60),
                duration_secs: 60,
                event_count: 2,
                window_count: 1,
                primary_source: ActivitySourceKind::Window,
                metadata: serde_json::json!({}),
            },
        ],
    };

    let (summary, counts, examples) = SessionLaneOutputs::diff(&baseline, &candidate, 10);
    assert_eq!(counts.session_new, 1);
    assert_eq!(counts.session_missing, 1);
    assert_eq!(counts.duration_changed, 1);
    assert_eq!(counts.event_count_changed, 1);
    assert_eq!(summary.added, 1);
    assert_eq!(summary.removed, 1);
    assert_eq!(summary.unchanged, 1);
    assert!(!examples.is_empty());
}
