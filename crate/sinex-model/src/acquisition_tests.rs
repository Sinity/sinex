use super::*;

fn clock(text: &str) -> OffsetDateTime {
    OffsetDateTime::parse(text, &Rfc3339).unwrap()
}

#[test]
fn completed_source_becomes_overdue_after_next_start() {
    let start = clock("2026-09-14T09:00:00Z");
    let plan = PollSchedule::after(start, 1000).unwrap();
    assert_eq!(
        state(Some("completed"), Some(&plan), start),
        AcquisitionState::Completed
    );
    assert_eq!(
        state(
            Some("completed"),
            Some(&plan),
            start + time::Duration::seconds(1)
        ),
        AcquisitionState::Completed
    );
    assert_eq!(
        state(
            Some("completed"),
            Some(&plan),
            start + time::Duration::seconds(2)
        ),
        AcquisitionState::Overdue
    );
    assert_eq!(
        state(
            Some("reading"),
            Some(&plan),
            start + time::Duration::hours(1)
        ),
        AcquisitionState::Reading
    );
    assert_eq!(
        state(
            Some("unavailable"),
            Some(&plan),
            start + time::Duration::hours(1)
        ),
        AcquisitionState::Unavailable
    );
}
#[test]
fn completed_source_without_schedule_is_never_overdue() {
    let now = clock("2026-09-14T09:00:00Z");
    assert_eq!(state(None, None, now), AcquisitionState::Unknown);
    assert_eq!(
        state(Some("completed"), None, now),
        AcquisitionState::Completed
    );
    assert_eq!(
        state(Some("unavailable"), None, now),
        AcquisitionState::Unavailable
    );
}
#[test]
fn poll_schedule_rejects_bad_intervals_and_edited_times() {
    let now = clock("2026-09-14T09:00:00Z");
    assert!(PollSchedule::after(now, u64::MAX).is_err());
    assert!(PollSchedule::after(now, 0).is_err());
    assert!(PollSchedule::after(clock("9999-12-31T23:59:59Z"), 1000).is_err());
    let mut plan = PollSchedule::after(now, 1000).unwrap();
    plan.next_start_at = "2026-09-14T10:00:00Z".into();
    assert!(plan.validate().is_err());
}
#[test]
fn sources_request_continuations_need_their_parent() {
    let id = Uuid::now_v7();
    assert!(SourcesRequest::default().validate().is_ok());
    assert!(
        SourcesRequest {
            before_seq: Some(3),
            ..Default::default()
        }
        .validate()
        .is_err()
    );
    assert!(
        SourcesRequest {
            input: Some(id),
            after_receipt: Some(id),
            ..Default::default()
        }
        .validate()
        .is_err()
    );
    assert!(
        SourcesRequest {
            input: Some(id),
            observation: Some(id),
            after_receipt: Some(id),
            ..Default::default()
        }
        .validate()
        .is_ok()
    );
    assert!(
        SourcesRequest {
            input: Some(id),
            after_input: Some(id),
            ..Default::default()
        }
        .validate()
        .is_err()
    );
}

#[test]
fn outcome_refs_keep_last_failure_across_successes() {
    let failed = Uuid::now_v7();
    let mut refs = OutcomeRefs::following(Some((failed, "unavailable", None)));
    assert!(!refs.success_known && refs.failure_known);
    for _ in 0..1000 {
        let success = Uuid::now_v7();
        refs = OutcomeRefs::following(Some((success, "completed", Some(refs))));
        assert_eq!(refs.last_failure, Some(failed));
        assert_eq!(refs.last_success, Some(success));
        assert!(refs.success_known && refs.failure_known);
    }
    let initial = OutcomeRefs::following(None);
    assert!(initial.success_known && initial.last_success.is_none());
}

#[test]
fn only_the_running_collector_matches_its_observations() {
    let run = Uuid::now_v7();
    let active = CollectorScope {
        source: "terminal.atuin-history".into(),
        locator: "/synthetic/history.db".into(),
        run,
    };
    assert!(is_active_collector(
        std::slice::from_ref(&active),
        "terminal.atuin-history",
        &active.locator,
        Some(run)
    ));
    assert!(!is_active_collector(
        &[],
        "terminal.atuin-history",
        &active.locator,
        Some(run)
    ));
    assert!(!is_active_collector(
        std::slice::from_ref(&active),
        "terminal.atuin-history",
        "/synthetic/old.db",
        Some(run)
    ));
    assert!(!is_active_collector(
        std::slice::from_ref(&active),
        "terminal.atuin-history",
        &active.locator,
        Some(Uuid::now_v7())
    ));
    assert!(!is_active_collector(
        std::slice::from_ref(&active),
        "terminal.atuin-history",
        &active.locator,
        None
    ));
}

#[test]
fn atuin_checkpoint_reused_only_for_same_reader_and_schema_before_due() {
    let epoch = Uuid::now_v7();
    let now = OffsetDateTime::parse("2026-10-17T10:00:00Z", &Rfc3339).unwrap();
    let mut checkpoint = AtuinCheckpoint {
        version: 1,
        epoch,
        schema_version: 3,
        user_version: 7,
        through_rowid: 999,
        full_completed_at: "2026-10-17T09:00:00Z".into(),
        full_due_at: "2026-10-18T09:00:00Z".into(),
        needs_full: false,
    };
    assert!(checkpoint.reusable(epoch, 3, 7, now));
    assert!(!checkpoint.reusable(Uuid::now_v7(), 3, 7, now));
    assert!(!checkpoint.reusable(epoch, 4, 7, now));
    assert!(!checkpoint.reusable(epoch, 3, 8, now));
    assert!(!checkpoint.reusable(epoch, 3, 7, now + time::Duration::days(1)));
    checkpoint.needs_full = true;
    assert!(!checkpoint.reusable(epoch, 3, 7, now));
    checkpoint.needs_full = false;
    checkpoint.full_due_at = "unknown".into();
    assert!(!checkpoint.reusable(epoch, 3, 7, now));
}
