use super::*;
#[test]
fn wall_clock_rollback_never_postpones_a_due_full_or_snapshot() {
    let epoch = Uuid::now_v7();
    let mut checkpoint = Checkpoint {
        version: 1,
        epoch,
        generation: Uuid::now_v7(),
        identity: sinex_model::browser::FileIdentity {
            device: 1,
            inode: 1,
            resolved: "/synthetic/History".into(),
        },
        schema_version: 1,
        visits_sequence: 10,
        visits_through: 10,
        download_max_coordinate: 1,
        full_completed_at: "2026-10-01T00:00:00Z".into(),
        full_due_at: "2099-01-01T00:00:00Z".into(),
        snapshot_at: None,
        snapshot: None,
        needs_full: false,
    };
    assert!(!due(
        Some(&checkpoint),
        epoch,
        1,
        Some(tokio::time::Instant::now() + std::time::Duration::from_secs(60))
    ));
    assert!(due(
        Some(&checkpoint),
        epoch,
        1,
        Some(tokio::time::Instant::now() - std::time::Duration::from_secs(1))
    ));
    assert!(due(
        Some(&checkpoint),
        Uuid::now_v7(),
        1,
        Some(tokio::time::Instant::now() + std::time::Duration::from_secs(60))
    ));
    let mono = tokio::time::Instant::now();
    assert_eq!(
        first_snapshot_deadline(None, OffsetDateTime::UNIX_EPOCH, mono, 1000).unwrap(),
        mono
    );
    checkpoint.snapshot_at = Some("2026-10-01T00:00:00Z".into());
    let initial = clock("2026-10-01T00:00:00Z").unwrap();
    let target = first_snapshot_deadline(Some(&checkpoint), initial, mono, 1000).unwrap();
    assert_eq!(target, mono + std::time::Duration::from_secs(1));
    // A wall clock far in the past still grants at most one interval.
    assert_eq!(
        first_snapshot_deadline(Some(&checkpoint), OffsetDateTime::UNIX_EPOCH, mono, 1000).unwrap(),
        target
    );
    assert_eq!(
        first_snapshot_deadline(
            Some(&checkpoint),
            initial + time::Duration::seconds(2),
            mono,
            1000
        )
        .unwrap(),
        mono
    );
}

#[test]
fn failure_variant_not_message_decides_the_fence() {
    let ordinary = Failure::from("provider says reconciliation required; retry later".to_owned());
    assert_eq!(
        ordinary.into_outcome(),
        (
            "provider says reconciliation required; retry later".to_owned(),
            false
        )
    );
    let fenced = Failure::ReconciliationRequired("declared generation changed");
    assert_eq!(
        fenced.into_outcome(),
        ("declared generation changed".to_owned(), true)
    );
}
