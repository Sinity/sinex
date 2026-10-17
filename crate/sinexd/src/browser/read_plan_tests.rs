use super::*;
#[test]
fn backward_wall_target_never_postpones_elapsed_monotonic_full() {
    let epoch = Uuid::now_v7();
    let checkpoint = Checkpoint {
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
}
