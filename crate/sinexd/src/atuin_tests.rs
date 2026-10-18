use super::*;
#[test]
fn wall_clock_rollback_cannot_postpone_a_due_full_read() {
    let epoch = Uuid::now_v7();
    let wall = OffsetDateTime::parse("2026-10-17T10:00:00Z", &Rfc3339).unwrap();
    let checkpoint = sinex_model::acquisition::AtuinCheckpoint {
        version: 1,
        epoch,
        schema_version: 1,
        user_version: 0,
        through_rowid: 195,
        full_completed_at: "2026-10-17T09:00:00Z".into(),
        full_due_at: "2026-10-18T09:00:00Z".into(),
        needs_full: false,
    };
    let before = tokio::time::Instant::now();
    let deadline = before
        .checked_add(std::time::Duration::from_secs(10))
        .unwrap();
    let after = deadline
        .checked_add(std::time::Duration::from_secs(1))
        .unwrap();
    assert!(checkpoint.reusable(epoch, 1, 0, wall - time::Duration::hours(5)));
    assert!(!full_required(
        Some(deadline),
        before,
        checkpoint.reusable(epoch, 1, 0, wall)
    ));
    assert!(
        full_required(
            Some(deadline),
            after,
            checkpoint.reusable(epoch, 1, 0, wall - time::Duration::hours(5))
        ),
        "elapsed monotonic deadline wins even after realtime rollback"
    );
    assert!(
        full_required(None, before, true),
        "missing live deadline cannot resume a persisted epoch"
    );
}
