use super::*;
use crate::{commit_log, read_models};

fn action(target: EventId, hide: bool) -> Batch {
    let mut change = batch(vec![]);
    change.archive.push(if hide {
        ArchiveRequest::Hide {
            target,
            operation: OperationId::new(),
        }
    } else {
        ArchiveRequest::Unhide {
            target,
            operation: OperationId::new(),
        }
    });
    change
}

async fn visible(conn: &mut PgConnection, position: i64) -> i64 {
    sqlx::query_scalar!(
        r#"SELECT count(*) AS "n!" FROM core.events e
        WHERE commit_seq <= $1 AND NOT audit.is_archived_at(e.id,$1)"#,
        position
    )
    .fetch_one(conn)
    .await
    .unwrap()
}

#[devkit::test]
async fn archive_history_preserves_hide_unhide_hide_intervals_without_new_events(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = <sqlx::PgConnection as sqlx::Connection>::connect(&db.url(None))
        .await
        .unwrap();
    let events: Vec<_> = (0..3).map(|key| keyed("history", key, 1, 1)).collect();
    admit_one(&mut conn, &batch(events.clone())).await;
    let target = events[0].id;
    let before = sqlx::query_scalar!("SELECT payload FROM core.events ORDER BY commit_seq")
        .fetch_all(&mut conn)
        .await
        .unwrap();
    assert_eq!(visible(&mut conn, 3).await, 3);
    for (hide, position, total) in [(true, 4, 2), (false, 5, 3), (true, 6, 2)] {
        let out = admit_one(&mut conn, &action(target, hide)).await;
        assert_eq!(out.admitted, 0);
        assert_eq!(out.commit_seq, None, "event admission range stays absent");
        assert_eq!(commit_log::latest(&mut conn).await.unwrap(), position);
        assert_eq!(
            visible(&mut conn, 3).await,
            3,
            "before-hide view survives every transition"
        );
        assert_eq!(visible(&mut conn, 4).await, 2);
        if position >= 5 {
            assert_eq!(visible(&mut conn, 5).await, 3);
        }
        assert_eq!(visible(&mut conn, position).await, total);
        let page = commit_log::after(&mut conn, position - 1, 10, i64::MAX)
            .await
            .unwrap();
        assert!(
            page.events.is_empty(),
            "metadata must not fabricate an event"
        );
        assert_eq!(page.up_to, Some(position));
        assert_eq!(
            read_models::tests::catch_up_search_text(&mut conn)
                .await
                .unwrap(),
            position
        );
    }
    let changes = commit_log::changes_after(&mut conn, 0, 10).await.unwrap();
    assert_eq!(
        changes
            .iter()
            .map(|n| (n.commit_seq, n.archived))
            .collect::<Vec<_>>(),
        [(4, true), (5, false), (6, true)]
    );
    assert!(
        changes
            .windows(2)
            .all(|pair| pair[0].change_seq < pair[1].change_seq)
    );
    assert!(changes.iter().all(|change| change.event_id == target.0));
    assert_eq!(
        sqlx::query_scalar!("SELECT payload FROM core.events ORDER BY commit_seq")
            .fetch_all(&mut conn)
            .await
            .unwrap(),
        before
    );
    assert_eq!(
        sqlx::query_scalar!(r#"SELECT count(*) AS "n!" FROM audit.archived"#)
            .fetch_one(&mut conn)
            .await
            .unwrap(),
        1
    );
}

#[devkit::test]
async fn archive_changes_and_positions_roll_back_together(db: devkit::pg::TestDatabase) {
    let mut conn = <sqlx::PgConnection as sqlx::Connection>::connect(&db.url(None))
        .await
        .unwrap();
    let event = keyed("rollback", 1, 1, 1);
    admit_one(&mut conn, &batch(vec![event.clone()])).await;
    for hide in [true, false] {
        let before = commit_log::latest(&mut conn).await.unwrap();
        let mut tx = conn.begin().await.unwrap();
        admit(&mut tx, &action(event.id, hide), &NoDerivations)
            .await
            .unwrap();
        assert_eq!(commit_log::latest(&mut tx).await.unwrap(), before + 1);
        tx.rollback().await.unwrap();
        assert_eq!(commit_log::latest(&mut conn).await.unwrap(), before);
        assert_eq!(
            commit_log::changes_after(&mut conn, 0, 10)
                .await
                .unwrap()
                .len(),
            usize::from(!hide)
        );
        let page = commit_log::after(&mut conn, before, 10, i64::MAX)
            .await
            .unwrap();
        assert!(page.up_to.is_none());
        if hide {
            admit_one(&mut conn, &action(event.id, true)).await;
        }
    }
}

#[devkit::test]
async fn cancelling_an_archive_transaction_does_not_publish_partial_history_or_a_position(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = <sqlx::PgConnection as sqlx::Connection>::connect(&db.url(None))
        .await
        .unwrap();
    let event = keyed("cancel", 1, 1, 1);
    admit_one(&mut conn, &batch(vec![event.clone()])).await;
    let url = db.url(None);
    let (ready, started) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let mut connection = PgConnection::connect(&url).await.unwrap();
        let mut tx = connection.begin().await.unwrap();
        admit(&mut tx, &action(event.id, true), &NoDerivations)
            .await
            .unwrap();
        ready.send(()).unwrap();
        std::future::pending::<()>().await;
    });
    started.await.unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    let position = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        sqlx::query_scalar!("SELECT value FROM core.counters WHERE name='commit_seq' FOR UPDATE")
            .fetch_one(&mut conn),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        position, 1,
        "the aborted transaction releases its counter lock"
    );
    assert_eq!(visible(&mut conn, 1).await, 1);
    assert!(
        commit_log::changes_after(&mut conn, 0, 10)
            .await
            .unwrap()
            .is_empty()
    );
}

#[devkit::test]
async fn commit_pages_cover_deleted_tail_gaps_and_refuse_zero_limits(db: devkit::pg::TestDatabase) {
    let mut conn = <sqlx::PgConnection as sqlx::Connection>::connect(&db.url(None))
        .await
        .unwrap();
    admit_one(
        &mut conn,
        &batch((0..3).map(|key| keyed("gaps", key, 1, 1)).collect()),
    )
    .await;
    assert!(commit_log::after(&mut conn, 0, 0, i64::MAX).await.is_err());
    sqlx::query!("DELETE FROM core.commit_log WHERE commit_seq>=2")
        .execute(&mut conn)
        .await
        .unwrap();
    let first = commit_log::after(&mut conn, 0, 1, i64::MAX).await.unwrap();
    assert_eq!(first.up_to, Some(1));
    assert_eq!(first.events.len(), 1);
    let gap = commit_log::after(&mut conn, 1, 1, i64::MAX).await.unwrap();
    assert_eq!(gap.up_to, Some(3));
    assert!(gap.events.is_empty());
    assert!(
        commit_log::after(&mut conn, 3, 1, i64::MAX)
            .await
            .unwrap()
            .up_to
            .is_none()
    );
}

#[devkit::test]
async fn a_commit_page_ends_once_its_payloads_pass_the_byte_bound(db: devkit::pg::TestDatabase) {
    let mut conn = <sqlx::PgConnection as sqlx::Connection>::connect(&db.url(None))
        .await
        .unwrap();
    admit_one(
        &mut conn,
        &batch((0..3).map(|key| keyed("bytes", key, 1, 1)).collect()),
    )
    .await;
    // Any payload passes one byte: each page holds just its first event.
    let first = commit_log::after(&mut conn, 0, 10, 1).await.unwrap();
    assert_eq!(first.events.len(), 1);
    assert_eq!(first.up_to, Some(first.events[0].commit_seq));
    let second = commit_log::after(&mut conn, first.up_to.unwrap(), 10, 1)
        .await
        .unwrap();
    assert_eq!(second.events.len(), 1);
    assert!(second.events[0].commit_seq > first.events[0].commit_seq);
    let all = commit_log::after(&mut conn, 0, 10, crate::PAGE_BYTES)
        .await
        .unwrap();
    assert_eq!(all.events.len(), 3);
}
