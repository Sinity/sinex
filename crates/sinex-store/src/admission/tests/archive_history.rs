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

#[tokio::test]
async fn archive_history_preserves_hide_unhide_hide_intervals_without_new_events() {
    let (_db, mut conn) = testdb::fresh().await;
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
        let page = commit_log::after(&mut conn, position - 1, 10)
            .await
            .unwrap();
        assert!(
            page.events.is_empty(),
            "metadata must not fabricate an event"
        );
        assert_eq!(page.up_to, Some(position));
        assert_eq!(
            read_models::catch_up_search_text(&mut conn).await.unwrap(),
            position
        );
    }
    let notices = commit_log::notices_after(&mut conn, 0, 10).await.unwrap();
    assert_eq!(
        notices
            .iter()
            .map(|n| (n.commit_seq, n.archived))
            .collect::<Vec<_>>(),
        [(4, true), (5, false), (6, true)]
    );
    assert!(
        notices
            .windows(2)
            .all(|pair| pair[0].notice_seq < pair[1].notice_seq)
    );
    assert!(notices.iter().all(|notice| notice.event_id == target.0));
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

#[tokio::test]
async fn archive_changes_and_positions_roll_back_together() {
    let (_db, mut conn) = testdb::fresh().await;
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
            commit_log::notices_after(&mut conn, 0, 10)
                .await
                .unwrap()
                .len(),
            usize::from(!hide)
        );
        let page = commit_log::after(&mut conn, before, 10).await.unwrap();
        assert!(page.up_to.is_none());
        if hide {
            admit_one(&mut conn, &action(event.id, true)).await;
        }
    }
}

#[tokio::test]
async fn cancelling_an_archive_transaction_does_not_publish_partial_history_or_a_position() {
    let (db, mut conn) = testdb::fresh().await;
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
        commit_log::notices_after(&mut conn, 0, 10)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn commit_pages_cover_deleted_tail_gaps_and_refuse_zero_limits() {
    let (_db, mut conn) = testdb::fresh().await;
    admit_one(
        &mut conn,
        &batch((0..3).map(|key| keyed("gaps", key, 1, 1)).collect()),
    )
    .await;
    assert!(commit_log::after(&mut conn, 0, 0).await.is_err());
    sqlx::query!("DELETE FROM core.commit_log WHERE commit_seq>=2")
        .execute(&mut conn)
        .await
        .unwrap();
    let first = commit_log::after(&mut conn, 0, 1).await.unwrap();
    assert_eq!(first.up_to, Some(1));
    assert_eq!(first.events.len(), 1);
    let gap = commit_log::after(&mut conn, 1, 1).await.unwrap();
    assert_eq!(gap.up_to, Some(3));
    assert!(gap.events.is_empty());
    assert!(
        commit_log::after(&mut conn, 3, 1)
            .await
            .unwrap()
            .up_to
            .is_none()
    );
}

#[tokio::test]
async fn old_schema_refuses_admission_before_data_or_counter_changes() {
    let (_db, mut conn) = testdb::fresh().await;
    sinex_schema::require_current(&mut conn).await.unwrap();
    sqlx::query!("DELETE FROM sinex_schema.applied WHERE version>0")
        .execute(&mut conn)
        .await
        .unwrap();
    assert!(matches!(
        sinex_schema::apply(&mut conn).await,
        Err(sinex_schema::Error::Shape(0))
    ));
    let mut tx = conn.begin().await.unwrap();
    let result = admit(&mut tx, &batch(vec![keyed("old", 1, 1, 1)]), &NoDerivations).await;
    assert!(matches!(
        result,
        Err(crate::Error::Schema(sinex_schema::Error::Upgrade {
            found: 0,
            declared: _
        }))
    ));
    tx.rollback().await.unwrap();
    assert_eq!(commit_log::latest(&mut conn).await.unwrap(), 0);
    assert_eq!(
        sqlx::query_scalar!(r#"SELECT count(*) AS "n!" FROM core.events"#)
            .fetch_one(&mut conn)
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn legacy_baseline_preserves_pending_notice_identity_and_refuses_unknown_empty_analysis() {
    let (_db, mut conn) = testdb::fresh().await;
    let event = keyed("legacy-notice", 1, 1, 1);
    admit_one(&mut conn, &batch(vec![event.clone()])).await;
    admit_one(&mut conn, &action(event.id, true)).await;
    let old = commit_log::notices_after(&mut conn, 0, 10)
        .await
        .unwrap()
        .remove(0);
    // The migration keeps original archival facts but establishes visibility
    // at cutover. A pending consumer must still receive this same notice ID.
    sqlx::query("UPDATE audit.archive_changes SET baseline=true,commit_seq=3 WHERE event_id=$1")
        .bind(event.id.0)
        .execute(&mut conn)
        .await
        .unwrap();
    sqlx::query("INSERT INTO audit.archive_changes(notice_seq,event_id,commit_seq,archived,reason,archived_seq,archived_at,baseline) VALUES(nextval(pg_get_serial_sequence('audit.archived','notice_seq')),NULL,3,false,'baseline',3,now(),true)")
        .execute(&mut conn).await.unwrap();
    let pending = commit_log::notices_after(&mut conn, 0, 10).await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].notice_seq, old.notice_seq);
    assert_eq!(pending[0].archived_seq, old.archived_seq);
    assert_eq!(pending[0].commit_seq, 3);
    assert!(
        commit_log::notices_after(&mut conn, old.notice_seq, 10)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        crate::analysis::source_evidence(&mut conn, "missing", 2)
            .await
            .unwrap_err()
            .to_string()
            .contains("visibility history before position 3 is unknown")
    );
}
