use super::*;
use sqlx::Connection;

async fn pairs(conn: &mut PgConnection) -> Vec<(Uuid, i64)> {
    sqlx::query_as(
        "SELECT m.event_id, m.stream_seq FROM core.published p,
         LATERAL unnest(p.event_ids, p.stream_seqs) m(event_id, stream_seq)
         ORDER BY m.event_id, m.stream_seq",
    )
    .fetch_all(conn)
    .await
    .unwrap()
}

async fn tombstone(conn: &mut PgConnection, id: Uuid) {
    sqlx::query(
        "INSERT INTO core.event_tombstones (event_id, source, event_type, operation_id)
         VALUES ($1, 'test.source', 'test.event', $2)",
    )
    .bind(id)
    .bind(Uuid::new_v4())
    .execute(conn)
    .await
    .unwrap();
}

#[devkit::test]
async fn exact_pairs_survive_overlapping_bounds_gaps_and_partial_erasure(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let a = Uuid::from_u128(1);
    let gap = Uuid::from_u128(2);
    let b = Uuid::from_u128(3);
    let c = Uuid::from_u128(4);
    record_published(&mut conn, &[(c, 12), (a, 7), (b, 2)])
        .await
        .unwrap();
    record_published(&mut conn, &[(a, 19), (c, 5)])
        .await
        .unwrap();
    // A duplicate acknowledgment returns its original stream sequence.
    record_published(&mut conn, &[(b, 2), (a, 7), (c, 12)])
        .await
        .unwrap();
    let expected = vec![(a, 7), (a, 19), (b, 2), (c, 5), (c, 12)];
    assert_eq!(pairs(&mut conn).await, expected);
    for id in [a, gap, c] {
        tombstone(&mut conn, id).await;
    }
    let page = excised_on_stream(&mut conn, 2).await.unwrap();
    let erased: Vec<_> = page.iter().map(|m| (m.event_id, m.stream_seq)).collect();
    assert_eq!(erased, [(c, 5), (a, 7)]);
    forget_published(&mut conn, &erased).await.unwrap();
    forget_published(&mut conn, &erased).await.unwrap();
    assert_eq!(pairs(&mut conn).await, [(a, 19), (b, 2), (c, 12)]);
    let remaining = excised_on_stream(&mut conn, 10).await.unwrap();
    assert_eq!(
        remaining
            .iter()
            .map(|m| (m.event_id, m.stream_seq))
            .collect::<Vec<_>>(),
        [(c, 12), (a, 19)]
    );
    forget_published(&mut conn, &[(a, 19), (c, 12)])
        .await
        .unwrap();
    assert_eq!(pairs(&mut conn).await, [(b, 2)]);
    assert!(excised_on_stream(&mut conn, 10).await.unwrap().is_empty());
    forget_published(&mut conn, &[(b, 2)]).await.unwrap();
    assert!(pairs(&mut conn).await.is_empty());
    let blocks: i64 = sqlx::query_scalar("SELECT count(*) FROM core.published")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(blocks, 0);
}

#[devkit::test]
async fn recording_and_position_commit_or_roll_back_together(db: devkit::pg::TestDatabase) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let id = Uuid::new_v4();
    let mut tx = conn.begin().await.unwrap();
    record_published(&mut tx, &[(id, 9)]).await.unwrap();
    set_consumer_position(&mut tx, "publisher", 12, 3)
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    assert!(pairs(&mut conn).await.is_empty());
    assert_eq!(
        consumer_position(&mut conn, "publisher").await.unwrap(),
        (0, 0)
    );
    let mut tx = conn.begin().await.unwrap();
    record_published(&mut tx, &[(id, 9)]).await.unwrap();
    set_consumer_position(&mut tx, "publisher", 12, 3)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(pairs(&mut conn).await, [(id, 9)]);
    assert_eq!(
        consumer_position(&mut conn, "publisher").await.unwrap(),
        (12, 3)
    );
}

#[devkit::test]
async fn duplicate_pairs_across_pages_are_erased_without_losing_other_copies(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let id = Uuid::new_v4();
    let other = Uuid::new_v4();
    record_published(&mut conn, &[(id, 1), (id, 4), (other, 6)])
        .await
        .unwrap();
    record_published(&mut conn, &[(id, 1), (other, 7)])
        .await
        .unwrap();
    tombstone(&mut conn, id).await;
    let page = excised_on_stream(&mut conn, 1).await.unwrap();
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].stream_seq, 1);
    forget_published(&mut conn, &[(id, 1)]).await.unwrap();
    assert_eq!(pairs(&mut conn).await.len(), 3);
    assert_eq!(
        excised_on_stream(&mut conn, 10).await.unwrap()[0].stream_seq,
        4
    );
}

mod upgrade;
