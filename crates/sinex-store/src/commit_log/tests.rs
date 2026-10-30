use super::*;
use sqlx::Connection;

async fn pairs(conn: &mut PgConnection) -> Vec<(Uuid, i64)> {
    sqlx::query_as("SELECT event_id, stream_seq FROM core.published ORDER BY event_id, stream_seq")
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
    .bind(Uuid::now_v7())
    .execute(conn)
    .await
    .unwrap();
}

#[devkit::test]
async fn exact_pairs_survive_gaps_redelivery_and_partial_erasure(db: devkit::pg::TestDatabase) {
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
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM core.published")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(rows, 0);
}

#[devkit::test]
async fn recording_and_position_commit_or_roll_back_together(db: devkit::pg::TestDatabase) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let id = Uuid::now_v7();
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
async fn duplicate_acknowledgments_are_erased_without_losing_other_copies(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let id = Uuid::now_v7();
    let other = Uuid::now_v7();
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

#[devkit::test]
async fn random_identifiers_keep_exact_copies_through_scattered_erasure(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    sqlx::raw_sql("INSERT INTO core.published SELECT md5(i::text)::uuid, i*3 FROM generate_series(1,10000) i;
        INSERT INTO core.event_tombstones(event_id,source,event_type,operation_id)
          SELECT md5(i::text)::uuid,'test.source','test.event',uuidv7() FROM generate_series(1,10000,10) i")
        .execute(&mut conn).await.unwrap();
    let id: Uuid = sqlx::query_scalar("SELECT md5('1')::uuid")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    record_published(&mut conn, &[(id, 3), (id, 3), (id, 30001)])
        .await
        .unwrap();
    let first = excised_on_stream(&mut conn, 1000).await.unwrap();
    assert_eq!(first.len(), 1000);
    assert_eq!(first[0].stream_seq, 3);
    let erased: Vec<_> = first
        .into_iter()
        .map(|pair| (pair.event_id, pair.stream_seq))
        .collect();
    forget_published(&mut conn, &erased).await.unwrap();
    let remaining = excised_on_stream(&mut conn, 1000).await.unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(
        (remaining[0].event_id, remaining[0].stream_seq),
        (id, 30001)
    );
    assert_eq!(pairs(&mut conn).await.len(), 9001);
    forget_published(&mut conn, &[(id, 30001)]).await.unwrap();
    assert!(excised_on_stream(&mut conn, 1000).await.unwrap().is_empty());
}

mod upgrade;

mod changes;
