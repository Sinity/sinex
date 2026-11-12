use super::*;
#[devkit::test]
async fn closed_predecessor_gaps_do_not_poison_post_origin_deltas(db: devkit::pg::TestDatabase) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    sqlx::raw_sql("ALTER TABLE core.event_tombstones DROP CONSTRAINT tombstone_history_complete;
        INSERT INTO core.event_tombstones(event_id,source,event_type,operation_id)
        SELECT uuidv7(),'synthetic','example.event',uuidv7() FROM generate_series(1,92);
        ALTER TABLE core.event_tombstones ADD CONSTRAINT tombstone_history_complete CHECK
        (commit_seq IS NOT NULL AND original_commit_seq IS NOT NULL AND class IS NOT NULL) NOT VALID;
        UPDATE core.counters SET value=7 WHERE name='commit_seq'")
        .execute(&mut conn).await.unwrap();
    let before: Vec<serde_json::Value> =
        sqlx::query_scalar("SELECT to_jsonb(t) FROM core.event_tombstones t ORDER BY event_id")
            .fetch_all(&mut conn)
            .await
            .unwrap();
    assert!(
        changes_page(&mut conn, ChangeCursor::after(0), 10, 4096)
            .await
            .unwrap()
            .gap
            .is_some()
    );
    let origin = HistoryOrigin::establish(&mut conn).await.unwrap();
    assert_eq!(origin.position, 7);
    assert!(origin.requires_projection);
    sqlx::raw_sql("UPDATE core.counters SET value=8 WHERE name='commit_seq'; INSERT INTO core.commit_log(commit_seq) VALUES(8)").execute(&mut conn).await.unwrap();
    let page = changes_page_after_origin(&mut conn, ChangeCursor::after(7), &origin, 10, 4096)
        .await
        .unwrap();
    assert!(page.gap.is_none());
    assert_eq!(page.entries.len(), 1);
    assert_eq!(page.entries[0].position, 8);
    assert!(matches!(page.entries[0].change, Change::PositionComplete));
    let after: Vec<serde_json::Value> =
        sqlx::query_scalar("SELECT to_jsonb(t) FROM core.event_tombstones t ORDER BY event_id")
            .fetch_all(&mut conn)
            .await
            .unwrap();
    assert_eq!(before, after);
    assert!(
        changes_page_after_origin(&mut conn, ChangeCursor::after(6), &origin, 10, 4096)
            .await
            .is_err()
    );
    let mut wrong = origin.clone();
    wrong.store = "other store".into();
    assert!(wrong.validate(&mut conn).await.is_err());
    sqlx::query("UPDATE core.counters SET value=6 WHERE name='commit_seq'")
        .execute(&mut conn)
        .await
        .unwrap();
    assert!(origin.validate(&mut conn).await.is_err());
}
