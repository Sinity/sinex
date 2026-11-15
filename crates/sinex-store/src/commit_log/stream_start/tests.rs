use super::*;
#[devkit::test]
async fn older_format_gaps_do_not_block_changes_after_start(db: devkit::pg::TestDatabase) {
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
    let start = StreamStart::establish(&mut conn).await.unwrap();
    assert_eq!(start.position, 7);
    assert!(start.requires_snapshot);
    sqlx::raw_sql("UPDATE core.counters SET value=8 WHERE name='commit_seq'; INSERT INTO core.commit_log(commit_seq) VALUES(8)").execute(&mut conn).await.unwrap();
    let page = stream_changes_after_start(&mut conn, ChangeCursor::after(7), &start, 10, 4096)
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
        stream_changes_after_start(&mut conn, ChangeCursor::after(6), &start, 10, 4096)
            .await
            .is_err()
    );
    let mut wrong = start.clone();
    wrong.store = "other store".into();
    assert!(wrong.validate(&mut conn).await.is_err());
    sqlx::query("UPDATE core.counters SET value=6 WHERE name='commit_seq'")
        .execute(&mut conn)
        .await
        .unwrap();
    assert!(start.validate(&mut conn).await.is_err());
}
