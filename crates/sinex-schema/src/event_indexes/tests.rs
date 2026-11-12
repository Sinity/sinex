use super::*;
use sqlx::{Connection, Executor, PgConnection};

async fn positions(conn: &mut PgConnection) -> Vec<serde_json::Value> {
    sqlx::query_scalar("SELECT to_jsonb(c) FROM core.commit_log c ORDER BY commit_seq")
        .fetch_all(conn)
        .await
        .unwrap()
}

async fn wal(conn: &mut PgConnection) -> String {
    sqlx::query_scalar("SELECT pg_current_wal_insert_lsn()::text")
        .fetch_one(conn)
        .await
        .unwrap()
}

async fn wal_since(conn: &mut PgConnection, before: &str) -> i64 {
    sqlx::query_scalar("SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(), $1::pg_lsn)::bigint")
        .bind(before)
        .fetch_one(conn)
        .await
        .unwrap()
}

#[devkit::test]
async fn commit_positions_compact_without_deleting_event_history(db: devkit::pg::TestDatabase) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    conn.execute("ALTER TABLE core.commit_log ADD COLUMN event_id uuid, ADD COLUMN event_type text;
        CREATE INDEX commit_log_type_seq ON core.commit_log(event_type,commit_seq);
        DROP INDEX core.events_record_location;
        CREATE INDEX events_ts_orig_idx ON core.events(ts_orig);
        CREATE INDEX events_source_ts_orig_idx ON core.events(source,ts_orig);
        CREATE INDEX events_material_id_anchor_idx ON core.events(material_id,anchor);
        INSERT INTO core.events (id,event_type,source,run_id,material_id,anchor,ts_quality,commit_seq,payload,reading_order,record_hash,class)
        SELECT uuidv7(),'command.imported','fixture',uuidv7(),uuidv7(),0,'unknown',n,'{}',1,decode(repeat('01',32),'hex'),'open'
        FROM generate_series(1,32768) n;
        INSERT INTO core.commit_log(commit_seq,event_id,event_type,persisted_at)
        SELECT commit_seq,id,event_type,'2026-01-01T00:00:00Z' FROM core.events;
        INSERT INTO core.commit_log(commit_seq,event_id,event_type,persisted_at) VALUES
          (40001,NULL,NULL,NULL),
          (40002,uuidv7(),'fixture.orphan','2026-01-02T00:00:00Z'),
          (40004,uuidv7(),'fixture.tombstone','2026-01-04T00:00:00Z'),
          (40005,uuidv7(),'fixture.tombstone','2026-01-05T00:00:00Z');
        INSERT INTO core.commit_log(commit_seq,event_id,event_type,persisted_at)
        SELECT 40003,id,event_type,'2026-01-03T00:00:00Z' FROM core.events WHERE commit_seq=1;
        INSERT INTO core.event_tombstones(event_id,source,event_type,operation_id,original_commit_seq)
        SELECT event_id,'fixture',event_type,uuidv7(),CASE WHEN commit_seq=40005 THEN 17 END
        FROM core.commit_log WHERE commit_seq IN(40004,40005)")
        .await.unwrap();
    let oid: i64 = sqlx::query_scalar("SELECT 'core.commit_log'::regclass::oid::bigint")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    let before = positions(&mut conn).await;
    let expected: Vec<serde_json::Value> = sqlx::query_scalar(
        "SELECT jsonb_build_object('commit_seq',c.commit_seq,'persisted_at',c.persisted_at)
         FROM core.commit_log c WHERE NOT EXISTS(SELECT 1 FROM core.events e
         WHERE e.id=c.event_id AND e.commit_seq=c.commit_seq) ORDER BY c.commit_seq",
    )
    .fetch_all(&mut conn)
    .await
    .unwrap();
    assert_eq!(expected.len(), 5);

    let mut old = conn.begin().await.unwrap();
    let start = wal(&mut old).await;
    assert_eq!(sqlx::query("DELETE FROM core.commit_log c USING core.events e WHERE e.id=c.event_id AND e.commit_seq=c.commit_seq")
        .execute(&mut *old).await.unwrap().rows_affected(),32768);
    let old_wal = wal_since(&mut old, &start).await;
    old.rollback().await.unwrap();
    assert_eq!(positions(&mut conn).await, before);

    // A failed upgrade rolls back the replacement heap, columns, and backfill.
    let mut failed = conn.begin().await.unwrap();
    for statement in statements() {
        sqlx::raw_sql(&statement)
            .execute(&mut *failed)
            .await
            .unwrap();
    }
    assert_eq!(positions(&mut failed).await, expected);
    failed.rollback().await.unwrap();
    assert_eq!(positions(&mut conn).await, before);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM core.event_tombstones WHERE original_commit_seq IS NULL"
        )
        .fetch_one(&mut conn)
        .await
        .unwrap(),
        1
    );

    let mut tx = conn.begin().await.unwrap();
    let start = wal(&mut tx).await;
    for statement in statements() {
        sqlx::raw_sql(&statement).execute(&mut *tx).await.unwrap();
    }
    assert_eq!(positions(&mut tx).await, expected);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT 'core.commit_log'::regclass::oid::bigint")
            .fetch_one(&mut *tx)
            .await
            .unwrap(),
        oid
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM core.events")
            .fetch_one(&mut *tx)
            .await
            .unwrap(),
        32768
    );
    let deleted: i64 = sqlx::query_scalar(
        "SELECT n_tup_del FROM pg_stat_xact_user_tables WHERE relid='core.commit_log'::regclass",
    )
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    assert_eq!(deleted, 0);
    let new_wal = wal_since(&mut tx, &start).await;
    assert!(
        new_wal * 4 < old_wal,
        "compaction WAL {new_wal}, original DELETE WAL {old_wal}"
    );
    let original_positions: Vec<i64> = sqlx::query_scalar(
        "SELECT original_commit_seq FROM core.event_tombstones ORDER BY original_commit_seq",
    )
    .fetch_all(&mut *tx)
    .await
    .unwrap();
    assert_eq!(original_positions, vec![17, 40004]);
    tx.commit().await.unwrap();
    assert_eq!(positions(&mut conn).await, expected);
    assert!(
        sqlx::query_scalar::<_, bool>(
            "SELECT to_regclass('pg_temp.sinex_commit_log_positions_kept') IS NULL"
        )
        .fetch_one(&mut conn)
        .await
        .unwrap()
    );
}
