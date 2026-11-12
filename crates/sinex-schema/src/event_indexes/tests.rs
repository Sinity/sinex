use super::*;
use sqlx::{Connection, Executor, PgConnection};

async fn positions(conn: &mut PgConnection) -> Vec<serde_json::Value> {
    sqlx::query_scalar("SELECT to_jsonb(c) FROM core.commit_log c ORDER BY commit_seq")
        .fetch_all(conn)
        .await
        .unwrap()
}

async fn statement_wal(conn: &mut PgConnection, statement: &str) -> i64 {
    let plan: serde_json::Value =
        sqlx::query_scalar(&format!("EXPLAIN (ANALYZE, WAL, FORMAT JSON) {statement}"))
            .fetch_one(conn)
            .await
            .unwrap();
    plan[0]["Plan"]["WAL Bytes"].as_i64().unwrap()
}

#[devkit::test]
async fn commit_positions_compact_without_deleting_event_history(db: devkit::pg::TestDatabase) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    conn.execute("ALTER TABLE core.event_tombstones DROP CONSTRAINT tombstone_history_complete;
        ALTER TABLE core.commit_log ADD COLUMN event_id uuid, ADD COLUMN event_type text;
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
    let old_wal = statement_wal(&mut old,
        "DELETE FROM core.commit_log c USING core.events e WHERE e.id=c.event_id AND e.commit_seq=c.commit_seq"
    ).await;
    let old_deleted: i64 = sqlx::query_scalar(
        "SELECT n_tup_del FROM pg_stat_xact_user_tables WHERE relid='core.commit_log'::regclass",
    )
    .fetch_one(&mut *old)
    .await
    .unwrap();
    assert_eq!(old_deleted, 32768);
    assert_eq!(positions(&mut old).await.len(), 5);
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

    // The positive control's rolled-back work can remain in backend stats.
    // A fresh backend scopes the candidate's mutation counters to its own work.
    conn.close().await.unwrap();
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let mut tx = conn.begin().await.unwrap();
    let mut new_wal = None;
    for statement in statements() {
        if statement.starts_with("INSERT INTO core.commit_log(") {
            new_wal = Some(statement_wal(&mut tx, &statement).await);
        } else {
            sqlx::raw_sql(&statement).execute(&mut *tx).await.unwrap();
        }
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
    // EXPLAIN counts this statement's WAL, so parallel fixture databases
    // cannot contaminate the old DELETE versus keeper INSERT comparison.
    let new_wal = new_wal.expect("the production step reinserts its keepers");
    assert!(
        new_wal * 4 < old_wal,
        "keeper INSERT WAL {new_wal}, original DELETE WAL {old_wal}"
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
