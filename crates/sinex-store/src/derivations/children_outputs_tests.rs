use super::*;
use sqlx::{Connection, Executor};

const SOURCE: &str = "synthetic.comparison";

async fn predecessor(conn: &mut PgConnection, input: EventId) -> Vec<Event> {
    sqlx::query_file!(
        "src/derivations/children_outputs_tests/old_parent_any.sql",
        SOURCE,
        input.0
    )
    .fetch_all(conn)
    .await
    .unwrap()
    .into_iter()
    .map(|row| Ok(committed!(row).event))
    .collect::<Result<Vec<_>>>()
    .unwrap()
}

async fn work(conn: &mut PgConnection) -> i64 {
    sqlx::query_scalar("SELECT coalesce(sum(seq_tup_read+idx_tup_fetch),0)::bigint FROM pg_stat_xact_user_tables WHERE relid='core.events'::regclass OR relid IN(SELECT show_chunks('core.events')) OR relid IN(SELECT compress_relid FROM _timescaledb_catalog.compression_settings WHERE relid IN(SELECT show_chunks('core.events')))")
        .fetch_one(conn).await.unwrap()
}

#[devkit::test]
async fn parent_containment_has_the_same_selected_rows_for_null_empty_and_duplicate_arrays(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let a = Uuid::now_v7();
    let b = Uuid::now_v7();
    let mismatch: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM (VALUES(NULL::uuid[]),('{}'::uuid[]),(ARRAY[$1]),(ARRAY[$1,$1]),(ARRAY[$1,$2]),(ARRAY[$2]),(ARRAY[NULL::uuid]),(ARRAY[$1,NULL::uuid]),(ARRAY[$2,NULL::uuid])) v(parents) WHERE (($1=ANY(parents)) IS TRUE) IS DISTINCT FROM ((parents IS NOT NULL AND parents @> ARRAY[$1]::uuid[]) IS TRUE))",
    ).bind(a).bind(b).fetch_one(&mut conn).await.unwrap();
    assert!(!mismatch);
}

async fn fixture(conn: &mut PgConnection, a: EventId, b: EventId) -> (String, Vec<Uuid>) {
    // One old chunk and one hot chunk, with the same producer dominating
    // both. The selected parents are rare independently of the source.
    conn.execute("INSERT INTO core.events(id,event_type,source,run_id,parents,ts_quality,commit_seq,payload,equivalence_key,reading_order,record_hash,class)
      SELECT ('01700000-0000-7000-8000-'||lpad(to_hex(g),12,'0'))::uuid,'operator_log.entry','synthetic.comparison',uuidv7(),ARRAY[uuidv7()],'unknown',g,jsonb_build_object('text',repeat(md5(g::text),32)),g::text,g,decode(repeat('01',32),'hex'),'open' FROM generate_series(1,8192)g;
      INSERT INTO core.events(id,event_type,source,run_id,parents,ts_quality,commit_seq,payload,equivalence_key,reading_order,record_hash,class)
      SELECT uuidv7(),'operator_log.entry','synthetic.comparison',uuidv7(),ARRAY[uuidv7()],'unknown',8192+g,jsonb_build_object('text',repeat(md5(g::text),32)),(8192+g)::text,8192+g,decode(repeat('02',32),'hex'),'open' FROM generate_series(1,8192)g").await.unwrap();
    let ids = [
        Uuid::parse_str("01700000-0000-7000-8000-000000009001").unwrap(),
        Uuid::parse_str("01700000-0000-7000-8000-000000009002").unwrap(),
        Uuid::parse_str("01700000-0000-7000-8000-000000009003").unwrap(),
        Uuid::now_v7(),
        Uuid::now_v7(),
        Uuid::now_v7(),
    ];
    let parents = [
        vec![a.0, a.0],
        vec![a.0, b.0],
        vec![a.0],
        vec![a.0],
        vec![a.0],
        vec![b.0],
    ];
    for (i, (id, parents)) in ids.iter().zip(parents).enumerate() {
        let at = match i {
            0 => Some(OffsetDateTime::UNIX_EPOCH + time::Duration::hours(3)),
            1 => Some(OffsetDateTime::UNIX_EPOCH + time::Duration::hours(1)),
            3 => Some(OffsetDateTime::UNIX_EPOCH + time::Duration::hours(2)),
            _ => None,
        };
        sqlx::query("INSERT INTO core.events(id,event_type,source,run_id,parents,ts_orig,ts_quality,commit_seq,payload,equivalence_key,reading_order,record_hash,class,record_state)
          VALUES($1,'operator_log.entry',$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)")
            .bind(id).bind(if i==4 {"synthetic.other"} else {SOURCE}).bind(Uuid::now_v7())
            .bind(parents).bind(at).bind(if at.is_some(){"intrinsic"}else{"unknown"})
            .bind(20_000+i as i64).bind(serde_json::json!({"text":format!("selected-{i}"),"label":null}))
            .bind(format!("selected-{i}")).bind(1_000_000+i as i64).bind(vec![i as u8;32])
            .bind(if i==1 {"guarded"}else{"open"}).bind(if i==3 {"removed"}else{"present"})
            .execute(&mut *conn).await.unwrap();
    }
    sqlx::query(
        "INSERT INTO audit.archived(event_id,archived_seq,reason) VALUES($1,30000,'archive')",
    )
    .bind(ids[2])
    .execute(&mut *conn)
    .await
    .unwrap();
    // Ordinary record events have NULL parents and cannot become outputs.
    conn.execute("INSERT INTO core.events(id,event_type,source,run_id,material_id,anchor,ts_quality,commit_seq,payload,reading_order,record_hash,class)
      VALUES(uuidv7(),'operator_log.entry','synthetic.comparison',uuidv7(),uuidv7(),0,'unknown',30001,'{}',1,decode(repeat('03',32),'hex'),'open'); ANALYZE core.events").await.unwrap();
    let chunk: String =
        sqlx::query_scalar("SELECT tableoid::regclass::text FROM core.events WHERE id=$1")
            .bind(ids[0])
            .fetch_one(conn)
            .await
            .unwrap();
    (chunk, ids.to_vec())
}

async fn parity(conn: &mut PgConnection, a: EventId, b: EventId, ids: &[Uuid]) {
    for input in [a, b, EventId::new(), a] {
        let old = predecessor(conn, input).await;
        let new = children_outputs(conn, SOURCE, input).await.unwrap();
        assert_eq!(new, old, "every ordered Event field");
    }
    let rows = children_outputs(conn, SOURCE, a).await.unwrap();
    assert_eq!(
        rows.iter().map(|e| e.id.0).collect::<Vec<_>>(),
        [ids[1], ids[3], ids[0]]
    );
    assert_eq!(rows[0].class, Class::Guarded);
    assert_eq!(rows[1].record_state, RecordState::Removed);
    assert_eq!(
        rows[2].origin,
        Origin::Derived {
            parents: vec![a, a]
        }
    );
    let rows = children_outputs(conn, SOURCE, b).await.unwrap();
    assert_eq!(
        rows.iter().map(|e| e.id.0).collect::<Vec<_>>(),
        [ids[1], ids[5]]
    );
    assert!(rows[1].ts_orig.at.is_none());
}

async fn measured(conn: &mut PgConnection, a: EventId, phase: &str) {
    let mut tx = conn.begin().await.unwrap();
    tx.execute("SET LOCAL max_parallel_workers_per_gather=0; SET LOCAL jit=off")
        .await
        .unwrap();
    let before = work(&mut tx).await;
    let expected = predecessor(&mut tx, a).await;
    for _ in 1..8 {
        assert_eq!(predecessor(&mut tx, a).await, expected);
    }
    let old = work(&mut tx).await - before;
    let before = work(&mut tx).await;
    for _ in 0..8 {
        assert_eq!(
            children_outputs(&mut tx, SOURCE, a).await.unwrap(),
            expected
        );
    }
    let new = work(&mut tx).await - before;
    eprintln!("{phase} parent lookup tuple visits over8: ANY={old},containment={new}");
    assert!(
        old >= 8192 * 8,
        "predecessor must scan unrelated hot output tuples: {old}"
    );
    assert!(
        new < old / 4,
        "existing parent index must remove unrelated hot work: old={old},new={new}"
    );
    tx.rollback().await.unwrap();
}

#[devkit::test]
async fn children_outputs_preserve_full_events_and_avoid_unrelated_hot_history(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let (a, b) = (EventId::new(), EventId::new());
    let (old_chunk, ids) = fixture(&mut conn, a, b).await;
    parity(&mut conn, a, b, &ids).await;
    measured(&mut conn, a, "rowstore").await;
    sqlx::query("SELECT compress_chunk($1::regclass)")
        .bind(old_chunk)
        .execute(&mut conn)
        .await
        .unwrap();
    parity(&mut conn, a, b, &ids).await;
    measured(&mut conn, a, "compressed+hot").await;
}
