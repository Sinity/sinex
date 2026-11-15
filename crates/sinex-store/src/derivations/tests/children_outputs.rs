use super::*;
use sqlx::{Connection, Executor};

const SOURCE: &str = "synthetic.comparison";

async fn fixture(conn: &mut PgConnection, a: EventId, b: EventId) -> (String, Vec<Uuid>) {
    // One old chunk and one hot chunk, with the same producer dominating
    // both. The selected parents are rare independently of the source.
    conn.execute("INSERT INTO core.events(id,event_type,source,run_id,parents,ts_quality,commit_seq,payload,equivalence_key,reading_order,record_hash,class)
      SELECT ('01700000-0000-7000-8000-'||lpad(to_hex(g),12,'0'))::uuid,'synthetic.comparison_output','synthetic.comparison',uuidv7(),ARRAY[uuidv7()],'unknown',g,jsonb_build_object('text',repeat(md5(g::text),32)),g::text,g,decode(repeat('01',32),'hex'),'open' FROM generate_series(1,8192)g;
      INSERT INTO core.events(id,event_type,source,run_id,parents,ts_quality,commit_seq,payload,equivalence_key,reading_order,record_hash,class)
      SELECT uuidv7(),'synthetic.comparison_output','synthetic.comparison',uuidv7(),ARRAY[uuidv7()],'unknown',8192+g,jsonb_build_object('text',repeat(md5(g::text),32)),(8192+g)::text,8192+g,decode(repeat('02',32),'hex'),'open' FROM generate_series(1,8192)g").await.unwrap();
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
          VALUES($1,'synthetic.comparison_output',$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)")
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
      VALUES(uuidv7(),'synthetic.comparison_output','synthetic.comparison',uuidv7(),uuidv7(),0,'unknown',30001,'{}',1,decode(repeat('03',32),'hex'),'open'); ANALYZE core.events").await.unwrap();
    let chunk: String =
        sqlx::query_scalar("SELECT tableoid::regclass::text FROM core.events WHERE id=$1")
            .bind(ids[0])
            .fetch_one(conn)
            .await
            .unwrap();
    (chunk, ids.to_vec())
}

async fn selected(conn: &mut PgConnection, a: EventId, b: EventId, ids: &[Uuid]) {
    assert!(
        children_outputs(conn, SOURCE, EventId::new())
            .await
            .unwrap()
            .is_empty()
    );
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

#[devkit::test]
async fn children_outputs_select_the_source_outputs_citing_a_parent(db: devkit::pg::TestDatabase) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let (a, b) = (EventId::new(), EventId::new());
    let (old_chunk, ids) = fixture(&mut conn, a, b).await;
    selected(&mut conn, a, b, &ids).await;
    sqlx::query("SELECT compress_chunk($1::regclass)")
        .bind(old_chunk)
        .execute(&mut conn)
        .await
        .unwrap();
    selected(&mut conn, a, b, &ids).await;
}
