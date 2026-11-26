use super::*;
use serde_json::json;
use sqlx::{Connection, Executor};

const DERIVATION: &str = "synthetic.keyed";

/// Outputs of two keys in an old chunk and in the current one, among many
/// other outputs of the same producer and of another.
async fn fixture(conn: &mut PgConnection) -> (String, Vec<Uuid>) {
    conn.execute("INSERT INTO core.events(id,event_type,source,run_id,parents,ts_quality,commit_seq,payload,equivalence_key,reading_order,record_hash,class)
      SELECT ('01700000-0000-7000-8000-'||lpad(to_hex(g),12,'0'))::uuid,'synthetic.keyed_output',
        CASE WHEN g%2=0 THEN 'synthetic.keyed' ELSE 'synthetic.other' END,uuidv7(),ARRAY[uuidv7()],'unknown',g,
        jsonb_build_object('text',repeat(md5(g::text),32)),g::text,g,decode(repeat('01',32),'hex'),'open' FROM generate_series(1,4096)g;
      INSERT INTO core.events(id,event_type,source,run_id,parents,ts_quality,commit_seq,payload,equivalence_key,reading_order,record_hash,class)
      SELECT uuidv7(),'synthetic.keyed_output','synthetic.keyed',uuidv7(),ARRAY[uuidv7()],'unknown',4096+g,
        jsonb_build_object('text',repeat(md5(g::text),32)),(4096+g)::text,4096+g,decode(repeat('02',32),'hex'),'open' FROM generate_series(1,4096)g").await.unwrap();
    let ids = [
        Uuid::parse_str("01700000-0000-7000-8000-000000009001").unwrap(),
        Uuid::parse_str("01700000-0000-7000-8000-000000009002").unwrap(),
        Uuid::parse_str("01700000-0000-7000-8000-000000009003").unwrap(),
        Uuid::now_v7(),
        Uuid::now_v7(),
        Uuid::now_v7(),
    ];
    // Members of the first key: 0, 1, 2 (archived), 3 and 4; of the second: 5.
    for (i, id) in ids.iter().enumerate() {
        let at = match i {
            0 => Some(OffsetDateTime::UNIX_EPOCH + time::Duration::hours(3)),
            1 => Some(OffsetDateTime::UNIX_EPOCH + time::Duration::hours(1)),
            3 => Some(OffsetDateTime::UNIX_EPOCH + time::Duration::hours(2)),
            _ => None,
        };
        sqlx::query("INSERT INTO core.events(id,event_type,source,run_id,parents,ts_orig,ts_quality,commit_seq,payload,equivalence_key,reading_order,record_hash,class)
          VALUES($1,'synthetic.keyed_output',$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'open')")
            .bind(id).bind(DERIVATION).bind(Uuid::now_v7()).bind(vec![Uuid::now_v7()])
            .bind(at).bind(if at.is_some(){"intrinsic"}else{"unknown"})
            .bind(20_000+i as i64).bind(json!({"text":format!("member-{i}")}))
            .bind(format!("member-{i}")).bind(1_000_000+i as i64).bind(vec![i as u8;32])
            .execute(&mut *conn).await.unwrap();
        sqlx::query("INSERT INTO derivation.outputs(derivation,key,event_id) VALUES($1,$2,$3)")
            .bind(DERIVATION)
            .bind(if i == 5 {
                json!({"day": 2})
            } else {
                json!({"day": 1})
            })
            .bind(id)
            .execute(&mut *conn)
            .await
            .unwrap();
    }
    // Another derivation holds the same key over the same member.
    sqlx::query(
        "INSERT INTO derivation.outputs(derivation,key,event_id) VALUES('synthetic.other',$1,$2)",
    )
    .bind(json!({"day": 2}))
    .bind(ids[0])
    .execute(&mut *conn)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO audit.archived(event_id,archived_seq,reason) VALUES($1,30000,'recomputed')",
    )
    .bind(ids[2])
    .execute(&mut *conn)
    .await
    .unwrap();
    conn.execute("ANALYZE core.events").await.unwrap();
    let chunk: String =
        sqlx::query_scalar("SELECT tableoid::regclass::text FROM core.events WHERE id=$1")
            .bind(ids[0])
            .fetch_one(conn)
            .await
            .unwrap();
    (chunk, ids.to_vec())
}

async fn members(conn: &mut PgConnection, key: Value) -> Vec<Uuid> {
    outputs(conn, DERIVATION, &key)
        .await
        .unwrap()
        .iter()
        .map(|event| event.id.0)
        .collect()
}

/// A key's outputs are its live members, by time then commit order, from
/// plain and compressed chunks alike; no other key's, producer's or archived
/// output is among them.
#[devkit::test]
async fn a_key_reads_its_live_outputs_from_plain_and_compressed_chunks(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let (old_chunk, ids) = fixture(&mut conn).await;
    for compressed in [false, true] {
        if compressed {
            sqlx::query("SELECT compress_chunk($1::regclass)")
                .bind(&old_chunk)
                .execute(&mut conn)
                .await
                .unwrap();
        }
        assert_eq!(
            members(&mut conn, json!({"day": 1})).await,
            [ids[1], ids[3], ids[0], ids[4]],
            "compressed: {compressed}"
        );
        assert_eq!(members(&mut conn, json!({"day": 2})).await, [ids[5]]);
        assert!(members(&mut conn, json!({"day": 3})).await.is_empty());
    }
    let stored = outputs(&mut conn, DERIVATION, &json!({"day": 1}))
        .await
        .unwrap();
    assert_eq!(stored[0].payload, json!({"text": "member-1"}));
    assert_eq!(stored[3].ts_orig.at, None);
}
