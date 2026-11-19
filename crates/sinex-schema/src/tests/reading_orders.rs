use super::*;
use sqlx::Executor;
use std::collections::BTreeMap;

#[devkit::test]
async fn reading_order_upgrade_keeps_old_maxima_and_positions(db: devkit::pg::TestDatabase) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let fresh = catalog(&mut conn).await.unwrap();
    conn.execute("INSERT INTO core.runs(id,name,host,events)
      SELECT ('01810000-0000-7000-8000-'||lpad(to_hex(n),12,'0'))::uuid,
        CASE WHEN n<=8 THEN 'synthetic.orders.'||((n-1)/2)::text WHEN n=9 THEN 'synthetic.unrelated' ELSE 'synthetic.empty' END,
        'synthetic',CASE WHEN n<=8 THEN 1024 WHEN n=9 THEN 8192 ELSE 0 END
      FROM generate_series(1,10)n;
      INSERT INTO core.runs(id,name,host,events)
      SELECT ('01810000-0000-7000-8000-'||lpad(to_hex(n+11),12,'0'))::uuid,'synthetic.orders.'||n::text,'synthetic',0 FROM generate_series(0,3)n;
      INSERT INTO core.events(id,event_type,source,run_id,material_id,record_offset,ts_quality,commit_seq,payload,reading_order,record_hash,class,record_state)
      SELECT ('01800000-0000-7000-8000-'||lpad(to_hex(n),12,'0'))::uuid,
        'synthetic.orders','synthetic.shared',
        ('01810000-0000-7000-8000-'||lpad(to_hex(CASE WHEN n<=8192 THEN (n-1)/1024+1 ELSE 9 END),12,'0'))::uuid,
        uuidv7(),n,'unknown',n,'{}',
        CASE WHEN n>8192 THEN 9223372036854775790 WHEN (n-1)%2048=0 THEN 4398046511104+(n-1)/2048 ELSE n END,
        decode(repeat('01',32),'hex'),'open',CASE WHEN n=2049 THEN 'removed' ELSE 'present' END
      FROM generate_series(1,16384)n;
      INSERT INTO audit.archived(event_id,archived_seq,reason) VALUES('01800000-0000-7000-8000-000000000001',1,'archive');
      ANALYZE core.runs; ANALYZE core.events").await.unwrap();

    conn.execute("INSERT INTO registry.derivations(name,version,kind,inputs,outputs,build)
        SELECT 'synthetic.orders.'||n::text,'1','per_event','{}','{}','synthetic' FROM generate_series(0,3)n;
        INSERT INTO registry.derivations(name,version,kind,inputs,outputs,build)
        VALUES('synthetic.empty','1','per_event','{}','{}','synthetic');
        INSERT INTO derivation.positions(derivation,commit_seq) VALUES('synthetic.orders.0',123),('synthetic.legacy',456)").await.unwrap();
    let mut expected = (0..4)
        .map(|n| (format!("synthetic.orders.{n}"), (1_i64 << 42) + n))
        .collect::<BTreeMap<_, _>>();
    expected.insert("synthetic.empty".into(), 0);
    expected.insert("synthetic.legacy".into(), 0);
    for compressed in [false, true] {
        if compressed {
            conn.execute("SELECT compress_chunk(c) FROM show_chunks('core.events')c")
                .await
                .unwrap();
        }
        positions_without_reading_orders(&mut conn).await;
        conn.execute(
            "DELETE FROM sinex_schema.applied; INSERT INTO sinex_schema.applied(version)VALUES(43)",
        )
        .await
        .unwrap();
        assert!(matches!(
            apply(&mut conn).await.unwrap(),
            Applied::Upgraded {
                from: 43,
                to: VERSION
            }
        ));
        assert_eq!(catalog(&mut conn).await.unwrap(), fresh);
        let rows: Vec<(String, i64)> = sqlx::query_as(
            "SELECT derivation,reading_order FROM derivation.positions ORDER BY derivation",
        )
        .fetch_all(&mut conn)
        .await
        .unwrap();
        assert_eq!(rows.into_iter().collect::<BTreeMap<_, _>>(), expected);
        let progress:Vec<(String,i64)>=sqlx::query_as("SELECT derivation,commit_seq FROM derivation.positions WHERE commit_seq<>0 ORDER BY derivation").fetch_all(&mut conn).await.unwrap();
        assert_eq!(
            progress,
            vec![
                ("synthetic.legacy".into(), 456),
                ("synthetic.orders.0".into(), 123)
            ]
        );
    }
}

/// Recreate the physical old shape: dropping one column leaves its ordinal behind.
pub(super) async fn positions_without_reading_orders(conn: &mut PgConnection) {
    conn.execute("DROP TABLE core.library_copy_fields;
        DROP TABLE raw.owned_blob_releases;
        CREATE TEMP TABLE positions_before_orders AS SELECT derivation,commit_seq FROM derivation.positions;
        DROP TABLE derivation.positions;
        CREATE TABLE derivation.positions(derivation text PRIMARY KEY,commit_seq bigint NOT NULL);
        INSERT INTO derivation.positions SELECT * FROM positions_before_orders;
        DROP TABLE positions_before_orders").await.unwrap();
}
