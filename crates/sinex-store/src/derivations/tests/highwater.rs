use super::*;
use sqlx::{Connection, Executor};

#[devkit::test]
async fn grouped_reading_orders_take_each_producers_history_maximum(db: devkit::pg::TestDatabase) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    // Two nonempty runs per requested producer; source labels deliberately
    // do not equal run names. New empty runs cannot replace old maxima.
    conn.execute("INSERT INTO core.runs(id,name,host,events)
      SELECT ('01810000-0000-7000-8000-'||lpad(to_hex(n),12,'0'))::uuid,
        CASE WHEN n<=8 THEN 'synthetic.highwater.'||((n-1)/2)::text WHEN n=9 THEN 'synthetic.unrelated' ELSE 'synthetic.empty' END,
        'synthetic',CASE WHEN n<=8 THEN 1024 WHEN n=9 THEN 8192 ELSE 0 END
      FROM generate_series(1,10)n;
      INSERT INTO core.runs(id,name,host,events)
      SELECT ('01810000-0000-7000-8000-'||lpad(to_hex(n+11),12,'0'))::uuid,'synthetic.highwater.'||n::text,'synthetic',0 FROM generate_series(0,3)n;
      INSERT INTO core.events(id,event_type,source,run_id,material_id,record_offset,ts_quality,commit_seq,payload,reading_order,record_hash,class,record_state)
      SELECT ('01800000-0000-7000-8000-'||lpad(to_hex(n),12,'0'))::uuid,
        'synthetic.highwater','synthetic.shared',
        ('01810000-0000-7000-8000-'||lpad(to_hex(CASE WHEN n<=8192 THEN (n-1)/1024+1 ELSE 9 END),12,'0'))::uuid,
        uuidv7(),n,'unknown',n,'{}',
        CASE WHEN n>8192 THEN 9223372036854775790 WHEN (n-1)%2048=0 THEN 4398046511104+(n-1)/2048 ELSE n END,
        decode(repeat('01',32),'hex'),'open',CASE WHEN n=2049 THEN 'removed' ELSE 'present' END
      FROM generate_series(1,16384)n;
      INSERT INTO audit.archived(event_id,archived_seq,reason) VALUES('01800000-0000-7000-8000-000000000001',1,'archive');
      ANALYZE core.runs; ANALYZE core.events").await.unwrap();
    let mut names = (0..4)
        .map(|n| format!("synthetic.highwater.{n}"))
        .collect::<Vec<_>>();
    names.extend([
        "synthetic.empty".into(),
        "synthetic.unknown".into(),
        names[0].clone(),
    ]);
    let mut expected = (0..4)
        .map(|n| (format!("synthetic.highwater.{n}"), (1_i64 << 42) + n))
        .collect::<BTreeMap<_, _>>();
    expected.insert("synthetic.empty".into(), 0);
    expected.insert("synthetic.unknown".into(), 0);
    for compressed in [false, true] {
        if compressed {
            conn.execute(
                "SELECT compress_chunk(c) FROM show_chunks('core.events') c; ANALYZE core.events",
            )
            .await
            .unwrap();
        }
        assert_eq!(
            latest_reading_orders(&mut conn, &names).await.unwrap(),
            expected
        );
        assert!(
            latest_reading_orders(&mut conn, &[])
                .await
                .unwrap()
                .is_empty()
        );
        let empty = latest_reading_orders(
            &mut conn,
            &["synthetic.empty".into(), "synthetic.unknown".into()],
        )
        .await
        .unwrap();
        assert_eq!(empty.values().copied().collect::<Vec<_>>(), vec![0, 0]);
    }
}
