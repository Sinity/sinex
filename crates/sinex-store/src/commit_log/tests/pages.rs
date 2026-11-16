use super::*;
use sqlx::Executor;

#[devkit::test]
async fn commit_pages_follow_commit_order_and_byte_bounds(db: devkit::pg::TestDatabase) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let declaration = sinex_model::KindDeclaration::schemaless(
        sinex_model::KindRef {
            name: "synthetic.page".into(),
            version: 1,
        },
        "synthetic".into(),
    );
    sinex_schema::kinds::register(&mut conn, "synthetic", &[declaration])
        .await
        .unwrap();
    // Random UUID suffixes have no relationship to commit order. TimescaleDB's
    // UUID time dimension requires the version-7 envelope. All rows share
    // a time chunk; timestamps keep the usual compression ordering.
    conn.execute("INSERT INTO core.events(id,event_type,source,run_id,material_id,record_offset,output_index,parents,ts_orig,ts_quality,ts_persisted,commit_seq,payload,reading_order,record_hash,class)
      SELECT ('01800000-0000-7000-8'||substr(md5(n::text),1,3)||'-'||substr(md5(n::text),5,12))::uuid,
        'synthetic.page','synthetic.page', '01800000-0000-4000-8000-000000000001'::uuid,
        CASE WHEN n%2=0 THEN '01800000-0000-4000-8000-000000000002'::uuid END,
        CASE WHEN n%2=0 THEN n END,n%3,
        CASE WHEN n%2=1 THEN ARRAY['01800000-0000-4000-8000-000000000003'::uuid] END,
        '2022-04-01'::timestamptz+n*interval '1 second','intrinsic',
        '2022-04-01'::timestamptz+n*interval '1 second',n,
        jsonb_build_object('n',n,'text',repeat(md5(n::text),128)),n,decode(repeat(md5(n::text),2),'hex'),
        CASE n%3 WHEN 0 THEN 'open' WHEN 1 THEN 'guarded' ELSE 'sealed' END
      FROM generate_series(1,8448)n;
      INSERT INTO core.commit_log(commit_seq) VALUES(8449),(8450);
      UPDATE core.counters SET value=8450 WHERE name='commit_seq'")
        .await.unwrap();
    for compressed in [false, true] {
        if compressed {
            conn.execute(
                "SELECT compress_chunk(c) FROM show_chunks('core.events') c; ANALYZE core.events",
            )
            .await
            .unwrap();
        }
        let mut tx = conn.begin().await.unwrap();
        let all = after(&mut tx, 8192, 16, i64::MAX).await.unwrap();
        assert_eq!(all.events.len(), 16);
        for (offset, event) in all.events.iter().enumerate() {
            let n = 8193 + offset as i64;
            assert_eq!(event.row.commit_seq, n);
            assert_eq!(event.row.payload["n"], n);
            assert_eq!(event.row.record_offset, (n % 2 == 0).then_some(n));
            assert_eq!(event.row.output_index, n % 3);
            assert_eq!(
                event.row.class,
                match n % 3 {
                    0 => "open",
                    1 => "guarded",
                    _ => "sealed",
                }
            );
        }
        let payload_size = crate::payload_bytes(&all.events[0].row.payload);
        assert_eq!(
            after(&mut tx, 8192, 16, payload_size)
                .await
                .unwrap()
                .events
                .len(),
            1
        );
        let first = after(&mut tx, 8192, 16, 1).await.unwrap();
        assert_eq!(
            first.events.len(),
            1,
            "oversized first event remains readable"
        );
        assert_eq!(first.up_to, Some(8193));
        let meta = after(&mut tx, 8448, 16, 1).await.unwrap();
        assert!(meta.events.is_empty());
        assert_eq!(meta.up_to, Some(8450));
        // The last event is followed by two commits without events.
        let tail = after(&mut tx, 8447, 16, i64::MAX).await.unwrap();
        assert_eq!(
            tail.events
                .iter()
                .map(|e| e.row.commit_seq)
                .collect::<Vec<_>>(),
            [8448]
        );
        assert_eq!(tail.up_to, Some(8450));
        let quiet = after(&mut tx, 8450, 16, 1).await.unwrap();
        assert!(quiet.events.is_empty());
        assert_eq!(quiet.up_to, None);
        tx.rollback().await.unwrap();
    }
}

/// The head counter can cover a commit whose rows a page did not read; the
/// page then ends at the last position it read a row for, and the next page
/// reads that commit once its rows are visible.
#[devkit::test]
async fn a_page_ends_at_the_last_position_it_read_a_row_for(db: devkit::pg::TestDatabase) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let declaration = sinex_model::KindDeclaration::schemaless(
        sinex_model::KindRef {
            name: "synthetic.page".into(),
            version: 1,
        },
        "synthetic".into(),
    );
    sinex_schema::kinds::register(&mut conn, "synthetic", &[declaration])
        .await
        .unwrap();
    let insert = |n: i64| {
        format!(
            "INSERT INTO core.events(id,event_type,source,run_id,parents,ts_orig,ts_quality,commit_seq,payload,reading_order,record_hash)
             VALUES (uuidv7(),'synthetic.page','synthetic.page',uuidv7(),ARRAY[uuidv7()],now(),'intrinsic',{n},
               jsonb_build_object('n',{n}),{n},decode(repeat(md5('{n}'),2),'hex'))"
        )
    };
    conn.execute(
        format!(
            "{}; {}; UPDATE core.counters SET value=4 WHERE name='commit_seq'",
            insert(1),
            insert(2)
        )
        .as_str(),
    )
    .await
    .unwrap();
    let read = after(&mut conn, 0, 16, i64::MAX).await.unwrap();
    assert_eq!(read.up_to, Some(2));
    let unseen = after(&mut conn, 2, 16, i64::MAX).await.unwrap();
    assert_eq!(unseen.up_to, None);
    assert!(unseen.events.is_empty());
    conn.execute(format!("{}; {}", insert(3), insert(4)).as_str())
        .await
        .unwrap();
    let later = after(&mut conn, 2, 16, i64::MAX).await.unwrap();
    assert_eq!(later.up_to, Some(4));
    assert_eq!(
        later
            .events
            .iter()
            .map(|e| e.row.commit_seq)
            .collect::<Vec<_>>(),
        [3, 4]
    );
}
