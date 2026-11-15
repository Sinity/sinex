use super::*;
use sqlx::Executor;

const OLD: &str = include_str!("old_page.sql");
const NEW: &str = include_str!("../page.sql");

async fn rows(
    conn: &mut PgConnection,
    query: &str,
    after: i64,
    limit: i64,
    bytes: i64,
) -> Vec<Value> {
    sqlx::query_scalar(&format!("SELECT to_jsonb(p) FROM ({query}) p"))
        .bind(after)
        .bind(limit)
        .bind(bytes)
        .persistent(false)
        .fetch_all(conn)
        .await
        .unwrap()
}
async fn work(conn: &mut PgConnection) -> i64 {
    sqlx::query_scalar("SELECT coalesce(sum(seq_tup_read+idx_tup_fetch),0)::bigint FROM pg_stat_xact_user_tables WHERE relid='core.events'::regclass OR relid IN(SELECT show_chunks('core.events')) OR relid IN(SELECT compress_relid FROM _timescaledb_catalog.compression_settings WHERE relid IN(SELECT show_chunks('core.events')))")
        .fetch_one(conn).await.unwrap()
}

#[devkit::test]
async fn commit_pairs_preserve_full_pages_and_bound_legacy_uuid_hydration(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    // Legacy random UUID suffixes have no relationship to commit order. The
    // native UUID time dimension requires the version-7 envelope. All rows share
    // a time chunk; timestamps retain the ordinary compression ordering.
    conn.execute("INSERT INTO core.events(id,event_type,source,run_id,material_id,anchor,output_index,parents,ts_orig,ts_quality,ts_persisted,commit_seq,payload,reading_order,record_hash,class)
      SELECT ('01800000-0000-7000-8'||substr(md5(n::text),1,3)||'-'||substr(md5(n::text),5,12))::uuid,
        'synthetic.page','synthetic.page', '01800000-0000-4000-8000-000000000001'::uuid,
        '01800000-0000-4000-8000-000000000002'::uuid,n,n%3,
        ARRAY['01800000-0000-4000-8000-000000000003'::uuid],
        '2022-04-01'::timestamptz+n*interval '1 second','unknown',
        '2022-04-01'::timestamptz+n*interval '1 second',n,
        jsonb_build_object('n',n,'text',repeat(md5(n::text),128)),n,decode(md5(n::text),'hex'),
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
        tx.execute("SET LOCAL max_parallel_workers_per_gather=0; SET LOCAL jit=off")
            .await
            .unwrap();
        for (after, limit, bytes) in [
            (8192, 16, i64::MAX),
            (8192, 16, 1),
            (8447, 16, i64::MAX),
            (8448, 16, 1),
            (8450, 16, 1),
        ] {
            assert_eq!(
                rows(&mut tx, NEW, after, limit, bytes).await,
                rows(&mut tx, OLD, after, limit, bytes).await
            );
        }
        let all = after(&mut tx, 8192, 16, i64::MAX).await.unwrap();
        assert_eq!(all.events.len(), 16);
        for (offset, event) in all.events.iter().enumerate() {
            let n = 8193 + offset as i64;
            assert_eq!(event.commit_seq, n);
            assert_eq!(event.payload["n"], n);
            assert_eq!(event.anchor, Some(n));
            assert_eq!(event.output_index, n % 3);
            assert_eq!(
                event.class,
                match n % 3 {
                    0 => "open",
                    1 => "guarded",
                    _ => "sealed",
                }
            );
        }
        let exact: i64 = sqlx::query_scalar(
            "SELECT pg_column_size(payload)::bigint FROM core.events WHERE commit_seq=8193",
        )
        .fetch_one(&mut *tx)
        .await
        .unwrap();
        assert_eq!(
            rows(&mut tx, NEW, 8192, 16, exact).await,
            rows(&mut tx, OLD, 8192, 16, exact).await
        );
        assert_eq!(
            after(&mut tx, 8192, 16, exact).await.unwrap().events.len(),
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
        let expected = rows(&mut tx, OLD, 8192, 16, i64::MAX).await;
        let before = work(&mut tx).await;
        for _ in 0..8 {
            assert_eq!(rows(&mut tx, OLD, 8192, 16, i64::MAX).await, expected);
        }
        let old = work(&mut tx).await - before;
        let before = work(&mut tx).await;
        for _ in 0..8 {
            assert_eq!(rows(&mut tx, NEW, 8192, 16, i64::MAX).await, expected);
        }
        let new = work(&mut tx).await - before;
        let before = work(&mut tx).await;
        for _ in 0..8 {
            let page = after(&mut tx, 8450, 16, 4096).await.unwrap();
            assert!(page.events.is_empty());
            assert_eq!(page.up_to, None);
        }
        let quiet = work(&mut tx).await - before;
        eprintln!("commit-pair compressed={compressed} old={old} new={new} quiet={quiet}");
        assert_eq!(quiet, 0, "quiet head must not hydrate history");
        if compressed {
            assert!(
                new < old,
                "selected commit pairs must reduce actual compressed tuple visits: {old} -> {new}"
            );
        }
        tx.rollback().await.unwrap();
    }
}
