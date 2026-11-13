//! Existing output ranges are checked by event identity, independently of history size.
use super::*;
use std::collections::BTreeSet;

const NAME: &str = "fixture.dirty_ranges";
const KIND: &str = "synthetic.interval";

fn at(second: i64) -> Timestamp {
    Timestamp::from_unix_secs(1_700_000_000 + second).unwrap()
}

struct Ranges;
impl DirtyKeys for Ranges {
    fn keys(&self, _: &KeyInput<'_>) -> Vec<(String, Value)> {
        vec![]
    }
    fn affected_ranges(&self, _: &KeyInput<'_>) -> Vec<AffectedRange> {
        let range = AffectedRange {
            derivation: NAME,
            output_type: KIND,
            start_field: "start",
            end_field: "end",
            open_field: Some("open"),
            start: at(12).0,
            end: at(15).0,
        };
        vec![range.clone(), range]
    }
}

fn event(kind: &str, payload: Value) -> Event {
    Event::submitted(
        sinex_model::KindRef {
            name: kind.into(),
            version: 1,
        },
        payload.clone(),
        "synthetic".into(),
        Origin::Record {
            location: RecordLocation {
                material: MaterialId::new(),
                anchor: 0,
            },
            output_index: 0,
        },
        ReadingOrder(1),
        *blake3::hash(&serde_json::to_vec(&payload).unwrap()).as_bytes(),
        Class::Open,
    )
}

#[devkit::test]
async fn dirty_output_ranges_preserve_boundaries_without_scanning_unrelated_events(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let declarations: Vec<_> = [KIND, "synthetic.other_interval"]
        .into_iter()
        .map(|name| {
            sinex_model::KindDeclaration::schemaless(
                sinex_model::KindRef {
                    name: name.into(),
                    version: 1,
                },
                "synthetic".into(),
            )
        })
        .collect();
    sinex_schema::kinds::register(&mut conn, "synthetic", &declarations)
        .await
        .unwrap();
    let mut events: Vec<_> = (0..8192)
        .map(|n| event(KIND, json!({"n":n,"start":at(0),"end":at(30),"open":false})))
        .collect();
    let mut outputs = Vec::new();
    for (label, start, end, open) in [
        ("overlap", 10, Some(20), false),
        ("before", 0, Some(5), false),
        ("after", 30, Some(40), false),
        ("no_end", 11, None, false),
        ("open", 0, Some(1), true),
        ("closed", 0, Some(1), false),
        ("archived", 11, Some(13), false),
        ("wrong_kind", 11, Some(13), false),
        ("other_derivation", 11, Some(13), false),
        ("start_boundary", 15, Some(20), false),
        ("end_boundary", 0, Some(12), false),
        ("future_open", 20, Some(21), true),
        ("overlap", 11, Some(14), false),
    ] {
        let reading = event(
            if label == "wrong_kind" {
                "synthetic.other_interval"
            } else {
                KIND
            },
            json!({"label":label,"start":at(start),"end":end.map(at),"open":open}),
        );
        outputs.push((label, reading.id));
        events.push(reading);
    }
    admit_one(&mut conn, &batch(events)).await;
    let archived = outputs
        .iter()
        .find(|(label, _)| *label == "archived")
        .unwrap()
        .1;
    crate::lifecycle::move_events(
        &mut conn,
        sinex_model::lifecycle::Act::Archive,
        vec![archived],
        &NoDerivations,
    )
    .await;
    for (label, id) in &outputs {
        sqlx::query("INSERT INTO derivation.outputs(derivation,key,event_id) VALUES($1,$2,$3)")
            .bind(if *label == "other_derivation" {
                "fixture.other_ranges"
            } else {
                NAME
            })
            .bind(json!({"label":label}))
            .bind(id.0)
            .execute(&mut *conn)
            .await
            .unwrap();
    }
    sqlx::raw_sql("ANALYZE core.events; ANALYZE derivation.outputs")
        .execute(&mut *conn)
        .await
        .unwrap();
    let position = crate::query::current_seq(&mut conn).await.unwrap();
    let mut probe = PgConnection::connect(&db.url(None)).await.unwrap();
    let mut tx = probe.begin().await.unwrap();
    // Prefer whole-relation joins where legal, reproducing the long-history
    // cardinality risk without making this fixture depend on planner estimates.
    sqlx::raw_sql("SET LOCAL max_parallel_workers_per_gather=0; SET LOCAL enable_nestloop=off; SET LOCAL enable_indexonlyscan=off")
        .execute(&mut *tx).await.unwrap();
    let work = "SELECT coalesce(sum(seq_tup_read+idx_tup_fetch),0)::bigint FROM pg_stat_xact_user_tables WHERE relid='core.events'::regclass OR relid IN (SELECT show_chunks('core.events'))";
    let before: i64 = sqlx::query_scalar(work).fetch_one(&mut *tx).await.unwrap();
    let payload = json!({});
    let marked = derived::mark_inputs_dirty(
        &mut tx,
        &[KeyInput {
            event_type: "synthetic.trigger",
            ts_orig: None,
            payload: &payload,
        }],
        position,
        &Ranges,
    )
    .await
    .unwrap();
    let after: i64 = sqlx::query_scalar(work).fetch_one(&mut *tx).await.unwrap();
    assert!(
        after - before <= 2 * outputs.len() as i64,
        "selected output lookup visited {} event tuples",
        after - before
    );
    let expected: BTreeSet<String> = [
        "overlap",
        "no_end",
        "open",
        "start_boundary",
        "end_boundary",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect();
    let dirty: Vec<String> =
        sqlx::query_scalar("SELECT key->>'label' FROM derivation.dirty WHERE derivation=$1")
            .bind(NAME)
            .fetch_all(&mut *tx)
            .await
            .unwrap();
    assert_eq!(marked, expected.len());
    assert_eq!(dirty.into_iter().collect::<BTreeSet<_>>(), expected);

    // The predecessor join must expose the unrelated-history work that the
    // production query avoids, while selecting exactly the same keys.
    let before: i64 = sqlx::query_scalar(work).fetch_one(&mut *tx).await.unwrap();
    let original: Vec<Value> = sqlx::query_scalar(
        "SELECT DISTINCT o.key FROM derivation.outputs o JOIN core.events e ON e.id=o.event_id AND e.event_type=$2
         WHERE o.derivation=$1 AND (e.payload->>'start')::timestamptz <= $4
         AND ((e.payload->>'end') IS NULL OR (e.payload->>'end')::timestamptz >= $3 OR coalesce((e.payload->>'open')::boolean,false))
         AND NOT EXISTS(SELECT 1 FROM audit.archived a WHERE a.event_id=e.id)"
    ).bind(NAME).bind(KIND).bind(at(12).0).bind(at(15).0).fetch_all(&mut *tx).await.unwrap();
    let after: i64 = sqlx::query_scalar(work).fetch_one(&mut *tx).await.unwrap();
    assert!(
        after - before >= 8192,
        "original join visited only {} tuples",
        after - before
    );
    assert_eq!(
        original
            .into_iter()
            .map(|key| key["label"].as_str().unwrap().to_owned())
            .collect::<BTreeSet<_>>(),
        expected
    );
    tx.rollback().await.unwrap();
}

#[devkit::test]
async fn dirty_ranges_page_compressed_members_once_and_keep_distinct_keys(
    db: devkit::pg::TestDatabase,
) {
    use sqlx::Executor;
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let mut competing = PgConnection::connect(&db.url(None)).await.unwrap();
    assert!(!crate::writer_lock::try_claim(&mut competing).await.unwrap());
    sqlx::query("INSERT INTO core.events(id,event_type,source,run_id,material_id,anchor,ts_quality,commit_seq,payload,reading_order,record_hash,class)
      SELECT ('01800000-0000-7000-8000-'||lpad(to_hex(n),12,'0'))::uuid,
        CASE WHEN n=8793 THEN 'synthetic.other_interval' ELSE $1 END,
        'synthetic-ranges',uuidv7(),uuidv7(),n,'unknown',1,
        jsonb_build_object('start',$2::timestamptz,'end',$3::timestamptz,'open',false),
        1,decode(repeat('01',32),'hex'),'open' FROM generate_series(1,8794)n")
        .bind(KIND).bind(at(10).0).bind(at(20).0).execute(&mut *conn).await.unwrap();
    sqlx::query(
        "INSERT INTO derivation.outputs(derivation,key,event_id)
      SELECT $1,jsonb_build_object('label',(n-8193)%300),
        ('01800000-0000-7000-8000-'||lpad(to_hex(n),12,'0'))::uuid
      FROM generate_series(8193,8792)n",
    )
    .bind(NAME)
    .execute(&mut *conn)
    .await
    .unwrap();
    sqlx::query(
        r#"INSERT INTO derivation.outputs(derivation,key,event_id) VALUES
      ($1,'{"wrong_kind":true}','01800000-0000-7000-8000-000000002259'),
      ($1,'{"missing":true}','01800000-0000-7000-8000-00000000dead'),
      ('fixture.other_ranges','{"other_owner":true}','01800000-0000-7000-8000-00000000225a')"#,
    )
    .bind(NAME)
    .execute(&mut *conn)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO audit.archived(event_id,archived_seq,reason)
      SELECT ('01800000-0000-7000-8000-'||lpad(to_hex(n),12,'0'))::uuid,2,'archive'
      FROM unnest(ARRAY[8193,8493])n",
    )
    .execute(&mut *conn)
    .await
    .unwrap();
    conn.execute("SELECT compress_chunk(c) FROM show_chunks('core.events')c")
        .await
        .unwrap();
    conn.execute("ANALYZE core.events; ANALYZE derivation.outputs; ANALYZE audit.archived")
        .await
        .unwrap();
    let compressed: String = sqlx::query_scalar("SELECT compress_relid::regclass::text FROM _timescaledb_catalog.compression_settings WHERE relid IN(SELECT show_chunks('core.events')) AND compress_relid IS NOT NULL")
        .fetch_one(&mut *conn).await.unwrap();
    let batches: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM {compressed}"))
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    assert!(batches >= 8);
    let mut tx = sqlx::Connection::begin(&mut *conn).await.unwrap();
    tx.execute("SET LOCAL plan_cache_mode=auto; SET LOCAL max_parallel_workers_per_gather=0")
        .await
        .unwrap();
    let work = "SELECT coalesce(sum(seq_tup_read+idx_tup_fetch),0)::bigint FROM pg_stat_xact_user_tables WHERE relid IN(SELECT compress_relid FROM _timescaledb_catalog.compression_settings WHERE relid IN(SELECT show_chunks('core.events')) AND compress_relid IS NOT NULL)";
    let before: i64 = sqlx::query_scalar(work).fetch_one(&mut *tx).await.unwrap();
    let payload = json!({});
    let inputs: Vec<_> = (0..256)
        .map(|_| KeyInput {
            event_type: "synthetic.trigger",
            ts_orig: None,
            payload: &payload,
        })
        .collect();
    for _ in 0..8 {
        assert_eq!(
            derived::mark_inputs_dirty(&mut tx, &inputs, 2, &Ranges)
                .await
                .unwrap(),
            299
        );
        let keys: Vec<Value> =
            sqlx::query_scalar("SELECT key FROM derivation.dirty WHERE derivation=$1 ORDER BY key")
                .bind(NAME)
                .fetch_all(&mut *tx)
                .await
                .unwrap();
        assert_eq!(
            keys.into_iter().collect::<std::collections::HashSet<_>>(),
            (1..300).map(|n| json!({"label":n})).collect()
        );
    }
    let new_work = sqlx::query_scalar::<_, i64>(work)
        .fetch_one(&mut *tx)
        .await
        .unwrap()
        - before;
    assert!(
        new_work <= 8 * 3 * 4 * batches,
        "paged payloads read {new_work} compressed metadata tuples for {batches} batches"
    );
    // The old per-range/per-ID path is a real work and result control. A
    // forced generic plan is deliberately not claimed to support ID pruning.
    tx.execute("SET LOCAL plan_cache_mode=force_generic_plan")
        .await
        .unwrap();
    let before: i64 = sqlx::query_scalar(work).fetch_one(&mut *tx).await.unwrap();
    let old:Vec<Value>=sqlx::query_scalar("SELECT DISTINCT o.key FROM unnest($1::text[],$2::text[])r(derivation,event_type)
      JOIN derivation.outputs o ON o.derivation=r.derivation
      CROSS JOIN LATERAL(SELECT payload FROM core.events WHERE id=o.event_id AND event_type=r.event_type OFFSET 0)e
      WHERE(e.payload->>'start')::timestamptz <= $4
        AND((e.payload->>'end')IS NULL OR(e.payload->>'end')::timestamptz >= $3 OR coalesce((e.payload->>'open')::boolean,false))
        AND NOT EXISTS(SELECT 1 FROM audit.archived a WHERE a.event_id=o.event_id)")
        .bind(vec![NAME,NAME]).bind(vec![KIND,KIND]).bind(at(12).0).bind(at(15).0)
        .persistent(false).fetch_all(&mut *tx).await.unwrap();
    let old_work = sqlx::query_scalar::<_, i64>(work)
        .fetch_one(&mut *tx)
        .await
        .unwrap()
        - before;
    assert_eq!(
        old.into_iter().collect::<std::collections::HashSet<_>>(),
        (1..300).map(|n| json!({"label":n})).collect()
    );
    assert!(
        old_work >= 598 * batches,
        "old per-ID control read only {old_work} compressed metadata tuples for {batches} batches"
    );
    assert!(!crate::writer_lock::try_claim(&mut competing).await.unwrap());
    tx.rollback().await.unwrap();
}
