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
async fn dirty_output_ranges_mark_overlapping_keys_and_their_boundaries(
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
    let position = crate::query::current_seq(&mut conn).await.unwrap();
    let mut probe = PgConnection::connect(&db.url(None)).await.unwrap();
    let mut tx = probe.begin().await.unwrap();
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
    tx.rollback().await.unwrap();
}

#[devkit::test]
async fn dirty_ranges_page_compressed_members_and_keep_distinct_keys(db: devkit::pg::TestDatabase) {
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
      ($1,'{"label":1.5}','01800000-0000-7000-8000-000000002002'),
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
    let mut tx = sqlx::Connection::begin(&mut *conn).await.unwrap();
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
            300
        );
        let keys: Vec<Value> =
            sqlx::query_scalar("SELECT key FROM derivation.dirty WHERE derivation=$1 ORDER BY key")
                .bind(NAME)
                .fetch_all(&mut *tx)
                .await
                .unwrap();
        assert_eq!(
            keys.into_iter().collect::<std::collections::HashSet<_>>(),
            (1..300)
                .map(|n| json!({"label":n}))
                .chain([json!({"label":1.5})])
                .collect()
        );
    }
    // Run the page queries on another connection so this writer's
    // transaction stays untouched.
    let mut checked = PgConnection::connect(&db.url(None)).await.unwrap();
    let targets = vec![NAME.to_owned()];
    let first = sqlx::query_file!(
        "src/admission/affected_output_page.sql",
        &targets,
        None::<&str>,
        None::<&Value>,
        None::<Uuid>
    )
    .fetch_all(&mut checked)
    .await
    .unwrap();
    assert_eq!(first.len(), 256);
    let last = first.last().unwrap();
    let second = sqlx::query_file!(
        "src/admission/affected_output_page.sql",
        &targets,
        Some(last.derivation.as_str()),
        Some(&last.key),
        Some(last.event_id)
    )
    .fetch_all(&mut checked)
    .await
    .unwrap();
    assert_eq!(second.len(), 256);
    assert!(second.iter().all(|row| {
        !first
            .iter()
            .any(|prior| prior.key == row.key && prior.event_id == row.event_id)
    }));
    let ids: Vec<_> = first.iter().map(|row| row.event_id).collect();
    assert!(
        ids.len()
            > ids
                .iter()
                .copied()
                .collect::<std::collections::HashSet<_>>()
                .len(),
        "duplicate event has distinct owners in one page"
    );
    let owners: Vec<_> = first.iter().map(|row| row.derivation.clone()).collect();
    let page_keys: Vec<_> = first.iter().map(|row| row.key.clone()).collect();
    let output_types = vec![KIND.to_owned()];
    let names = vec![NAME.to_owned(); 2];
    let types = vec![KIND.to_owned(); 2];
    let starts = vec!["start".to_owned(); 2];
    let ends = vec!["end".to_owned(); 2];
    let from = vec![at(12).0; 2];
    let until = vec![at(15).0; 2];
    let open = vec![Some("open".to_owned()); 2];
    let mut checked_tx = checked.begin().await.unwrap();
    let checked_keys = sqlx::query_file!(
        "src/admission/affected_outputs.sql",
        &ids,
        &owners,
        &page_keys,
        &output_types,
        &names,
        &types,
        &starts,
        &ends,
        &from,
        &until,
        &open as &[Option<String>]
    )
    .fetch_all(&mut *checked_tx)
    .await
    .unwrap();
    assert!(checked_keys.iter().all(|row| row.derivation == NAME));
    assert_eq!(
        checked_keys
            .into_iter()
            .map(|row| row.key)
            .collect::<std::collections::HashSet<_>>(),
        page_keys.into_iter().collect()
    );
    checked_tx.rollback().await.unwrap();
    checked.close().await.unwrap();
    assert!(!crate::writer_lock::try_claim(&mut competing).await.unwrap());
    tx.rollback().await.unwrap();
}
