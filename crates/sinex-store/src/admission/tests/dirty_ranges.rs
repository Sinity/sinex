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
    fn affected_ranges(&self, input: &KeyInput<'_>) -> Vec<AffectedRange> {
        let range = AffectedRange {
            key_match: (input.event_type == "synthetic.selected").then_some(("label", "selected")),
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
                record_offset: 0,
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
    sqlx::query("INSERT INTO core.events(id,event_type,source,run_id,material_id,record_offset,ts_quality,commit_seq,payload,reading_order,record_hash,class)
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
      ('fixture.other_ranges','{"other_key":true}','01800000-0000-7000-8000-00000000225a')"#,
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
    let mut inputs: Vec<_> = (0..256)
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
    let first = sqlx::query_file!(
        "src/admission/affected_output_page.sql",
        NAME,
        None::<&Value>,
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
        NAME,
        None::<&Value>,
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
    // The page holds 256 members whatever their archive state; the two
    // archived members of the first key stay out of the payload lookup.
    assert_eq!(first.iter().filter(|row| row.archived).count(), 2);
    let first: Vec<_> = first.into_iter().filter(|row| !row.archived).collect();
    let ids: Vec<_> = first.iter().map(|row| row.event_id).collect();
    assert!(
        ids.len()
            > ids
                .iter()
                .copied()
                .collect::<std::collections::HashSet<_>>()
                .len(),
        "one event is a member of two keys in one page"
    );
    let output_derivations: Vec<_> = first.iter().map(|row| row.derivation.clone()).collect();
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
        &output_derivations,
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

    // The matching keys follow the first 300 one-field keys. One member is
    // live and the others are archived or the wrong event type.
    let selected = json!({"label":"selected","case":1});
    let archived = json!({"label":"selected","case":2});
    let wrong_kind = json!({"label":"selected","case":3});
    sqlx::query(
        "INSERT INTO derivation.outputs(derivation,key,event_id) VALUES
        ($1,$2,'01800000-0000-7000-8000-000000002002'),
        ($1,$3,'01800000-0000-7000-8000-000000002001'),
        ($1,$4,'01800000-0000-7000-8000-000000002259')",
    )
    .bind(NAME)
    .bind(&selected)
    .bind(&archived)
    .bind(&wrong_kind)
    .execute(&mut *tx)
    .await
    .unwrap();
    let trigger = KeyInput {
        event_type: "synthetic.selected",
        ts_orig: None,
        payload: &payload,
    };
    let first_keys =
        sqlx::query_file_scalar!("src/admission/affected_key_page.sql", NAME, None::<&Value>)
            .fetch_all(&mut *tx)
            .await
            .unwrap();
    assert_eq!(first_keys.len(), 256);
    assert!(!first_keys.contains(&selected));
    let second_keys = sqlx::query_file_scalar!(
        "src/admission/affected_key_page.sql",
        NAME,
        first_keys.last()
    )
    .fetch_all(&mut *tx)
    .await
    .unwrap();
    assert!(second_keys.contains(&selected));
    let matching = vec![selected.clone(), archived, wrong_kind];
    let mut members = Vec::new();
    for key in &matching {
        members.extend(
            sqlx::query_file!(
                "src/admission/affected_output_page.sql",
                NAME,
                Some(key),
                None::<&Value>,
                None::<Uuid>
            )
            .fetch_all(&mut *tx)
            .await
            .unwrap(),
        );
    }
    assert_eq!(members.len(), 3, "one member under each selected key");
    assert!(members.iter().all(|row| matching.contains(&row.key)));
    assert_eq!(
        members.iter().filter(|row| !row.archived).count(),
        2,
        "only live members of selected keys reach the payload lookup"
    );
    let mixed = [trigger, inputs.pop().unwrap()];
    assert_eq!(
        derived::mark_inputs_dirty(&mut tx, &mixed, 3, &Ranges)
            .await
            .unwrap(),
        301
    );

    // A selected same-kind payload would fail its time conversion. A valid
    // admission cannot inspect this unrelated key's payload in the first place.
    let bad = uuid::Uuid::now_v7();
    sqlx::query("INSERT INTO core.events(id,event_type,source,run_id,material_id,record_offset,ts_quality,commit_seq,payload,reading_order,record_hash,class)
        VALUES ($1,$2,'synthetic-ranges',uuidv7(),uuidv7(),0,'unknown',4,jsonb_build_object('start','invalid'),1,decode(repeat('01',32),'hex'),'open')")
        .bind(bad).bind(KIND).execute(&mut *tx).await.unwrap();
    sqlx::query("INSERT INTO derivation.outputs(derivation,key,event_id) VALUES($1,$2,$3)")
        .bind(NAME)
        .bind(json!({"label":"unrelated"}))
        .bind(bad)
        .execute(&mut *tx)
        .await
        .unwrap();
    assert_eq!(
        derived::mark_inputs_dirty(&mut tx, &mixed[..1], 4, &Ranges)
            .await
            .unwrap(),
        1
    );
    let updated: Vec<Value> = sqlx::query_scalar(
        "SELECT key FROM derivation.dirty WHERE derivation=$1 AND last_dirty_seq=4",
    )
    .bind(NAME)
    .fetch_all(&mut *tx)
    .await
    .unwrap();
    assert_eq!(updated, [selected]);
    assert!(!crate::writer_lock::try_claim(&mut competing).await.unwrap());
    tx.rollback().await.unwrap();
}

const OTHER: &str = "fixture.other_ranges";

struct TwoDerivations;
impl DirtyKeys for TwoDerivations {
    fn keys(&self, _: &KeyInput<'_>) -> Vec<(String, Value)> {
        vec![]
    }
    fn affected_ranges(&self, _: &KeyInput<'_>) -> Vec<AffectedRange> {
        [NAME, OTHER]
            .into_iter()
            .map(|derivation| AffectedRange {
                key_match: None,
                derivation,
                output_type: KIND,
                start_field: "start",
                end_field: "end",
                open_field: None,
                start: at(12).0,
                end: at(15).0,
            })
            .collect()
    }
}

/// Blocks the membership scan of one page statement reads, from its plan.
async fn page_blocks(
    conn: &mut PgConnection,
    key: Option<&Value>,
    after: Option<(&Value, Uuid)>,
) -> f64 {
    let plan: Value = sqlx::query_scalar(&format!(
        "EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON,TIMING OFF) {}",
        include_str!("../affected_output_page.sql")
    ))
    .bind(NAME)
    .bind(key)
    .bind(after.map(|(key, _)| key))
    .bind(after.map(|(_, id)| id))
    .persistent(false)
    .fetch_one(conn)
    .await
    .unwrap();
    fn membership_scan(node: &Value) -> Option<&Value> {
        if node["Relation Name"] == "outputs" {
            return Some(node);
        }
        node["Plans"].as_array()?.iter().find_map(membership_scan)
    }
    let scan = membership_scan(&plan[0]["Plan"]).expect("the page reads memberships");
    scan["Shared Hit Blocks"].as_f64().unwrap() + scan["Shared Read Blocks"].as_f64().unwrap()
}

#[devkit::test]
async fn affected_output_pages_start_at_their_cursor(db: devkit::pg::TestDatabase) {
    use sqlx::Executor;
    const MEMBERS: i64 = 32_768;
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    // Every member overlaps the probed range. The first half is 257 small
    // keys and the second half one large key, whose last 64 members belong to
    // another derivation that is paged after the first. Members are stored in
    // key order, so table reads are as local for a late page as for the first.
    sqlx::query("INSERT INTO core.events(id,event_type,source,run_id,material_id,record_offset,ts_quality,commit_seq,payload,reading_order,record_hash,class)
      SELECT ('01800000-0000-7000-8000-'||lpad(to_hex(n),12,'0'))::uuid,$1,
        'synthetic-ranges',uuidv7(),uuidv7(),n,'unknown',1,
        jsonb_build_object('start',$2::timestamptz,'end',$3::timestamptz),
        1,decode(repeat('01',32),'hex'),'open' FROM generate_series(1,$4::bigint)n")
        .bind(KIND).bind(at(10).0).bind(at(20).0).bind(MEMBERS).execute(&mut conn).await.unwrap();
    sqlx::query(
        "INSERT INTO derivation.outputs(derivation,key,event_id)
      SELECT CASE WHEN n>$3-64 THEN $2 ELSE $1 END,
        jsonb_build_object('label',CASE WHEN n<=$3/2 THEN n/64 ELSE 512 END),
        ('01800000-0000-7000-8000-'||lpad(to_hex(n),12,'0'))::uuid
      FROM generate_series(1,$3::bigint)n",
    )
    .bind(NAME)
    .bind(OTHER)
    .bind(MEMBERS)
    .execute(&mut conn)
    .await
    .unwrap();
    conn.execute("VACUUM (ANALYZE) derivation.outputs")
        .await
        .unwrap();

    // Paging one derivation visits each of its members once, in key order,
    // and paging one key visits that key's members once.
    let large = json!({"label":512});
    for (key, expected) in [(None, MEMBERS - 64), (Some(&large), MEMBERS / 2 - 64)] {
        let mut after: Option<(Value, Uuid)> = None;
        let mut cursors: Vec<(String, Uuid)> = Vec::new();
        let mut members = 0;
        loop {
            let page = sqlx::query_file!(
                "src/admission/affected_output_page.sql",
                NAME,
                key,
                after.as_ref().map(|(key, _)| key),
                after.as_ref().map(|(_, id)| *id)
            )
            .fetch_all(&mut conn)
            .await
            .unwrap();
            let Some(last) = page.last() else { break };
            assert!(
                page.iter()
                    .all(|row| row.derivation == NAME && !row.archived)
            );
            assert!(page.iter().all(|row| key.is_none_or(|key| row.key == *key)));
            members += page.len() as i64;
            after = Some((last.key.clone(), last.event_id));
            cursors.push((last.key.to_string(), last.event_id));
        }
        assert_eq!(members, expected);
        let distinct: std::collections::HashSet<_> = cursors.iter().collect();
        assert_eq!(distinct.len(), cursors.len());
    }

    // A page reads the same blocks wherever its cursor stands: the scan
    // starts at the cursor, not at the first member of the derivation or key.
    let member = |n: i64| Uuid::parse_str(&format!("01800000-0000-7000-8000-{n:012x}")).unwrap();
    for key in [None, Some(&large)] {
        let late = (&large, member(MEMBERS - 1024));
        let first = page_blocks(&mut conn, key, None).await;
        let late = page_blocks(&mut conn, key, Some(late)).await;
        assert!(
            first <= 64.0 && late <= first + 16.0,
            "a late page read {late} blocks, the first page {first}"
        );
    }

    // Both derivations' members reach the range test.
    let mut tx = conn.begin().await.unwrap();
    let payload = json!({});
    let marked = derived::mark_inputs_dirty(
        &mut tx,
        &[KeyInput {
            event_type: "synthetic.trigger",
            ts_orig: None,
            payload: &payload,
        }],
        2,
        &TwoDerivations,
    )
    .await
    .unwrap();
    let dirty: Vec<(String, i64)> = sqlx::query_as(
        "SELECT derivation,count(*) FROM derivation.dirty GROUP BY derivation ORDER BY derivation",
    )
    .fetch_all(&mut *tx)
    .await
    .unwrap();
    assert_eq!(dirty, [(NAME.to_owned(), 258), (OTHER.to_owned(), 1)]);
    assert_eq!(marked, 258 + 1);
    tx.rollback().await.unwrap();
}
