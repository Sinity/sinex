use super::*;

#[devkit::test]
async fn every_search_kind_keeps_words_phrases_prefixes_text_and_snippets(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    crate::read_models::tests::admit_search_kinds(&mut conn, Class::Open).await;
    catch_up_search_text(&mut conn).await.unwrap();
    for text in [
        "events where STRASSE",
        "events where CAFÉ",
        "events where cafe\u{301}",
        "events where cargo test",
        "events where \"cargo test\"",
        "events where car*",
        "events where NOT absent*",
    ] {
        let answer = ask(&mut conn, text).await;
        assert_eq!(answer.total, 3, "{text}: {:?}", answer.rows);
    }
    for text in [
        "events where absent*",
        "events where \"test cargo\"",
        "events where NOT car*",
    ] {
        assert_eq!(ask(&mut conn, text).await.total, 0, "{text}");
    }
    let rows = ask(
        &mut conn,
        "cargo | fields event_type,text,snippet | sort by event_type",
    )
    .await;
    assert_eq!(rows.total, 3);
    for row in rows.rows {
        assert!(row["text"].as_str().unwrap().contains("strasse café café"));
        assert!(row["snippet"].as_str().unwrap().contains("[cargo]"));
    }
}

#[devkit::test]
async fn both_prefix_branches_obey_class_grants(db: devkit::pg::TestDatabase) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    crate::read_models::tests::admit_search_kinds(&mut conn, Class::Guarded).await;
    catch_up_search_text(&mut conn).await.unwrap();
    for grants in ["", "guarded", ""] {
        set_grants(&mut conn, grants).await.unwrap();
        for text in [
            "events where car*",
            "events where cargo",
            "events where \"cargo test\"",
        ] {
            assert_eq!(
                ask(&mut conn, text).await.total,
                if grants.is_empty() { 0 } else { 3 },
                "{grants}: {text}"
            );
        }
        let answer = ask(&mut conn, "events | fields text").await;
        if grants.is_empty() {
            for row in answer.rows {
                assert!(row["text"].is_null());
            }
        }
    }
    sqlx::query("SET ROLE sinex_reader")
        .execute(&mut conn)
        .await
        .unwrap();
    let visible: i64 = sqlx::query_scalar("SELECT count(*) FROM core.visible_search_text")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(visible, 0);
    let denied = sqlx::query("SELECT text FROM core.search_text")
        .execute(&mut conn)
        .await
        .unwrap_err();
    assert_eq!(
        denied.as_database_error().unwrap().code().as_deref(),
        Some("42501")
    );
}

#[devkit::test]
async fn eligible_prefix_branch_can_use_its_trigram_index(db: devkit::pg::TestDatabase) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    crate::read_models::tests::admit_search_kinds(&mut conn, Class::Open).await;
    catch_up_search_text(&mut conn).await.unwrap();
    // A three-event fixture can legitimately start at the event primary key
    // and probe the text primary key. Give the planner a selective prefix
    // among unrelated readings, with real statistics for both sides.
    sqlx::query(
        "WITH events AS (
           INSERT INTO core.events(id,event_type,source,run_id,material_id,anchor,ts_quality,
             commit_seq,payload,reading_order,record_hash,class)
           SELECT uuidv7(),'command.imported','synthetic-plan',uuidv7(),uuidv7(),n,'unknown',
             1,'{}',1,decode(repeat('01',32),'hex'),'open' FROM generate_series(1,5000)n
           RETURNING id
         ), words AS (
           INSERT INTO core.search_text(event_id,text)
           SELECT id,'unrelated synthetic reading '||id::text FROM events
         ) INSERT INTO core.search_trigram(event_id,text)
           SELECT id,'unrelated synthetic reading '||id::text FROM events",
    )
    .execute(&mut conn)
    .await
    .unwrap();
    // Match the upgrade's bulk-built index. Fixture inserts otherwise leave
    // a GIN pending list whose scan cost can make a primary-key scan cheaper.
    sqlx::raw_sql(
        "REINDEX INDEX core.search_trigram_text;
         ANALYZE core.events; ANALYZE core.search_text; ANALYZE core.search_trigram",
    )
    .execute(&mut conn)
    .await
    .unwrap();
    sqlx::query("SET enable_seqscan=off")
        .execute(&mut conn)
        .await
        .unwrap();
    let context = ctx();
    let bound = bind("events where car*", &context).unwrap();
    let compiled = compile::Compiler::new(&context, current_seq(&mut conn).await.unwrap())
        .compile(&bound)
        .unwrap();
    let (statement, params) = compiled.count.render();
    let statement = format!("EXPLAIN (FORMAT JSON) {statement}");
    let plan: serde_json::Value = sql::bind_scalar(sqlx::query_scalar(&statement), params)
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert!(plan.to_string().contains("search_trigram_text"), "{plan}");
    assert_eq!(ask(&mut conn, "events where car*").await.total, 3);
}

async fn flags(conn: &mut PgConnection) -> Vec<(String, bool)> {
    sqlx::query_as(
        "SELECT e.event_type, (t.event_id IS NOT NULL) FROM core.search_text s JOIN core.events e ON e.id=s.event_id LEFT JOIN core.search_trigram t ON t.event_id=s.event_id ORDER BY 1",
    )
    .fetch_all(conn)
    .await
    .unwrap()
}

/// The bulk upgrade changes physical index placement without changing public
/// search answers or the consumer's committed coverage.
#[devkit::test]
async fn search_answers_and_position_hold_across_bulk_trigram_upgrade(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let fresh = crate::test_schema::catalog(&mut conn).await;
    crate::read_models::tests::admit_search_kinds(&mut conn, Class::Open).await;
    let position = catch_up_search_text(&mut conn).await.unwrap();
    let texts = [
        r#"events where car*"#,
        r#"events where cargo"#,
        r#"events where "cargo test""#,
        r#"events where STRASSE"#,
        r#"events where NOT car*"#,
        r#"events where absent*"#,
    ];
    let mut expected = Vec::new();
    for text in texts {
        let answer = ask(
            &mut conn,
            &format!("{text} | fields id,event_type,text | sort by id"),
        )
        .await;
        expected.push((answer.total, answer.rows));
    }
    let indexed = flags(&mut conn).await;
    crate::test_schema::downgrade(&mut conn).await;
    crate::test_schema::remove_version_4(&mut conn).await;
    sqlx::query("UPDATE sinex_schema.applied SET version=2")
        .execute(&mut conn)
        .await
        .unwrap();
    sinex_schema::apply(&mut conn).await.unwrap();
    assert_eq!(catch_up_search_text(&mut conn).await.unwrap(), position);
    for (text, expected) in texts.iter().zip(expected) {
        let answer = ask(
            &mut conn,
            &format!("{text} | fields id,event_type,text | sort by id"),
        )
        .await;
        assert_eq!((answer.total, answer.rows), expected, "{text}");
    }
    assert_eq!(flags(&mut conn).await, indexed);
    sinex_schema::online::finish(&mut conn).await.unwrap();
    assert_eq!(crate::test_schema::catalog(&mut conn).await, fresh);
}

#[devkit::test]
async fn bulk_trigram_copy_keeps_each_id_within_one_uuid_millisecond(db: devkit::pg::TestDatabase) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let payloads = [
        (
            "command.imported",
            json!({"command":"cargo test", "hostname":"synthetic", "n":0}),
        ),
        (
            "journal.entry",
            json!({"cursor":"synthetic", "message":"cargo test"}),
        ),
        (
            "lake.observed",
            json!({"observed_at":"2026-09-14T08:00:00Z", "lane":"synthetic", "host":"synthetic", "seq":1, "payload":{"message":"cargo test"}}),
        ),
    ];
    let events = payloads
        .into_iter()
        .enumerate()
        .map(|(index, (kind, payload))| {
            let mut event = (sinex_model::event_types::find(kind).unwrap().build)(
                &payload,
                "synthetic.search",
                Origin::Record {
                    location: RecordLocation {
                        material: MaterialId::new(),
                        anchor: 0,
                    },
                    output_index: 0,
                },
                ReadingOrder(1),
                [1; 32],
                Class::Open,
            )
            .unwrap();
            // All three UUIDv7 identities share their timestamp; their low bits
            // still order distinct records across one-row rewrite batches.
            event.id = EventId(Uuid::from_u128(
                0x01900000_0000_7000_8000_000000000001 + index as u128,
            ));
            event
        })
        .collect();
    let mut tx = conn.begin().await.unwrap();
    tx.admit(
        &Batch {
            run: RunId::new(),
            kind: BatchKind::Api {
                actor: "synthetic".into(),
            },
            records: vec![RecordOutcome::Events {
                location: None,
                events,
            }],
            archive: vec![],
        },
        &NoDerivations,
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    catch_up_search_text(&mut conn).await.unwrap();
    let expected = flags(&mut conn).await;
    assert_eq!(
        expected,
        [
            ("command.imported".into(), true),
            ("journal.entry".into(), false),
            ("lake.observed".into(), false)
        ]
    );
    crate::test_schema::downgrade(&mut conn).await;
    crate::test_schema::remove_version_4(&mut conn).await;
    sqlx::query("UPDATE sinex_schema.applied SET version=2")
        .execute(&mut conn)
        .await
        .unwrap();
    sinex_schema::apply(&mut conn).await.unwrap();
    sinex_schema::online::finish(&mut conn).await.unwrap();
    assert_eq!(flags(&mut conn).await, expected);
}

#[devkit::test]
async fn indexed_words_keep_boolean_stage_and_historical_visibility(db: devkit::pg::TestDatabase) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let ids = admit_commands(
        &mut conn,
        &[
            ("cargo test", "/one"),
            ("cargo build", "/two"),
            ("echo ready", "/three"),
        ],
    )
    .await;
    catch_up_search_text(&mut conn).await.unwrap();
    for (text, expected) in [
        ("events where cargo AND test", vec![ids[0]]),
        ("events | where cargo AND test", vec![ids[0]]),
        ("events where cargo OR ready", ids.clone()),
        ("events where NOT cargo", vec![ids[2]]),
        ("events | limit 1 | where ready", vec![]),
        ("events where \"cargo test\"", vec![ids[0]]),
    ] {
        let answer = ask(&mut conn, text).await;
        let actual: std::collections::BTreeSet<_> = answer
            .rows
            .iter()
            .map(|row| row["id"].as_str().unwrap().parse::<Uuid>().unwrap())
            .collect();
        let expected: std::collections::BTreeSet<_> =
            expected.into_iter().flat_map(|(a, b)| [a, b]).collect();
        assert_eq!(actual, expected, "{text}");
        assert_eq!(answer.total as usize, expected.len(), "{text}");
    }
    let before = current_seq(&mut conn).await.unwrap();
    crate::lifecycle::move_events(
        &mut conn,
        sinex_model::lifecycle::Act::Archive,
        vec![EventId(ids[0].1)],
        &NoDerivations,
    )
    .await;
    assert_eq!(ask(&mut conn, "events where cargo AND test").await.total, 0);
    let old = ask(
        &mut conn,
        &format!("events where cargo AND test | as of seq {before}"),
    )
    .await;
    assert_eq!(old.total, 2);
    crate::lifecycle::move_events(
        &mut conn,
        sinex_model::lifecycle::Act::Restore,
        vec![EventId(ids[0].1)],
        &NoDerivations,
    )
    .await;
    assert_eq!(ask(&mut conn, "events where cargo AND test").await.total, 2);
}

#[devkit::test]
async fn indexed_words_do_not_decompress_unrelated_history(db: devkit::pg::TestDatabase) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let selected = admit_commands(&mut conn, &[("specificsearchneedle", "/")]).await;
    catch_up_search_text(&mut conn).await.unwrap();
    sqlx::query("WITH events AS (
        INSERT INTO core.events(id,event_type,source,run_id,material_id,anchor,ts_quality,commit_seq,payload,reading_order,record_hash,class)
        SELECT ((CASE WHEN n<=4096 THEN '01800000-0000-7000-8000-' ELSE '01810000-0000-7000-8000-' END)||lpad(to_hex(n),12,'0'))::uuid,
          'command.imported','synthetic-search-history',uuidv7(),uuidv7(),n,'unknown',1,'{}',1,decode(repeat('01',32),'hex'),'open'
        FROM generate_series(1,8192)n RETURNING id
      ) INSERT INTO core.search_text(event_id,text) SELECT id,'unrelated synthetic history '||id::text FROM events")
        .execute(&mut conn).await.unwrap();
    sqlx::query("SELECT compress_chunk(chunk) FROM show_chunks('core.events') chunks(chunk)
        WHERE chunk NOT IN (SELECT tableoid::regclass FROM core.events WHERE source IN ('terminal.atuin','derive.shell-fusion'))")
        .execute(&mut conn).await.unwrap();
    sqlx::raw_sql(
        "REINDEX INDEX core.search_text_tsv; ANALYZE core.events; ANALYZE core.search_text",
    )
    .execute(&mut conn)
    .await
    .unwrap();
    let names: Vec<String> = sqlx::query_scalar(
        "SELECT relname FROM pg_class WHERE oid IN (
        SELECT compress_relid FROM _timescaledb_catalog.compression_settings
        WHERE relid IN (SELECT show_chunks('core.events')) AND compress_relid IS NOT NULL)",
    )
    .fetch_all(&mut conn)
    .await
    .unwrap();
    assert_eq!(names.len(), 2);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM core.events WHERE source='synthetic-search-history'"
        )
        .fetch_one(&mut conn)
        .await
        .unwrap(),
        8192
    );
    let answer = ask(&mut conn, "specificsearchneedle").await;
    assert_eq!(answer.total, 2);
    assert_eq!(
        answer
            .rows
            .iter()
            .map(|row| row["id"].as_str().unwrap().parse::<Uuid>().unwrap())
            .collect::<std::collections::BTreeSet<_>>(),
        std::collections::BTreeSet::from([selected[0].0, selected[0].1])
    );
    let context = ctx();
    let bound = bind("specificsearchneedle", &context).unwrap();
    let compiled = compile::Compiler::new(&context, current_seq(&mut conn).await.unwrap())
        .compile(&bound)
        .unwrap();
    let (statement, params) = compiled.answer.render();
    let mut tx = conn.begin().await.unwrap();
    sqlx::raw_sql("SET LOCAL max_parallel_workers_per_gather=0; SET LOCAL enable_nestloop=off")
        .execute(&mut *tx)
        .await
        .unwrap();
    let plan: serde_json::Value = sql::bind_scalar(
        sqlx::query_scalar(&format!("EXPLAIN (ANALYZE,FORMAT JSON) {statement}")),
        params,
    )
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    fn batches(value: &serde_json::Value, names: &[String]) -> u64 {
        match value {
            serde_json::Value::Object(object) => {
                let own = object
                    .get("Relation Name")
                    .and_then(|v| v.as_str())
                    .filter(|name| names.iter().any(|n| n.as_str() == *name))
                    .map(|_| {
                        (object
                            .get("Actual Rows")
                            .and_then(|n| n.as_f64())
                            .unwrap_or(0.0)
                            * object
                                .get("Actual Loops")
                                .and_then(|n| n.as_f64())
                                .unwrap_or(0.0))
                        .ceil() as u64
                    })
                    .unwrap_or(0);
                own + object.values().map(|v| batches(v, names)).sum::<u64>()
            }
            serde_json::Value::Array(values) => values.iter().map(|v| batches(v, names)).sum(),
            _ => 0,
        }
    }
    assert_eq!(
        batches(&plan, &names),
        0,
        "indexed search decompressed unrelated history: {plan}"
    );
    // The predecessor's protected search semijoin reads the same unrelated
    // compressed history even though the full-text index selects two events.
    let original:serde_json::Value=sqlx::query_scalar("EXPLAIN (ANALYZE,FORMAT JSON) SELECT e.id FROM core.visible_events e WHERE EXISTS
        (SELECT 1 FROM core.visible_search_text s WHERE s.event_id=e.id AND s.tsv @@ plainto_tsquery('simple','specificsearchneedle'))")
        .fetch_one(&mut *tx).await.unwrap();
    assert_eq!(original[0]["Plan"]["Actual Rows"].as_f64().unwrap(), 2.0);
    assert!(
        batches(&original, &names) >= 8,
        "original search did not exercise unrelated compressed history: {original}"
    );
    tx.rollback().await.unwrap();
}
