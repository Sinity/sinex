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
    let denied = sqlx::query("SELECT text, trigram_indexed FROM core.search_text")
        .execute(&mut conn)
        .await
        .unwrap_err();
    assert_eq!(
        denied.as_database_error().unwrap().code().as_deref(),
        Some("42501")
    );
}

#[devkit::test]
async fn eligible_prefix_branch_can_use_partial_index(db: devkit::pg::TestDatabase) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    crate::read_models::tests::admit_search_kinds(&mut conn, Class::Open).await;
    catch_up_search_text(&mut conn).await.unwrap();
    // Tiny fixtures otherwise favor a sequential scan. Force index choice to
    // distinguish an applicable partial predicate from an unusable index.
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
    assert!(plan.to_string().contains("search_text_trigram"), "{plan}");
}

async fn flags(conn: &mut PgConnection) -> Vec<(String, bool)> {
    sqlx::query_as(
        "SELECT e.event_type, s.trigram_indexed FROM core.search_text s JOIN core.events e ON e.id=s.event_id ORDER BY 1",
    )
    .fetch_all(conn)
    .await
    .unwrap()
}

/// The upgrade leaves stored search rows in the trigram index; the daemon
/// rewrites them after start. Every answer and the search position read
/// during the rewrite equal those of the fully rewritten store, and its flags
/// equal those of a store indexed from the start.
#[devkit::test]
async fn search_answers_and_position_hold_while_trigram_rewrite_runs(db: devkit::pg::TestDatabase) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let fresh = crate::test_schema::catalog(&mut conn).await;
    crate::read_models::tests::admit_search_kinds(&mut conn, Class::Open).await;
    let position = catch_up_search_text(&mut conn).await.unwrap();
    let texts = [
        "events where car*",
        "events where cargo",
        "events where \"cargo test\"",
        "events where STRASSE",
        "events where NOT car*",
        "events where absent*",
    ];
    let mut totals = Vec::new();
    for text in texts {
        totals.push(ask(&mut conn, text).await.total);
    }
    assert_eq!(totals, [3, 3, 3, 3, 0, 0]);
    let indexed_flags = flags(&mut conn).await;

    crate::test_schema::downgrade(&mut conn).await;
    crate::test_schema::remove_version_4(&mut conn).await;
    sqlx::query("UPDATE sinex_schema.applied SET version=2")
        .execute(&mut conn)
        .await
        .unwrap();
    assert_eq!(
        sinex_schema::apply(&mut conn).await.unwrap(),
        sinex_schema::Applied::Upgraded {
            from: 2,
            to: sinex_schema::VERSION
        }
    );
    // The output projection is activated as at the daemon's start; nothing
    // was committed since, so the search position stays at the head.
    assert_eq!(catch_up_search_text(&mut conn).await.unwrap(), position);
    assert!(flags(&mut conn).await.iter().all(|(_, trigram)| *trigram));
    let mut stages = Vec::new();
    let mut search_steps = 0;
    loop {
        assert_eq!(current_seq(&mut conn).await.unwrap(), position);
        assert_eq!(
            crate::read_models::search_position(&mut conn)
                .await
                .unwrap(),
            position,
            "the rewrite never moves search coverage"
        );
        let mut answers = Vec::new();
        for text in texts {
            let answer = ask(&mut conn, text).await;
            answers.push((answer.total, answer.rows));
        }
        stages.push(answers);
        use sinex_schema::online::{Progress, Task};
        match sinex_schema::online::step(&mut conn, 1).await.unwrap() {
            Progress::Advanced {
                task: Task::SearchRewrite,
                ..
            }
            | Progress::Finished {
                task: Task::SearchRewrite,
            } => search_steps += 1,
            Progress::Advanced { .. } | Progress::Finished { .. } => {}
            Progress::Done => break,
            Progress::Waiting => panic!("no other session holds the work"),
        }
    }
    assert_eq!(search_steps, 4, "one search row per step, then the index");
    assert_eq!(flags(&mut conn).await, indexed_flags);
    let mut rewritten = Vec::new();
    for text in texts {
        let answer = ask(&mut conn, text).await;
        rewritten.push((answer.total, answer.rows));
    }
    assert_eq!(
        rewritten
            .iter()
            .map(|(total, _)| *total)
            .collect::<Vec<_>>(),
        totals
    );
    for (stage, answers) in stages.iter().enumerate() {
        assert_eq!(answers, &rewritten, "answers before step {stage}");
    }
    assert_eq!(crate::test_schema::catalog(&mut conn).await, fresh);
}

#[devkit::test]
async fn trigram_rewrite_visits_each_id_within_one_uuid_millisecond(db: devkit::pg::TestDatabase) {
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
    loop {
        use sinex_schema::online::Progress;
        match sinex_schema::online::step(&mut conn, 1).await.unwrap() {
            Progress::Done => break,
            Progress::Waiting => panic!("the fixture owns the rewrite"),
            Progress::Advanced { .. } | Progress::Finished { .. } => {}
        }
    }
    assert_eq!(flags(&mut conn).await, expected);
}
