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
    let rows = ask(&mut conn, "cargo fields event_type,text sort event_type").await;
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
        let answer = ask(&mut conn, "events fields text").await;
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
async fn search_answers_and_position_hold_while_trigram_rewrite_runs(
    db: devkit::pg::TestDatabase,
) {
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
    loop {
        assert_eq!(current_seq(&mut conn).await.unwrap(), position);
        assert_eq!(
            crate::commit_log::consumer_position(&mut conn, crate::read_models::SEARCH_TEXT)
                .await
                .unwrap()
                .0,
            position,
            "the rewrite never moves search coverage"
        );
        let mut answers = Vec::new();
        for text in texts {
            let answer = ask(&mut conn, text).await;
            answers.push((answer.total, answer.rows));
        }
        stages.push(answers);
        match sinex_schema::online::step(&mut conn, 1).await.unwrap() {
            sinex_schema::online::Progress::Advanced { .. } => {}
            sinex_schema::online::Progress::Done => break,
            sinex_schema::online::Progress::IndexBuildRunning => {
                panic!("no other session builds the index")
            }
        }
    }
    assert_eq!(stages.len(), 4, "one search row per step, then the index");
    assert_eq!(flags(&mut conn).await, indexed_flags);
    let mut rewritten = Vec::new();
    for text in texts {
        let answer = ask(&mut conn, text).await;
        rewritten.push((answer.total, answer.rows));
    }
    assert_eq!(
        rewritten.iter().map(|(total, _)| *total).collect::<Vec<_>>(),
        totals
    );
    for (stage, answers) in stages.iter().enumerate() {
        assert_eq!(answers, &rewritten, "answers before step {stage}");
    }
    assert_eq!(crate::test_schema::catalog(&mut conn).await, fresh);
}
