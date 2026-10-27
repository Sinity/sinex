use super::*;

#[devkit::test]
async fn every_search_kind_keeps_words_phrases_prefixes_text_and_snippets(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None)).await.unwrap();
    crate::read_models::tests::admit_search_kinds(&mut conn, Class::Open).await;
    catch_up_search_text(&mut conn).await.unwrap();
    for text in [
        "events where STRASSE",
        "events where CAFÉ",
        "events where cafe\u{301}",
        "events where cargo test",
        "events where \"cargo test\"",
        "events where car*",
        "events where not absent*",
    ] {
        let answer = ask(&mut conn, text).await;
        assert_eq!(answer.total, 3, "{text}: {:?}", answer.rows);
    }
    for text in [
        "events where absent*",
        "events where \"test cargo\"",
        "events where not car*",
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
    let mut conn = crate::AdmissionContext::connect(&db.url(None)).await.unwrap();
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
    let mut conn = crate::AdmissionContext::connect(&db.url(None)).await.unwrap();
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
