use super::*;

async fn search_page(conn: &mut sinex_store::AdmissionContext) -> commit_log::Page {
    let mut declaration = sinex_model::KindDeclaration::schemaless(
        sinex_model::KindRef {
            name: "synthetic.search.memory".into(),
            version: 1,
        },
        "synthetic".into(),
    );
    declaration.search = sinex_model::kinds::Search::WordsAndTrigram;
    sinex_schema::kinds::register(conn, "synthetic", std::slice::from_ref(&declaration))
        .await
        .unwrap();
    let events = ["coastline bright meadow", "coastline calm meadow"]
        .into_iter()
        .enumerate()
        .map(|(index, text)| {
            Event::submitted(
                declaration.kind.clone(),
                json!({"text":text}),
                "synthetic".into(),
                Origin::Record {
                    location: RecordLocation {
                        material: MaterialId::new(),
                        record_offset: 0,
                    },
                    output_index: 0,
                },
                ReadingOrder(1),
                [index as u8; 32],
                Class::Open,
            )
        })
        .collect();
    let mut tx = conn.begin().await.unwrap();
    tx.admit(&api_batch(events), &sinex_store::NoDerivations)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    commit_log::after(conn, 0, PAGE, sinex_store::PAGE_BYTES)
        .await
        .unwrap()
}

async fn work_bytes(conn: &mut PgConnection) -> i64 {
    sqlx::query_scalar("SELECT pg_size_bytes(current_setting('work_mem'))")
        .fetch_one(conn)
        .await
        .unwrap()
}

async fn assert_search(conn: &mut PgConnection, up_to: i64) {
    let counts: (i64, i64, i64, i64) = sqlx::query_as(
        "SELECT count(*),
          count(*) FILTER (WHERE tsv @@ to_tsquery('simple','coast:*')),
          count(*) FILTER (WHERE tsv @@ phraseto_tsquery('simple','bright meadow')),
          (SELECT count(*) FROM core.search_trigram WHERE text LIKE '%meadow%')
         FROM core.search_text WHERE tsv @@ plainto_tsquery('simple','coastline')",
    )
    .fetch_one(&mut *conn)
    .await
    .unwrap();
    assert_eq!(counts, (2, 2, 1, 2));
    assert_eq!(
        sinex_store::read_models::search_position(conn)
            .await
            .unwrap(),
        up_to
    );
}

async fn check_budget_at_both_indexes(conn: &mut PgConnection) {
    sqlx::raw_sql(
        "CREATE FUNCTION public.check_search_memory() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
           IF pg_size_bytes(current_setting('work_mem')) <>
              pg_size_bytes(current_setting('sinex.expected_search_memory')) THEN
             RAISE EXCEPTION 'incorrect search memory budget';
           END IF;
           RETURN NULL;
         END $$;
         CREATE TRIGGER check_search_memory AFTER INSERT ON core.search_text
          FOR EACH STATEMENT EXECUTE FUNCTION public.check_search_memory();
         CREATE TRIGGER check_search_memory AFTER INSERT ON core.search_trigram
          FOR EACH STATEMENT EXECUTE FUNCTION public.check_search_memory()",
    )
    .execute(conn)
    .await
    .unwrap();
}

#[devkit::test]
async fn maintenance_budget_is_capped_only_for_search_and_restored_on_commit(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = sinex_store::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let page = search_page(&mut conn).await;
    check_budget_at_both_indexes(&mut conn).await;
    sqlx::query("SET work_mem='4MB'")
        .execute(&mut conn)
        .await
        .unwrap();
    let mut worker = ReadModelRunner(Box::new(SearchText));
    for (maintenance, expected) in [("32MB", "32MB"), ("1GB", "512MB")] {
        sqlx::query("SELECT set_config('maintenance_work_mem',$1,false),set_config('sinex.expected_search_memory',$2,false)")
            .bind(maintenance)
            .bind(expected)
            .execute(&mut conn)
            .await
            .unwrap();
        worker
            .handle(&page.events, page.up_to.unwrap(), &mut conn)
            .await
            .unwrap();
        assert_eq!(work_bytes(&mut conn).await, 4 * 1024 * 1024);
        assert_search(&mut conn, page.up_to.unwrap()).await;
        // The following non-search consumer starts with the original budget.
        let mut tx = conn.begin().await.unwrap();
        EmbeddingVectors.apply(&[], &mut tx).await.unwrap();
        assert_eq!(work_bytes(&mut tx).await, 4 * 1024 * 1024);
        tx.rollback().await.unwrap();
    }
}

async fn transient_failure_restores_memory(db: devkit::pg::TestDatabase, code: &str) {
    let mut conn = sinex_store::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let page = search_page(&mut conn).await;
    sqlx::raw_sql("SET work_mem='4MB'; SET maintenance_work_mem='1GB'; SET sinex.expected_search_memory='512MB'")
        .execute(&mut conn)
        .await
        .unwrap();
    check_budget_at_both_indexes(&mut conn).await;
    // A bulk-only transient failure would vanish under an incorrect row retry.
    sqlx::raw_sql(&format!(
        "CREATE FUNCTION public.fail_search_memory_page() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
           IF (SELECT count(*) FROM proposed) > 1 THEN
             RAISE EXCEPTION 'synthetic search page failure' USING ERRCODE='{code}';
           END IF;
           RETURN NULL;
         END $$;
         CREATE TRIGGER fail_search_memory_page AFTER INSERT ON core.search_trigram
           REFERENCING NEW TABLE AS proposed FOR EACH STATEMENT
           EXECUTE FUNCTION public.fail_search_memory_page()"
    ))
    .execute(&mut conn)
    .await
    .unwrap();
    let mut worker = ReadModelRunner(Box::new(SearchText));
    let error = worker
        .handle(&page.events, page.up_to.unwrap(), &mut conn)
        .await
        .unwrap_err();
    let Error::Store(sinex_store::Error::Database(sqlx::Error::Database(error))) = error else {
        panic!("expected the transient database failure");
    };
    assert_eq!(error.code().as_deref(), Some(code));
    assert_eq!(work_bytes(&mut conn).await, 4 * 1024 * 1024);
    assert_eq!(worker.position(&mut conn).await.unwrap(), 0);
    let counts: (i64, i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM core.search_text),
          (SELECT count(*) FROM core.search_trigram),
          (SELECT count(*) FROM core.search_refusals)",
    )
    .fetch_one(&mut conn)
    .await
    .unwrap();
    assert_eq!(counts, (0, 0, 0));
    sqlx::query("DROP TRIGGER fail_search_memory_page ON core.search_trigram")
        .execute(&mut conn)
        .await
        .unwrap();
    worker
        .handle(&page.events, page.up_to.unwrap(), &mut conn)
        .await
        .unwrap();
    assert_eq!(work_bytes(&mut conn).await, 4 * 1024 * 1024);
    assert_search(&mut conn, page.up_to.unwrap()).await;
}

#[devkit::test]
async fn cancellation_restores_memory_and_retries_same_page(db: devkit::pg::TestDatabase) {
    transient_failure_restores_memory(db, "57014").await;
}

#[devkit::test]
async fn serialization_failure_restores_memory_and_retries_same_page(db: devkit::pg::TestDatabase) {
    transient_failure_restores_memory(db, "40001").await;
}
