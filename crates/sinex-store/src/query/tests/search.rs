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
async fn prefix_search_selects_matches_among_unrelated_readings(db: devkit::pg::TestDatabase) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    crate::read_models::tests::admit_search_kinds(&mut conn, Class::Open).await;
    catch_up_search_text(&mut conn).await.unwrap();
    // The prefix matches three readings among many unrelated ones.
    sqlx::query(
        "WITH events AS (
           INSERT INTO core.events(id,event_type,source,run_id,material_id,anchor,ts_quality,
             commit_seq,payload,reading_order,record_hash,class)
           SELECT uuidv7(),'command.imported','synthetic-unrelated',uuidv7(),uuidv7(),n,'unknown',
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
    assert_eq!(ask(&mut conn, "events where car*").await.total, 3);
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
        ("events | sort by when | limit 1 | where ready", vec![]),
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
async fn indexed_words_find_matches_among_compressed_unrelated_history(
    db: devkit::pg::TestDatabase,
) {
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
}

#[devkit::test]
async fn matching_compressed_search_rows_page_with_grants_and_snippets(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    sqlx::query("WITH inserted AS (
        INSERT INTO core.events(id,event_type,source,run_id,material_id,anchor,ts_quality,commit_seq,payload,reading_order,record_hash,class)
        SELECT ((CASE WHEN n<=128 THEN '01800000-0000-7000-8000-' ELSE '01810000-0000-7000-8000-' END)||lpad(to_hex(n),12,'0'))::uuid,
          'command.imported','synthetic-search-reuse',uuidv7(),uuidv7(),n,'unknown',1,'{}',1,decode(repeat('01',32),'hex'),
          CASE WHEN n<=128 AND n%4=0 THEN 'guarded' ELSE 'open' END
        FROM generate_series(1,8320)n RETURNING id,anchor
      ) INSERT INTO core.search_text(event_id,text)
        SELECT id,CASE WHEN anchor<=128 THEN 'matchingreuse common words' ELSE 'unrelated history' END FROM inserted")
        .execute(&mut conn).await.unwrap();
    sqlx::query("UPDATE core.counters SET value=greatest(value,1) WHERE name='commit_seq'")
        .execute(&mut conn)
        .await
        .unwrap();
    sqlx::query("SELECT compress_chunk(chunk) FROM show_chunks('core.events') chunks(chunk)")
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
    let first = ask(&mut conn, "matchingreuse").await;
    assert_eq!(first.total, 96);
    assert_eq!(first.rows.len(), 50);
    assert!(
        first
            .rows
            .iter()
            .all(|r| r["snippet"].as_str().unwrap().contains("[matchingreuse]"))
    );
    let second = ask_page(
        &mut conn,
        "matchingreuse",
        Page {
            size: Some(50),
            cursor: first.page.cursor.clone(),
        },
    )
    .await;
    assert_eq!(second.total, 96);
    assert_eq!(second.rows.len(), 46);
    assert!(second.page.cursor.is_none());
    let ids: std::collections::BTreeSet<_> = first
        .rows
        .iter()
        .chain(&second.rows)
        .map(|r| r["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids.len(), 96);
    set_grants(&mut conn, "guarded").await.unwrap();
    assert_eq!(ask(&mut conn, "matchingreuse").await.total, 128);
    set_grants(&mut conn, "").await.unwrap();
}
