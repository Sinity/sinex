use super::*;
use crate::online::{Progress, step};

/// A version-two store with thirty events whose kinds cycle through journal,
/// lake and command (the first two outside the trigram index), each with a
/// search row in insertion order, plus one search row without an event.
async fn upgraded_with_search_rows(conn: &mut PgConnection) {
    search_version_two(conn).await;
    conn.execute(
        "INSERT INTO core.events (id,event_type,source,run_id,material_id,anchor,ts_quality,commit_seq,payload,reading_order,record_hash,class)
         SELECT uuidv7(), (ARRAY['command.imported','journal.entry','lake.observed'])[1 + n % 3], 'test', uuidv7(), uuidv7(), 0, 'unknown', n, '{}', 1, decode(repeat('01',32),'hex'), 'open'
         FROM generate_series(1, 30) n ORDER BY n;
         INSERT INTO core.search_text (event_id,text) SELECT id, 'cargo test ' || commit_seq FROM core.events ORDER BY commit_seq;
         INSERT INTO core.search_text (event_id,text) VALUES (uuidv7(),'orphan stays searchable')",
    )
    .await
    .unwrap();
    assert_eq!(
        apply(conn).await.unwrap(),
        Applied::Upgraded {
            from: 2,
            to: VERSION
        }
    );
}

/// Each event kind with its rows' flags, and the event-less row's flag.
async fn flags(conn: &mut PgConnection) -> (Vec<(String, bool, i64)>, bool) {
    let kinds = sqlx::query_as(
        "SELECT e.event_type, s.trigram_indexed, count(*) FROM core.search_text s JOIN core.events e ON e.id=s.event_id GROUP BY 1,2 ORDER BY 1,2",
    )
    .fetch_all(&mut *conn)
    .await
    .unwrap();
    let orphan = sqlx::query_scalar(
        "SELECT trigram_indexed FROM core.search_text WHERE text='orphan stays searchable'",
    )
    .fetch_one(&mut *conn)
    .await
    .unwrap();
    (kinds, orphan)
}

/// The search query's two branches, as the query compiler writes them.
async fn matches(conn: &mut PgConnection, pattern: &str) -> Vec<String> {
    sqlx::query_scalar(
        "SELECT event_id::text FROM (
           SELECT s.event_id FROM core.search_text s WHERE s.trigram_indexed AND s.text ~ $1
           UNION ALL
           SELECT s.event_id FROM core.search_text s WHERE NOT s.trigram_indexed AND s.text ~ $1) m
         ORDER BY 1",
    )
    .bind(pattern)
    .fetch_all(&mut *conn)
    .await
    .unwrap()
}

#[devkit::test]
async fn online_rewrite_resumes_from_its_mark_and_converges(db: devkit::pg::TestDatabase) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let fresh = catalog(&mut conn).await.unwrap();
    upgraded_with_search_rows(&mut conn).await;
    let patterns = ["cargo", "test 1", "orphan", "absent"];
    let mut answers = Vec::new();
    for pattern in patterns {
        answers.push(matches(&mut conn, pattern).await);
    }
    assert_eq!(answers[0].len(), 30);

    // Rows follow event order: 1, 2 are outside the trigram index, 3 is in it.
    assert_eq!(
        step(&mut conn, 7).await.unwrap(),
        Progress::Advanced { rewritten: 5 }
    );
    assert_eq!(
        step(&mut conn, 7).await.unwrap(),
        Progress::Advanced { rewritten: 5 }
    );
    for (pattern, answer) in patterns.iter().zip(&answers) {
        assert_eq!(&matches(&mut conn, pattern).await, answer, "{pattern}");
    }

    // A restart resumes after the fourteenth row.
    drop(conn);
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let (mark, fourteenth): (String, String) = sqlx::query_as(
        "SELECT (SELECT rewritten_through::text FROM sinex_schema.search_trigram_rewrite),
                (SELECT event_id::text FROM core.search_text ORDER BY event_id OFFSET 13 LIMIT 1)",
    )
    .fetch_one(&mut conn)
    .await
    .unwrap();
    assert_eq!(mark, fourteenth);
    let (through_mark, after_mark): (i64, i64) = sqlx::query_as(
        "SELECT count(*) FILTER (WHERE event_id <= $1::uuid), count(*) FILTER (WHERE event_id > $1::uuid)
         FROM core.search_text WHERE NOT trigram_indexed",
    )
    .bind(&mark)
    .fetch_one(&mut conn)
    .await
    .unwrap();
    assert_eq!((through_mark, after_mark), (10, 0));
    // Rows fifteen to twenty-one: 15, 18 and 21 are commands.
    assert_eq!(
        step(&mut conn, 7).await.unwrap(),
        Progress::Advanced { rewritten: 4 }
    );

    crate::online::finish(&mut conn).await.unwrap();
    assert_eq!(
        flags(&mut conn).await,
        (
            vec![
                ("command.imported".into(), true, 10),
                ("journal.entry".into(), false, 10),
                ("lake.observed".into(), false, 10),
            ],
            true
        )
    );
    for (pattern, answer) in patterns.iter().zip(&answers) {
        assert_eq!(&matches(&mut conn, pattern).await, answer, "{pattern}");
    }
    assert_eq!(catalog(&mut conn).await.unwrap(), fresh);
    assert_eq!(step(&mut conn, 7).await.unwrap(), Progress::Done);
}

#[devkit::test]
async fn online_index_rebuild_drops_an_interrupted_build_and_starts_again(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let fresh = catalog(&mut conn).await.unwrap();
    upgraded_with_search_rows(&mut conn).await;
    conn.execute("INSERT INTO core.search_text (event_id,text) VALUES (uuidv7(),'cargo test 1')")
        .await
        .unwrap();
    // A concurrent build that fails leaves an invalid index under the name.
    assert!(
        sqlx::raw_sql(
            "CREATE UNIQUE INDEX CONCURRENTLY search_text_trigram_partial ON core.search_text (text)"
        )
        .execute(&mut conn)
        .await
        .is_err()
    );
    let valid: bool = sqlx::query_scalar(
        "SELECT indisvalid FROM pg_index WHERE indexrelid='core.search_text_trigram_partial'::regclass",
    )
    .fetch_one(&mut conn)
    .await
    .unwrap();
    assert!(!valid);
    conn.execute("DELETE FROM core.search_text WHERE text='cargo test 1' AND event_id NOT IN (SELECT id FROM core.events)")
        .await
        .unwrap();
    crate::online::finish(&mut conn).await.unwrap();
    assert_eq!(catalog(&mut conn).await.unwrap(), fresh);
}

#[devkit::test]
async fn online_index_rebuild_keeps_a_finished_build_and_completes_the_swap(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let fresh = catalog(&mut conn).await.unwrap();
    upgraded_with_search_rows(&mut conn).await;
    // Interrupted after the build and after the old index's drop.
    conn.execute("CREATE INDEX search_text_trigram_partial ON core.search_text USING gin (text gin_trgm_ops) WHERE trigram_indexed; DROP INDEX core.search_text_trigram")
        .await
        .unwrap();
    crate::online::finish(&mut conn).await.unwrap();
    assert_eq!(catalog(&mut conn).await.unwrap(), fresh);
    assert_eq!(
        flags(&mut conn).await.0,
        vec![
            ("command.imported".into(), true, 10),
            ("journal.entry".into(), false, 10),
            ("lake.observed".into(), false, 10),
        ]
    );
}
