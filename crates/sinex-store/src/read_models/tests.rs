use super::*;
use sqlx::Connection;

/// Bring the search read model up to the newest commit without the daemon's
/// reader, page by page with its position.
pub(crate) async fn catch_up_search_text(conn: &mut PgConnection) -> Result<i64> {
    let mut position = search_position(conn).await?;
    loop {
        let page = crate::commit_log::after(conn, position, 2_000, i64::MAX).await?;
        let Some(up_to) = page.up_to else {
            return Ok(position);
        };
        let mut tx = sqlx::Connection::begin(&mut *conn).await?;
        apply_search_text(&mut tx, &page.events).await?;
        set_search_position(&mut tx, up_to).await?;
        tx.commit().await?;
        position = up_to;
    }
}

#[devkit::test]
async fn indexes_committed_payload_text_once_and_resumes(db: devkit::pg::TestDatabase) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let ids = crate::query::tests::admit_commands(
        &mut conn,
        &[("Cargo test --workspace", "/src/tide"), ("ls", "/")],
    )
    .await;
    let up_to = crate::query::current_seq(&mut conn).await.unwrap();
    assert_eq!(catch_up_search_text(&mut conn).await.unwrap(), up_to);
    let text = sqlx::query_scalar!(
        "SELECT text FROM core.search_text WHERE event_id = $1",
        ids[0].0
    )
    .fetch_one(&mut conn)
    .await
    .unwrap();
    assert!(text.contains("cargo test workspace"), "{text}");
    // Running again changes nothing.
    assert_eq!(catch_up_search_text(&mut conn).await.unwrap(), up_to);
    let n = sqlx::query_scalar!(r#"SELECT count(*) AS "n!" FROM core.search_text"#)
        .fetch_one(&mut conn)
        .await
        .unwrap();
    // Two commands, each an observation and a fused command.
    assert_eq!(n, 4);
}

#[devkit::test]
async fn search_updates_can_run_during_vacuum(db: devkit::pg::TestDatabase) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    crate::query::tests::admit_commands(&mut conn, &[("cargo check", "/src/tide")]).await;
    let page = crate::commit_log::after(&mut conn, 0, 2_000, i64::MAX)
        .await
        .unwrap();
    let mut maintenance = PgConnection::connect(&db.url(None)).await.unwrap();
    let mut vacuum_lock = maintenance.begin().await.unwrap();
    // Plain VACUUM holds this relation lock. Hold it deterministically
    // while the real search writer attempts to index a committed event.
    sqlx::query("LOCK TABLE core.search_text IN SHARE UPDATE EXCLUSIVE MODE")
        .execute(&mut *vacuum_lock)
        .await
        .unwrap();
    let mut tx = sqlx::Connection::begin(&mut *conn).await.unwrap();
    sqlx::query("SET LOCAL lock_timeout = '1s'")
        .execute(&mut *tx)
        .await
        .unwrap();
    apply_search_text(&mut tx, &page.events).await.unwrap();
    tx.commit().await.unwrap();
    vacuum_lock.rollback().await.unwrap();
}

/// Same text under kinds with and without trigram indexing.
pub(crate) async fn admit_search_kinds(
    conn: &mut crate::AdmissionContext,
    class: sinex_model::Class,
) {
    use serde_json::json;
    use sinex_model::{
        Batch, BatchKind, Event, MaterialId, Origin, ReadingOrder, RecordLocation, RecordOutcome,
        RunId, event_types,
    };
    let message = "Straße CAFÉ cafe\u{301} cargo test workspace";
    let payloads = [
        (
            "command.imported",
            json!({"command": message, "hostname": "h", "n": 0}),
        ),
        ("journal.entry", json!({"cursor": "c1", "message": message})),
        (
            "chat.message",
            json!({"buffer": "test", "sent_at": "2026-09-14T08:00:00Z",
            "kind": "message", "text": message}),
        ),
        (
            "lake.observed",
            json!({"observed_at": "2026-09-14T08:00:00Z", "lane": "test",
            "host": "h", "seq": 1, "payload": {"message": message}}),
        ),
    ];
    let events: Vec<Event> = payloads
        .into_iter()
        .map(|(kind, payload)| {
            (event_types::find(kind).unwrap().build)(
                &payload,
                "test.search",
                Origin::Record {
                    location: RecordLocation {
                        material: MaterialId::new(),
                        record_offset: 0,
                    },
                    output_index: 0,
                },
                ReadingOrder(1),
                [1; 32],
                class,
            )
            .unwrap()
        })
        .collect();
    let mut tx = conn.begin().await.unwrap();
    tx.admit(
        &Batch {
            run: RunId::new(),
            kind: BatchKind::Api {
                actor: "test".into(),
            },
            records: vec![RecordOutcome::Events {
                location: None,
                events,
            }],
            archive: vec![],
        },
        &crate::NoDerivations,
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
}

#[devkit::test]
async fn writes_kind_policy_and_skips_unchanged_rows(db: devkit::pg::TestDatabase) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    admit_search_kinds(&mut conn, sinex_model::Class::Open).await;
    catch_up_search_text(&mut conn).await.unwrap();
    let before: Vec<(String, String, String, bool)> = sqlx::query_as(
        "SELECT e.event_type, s.text, s.ctid::text, s.trigram FROM core.search_text s JOIN core.events e ON e.id=s.event_id ORDER BY e.event_type")
        .fetch_all(&mut conn).await.unwrap();
    assert_eq!(
        before
            .iter()
            .map(|r| (r.0.as_str(), r.3))
            .collect::<Vec<_>>(),
        [
            ("chat.message", true),
            ("command.imported", true),
            ("journal.entry", false)
        ]
    );
    assert!(
        before
            .iter()
            .all(|r| r.1.contains("strasse café café cargo test workspace"))
    );
    let page = crate::commit_log::after(&mut conn, 0, 10, i64::MAX)
        .await
        .unwrap();
    let mut tx = conn.begin().await.unwrap();
    apply_search_text(&mut tx, &page.events).await.unwrap();
    tx.commit().await.unwrap();
    let after: Vec<(String, String, String, bool)> = sqlx::query_as(
        "SELECT e.event_type, s.text, s.ctid::text, s.trigram FROM core.search_text s JOIN core.events e ON e.id=s.event_id ORDER BY e.event_type")
        .fetch_all(&mut conn).await.unwrap();
    assert_eq!(
        after, before,
        "recovery must leave unchanged heap rows in place"
    );
    // A policy change is meaningful even when normalized text is identical.
    sqlx::query("UPDATE core.search_text SET trigram = true")
        .execute(&mut conn)
        .await
        .unwrap();
    let mut tx = conn.begin().await.unwrap();
    apply_search_text(&mut tx, &page.events).await.unwrap();
    tx.commit().await.unwrap();
    let excluded: i64 =
        sqlx::query_scalar("SELECT count(*) FROM core.search_text WHERE NOT trigram")
            .fetch_one(&mut conn)
            .await
            .unwrap();
    assert_eq!(excluded, 1);
}

#[devkit::test]
async fn older_versions_follow_the_current_declaration(db: devkit::pg::TestDatabase) {
    use sinex_model::{DbCheck, KindDeclaration};

    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    admit_search_kinds(&mut conn, sinex_model::Class::Open).await;
    let mut historical = KindDeclaration::historical("lake.observed".into());
    historical.search = Search::WordsAndTrigram;
    sqlx::query(
        "INSERT INTO registry.event_types (name, version, declaring_producer, normalization,
          key_rule, time_rule, field_hints, removed_rule, keep_known, owned_blobs, search, definition_hash)
         VALUES ($1, 1, 'sinex', 'json', 'null', 'null', '[]', 'null', '{}', '[]', $2, $3)",
    )
    .bind(&historical.kind.name)
    .bind(historical.search.as_db())
    .bind(historical.definition_hash())
    .execute(&mut conn)
    .await
    .unwrap();
    // This fixture represents a reading already stored by the earlier parser.
    let mut tx = conn.begin().await.unwrap();
    sqlx::raw_sql(
        "SET LOCAL session_replication_role = replica;
         UPDATE core.events SET kind_version = 1, validation = 'unvalidated'
         WHERE event_type = 'lake.observed'",
    )
    .execute(&mut *tx)
    .await
    .unwrap();
    tx.commit().await.unwrap();
    let page = crate::commit_log::after(&mut conn, 0, 10, i64::MAX)
        .await
        .unwrap();
    let old = page
        .events
        .iter()
        .find(|event| event.row.event_type == "lake.observed")
        .unwrap();
    assert_eq!(old.search, Search::WordsAndTrigram);
    catch_up_search_text(&mut conn).await.unwrap();
    let indexed: Vec<String> = sqlx::query_scalar(
        "SELECT e.event_type FROM core.search_text s JOIN core.events e ON e.id=s.event_id ORDER BY 1",
    )
    .fetch_all(&mut conn)
    .await
    .unwrap();
    assert_eq!(
        indexed,
        ["chat.message", "command.imported", "journal.entry"]
    );
}

#[devkit::test]
async fn sealed_and_empty_text_still_advance_search_position(db: devkit::pg::TestDatabase) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let mut declaration = sinex_model::KindDeclaration::schemaless(
        sinex_model::KindRef {
            name: "synthetic.search".into(),
            version: 1,
        },
        "synthetic".into(),
    );
    declaration.search = sinex_model::kinds::Search::Words;
    sinex_schema::kinds::register(&mut conn, "synthetic", std::slice::from_ref(&declaration))
        .await
        .unwrap();
    let key = sinex_sealing::new_key().unwrap();
    let events: Vec<sinex_model::Event> = [
        (
            serde_json::json!({"text":"private"}),
            sinex_model::Class::Sealed,
        ),
        (serde_json::json!({"count":1}), sinex_model::Class::Open),
        (
            serde_json::json!({"text":"visible"}),
            sinex_model::Class::Open,
        ),
    ]
    .into_iter()
    .map(|(payload, class)| {
        let mut event = sinex_model::Event::submitted(
            declaration.kind.clone(),
            payload,
            "synthetic".into(),
            sinex_model::Origin::Record {
                location: sinex_model::RecordLocation {
                    material: sinex_model::MaterialId::new(),
                    record_offset: 0,
                },
                output_index: 0,
            },
            sinex_model::ReadingOrder(1),
            [1; 32],
            class,
        );
        if class == sinex_model::Class::Sealed {
            sinex_sealing::seal_event(&mut event, &key).unwrap();
        }
        event
    })
    .collect();
    let sealed_id = events
        .iter()
        .find(|event| event.class == sinex_model::Class::Sealed)
        .unwrap()
        .id;
    let mut tx = conn.begin().await.unwrap();
    tx.admit(
        &sinex_model::Batch {
            run: sinex_model::RunId::new(),
            kind: sinex_model::BatchKind::Api {
                actor: "synthetic".into(),
            },
            records: vec![sinex_model::RecordOutcome::Events {
                location: None,
                events,
            }],
            archive: vec![],
        },
        &crate::NoDerivations,
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    let sealed_payload: serde_json::Value =
        sqlx::query_scalar("SELECT payload FROM core.events WHERE id=$1")
            .bind(sealed_id.0)
            .fetch_one(&mut conn)
            .await
            .unwrap();
    assert_eq!(
        sinex_sealing::unseal_payload(&sealed_payload, &key.key).unwrap(),
        serde_json::json!({"text":"private"})
    );
    let wrong_key = sinex_sealing::new_key().unwrap();
    assert!(sinex_sealing::unseal_payload(&sealed_payload, &wrong_key.key).is_err());
    // Rebuilding the read model starts empty, while stored policy outputs
    // keep the sealed class and payload with no searchable text.
    sqlx::query("DELETE FROM core.search_text")
        .execute(&mut conn)
        .await
        .unwrap();
    let page = crate::commit_log::after(&mut conn, 0, 10, i64::MAX)
        .await
        .unwrap();
    let mut tx = conn.begin().await.unwrap();
    apply_search_text(&mut tx, &page.events).await.unwrap();
    set_search_position(&mut tx, page.up_to.unwrap())
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM core.search_text")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(n, 1);
    assert_eq!(
        search_position(&mut conn).await.unwrap(),
        page.up_to.unwrap()
    );
}

/// A reading whose complete word vector would exceed PostgreSQL's limit can
/// still be found by its last word. Several matching pieces give one result.
#[devkit::test]
async fn long_text_is_indexed_in_pieces(db: devkit::pg::TestDatabase) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let mut declaration = sinex_model::KindDeclaration::schemaless(
        sinex_model::KindRef {
            name: "synthetic.search".into(),
            version: 1,
        },
        "synthetic".into(),
    );
    declaration.search = sinex_model::kinds::Search::Words;
    sinex_schema::kinds::register(&mut conn, "synthetic", std::slice::from_ref(&declaration))
        .await
        .unwrap();
    // About 40,000 distinct words: its full-text vector is over 1 MB.
    let oversized: String = (0..40_000)
        .map(|n| format!("w{n:05}x{}", uuid::Uuid::from_u128(n as u128).simple()))
        .collect::<Vec<_>>()
        .join(" ");
    let oversized = format!("needle {oversized} needle endtag");
    let events: Vec<sinex_model::Event> = [oversized, "after the large text".to_owned()]
        .into_iter()
        .enumerate()
        .map(|(n, text)| {
            sinex_model::Event::submitted(
                declaration.kind.clone(),
                serde_json::json!({"text": text}),
                "synthetic".into(),
                sinex_model::Origin::Record {
                    location: sinex_model::RecordLocation {
                        material: sinex_model::MaterialId::new(),
                        record_offset: n as u64,
                    },
                    output_index: 0,
                },
                sinex_model::ReadingOrder(1),
                [n as u8 + 1; 32],
                sinex_model::Class::Open,
            )
        })
        .collect();
    let (large, small) = (events[0].id, events[1].id);
    let mut tx = conn.begin().await.unwrap();
    tx.admit(
        &sinex_model::Batch {
            run: sinex_model::RunId::new(),
            kind: sinex_model::BatchKind::Api {
                actor: "synthetic".into(),
            },
            records: vec![sinex_model::RecordOutcome::Events {
                location: None,
                events,
            }],
            archive: vec![],
        },
        &crate::NoDerivations,
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    let up_to = crate::query::current_seq(&mut conn).await.unwrap();
    assert_eq!(catch_up_search_text(&mut conn).await.unwrap(), up_to);
    let context = sinex_query::Context::utc("2026-09-15T00:00:00Z".parse().unwrap());
    for (query, expected) in [
        ("events where endtag", large),
        ("events where needle", large),
        ("events where needle AND endtag", large),
        ("events where aft*", small),
    ] {
        let answer = crate::query::run(&mut conn, query, None, &context, &Default::default())
            .await
            .unwrap();
        assert_eq!(answer.total, 1, "{query}");
        assert_eq!(answer.rows[0]["id"], expected.to_string());
    }
}

async fn failed_search_batch_keeps_its_page(db: devkit::pg::TestDatabase, sqlstate: &str) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    admit_search_kinds(&mut conn, sinex_model::Class::Open).await;
    let before = search_position(&mut conn).await.unwrap();
    let head = crate::query::current_seq(&mut conn).await.unwrap();
    // A bulk-only failure would disappear if the consumer incorrectly split
    // this page into single-row writes.
    sqlx::raw_sql(&format!(
        "CREATE FUNCTION public.fail_search_batch() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
           IF (SELECT count(*) FROM proposed) > 1 THEN
             RAISE EXCEPTION 'synthetic search batch failure' USING ERRCODE = '{sqlstate}';
           END IF;
           RETURN NULL;
         END $$;
         CREATE TRIGGER fail_search_batch AFTER INSERT ON core.search_text
           REFERENCING NEW TABLE AS proposed FOR EACH STATEMENT
           EXECUTE FUNCTION public.fail_search_batch()"
    ))
    .execute(&mut conn)
    .await
    .unwrap();
    let error = catch_up_search_text(&mut conn).await.unwrap_err();
    let crate::Error::Database(sqlx::Error::Database(error)) = error else {
        panic!("expected the database batch failure");
    };
    assert_eq!(error.code().as_deref(), Some(sqlstate));
    assert_eq!(search_position(&mut conn).await.unwrap(), before);
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM core.search_text")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(count, 0, "the whole search page must roll back");
    sqlx::raw_sql(
        "DROP TRIGGER fail_search_batch ON core.search_text;
                   DROP FUNCTION public.fail_search_batch()",
    )
    .execute(&mut conn)
    .await
    .unwrap();
    assert_eq!(catch_up_search_text(&mut conn).await.unwrap(), head);
    let indexed: i64 = sqlx::query_scalar("SELECT count(*) FROM core.search_text")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(indexed, 3, "normal retry indexes every offered row");
}

#[devkit::test]
async fn cancelled_search_batch_retries_the_whole_page(db: devkit::pg::TestDatabase) {
    failed_search_batch_keeps_its_page(db, "57014").await;
}

#[devkit::test]
async fn serialization_failure_retries_the_whole_search_page(db: devkit::pg::TestDatabase) {
    failed_search_batch_keeps_its_page(db, "40001").await;
}
