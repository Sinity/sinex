use super::*;

/// Bring the search read model up to the newest commit without the daemon's
/// reader, page by page with its position.
pub(crate) async fn catch_up_search_text(conn: &mut PgConnection) -> Result<i64> {
    crate::privacy::projection::ensure_current(conn).await?;
    let (mut position, _) = crate::commit_log::consumer_position(conn, SEARCH_TEXT).await?;
    loop {
        let page = crate::commit_log::after(conn, position, 2_000, i64::MAX).await?;
        let Some(up_to) = page.up_to else {
            return Ok(position);
        };
        let mut tx = sqlx::Connection::begin(&mut *conn).await?;
        apply_search_text(&mut tx, &page.events).await?;
        crate::commit_log::set_consumer_position(&mut tx, SEARCH_TEXT, up_to, 0).await?;
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
                        anchor: 0,
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
        "SELECT e.event_type, s.text, s.ctid::text, s.trigram_indexed FROM core.search_text s JOIN core.events e ON e.id=s.event_id ORDER BY e.event_type")
        .fetch_all(&mut conn).await.unwrap();
    assert_eq!(
        before
            .iter()
            .map(|r| (r.0.as_str(), r.3))
            .collect::<Vec<_>>(),
        [
            ("command.imported", true),
            ("journal.entry", false),
            ("lake.observed", false)
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
        "SELECT e.event_type, s.text, s.ctid::text, s.trigram_indexed FROM core.search_text s JOIN core.events e ON e.id=s.event_id ORDER BY e.event_type")
        .fetch_all(&mut conn).await.unwrap();
    assert_eq!(
        after, before,
        "recovery must leave unchanged heap rows in place"
    );
    // A policy change is meaningful even when normalized text is identical.
    sqlx::query("UPDATE core.search_text SET trigram_indexed=true")
        .execute(&mut conn)
        .await
        .unwrap();
    let mut tx = conn.begin().await.unwrap();
    apply_search_text(&mut tx, &page.events).await.unwrap();
    tx.commit().await.unwrap();
    let excluded: i64 =
        sqlx::query_scalar("SELECT count(*) FROM core.search_text WHERE NOT trigram_indexed")
            .fetch_one(&mut conn)
            .await
            .unwrap();
    assert_eq!(excluded, 2);
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
                    anchor: 0,
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
    // Rebuilding the read model starts empty, while canonical policy outputs
    // retain the actual sealed class and payload with no searchable text.
    sqlx::query("DELETE FROM core.search_text")
        .execute(&mut conn)
        .await
        .unwrap();
    let page = crate::commit_log::after(&mut conn, 0, 10, i64::MAX)
        .await
        .unwrap();
    let mut tx = conn.begin().await.unwrap();
    apply_search_text(&mut tx, &page.events).await.unwrap();
    crate::commit_log::set_consumer_position(&mut tx, SEARCH_TEXT, page.up_to.unwrap(), 0)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM core.search_text")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(n, 1);
    assert_eq!(
        crate::commit_log::consumer_position(&mut conn, SEARCH_TEXT)
            .await
            .unwrap()
            .0,
        page.up_to.unwrap()
    );
}
