use super::*;
use sinex_model::event_types::library::{Assignment, Level, Target};

#[devkit::test]
async fn a_populated_library_upgrade_keeps_every_record_until_id_continuity_is_decided(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let assignment = Assignment {
        assignment: "assignment".into(),
        target: Target {
            item: "existing-item".into(),
            ..Default::default()
        },
        field: "tag".into(),
        value: Some("synthetic".into()),
        level: Level::Override,
        source_ref: None,
        by: "operator".into(),
        at: Timestamp::from_unix_secs(1).unwrap(),
        withdrawn_at: None,
    };
    let event = Event::build(
        &assignment,
        "library.request",
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
    admit_one(&mut conn, &batch(vec![event.clone()])).await;
    let previous_version = sinex_schema::VERSION - 1;
    sqlx::query("UPDATE sinex_schema.applied SET version = $1")
        .bind(previous_version)
        .execute(&mut conn)
        .await
        .unwrap();
    let before = sinex_schema::catalog(&mut conn).await.unwrap();
    let error = sinex_schema::apply(&mut conn).await.unwrap_err();
    assert!(
        error.to_string().contains("request-created item IDs"),
        "{error}"
    );
    assert_eq!(sinex_schema::catalog(&mut conn).await.unwrap(), before);
    let payload: serde_json::Value =
        sqlx::query_scalar("SELECT payload FROM core.events WHERE id = $1")
            .bind(event.id.0)
            .fetch_one(&mut conn)
            .await
            .unwrap();
    assert_eq!(payload, event.payload);
    let version: i32 = sqlx::query_scalar("SELECT max(version) FROM sinex_schema.applied")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(version, previous_version);
}

#[devkit::test]
async fn source_kind_upgrade_adds_import_views_without_changing_stored_records(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let event = Event::build(
        &sinex_model::event_types::media::NoteDocument {
            vault: "synthetic".into(), path: "existing.md".into(), text: Some("stored text".into()),
            size: 11, modified_at: None, removed_at: None,
            title: None, note_id: None, tags: None, wikilinks: None, front_matter: None,
        },
        "knowledge.notes-vault",
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
    admit_one(&mut conn, &batch(vec![event.clone()])).await;
    for name in ["document.captured", "transcript.cue", "ocr.text"] {
        sqlx::query(&format!("DROP VIEW event_types.{}", name.replace('.', "_")))
            .execute(&mut conn)
            .await
            .unwrap();
        sqlx::query("DELETE FROM registry.event_types WHERE name = $1")
            .bind(name)
            .execute(&mut conn)
            .await
            .unwrap();
    }
    sqlx::query("UPDATE sinex_schema.applied SET version = $1")
        .bind(sinex_schema::VERSION - 1)
        .execute(&mut conn)
        .await
        .unwrap();
    sinex_schema::apply(&mut conn).await.unwrap();
    let payload: serde_json::Value =
        sqlx::query_scalar("SELECT payload FROM core.events WHERE id = $1")
            .bind(event.id.0)
            .fetch_one(&mut conn)
            .await
            .unwrap();
    assert_eq!(payload, event.payload);
    for name in ["document.captured", "transcript.cue", "ocr.text"] {
        let registered: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM registry.event_types WHERE name = $1)",
        )
        .bind(name)
        .fetch_one(&mut conn)
        .await
        .unwrap();
        assert!(registered, "{name}");
        let view: Option<String> = sqlx::query_scalar("SELECT to_regclass($1)::text")
            .bind(format!("event_types.{}", name.replace('.', "_")))
            .fetch_one(&mut conn)
            .await
            .unwrap();
        assert!(view.is_some(), "{name}");
    }
}
