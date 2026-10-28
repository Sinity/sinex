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
    let previous_version = 4;
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
