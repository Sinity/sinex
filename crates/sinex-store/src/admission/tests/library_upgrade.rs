use super::*;
use sinex_model::event_types::library::{Curation, CurationKind, Target, ValueOrigin};

#[devkit::test]
async fn a_populated_library_upgrade_keeps_every_record_until_id_continuity_is_decided(
    db: devkit::pg::TestDatabase,
) {
    refused_from(db, 5, "request-created item IDs").await;
}

/// The curation record and the changed library kinds are never read from
/// records written before them.
#[devkit::test]
async fn the_curation_upgrade_refuses_a_database_holding_library_records(
    db: devkit::pg::TestDatabase,
) {
    refused_from(
        db,
        sinex_schema::version_before("library_curation")
            .expect("the library curation step is declared"),
        "library records predate the curation record",
    )
    .await;
}

async fn refused_from(db: devkit::pg::TestDatabase, previous_version: i32, message: &str) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let assignment = Curation {
        root: "record".into(),
        rev: "record".into(),
        revises: vec![],
        withdraws: vec![],
        kind: CurationKind::Tag,
        target: Some(Target {
            item: "existing-item".into(),
            ..Default::default()
        }),
        key: Some("synthetic".into()),
        value: None,
        origin: ValueOrigin::Authored,
        cites: None,
        author: "operator".into(),
        via: "api".into(),
        at: Timestamp::from_unix_secs(1).unwrap(),
        request: "record".into(),
        request_digest: String::new(),
        slots: vec![],
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
    // The fixture stands before library curation, later backup custody and
    // search refusals.
    sqlx::raw_sql(
        "DROP TABLE core.search_refusals;
         DROP FUNCTION core.backup_covers(text, bigint);
         DROP TABLE core.backup_segments, core.backup_holds;",
    )
    .execute(&mut conn)
    .await
    .unwrap();
    sqlx::query("UPDATE sinex_schema.applied SET version = $1")
        .bind(previous_version)
        .execute(&mut conn)
        .await
        .unwrap();
    let before = sinex_schema::catalog(&mut conn).await.unwrap();
    let error = sinex_schema::apply(&mut conn).await.unwrap_err();
    assert!(error.to_string().contains(message), "{error}");
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
