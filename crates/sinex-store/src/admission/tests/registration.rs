//! Registering an input reads its current revision through an index and
//! leaves a known input's row as it is.
use super::*;

fn file(kind: &str) -> InputRef {
    InputRef {
        kind: kind.into(),
        locator: "/synthetic/input".into(),
        host: "synthetic-host".into(),
    }
}

async fn row_version(conn: &mut PgConnection, id: uuid::Uuid) -> (String, String) {
    sqlx::query_as("SELECT xmin::text, kind FROM raw.inputs WHERE id = $1")
        .bind(id)
        .fetch_one(conn)
        .await
        .unwrap()
}

#[devkit::test]
async fn unchanged_input_registration_writes_no_row(db: devkit::pg::TestDatabase) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let changed = ObservationFacts {
        changed: true,
        ..Default::default()
    };
    let (first, _) = observed(
        &mut conn,
        "b",
        &file("file"),
        &changed,
        RevisionChange::Appended,
    )
    .await
    .unwrap();
    let before = row_version(&mut conn, first).await;

    let unchanged = ObservationFacts::default();
    let (again, _) = observed(
        &mut conn,
        "b",
        &file("file"),
        &unchanged,
        RevisionChange::Same,
    )
    .await
    .unwrap();
    assert_eq!(again, first);
    assert_eq!(row_version(&mut conn, first).await, before);

    let (moved, _) = observed(
        &mut conn,
        "b",
        &file("directory"),
        &changed,
        RevisionChange::Same,
    )
    .await
    .unwrap();
    assert_eq!(moved, first);
    assert_eq!(row_version(&mut conn, first).await.1, "directory");
}
