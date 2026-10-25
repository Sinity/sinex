use super::*;

fn source(name: &str, version: i32) -> Source {
    Source {
        name: name.into(),
        driver: "append-file".into(),
        version,
        event_types: vec!["command.imported".into()],
        purpose: "shell recall".into(),
    }
}

#[devkit::test]
async fn the_source_rows_follow_the_catalog(db: devkit::pg::TestDatabase) {
    let mut conn = <sqlx::PgConnection as sqlx::Connection>::connect(&db.url(None))
        .await
        .unwrap();
    write_sources(&mut conn, &[source("a.one", 1), source("a.two", 1)])
        .await
        .unwrap();
    write_sources(&mut conn, &[source("a.two", 2)])
        .await
        .unwrap();
    let rows = sqlx::query!("SELECT name, version, driver FROM registry.sources ORDER BY name")
        .fetch_all(&mut conn)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        (
            rows[0].name.as_str(),
            rows[0].version,
            rows[0].driver.as_str()
        ),
        ("a.two", 2, "append-file")
    );
}
