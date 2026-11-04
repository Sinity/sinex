use super::*;
use std::time::Duration;

fn source(name: &str, version: i32) -> Source {
    Source {
        name: name.into(),
        shapes: vec!["append-file".into()],
        version,
        event_types: vec!["command.imported".into()],
        purpose: "shell recall".into(),
        expect: sinex_model::Expectation::continuous_days(2),
        expires_after: None,
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
    let rows = sqlx::query!("SELECT name, version, shapes FROM registry.sources ORDER BY name")
        .fetch_all(&mut conn)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        (
            rows[0].name.as_str(),
            rows[0].version,
            rows[0].shapes.as_slice()
        ),
        ("a.two", 2, &["append-file".to_owned()][..])
    );
}

/// law: sx.decl.expectations-as-data
#[devkit::test]
async fn expectations_as_data_store_each_declaration_exactly(db: devkit::pg::TestDatabase) {
    let mut conn = <sqlx::PgConnection as sqlx::Connection>::connect(&db.url(None))
        .await
        .unwrap();
    let declared = [
        Source {
            expires_after: Some(Duration::from_secs(90 * 86_400)),
            ..source("a.continuous", 1)
        },
        Source {
            expect: sinex_model::Expectation::cadence_hours(6),
            ..source("a.cadence", 1)
        },
        Source {
            expect: sinex_model::Expectation::Export,
            ..source("a.export", 1)
        },
        Source {
            shapes: vec![],
            expect: sinex_model::Expectation::OnDemand,
            ..source("a.request", 1)
        },
    ];
    write_sources(&mut conn, &declared).await.unwrap();
    let rows: Vec<(
        String,
        String,
        Option<f64>,
        Option<f64>,
        Option<f64>,
        Vec<String>,
    )> = sqlx::query_as(
        "SELECT name, expectation, extract(epoch FROM silent_after)::float8,
               extract(epoch FROM every)::float8, extract(epoch FROM expires_after)::float8, shapes
             FROM registry.sources ORDER BY name",
    )
    .fetch_all(&mut conn)
    .await
    .unwrap();
    let request: &[String] = &[];
    assert_eq!(
        rows,
        vec![
            (
                "a.cadence".into(),
                "cadence".into(),
                None,
                Some(21_600.0),
                None,
                vec!["append-file".to_owned()]
            ),
            (
                "a.continuous".into(),
                "continuous".into(),
                Some(172_800.0),
                None,
                Some(7_776_000.0),
                vec!["append-file".to_owned()]
            ),
            (
                "a.export".into(),
                "export".into(),
                None,
                None,
                None,
                vec!["append-file".to_owned()]
            ),
            (
                "a.request".into(),
                "on_demand".into(),
                None,
                None,
                None,
                request.to_vec()
            ),
        ]
    );
    // The table refuses an expectation without its deadline.
    assert!(
        sqlx::query("UPDATE registry.sources SET silent_after = NULL WHERE name = 'a.continuous'")
            .execute(&mut conn)
            .await
            .is_err()
    );
}
