use sqlx::{Executor, PgConnection};

pub(crate) async fn downgrade(conn: &mut PgConnection) {
    sqlx::raw_sql(include_str!(
        "../../sinex-schema/src/tests/version_four.sql"
    ))
    .execute(conn)
    .await
    .unwrap();
}

/// Synthetic downgrades leave PostgreSQL's dropped-column positions behind.
/// Compare every column attribute except those unused physical positions.
pub(crate) async fn catalog(conn: &mut PgConnection) -> Vec<String> {
    let mut rows = sinex_schema::catalog(conn).await.unwrap();
    for row in &mut rows {
        let Some((column, attributes)) = row.split_once(' ') else {
            continue;
        };
        if [
            "core.events.kind_version",
            "core.events.validation",
            "telemetry.events.kind_version",
            "telemetry.events.validation",
            "core.admission_stage.kind_version",
            "core.admission_stage.validation",
        ]
        .contains(&column)
        {
            let (_, attributes) = attributes.split_once(' ').expect("catalog column position");
            *row = format!("{column} {attributes}");
        }
    }
    rows
}
