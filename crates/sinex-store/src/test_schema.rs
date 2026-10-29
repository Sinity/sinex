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
        if column.matches('.').count() == 2 {
            let (_, attributes) = attributes.split_once(' ').expect("catalog column position");
            *row = format!("{column} {attributes}");
        }
    }
    rows
}

pub(crate) async fn remove_foundations(conn: &mut PgConnection) {
    downgrade(conn).await;
}

pub(crate) async fn remove_version_4(conn: &mut PgConnection) {
    sqlx::raw_sql(include_str!("../../sinex-schema/src/tests/version_two.sql"))
        .execute(conn)
        .await
        .unwrap();
}
