//! A database per test, cloned from the schema template by devkit.
use sqlx::{Connection, PgConnection};

pub(crate) async fn fresh() -> (devkit::pg::TestDatabase, String, PgConnection) {
    let db = devkit::pg::test_database().expect("test database: run tests through `xtask test`");
    let url = db.url(None);
    let connection = PgConnection::connect(&url)
        .await
        .expect("test database connection");
    (db, url, connection)
}
