//! A database per test, cloned from the schema template by devkit, so tests
//! and compile-time checks cannot disagree about the schema. Kept for
//! inspection when the test fails.
use sqlx::{Connection, PgConnection};

pub(crate) async fn fresh() -> (devkit::pg::TestDatabase, PgConnection) {
    let db = devkit::pg::test_database().expect("test database: run tests through `xtask test`");
    let connection = PgConnection::connect(&db.url(None))
        .await
        .expect("test database connection");
    (db, connection)
}
