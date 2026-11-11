use super::*;
use sqlx::Connection;
use sqlx::postgres::PgPoolOptions;

/// The failure a statement meets on its own connection.
async fn failure_of(url: &str, statements: &[&str]) -> Failure {
    let mut conn = sqlx::PgConnection::connect(url).await.unwrap();
    for (n, statement) in statements.iter().enumerate() {
        match sqlx::raw_sql(statement).execute(&mut conn).await {
            Ok(_) if n + 1 < statements.len() => {}
            Ok(_) => panic!("{statement}: expected a failure"),
            Err(error) => return database_failure(&error),
        }
    }
    unreachable!("statements is not empty")
}

fn expect(failure: &Failure, kind: FailureKind, retryable: bool, code: &str) {
    assert_eq!(
        (failure.kind, failure.retryable, failure.code.as_deref()),
        (kind, retryable, Some(code)),
        "{failure:?}"
    );
}

#[devkit::test]
async fn every_sqlx_error_class_maps_to_its_kind(db: devkit::pg::TestDatabase) {
    let url = db.url(None);
    let table = "CREATE TEMP TABLE parent (id int CONSTRAINT parent_key PRIMARY KEY); \
                 CREATE TEMP TABLE child (id int CONSTRAINT child_parent REFERENCES parent(id)); \
                 INSERT INTO parent VALUES (1)";

    let unique = failure_of(&url, &[table, "INSERT INTO parent VALUES (1)"]).await;
    expect(&unique, FailureKind::Conflict, false, "parent_key");

    let foreign = failure_of(&url, &[table, "INSERT INTO child VALUES (2)"]).await;
    expect(&foreign, FailureKind::BadValue, false, "child_parent");

    let cancelled = failure_of(
        &url,
        &["SET statement_timeout = '20ms'", "SELECT pg_sleep(5)"],
    )
    .await;
    expect(&cancelled, FailureKind::Timeout, true, "statement_timeout");

    let serialization = failure_of(
        &url,
        &["DO $$ BEGIN RAISE EXCEPTION 'synthetic' USING ERRCODE = '40001'; END $$"],
    )
    .await;
    expect(&serialization, FailureKind::Conflict, true, "40001");

    let other = failure_of(&url, &["SELECT 1 / 0"]).await;
    expect(&other, FailureKind::Io, false, "22012");

    // One connection, held: the next acquire waits out its timeout.
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(std::time::Duration::from_millis(50))
        .connect(&url)
        .await
        .unwrap();
    let held = pool.acquire().await.unwrap();
    let busy = database_failure(&pool.acquire().await.unwrap_err());
    expect(&busy, FailureKind::Timeout, true, "read_pool_busy");
    drop(held);

    pool.close().await;
    let closed = database_failure(&pool.acquire().await.unwrap_err());
    expect(
        &closed,
        FailureKind::NotRunning,
        true,
        "database_unavailable",
    );

    // A connection the server ended.
    let mut victim = sqlx::PgConnection::connect(&url).await.unwrap();
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut victim)
        .await
        .unwrap();
    let mut killer = sqlx::PgConnection::connect(&url).await.unwrap();
    sqlx::query("SELECT pg_terminate_backend($1)")
        .bind(pid)
        .execute(&mut killer)
        .await
        .unwrap();
    let ended = database_failure(
        &sqlx::raw_sql("SELECT 1")
            .execute(&mut victim)
            .await
            .unwrap_err(),
    );
    expect(
        &ended,
        FailureKind::NotRunning,
        true,
        "database_unavailable",
    );

    let missing = database_failure(&sqlx::Error::RowNotFound);
    assert_eq!(
        (missing.kind, missing.retryable),
        (FailureKind::NotFound, false)
    );
}

#[test]
fn blob_errors_keep_their_kind() {
    let missing = hashed_blobs::Error::Io(std::io::Error::from(std::io::ErrorKind::NotFound));
    assert_eq!(blob_failure(&missing).kind, FailureKind::NotFound);
    let locked = hashed_blobs::Error::Locked(hashed_blobs::Hash([0; 32]));
    assert_eq!(blob_failure(&locked).kind, FailureKind::Refused);
    let unwrap = hashed_blobs::Error::Unwrap("wrong key".into());
    assert_eq!(blob_failure(&unwrap).kind, FailureKind::Io);
}
