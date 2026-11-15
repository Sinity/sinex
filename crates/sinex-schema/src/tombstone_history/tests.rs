use super::*;
use sqlx::{Connection, Executor, PgConnection};

const INCOMPLETE: &str =
    "INSERT INTO core.event_tombstones(event_id,source,event_type,operation_id)
    VALUES(uuidv7(),'synthetic','example.reading',uuidv7())";

/// A store holding tombstones written before the history columns existed:
/// the constraint is added after them, as it was on such a store.
async fn with_incomplete_history(conn: &mut PgConnection) {
    conn.execute("ALTER TABLE core.event_tombstones DROP CONSTRAINT tombstone_history_complete; INSERT INTO core.event_tombstones(event_id,source,event_type,operation_id) SELECT uuidv7(),'synthetic','example.reading',uuidv7() FROM generate_series(1,92)")
        .await
        .unwrap();
    conn.execute(COMPLETE).await.unwrap();
}

async fn tombstones(conn: &mut PgConnection) -> Vec<serde_json::Value> {
    sqlx::query_scalar("SELECT to_jsonb(t) FROM core.event_tombstones t ORDER BY event_id")
        .fetch_all(conn)
        .await
        .unwrap()
}

fn assert_check_violation(error: sqlx::Error) {
    assert_eq!(
        error.as_database_error().unwrap().code().as_deref(),
        Some("23514")
    );
}

#[devkit::test]
async fn a_tombstone_without_its_history_is_refused(db: devkit::pg::TestDatabase) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    assert_check_violation(conn.execute(INCOMPLETE).await.unwrap_err());
    let id: sqlx::types::Uuid = sqlx::query_scalar(
        "INSERT INTO core.event_tombstones
        (event_id,source,event_type,operation_id,commit_seq,original_commit_seq,class)
        VALUES(uuidv7(),'synthetic','example.reading',uuidv7(),11,7,'guarded') RETURNING event_id",
    )
    .fetch_one(&mut conn)
    .await
    .unwrap();
    assert_check_violation(
        sqlx::query("UPDATE core.event_tombstones SET class=NULL WHERE event_id=$1")
            .bind(id)
            .execute(&mut conn)
            .await
            .unwrap_err(),
    );
}

/// Backups dump the table with PostgreSQL's own tools; a restore keeps the
/// earlier incomplete rows and still refuses a new one.
#[devkit::test]
async fn native_dump_restore_keeps_earlier_incomplete_history(db: devkit::pg::TestDatabase) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    with_incomplete_history(&mut conn).await;
    crate::apply(&mut conn).await.unwrap();
    let expected = tombstones(&mut conn).await;
    assert_eq!(expected.len(), 92);
    let dump = std::process::Command::new("pg_dump")
        .args([
            "--format=custom",
            "--table=core.event_tombstones",
            "--dbname",
            &db.url(None),
        ])
        .output()
        .unwrap();
    assert!(
        dump.status.success(),
        "{}",
        String::from_utf8_lossy(&dump.stderr)
    );
    let restored = devkit::pg::test_database().unwrap();
    let mut target = PgConnection::connect(&restored.url(None)).await.unwrap();
    // This disposable target contains the current empty template relation.
    target
        .execute("DROP TABLE core.event_tombstones CASCADE")
        .await
        .unwrap();
    let mut child = std::process::Command::new("pg_restore")
        .args(["--exit-on-error", "--dbname", &restored.url(None)])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    std::io::Write::write_all(&mut child.stdin.take().unwrap(), &dump.stdout).unwrap();
    let result = child.wait_with_output().unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert_eq!(tombstones(&mut target).await, expected);
    assert_check_violation(target.execute(INCOMPLETE).await.unwrap_err());
}
