use super::*;
use crate::{Applied, VERSION, apply, catalog};
use sqlx::{Connection, Executor, PgConnection};

async fn predecessor(conn: &mut PgConnection) {
    conn.execute("ALTER TABLE core.event_tombstones DROP CONSTRAINT tombstone_history_complete")
        .await
        .unwrap();
    sqlx::query("UPDATE sinex_schema.applied SET version=$1")
        .bind(crate::version_before("closed_tombstone_history").unwrap())
        .execute(conn)
        .await
        .unwrap();
}

#[devkit::test]
async fn grandfathered_history_stays_unchanged_and_later_gaps_are_rejected(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let fresh = catalog(&mut conn).await.unwrap();
    predecessor(&mut conn).await;
    conn.execute(
        "INSERT INTO core.event_tombstones(event_id,source,event_type,operation_id)
        SELECT uuidv7(),'synthetic','example.reading',uuidv7() FROM generate_series(1,92)",
    )
    .await
    .unwrap();
    let before: Vec<serde_json::Value> =
        sqlx::query_scalar("SELECT to_jsonb(t) FROM core.event_tombstones t ORDER BY event_id")
            .fetch_all(&mut conn)
            .await
            .unwrap();
    assert_eq!(
        apply(&mut conn).await.unwrap(),
        Applied::Upgraded {
            from: crate::version_before("closed_tombstone_history").unwrap(),
            to: VERSION,
        }
    );
    assert_eq!(catalog(&mut conn).await.unwrap(), fresh);
    let after: Vec<serde_json::Value> =
        sqlx::query_scalar("SELECT to_jsonb(t) FROM core.event_tombstones t ORDER BY event_id")
            .fetch_all(&mut conn)
            .await
            .unwrap();
    assert_eq!(after, before);
    let error = conn
        .execute(
            "INSERT INTO core.event_tombstones(event_id,source,event_type,operation_id)
        VALUES(uuidv7(),'synthetic','example.reading',uuidv7())",
        )
        .await
        .unwrap_err();
    assert_eq!(
        error.as_database_error().unwrap().code().as_deref(),
        Some("23514")
    );
    let id: sqlx::types::Uuid = sqlx::query_scalar(
        "INSERT INTO core.event_tombstones
        (event_id,source,event_type,operation_id,commit_seq,original_commit_seq,class)
        VALUES(uuidv7(),'synthetic','example.reading',uuidv7(),11,7,'guarded') RETURNING event_id",
    )
    .fetch_one(&mut conn)
    .await
    .unwrap();
    let error = sqlx::query("UPDATE core.event_tombstones SET class=NULL WHERE event_id=$1")
        .bind(id)
        .execute(&mut conn)
        .await
        .unwrap_err();
    assert_eq!(
        error.as_database_error().unwrap().code().as_deref(),
        Some("23514")
    );
}

#[devkit::test]
async fn history_constraint_rolls_back_with_the_schema_transaction(db: devkit::pg::TestDatabase) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    predecessor(&mut conn).await;
    let before = catalog(&mut conn).await.unwrap();
    let mut tx = conn.begin().await.unwrap();
    tx.execute(COMPLETE).await.unwrap();
    tx.rollback().await.unwrap();
    assert_eq!(catalog(&mut conn).await.unwrap(), before);
    conn.execute(
        "INSERT INTO core.event_tombstones(event_id,source,event_type,operation_id)
        VALUES(uuidv7(),'synthetic','example.reading',uuidv7())",
    )
    .await
    .unwrap();
}

#[devkit::test]
async fn native_dump_restore_preserves_the_closed_predecessor_exception(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    predecessor(&mut conn).await;
    conn.execute("INSERT INTO core.event_tombstones(event_id,source,event_type,operation_id) SELECT uuidv7(),'synthetic','example.reading',uuidv7() FROM generate_series(1,92)").await.unwrap();
    apply(&mut conn).await.unwrap();
    let expected: Vec<serde_json::Value> =
        sqlx::query_scalar("SELECT to_jsonb(t) FROM core.event_tombstones t ORDER BY event_id")
            .fetch_all(&mut conn)
            .await
            .unwrap();
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
    let actual: Vec<serde_json::Value> =
        sqlx::query_scalar("SELECT to_jsonb(t) FROM core.event_tombstones t ORDER BY event_id")
            .fetch_all(&mut target)
            .await
            .unwrap();
    assert_eq!(actual, expected);
    let error=target.execute("INSERT INTO core.event_tombstones(event_id,source,event_type,operation_id) VALUES(uuidv7(),'synthetic','example.reading',uuidv7())").await.unwrap_err();
    assert_eq!(
        error.as_database_error().unwrap().code().as_deref(),
        Some("23514")
    );
}
