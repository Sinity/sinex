use super::*;
use crate::{Applied, VERSION, apply};
use sqlx::Executor;

async fn version_two(conn: &mut PgConnection) {
    sqlx::raw_sql(include_str!("../tests/version_four.sql"))
        .execute(&mut *conn)
        .await
        .unwrap();
    sqlx::raw_sql(include_str!("../tests/version_two.sql"))
        .execute(&mut *conn)
        .await
        .unwrap();
}

/// Key rows of every form the encoding distinguishes, numbered so each has
/// its own event.
async fn keys(conn: &mut PgConnection, count: i32) {
    sqlx::query(
        "INSERT INTO core.live_equivalence (source, event_type, equivalence_key, event_id, reading_order, record_hash)
         SELECT 's' || (n % 3), 'command.executed',
           CASE n % 4 WHEN 0 THEN md5(n::text) || md5((n + 1)::text)
                      WHEN 1 THEN 'hash:' || md5(n::text) || md5((n + 1)::text)
                      WHEN 2 THEN 'sealed:' || md5(n::text) || md5((n + 1)::text)
                      ELSE 'free text ' || n END,
           md5(n::text)::uuid, n, decode(md5(n::text), 'hex')
         FROM generate_series(1, $1) n",
    )
    .bind(count)
    .execute(&mut *conn)
    .await
    .unwrap();
}

/// The table, encoded, and the copy, each in key order.
async fn table_and_copy(
    conn: &mut PgConnection,
) -> (Vec<serde_json::Value>, Vec<serde_json::Value>) {
    let table = sqlx::query_scalar(
        "SELECT jsonb_build_array(source, event_type, encode(sinex_schema.encode_equivalence_key(equivalence_key), 'hex'), event_id, reading_order, encode(record_hash, 'hex'))
         FROM core.live_equivalence ORDER BY 1",
    )
    .fetch_all(&mut *conn)
    .await
    .unwrap();
    let copy = sqlx::query_scalar(
        "SELECT jsonb_build_array(source, event_type, encode(equivalence_key, 'hex'), event_id, reading_order, encode(record_hash, 'hex'))
         FROM sinex_schema.live_equivalence_next ORDER BY 1",
    )
    .fetch_all(&mut *conn)
    .await
    .unwrap();
    (table, copy)
}

async fn assert_mirrored(conn: &mut PgConnection) {
    let (table, copy) = table_and_copy(conn).await;
    assert!(!table.is_empty());
    assert_eq!(copy, table);
}

/// The key table's owner and grants, which the catalog does not compare.
async fn access(conn: &mut PgConnection) -> (String, Option<String>) {
    sqlx::query_as(
        "SELECT pg_get_userbyid(relowner)::text, relacl::text FROM pg_class WHERE oid = 'core.live_equivalence'::regclass",
    )
    .fetch_one(&mut *conn)
    .await
    .unwrap()
}

async fn key_rows(conn: &mut PgConnection) -> Vec<serde_json::Value> {
    sqlx::query_scalar(
        "SELECT jsonb_build_array(source, event_type, core.decode_equivalence_key(equivalence_key), event_id, reading_order, encode(record_hash, 'hex'))
         FROM core.live_equivalence ORDER BY 1",
    )
    .fetch_all(&mut *conn)
    .await
    .unwrap()
}

/// law: prepared-copy-mirrors
#[devkit::test]
async fn prepared_copy_mirrors_every_change_and_is_swapped_in(db: devkit::pg::TestDatabase) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let fresh = crate::tests::catalog(&mut conn).await.unwrap();
    let fresh_access = access(&mut conn).await;
    version_two(&mut conn).await;
    keys(&mut conn, 40).await;
    assert_eq!(
        prepare(&mut conn, 7).await.unwrap(),
        Prepared::Complete { rows: 40 }
    );
    assert!(ready(&mut conn).await.unwrap());
    assert_mirrored(&mut conn).await;

    // What admission does to the table reaches the copy.
    conn.execute(
        "INSERT INTO core.live_equivalence VALUES ('s9', 'command.executed', 'added later', md5('x')::uuid, 1, '\\x01');
         UPDATE core.live_equivalence SET event_id = md5('replaced')::uuid, reading_order = 99 WHERE equivalence_key = 'free text 3';
         UPDATE core.live_equivalence SET equivalence_key = 'moved key' WHERE equivalence_key = 'free text 7';
         DELETE FROM core.live_equivalence WHERE equivalence_key = 'free text 11'",
    )
    .await
    .unwrap();
    assert_mirrored(&mut conn).await;
    // A second run copies nothing and keeps the copy whole.
    assert_eq!(
        prepare(&mut conn, 7).await.unwrap(),
        Prepared::Complete { rows: 0 }
    );
    assert_mirrored(&mut conn).await;

    let expected = key_rows_from_text(&mut conn).await;
    let copy: i64 =
        sqlx::query_scalar("SELECT 'sinex_schema.live_equivalence_next'::regclass::oid::bigint")
            .fetch_one(&mut conn)
            .await
            .unwrap();
    assert_eq!(
        apply(&mut conn).await.unwrap(),
        Applied::Upgraded {
            from: 2,
            to: VERSION
        }
    );
    let table: i64 = sqlx::query_scalar("SELECT 'core.live_equivalence'::regclass::oid::bigint")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(table, copy, "the prepared copy is the table now");
    assert_eq!(key_rows(&mut conn).await, expected);
    crate::online::finish(&mut conn).await.unwrap();
    assert_eq!(crate::tests::catalog(&mut conn).await.unwrap(), fresh);
    assert_eq!(access(&mut conn).await, fresh_access);
    assert_eq!(prepare(&mut conn, 7).await.unwrap(), Prepared::NotNeeded);
}

/// The text table's rows in the decoded form an upgraded table reads back.
async fn key_rows_from_text(conn: &mut PgConnection) -> Vec<serde_json::Value> {
    sqlx::query_scalar(
        "SELECT jsonb_build_array(source, event_type, equivalence_key, event_id, reading_order, encode(record_hash, 'hex'))
         FROM core.live_equivalence ORDER BY 1",
    )
    .fetch_all(&mut *conn)
    .await
    .unwrap()
}

/// law: prepared-copy-mirrors
/// A batch waits for an update and a delete in flight, and copies what they
/// left; their mirror already applied them.
#[devkit::test]
async fn changes_racing_a_backfill_batch_reach_the_copy(db: devkit::pg::TestDatabase) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    version_two(&mut conn).await;
    keys(&mut conn, 20).await;
    let mut tx = conn.begin().await.unwrap();
    for statement in objects() {
        tx.execute(statement.as_str()).await.unwrap();
    }
    tx.commit().await.unwrap();

    let mut backfill = PgConnection::connect(&db.url(None)).await.unwrap();
    let mut changer = PgConnection::connect(&db.url(None)).await.unwrap();
    // The changer has changed two keys and not committed.
    changer.execute("BEGIN").await.unwrap();
    changer
        .execute(
            "UPDATE core.live_equivalence SET reading_order = 500 WHERE equivalence_key = 'free text 3';
             DELETE FROM core.live_equivalence WHERE equivalence_key = 'free text 7'",
        )
        .await
        .unwrap();
    let batch = tokio::spawn(async move {
        let copied = copy_batch(&mut backfill, 1_000).await.unwrap();
        (backfill, copied)
    });
    // The batch waits for the changer.
    let mut waiting = false;
    for _ in 0..200 {
        waiting = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND query = 'LOCK TABLE core.live_equivalence IN SHARE MODE')",
        )
        .fetch_one(&mut conn)
        .await
        .unwrap();
        if waiting {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(waiting, "the backfill batch waits for the changer");
    changer.execute("COMMIT").await.unwrap();
    let (mut backfill, copied) = batch.await.unwrap();
    // Twenty rows: one deleted, one already mirrored by the update.
    assert_eq!(copied, Some(18));
    assert_eq!(copy_batch(&mut backfill, 1_000).await.unwrap(), None);
    assert!(ready(&mut conn).await.unwrap());
    assert_mirrored(&mut conn).await;
    let order: i64 = sqlx::query_scalar(
        "SELECT reading_order FROM sinex_schema.live_equivalence_next WHERE equivalence_key = sinex_schema.encode_equivalence_key('free text 3')",
    )
    .fetch_one(&mut conn)
    .await
    .unwrap();
    assert_eq!(order, 500);
}

/// A truncate empties the copy too.
#[devkit::test]
async fn a_truncate_reaches_the_copy(db: devkit::pg::TestDatabase) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    version_two(&mut conn).await;
    keys(&mut conn, 5).await;
    prepare(&mut conn, 2).await.unwrap();
    conn.execute("TRUNCATE core.live_equivalence")
        .await
        .unwrap();
    let copied: i64 = sqlx::query_scalar("SELECT count(*) FROM sinex_schema.live_equivalence_next")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(copied, 0);
}

/// An unfinished preparation is removed by the upgrade, which then converts
/// the table in place; `--unprepare` leaves the store as it was.
#[devkit::test]
async fn an_unfinished_preparation_is_removed_and_the_table_converted(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let fresh = crate::tests::catalog(&mut conn).await.unwrap();
    version_two(&mut conn).await;
    keys(&mut conn, 12).await;
    let unprepared = crate::tests::catalog(&mut conn).await.unwrap();
    prepare(&mut conn, 5).await.unwrap();
    unprepare(&mut conn).await.unwrap();
    assert_eq!(crate::tests::catalog(&mut conn).await.unwrap(), unprepared);

    let mut tx = conn.begin().await.unwrap();
    for statement in objects() {
        tx.execute(statement.as_str()).await.unwrap();
    }
    tx.commit().await.unwrap();
    assert_eq!(copy_batch(&mut conn, 5).await.unwrap(), Some(5));
    assert!(!ready(&mut conn).await.unwrap());
    let expected = key_rows_from_text(&mut conn).await;
    assert_eq!(
        apply(&mut conn).await.unwrap(),
        Applied::Upgraded {
            from: 2,
            to: VERSION
        }
    );
    assert_eq!(key_rows(&mut conn).await, expected);
    crate::online::finish(&mut conn).await.unwrap();
    assert_eq!(crate::tests::catalog(&mut conn).await.unwrap(), fresh);
}

/// The upgrade refuses a copy whose row count differs from the table's, and
/// rolls back whole.
#[devkit::test]
async fn a_copy_that_lost_a_row_is_refused(db: devkit::pg::TestDatabase) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    version_two(&mut conn).await;
    keys(&mut conn, 6).await;
    prepare(&mut conn, 4).await.unwrap();
    conn.execute(
        "DELETE FROM sinex_schema.live_equivalence_next WHERE equivalence_key = sinex_schema.encode_equivalence_key('free text 3')",
    )
    .await
    .unwrap();
    let error = apply(&mut conn).await.unwrap_err().to_string();
    assert!(
        error.contains("does not hold the same number of rows"),
        "{error}"
    );
    let version: i32 = sqlx::query_scalar("SELECT max(version) FROM sinex_schema.applied")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(version, 2);
    assert!(ready(&mut conn).await.unwrap());
}
