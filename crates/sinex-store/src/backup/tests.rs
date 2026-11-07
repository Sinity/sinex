use super::*;
use crate::Blobs;
use sqlx::Connection;

async fn stored(conn: &mut PgConnection, blobs: &Blobs, bytes: &[u8]) -> String {
    let name = blobs.put(bytes).unwrap();
    let mut tx = conn.begin().await.unwrap();
    let receipt = blobs.commit_extents(&mut tx).await.unwrap();
    tx.commit().await.unwrap();
    blobs.acknowledge(receipt).unwrap();
    name
}

#[devkit::test]
async fn two_durable_holds_protect_a_prefix_across_restart_until_last_release(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = Blobs::open(dir.path()).unwrap();
    blobs.load(&mut conn).await.unwrap();
    let bytes = b"independent original bytes, including the deleting extent";
    let name = stored(&mut conn, &blobs, bytes).await;
    let original = std::fs::read(dir.path().join("segments/current.hb")).unwrap();
    let one = OperationId::new();
    let two = OperationId::new();
    let (first, created) = hold(&mut conn, one, "test").await.unwrap();
    assert!(created);
    hold(&mut conn, two, "test").await.unwrap();
    assert_eq!(
        hold(&mut conn, one, "test").await.unwrap(),
        (first.clone(), false)
    );
    assert!(hold(&mut conn, one, "forged").await.is_err());
    sqlx::query!(
        "UPDATE core.blob_extents SET deleting=true WHERE hash=$1",
        name
    )
    .execute(&mut conn)
    .await
    .unwrap();
    assert!(!blobs.delete(&mut conn, &name).await.unwrap());
    drop(blobs);
    let reopened = Blobs::open(dir.path()).unwrap();
    reopened.recover(&mut conn).await.unwrap();
    assert_eq!(
        std::fs::read(dir.path().join("segments/current.hb")).unwrap(),
        original
    );
    release(&mut conn, one, OperationId::new(), "test")
        .await
        .unwrap();
    assert!(!reopened.delete(&mut conn, &name).await.unwrap());
    release(&mut conn, two, OperationId::new(), "test")
        .await
        .unwrap();
    assert!(reopened.delete(&mut conn, &name).await.unwrap());
    assert_ne!(
        std::fs::read(dir.path().join("segments/current.hb")).unwrap(),
        original
    );
    assert!(list(&mut conn, None, 10).await.unwrap().is_empty());
    assert!(
        get(&mut conn, one)
            .await
            .unwrap()
            .unwrap()
            .released_at
            .is_some()
    );
}

#[devkit::test]
async fn a_prefix_hold_defers_orphan_cleanup_but_not_newer_appends(db: devkit::pg::TestDatabase) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = Blobs::open(dir.path()).unwrap();
    blobs.load(&mut conn).await.unwrap();
    let old = stored(&mut conn, &blobs, b"old protected orphan").await;
    let id = OperationId::new();
    hold(&mut conn, id, "test").await.unwrap();
    let before = std::fs::read(dir.path().join("segments/current.hb")).unwrap();
    let new = stored(&mut conn, &blobs, b"new unprotected orphan").await;
    let removed = crate::blobs::delete_unreferenced_extents(&mut conn, &blobs)
        .await
        .unwrap();
    assert_eq!(removed, vec![new]);
    assert_eq!(
        &std::fs::read(dir.path().join("segments/current.hb")).unwrap()[..before.len()],
        before
    );
    release(&mut conn, id, OperationId::new(), "test")
        .await
        .unwrap();
    assert_eq!(
        crate::blobs::finish_deleting_extents(&mut conn, &blobs)
            .await
            .unwrap(),
        vec![old]
    );
}

#[devkit::test]
async fn a_lost_exporter_cannot_be_replaced_by_reusing_its_hold(db: devkit::pg::TestDatabase) {
    let url = db.url(None);
    let mut conn = PgConnection::connect(&url).await.unwrap();
    let (hold, _) = hold(&mut conn, OperationId::new(), "test").await.unwrap();
    let snapshot = Snapshot::export(
        PgConnection::connect(&url).await.unwrap(),
        hold.clone(),
        url.clone(),
    )
    .await
    .unwrap();
    let exported = snapshot.exported.clone();
    snapshot.close().await.unwrap();
    let mut importer = PgConnection::connect(&url).await.unwrap();
    sqlx::query("BEGIN ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut importer)
        .await
        .unwrap();
    assert!(
        sqlx::query(&format!(
            "SET TRANSACTION SNAPSHOT '{}'",
            exported.replace('\'', "''")
        ))
        .execute(&mut importer)
        .await
        .is_err()
    );
    assert!(
        get(&mut conn, hold.id)
            .await
            .unwrap()
            .unwrap()
            .released_at
            .is_none()
    );
    // A later writer boundary makes a fresh snapshot under the old hold invalid,
    // even though its durable row and protected extents remain present.
    super::hold(&mut conn, OperationId::new(), "test")
        .await
        .unwrap();
    assert!(
        Snapshot::export(PgConnection::connect(&url).await.unwrap(), hold, url)
            .await
            .is_err()
    );
}

#[devkit::test]
async fn release_is_an_attributed_committed_operation_and_an_exact_retry(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let (hold, _) = hold(&mut conn, OperationId::new(), "holder").await.unwrap();
    let release_id = OperationId::new();
    let released = release(&mut conn, hold.id, release_id, "releaser")
        .await
        .unwrap();
    assert!(released.released_at.is_some());
    let actual: (String, String, i64, i64, String) = sqlx::query_as(
        "SELECT kind,actor,start_seq,end_seq,result FROM core.operations WHERE id=$1",
    )
    .bind(release_id.0)
    .fetch_one(&mut conn)
    .await
    .unwrap();
    assert_eq!(actual.0, "backup_release");
    assert_eq!(actual.1, "releaser");
    assert!(actual.2 > hold.commit_position);
    assert_eq!(actual.2, actual.3);
    assert_eq!(actual.4, "ok");
    assert_eq!(
        release(&mut conn, hold.id, OperationId::new(), "retry")
            .await
            .unwrap(),
        released
    );
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM core.operations WHERE kind='backup_release'")
            .fetch_one(&mut conn)
            .await
            .unwrap();
    assert_eq!(count, 1);
}
