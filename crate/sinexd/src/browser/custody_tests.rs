use super::*;
struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let p = std::env::temp_dir().join(format!("sinex-browser-custody-{}", Uuid::now_v7()));
        std::fs::create_dir(&p).unwrap();
        Self(p)
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}
async fn database(path: &Path) -> SqliteConnection {
    let mut c = SqliteConnection::connect_with(
        &SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true),
    )
    .await
    .unwrap();
    sqlx::query("CREATE TABLE proof(id INTEGER PRIMARY KEY,value TEXT)")
        .execute(&mut c)
        .await
        .unwrap();
    sqlx::query("INSERT INTO proof VALUES(1,'committed')")
        .execute(&mut c)
        .await
        .unwrap();
    c
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backup_includes_committed_wal_and_omits_uncommitted_writer_values() {
    let f = Fixture::new();
    let path = f.0.join("History");
    let mut producer = database(&path).await;
    sqlx::query("PRAGMA journal_mode=WAL")
        .execute(&mut producer)
        .await
        .unwrap();
    sqlx::query("INSERT INTO proof VALUES(2,'wal only')")
        .execute(&mut producer)
        .await
        .unwrap();
    let mut reader = open(&path, true).await.unwrap();
    let mut tx = reader.begin().await.unwrap();
    let _: i64 = sqlx::query_scalar("SELECT count(*) FROM proof")
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut producer)
        .await
        .unwrap();
    sqlx::query("UPDATE proof SET value='not committed'")
        .execute(&mut producer)
        .await
        .unwrap();
    let copy = f.0.join("owned.sqlite");
    let (_keep_stop, stop) = tokio::sync::watch::channel(false);
    backup(&mut tx, &copy, &stop).await.unwrap();
    tx.commit().await.unwrap();
    let mut copy = open(&copy, true).await.unwrap();
    let values: Vec<String> = sqlx::query_scalar("SELECT value FROM proof ORDER BY id")
        .fetch_all(&mut copy)
        .await
        .unwrap();
    assert_eq!(values, vec!["committed", "wal only"]);
    sqlx::query("ROLLBACK")
        .execute(&mut producer)
        .await
        .unwrap();
    copy.close().await.unwrap();
    reader.close().await.unwrap();
    producer.close().await.unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn locked_hot_journal_copy_recovers_only_owned_files_and_reuses_quiet_view() {
    let f = Fixture::new();
    let path = f.0.join("History");
    let mut producer = database(&path).await;
    sqlx::query("PRAGMA locking_mode=EXCLUSIVE")
        .execute(&mut producer)
        .await
        .unwrap();
    sqlx::query("PRAGMA cache_size=1")
        .execute(&mut producer)
        .await
        .unwrap();
    sqlx::query("BEGIN EXCLUSIVE")
        .execute(&mut producer)
        .await
        .unwrap();
    sqlx::query("UPDATE proof SET value='uncommitted'")
        .execute(&mut producer)
        .await
        .unwrap();
    let source_before = std::fs::read(&path).unwrap();
    let journal_before = std::fs::read(sidecar(&path, "-journal")).unwrap();
    let mut owner = Custodian::new(&path, f.0.join("owned")).await.unwrap();
    assert!(owner.direct.is_none());
    let (_keep_stop, stop) = tokio::sync::watch::channel(false);
    let (copy, evidence) = owner.copied_view(&stop, &mut None).await.unwrap();
    assert_eq!(evidence["kind"], "verified_locked_rollback_copy");
    let mut connection = open(&copy, true).await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT value FROM proof")
            .fetch_one(&mut connection)
            .await
            .unwrap(),
        "committed"
    );
    connection.close().await.unwrap();
    let (_, reused) = owner.copied_view(&stop, &mut None).await.unwrap();
    assert_eq!(reused["kind"], "verified_locked_copy_reused");
    assert_eq!(std::fs::read(&path).unwrap(), source_before);
    assert_eq!(
        std::fs::read(sidecar(&path, "-journal")).unwrap(),
        journal_before
    );
    drop(owner);
    sqlx::query("ROLLBACK")
        .execute(&mut producer)
        .await
        .unwrap();
    producer.close().await.unwrap();
}
#[test]
fn journal_appearance_disappearance_and_change_are_part_of_validation() {
    let f = Fixture::new();
    let path = f.0.join("History");
    std::fs::write(&path, b"synthetic").unwrap();
    let before = pair(&path).unwrap();
    std::fs::write(sidecar(&path, "-journal"), b"a").unwrap();
    let appeared = pair(&path).unwrap();
    assert_ne!(before, appeared);
    std::fs::write(sidecar(&path, "-journal"), b"different").unwrap();
    assert_ne!(appeared, pair(&path).unwrap());
    std::fs::remove_file(sidecar(&path, "-journal")).unwrap();
    assert_ne!(appeared, pair(&path).unwrap());
}
#[test]
fn external_super_journal_is_refused_before_any_recovery() {
    let f = Fixture::new();
    let p = f.0.join("journal");
    std::fs::write(&p, [0xd9, 0xd5, 0x05, 0xf9, 0x20, 0xa1, 0x63, 0xd7]).unwrap();
    assert!(reject_super_journal(&p).is_err());
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancellation_prevents_backup_completion() {
    let f = Fixture::new();
    let mut source = database(&f.0.join("History")).await;
    let (cancel, stop) = tokio::sync::watch::channel(false);
    cancel.send(true).unwrap();
    assert!(
        backup(&mut source, &f.0.join("owned"), &stop)
            .await
            .is_err()
    );
    source.close().await.unwrap();
}

#[test]
fn opened_file_replacement_requires_reconciliation_but_absence_is_unavailable() {
    let f = Fixture::new();
    let path = f.0.join("History");
    std::fs::write(&path, b"original").unwrap();
    let identity = stamp(&path).unwrap().identity;
    let file = File::open(&path).unwrap();
    std::fs::rename(&path, f.0.join("held")).unwrap();
    std::fs::write(&path, b"replacement").unwrap();
    let (reason, fence) = check_handle(&file, &path, &identity)
        .unwrap_err()
        .into_outcome();
    assert!(fence);
    assert_eq!(
        reason,
        "configured source no longer matches the opened file; explicit generation reconciliation required"
    );
    std::fs::remove_file(&path).unwrap();
    let (reason, fence) = check_handle(&file, &path, &identity)
        .unwrap_err()
        .into_outcome();
    assert!(!fence);
    assert_eq!(reason, "source file metadata unavailable");
}
