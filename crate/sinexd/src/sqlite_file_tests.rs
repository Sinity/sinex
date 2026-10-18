use super::*;
use sqlx::{Connection, sqlite::SqliteConnectOptions};
struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!("sinex-sqlite-file-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir(&root).unwrap();
        Self(root)
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}
async fn create(path: &Path) {
    let mut connection = SqliteConnection::connect_with(
        &SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true),
    )
    .await
    .unwrap();
    sqlx::query("CREATE TABLE history(id TEXT, value INTEGER)")
        .execute(&mut connection)
        .await
        .unwrap();
    sqlx::query("INSERT INTO history(rowid,id,value) VALUES(9,'old',1),(27,'later',2)")
        .execute(&mut connection)
        .await
        .unwrap();
    connection.close().await.unwrap();
}
async fn open(path: &Path) -> SqliteConnection {
    SqliteConnection::connect_with(
        &SqliteConnectOptions::new()
            .filename(path)
            .read_only(true)
            .create_if_missing(false),
    )
    .await
    .unwrap()
}
#[tokio::test]
async fn rename_and_replacement_fail_until_reopened() {
    let f = Fixture::new();
    let path = f.0.join("history.db");
    create(&path).await;
    let mut reader = open(&path).await;
    assert_eq!(verify(&mut reader, &path).await.unwrap(), path);
    std::fs::rename(&path, f.0.join("old.db")).unwrap();
    create(&path).await;
    assert!(verify(&mut reader, &path).await.is_err());
    reader.close().await.unwrap();
    let mut reopened = open(&path).await;
    assert!(verify(&mut reopened, &path).await.is_ok());
    reopened.close().await.unwrap();
}
#[tokio::test]
async fn symlink_retarget_fails_until_reopened() {
    let f = Fixture::new();
    let a = f.0.join("a.db");
    let b = f.0.join("b.db");
    let link = f.0.join("configured.db");
    create(&a).await;
    create(&b).await;
    std::os::unix::fs::symlink(&a, &link).unwrap();
    let mut reader = open(&link).await;
    assert_eq!(verify(&mut reader, &link).await.unwrap(), a);
    std::fs::remove_file(&link).unwrap();
    std::os::unix::fs::symlink(&b, &link).unwrap();
    assert!(verify(&mut reader, &link).await.is_err());
    reader.close().await.unwrap();
    let mut reopened = open(&link).await;
    assert_eq!(verify(&mut reopened, &link).await.unwrap(), b);
    reopened.close().await.unwrap();
}
#[tokio::test]
async fn vacuum_renumbers_rowids_but_keeps_the_same_file() {
    let f = Fixture::new();
    let path = f.0.join("history.db");
    create(&path).await;
    let mut reader = open(&path).await;
    let before: i64 = sqlx::query_scalar("PRAGMA schema_version")
        .fetch_one(&mut reader)
        .await
        .unwrap();
    let mut producer = SqliteConnection::connect_with(&SqliteConnectOptions::new().filename(&path))
        .await
        .unwrap();
    sqlx::query("VACUUM").execute(&mut producer).await.unwrap();
    let after: i64 = sqlx::query_scalar("PRAGMA schema_version")
        .fetch_one(&mut reader)
        .await
        .unwrap();
    let coordinates: Vec<i64> = sqlx::query_scalar("SELECT rowid FROM history ORDER BY rowid")
        .fetch_all(&mut reader)
        .await
        .unwrap();
    assert_ne!(before, after);
    assert_eq!(coordinates, vec![1, 2]);
    assert!(
        verify(&mut reader, &path).await.is_ok(),
        "a schema change is not a file change"
    );
    producer.close().await.unwrap();
    reader.close().await.unwrap();
}
