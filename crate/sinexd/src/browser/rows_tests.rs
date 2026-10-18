use super::*;
use sqlx::Connection;
#[tokio::test]
async fn oversized_row_fails_before_transfer_and_small_blob_round_trips() {
    let mut c = SqliteConnection::connect("sqlite::memory:").await.unwrap();
    for ddl in [
        "CREATE TABLE visits(id INTEGER PRIMARY KEY,original BLOB)",
        "CREATE TABLE urls(id INTEGER PRIMARY KEY)",
        "CREATE TABLE downloads(id INTEGER PRIMARY KEY)",
        "CREATE TABLE downloads_url_chains(id INTEGER,chain_index INTEGER,PRIMARY KEY(id,chain_index))",
    ] {
        sqlx::query(ddl).execute(&mut c).await.unwrap();
    }
    sqlx::query("INSERT INTO visits VALUES(1,zeroblob(?))")
        .bind((MAX_STEP_BYTES * 2) as i64)
        .execute(&mut c)
        .await
        .unwrap();
    let rows = ProviderRows::new(&mut c).await.unwrap();
    assert!(
        rows.read(&mut c, "visits", 1, None)
            .await
            .unwrap_err()
            .contains("before provider transfer")
    );
    sqlx::query("UPDATE visits SET original=x'0001feff'")
        .execute(&mut c)
        .await
        .unwrap();
    assert_eq!(
        rows.read(&mut c, "visits", 1, None).await.unwrap(),
        Some(json!({"id":1,"original":{"sqlite_blob":[0,1,254,255]}}))
    );
    assert!(
        rows.read(&mut c, "visits", 2, None)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn nonfinite_real_keeps_its_bits() {
    let mut c = SqliteConnection::connect("sqlite::memory:").await.unwrap();
    let row = sqlx::query("SELECT 1e999 AS value")
        .fetch_one(&mut c)
        .await
        .unwrap();
    assert_eq!(
        decode(row).unwrap(),
        json!({"value":{"sqlite_real_bits":"7ff0000000000000"}})
    );
}
