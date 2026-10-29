use super::*;

#[devkit::test]
async fn shutdown_cleanup_releases_the_writer_claim_even_on_failure(db: devkit::pg::TestDatabase) {
    let url = db.url(None);
    let dir = tempfile::tempdir().unwrap();
    let blobs = Blobs::open(dir.path()).unwrap();
    forget_admitted(&url, &blobs).await.unwrap();
    let mut conn = connect(&url).await.unwrap();
    assert!(
        sinex_store::writer_lock::try_claim(&mut conn)
            .await
            .unwrap()
    );
    conn.close().await.unwrap();
    let mut fault = connect(&url).await.unwrap();
    sqlx::query("DROP TABLE core.blob_extents")
        .execute(&mut fault)
        .await
        .unwrap();
    fault.close().await.unwrap();
    assert!(forget_admitted(&url, &blobs).await.is_err());
    let mut conn = connect(&url).await.unwrap();
    assert!(
        sinex_store::writer_lock::try_claim(&mut conn)
            .await
            .unwrap()
    );
    conn.close().await.unwrap();
}
