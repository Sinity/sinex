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
    std::fs::remove_dir(dir.path().join("unadmitted-blobs")).unwrap();
    assert!(forget_admitted(&url, &blobs).await.is_err());
    let mut conn = connect(&url).await.unwrap();
    assert!(
        sinex_store::writer_lock::try_claim(&mut conn)
            .await
            .unwrap()
    );
    conn.close().await.unwrap();
}
