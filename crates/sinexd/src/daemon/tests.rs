use super::*;

#[tokio::test]
async fn shutdown_summary_keeps_unjoined_completions_and_actual_cancellations() {
    let mut tasks = JoinSet::new();
    let writer = tasks.spawn(async { ("writer", Ok(())) });
    let (entered, ready) = tokio::sync::oneshot::channel();
    tasks.spawn(async move {
        entered.send(()).unwrap();
        std::future::pending::<()>().await;
        ("held", Ok(()))
    });
    ready.await.unwrap();
    while !writer.is_finished() {
        tokio::task::yield_now().await;
    }
    let mut summary = ShutdownSummary {
        requested: tasks.len(),
        ..Default::default()
    };
    assert_eq!(summary.requested, 2, "completed but unjoined tasks count");
    summary.record(tasks.join_next().await.unwrap());
    summary.abort_requested = tasks.len();
    tasks.abort_all();
    while let Some(done) = tasks.join_next().await {
        summary.record(done);
    }
    assert_eq!(summary.joined, 2);
    assert!(summary.writer_joined);
    assert_eq!(summary.abort_requested, 1);
    assert_eq!(summary.cancelled, 1);
    assert_eq!(summary.failed, 0);
    assert_eq!(summary.panicked, 0);
    let retained = serde_json::to_value(summary).unwrap();
    assert_eq!(retained["tasks"][0]["name"], "writer");
    assert_eq!(retained["tasks"][1]["outcome"], "cancelled");
    assert!(retained["tasks"][1]["name"].is_null());
}

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
