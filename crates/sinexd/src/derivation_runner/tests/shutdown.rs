use super::*;

#[devkit::test]
async fn shutdown_finishes_the_active_pass_and_its_queued_admission(db: devkit::pg::TestDatabase) {
    let w = World::with_keys(db, vec![Box::new(Count::<1>)]).await;
    w.admit(vec![zsh("retained input", 0, 1)]).await;
    let driver = w.driver_with(vec![Box::new(Count::<1>)], vec![]).await;
    let mut blocker = PgConnection::connect(&w.url).await.unwrap();
    sqlx::query("BEGIN").execute(&mut blocker).await.unwrap();
    sqlx::query("LOCK TABLE core.events IN SHARE MODE")
        .execute(&mut blocker)
        .await
        .unwrap();
    let mut observer = PgConnection::connect(&w.url).await.unwrap();
    let (stop_tx, stop) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(driver.run(Arc::new(Notify::new()), Duration::from_secs(30), stop));
    let started = Instant::now();
    loop {
        let waiting: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_locks WHERE relation='core.events'::regclass AND NOT granted)")
            .fetch_one(&mut observer).await.unwrap();
        if waiting {
            break;
        }
        assert!(!task.is_finished(), "driver stopped before its admission");
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "writer did not reach the controlled admission boundary"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    stop_tx.send(true).unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(
        !task.is_finished(),
        "stop cancelled an in-flight pass before its queued batch was acknowledged"
    );
    sqlx::query("COMMIT").execute(&mut blocker).await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let outputs = w.live::<CommandFused>().await;
    assert_eq!(outputs.len(), 1);
    assert_eq!(outputs[0].command, "v1:1");
    assert_eq!(
        w.count("SELECT count(*) FROM derivation.dirty WHERE derivation='test.count'")
            .await,
        0
    );
}

#[devkit::test]
async fn a_closed_stop_channel_also_releases_the_idle_driver(db: devkit::pg::TestDatabase) {
    let w = World::with_keys(db, vec![]).await;
    let driver = w.driver_with(vec![], vec![]).await;
    let (stop_tx, stop) = tokio::sync::watch::channel(false);
    drop(stop_tx);
    tokio::time::timeout(
        Duration::from_secs(1),
        driver.run(Arc::new(Notify::new()), Duration::from_secs(30), stop),
    )
    .await
    .unwrap()
    .unwrap();
}
