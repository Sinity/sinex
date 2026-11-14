//! The public operation owns actor identity and exposes forgotten holds.
use super::*;

#[devkit::test]
async fn backup_captures_producer_while_saved_binding_metadata_is_blocked(
    db: devkit::pg::TestDatabase,
) {
    use sqlx::Connection;
    let broker = devkit::nats::test_nats().unwrap();
    let client = async_nats::connect(broker.url()).await.unwrap();
    let subjects = sinex_protocol::Subjects::sinex();
    let producer = broker.prefix().to_owned();
    let (captured, observed) = tokio::sync::oneshot::channel();
    let captured = std::sync::Mutex::new(Some(captured));
    let _responder = sinex_transport::backup::BackupResponder::start(
        client.clone(),
        subjects.clone(),
        producer.clone(),
        sinex_transport::backup::Publication::default(),
        Arc::new(move |_| {
            if let Some(captured) = captured.lock().unwrap().take() {
                let _ = captured.send(());
            }
            Err("synthetic producer capture refusal".into())
        }),
    )
    .await
    .unwrap();
    let url = db.url(None);
    let w = world_with_api(
        db,
        Default::default(),
        PgPoolOptions::new().max_connections(READ_CONNECTIONS),
        |api| {
            api.backup_producers = Some((client, subjects, vec![producer.clone()]));
        },
    )
    .await;
    let mut blocker = sqlx::PgConnection::connect(&url).await.unwrap();
    let mut lock = blocker.begin().await.unwrap();
    sqlx::query("LOCK TABLE raw.bindings IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *lock)
        .await
        .unwrap();
    let root = tempfile::tempdir().unwrap();
    let artifact = root.path().join("capture-refused");
    let request = call(
        &w,
        Some(OPERATOR),
        json!({"method":"backup","action":"prepare","output":artifact}),
    );
    let release_metadata = async {
        let observed = tokio::time::timeout(std::time::Duration::from_secs(10), observed).await;
        lock.rollback().await.unwrap();
        observed
    };
    let ((_, result), observed) = tokio::join!(request, release_metadata);
    observed
        .expect("Capture must be polled while saved metadata waits")
        .unwrap();
    assert!(
        result
            .to_string()
            .contains("synthetic producer capture refusal"),
        "{result}"
    );
    let report: sinex_model::backup::Manifest = serde_json::from_reader(
        std::fs::File::open(artifact.join("producer-snapshots.json")).unwrap(),
    )
    .unwrap();
    let snapshot = report
        .producer_snapshots
        .iter()
        .find(|snapshot| snapshot.producer == producer)
        .unwrap();
    assert!(snapshot.run_id.is_some());
    assert!(
        snapshot
            .unavailable
            .as_deref()
            .unwrap()
            .contains("synthetic producer capture refusal")
    );
    assert!(!report.producers_complete);
    assert!(!artifact.join("main/sinex.dump").exists());
    assert!(!artifact.join("keys/data-keys.dump").exists());
    assert!(!artifact.join("main/segments").exists());
    assert!(!artifact.join("manifest.json").exists());
}

#[devkit::test]
async fn backup_discovers_inputs_before_pausing_producer_publication(db: devkit::pg::TestDatabase) {
    use futures::{FutureExt, StreamExt};
    let broker = devkit::nats::test_nats().unwrap();
    let client = async_nats::connect(broker.url()).await.unwrap();
    let subjects = sinex_protocol::Subjects::sinex();
    let producer = broker.prefix().to_owned();
    let mut requests = client.subscribe(subjects.backup(&producer)).await.unwrap();
    client.flush().await.unwrap();
    let config = serde_json::from_value(
        json!({"discovery":{"inputs":{"unknown-backup-format":["/synthetic"]}}}),
    )
    .unwrap();
    let w = world_with_api(
        db,
        config,
        PgPoolOptions::new().max_connections(READ_CONNECTIONS),
        |api| {
            api.backup_producers = Some((client.clone(), subjects, vec![producer]));
        },
    )
    .await;
    let root = tempfile::tempdir().unwrap();
    let artifact = root.path().join("discovery-refused");
    let (_, result) = call(
        &w,
        Some(OPERATOR),
        json!({"method":"backup","action":"prepare","output":artifact}),
    )
    .await;
    assert!(
        result.to_string().contains("unknown-backup-format"),
        "{result}"
    );
    client.flush().await.unwrap();
    assert!(
        requests.next().now_or_never().is_none(),
        "discovery must finish before Pause is sent"
    );
    let (_, holds) = call(
        &w,
        Some(OPERATOR),
        json!({"method":"backup","action":"list"}),
    )
    .await;
    assert_eq!(holds["result"]["holds"], json!([]));
    assert!(!artifact.join("main/sinex.dump").exists());
}

#[devkit::test]
async fn backup_holds_require_operator_access_and_page_without_expiry(
    db: devkit::pg::TestDatabase,
) {
    let w = world(db).await;
    for token in [READ, AGENT] {
        let (status, _) = call(&w, Some(token), json!({"method":"backup","action":"hold"})).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
    }
    let (_, one) = call(
        &w,
        Some(OPERATOR),
        json!({"method":"backup","action":"hold"}),
    )
    .await;
    assert_eq!(one["outcome"], "ok", "{one}");
    assert_eq!(one["result"]["hold"]["actor"], "operator");
    let (_, two) = call(
        &w,
        Some(OPERATOR),
        json!({"method":"backup","action":"hold"}),
    )
    .await;
    let (_, first) = call(
        &w,
        Some(OPERATOR),
        json!({"method":"backup","action":"list","page":1}),
    )
    .await;
    assert_eq!(first["result"]["holds"].as_array().unwrap().len(), 1);
    let cursor = first["result"]["page"]["cursor"].as_str().unwrap();
    let (_, second) = call(
        &w,
        Some(OPERATOR),
        json!({"method":"backup","action":"list","page":1,"cursor":cursor}),
    )
    .await;
    assert_eq!(second["result"]["holds"].as_array().unwrap().len(), 1);
    assert_ne!(
        first["result"]["holds"][0]["id"],
        second["result"]["holds"][0]["id"]
    );
    let (_, status) = call(&w, Some(READ), json!({"method":"status"})).await;
    assert_eq!(status["result"]["backup_holds"]["open"], 2);
    assert!(status["result"]["backup_holds"]["oldest"].is_string());
    for hold in [&one, &two] {
        let (_, released) = call(
            &w,
            Some(OPERATOR),
            json!({"method":"backup","action":"release","hold":hold["result"]["hold"]["id"]}),
        )
        .await;
        assert_eq!(released["outcome"], "ok", "{released}");
        assert!(!released["result"]["hold"]["released_at"].is_null());
    }
    let (_, listed) = call(
        &w,
        Some(OPERATOR),
        json!({"method":"backup","action":"list"}),
    )
    .await;
    assert_eq!(listed["result"]["holds"], json!([]));
}
