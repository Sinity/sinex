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
    let request = call_unbounded(
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
    let (_, result) = call_unbounded(
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
    let (_, holds) = call_unbounded(
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
        let (status, _) =
            call_unbounded(&w, Some(token), json!({"method":"backup","action":"hold"})).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
    }
    let (_, one) = call_unbounded(
        &w,
        Some(OPERATOR),
        json!({"method":"backup","action":"hold"}),
    )
    .await;
    assert_eq!(one["outcome"], "ok", "{one}");
    assert_eq!(one["result"]["hold"]["actor"], "operator");
    let (_, two) = call_unbounded(
        &w,
        Some(OPERATOR),
        json!({"method":"backup","action":"hold"}),
    )
    .await;
    let (_, first) = call_unbounded(
        &w,
        Some(OPERATOR),
        json!({"method":"backup","action":"list","page":1}),
    )
    .await;
    assert_eq!(first["result"]["holds"].as_array().unwrap().len(), 1);
    let cursor = first["result"]["page"]["cursor"].as_str().unwrap();
    let (_, second) = call_unbounded(
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
    let (_, status) = call_unbounded(&w, Some(READ), json!({"method":"status"})).await;
    assert_eq!(status["result"]["backup_holds"]["open"], 2);
    assert!(status["result"]["backup_holds"]["oldest"].is_string());
    for hold in [&one, &two] {
        let (_, released) = call_unbounded(
            &w,
            Some(OPERATOR),
            json!({"method":"backup","action":"release","hold":hold["result"]["hold"]["id"]}),
        )
        .await;
        assert_eq!(released["outcome"], "ok", "{released}");
        assert!(!released["result"]["hold"]["released_at"].is_null());
    }
    let (_, listed) = call_unbounded(
        &w,
        Some(OPERATOR),
        json!({"method":"backup","action":"list"}),
    )
    .await;
    assert_eq!(listed["result"]["holds"], json!([]));
}

/// Staging a directory in Sinex's spool format stores its bytes as
/// materials; the staged directory is not one of the daemon's spools, so a
/// backup neither writes into it nor copies it. An unreadable spool under the
/// daemon's spool root still refuses the backup, and the refused attempt
/// releases its own hold.
#[devkit::test]
async fn backup_skips_staged_spool_directories_and_refuses_unreadable_spools(
    db: devkit::pg::TestDatabase,
) {
    use sinex_source_core::drivers::live_stream::{Rotation, SpoolWriter, capture_lines};
    use std::os::unix::fs::PermissionsExt;
    let broker = devkit::nats::test_nats().unwrap();
    let client = async_nats::connect(broker.url()).await.unwrap();
    let root = tempfile::tempdir().unwrap();
    let staged = root.path().join("staged/hyprland-copy");
    let spool = root.path().join("spool");
    std::fs::create_dir_all(&spool).unwrap();
    let mut writer = SpoolWriter::open(&staged, Rotation::default()).unwrap();
    capture_lines(
        &b"activewindow>>kitty,synthetic\nworkspacev2>>1,one\n"[..],
        &mut writer,
        std::time::Duration::ZERO,
    )
    .unwrap();
    drop(writer);
    let config = serde_json::from_value(json!({"discovery": {"spool": spool}})).unwrap();
    let w = world_with_api(
        db,
        config,
        PgPoolOptions::new().max_connections(READ_CONNECTIONS),
        |api| {
            api.backup_producers = Some((client, sinex_protocol::Subjects::sinex(), vec![]));
        },
    )
    .await;
    let (_, stage) = call_unbounded(
        &w,
        Some(OPERATOR),
        json!({"method": "stage", "path": [staged], "parser": ["hyprland"], "follow": true}),
    )
    .await;
    assert_eq!(stage["result"]["result"], "ok", "{stage}");
    let mut conn = w.reads.acquire().await.unwrap();
    let saved: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM raw.bindings WHERE source = 'desktop.hyprland' AND input = $1",
    )
    .bind(staged.to_str().unwrap())
    .fetch_one(&mut *conn)
    .await
    .unwrap();
    assert_eq!(saved, 1, "the stage saved its binding");
    let listing = |path: &std::path::Path| {
        let mut names: Vec<_> = std::fs::read_dir(path)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        names.sort();
        names
    };
    let before = listing(&staged);
    // The daemon may not write into a staged directory.
    std::fs::set_permissions(&staged, std::fs::Permissions::from_mode(0o555)).unwrap();
    let (_, prepared) = call_unbounded(
        &w,
        Some(OPERATOR),
        json!({"method":"backup","action":"prepare","output":root.path().join("backup")}),
    )
    .await;
    assert_eq!(prepared["outcome"], "ok", "{prepared}");
    assert_eq!(listing(&staged), before);
    let daemon = prepared["result"]["artifact"]["producer_snapshots"]
        .as_array()
        .unwrap()
        .iter()
        .find(|snapshot| snapshot["producer"] == "@daemon")
        .unwrap()
        .clone();
    assert!(
        daemon["files"]
            .as_array()
            .unwrap()
            .iter()
            .all(|file| !file["path"].as_str().unwrap().contains("/spool/")),
        "{daemon}"
    );
    let hold = prepared["result"]["artifact"]["boundary"]["id"].clone();
    let (_, released) = call_unbounded(
        &w,
        Some(OPERATOR),
        json!({"method":"backup","action":"release","hold":hold}),
    )
    .await;
    assert_eq!(released["outcome"], "ok", "{released}");

    // A kept clipboard spool under the spool root that the daemon cannot
    // read refuses the backup.
    let clipboard = spool.join("clipboard");
    let mut writer = SpoolWriter::open(&clipboard, Rotation::default()).unwrap();
    capture_lines(
        &b"synthetic clip\n"[..],
        &mut writer,
        std::time::Duration::ZERO,
    )
    .unwrap();
    drop(writer);
    let binding = format!("desktop.clipboard:{}@h", clipboard.display());
    sqlx::query("INSERT INTO raw.bindings (binding, source, input, settings) VALUES ($1, 'desktop.clipboard', $2, $3)")
        .bind(&binding)
        .bind(clipboard.to_str().unwrap())
        .bind(json!({"host": "h"}))
        .execute(&mut *conn)
        .await
        .unwrap();
    let input = uuid::Uuid::now_v7();
    let material = uuid::Uuid::now_v7();
    let blob = format!("blake3:{}", blake3::hash(material.as_bytes()).to_hex());
    sqlx::query("INSERT INTO core.blobs (hash, size, status) VALUES ($1, 0, 'present')")
        .bind(&blob)
        .execute(&mut *conn)
        .await
        .unwrap();
    sqlx::query("INSERT INTO raw.inputs (id, binding, kind, locator, host) VALUES ($1, $2, 'file', $3, 'h')")
        .bind(input)
        .bind(&binding)
        .bind(clipboard.to_str().unwrap())
        .execute(&mut *conn)
        .await
        .unwrap();
    sqlx::query("INSERT INTO raw.materials (id, input_id, blob, length, manifest, parser, parser_version) VALUES ($1, $2, $3, 0, '{}', $4, 1)")
        .bind(material)
        .bind(input)
        .bind(&blob)
        .bind(sinex_sources::sources::clipboard::SPEC.parser)
        .execute(&mut *conn)
        .await
        .unwrap();
    sqlx::query("INSERT INTO raw.positions (binding, cursor, carry, committed_seq) VALUES ($1, $2, '{}', 0)")
        .bind(&binding)
        .bind(json!({"material": material, "through": 0}))
        .execute(&mut *conn)
        .await
        .unwrap();
    drop(conn);
    std::fs::set_permissions(&clipboard, std::fs::Permissions::from_mode(0o000)).unwrap();
    let (_, refused) = call_unbounded(
        &w,
        Some(OPERATOR),
        json!({"method":"backup","action":"prepare","output":root.path().join("refused")}),
    )
    .await;
    std::fs::set_permissions(&clipboard, std::fs::Permissions::from_mode(0o755)).unwrap();
    std::fs::set_permissions(&staged, std::fs::Permissions::from_mode(0o755)).unwrap();
    assert_eq!(refused["outcome"], "failed", "{refused}");
    let message = refused.to_string();
    assert!(message.contains("@daemon"), "{refused}");
    assert!(message.contains("released"), "{refused}");
    let (_, holds) = call_unbounded(
        &w,
        Some(OPERATOR),
        json!({"method":"backup","action":"list"}),
    )
    .await;
    assert_eq!(holds["result"]["holds"], json!([]), "{holds}");
}
