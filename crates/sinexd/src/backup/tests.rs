use super::*;
use sqlx::{Connection, PgConnection};

async fn key(conn: &mut PgConnection, name: &str, bytes: &[u8]) {
    sqlx::query("INSERT INTO privacy.data_keys(material_id,key_ref,class,wrapped_key) VALUES($1,$2,'sealed',$3)")
        .bind(uuid::Uuid::now_v7()).bind(name).bind(bytes).execute(conn).await.unwrap();
}

/// The two real pg_dump children import one still-live snapshot. Admission
/// and wrapped keys changed after the cut are absent from both the SQL cut
/// and the immutable byte prefix, without stopping the original writer.
#[devkit::test]
async fn paired_dumps_and_segment_copy_keep_the_cut_while_the_writer_advances(
    db: devkit::pg::TestDatabase,
) {
    let url = db.url(None);
    let mut conn = PgConnection::connect(&url).await.unwrap();
    let source = tempfile::tempdir().unwrap();
    let output = tempfile::tempdir().unwrap();
    let artifact = output.path().join("artifact");
    create(&artifact).unwrap();
    let blobs = sinex_store::Blobs::open(source.path()).unwrap();
    blobs.load(&mut conn).await.unwrap();
    let original_name = blobs.put(b"original independent byte oracle").unwrap();
    sinex_store::blobs::commit_pending_extents(&mut conn, &blobs)
        .await
        .unwrap();
    key(&mut conn, "original-key", b"original-wrapped-key").await;
    let original = std::fs::read(source.path().join("segments/current.hb")).unwrap();
    let (writer, queues) = crate::writer::channels();
    let task = tokio::spawn(crate::writer::run(
        url.clone(),
        crate::writer::claim(PgConnection::connect(&url).await.unwrap())
            .await
            .unwrap(),
        queues,
        std::sync::Arc::new(sinex_store::NoDerivations),
        Some(blobs.clone()),
    ));
    let mut snapshot = writer.backup_snapshot("operator").await.unwrap();
    let hold = snapshot.hold.id;
    assert_eq!(snapshot.database.name, db.name);
    // This ordinary writer job proves that the snapshot did not retain the
    // admission lock for the duration of copying and dumping.
    blobs
        .put(b"post-boundary append must not be copied")
        .unwrap();
    let outcome = writer
        .submit(
            crate::writer::Priority::Live,
            live_batch("post-cut-admission"),
        )
        .await
        .unwrap();
    assert_eq!(outcome.admitted, 1);
    key(&mut conn, "later-key", b"later-wrapped-key").await;
    let at_cut: i64 = sqlx::query_scalar("SELECT count(*) FROM core.events")
        .fetch_one(snapshot.connection())
        .await
        .unwrap();
    let current: i64 = sqlx::query_scalar("SELECT count(*) FROM core.events")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(at_cut, 0);
    assert_eq!(current, 1);
    let (_stop, stop) = watch::channel(false);
    let manifest = prepare(
        snapshot,
        source.path(),
        &artifact,
        stop,
        "synthetic-source-revision".into(),
    )
    .await
    .unwrap();
    assert_eq!(manifest.boundary.id, hold);
    assert_eq!(
        manifest.boundary.producer_custody,
        sinex_model::backup::ProducerCustody::Incomplete
    );
    assert_eq!(
        std::fs::read(artifact.join("main/segments/current.hb")).unwrap(),
        original
    );
    assert_ne!(
        std::fs::read(source.path().join("segments/current.hb")).unwrap(),
        original
    );
    assert_eq!(
        std::fs::read(artifact.join("main/manifest.json")).unwrap(),
        std::fs::read(artifact.join("keys/manifest.json")).unwrap()
    );
    assert!(artifact.join("manifest.json").is_file());
    assert!(
        sinex_store::backup::get(&mut conn, hold)
            .await
            .unwrap()
            .unwrap()
            .released_at
            .is_none()
    );
    // Restore the key dump, rather than inferring its contents from its name
    // or pg_dump's exit status.
    sqlx::query("DELETE FROM privacy.data_keys")
        .execute(&mut conn)
        .await
        .unwrap();
    let status = tokio::process::Command::new("pg_restore")
        .env("PGDATABASE", &url)
        .args(["--no-password", "--exit-on-error", "--data-only"])
        .arg(artifact.join("keys/data-keys.dump"))
        .status()
        .await
        .unwrap();
    assert!(status.success());
    let restored: Vec<(String, Vec<u8>)> =
        sqlx::query_as("SELECT key_ref,wrapped_key FROM privacy.data_keys ORDER BY key_ref")
            .fetch_all(&mut conn)
            .await
            .unwrap();
    assert_eq!(
        restored,
        vec![("original-key".into(), b"original-wrapped-key".to_vec())]
    );
    // Inspect the actual dumped core operation stream through pg_restore.
    let result = tokio::process::Command::new("pg_restore")
        .args(["--data-only", "--table=operations", "--file=-"])
        .arg(artifact.join("main/sinex.dump"))
        .output()
        .await
        .unwrap();
    assert!(result.status.success());
    let dumped = String::from_utf8(result.stdout).unwrap();
    assert!(dumped.contains(&hold.to_string()));
    assert!(!dumped.contains("post-cut-admission"));
    let descriptor = blobs.descriptors(&original_name).unwrap();
    assert!(
        hashed_blobs::Reader::new(&artifact.join("main"))
            .scan(&[hashed_blobs::Extent::decode(&descriptor[0]).unwrap()])
            .is_empty()
    );
    drop(writer);
    task.await.unwrap();
}

#[devkit::test]
async fn stopping_preparation_never_publishes_an_artifact(db: devkit::pg::TestDatabase) {
    let url = db.url(None);
    let mut conn = PgConnection::connect(&url).await.unwrap();
    let (hold, _) = backup::hold(&mut conn, sinex_model::OperationId::new(), "test")
        .await
        .unwrap();
    let snapshot = Snapshot::export(
        PgConnection::connect(&url).await.unwrap(),
        hold.clone(),
        url,
    )
    .await
    .unwrap();
    let output = tempfile::tempdir().unwrap();
    let artifact = output.path().join("stopped");
    create(&artifact).unwrap();
    let (_sender, stop) = watch::channel(true);
    let result = prepare(snapshot, output.path(), &artifact, stop, "synthetic".into()).await;
    assert!(result.is_err());
    assert!(!artifact.join("manifest.json").exists());
    assert!(artifact.join("boundary.json").is_file());
    assert!(
        backup::get(&mut conn, hold.id)
            .await
            .unwrap()
            .unwrap()
            .released_at
            .is_none()
    );
    assert!(
        create(&artifact).is_err(),
        "an incomplete attempt cannot acquire a new snapshot under its old directory"
    );
}

fn live_batch(text: &str) -> sinex_model::Batch {
    use sinex_model::{
        Batch, BatchKind, Class, Event, MaterialId, Origin, ReadingOrder, RecordLocation,
        RecordOutcome, RunId, Timestamp,
    };
    let event = Event::build(
        &sinex_model::event_types::CommandImported {
            command: text.into(),
            hostname: "synthetic".into(),
            started_at: Some(Timestamp::from_unix_secs(1).unwrap()),
            header_elapsed: Some(0),
            paths: vec![],
            n: 0,
        },
        "t",
        Origin::Record {
            location: RecordLocation {
                material: MaterialId::new(),
                anchor: 0,
            },
            output_index: 0,
        },
        ReadingOrder(0),
        [1; 32],
        Class::Open,
    )
    .unwrap();
    Batch {
        run: RunId::new(),
        kind: BatchKind::Api {
            actor: "operator".into(),
        },
        records: vec![RecordOutcome::Events {
            location: None,
            events: vec![event],
        }],
        archive: vec![],
    }
}

#[devkit::test]
async fn exporter_loss_between_actual_dump_imports_refuses_the_second_dump(
    db: devkit::pg::TestDatabase,
) {
    let url = db.url(None);
    let mut conn = PgConnection::connect(&url).await.unwrap();
    key(&mut conn, "before-loss", b"wrapped").await;
    let (hold, _) = backup::hold(&mut conn, sinex_model::OperationId::new(), "test")
        .await
        .unwrap();
    let mut snapshot = Snapshot::export(
        PgConnection::connect(&url).await.unwrap(),
        hold.clone(),
        url,
    )
    .await
    .unwrap();
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(snapshot.connection())
        .await
        .unwrap();
    let output = tempfile::tempdir().unwrap();
    for name in ["main", "keys"] {
        std::fs::create_dir(output.path().join(name)).unwrap();
    }
    let (_sender, stop) = watch::channel(false);
    dump(&snapshot, output.path(), false, stop.clone())
        .await
        .unwrap();
    let terminated: bool = sqlx::query_scalar("SELECT pg_terminate_backend($1)")
        .bind(pid)
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert!(terminated);
    assert!(dump(&snapshot, output.path(), true, stop).await.is_err());
    assert!(!output.path().join("manifest.json").exists());
    assert!(
        backup::get(&mut conn, hold.id)
            .await
            .unwrap()
            .unwrap()
            .released_at
            .is_none()
    );
}
