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
        Vec::new(),
        None,
    )
    .await
    .unwrap();
    assert_eq!(manifest.boundary.id, hold);
    assert_eq!(
        manifest.producer_custody,
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
    let result = prepare(
        snapshot,
        output.path(),
        &artifact,
        stop,
        "synthetic".into(),
        Vec::new(),
        None,
    )
    .await;
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

/// Restore the real older SQL checkpoint and native spool cut after ordinary
/// publication advanced and deleted the original segment. Resume must admit
/// the retained pending record rather than skip it using the newer checkpoint.
#[devkit::test]
async fn older_core_and_native_source_cut_resume_after_post_cut_retirement(
    db: devkit::pg::TestDatabase,
) {
    use crate::{
        binding_reader::{self, BindingReader, Rounds},
        material_access::{Access, Sink},
        reader_context::ReaderContext,
    };
    use sinex_source_core::{
        Binding, Settings,
        drivers::live_stream::{Rotation, SpoolWriter},
    };
    use sinex_sources::sources::journald;
    use sinex_transport::backup::capture_tree;
    let root = tempfile::tempdir().unwrap();
    let spool = root.path().join("spool");
    let source = root.path().join("store");
    let artifact = root.path().join("artifact");
    let restored_store = root.path().join("restored-store");
    let url = db.url(None);
    let settings = sinex_model::time_settings::TimeSettings::new("UTC", 4, 0).unwrap();
    let mut context = ReaderContext::open(&url, &source, settings.clone())
        .await
        .unwrap();
    let binding = Binding::of(&journald::SPEC, &spool, Settings::new("synthetic-host"));
    let mut reader = vec![BindingReader::new(&journald::FORMAT, binding.clone()).unwrap()];
    let mut rounds = Rounds::default();
    let sink = Sink(context.blobs.clone());
    let (_stop, stop) = watch::channel(false);
    let append = |writer: &mut SpoolWriter, cursor: &str, message: &str| {
        writer.append(sinex_model::Timestamp::now(),&serde_json::to_vec(&serde_json::json!({"__CURSOR":cursor,"__REALTIME_TIMESTAMP":"1000000","MESSAGE":message,"_HOSTNAME":"synthetic-host"})).unwrap()).unwrap();
        writer.sync().unwrap();
    };
    let mut capture = SpoolWriter::open(&spool, Rotation::default()).unwrap();
    append(&mut capture, "original-cursor", "original-record");
    for (_, result) in binding_reader::read(
        &mut context.read_conn,
        &context.blobs,
        &context.writer,
        &sink,
        &mut reader,
        &mut rounds,
        stop.clone(),
        Access::default(),
        &mut |_, _| {},
    )
    .await
    {
        result.unwrap();
    }
    let original_ids: Vec<uuid::Uuid> = sqlx::query_scalar(
        "SELECT id FROM core.events WHERE source='machine.journald' ORDER BY id",
    )
    .fetch_all(&mut context.read_conn)
    .await
    .unwrap();
    assert_eq!(original_ids.len(), 1);
    append(&mut capture, "pending-cursor", "pending-before-cut");
    let publication = context.writer.source_publication();
    let frozen = publication.freeze().await;
    let snapshot = context
        .writer
        .backup_snapshot("synthetic-backup")
        .await
        .unwrap();
    create(&artifact).unwrap();
    let local = local_sources(Ok(vec![binding.clone()]), &artifact).await;
    assert_eq!(local.unavailable, None);
    drop(frozen);
    drop(capture);
    let mut next = SpoolWriter::open(&spool, Rotation::default()).unwrap();
    append(&mut next, "later-cursor", "after-cut-record");
    for (_, result) in binding_reader::read(
        &mut context.read_conn,
        &context.blobs,
        &context.writer,
        &sink,
        &mut reader,
        &mut rounds,
        stop.clone(),
        Access::default(),
        &mut |_, _| {},
    )
    .await
    {
        result.unwrap();
    }
    assert!(
        !spool.join("000000000000.ndjson").exists(),
        "ordinary source retirement must actually remove the older generation"
    );
    let current: i64 =
        sqlx::query_scalar("SELECT count(*) FROM core.events WHERE source='machine.journald'")
            .fetch_one(&mut context.read_conn)
            .await
            .unwrap();
    assert_eq!(current, 3);
    let manifest = prepare(
        snapshot,
        &source,
        &artifact,
        stop.clone(),
        "synthetic-source-cut".into(),
        vec![local],
        Some(vec!["@daemon".into()]),
    )
    .await
    .unwrap();
    assert_eq!(
        manifest.producer_custody,
        sinex_model::backup::ProducerCustody::Complete
    );
    drop(next);
    drop(sink);
    context.close().await.unwrap();
    // This database and every filesystem path in this test are disposable
    // fixtures. The restore uses only the paired immutable artifact.
    let status = tokio::process::Command::new("pg_restore")
        .env("PGDATABASE", &url)
        .args([
            "--no-password",
            "--clean",
            "--if-exists",
            "--no-owner",
            "--exit-on-error",
        ])
        .arg(artifact.join("main/sinex.dump"))
        .status()
        .await
        .unwrap();
    assert!(status.success());
    let status = tokio::process::Command::new("pg_restore")
        .env("PGDATABASE", &url)
        .args(["--no-password", "--data-only", "--exit-on-error"])
        .arg(artifact.join("keys/data-keys.dump"))
        .status()
        .await
        .unwrap();
    assert!(status.success());
    std::fs::remove_dir_all(&spool).unwrap();
    capture_tree(&artifact.join("main/local-sources/spool/0"), &spool).unwrap();
    capture_tree(
        &artifact.join("main/segments"),
        &restored_store.join("segments"),
    )
    .unwrap();
    let mut context = ReaderContext::open(&url, &restored_store, settings)
        .await
        .unwrap();
    let restored_ids: Vec<uuid::Uuid> = sqlx::query_scalar(
        "SELECT id FROM core.events WHERE source='machine.journald' ORDER BY id",
    )
    .fetch_all(&mut context.read_conn)
    .await
    .unwrap();
    assert_eq!(restored_ids, original_ids);
    let sink = Sink(context.blobs.clone());
    let mut resumed = vec![BindingReader::new(&journald::FORMAT, binding).unwrap()];
    for (_, result) in binding_reader::read(
        &mut context.read_conn,
        &context.blobs,
        &context.writer,
        &sink,
        &mut resumed,
        &mut Rounds::default(),
        stop,
        Access::default(),
        &mut |_, _| {},
    )
    .await
    {
        result.unwrap();
    }
    let payloads: Vec<serde_json::Value> = sqlx::query_scalar(
        "SELECT payload FROM core.events WHERE source='machine.journald' ORDER BY id",
    )
    .fetch_all(&mut context.read_conn)
    .await
    .unwrap();
    assert_eq!(payloads.len(), 2);
    let encoded = serde_json::to_string(&payloads).unwrap();
    assert!(encoded.contains("pending-before-cut"));
    assert!(!encoded.contains("after-cut-record"));
    drop(sink);
    context.close().await.unwrap();
}
