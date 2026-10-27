use super::*;
use crate::reader_context::ReaderContext;
use serde_json::json;
use sinex_source_core::Settings;
use sinex_sources::sources::{
    self,
    google_takeout::{self, fit, tasks},
};
use std::io::Write;

fn archive(path: &std::path::Path) -> usize {
    let fit = json!({"Data Points": (0..40).map(|i| json!({"dataTypeName":"synthetic.steps", "startTimeNanos":i+1,"endTimeNanos":i+2,"fitValue":[{"intVal":i}]})).collect::<Vec<_>>()});
    let tasks = json!({"items":[{"id":"list", "items":[{"id":"task", "title":"synthetic task"}]}]});
    let mut tar = tar::Builder::new(Vec::new());
    for (name, bytes) in [
        (
            "Takeout/Fit/All Data/a.json",
            serde_json::to_vec(&fit).unwrap(),
        ),
        (
            "Takeout/Tasks/Tasks.json",
            serde_json::to_vec(&tasks).unwrap(),
        ),
        ("unused.txt", vec![b'x'; 300_000]),
    ] {
        let mut header = tar::Header::new_gnu();
        header.set_size(bytes.len() as u64);
        header.set_mode(0o600);
        header.set_cksum();
        tar.append_data(&mut header, name, bytes.as_slice())
            .unwrap();
    }
    let tar = tar.into_inner().unwrap();
    let mut gzip = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    gzip.write_all(&tar).unwrap();
    std::fs::write(path, gzip.finish().unwrap()).unwrap();
    tar.len()
}

/// The binding's Takeout driver with `part_bytes` per record part.
fn takeout_driver(
    binding: &Binding,
    part_bytes: usize,
) -> sinex_source_core::drivers::export_archive::ExportArchive {
    let host = binding.settings.host.clone();
    let mut driver = if binding.source == fit::SPEC.name {
        fit::driver(binding.input.clone(), host)
    } else {
        tasks::driver(binding.input.clone(), host)
    };
    driver.part_bytes = part_bytes;
    driver
}

/// The fields of a shared-pass cursor these tests check.
#[derive(serde::Deserialize)]
struct PassCursor {
    record: usize,
    material: Option<sinex_model::MaterialId>,
    original_material: Option<sinex_model::MaterialId>,
    done: bool,
    capture: PassCapture,
}

#[derive(serde::Deserialize)]
struct PassCapture {
    class: Class,
}

fn pass_cursor(cursor: &Value) -> PassCursor {
    serde_json::from_value(cursor.clone()).unwrap()
}

/// The polled reader of `b`.
fn polled(b: &mut BindingReader) -> &mut dyn Reader {
    let Reading::Polled(reader) = &mut b.reading else {
        panic!("{} is read in a shared pass", b.binding.name)
    };
    reader.as_mut()
}

fn readers(path: &std::path::Path) -> Vec<BindingReader> {
    let settings = Settings::new("synthetic");
    [fit::SPEC, tasks::SPEC]
        .iter()
        .map(|spec| {
            let binding = Binding::of(spec, path, settings.clone());
            let mut b = BindingReader::new(&google_takeout::FORMAT, binding).unwrap();
            // Force several ordinary parts without changing the production limits.
            b.reading = Reading::Shared(Box::new(takeout_driver(&b.binding, 400)));
            b
        })
        .collect()
}

type EventFields = (
    String,
    Option<String>,
    Value,
    Option<uuid::Uuid>,
    Option<i64>,
);

async fn events(conn: &mut PgConnection) -> Vec<EventFields> {
    sqlx::query_as("SELECT source, equivalence_key, payload, material_id, anchor FROM core.events WHERE event_type IN ('health.record', 'task.revised') ORDER BY source,equivalence_key,material_id,anchor")
        .fetch_all(conn).await.unwrap()
}

#[devkit::test]
async fn archive_acquisition_restart_replay_and_privacy_use_real_admission(
    db: devkit::pg::TestDatabase,
) {
    let url = db.url(None);
    let dir = std::env::temp_dir().join(format!("sinex-archive-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("export.tgz");
    let decoded = archive(&path);
    let mut ctx = ReaderContext::open(&url, &dir.join("store")).await.unwrap();
    let sink = Sink(ctx.blobs.clone());
    let (_stop, stop) = watch::channel(false);
    let mut bindings = readers(&path);
    let result = read(
        &mut ctx.read_conn,
        &ctx.blobs,
        &ctx.writer,
        RunId::new(),
        &sink,
        &mut bindings,
        stop.clone(),
        Access::default(),
        &mut |_, _| {},
    )
    .await;
    let mut total = Totals::default();
    for (_, r) in result {
        total.add(r.unwrap());
    }
    let metrics = total.to_json();
    assert_eq!(metrics["files_stored"], 1);
    assert!(metrics["pass_decoded_bytes"].as_u64().unwrap() <= decoded as u64);
    let before = events(&mut ctx.read_conn).await;
    assert_eq!(before.len(), 41);
    let originals: Vec<(uuid::Uuid, String)> = sqlx::query_as(
        "SELECT id,blob FROM raw.materials WHERE manifest->>'role' = 'original-archive'",
    )
    .fetch_all(&mut ctx.read_conn)
    .await
    .unwrap();
    assert_eq!(originals.len(), 2);
    assert_ne!(originals[0].0, originals[1].0);
    assert_eq!(originals[0].1, originals[1].1);
    let materials: i64 = sqlx::query_scalar("SELECT count(*) FROM raw.materials")
        .fetch_one(&mut ctx.read_conn)
        .await
        .unwrap();
    // Fresh reader objects model a daemon restart. A completed cursor reads nothing.
    let mut restarted = readers(&path);
    let resumed = read(
        &mut ctx.read_conn,
        &ctx.blobs,
        &ctx.writer,
        RunId::new(),
        &sink,
        &mut restarted,
        stop.clone(),
        Access::default(),
        &mut |_, _| {},
    )
    .await;
    for (_, result) in resumed {
        assert_eq!(result.unwrap().to_json()["files_stored"], 0);
    }
    assert_eq!(events(&mut ctx.read_conn).await, before);
    let report = crate::replay::run(
        &mut ctx.read_conn,
        &ctx.blobs,
        &ctx.writer,
        crate::replay::Options {
            scope: crate::replay::Scope::default(),
            compare: false,
            resume: None,
            actor: "synthetic".into(),
            unlock: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(report.failures, 0);
    assert_eq!(report.events_read, 41);
    let after = events(&mut ctx.read_conn).await;
    // Replay creates readings, but preserves source keys, exact values and record locations.
    assert_eq!(after.len(), 82);
    for event in &before {
        assert_eq!(after.iter().filter(|e| *e == event).count(), 2);
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raw.materials")
            .fetch_one(&mut ctx.read_conn)
            .await
            .unwrap(),
        materials
    );
    ctx.close().await.unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
}

#[devkit::test]
async fn archive_acquisition_separates_effective_privacy_classes(db: devkit::pg::TestDatabase) {
    let url = db.url(None);
    let dir = std::env::temp_dir().join(format!("sinex-archive-privacy-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("export.tgz");
    archive(&path);
    let mut ctx = ReaderContext::open(&url, &dir.join("store")).await.unwrap();
    sqlx::query("INSERT INTO privacy.source_classes(source,class,author) VALUES ('google.tasks','guarded','synthetic')").execute(&mut ctx.read_conn).await.unwrap();
    let sink = Sink(ctx.blobs.clone());
    let (_stop, stop) = watch::channel(false);
    let mut bindings = readers(&path);
    let mut total = Totals::default();
    for (_, result) in read(
        &mut ctx.read_conn,
        &ctx.blobs,
        &ctx.writer,
        RunId::new(),
        &sink,
        &mut bindings,
        stop,
        Access::default(),
        &mut |_, _| {},
    )
    .await
    {
        total.add(result.unwrap());
    }
    assert_eq!(total.to_json()["files_stored"], 2);
    let classes: Vec<String> =
        sqlx::query_scalar("SELECT DISTINCT class FROM core.events WHERE source='google.tasks'")
            .fetch_all(&mut ctx.read_conn)
            .await
            .unwrap();
    assert_eq!(classes, vec!["guarded"]);
    ctx.close().await.unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
}

#[devkit::test]
async fn cancelled_archive_worker_joins_and_restart_catches_up(db: devkit::pg::TestDatabase) {
    let url = db.url(None);
    let dir = std::env::temp_dir().join(format!("sinex-archive-cancel-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("export.tgz");
    archive(&path);
    let mut ctx = ReaderContext::open(&url, &dir.join("store")).await.unwrap();
    let sink = Sink(ctx.blobs.clone());
    let (_stop, stop) = watch::channel(true);
    let mut bindings = readers(&path);
    let cancelled = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        read(
            &mut ctx.read_conn,
            &ctx.blobs,
            &ctx.writer,
            RunId::new(),
            &sink,
            &mut bindings,
            stop,
            Access::default(),
            &mut |_, _| {},
        ),
    )
    .await
    .expect("cancelled worker must join");
    assert!(cancelled.iter().all(|(_, result)| result.is_err()));
    let (_stop, stop) = watch::channel(false);
    let mut restarted = readers(&path);
    for (_, result) in read(
        &mut ctx.read_conn,
        &ctx.blobs,
        &ctx.writer,
        RunId::new(),
        &sink,
        &mut restarted,
        stop,
        Access::default(),
        &mut |_, _| {},
    )
    .await
    {
        result.unwrap();
    }
    assert_eq!(events(&mut ctx.read_conn).await.len(), 41);
    ctx.close().await.unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
}

struct SmallBatches(Box<dyn Source>);
impl Source for SmallBatches {
    fn spec(&self) -> &sinex_source_core::SourceSpec {
        self.0.spec()
    }
    fn drainer<'a>(
        &'a self,
        mut input: sinex_source_core::DrainInput<'a>,
    ) -> Result<
        Box<
            dyn Iterator<
                    Item = Result<sinex_source_core::DrainOutput, sinex_source_core::RecordError>,
                > + Send
                + 'a,
        >,
        sinex_source_core::RecordError,
    > {
        input.limits.max_events = 2;
        self.0.drainer(input)
    }
}

#[devkit::test]
async fn sealed_archive_restart_replay_and_class_changes_preserve_exact_materials() {
    for zip in [false, true] {
        // Each container kind gets its own database.
        let db = devkit::pg::test_database().unwrap();
        let url = db.url(None);
        let dir =
            std::env::temp_dir().join(format!("sinex-sealed-archive-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(if zip { "export.zip" } else { "export.tgz" });
        if zip {
            let mut file = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
            file.start_file(
                "Takeout/Fit/All Data/a.json",
                zip::write::SimpleFileOptions::default(),
            )
            .unwrap();
            file.write_all(&serde_json::to_vec(&json!({"Data Points":(0..40).map(|i|json!({"dataTypeName":"synthetic.steps","startTimeNanos":i+1,"endTimeNanos":i+2,"fitValue":[{"intVal":i}]})).collect::<Vec<_>>()})).unwrap()).unwrap();
            std::fs::write(&path, file.finish().unwrap().into_inner()).unwrap();
        } else {
            archive(&path);
        }
        let mut ctx = ReaderContext::open(&url, &dir.join("store")).await.unwrap();
        sqlx::query("INSERT INTO privacy.source_classes(source,class,author) VALUES ('google.fit','sealed','synthetic')").execute(&mut ctx.read_conn).await.unwrap();
        let identity = sinex_sealing::Identity::generate();
        let recipient = identity.to_public();
        let sink = Sink(ctx.blobs.clone());
        let (_stop, stop) = watch::channel(false);
        let make_bound = || {
            let mut bindings = readers(&path);
            bindings.truncate(1);
            let b = &mut bindings[0];
            b.reading = Reading::Shared(Box::new(takeout_driver(&b.binding, 1 << 20)));
            b.source = Box::new(SmallBatches(std::mem::replace(
                &mut b.source,
                Box::new(fit::source()),
            )));
            bindings
        };
        // The second batch fails through real admission, leaving a durable partial part.
        sqlx::raw_sql("CREATE FUNCTION synthetic_stop() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF (SELECT count(*) FROM core.events) >= 2 THEN RAISE EXCEPTION 'synthetic admission stop'; END IF; RETURN NEW; END $$; CREATE TRIGGER synthetic_stop BEFORE INSERT ON core.events FOR EACH ROW EXECUTE FUNCTION synthetic_stop();").execute(&mut ctx.read_conn).await.unwrap();
        let mut bindings = make_bound();
        let name = bindings[0].binding.name.clone();
        let first = read(
            &mut ctx.read_conn,
            &ctx.blobs,
            &ctx.writer,
            RunId::new(),
            &sink,
            &mut bindings,
            stop.clone(),
            Access {
                recipient: Some(&recipient),
                unlock: None,
            },
            &mut |_, _| {},
        )
        .await;
        assert!(
            first[0].1.is_err(),
            "synthetic admission stop must be reported"
        );
        let checkpoint = positions::load(&mut ctx.read_conn, &name)
            .await
            .unwrap()
            .unwrap();
        let cursor = pass_cursor(&checkpoint.0);
        assert_eq!(cursor.record, 2);
        assert!(checkpoint.1.is_null() || sinex_sealing::is_sealed(&checkpoint.1));
        let partial = cursor.material.unwrap();
        let original = cursor.original_material.unwrap();
        assert_ne!(partial, original);
        sqlx::raw_sql(
            "DROP TRIGGER synthetic_stop ON core.events; DROP FUNCTION synthetic_stop();",
        )
        .execute(&mut ctx.read_conn)
        .await
        .unwrap();
        let mut restarted = make_bound();
        let locked = read(
            &mut ctx.read_conn,
            &ctx.blobs,
            &ctx.writer,
            RunId::new(),
            &sink,
            &mut restarted,
            stop.clone(),
            Access {
                recipient: Some(&recipient),
                unlock: None,
            },
            &mut |_, _| {},
        )
        .await;
        assert!(
            locked[0]
                .1
                .as_ref()
                .is_err_and(|e| e.contains("needs an operator unlock"))
        );
        assert_eq!(
            positions::load(&mut ctx.read_conn, &name)
                .await
                .unwrap()
                .unwrap(),
            checkpoint
        );
        let unlock =
            crate::sealing::Unlock::new(identity, std::time::Duration::from_secs(60)).unwrap();
        let mut restarted = make_bound();
        let unlocked = read(
            &mut ctx.read_conn,
            &ctx.blobs,
            &ctx.writer,
            RunId::new(),
            &sink,
            &mut restarted,
            stop.clone(),
            Access {
                recipient: Some(&recipient),
                unlock: Some(&unlock),
            },
            &mut |_, _| {},
        )
        .await;
        for (_, result) in unlocked {
            let total = result.unwrap();
            assert_eq!(total.to_json()["files_stored"], 0);
        }
        let final_cursor = pass_cursor(
            &positions::load(&mut ctx.read_conn, &name)
                .await
                .unwrap()
                .unwrap()
                .0,
        );
        assert!(final_cursor.done);
        // The rest of the partly admitted part was read again into a new part.
        assert_ne!(final_cursor.material, Some(partial));
        assert_eq!(final_cursor.original_material, Some(original));
        let before = live_events(&mut ctx.read_conn).await;
        assert_eq!(before.len(), 40);
        let keys: Vec<String> = sqlx::query_scalar("SELECT key_ref FROM privacy.data_keys")
            .fetch_all(&mut ctx.read_conn)
            .await
            .unwrap();
        assert_eq!(keys.len(), 3);
        assert_eq!(
            keys.iter().collect::<std::collections::BTreeSet<_>>().len(),
            3,
            "every material has its own key"
        );
        assert!(before.iter().all(|e| sinex_sealing::is_sealed(&e.2)));
        let scope = crate::replay::Scope {
            binding: Some(name.clone()),
            ..Default::default()
        };
        let replay = |unlock| crate::replay::Options {
            scope: scope.clone(),
            compare: false,
            resume: None,
            actor: "operator".into(),
            unlock,
        };
        assert!(
            crate::replay::run(&mut ctx.read_conn, &ctx.blobs, &ctx.writer, replay(None))
                .await
                .is_err()
        );
        assert_eq!(live_events(&mut ctx.read_conn).await, before);
        let report = crate::replay::run(
            &mut ctx.read_conn,
            &ctx.blobs,
            &ctx.writer,
            replay(Some(unlock.clone())),
        )
        .await
        .unwrap();
        assert_eq!(report.events_read, 40);
        assert_eq!((report.archived, report.admitted), (40, 40));
        let after = live_events(&mut ctx.read_conn).await;
        assert_eq!(after.len(), 40);
        for (old, new) in before.iter().zip(&after) {
            assert_eq!(
                (&old.0, &old.1, old.3, old.4),
                (&new.0, &new.1, new.3, new.4)
            );
            let key = unlock
                .data_key(&mut ctx.read_conn, sinex_model::MaterialId(old.3.unwrap()))
                .await
                .unwrap();
            assert_eq!(
                sinex_sealing::unseal_payload(&old.2, &key.key).unwrap(),
                sinex_sealing::unseal_payload(&new.2, &key.key).unwrap()
            );
        }
        unlock.revoke();
        assert!(
            crate::replay::run(
                &mut ctx.read_conn,
                &ctx.blobs,
                &ctx.writer,
                replay(Some(unlock))
            )
            .await
            .is_err()
        );
        // Changing the effective class starts a new capture, rather than falsely reusing completion.
        sqlx::query("INSERT INTO privacy.source_classes(source,class,author) VALUES ('google.fit','guarded','synthetic')").execute(&mut ctx.read_conn).await.unwrap();
        let mut changed = make_bound();
        let result = read(
            &mut ctx.read_conn,
            &ctx.blobs,
            &ctx.writer,
            RunId::new(),
            &sink,
            &mut changed,
            stop,
            Access::default(),
            &mut |_, _| {},
        )
        .await;
        assert_eq!(
            result.into_iter().next().unwrap().1.unwrap().to_json()["files_stored"],
            1
        );
        let cursor = pass_cursor(
            &positions::load(&mut ctx.read_conn, &name)
                .await
                .unwrap()
                .unwrap()
                .0,
        );
        assert_eq!(cursor.capture.class, Class::Guarded);
        assert_ne!(cursor.original_material, Some(original));
        sqlx::query("INSERT INTO privacy.source_classes(source,class,author) VALUES ('google.fit','sealed','synthetic')").execute(&mut ctx.read_conn).await.unwrap();
        let mut changed = make_bound();
        let (_stop, stop) = watch::channel(false);
        let result = read(
            &mut ctx.read_conn,
            &ctx.blobs,
            &ctx.writer,
            RunId::new(),
            &sink,
            &mut changed,
            stop,
            Access {
                recipient: Some(&recipient),
                unlock: None,
            },
            &mut |_, _| {},
        )
        .await;
        assert_eq!(
            result.into_iter().next().unwrap().1.unwrap().to_json()["files_stored"],
            1
        );
        let cursor = pass_cursor(
            &positions::load(&mut ctx.read_conn, &name)
                .await
                .unwrap()
                .unwrap()
                .0,
        );
        assert_eq!(cursor.capture.class, Class::Sealed);
        assert_ne!(cursor.original_material, Some(original));
        ctx.close().await.unwrap();
        std::fs::remove_dir_all(&dir).unwrap();
    }
}

#[devkit::test]
async fn sealed_bindings_never_share_data_keys_or_original_blob_names(
    db: devkit::pg::TestDatabase,
) {
    let url = db.url(None);
    let dir = std::env::temp_dir().join(format!("sinex-sealed-bindings-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("export.tgz");
    archive(&path);
    let mut ctx = ReaderContext::open(&url, &dir.join("store")).await.unwrap();
    sqlx::query("INSERT INTO privacy.source_classes(source,class,author) VALUES ('google.fit','sealed','synthetic'),('google.tasks','sealed','synthetic')").execute(&mut ctx.read_conn).await.unwrap();
    let identity = sinex_sealing::Identity::generate();
    let recipient = identity.to_public();
    let sink = Sink(ctx.blobs.clone());
    let (_stop, stop) = watch::channel(false);
    let mut bindings = readers(&path);
    for (_, result) in read(
        &mut ctx.read_conn,
        &ctx.blobs,
        &ctx.writer,
        RunId::new(),
        &sink,
        &mut bindings,
        stop,
        Access {
            recipient: Some(&recipient),
            unlock: None,
        },
        &mut |_, _| {},
    )
    .await
    {
        result.unwrap();
    }
    let originals:Vec<(String,String)>=sqlx::query_as("SELECT m.blob,k.key_ref FROM raw.materials m JOIN privacy.data_keys k ON m.id=k.material_id WHERE m.manifest->>'role'='original-archive'").fetch_all(&mut ctx.read_conn).await.unwrap();
    assert_eq!(originals.len(), 2);
    assert_ne!(originals[0].0, originals[1].0);
    assert_ne!(originals[0].1, originals[1].1);
    ctx.close().await.unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
}

async fn live_events(conn: &mut PgConnection) -> Vec<EventFields> {
    sqlx::query_as("SELECT e.source,e.equivalence_key,e.payload,e.material_id,e.anchor FROM core.events e WHERE e.id IN (SELECT event_id FROM core.live_equivalence UNION SELECT event_id FROM core.live_locations) ORDER BY source,equivalence_key,material_id,anchor").fetch_all(conn).await.unwrap()
}

mod directory;
mod long_history;

#[devkit::test]
async fn an_unchanged_append_only_file_is_not_polled_again(db: devkit::pg::TestDatabase) {
    let url = db.url(None);
    let dir = std::env::temp_dir().join(format!("sinex-append-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&dir).unwrap();
    let history = dir.join("zsh_history");
    std::fs::write(&history, b": 1700000000:0;echo one\n").unwrap();
    let mut ctx = ReaderContext::open(&url, &dir.join("store")).await.unwrap();
    let sink = Sink(ctx.blobs.clone());
    let (_stop, stop) = watch::channel(false);
    let format = sinex_sources::sources::format_of("terminal.zsh-history").unwrap();
    let settings = Settings {
        host: "h".into(),
        zone: None,
        options: Value::Null,
    };
    let mut bindings: Vec<BindingReader> = (format.plan.unwrap())(&history, &settings)
        .unwrap()
        .into_iter()
        .map(|b| BindingReader::new(format, b).unwrap())
        .collect();
    let mut round = async |bindings: &mut Vec<BindingReader>| {
        let mut total = Totals::default();
        for (_, r) in read(
            &mut ctx.read_conn,
            &ctx.blobs,
            &ctx.writer,
            RunId::new(),
            &sink,
            bindings,
            stop.clone(),
            Access::default(),
            &mut |_, _| {},
        )
        .await
        {
            total.add(r.unwrap());
        }
        total.to_json()["polls"].as_u64().unwrap()
    };
    assert!(
        round(&mut bindings).await > 0,
        "the first round reads the file"
    );
    assert_eq!(
        round(&mut bindings).await,
        0,
        "an unchanged file is skipped"
    );
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(&history)
        .unwrap();
    file.write_all(b": 1700000005:0;echo two\n").unwrap();
    drop(file);
    assert!(round(&mut bindings).await > 0, "an appended file is read");
    let commands: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM core.events WHERE event_type = 'command.imported'",
    )
    .fetch_one(&mut ctx.read_conn)
    .await
    .unwrap();
    assert_eq!(commands, 2);
    ctx.close().await.unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
}
