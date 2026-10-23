use super::*;
use crate::one_shot::OneShot;
use serde_json::json;
use sinex_sources::{
    Settings,
    binding::stateless,
    sources::google_takeout::{self, fit, tasks},
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

fn bound(path: &std::path::Path) -> Vec<Bound> {
    let settings = Settings::new("synthetic");
    [fit::SPEC, tasks::SPEC]
        .iter()
        .map(|spec| {
            let binding = Binding::of(spec, path, settings.clone());
            let mut b = Bound::new(&google_takeout::KIND, binding).unwrap();
            // Force several ordinary parts without changing the production limits.
            let mut driver = b.reader.export_archive().unwrap().clone();
            driver.part_bytes = 400;
            b.reader = stateless(driver);
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

#[tokio::test]
async fn archive_acquisition_restart_replay_and_privacy_use_real_admission() {
    let (_db, url, _) = crate::testdb::fresh().await;
    let dir = std::env::temp_dir().join(format!("sinex-archive-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("export.tgz");
    let decoded = archive(&path);
    let mut ctx = OneShot::open(&url, &dir.join("store")).await.unwrap();
    let sink = Sink(ctx.blobs.clone());
    let (_stop, stop) = watch::channel(false);
    let mut bindings = bound(&path);
    let result = read(
        &mut ctx.read_conn,
        &ctx.blobs,
        &ctx.writer,
        RunId::new(),
        &sink,
        &mut bindings,
        stop.clone(),
        Access::default(),
    )
    .await;
    let mut total = Totals::default();
    for (_, r) in result {
        total.add(r.unwrap());
    }
    let metrics = total.to_json();
    assert_eq!(metrics["archive_source_opens"], 1);
    assert_eq!(metrics["archive_captures"], 1);
    assert!(metrics["archive_decoded_bytes"].as_u64().unwrap() <= decoded as u64);
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
    let extents: Vec<String> = sqlx::query_scalar("SELECT blob FROM raw.revision_extents")
        .fetch_all(&mut ctx.read_conn)
        .await
        .unwrap();
    assert_eq!(
        extents,
        vec![originals[0].1.clone(), originals[0].1.clone()],
        "only original input bytes belong to the revision stream"
    );
    let materials: i64 = sqlx::query_scalar("SELECT count(*) FROM raw.materials")
        .fetch_one(&mut ctx.read_conn)
        .await
        .unwrap();
    // Fresh reader objects model a daemon restart. A completed cursor does no physical work.
    let mut restarted = bound(&path);
    let resumed = read(
        &mut ctx.read_conn,
        &ctx.blobs,
        &ctx.writer,
        RunId::new(),
        &sink,
        &mut restarted,
        stop.clone(),
        Access::default(),
    )
    .await;
    for (_, result) in resumed {
        assert_eq!(result.unwrap().to_json()["archive_source_opens"], 0);
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
            authority: None,
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

#[tokio::test]
async fn archive_acquisition_separates_effective_privacy_classes() {
    let (_db, url, _) = crate::testdb::fresh().await;
    let dir = std::env::temp_dir().join(format!("sinex-archive-privacy-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("export.tgz");
    archive(&path);
    let mut ctx = OneShot::open(&url, &dir.join("store")).await.unwrap();
    sqlx::query("INSERT INTO privacy.source_classes(source,class,author) VALUES ('google.tasks','guarded','synthetic')").execute(&mut ctx.read_conn).await.unwrap();
    let sink = Sink(ctx.blobs.clone());
    let (_stop, stop) = watch::channel(false);
    let mut bindings = bound(&path);
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
    )
    .await
    {
        total.add(result.unwrap());
    }
    assert_eq!(total.to_json()["archive_source_opens"], 2);
    assert_eq!(total.to_json()["archive_captures"], 2);
    let classes: Vec<String> =
        sqlx::query_scalar("SELECT DISTINCT class FROM core.events WHERE source='google.tasks'")
            .fetch_all(&mut ctx.read_conn)
            .await
            .unwrap();
    assert_eq!(classes, vec!["guarded"]);
    ctx.close().await.unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn cancelled_archive_worker_joins_and_restart_catches_up() {
    let (_db, url, _) = crate::testdb::fresh().await;
    let dir = std::env::temp_dir().join(format!("sinex-archive-cancel-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("export.tgz");
    archive(&path);
    let mut ctx = OneShot::open(&url, &dir.join("store")).await.unwrap();
    let sink = Sink(ctx.blobs.clone());
    let (_stop, stop) = watch::channel(true);
    let mut bindings = bound(&path);
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
        ),
    )
    .await
    .expect("cancelled worker must join");
    assert!(cancelled.iter().all(|(_, result)| result.is_err()));
    let (_stop, stop) = watch::channel(false);
    let mut restarted = bound(&path);
    for (_, result) in read(
        &mut ctx.read_conn,
        &ctx.blobs,
        &ctx.writer,
        RunId::new(),
        &sink,
        &mut restarted,
        stop,
        Access::default(),
    )
    .await
    {
        result.unwrap();
    }
    assert_eq!(events(&mut ctx.read_conn).await.len(), 41);
    ctx.close().await.unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn legacy_cursor_is_refused_without_reinterpretation_or_blocking_another_source() {
    let (_db, url, _) = crate::testdb::fresh().await;
    let dir = std::env::temp_dir().join(format!("sinex-archive-legacy-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("export.tgz");
    archive(&path);
    let mut ctx = OneShot::open(&url, &dir.join("store")).await.unwrap();
    let mut bindings = bound(&path);
    let name = bindings[0].binding.name.clone();
    backfill::record(&mut ctx.read_conn, &bindings[0].binding)
        .await
        .unwrap();
    let legacy = json!({"size":123,"mtime_ns":456,"through":789,"material":uuid::Uuid::now_v7()});
    sqlx::query(
        "INSERT INTO raw.positions(binding,cursor,carry,committed_seq) VALUES ($1,$2,'null',0)",
    )
    .bind(&name)
    .bind(&legacy)
    .execute(&mut ctx.read_conn)
    .await
    .unwrap();
    let sink = Sink(ctx.blobs.clone());
    let (_stop, stop) = watch::channel(false);
    let outcomes = read(
        &mut ctx.read_conn,
        &ctx.blobs,
        &ctx.writer,
        RunId::new(),
        &sink,
        &mut bindings,
        stop,
        Access::default(),
    )
    .await;
    assert!(outcomes.iter().any(|(source, result)| {
        source == "google.fit"
            && result
                .as_ref()
                .is_err_and(|e| e.contains("explicit migration"))
    }));
    assert!(
        outcomes
            .iter()
            .any(|(source, result)| source == "google.tasks" && result.is_ok())
    );
    assert_eq!(
        positions::load(&mut ctx.read_conn, &name)
            .await
            .unwrap()
            .unwrap()
            .0,
        legacy
    );
    assert_eq!(events(&mut ctx.read_conn).await.len(), 1);
    ctx.close().await.unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
}

struct SmallBatches(Box<dyn Source>);
impl Source for SmallBatches {
    fn spec(&self) -> &sinex_sources::SourceSpec {
        self.0.spec()
    }
    fn drainer<'a>(
        &'a self,
        mut input: sinex_sources::DrainInput<'a>,
    ) -> Result<
        Box<
            dyn Iterator<Item = Result<sinex_sources::DrainOutput, sinex_sources::RecordError>>
                + Send
                + 'a,
        >,
        sinex_sources::RecordError,
    > {
        input.limits.max_events = 2;
        self.0.drainer(input)
    }
}

#[tokio::test]
async fn sealed_archive_restart_replay_and_class_changes_preserve_exact_materials() {
    for zip in [false, true] {
        let (_db, url, _) = crate::testdb::fresh().await;
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
        let mut ctx = OneShot::open(&url, &dir.join("store")).await.unwrap();
        sqlx::query("INSERT INTO privacy.source_classes(source,class,author) VALUES ('google.fit','sealed','synthetic')").execute(&mut ctx.read_conn).await.unwrap();
        let identity = sinex_store::sealed::Identity::generate();
        let recipient = identity.to_public();
        let sink = Sink(ctx.blobs.clone());
        let (_stop, stop) = watch::channel(false);
        let make_bound = || {
            let mut bindings = bound(&path);
            bindings.truncate(1);
            let b = &mut bindings[0];
            let mut driver = b.reader.export_archive().unwrap().clone();
            driver.part_bytes = 1 << 20;
            b.reader = stateless(driver);
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
                authority: None,
            },
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
        let cursor = Cursor::parse(&checkpoint.0).unwrap().unwrap();
        assert_eq!(cursor.record, 2);
        assert!(cursor.pending_material().is_some());
        assert!(sinex_store::sealed::is_sealed(&checkpoint.1));
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
                authority: None,
            },
        )
        .await;
        assert!(
            locked[0]
                .1
                .as_ref()
                .is_err_and(|e| e.contains("pending explicit operator unlock"))
        );
        assert_eq!(
            positions::load(&mut ctx.read_conn, &name)
                .await
                .unwrap()
                .unwrap(),
            checkpoint
        );
        let authority =
            crate::sealed_access::Authority::new(identity, std::time::Duration::from_secs(60))
                .unwrap();
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
                authority: Some(&authority),
            },
        )
        .await;
        for (_, result) in unlocked {
            let total = result.unwrap();
            assert_eq!(total.to_json()["archive_captures"], 0);
        }
        let final_cursor = Cursor::parse(
            &positions::load(&mut ctx.read_conn, &name)
                .await
                .unwrap()
                .unwrap()
                .0,
        )
        .unwrap()
        .unwrap();
        assert!(final_cursor.done);
        assert_eq!(final_cursor.material, Some(partial));
        assert_eq!(final_cursor.original_material, Some(original));
        let before = live_events(&mut ctx.read_conn).await;
        assert_eq!(before.len(), 40);
        let keys: Vec<String> = sqlx::query_scalar("SELECT key_ref FROM privacy.material_keys")
            .fetch_all(&mut ctx.read_conn)
            .await
            .unwrap();
        assert_eq!(keys.len(), 2);
        assert_ne!(keys[0], keys[1]);
        assert!(before.iter().all(|e| sinex_store::sealed::is_sealed(&e.2)));
        let scope = crate::replay::Scope {
            binding: Some(name.clone()),
            ..Default::default()
        };
        let replay = |authority| crate::replay::Options {
            scope: scope.clone(),
            compare: false,
            resume: None,
            actor: "operator".into(),
            authority,
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
            replay(Some(authority.clone())),
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
            let key = authority
                .material_key(&mut ctx.read_conn, sinex_model::MaterialId(old.3.unwrap()))
                .await
                .unwrap();
            assert_eq!(
                sinex_store::sealed::unseal_payload(&old.2, &key.key).unwrap(),
                sinex_store::sealed::unseal_payload(&new.2, &key.key).unwrap()
            );
        }
        authority.revoke();
        assert!(
            crate::replay::run(
                &mut ctx.read_conn,
                &ctx.blobs,
                &ctx.writer,
                replay(Some(authority))
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
        )
        .await;
        assert_eq!(
            result.into_iter().next().unwrap().1.unwrap().to_json()["archive_captures"],
            1
        );
        let cursor = Cursor::parse(
            &positions::load(&mut ctx.read_conn, &name)
                .await
                .unwrap()
                .unwrap()
                .0,
        )
        .unwrap()
        .unwrap();
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
                authority: None,
            },
        )
        .await;
        assert_eq!(
            result.into_iter().next().unwrap().1.unwrap().to_json()["archive_captures"],
            1
        );
        let cursor = Cursor::parse(
            &positions::load(&mut ctx.read_conn, &name)
                .await
                .unwrap()
                .unwrap()
                .0,
        )
        .unwrap()
        .unwrap();
        assert_eq!(cursor.capture.class, Class::Sealed);
        assert_ne!(cursor.original_material, Some(original));
        ctx.close().await.unwrap();
        std::fs::remove_dir_all(&dir).unwrap();
    }
}

#[tokio::test]
async fn sealed_bindings_never_share_material_keys_or_original_blob_names() {
    let (_db, url, _) = crate::testdb::fresh().await;
    let dir = std::env::temp_dir().join(format!("sinex-sealed-bindings-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("export.tgz");
    archive(&path);
    let mut ctx = OneShot::open(&url, &dir.join("store")).await.unwrap();
    sqlx::query("INSERT INTO privacy.source_classes(source,class,author) VALUES ('google.fit','sealed','synthetic'),('google.tasks','sealed','synthetic')").execute(&mut ctx.read_conn).await.unwrap();
    let identity = sinex_store::sealed::Identity::generate();
    let recipient = identity.to_public();
    let sink = Sink(ctx.blobs.clone());
    let (_stop, stop) = watch::channel(false);
    let mut bindings = bound(&path);
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
            authority: None,
        },
    )
    .await
    {
        result.unwrap();
    }
    let originals:Vec<(String,String)>=sqlx::query_as("SELECT m.blob,k.key_ref FROM raw.materials m JOIN privacy.material_keys k ON m.id=k.material_id WHERE m.manifest->>'role'='original-archive'").fetch_all(&mut ctx.read_conn).await.unwrap();
    assert_eq!(originals.len(), 2);
    assert_ne!(originals[0].0, originals[1].0);
    assert_ne!(originals[0].1, originals[1].1);
    ctx.close().await.unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
}

async fn live_events(conn: &mut PgConnection) -> Vec<EventFields> {
    sqlx::query_as("SELECT e.source,e.equivalence_key,e.payload,e.material_id,e.anchor FROM core.events e WHERE e.id IN (SELECT event_id FROM core.live_equivalence UNION SELECT event_id FROM core.live_locations) ORDER BY source,equivalence_key,material_id,anchor").fetch_all(conn).await.unwrap()
}

type NoteReadings = Vec<(String, Value, i64, Vec<u8>)>;

async fn current_notes(conn: &mut PgConnection) -> NoteReadings {
    sqlx::query_as("SELECT e.payload->>'path', e.payload->'text', e.anchor, e.record_hash FROM core.live_equivalence l JOIN core.events e ON e.id=l.event_id WHERE e.event_type='note.document' ORDER BY e.payload->>'path'")
        .fetch_all(conn).await.unwrap()
}

#[tokio::test]
async fn empty_directory_files_keep_distinct_paths_bytes_and_locations_through_replay() {
    use sinex_sources::sources::notes;
    let (_db, url, _) = crate::testdb::fresh().await;
    let dir = std::env::temp_dir().join(format!("sinex-empty-notes-{}", uuid::Uuid::now_v7()));
    let root = dir.join("vault");
    std::fs::create_dir_all(&root).unwrap();
    for (name, bytes) in [("a.md", ""), ("b.md", ""), ("c.md", "gamma")] {
        std::fs::write(root.join(name), bytes).unwrap();
    }
    let mut ctx = OneShot::open(&url, &dir.join("store")).await.unwrap();
    let sink = Sink(ctx.blobs.clone());
    let binding = Binding::of(&notes::SPEC, &root, Settings::new("synthetic"));
    let name = binding.name.clone();
    let mut b = Bound::new(&notes::KIND, binding).unwrap();
    b.source = Box::new(SmallBatches(b.source));
    let mut bindings = vec![b];
    let (_stop, stop) = watch::channel(false);
    let captured = read(
        &mut ctx.read_conn,
        &ctx.blobs,
        &ctx.writer,
        RunId::new(),
        &sink,
        &mut bindings,
        stop.clone(),
        Access::default(),
    )
    .await;
    let capture_results: Vec<_> = captured
        .into_iter()
        .map(|(_, r)| r.map(|t| t.to_json()))
        .collect();
    let before = current_notes(&mut ctx.read_conn).await;
    let replay = crate::replay::run(
        &mut ctx.read_conn,
        &ctx.blobs,
        &ctx.writer,
        crate::replay::Options {
            scope: crate::replay::Scope {
                binding: Some(name),
                ..Default::default()
            },
            compare: false,
            resume: None,
            actor: "synthetic".into(),
            authority: None,
        },
    )
    .await;
    let after = current_notes(&mut ctx.read_conn).await;
    std::fs::remove_file(root.join("a.md")).unwrap();
    std::fs::remove_file(root.join("b.md")).unwrap();
    let removed = read(
        &mut ctx.read_conn,
        &ctx.blobs,
        &ctx.writer,
        RunId::new(),
        &sink,
        &mut bindings,
        stop,
        Access::default(),
    )
    .await;
    let removal_results: Vec<_> = removed
        .into_iter()
        .map(|(_, r)| r.map(|t| t.to_json()))
        .collect();
    let final_notes = current_notes(&mut ctx.read_conn).await;
    ctx.close().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
    assert!(
        capture_results.iter().all(Result::is_ok),
        "capture: {capture_results:?}"
    );
    assert!(replay.is_ok(), "replay: {replay:?}");
    assert!(
        removal_results.iter().all(Result::is_ok),
        "removal: {removal_results:?}"
    );
    assert_eq!(
        before
            .iter()
            .map(|r| (&*r.0, r.1.clone()))
            .collect::<Vec<_>>(),
        vec![
            ("a.md", json!("")),
            ("b.md", json!("")),
            ("c.md", json!("gamma"))
        ],
        "ordinary capture: {before:?}; replay: {after:?}; removed: {final_notes:?}"
    );
    assert_eq!(
        after, before,
        "replay preserves paths, exact bodies, anchors and raw hashes"
    );
    let anchors: std::collections::BTreeSet<_> = before.iter().map(|r| r.2).collect();
    assert_eq!(anchors.len(), 3);
    assert_eq!(before[0].3, blake3::hash(b"").as_bytes());
    assert_eq!(before[1].3, blake3::hash(b"").as_bytes());
    assert_eq!(before[2].3, blake3::hash(b"gamma").as_bytes());
    assert_eq!(
        final_notes
            .iter()
            .map(|r| (&*r.0, r.1.clone()))
            .collect::<Vec<_>>(),
        vec![
            ("a.md", Value::Null),
            ("b.md", Value::Null),
            ("c.md", json!("gamma"))
        ]
    );
}
