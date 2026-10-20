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
