//! Directory capture, replay of stored bytes and byte checkpoints through the
//! real writer.
use super::*;

type NoteReadings = Vec<(String, Value, i64, Vec<u8>, i64)>;

async fn current_notes(conn: &mut PgConnection) -> NoteReadings {
    sqlx::query_as("SELECT e.payload->>'path', e.payload->'text', e.anchor, e.record_hash, e.reading_order FROM core.live_equivalence l JOIN core.events e ON e.id=l.event_id WHERE e.event_type='note.document' ORDER BY e.payload->>'path'")
        .fetch_all(conn).await.unwrap()
}

#[devkit::test]
async fn empty_directory_files_keep_distinct_paths_bytes_and_locations_through_replay(
    db: devkit::pg::TestDatabase,
) {
    use sinex_sources::sources::notes;
    let url = db.url(None);
    let dir = std::env::temp_dir().join(format!("sinex-empty-notes-{}", uuid::Uuid::now_v7()));
    let root = dir.join("vault");
    std::fs::create_dir_all(&root).unwrap();
    for (name, bytes) in [("a.md", ""), ("b.md", ""), ("c.md", "gamma")] {
        std::fs::write(root.join(name), bytes).unwrap();
    }
    let mut ctx = ReaderContext::open(&url, &dir.join("store")).await.unwrap();
    let sink = Sink(ctx.blobs.clone());
    let binding = Binding::of(&notes::SPEC, &root, Settings::new("synthetic"));
    let name = binding.name.clone();
    let mut b = BindingReader::new(&notes::FORMAT, binding).unwrap();
    b.source = Arc::new(SmallBatches(b.source.clone()));
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
        &mut |_, _| {},
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
            unlock: None,
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
        &mut |_, _| {},
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

mod restart;
