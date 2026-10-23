//! Directory capture, retained replay and byte checkpoints through the real writer.
use super::*;

type NoteReadings = Vec<(String, Value, i64, Vec<u8>, i64)>;

async fn current_notes(conn: &mut PgConnection) -> NoteReadings {
    sqlx::query_as("SELECT e.payload->>'path', e.payload->'text', e.anchor, e.record_hash, e.reading_order FROM core.live_equivalence l JOIN core.events e ON e.id=l.event_id WHERE e.event_type='note.document' ORDER BY e.payload->>'path'")
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
                binding: Some(name.clone()),
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
    let target: Option<uuid::Uuid> = sqlx::query_scalar("SELECT e.id FROM core.live_equivalence l JOIN core.events e ON e.id=l.event_id WHERE e.event_type='note.document' AND e.payload->>'path'='c.md'")
        .fetch_optional(&mut ctx.read_conn).await.unwrap();
    let excised = match target {
        Some(id) => {
            crate::excision::excise_with(
                &mut ctx.read_conn,
                &ctx.blobs,
                &ctx.writer,
                sinex_store::excision::Target::Event(sinex_model::EventId(id)),
                "synthetic",
            )
            .await
        }
        None => Err("no nonempty sibling to excise".into()),
    };
    let replay_after_excision = crate::replay::run(
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
    let after_excision = current_notes(&mut ctx.read_conn).await;
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
        vec![("a.md", Value::Null), ("b.md", Value::Null)]
    );
    assert!(excised.is_ok(), "nonempty sibling excision: {excised:?}");
    assert!(
        replay_after_excision.is_ok(),
        "replay after sibling excision: {replay_after_excision:?}"
    );
    assert_eq!(
        after_excision
            .iter()
            .map(|r| (&*r.0, r.1.clone()))
            .collect::<Vec<_>>(),
        vec![("a.md", json!("")), ("b.md", json!(""))]
    );
}

#[tokio::test]
async fn legacy_empty_tail_commits_with_its_shared_checkpoint_and_retries_exactly() {
    use sinex_model::{
        BatchKind, InputRef, MaterialId, ObservationFacts, ReadingOrder, RevisionChange,
    };
    use sinex_sources::{
        Limits,
        drivers::{Acquired, NewMaterial},
        sources::notes,
    };
    let (_db, url, _) = crate::testdb::fresh().await;
    let dir = std::env::temp_dir().join(format!("sinex-legacy-tail-{}", uuid::Uuid::now_v7()));
    let mut ctx = OneShot::open(&url, &dir).await.unwrap();
    let sink = Sink(ctx.blobs.clone());
    let source = notes::source();
    let id = MaterialId::new();
    let manifest = json!({"path":"synthetic-vault", "media_type":"application/x-sinex-file-bundle", "read_at_ns":1, "scan_complete":true,
    "records":[[0,5],[5,5]], "members":[
        {"path":"a.md","stream_offset":0,"length":5}, {"path":"b.md","stream_offset":5,"length":0}
    ]});
    let acquired = |id: MaterialId, manifest: Value| Acquired {
        image: None,
        input: InputRef {
            kind: "directory".into(),
            locator: "synthetic-vault".into(),
            host: "synthetic".into(),
        },
        observation: ObservationFacts::default(),
        revision: RevisionChange::Snapshot,
        material: Some(NewMaterial {
            id,
            bytes: b"alpha".to_vec(),
            stored: None,
            records: vec![(0, 5), (5, 5)],
            part_index: 0,
            previous_part: None,
            manifest,
            reading_order: Box::new(|_| ReadingOrder(1)),
            cursor_at: Box::new(
                move |end| json!({"material":id,"part_index":0,"last_complete_scan_ns":1,"through":end}),
            ),
            aligned: false,
            vanished: vec![],
        }),
        cursor_unchanged: Value::Null,
    };
    crate::backfill::record(
        &mut ctx.read_conn,
        &Binding {
            name: "legacy-tail".into(),
            source: notes::SPEC.name.into(),
            parser: notes::SPEC.parser.into(),
            input: "synthetic-vault".into(),
            settings: Settings::new("synthetic"),
        },
    )
    .await
    .unwrap();
    for attempt in 0..2 {
        let prepared = drivers::prepare(
            &source,
            "legacy-tail",
            RunId::new(),
            acquired(id, manifest.clone()),
            Value::Null,
            &sink,
            Limits {
                max_events: 1,
                max_bytes: 1,
            },
        )
        .unwrap();
        let mut batches = prepared.batches().unwrap();
        let first = batches.next().unwrap().unwrap();
        assert_eq!(
            first.event_count(),
            2,
            "no checkpoint before its same-end tail"
        );
        let BatchKind::Source(progress) = &first.kind else {
            panic!()
        };
        assert_eq!(progress.cursor["through"], 5);
        let result = ctx
            .writer
            .submit(crate::writer::Priority::Bulk, first)
            .await
            .unwrap();
        assert_eq!(result.admitted, if attempt == 0 { 2 } else { 0 });
        assert!(batches.next().is_none());
        let (cursor, _) = positions::load(&mut ctx.read_conn, "legacy-tail")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(cursor["through"], 5);
        assert_eq!(current_notes(&mut ctx.read_conn).await.len(), 2);
    }
    let before = current_notes(&mut ctx.read_conn).await;
    let replay = crate::replay::run(
        &mut ctx.read_conn,
        &ctx.blobs,
        &ctx.writer,
        crate::replay::Options {
            scope: crate::replay::Scope {
                binding: Some("legacy-tail".into()),
                ..Default::default()
            },
            compare: false,
            resume: None,
            actor: "synthetic".into(),
            authority: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(replay.events_read, 2);
    assert_eq!(current_notes(&mut ctx.read_conn).await, before);
    assert_eq!(before[0].0, "a.md");
    assert_eq!(before[0].1, json!("alpha"));
    assert_eq!(before[0].2, 0);
    assert_eq!(before[1].0, "b.md");
    assert_eq!(before[1].1, json!(""));
    assert_eq!(before[1].2, 5);
    let old_binding = "legacy-preexisting-checkpoint";
    crate::backfill::record(
        &mut ctx.read_conn,
        &Binding {
            name: old_binding.into(),
            source: notes::SPEC.name.into(),
            parser: notes::SPEC.parser.into(),
            input: "synthetic-vault".into(),
            settings: Settings::new("synthetic"),
        },
    )
    .await
    .unwrap();
    let old_id = MaterialId::new();
    let mut old_manifest = manifest.clone();
    old_manifest["members"][0]["path"] = json!("old-c.md");
    old_manifest["members"][1]["path"] = json!("old-d.md");
    let mut old_source = notes::source();
    old_source.spec.parser_version = 1;
    let prepared = drivers::prepare(
        &old_source,
        old_binding,
        RunId::new(),
        acquired(old_id, old_manifest),
        Value::Null,
        &sink,
        Limits {
            max_events: 1,
            max_bytes: 1,
        },
    )
    .unwrap();
    let mut old_batch = prepared.batches().unwrap().next().unwrap().unwrap();
    // Seed the former writer's persisted state: its scalar N checkpoint and
    // complete retained manifest, but only the preceding nonempty record.
    old_batch.records.truncate(1);
    assert_eq!(
        ctx.writer
            .submit(crate::writer::Priority::Bulk, old_batch)
            .await
            .unwrap()
            .admitted,
        1
    );
    drop(prepared);
    let (old_cursor, _) = positions::load(&mut ctx.read_conn, old_binding)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(old_cursor["through"], 5);
    let old_before: Vec<_> = current_notes(&mut ctx.read_conn)
        .await
        .into_iter()
        .filter(|r| r.0.starts_with("old-"))
        .collect();
    assert_eq!(old_before.len(), 1);
    let recovered = crate::replay::run(
        &mut ctx.read_conn,
        &ctx.blobs,
        &ctx.writer,
        crate::replay::Options {
            scope: crate::replay::Scope {
                binding: Some(old_binding.into()),
                ..Default::default()
            },
            compare: false,
            resume: None,
            actor: "synthetic".into(),
            authority: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(recovered.events_read, 2);
    let old_after: Vec<_> = current_notes(&mut ctx.read_conn)
        .await
        .into_iter()
        .filter(|r| r.0.starts_with("old-"))
        .collect();
    assert_eq!(old_after.len(), 2);
    assert_eq!(old_after[0], old_before[0]);
    assert_eq!(old_after[1].0, "old-d.md");
    assert_eq!(old_after[1].1, json!(""));
    assert_eq!(old_after[1].2, 5);
    let at_original_material: i64 = sqlx::query_scalar("SELECT count(*) FROM core.live_equivalence l JOIN core.events e ON e.id=l.event_id WHERE e.event_type='note.document' AND e.payload->>'path' IN ('old-c.md','old-d.md') AND e.material_id=$1")
        .bind(old_id.0).fetch_one(&mut ctx.read_conn).await.unwrap();
    assert_eq!(
        at_original_material, 2,
        "retained replay keeps the existing legacy material and anchors"
    );
    ctx.close().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}
