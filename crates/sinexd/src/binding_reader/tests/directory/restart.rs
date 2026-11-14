use super::*;
use crate::Result;
use sinex_model::{BatchKind, MaterialRef};
use sinex_source_core::Limits;
use sinex_sources::sources::notes;

#[devkit::test]
async fn restarted_directory_reader_observes_files_removed_after_its_committed_capture(
    db: devkit::pg::TestDatabase,
) {
    use sinex_sources::sources::notes;
    let url = db.url(None);
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("vault");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("a.md"), "alpha").unwrap();
    std::fs::write(root.join("b.md"), "beta").unwrap();
    let mut ctx = ReaderContext::open(
        &url,
        &dir.path().join("store"),
        sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0)
            .expect("fixture day setting"),
    )
    .await
    .unwrap();
    let sink = Sink(ctx.blobs.clone());
    let binding = Binding::of(&notes::SPEC, &root, Settings::new("synthetic"));
    let mut readers = vec![BindingReader::new(&notes::FORMAT, binding.clone()).unwrap()];
    let (_stop, stop) = watch::channel(false);
    let result = read(
        &mut ctx.read_conn,
        &ctx.blobs,
        &ctx.writer,
        &sink,
        &mut readers,
        &mut crate::binding_reader::Rounds::default(),
        stop.clone(),
        Access::default(),
        &mut |_, _| {},
    )
    .await;
    for (_, result) in result {
        result.unwrap();
    }
    assert_eq!(current_notes(&mut ctx.read_conn).await.len(), 2);
    drop(readers);
    std::fs::remove_file(root.join("a.md")).unwrap();
    let mut restarted = vec![BindingReader::new(&notes::FORMAT, binding).unwrap()];
    let result = read(
        &mut ctx.read_conn,
        &ctx.blobs,
        &ctx.writer,
        &sink,
        &mut restarted,
        &mut crate::binding_reader::Rounds::default(),
        stop,
        Access::default(),
        &mut |_, _| {},
    )
    .await;
    for (_, result) in result {
        result.unwrap();
    }
    let notes = current_notes(&mut ctx.read_conn).await;
    let states = note_states(&mut ctx.read_conn).await;
    ctx.close().await.unwrap();
    assert_eq!(
        notes
            .iter()
            .map(|r| (&*r.0, r.1.clone()))
            .collect::<Vec<_>>(),
        vec![("b.md", json!("beta"))]
    );
    assert_eq!(
        states,
        vec![
            ("a.md".into(), "removed".into()),
            ("b.md".into(), "present".into()),
        ]
    );
}

async fn turn(
    ctx: &mut ReaderContext,
    readers: &mut [BindingReader],
    access: Access<'_>,
) -> Result<()> {
    let sink = Sink(ctx.blobs.clone());
    let (_stop, stop) = watch::channel(false);
    for (_, result) in read(
        &mut ctx.read_conn,
        &ctx.blobs,
        &ctx.writer,
        &sink,
        readers,
        &mut crate::binding_reader::Rounds::default(),
        stop,
        access,
        &mut |_, _| {},
    )
    .await
    {
        result?;
    }
    Ok(())
}

async fn prefix(
    ctx: &mut ReaderContext,
    b: &mut BindingReader,
    sink: &dyn drivers::MaterialSink,
    sealing: Option<&crate::material_access::sealing::SealingSink>,
) -> MaterialRef {
    crate::stage::record(&mut ctx.read_conn, &b.binding)
        .await
        .unwrap();
    let (cursor, carry) = positions::load(&mut ctx.read_conn, &b.binding.name)
        .await
        .unwrap()
        .unwrap_or((Value::Null, Value::Null));
    let acquired = polled(b).poll(&cursor).unwrap();
    let prepared = drivers::prepare(
        b.source.as_ref(),
        &b.binding.name,
        RunId::new(),
        acquired,
        carry,
        sink,
        Limits {
            max_events: 1,
            max_bytes: 1,
        },
    )
    .unwrap();
    let batch = prepared.batches().unwrap().next().unwrap().unwrap();
    let mut one = [batch];
    if let Some(sealing) = sealing {
        sealing.seal(&mut one).unwrap();
    }
    let BatchKind::Source(progress) = &one[0].kind else {
        panic!()
    };
    let reference = progress.material.as_ref().unwrap().clone();
    let [batch] = one;
    ctx.writer
        .submit(crate::writer::Priority::Bulk, batch)
        .await
        .unwrap();
    reference
}

#[devkit::test]
async fn sealed_history_needs_no_private_key_and_keeps_the_binding_sealed(
    db: devkit::pg::TestDatabase,
) {
    let url = db.url(None);
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("vault");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("a.md"), "synthetic sealed alpha").unwrap();
    let identity = sinex_sealing::Identity::generate();
    let recipient = identity.to_public();
    let mut ctx = ReaderContext::open(
        &url,
        &dir.path().join("store"),
        sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0)
            .expect("fixture day setting"),
    )
    .await
    .unwrap();
    sinex_store::privacy::set_source_class(
        &mut ctx.read_conn,
        notes::SPEC.name,
        Class::Sealed,
        "synthetic",
    )
    .await
    .unwrap();
    let binding = Binding::of(&notes::SPEC, &root, Settings::new("synthetic"));
    let mut readers = vec![BindingReader::new(&notes::FORMAT, binding.clone()).unwrap()];
    turn(
        &mut ctx,
        &mut readers,
        Access {
            recipient: Some(&recipient),
            unlock: None,
        },
    )
    .await
    .unwrap();
    drop(readers);
    std::fs::remove_file(root.join("a.md")).unwrap();
    let mut restarted = vec![BindingReader::new(&notes::FORMAT, binding.clone()).unwrap()];
    turn(
        &mut ctx,
        &mut restarted,
        Access {
            recipient: Some(&recipient),
            unlock: None,
        },
    )
    .await
    .unwrap();
    let states:Vec<(String,String)>=sqlx::query_as("SELECT record_state,class FROM core.events WHERE event_type='note.document' ORDER BY commit_seq").fetch_all(&mut ctx.read_conn).await.unwrap();
    assert!(states.iter().any(|r| r.0 == "removed"));
    assert!(states.iter().all(|r| r.1 == "sealed"));
    sinex_store::privacy::set_source_class(
        &mut ctx.read_conn,
        notes::SPEC.name,
        Class::Open,
        "synthetic",
    )
    .await
    .unwrap();
    drop(restarted);
    // Sealed history keeps the binding sealed after its class is lowered,
    // and remembering it needs no private key.
    let mut lowered = vec![BindingReader::new(&notes::FORMAT, binding.clone()).unwrap()];
    std::fs::write(
        root.join("b.md"),
        "synthetic beta after the class was lowered",
    )
    .unwrap();
    turn(
        &mut ctx,
        &mut lowered,
        Access {
            recipient: Some(&recipient),
            unlock: None,
        },
    )
    .await
    .unwrap();
    let open: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM core.events WHERE event_type='note.document' AND class<>'sealed'",
    )
    .fetch_one(&mut ctx.read_conn)
    .await
    .unwrap();
    assert_eq!(open, 0);
    ctx.close().await.unwrap();
}

struct IgnoreFirst;
impl sinex_source_core::Parser for IgnoreFirst {
    type Record<'a> = &'a [u8];
    type Carry = ();
    type Payload = sinex_model::event_types::media::NoteDocument;
    fn record_hash(&self, raw: &[u8], context: &sinex_source_core::RecordContext<'_>) -> [u8; 32] {
        sinex_source_core::Parser::record_hash(&notes::NotesParser, raw, context)
    }
    fn parse(
        &self,
        record: &&[u8],
        carry: &(),
        context: &sinex_source_core::drain::RecordContext<'_>,
    ) -> Result<(Vec<Self::Payload>, ()), sinex_source_core::RecordError> {
        if context
            .member
            .is_some_and(|m| m.path == "a.md" && !m.details.contains_key("removed_at_ns"))
        {
            return Ok((Vec::new(), ()));
        }
        sinex_source_core::Parser::parse(&notes::NotesParser, record, carry, context)
    }
}
fn ignore_first(b: &mut BindingReader) {
    let mut spec = notes::SPEC;
    spec.parser_version = 99;
    b.source = Arc::new(sinex_source_core::DeclaredSource {
        spec,
        parser: IgnoreFirst,
    });
}

#[devkit::test]
async fn a_restart_remembers_a_file_whose_record_produced_no_event(db: devkit::pg::TestDatabase) {
    let url = db.url(None);
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("vault");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("a.md"), "ignored alpha").unwrap();
    std::fs::write(root.join("b.md"), "beta").unwrap();
    let mut ctx = ReaderContext::open(
        &url,
        &dir.path().join("store"),
        sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0)
            .expect("fixture day setting"),
    )
    .await
    .unwrap();
    // The synthetic parser is a real owner-installed version, with the same
    // immutable emitted schemas and a distinct parser version.
    let mut declaration = crate::catalog::source_declarations()
        .into_iter()
        .find(|declaration| {
            declaration.source.name == notes::SPEC.name
                && declaration.source.version == notes::SPEC.parser_version
        })
        .unwrap();
    declaration.source.version = 99;
    sinex_store::source_declarations::register(&mut ctx.read_conn, &[declaration])
        .await
        .unwrap();
    let sink = Sink(ctx.blobs.clone());
    let binding = Binding::of(&notes::SPEC, &root, Settings::new("synthetic"));
    let mut b = BindingReader::new(&notes::FORMAT, binding.clone()).unwrap();
    ignore_first(&mut b);
    prefix(&mut ctx, &mut b, &sink, None).await;
    let checkpoint = positions::load(&mut ctx.read_conn, &binding.name)
        .await
        .unwrap()
        .unwrap();
    assert!(checkpoint.0["through"].as_u64().unwrap() > 0);
    drop(b);
    std::fs::remove_file(root.join("a.md")).unwrap();
    let mut restarted = vec![BindingReader::new(&notes::FORMAT, binding).unwrap()];
    ignore_first(&mut restarted[0]);
    turn(&mut ctx, &mut restarted, Access::default())
        .await
        .unwrap();
    assert_eq!(
        current_notes(&mut ctx.read_conn)
            .await
            .iter()
            .map(|r| (&*r.0, r.1.clone()))
            .collect::<Vec<_>>(),
        vec![("b.md", json!("beta"))]
    );
    let removed: i64 = sqlx::query_scalar("SELECT count(*) FROM raw.materials m, jsonb_array_elements(m.manifest->'members') member WHERE member->>'path'='a.md' AND member ? 'removed_at_ns'")
        .fetch_one(&mut ctx.read_conn).await.unwrap();
    assert_eq!(
        removed, 1,
        "the reader remembers an ignored member and records its disappearance"
    );
    assert_eq!(
        note_states(&mut ctx.read_conn).await,
        vec![("b.md".into(), "present".into())]
    );
    ctx.close().await.unwrap();
}

#[devkit::test]
async fn stopped_directory_restore_keeps_progress_and_can_be_retried(db: devkit::pg::TestDatabase) {
    let url = db.url(None);
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("vault");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("a.md"), "alpha").unwrap();
    let mut ctx = ReaderContext::open(
        &url,
        &dir.path().join("store"),
        sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0)
            .expect("fixture day setting"),
    )
    .await
    .unwrap();
    let sink = Sink(ctx.blobs.clone());
    let binding = Binding::of(&notes::SPEC, &root, Settings::new("synthetic"));
    let mut readers = vec![BindingReader::new(&notes::FORMAT, binding.clone()).unwrap()];
    turn(&mut ctx, &mut readers, Access::default())
        .await
        .unwrap();
    drop(readers);
    let checkpoint = positions::load(&mut ctx.read_conn, &binding.name)
        .await
        .unwrap()
        .unwrap();
    std::fs::remove_file(root.join("a.md")).unwrap();
    let mut restarted = vec![BindingReader::new(&notes::FORMAT, binding.clone()).unwrap()];
    let (_stop, stop) = watch::channel(true);
    let result = read(
        &mut ctx.read_conn,
        &ctx.blobs,
        &ctx.writer,
        &sink,
        &mut restarted,
        &mut crate::binding_reader::Rounds::default(),
        stop,
        Access::default(),
        &mut |_, _| {},
    )
    .await;
    assert!(
        result[0]
            .1
            .as_ref()
            .is_err_and(|e| e.to_string().contains("cancelled"))
    );
    assert_eq!(
        positions::load(&mut ctx.read_conn, &binding.name)
            .await
            .unwrap()
            .unwrap(),
        checkpoint
    );
    turn(&mut ctx, &mut restarted, Access::default())
        .await
        .unwrap();
    assert!(current_notes(&mut ctx.read_conn).await.is_empty());
    assert_eq!(
        note_states(&mut ctx.read_conn).await,
        vec![("a.md".into(), "removed".into())]
    );
    ctx.close().await.unwrap();
}

#[devkit::test]
async fn a_removed_guarded_member_keeps_its_class_after_the_source_floor_is_lowered(
    db: devkit::pg::TestDatabase,
) {
    let url = db.url(None);
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("vault");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("a.md"), "synthetic guarded alpha").unwrap();
    let mut ctx = ReaderContext::open(
        &url,
        &dir.path().join("store"),
        sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0)
            .expect("fixture day setting"),
    )
    .await
    .unwrap();
    sinex_store::privacy::set_source_class(
        &mut ctx.read_conn,
        notes::SPEC.name,
        Class::Guarded,
        "synthetic",
    )
    .await
    .unwrap();
    let binding = Binding::of(&notes::SPEC, &root, Settings::new("synthetic"));
    let mut readers = vec![BindingReader::new(&notes::FORMAT, binding.clone()).unwrap()];
    turn(&mut ctx, &mut readers, Access::default())
        .await
        .unwrap();
    drop(readers);
    sinex_store::privacy::set_source_class(
        &mut ctx.read_conn,
        notes::SPEC.name,
        Class::Open,
        "synthetic",
    )
    .await
    .unwrap();
    std::fs::remove_file(root.join("a.md")).unwrap();
    let mut restarted = vec![BindingReader::new(&notes::FORMAT, binding).unwrap()];
    turn(&mut ctx, &mut restarted, Access::default())
        .await
        .unwrap();
    let class: String = sqlx::query_scalar(
        "SELECT class FROM core.events WHERE event_type='note.document' AND record_state='removed'",
    )
    .fetch_one(&mut ctx.read_conn)
    .await
    .unwrap();
    assert_eq!(class, "guarded");
    let shown: Value = sqlx::query_scalar("SELECT payload FROM core.visible_events WHERE event_type='note.document' AND record_state='removed'").fetch_one(&mut ctx.read_conn).await.unwrap();
    assert_eq!(shown, json!({"withheld":"guarded"}));
    ctx.close().await.unwrap();
}

#[devkit::test]
async fn predecessor_directory_checkpoint_upgrades_without_new_material_and_survives_restart(
    db: devkit::pg::TestDatabase,
) {
    historical_checkpoint_roundtrip(db, false).await;
}

#[devkit::test]
async fn predecessor_saved_memory_recovers_original_removal_locations_from_older_parts(
    db: devkit::pg::TestDatabase,
) {
    historical_checkpoint_roundtrip(db, true).await;
}

async fn historical_checkpoint_roundtrip(db: devkit::pg::TestDatabase, removed_at_source: bool) {
    use sinex_model::{InputRef, MaterialId, ObservationFacts, ReaderMemory, RevisionChange};
    use sinex_source_core::drivers::{Acquired, NewMaterial};
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("vault");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("a.md"), b"alpha").unwrap();
    std::fs::write(root.join("b.md"), b"beta").unwrap();
    std::fs::write(root.join("c.md"), b"gamma").unwrap();
    let mut ctx = ReaderContext::open(
        &db.url(None),
        &dir.path().join("store"),
        sinex_model::time_settings::TimeSettings::new("UTC", 0, 0).unwrap(),
    )
    .await
    .unwrap();
    let sink = Sink(ctx.blobs.clone());
    let binding = Binding::of(&notes::SPEC, &root, Settings::new("synthetic"));
    let new_reader = || {
        let mut reader = BindingReader::new(&notes::FORMAT, binding.clone()).unwrap();
        if removed_at_source {
            reader.reading = sinex_source_core::binding::walked(
                sinex_source_core::drivers::dir_snapshot::DirSnapshot {
                    root: root.clone(),
                    host: "synthetic".into(),
                    select: |name| name.ends_with(".md"),
                    part_bytes: 4096,
                    removed_at_source: true,
                },
            );
        }
        reader
    };
    let mut b = new_reader();
    crate::stage::record(&mut ctx.read_conn, &binding)
        .await
        .unwrap();
    let first_id = MaterialId::new();
    let anchor_id = MaterialId::new();
    let last_id = MaterialId::new();
    let mut old_memory = Vec::new();
    for (path, bytes) in [
        (b"a.md", b"alpha".as_slice()),
        (b"b.md", b"beta".as_slice()),
    ] {
        old_memory.extend(4u32.to_le_bytes());
        old_memory.extend(path);
        old_memory.push(1);
        old_memory.extend((bytes.len() as u64).to_le_bytes());
        old_memory.extend((-17i128).to_le_bytes());
        old_memory.extend(blake3::hash(bytes).as_bytes());
    }
    for (index, id, path, text) in [
        (0u32, first_id, "a.md", "alpha"),
        (1, anchor_id, "b.md", "beta"),
        (2, last_id, "c.md", "gamma"),
    ] {
        let mut bytes = vec![0];
        bytes.extend(text.as_bytes());
        let length = bytes.len();
        let cursor = json!({"material":id,"through":length,"part_index":index,
            "memory_part":1,"changed_since_memory":index.saturating_sub(1),"last_complete_scan_ns":1});
        let acquired = Acquired {
            memory: (index == 1).then(|| ReaderMemory {
                material: id,
                bytes: old_memory.clone().into(),
            }),
            input: InputRef {
                kind: "directory".into(),
                locator: root.display().to_string(),
                host: "synthetic".into(),
            },
            observation: ObservationFacts {
                changed: true,
                ..Default::default()
            },
            revision: RevisionChange::Snapshot,
            material: Some(NewMaterial {
                id,
                bytes,
                stored: None,
                records: vec![(1, length)],
                part_index: index,
                previous_part: match index {
                    0 => None,
                    1 => Some(first_id),
                    _ => Some(anchor_id),
                },
                manifest: sinex_model::Manifest::from_json(
                    json!({"media_type":"application/x-sinex-file-bundle",
                    "path":root.display().to_string(),"host":"synthetic","scan_complete":false,
                    "previous_through":match index {0=>None,1=>Some(6),_=>Some(5)},
                    "members":[{"path":path,"stream_offset":1,"length":text.len(),"mtime_ns":"-17",
                        "hash":blake3::hash(text.as_bytes()).to_hex().to_string()}],"records":[[1,length]]}),
                ),
                reading_order: Box::new(move |_| sinex_model::ReadingOrder(index as i64)),
                cursor_at: Box::new(move |_| Ok(cursor.clone())),
                aligned: false,
                removed: Vec::new(),
                owned_files: Vec::new(),
            }),
            cursor_without_material: Value::Null,
        };
        let prepared = drivers::prepare(
            b.source.as_ref(),
            &binding.name,
            RunId::new(),
            acquired,
            Value::Null,
            &sink,
            Limits::default(),
        )
        .unwrap();
        for batch in prepared.batches().unwrap() {
            ctx.writer
                .submit(crate::writer::Priority::Bulk, batch.unwrap())
                .await
                .unwrap();
        }
    }
    let before = positions::load(&mut ctx.read_conn, &binding.name)
        .await
        .unwrap()
        .unwrap();
    let before_materials: Vec<(uuid::Uuid, String, Value)> =
        sqlx::query_as("SELECT id,blob,manifest FROM raw.materials ORDER BY id")
            .fetch_all(&mut ctx.read_conn)
            .await
            .unwrap();
    let (_stop, stop) = watch::channel(false);
    restore(
        &mut ctx.read_conn,
        &ctx.blobs,
        &mut b,
        Access::default(),
        &stop,
    )
    .await
    .unwrap();
    let acquired = polled(&mut b).poll(&before.0).unwrap();
    assert!(acquired.material.is_none());
    let checkpoint = acquired.memory.as_ref().unwrap().clone();
    assert_eq!(checkpoint.material, last_id);
    assert_eq!(
        acquired.cursor_without_material["material"],
        before.0["material"]
    );
    assert_eq!(
        acquired.cursor_without_material["through"],
        before.0["through"]
    );
    let prepared = drivers::prepare(
        b.source.as_ref(),
        &binding.name,
        RunId::new(),
        acquired,
        before.1.clone(),
        &sink,
        Limits::default(),
    )
    .unwrap();
    for batch in prepared.batches().unwrap() {
        ctx.writer
            .submit(crate::writer::Priority::Bulk, batch.unwrap())
            .await
            .unwrap();
    }
    drop(prepared);
    // The database committed; simulate losing that ACK before notifying the reader.
    assert!(polled(&mut b).poll(&before.0).unwrap().memory.is_some());
    drop(b);
    let saved = positions::reader_memory(&mut ctx.read_conn, &binding.name)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(saved.material, last_id);
    assert_eq!(saved.bytes.as_ref(), checkpoint.bytes.as_ref());
    let mut readers = [new_reader()];
    turn(&mut ctx, &mut readers, Access::default())
        .await
        .unwrap();
    let after_materials: Vec<(uuid::Uuid, String, Value)> =
        sqlx::query_as("SELECT id,blob,manifest FROM raw.materials ORDER BY id")
            .fetch_all(&mut ctx.read_conn)
            .await
            .unwrap();
    assert_eq!(
        after_materials, before_materials,
        "equal bytes must not create or relabel material"
    );
    let after = positions::load(&mut ctx.read_conn, &readers[0].binding.name)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after.0["material"], before.0["material"]);
    assert_eq!(after.0["through"], before.0["through"]);
    assert_eq!(after.1, before.1);
    std::fs::remove_file(root.join("a.md")).unwrap();
    turn(&mut ctx, &mut readers, Access::default())
        .await
        .unwrap();
    assert_eq!(
        note_states(&mut ctx.read_conn).await,
        [
            ("a.md".into(), "removed".into()),
            ("b.md".into(), "present".into()),
            ("c.md".into(), "present".into())
        ]
    );
    if removed_at_source {
        let removed: (uuid::Uuid, i64) = sqlx::query_as("SELECT material_id,anchor FROM core.events WHERE event_type='note.document' AND record_state='removed'")
            .fetch_one(&mut ctx.read_conn).await.unwrap();
        assert_eq!(removed, (first_id.0, 1));
    }
    ctx.close().await.unwrap();
}
