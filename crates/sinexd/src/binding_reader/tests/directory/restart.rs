use super::*;
use sinex_model::{BatchKind, MaterialRef};
use sinex_sources::{Limits, sources::notes};

#[tokio::test]
async fn restarted_directory_reader_observes_files_removed_after_its_committed_capture() {
    use sinex_sources::sources::notes;
    let (_db, url, _) = crate::testdb::fresh().await;
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("vault");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("a.md"), "alpha").unwrap();
    std::fs::write(root.join("b.md"), "beta").unwrap();
    let mut ctx = OneShot::open(&url, &dir.path().join("store"))
        .await
        .unwrap();
    let sink = Sink(ctx.blobs.clone());
    let binding = Binding::of(&notes::SPEC, &root, Settings::new("synthetic"));
    let mut bounds = vec![Bound::new(&notes::KIND, binding.clone()).unwrap()];
    let (_stop, stop) = watch::channel(false);
    let result = read(
        &mut ctx.read_conn,
        &ctx.blobs,
        &ctx.writer,
        RunId::new(),
        &sink,
        &mut bounds,
        stop.clone(),
        Access::default(),
    )
    .await;
    for (_, result) in result {
        result.unwrap();
    }
    assert_eq!(current_notes(&mut ctx.read_conn).await.len(), 2);
    drop(bounds);
    std::fs::remove_file(root.join("a.md")).unwrap();
    let mut restarted = vec![Bound::new(&notes::KIND, binding).unwrap()];
    let result = read(
        &mut ctx.read_conn,
        &ctx.blobs,
        &ctx.writer,
        RunId::new(),
        &sink,
        &mut restarted,
        stop,
        Access::default(),
    )
    .await;
    for (_, result) in result {
        result.unwrap();
    }
    let notes = current_notes(&mut ctx.read_conn).await;
    ctx.close().await.unwrap();
    assert_eq!(
        notes
            .iter()
            .map(|r| (&*r.0, r.1.clone()))
            .collect::<Vec<_>>(),
        vec![("a.md", Value::Null), ("b.md", json!("beta"))]
    );
}

async fn turn(ctx: &mut OneShot, bound: &mut [Bound], access: Access<'_>) -> Result<(), String> {
    let sink = Sink(ctx.blobs.clone());
    let (_stop, stop) = watch::channel(false);
    for (_, result) in read(
        &mut ctx.read_conn,
        &ctx.blobs,
        &ctx.writer,
        RunId::new(),
        &sink,
        bound,
        stop,
        access,
    )
    .await
    {
        result?;
    }
    Ok(())
}

async fn prefix(
    ctx: &mut OneShot,
    b: &mut Bound,
    sink: &dyn drivers::MaterialSink,
    sealing: Option<&crate::one_shot::SealingSink>,
) -> MaterialRef {
    crate::backfill::record(&mut ctx.read_conn, &b.binding)
        .await
        .unwrap();
    let (cursor, carry) = positions::load(&mut ctx.read_conn, &b.binding.name)
        .await
        .unwrap()
        .unwrap_or((Value::Null, Value::Null));
    let acquired = b.reader.poll(&cursor).unwrap();
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

#[tokio::test]
async fn sealed_history_needs_no_private_key_and_keeps_the_binding_sealed() {
    let (_db, url, _) = crate::testdb::fresh().await;
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("vault");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("a.md"), "synthetic sealed alpha").unwrap();
    let identity = sinex_store::sealed::Identity::generate();
    let recipient = identity.to_public();
    let mut ctx = OneShot::open(&url, &dir.path().join("store"))
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
    let mut bounds = vec![Bound::new(&notes::KIND, binding.clone()).unwrap()];
    turn(
        &mut ctx,
        &mut bounds,
        Access {
            recipient: Some(&recipient),
            authority: None,
        },
    )
    .await
    .unwrap();
    drop(bounds);
    std::fs::remove_file(root.join("a.md")).unwrap();
    let mut restarted = vec![Bound::new(&notes::KIND, binding.clone()).unwrap()];
    turn(
        &mut ctx,
        &mut restarted,
        Access {
            recipient: Some(&recipient),
            authority: None,
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
    let mut lowered = vec![Bound::new(&notes::KIND, binding.clone()).unwrap()];
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
            authority: None,
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
impl sinex_sources::Parser for IgnoreFirst {
    type Record<'a> = &'a [u8];
    type Carry = ();
    type Payload = sinex_model::event_types::media::NoteDocument;
    fn parse(
        &self,
        record: &&[u8],
        carry: &(),
        context: &sinex_sources::drain::RecordContext<'_>,
    ) -> Result<(Vec<Self::Payload>, ()), sinex_sources::RecordError> {
        if context
            .member
            .is_some_and(|m| m["path"] == "a.md" && m.get("vanished_at_ns").is_none())
        {
            return Ok((Vec::new(), ()));
        }
        sinex_sources::Parser::parse(&notes::NotesParser, record, carry, context)
    }
}
fn ignore_first(b: &mut Bound) {
    let mut spec = notes::SPEC;
    spec.parser_version = 99;
    b.source = Box::new(sinex_sources::DeclaredSource {
        spec,
        parser: IgnoreFirst,
    });
}

#[tokio::test]
async fn a_restart_remembers_a_file_whose_record_produced_no_event() {
    let (_db, url, _) = crate::testdb::fresh().await;
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("vault");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("a.md"), "ignored alpha").unwrap();
    std::fs::write(root.join("b.md"), "beta").unwrap();
    let mut ctx = OneShot::open(&url, &dir.path().join("store"))
        .await
        .unwrap();
    let sink = Sink(ctx.blobs.clone());
    let binding = Binding::of(&notes::SPEC, &root, Settings::new("synthetic"));
    let mut b = Bound::new(&notes::KIND, binding.clone()).unwrap();
    ignore_first(&mut b);
    prefix(&mut ctx, &mut b, &sink, None).await;
    let checkpoint = positions::load(&mut ctx.read_conn, &binding.name)
        .await
        .unwrap()
        .unwrap();
    assert!(checkpoint.0["through"].as_u64().unwrap() > 0);
    drop(b);
    std::fs::remove_file(root.join("a.md")).unwrap();
    let mut restarted = vec![Bound::new(&notes::KIND, binding).unwrap()];
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
        vec![("a.md", Value::Null), ("b.md", json!("beta"))]
    );
    ctx.close().await.unwrap();
}

#[tokio::test]
async fn stopped_directory_restore_keeps_progress_and_can_be_retried() {
    let (_db, url, _) = crate::testdb::fresh().await;
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("vault");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("a.md"), "alpha").unwrap();
    let mut ctx = OneShot::open(&url, &dir.path().join("store"))
        .await
        .unwrap();
    let sink = Sink(ctx.blobs.clone());
    let binding = Binding::of(&notes::SPEC, &root, Settings::new("synthetic"));
    let mut bounds = vec![Bound::new(&notes::KIND, binding.clone()).unwrap()];
    turn(&mut ctx, &mut bounds, Access::default())
        .await
        .unwrap();
    drop(bounds);
    let checkpoint = positions::load(&mut ctx.read_conn, &binding.name)
        .await
        .unwrap()
        .unwrap();
    std::fs::remove_file(root.join("a.md")).unwrap();
    let mut restarted = vec![Bound::new(&notes::KIND, binding.clone()).unwrap()];
    let (_stop, stop) = watch::channel(true);
    let result = read(
        &mut ctx.read_conn,
        &ctx.blobs,
        &ctx.writer,
        RunId::new(),
        &sink,
        &mut restarted,
        stop,
        Access::default(),
    )
    .await;
    assert!(result[0].1.as_ref().is_err_and(|e| e.contains("cancelled")));
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
    assert_eq!(current_notes(&mut ctx.read_conn).await[0].1, Value::Null);
    ctx.close().await.unwrap();
}

#[tokio::test]
async fn a_vanished_guarded_member_keeps_its_class_after_the_source_floor_is_lowered() {
    let (_db, url, _) = crate::testdb::fresh().await;
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("vault");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("a.md"), "synthetic guarded alpha").unwrap();
    let mut ctx = OneShot::open(&url, &dir.path().join("store"))
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
    let mut bounds = vec![Bound::new(&notes::KIND, binding.clone()).unwrap()];
    turn(&mut ctx, &mut bounds, Access::default())
        .await
        .unwrap();
    drop(bounds);
    sinex_store::privacy::set_source_class(
        &mut ctx.read_conn,
        notes::SPEC.name,
        Class::Open,
        "synthetic",
    )
    .await
    .unwrap();
    std::fs::remove_file(root.join("a.md")).unwrap();
    let mut restarted = vec![Bound::new(&notes::KIND, binding).unwrap()];
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
    let shown:Value=sqlx::query_scalar("SELECT payload FROM core.visible_events WHERE event_type='note.document' AND record_state='removed'").fetch_one(&mut ctx.read_conn).await.unwrap();
    assert_eq!(shown, json!({"withheld":"guarded"}));
    ctx.close().await.unwrap();
}
