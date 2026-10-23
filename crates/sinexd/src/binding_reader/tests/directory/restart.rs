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
async fn restart_reads_the_retained_suffix_before_missing_source_bytes_and_keeps_empty_prefix() {
    let (_db, url, _) = crate::testdb::fresh().await;
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("vault");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("a.md"), b"").unwrap();
    std::fs::write(root.join("b.md"), b"original beta").unwrap();
    let mut ctx = OneShot::open(&url, &dir.path().join("store"))
        .await
        .unwrap();
    let sink = Sink(ctx.blobs.clone());
    let binding = Binding::of(&notes::SPEC, &root, Settings::new("synthetic"));
    let mut b = Bound::new(&notes::KIND, binding.clone()).unwrap();
    let reference = prefix(&mut ctx, &mut b, &sink, None).await;
    let checkpoint = positions::load(&mut ctx.read_conn, &binding.name)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(checkpoint.0["through"], 1);
    assert!(reference.length > 1);
    assert_eq!(
        current_notes(&mut ctx.read_conn)
            .await
            .iter()
            .map(|r| (&*r.0, r.1.clone()))
            .collect::<Vec<_>>(),
        vec![("a.md", json!(""))]
    );
    drop(b);
    std::fs::remove_file(root.join("b.md")).unwrap();
    let mut restarted = vec![Bound::new(&notes::KIND, binding).unwrap()];
    turn(&mut ctx, &mut restarted, Access::default())
        .await
        .unwrap();
    let rows:Vec<(String,Value,uuid::Uuid,i64)>=sqlx::query_as("SELECT payload->>'path',payload->'text',material_id,anchor FROM core.events WHERE event_type='note.document' ORDER BY commit_seq").fetch_all(&mut ctx.read_conn).await.unwrap();
    let old = rows
        .iter()
        .find(|r| r.0 == "b.md" && r.1 == json!("original beta"))
        .expect("retained suffix must be interpreted before the current disappearance");
    assert_eq!(old.2, reference.id.0);
    assert_eq!(old.3, 2);
    assert_eq!(
        current_notes(&mut ctx.read_conn)
            .await
            .iter()
            .map(|r| (&*r.0, r.1.clone()))
            .collect::<Vec<_>>(),
        vec![("a.md", json!("")), ("b.md", Value::Null)]
    );
    ctx.close().await.unwrap();
}

#[tokio::test]
async fn restart_finishes_a_partial_vanished_bundle_before_a_reappeared_file() {
    let (_db, url, _) = crate::testdb::fresh().await;
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("vault");
    std::fs::create_dir_all(&root).unwrap();
    for p in ["a.md", "b.md"] {
        std::fs::write(root.join(p), p).unwrap();
    }
    let mut ctx = OneShot::open(&url, &dir.path().join("store"))
        .await
        .unwrap();
    let sink = Sink(ctx.blobs.clone());
    let binding = Binding::of(&notes::SPEC, &root, Settings::new("synthetic"));
    let mut bounds = vec![Bound::new(&notes::KIND, binding.clone()).unwrap()];
    turn(&mut ctx, &mut bounds, Access::default())
        .await
        .unwrap();
    for p in ["a.md", "b.md"] {
        std::fs::remove_file(root.join(p)).unwrap();
    }
    let reference = prefix(&mut ctx, &mut bounds[0], &sink, None).await;
    let records: Vec<(usize, usize)> =
        serde_json::from_value(reference.manifest["records"].clone()).unwrap();
    let checkpoint = positions::load(&mut ctx.read_conn, &binding.name)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(checkpoint.0["through"].as_u64(), Some(records[0].1 as u64));
    drop(bounds);
    std::fs::write(root.join("b.md"), "reappeared beta").unwrap();
    let mut restarted = vec![Bound::new(&notes::KIND, binding).unwrap()];
    turn(&mut ctx, &mut restarted, Access::default())
        .await
        .unwrap();
    let removed:Option<i64>=sqlx::query_scalar("SELECT anchor FROM core.events WHERE material_id=$1 AND payload->>'path'='b.md' AND record_state='removed'").bind(reference.id.0).fetch_optional(&mut ctx.read_conn).await.unwrap();
    assert_eq!(removed, Some(records[1].0 as i64));
    assert_eq!(
        current_notes(&mut ctx.read_conn)
            .await
            .iter()
            .map(|r| (&*r.0, r.1.clone()))
            .collect::<Vec<_>>(),
        vec![("a.md", Value::Null), ("b.md", json!("reappeared beta"))]
    );
    ctx.close().await.unwrap();
}

#[tokio::test]
async fn sealed_completed_membership_needs_no_private_key_and_never_lowers_its_floor() {
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
    let checkpoint = positions::load(&mut ctx.read_conn, &binding.name)
        .await
        .unwrap()
        .unwrap();
    sinex_store::privacy::set_source_class(
        &mut ctx.read_conn,
        notes::SPEC.name,
        Class::Open,
        "synthetic",
    )
    .await
    .unwrap();
    drop(restarted);
    let mut lowered = vec![Bound::new(&notes::KIND, binding.clone()).unwrap()];
    let error = turn(
        &mut ctx,
        &mut lowered,
        Access {
            recipient: Some(&recipient),
            authority: None,
        },
    )
    .await
    .unwrap_err();
    assert!(error.contains("lowering its class"), "{error}");
    assert_eq!(
        positions::load(&mut ctx.read_conn, &binding.name)
            .await
            .unwrap()
            .unwrap(),
        checkpoint
    );
    let authority =
        crate::sealed_access::Authority::new(identity, std::time::Duration::from_secs(60)).unwrap();
    std::fs::write(root.join("b.md"), "synthetic beta after authorized restart").unwrap();
    turn(
        &mut ctx,
        &mut lowered,
        Access {
            recipient: Some(&recipient),
            authority: Some(&authority),
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

#[tokio::test]
async fn sealed_pending_directory_requires_authority_and_preserves_the_original_suffix() {
    let (_db, url, _) = crate::testdb::fresh().await;
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("vault");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("a.md"), "alpha").unwrap();
    std::fs::write(root.join("b.md"), "synthetic sealed beta").unwrap();
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
    let sealing = crate::one_shot::SealingSink::new(ctx.blobs.clone(), recipient.clone());
    let binding = Binding::of(&notes::SPEC, &root, Settings::new("synthetic"));
    let mut b = Bound::new(&notes::KIND, binding.clone()).unwrap();
    let reference = prefix(&mut ctx, &mut b, &sealing, Some(&sealing)).await;
    let checkpoint = positions::load(&mut ctx.read_conn, &binding.name)
        .await
        .unwrap()
        .unwrap();
    drop(b);
    std::fs::remove_file(root.join("b.md")).unwrap();
    let mut restarted = vec![Bound::new(&notes::KIND, binding.clone()).unwrap()];
    let error = turn(
        &mut ctx,
        &mut restarted,
        Access {
            recipient: Some(&recipient),
            authority: None,
        },
    )
    .await
    .unwrap_err();
    assert!(error.contains("scoped authority"), "{error}");
    assert_eq!(
        positions::load(&mut ctx.read_conn, &binding.name)
            .await
            .unwrap()
            .unwrap(),
        checkpoint
    );
    let authority =
        crate::sealed_access::Authority::new(identity, std::time::Duration::from_secs(60)).unwrap();
    turn(
        &mut ctx,
        &mut restarted,
        Access {
            recipient: Some(&recipient),
            authority: Some(&authority),
        },
    )
    .await
    .unwrap();
    let key = authority
        .material_key(&mut ctx.read_conn, reference.id)
        .await
        .unwrap();
    let stored: Vec<Value> =
        sqlx::query_scalar("SELECT payload FROM core.events WHERE material_id=$1 ORDER BY anchor")
            .bind(reference.id.0)
            .fetch_all(&mut ctx.read_conn)
            .await
            .unwrap();
    let plain: Vec<_> = stored
        .iter()
        .map(|p| sinex_store::sealed::unseal_payload(p, &key.key).unwrap())
        .collect();
    assert!(
        plain
            .iter()
            .any(|p| p["path"] == "b.md" && p["text"] == "synthetic sealed beta"),
        "original pending record interpreted from authenticated retained bytes"
    );
    let classes: Vec<String> =
        sqlx::query_scalar("SELECT class FROM core.events WHERE event_type='note.document'")
            .fetch_all(&mut ctx.read_conn)
            .await
            .unwrap();
    assert!(classes.iter().all(|c| c == "sealed"));
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
async fn an_explicit_consumed_prefix_restores_an_ignored_first_member_without_an_event_anchor() {
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
    let reference = prefix(&mut ctx, &mut b, &sink, None).await;
    let anchors = positions::accounted_records(&mut ctx.read_conn, reference.id)
        .await
        .unwrap();
    assert!(
        anchors.anchors.is_empty(),
        "ignored source record has no event/error witness"
    );
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
async fn a_cached_authorized_directory_read_cannot_republish_an_excised_material_key() {
    let (_db, url, _) = crate::testdb::fresh().await;
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("vault");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("a.md"), "synthetic sealed alpha").unwrap();
    std::fs::write(root.join("b.md"), "synthetic sealed beta").unwrap();
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
    let checkpoint = positions::load(&mut ctx.read_conn, &binding.name)
        .await
        .unwrap()
        .unwrap();
    let material = sinex_model::MaterialId(
        uuid::Uuid::parse_str(checkpoint.0["material"].as_str().unwrap()).unwrap(),
    );
    let reference = sinex_store::material_bytes::reference(&mut ctx.read_conn, material)
        .await
        .unwrap()
        .unwrap();
    let authority =
        crate::sealed_access::Authority::new(identity, std::time::Duration::from_secs(60)).unwrap();
    let mut cached = Bound::new(&notes::KIND, binding.clone()).unwrap();
    let mut old_cursor = checkpoint.0.clone();
    old_cursor["through"] = json!(0);
    let (_stop, stop) = watch::channel(false);
    // An earlier acknowledged position can already have opened all bytes
    // before another operation excises the material.
    crate::binding_reader::directory::restore(
        &mut ctx.read_conn,
        &ctx.blobs,
        &mut cached,
        &old_cursor,
        Access {
            recipient: Some(&recipient),
            authority: Some(&authority),
        },
        stop,
    )
    .await
    .unwrap();
    let acquired = cached.reader.poll(&old_cursor).unwrap();
    assert!(matches!(
        acquired.material.as_ref().unwrap().stored,
        Some(drivers::StoredBytes::FramedReader(_))
    ));
    let sealing = crate::one_shot::SealingSink::new(ctx.blobs.clone(), recipient);
    let (key_ref, secrets) = authority
        .material_secrets(&mut ctx.read_conn, material)
        .await
        .unwrap();
    sealing
        .restore_secrets(reference.blob, key_ref, secrets)
        .unwrap();
    let carry = sealing.open_carry(&checkpoint.1).unwrap();
    crate::excision::excise_with(
        &mut ctx.read_conn,
        &ctx.blobs,
        &ctx.writer,
        sinex_store::excision::Target::Material(material),
        "synthetic",
    )
    .await
    .unwrap();
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM privacy.material_keys WHERE material_id=$1)",
    )
    .bind(material.0)
    .fetch_one(&mut ctx.read_conn)
    .await
    .unwrap();
    assert!(!exists);
    let mut total = Totals::default();
    crate::backfill::submit(
        &ctx.writer,
        RunId::new(),
        cached.source.as_ref(),
        &binding.name,
        &sealing,
        Some(&sealing),
        Some(&authority),
        Class::Sealed,
        acquired,
        carry,
        &mut total,
    )
    .await
    .unwrap();
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM privacy.material_keys WHERE material_id=$1)",
    )
    .bind(material.0)
    .fetch_one(&mut ctx.read_conn)
    .await
    .unwrap();
    assert!(
        !exists,
        "a retained read must not restore the destroyed wrapped key"
    );
    let events: i64 = sqlx::query_scalar("SELECT count(*) FROM core.events WHERE material_id=$1")
        .bind(material.0)
        .fetch_one(&mut ctx.read_conn)
        .await
        .unwrap();
    assert_eq!(
        events, 0,
        "ordinary excision digests still suppress the cached readings"
    );
    assert_eq!(total.to_json()["admitted"], 0);
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
