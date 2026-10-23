use super::*;
use serde_json::Value;
use sinex_sources::{Limits, drivers, sources::zsh};

#[tokio::test]
async fn sealed_material_encrypts_every_chunk_in_the_actual_admission_pipeline() {
    let (_db, url, _) = crate::testdb::fresh().await;
    let dir = std::env::temp_dir().join(format!("sinex-sealed-chunks-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("history");
    std::fs::write(&path,b": 1700000000:0;synthetic_secret_a\n: 1700000001:0;synthetic_secret_b\n: 1700000002:0;synthetic_secret_c\n").unwrap();
    let mut ctx = OneShot::open(&url, &dir.join("store")).await.unwrap();
    let identity = sinex_store::sealed::Identity::generate();
    let sink = SealingSink::new(ctx.blobs.clone(), identity.to_public());
    let source = zsh::source("synthetic".into());
    let driver = zsh::driver(path, "synthetic".into());
    let acquired = driver.poll(&Value::Null, &mut Default::default()).unwrap();
    let prepared = drivers::prepare(
        &source,
        "synthetic-history",
        sinex_model::RunId::new(),
        acquired,
        Value::Null,
        &sink,
        Limits {
            max_events: 1,
            max_bytes: 1024,
        },
    )
    .unwrap();
    let mut chunks = 0;
    let mut expected_carry = Value::Null;
    for batch in prepared.batches().unwrap() {
        let batch = batch.unwrap();
        chunks += 1;
        if let sinex_model::BatchKind::Source(progress) = &batch.kind {
            expected_carry = progress.carry.clone();
        }
        let mut batch = [batch];
        sink.seal(&mut batch).unwrap();
        ctx.writer
            .submit(
                crate::writer::Priority::Bulk,
                batch.into_iter().next().unwrap(),
            )
            .await
            .unwrap();
    }
    assert_eq!(chunks, 3);
    let payloads: Vec<Value> = sqlx::query_scalar("SELECT payload FROM core.events")
        .fetch_all(&mut ctx.read_conn)
        .await
        .unwrap();
    let encrypted = payloads
        .iter()
        .filter(|payload| sinex_store::sealed::is_sealed(payload))
        .count();
    assert_eq!(
        encrypted, 3,
        "every admitted chunk must be sealed, including chunks without MaterialRef"
    );
    let (_, carry) = sinex_store::positions::load(&mut ctx.read_conn, "synthetic-history")
        .await
        .unwrap()
        .unwrap();
    assert!(sinex_store::sealed::is_sealed(&carry));
    assert!(!carry.to_string().contains("synthetic_secret"));
    let reference = sinex_store::sealed::key_ref(&carry).unwrap();
    let wrapped = sinex_store::sealed::wrapped_key(&mut ctx.read_conn, reference)
        .await
        .unwrap()
        .unwrap();
    let key = sinex_store::sealed::unwrap(&wrapped, &identity).unwrap();
    assert_eq!(
        sinex_store::sealed::unseal_payload(&carry, &key).unwrap(),
        expected_carry
    );
    assert!(expected_carry.to_string().contains("synthetic_secret"));
    assert!(
        sink.state.lock().unwrap().pending.is_empty(),
        "registered chunk keys are not retained indefinitely"
    );
    ctx.close().await.unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn sealed_failure_details_are_encrypted_and_exactly_recoverable() {
    let (_db, url, _) = crate::testdb::fresh().await;
    let dir = std::env::temp_dir().join(format!("sinex-sealed-failure-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("history");
    std::fs::write(&path, b": 1700000000:0;synthetic_command\n").unwrap();
    let mut ctx = OneShot::open(&url, &dir.join("store")).await.unwrap();
    let identity = sinex_store::sealed::Identity::generate();
    let sink = SealingSink::new(ctx.blobs.clone(), identity.to_public());
    let source = zsh::source("synthetic".into());
    let driver = zsh::driver(path, "synthetic".into());
    let acquired = driver.poll(&Value::Null, &mut Default::default()).unwrap();
    let prepared = drivers::prepare(
        &source,
        "synthetic-failure",
        sinex_model::RunId::new(),
        acquired,
        Value::Null,
        &sink,
        Limits::default(),
    )
    .unwrap();
    let mut batch = prepared.batches().unwrap().next().unwrap().unwrap();
    let sinex_model::RecordOutcome::Events { events, .. } = &batch.records[0] else {
        panic!("events")
    };
    let location = events[0].location().unwrap().0;
    batch.records = vec![sinex_model::RecordOutcome::Failure {
        location,
        reason: "synthetic_failure_secret".into(),
    }];
    let literal_envelope =
        serde_json::json!({"key":"synthetic_carry_secret","sealed":"synthetic_carry_secret"});
    if let sinex_model::BatchKind::Source(progress) = &mut batch.kind {
        progress.carry = literal_envelope.clone();
    }
    let mut batch = [batch];
    sink.seal(&mut batch).unwrap();
    ctx.writer
        .submit(
            crate::writer::Priority::Bulk,
            batch.into_iter().next().unwrap(),
        )
        .await
        .unwrap();
    let reason: String = sqlx::query_scalar("SELECT reason FROM raw.failures")
        .fetch_one(&mut ctx.read_conn)
        .await
        .unwrap();
    assert!(!reason.contains("synthetic_failure_secret"));
    let sealed = sinex_store::sealed::sealed_text(&reason).unwrap();
    let reference = sinex_store::sealed::key_ref(&sealed).unwrap();
    let wrapped = sinex_store::sealed::wrapped_key(&mut ctx.read_conn, reference)
        .await
        .unwrap()
        .unwrap();
    let key = sinex_store::sealed::unwrap(&wrapped, &identity).unwrap();
    assert_eq!(
        sinex_store::sealed::unseal_payload(&sealed, &key).unwrap(),
        "synthetic_failure_secret"
    );
    let (_, carry) = sinex_store::positions::load(&mut ctx.read_conn, "synthetic-failure")
        .await
        .unwrap()
        .unwrap();
    assert!(!carry.to_string().contains("synthetic_carry_secret"));
    assert_eq!(
        sinex_store::sealed::unseal_payload(&carry, &key).unwrap(),
        literal_envelope
    );
    ctx.close().await.unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn sealed_material_cannot_silently_switch_its_wrapped_key() {
    let (_db, url, _) = crate::testdb::fresh().await;
    let mut conn = sqlx::Connection::connect(&url).await.unwrap();
    let identity = sinex_store::sealed::Identity::generate();
    let material = sinex_model::MaterialId::new();
    let first = sinex_store::sealed::new_key().unwrap();
    let other = sinex_store::sealed::new_key().unwrap();
    let wrapped = sinex_store::sealed::wrap(&first, &identity.to_public()).unwrap();
    sinex_store::sealed::store_key(&mut conn, material, &first.key_ref, &wrapped)
        .await
        .unwrap();
    sinex_store::sealed::store_key(&mut conn, material, &first.key_ref, &wrapped)
        .await
        .unwrap();
    let other_wrapped = sinex_store::sealed::wrap(&other, &identity.to_public()).unwrap();
    assert!(
        sinex_store::sealed::store_key(&mut conn, material, &other.key_ref, &other_wrapped)
            .await
            .is_err()
    );
    assert_eq!(
        sinex_store::sealed::wrapped_key(&mut conn, &first.key_ref)
            .await
            .unwrap(),
        Some(wrapped)
    );
}

#[tokio::test]
async fn staged_directory_resumes_from_actual_admitted_cursor_with_original_bytes() {
    use sinex_model::{BatchKind, RunId};
    use sinex_sources::{binding, sources::notes};
    let (_db, url, _) = crate::testdb::fresh().await;
    let dir = std::env::temp_dir().join(format!("sinex-directory-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir(&dir).unwrap();
    let root = dir.join("vault");
    std::fs::create_dir(&root).unwrap();
    std::fs::write(root.join("a.md"), "").unwrap();
    std::fs::write(root.join("b.md"), "beta").unwrap();
    let mut ctx = OneShot::open(&url, &dir.join("store")).await.unwrap();
    let sink = Sink(ctx.blobs.clone());
    let source = notes::source();
    let mut reader = binding::walked(notes::driver(root.clone(), "synthetic".into()));
    let acquired = reader.poll(&Value::Null).unwrap();
    let id = acquired.material.as_ref().unwrap().id;
    let prepared = drivers::prepare(
        &source,
        "synthetic-vault",
        RunId::new(),
        acquired,
        Value::Null,
        &sink,
        Limits {
            max_events: 1,
            max_bytes: 1024,
        },
    )
    .unwrap();
    let first = prepared.batches().unwrap().next().unwrap().unwrap();
    let BatchKind::Source(progress) = &first.kind else {
        panic!()
    };
    let blob = progress.material.as_ref().unwrap().blob.clone();
    let expected_cursor = progress.cursor.clone();
    ctx.writer
        .submit(crate::writer::Priority::Bulk, first)
        .await
        .unwrap();
    drop(prepared);
    let (cursor, carry) = sinex_store::positions::load(&mut ctx.read_conn, "synthetic-vault")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cursor, expected_cursor);
    assert_eq!(cursor["through"], 1);
    assert_eq!(ctx.blobs.read(&blob).unwrap(), b"\0\0beta");
    std::fs::write(root.join("b.md"), "later bytes").unwrap();
    let acquired = reader.poll(&cursor).unwrap();
    assert_eq!(acquired.material.as_ref().unwrap().id, id);
    let prepared = drivers::prepare(
        &source,
        "synthetic-vault",
        RunId::new(),
        acquired,
        carry,
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
    let payloads: Vec<Value> = sqlx::query_scalar(
        "SELECT payload FROM core.events WHERE event_type='note.document' ORDER BY anchor",
    )
    .fetch_all(&mut ctx.read_conn)
    .await
    .unwrap();
    assert_eq!(payloads.len(), 2);
    assert_eq!(payloads[0]["text"], "");
    assert_eq!(payloads[1]["text"], "beta");
    let (cursor, _) = sinex_store::positions::load(&mut ctx.read_conn, "synthetic-vault")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cursor["through"], 6);
    let next = reader.poll(&cursor).unwrap();
    assert_ne!(next.material.as_ref().unwrap().id, id);
    ctx.close().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

mod sqlite_rows;

mod sqlite_images;

#[test]
fn retained_sealing_rejects_naming_only_before_any_state_change() {
    let dir = tempfile::tempdir().unwrap();
    let blobs = Blobs::open(dir.path()).unwrap();
    let identity = sinex_store::sealed::Identity::generate();
    let sealing = SealingSink::new(blobs, identity.to_public());
    let old = sinex_store::sealed::new_key().unwrap();
    let fresh = sinex_store::sealed::new_key().unwrap();
    let naming = sinex_store::sealed::MaterialSecrets::from_data_key(&old).naming_only();
    assert!(
        sealing
            .restore_secrets("blob".into(), old.key_ref.clone(), naming.clone())
            .is_err()
    );
    assert!(
        sealing
            .restore_current(
                MaterialId::new(),
                "blob".into(),
                old.key_ref.clone(),
                naming.clone()
            )
            .is_err()
    );
    assert!(sealing.key("blob").is_err());
    let secrets = naming.with_data_key(&fresh);
    let wrapped = sinex_store::sealed::wrap_secrets(&secrets, &identity.to_public()).unwrap();
    let restored =
        sinex_store::sealed::unwrap_secrets(&wrapped, &identity, sinex_model::Class::Sealed)
            .unwrap();
    sealing
        .restore_secrets("blob".into(), fresh.key_ref.clone(), restored)
        .unwrap();
    assert_eq!(sealing.key("blob").unwrap().key, fresh.key);
    let carry = serde_json::json!({"synthetic":"retained carry"});
    let encrypted = sinex_store::sealed::seal_value(&carry, &fresh).unwrap();
    assert_eq!(sealing.open_carry(&encrypted).unwrap(), carry);
}

#[tokio::test]
async fn retained_record_naming_survives_key_restore_and_actual_replay() {
    let (_db, url, _) = crate::testdb::fresh().await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("history");
    std::fs::write(
        &path,
        b": 1700000000:0;synthetic retained a\n: 1700000001:0;synthetic retained b\n",
    )
    .unwrap();
    let mut ctx = OneShot::open(&url, &dir.path().join("store"))
        .await
        .unwrap();
    let identity = sinex_store::sealed::Identity::generate();
    let old = sinex_store::sealed::new_key().unwrap();
    let sink = SealingSink::new(ctx.blobs.clone(), identity.to_public());
    let source = zsh::source("synthetic".into());
    let driver = zsh::driver(path, "synthetic".into());
    let acquired = driver.poll(&Value::Null, &mut Default::default()).unwrap();
    let prepared = drivers::prepare(
        &source,
        "retained-names",
        sinex_model::RunId::new(),
        acquired,
        Value::Null,
        &sink,
        Limits::default(),
    )
    .unwrap();
    let mut expected = Vec::new();
    let mut data = None;
    for batch in prepared.batches().unwrap() {
        let batch = batch.unwrap();
        for event in batch.events() {
            let mut named = event.clone();
            sinex_store::sealed::seal_event(&mut named, &old).unwrap();
            expected.push((
                named.equivalence_key.unwrap().0,
                named.record_hash.to_vec(),
                event.payload.clone(),
            ));
        }
        if let sinex_model::BatchKind::Source(progress) = &batch.kind
            && let Some(material) = &progress.material
        {
            let key = sink.key(&material.blob).unwrap();
            let secrets = sinex_store::sealed::MaterialSecrets::from_data_key(&old)
                .naming_only()
                .with_data_key(&key);
            sink.restore_secrets(material.blob.clone(), key.key_ref.clone(), secrets)
                .unwrap();
            data = Some(key);
        }
        let mut batches = [batch];
        sink.seal(&mut batches).unwrap();
        ctx.writer
            .submit(
                crate::writer::Priority::Bulk,
                batches.into_iter().next().unwrap(),
            )
            .await
            .unwrap();
    }
    // Drop capture's in-memory secrets. Replay must unwrap the retained bundle.
    drop(sink);
    let authority =
        crate::sealed_access::Authority::new(identity, std::time::Duration::from_secs(60)).unwrap();
    let report = crate::replay::run(
        &mut ctx.read_conn,
        &ctx.blobs,
        &ctx.writer,
        crate::replay::Options {
            scope: crate::replay::Scope {
                binding: Some("retained-names".into()),
                ..Default::default()
            },
            compare: false,
            resume: None,
            actor: "synthetic".into(),
            authority: Some(authority),
        },
    )
    .await
    .unwrap();
    assert_eq!((report.archived, report.admitted), (2, 2));
    let readings: Vec<(String, Vec<u8>, Value)> = sqlx::query_as(
        "SELECT equivalence_key,record_hash,payload FROM core.events ORDER BY anchor,id",
    )
    .fetch_all(&mut ctx.read_conn)
    .await
    .unwrap();
    assert_eq!(readings.len(), 4);
    let data = data.unwrap();
    for (index, pair) in readings.as_chunks::<2>().0.iter().enumerate() {
        for (equivalence, hash, payload) in pair {
            assert_eq!(
                (equivalence, hash),
                (&expected[index].0, &expected[index].1)
            );
            assert_eq!(
                sinex_store::sealed::unseal_payload(payload, &data.key).unwrap(),
                expected[index].2
            );
            assert!(sinex_store::sealed::unseal_payload(payload, &old.key).is_err());
        }
    }
    ctx.close().await.unwrap();
}
