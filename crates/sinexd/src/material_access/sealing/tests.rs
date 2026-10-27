use super::*;
use crate::reader_context::ReaderContext;
use serde_json::Value;
use sinex_source_core::{Limits, drivers};
use sinex_sources::sources::zsh;

#[devkit::test]
async fn sealed_material_encrypts_every_chunk_in_the_actual_admission_pipeline(
    db: devkit::pg::TestDatabase,
) {
    let url = db.url(None);
    let dir = std::env::temp_dir().join(format!("sinex-sealed-chunks-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("history");
    std::fs::write(&path,b": 1700000000:0;synthetic_secret_a\n: 1700000001:0;synthetic_secret_b\n: 1700000002:0;synthetic_secret_c\n").unwrap();
    let mut ctx = ReaderContext::open(&url, &dir.join("store")).await.unwrap();
    let identity = sinex_sealing::Identity::generate();
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
        .filter(|payload| sinex_sealing::is_sealed(payload))
        .count();
    assert_eq!(
        encrypted, 3,
        "every admitted chunk must be sealed, including chunks without MaterialRef"
    );
    let (_, carry) = sinex_store::positions::load(&mut ctx.read_conn, "synthetic-history")
        .await
        .unwrap()
        .unwrap();
    assert!(sinex_sealing::is_sealed(&carry));
    assert!(!carry.to_string().contains("synthetic_secret"));
    let reference = sinex_sealing::key_ref(&carry).unwrap();
    let wrapped = sinex_store::sealed::wrapped_key(&mut ctx.read_conn, reference)
        .await
        .unwrap()
        .unwrap();
    let key = sinex_sealing::unwrap(&wrapped, &identity).unwrap();
    assert_eq!(
        sinex_sealing::unseal_payload(&carry, &key).unwrap(),
        expected_carry
    );
    assert!(!expected_carry.to_string().contains("synthetic_secret"));
    let counts = expected_carry["counts"].as_array().unwrap();
    assert_eq!(counts.len(), 3);
    assert!(
        counts
            .iter()
            .all(|entry| entry[1].as_array().is_some_and(|key| key.len() == 32) && entry[2] == 1)
    );
    assert!(
        sink.state.lock().unwrap().unadmitted.is_empty(),
        "registered chunk keys are not kept indefinitely"
    );
    ctx.close().await.unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
}

#[devkit::test]
async fn sealed_failure_details_are_encrypted_and_exactly_recoverable(
    db: devkit::pg::TestDatabase,
) {
    let url = db.url(None);
    let dir = std::env::temp_dir().join(format!("sinex-sealed-failure-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("history");
    std::fs::write(&path, b": 1700000000:0;synthetic_command\n").unwrap();
    let mut ctx = ReaderContext::open(&url, &dir.join("store")).await.unwrap();
    let identity = sinex_sealing::Identity::generate();
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
    let literal_carry =
        serde_json::json!({"key":"synthetic_carry_secret","sealed":"synthetic_carry_secret"});
    if let sinex_model::BatchKind::Source(progress) = &mut batch.kind {
        progress.carry = literal_carry.clone();
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
    let sealed = sinex_sealing::sealed_text(&reason).unwrap();
    let reference = sinex_sealing::key_ref(&sealed).unwrap();
    let wrapped = sinex_store::sealed::wrapped_key(&mut ctx.read_conn, reference)
        .await
        .unwrap()
        .unwrap();
    let key = sinex_sealing::unwrap(&wrapped, &identity).unwrap();
    assert_eq!(
        sinex_sealing::unseal_payload(&sealed, &key).unwrap(),
        "synthetic_failure_secret"
    );
    let (_, carry) = sinex_store::positions::load(&mut ctx.read_conn, "synthetic-failure")
        .await
        .unwrap()
        .unwrap();
    assert!(!carry.to_string().contains("synthetic_carry_secret"));
    assert_eq!(
        sinex_sealing::unseal_payload(&carry, &key).unwrap(),
        literal_carry
    );
    ctx.close().await.unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
}

#[devkit::test]
async fn sealed_material_cannot_silently_switch_its_wrapped_key(db: devkit::pg::TestDatabase) {
    let url = db.url(None);
    let mut conn = sqlx::Connection::connect(&url).await.unwrap();
    let identity = sinex_sealing::Identity::generate();
    let material = sinex_model::MaterialId::new();
    let first = sinex_sealing::new_key().unwrap();
    let other = sinex_sealing::new_key().unwrap();
    let wrapped = sinex_sealing::wrap(&first, &identity.to_public()).unwrap();
    sinex_store::sealed::store_key(&mut conn, material, &first.key_ref, &wrapped)
        .await
        .unwrap();
    sinex_store::sealed::store_key(&mut conn, material, &first.key_ref, &wrapped)
        .await
        .unwrap();
    let other_wrapped = sinex_sealing::wrap(&other, &identity.to_public()).unwrap();
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
