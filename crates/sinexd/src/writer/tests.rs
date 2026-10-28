use super::*;
use sinex_model::{
    BatchKind, Class, Event, MaterialId, Origin, ReadingOrder, RecordLocation, RecordOutcome,
    RunId, Timestamp, event_types::CommandImported,
};

fn batch(tag: &str) -> Batch {
    let event = Event::build(
        &CommandImported {
            command: tag.into(),
            hostname: "h".into(),
            started_at: Some(Timestamp::from_unix_secs(1).unwrap()),
            header_elapsed: Some(0),
            paths: vec![],
            n: 0,
        },
        "t",
        Origin::Record {
            location: RecordLocation {
                material: MaterialId::new(),
                anchor: 0,
            },
            output_index: 0,
        },
        ReadingOrder(0),
        [1; 32],
        Class::Open,
    )
    .unwrap();
    Batch {
        run: RunId::new(),
        kind: BatchKind::Api { actor: "t".into() },
        records: vec![RecordOutcome::Events {
            location: None,
            events: vec![event],
        }],
        archive: vec![],
    }
}

#[devkit::test]
async fn live_batches_go_before_queued_bulk_work(db: devkit::pg::TestDatabase) {
    let url = db.url(None);
    let connection = <sqlx::PgConnection as sqlx::Connection>::connect(&url)
        .await
        .unwrap();
    let (handle, queues) = channels();
    let mut waits = Vec::new();
    for i in 0..5 {
        let h = handle.clone();
        waits.push(tokio::spawn(async move {
            h.submit(Priority::Bulk, batch(&format!("bulk{i}"))).await
        }));
    }
    let h = handle.clone();
    let live = tokio::spawn(async move { h.submit(Priority::Live, batch("live")).await });
    // Everything is queued before the writer starts.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let writer = tokio::spawn(run(
        url,
        claim(connection).await.unwrap(),
        queues,
        Arc::new(sinex_store::NoDerivations),
        None,
    ));
    let live_seq = live.await.unwrap().unwrap().commit_seq.unwrap().0;
    let mut bulk_seqs = Vec::new();
    for w in waits {
        bulk_seqs.push(w.await.unwrap().unwrap().commit_seq.unwrap().0);
    }
    assert_eq!(live_seq, 1, "the live batch committed first");
    assert!(bulk_seqs.iter().all(|s| *s > live_seq));
    drop(handle);
    writer.await.unwrap();
}

#[devkit::test]
async fn a_refused_batch_does_not_stop_the_writer(db: devkit::pg::TestDatabase) {
    let url = db.url(None);
    let connection = <sqlx::PgConnection as sqlx::Connection>::connect(&url)
        .await
        .unwrap();
    let (handle, queues) = channels();
    let writer = tokio::spawn(run(
        url,
        claim(connection).await.unwrap(),
        queues,
        Arc::new(sinex_store::NoDerivations),
        None,
    ));
    let mut bad = batch("x");
    if let RecordOutcome::Events { events, .. } = &mut bad.records[0] {
        events[0].payload["command"] = serde_json::json!("nul\u{0}");
    }
    assert!(matches!(
        handle.submit(Priority::Live, bad).await,
        Err(WriteError::Refused(_))
    ));
    assert_eq!(
        handle
            .submit(Priority::Live, batch("ok"))
            .await
            .unwrap()
            .admitted,
        1
    );
    drop(handle);
    writer.await.unwrap();
}

/// No silent loss under backpressure: with the writer stalled, far
/// more batches than the queues hold are handed over; producers wait
/// rather than drop, and once the writer runs each is admitted once.
#[devkit::test]
async fn a_stalled_writer_makes_producers_wait_and_loses_nothing(db: devkit::pg::TestDatabase) {
    let url = db.url(None);
    let connection = <sqlx::PgConnection as sqlx::Connection>::connect(&url)
        .await
        .unwrap();
    let (handle, queues) = channels();
    let n = DEPTH * 10;
    let waits: Vec<_> = (0..n)
        .map(|i| {
            let h = handle.clone();
            tokio::spawn(async move { h.submit(Priority::Bulk, batch(&format!("b{i}"))).await })
        })
        .collect();
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert!(
        waits.iter().all(|w| !w.is_finished()),
        "nothing completes while the writer is stalled"
    );
    let writer = tokio::spawn(run(
        url,
        claim(connection).await.unwrap(),
        queues,
        Arc::new(sinex_store::NoDerivations),
        None,
    ));
    let mut seqs = std::collections::BTreeSet::new();
    for w in waits {
        let out = w.await.unwrap().unwrap();
        assert_eq!(out.admitted, 1);
        assert!(
            seqs.insert(out.commit_seq.unwrap().0),
            "one commit_seq per batch"
        );
    }
    assert_eq!(seqs.len(), n);
    drop(handle);
    writer.await.unwrap();
}

/// One writer per database: a second claim is refused while the
/// first connection lives, and granted once it is gone.
#[devkit::test]
async fn only_one_connection_can_claim_the_writer(db: devkit::pg::TestDatabase) {
    let url = db.url(None);
    let first = <sqlx::PgConnection as sqlx::Connection>::connect(&url)
        .await
        .unwrap();
    let first = claim(first).await.unwrap();
    let second = PgConnection::connect(&url).await.unwrap();
    assert!(claim(second).await.is_err());
    first.close().await.unwrap();
    let second = PgConnection::connect(&url).await.unwrap();
    claim(second).await.unwrap();
}

#[devkit::test]
async fn queued_checks_reject_a_revoked_unlock_and_roll_back_before_commit(
    db: devkit::pg::TestDatabase,
) {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let url = db.url(None);
    let connection = <sqlx::PgConnection as sqlx::Connection>::connect(&url)
        .await
        .unwrap();
    let (handle, queues) = channels();
    let unlock = crate::material_access::sealing::Unlock::new(
        sinex_sealing::Identity::generate(),
        std::time::Duration::from_secs(30),
    )
    .unwrap();
    let check_unlock = unlock.clone();
    let h = handle.clone();
    let queued = tokio::spawn(async move {
        h.submit_checked(
            Priority::Bulk,
            batch("revoked"),
            Arc::new(move || check_unlock.check()),
        )
        .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while queues.bulk.len() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    unlock.revoke();
    let writer = tokio::spawn(run(
        url.clone(),
        claim(connection).await.unwrap(),
        queues,
        Arc::new(sinex_store::NoDerivations),
        None,
    ));
    assert!(matches!(queued.await.unwrap(), Err(WriteError::Refused(_))));
    let calls = Arc::new(AtomicUsize::new(0));
    let checked = calls.clone();
    let result = handle
        .submit_checked(
            Priority::Bulk,
            batch("cancel-before-commit"),
            Arc::new(move || {
                if checked.fetch_add(1, Ordering::SeqCst) == 0 {
                    Ok(())
                } else {
                    Err("capture canceled before commit".into())
                }
            }),
        )
        .await;
    assert!(matches!(result, Err(WriteError::Refused(_))));
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "the second check is inside the admission transaction"
    );
    let mut conn = PgConnection::connect(&url).await.unwrap();
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM core.events")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(
        count, 0,
        "revoked queued work and canceled uncommitted events remain absent"
    );
    assert_eq!(
        handle
            .submit(Priority::Live, batch("unrelated retry"))
            .await
            .unwrap()
            .admitted,
        1
    );
    drop(handle);
    writer.await.unwrap();
}

/// A capture that stored its bytes before an excision deleted the same bytes
/// is refused, rather than registering a material whose file is gone.
#[devkit::test]
async fn a_batch_whose_bytes_are_not_stored_is_refused(db: devkit::pg::TestDatabase) {
    let url = db.url(None);
    let connection = <sqlx::PgConnection as sqlx::Connection>::connect(&url)
        .await
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = Blobs::open(dir.path()).unwrap();
    let (handle, queues) = channels();
    let writer = tokio::spawn(run(
        url,
        claim(connection).await.unwrap(),
        queues,
        Arc::new(sinex_store::NoDerivations),
        Some(blobs.clone()),
    ));
    let bytes = b"synthetic stored bytes";
    let blob = format!("blake3:{}", blake3::hash(bytes).to_hex());
    let source = || Batch {
        run: RunId::new(),
        kind: BatchKind::Source(Box::new(sinex_model::SourceProgress {
            binding: "synthetic".into(),
            data_keys: Vec::new(),
            input: sinex_model::InputRef {
                kind: "file".into(),
                locator: "synthetic".into(),
                host: "h".into(),
            },
            observation: Default::default(),
            revision: sinex_model::RevisionChange::Appended,
            material: Some(sinex_model::MaterialRef {
                id: MaterialId(uuid::Uuid::from_u128(7)),
                input: None,
                blob: blob.clone(),
                length: bytes.len() as u64,
                part_index: 0,
                previous_part: None,
                parser: "zsh".into(),
                parser_version: 1,
                manifest: sinex_model::Manifest::from_json(serde_json::json!({})),
            }),
            cursor: serde_json::json!({"offset": 1}),
            carry: serde_json::Value::Null,
            memory: None,
        })),
        records: vec![],
        archive: vec![],
    };
    let refused = handle.submit(Priority::Bulk, source()).await.unwrap_err();
    assert!(refused.to_string().contains("does not hold"), "{refused}");
    blobs.put(bytes).unwrap();
    handle.submit(Priority::Bulk, source()).await.unwrap();
    drop(handle);
    writer.await.unwrap();
}

#[devkit::test]
async fn source_content_refusals_keep_material_failures_and_advance_the_cursor(
    db: devkit::pg::TestDatabase,
) {
    use sinex_model::{InputRef, Manifest, MaterialRef, RevisionChange, SourceProgress};
    let url = db.url(None);
    let connection = PgConnection::connect(&url).await.unwrap();
    let mut reader = PgConnection::connect(&url).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = Blobs::open(dir.path()).unwrap();
    let bytes = b"unknown source kind";
    let blob = blobs.put(bytes).unwrap();
    let material = MaterialId::new();
    let progress = SourceProgress {
        binding: "synthetic-refusal".into(),
        data_keys: vec![],
        input: InputRef {
            kind: "file".into(),
            locator: "synthetic".into(),
            host: "h".into(),
        },
        observation: Default::default(),
        revision: RevisionChange::Appended,
        material: Some(MaterialRef {
            id: material,
            input: None,
            blob: blob.clone(),
            length: bytes.len() as u64,
            part_index: 0,
            previous_part: None,
            parser: "synthetic".into(),
            parser_version: 1,
            manifest: Manifest::default(),
        }),
        cursor: serde_json::json!({"offset": 7}),
        carry: serde_json::Value::Null,
        memory: None,
    };
    let source = |command: &str, anchor: u64, offset: usize| {
        let mut offered = batch(command);
        let mut progress = progress.clone();
        if anchor != 0 {
            progress.material = None;
            progress.revision = RevisionChange::Same;
        }
        progress.cursor = serde_json::json!({"offset": offset});
        offered.kind = BatchKind::Source(Box::new(progress));
        let location = RecordLocation { material, anchor };
        if let RecordOutcome::Events {
            location: record_location,
            events,
        } = &mut offered.records[0]
        {
            *record_location = Some(location);
            events[0].origin = Origin::Record {
                location,
                output_index: 0,
            };
        }
        offered
    };
    let mut bad = source("x", 0, 7);
    if let RecordOutcome::Events { events, .. } = &mut bad.records[0] {
        events[0].event_type = "synthetic.unknown".into();
    }
    let (handle, queues) = channels();
    let writer = tokio::spawn(run(
        url,
        connection,
        queues,
        Arc::new(sinex_store::NoDerivations),
        Some(blobs.clone()),
    ));
    let mut separate_file = bad.clone();
    let owned = b"separately owned source bytes";
    let owned_blob = blobs.put(owned).unwrap();
    if let RecordOutcome::Events { events, .. } = &mut separate_file.records[0] {
        events[0].owned_blobs.push(sinex_model::OwnedBlob {
            blob: owned_blob.clone(),
            size: owned.len() as u64,
        });
    }
    assert!(matches!(
        handle
            .submit_source(Priority::Live, separate_file, Arc::new(|| Ok(())))
            .await,
        Err(WriteError::Refused(_))
    ));
    let positions: i64 =
        sqlx::query_scalar("SELECT count(*) FROM raw.positions WHERE binding='synthetic-refusal'")
            .fetch_one(&mut reader)
            .await
            .unwrap();
    assert_eq!(positions, 0);
    assert!(blobs.contains(&owned_blob).unwrap());
    let outcome = handle
        .submit_source(Priority::Live, bad, Arc::new(|| Ok(())))
        .await
        .unwrap();
    assert_eq!(outcome.admitted, 0);
    assert_eq!(outcome.failures, 1);
    let failures: i64 = sqlx::query_scalar("SELECT count(*) FROM raw.failures WHERE material_id=$1 AND anchor=0 AND reason LIKE 'content refused:%'").bind(material.0).fetch_one(&mut reader).await.unwrap();
    assert_eq!(failures, 1);
    let cursor: Value =
        sqlx::query_scalar("SELECT cursor FROM raw.positions WHERE binding='synthetic-refusal'")
            .fetch_one(&mut reader)
            .await
            .unwrap();
    assert_eq!(cursor["offset"], 7);
    let outcome = handle
        .submit_source(
            Priority::Live,
            source("private\0value", 8, 15),
            Arc::new(|| Ok(())),
        )
        .await
        .unwrap();
    assert_eq!(outcome.failures, 1);
    let reason: String =
        sqlx::query_scalar("SELECT reason FROM raw.failures WHERE material_id=$1 AND anchor=8")
            .bind(material.0)
            .fetch_one(&mut reader)
            .await
            .unwrap();
    assert!(reason.contains("database refused invalid data"), "{reason}");
    assert!(!reason.contains("private"));
    assert!(blobs.contains(&blob).unwrap());
    assert_eq!(
        handle
            .submit_source(
                Priority::Live,
                source("after refusal", 16, bytes.len()),
                Arc::new(|| Ok(()))
            )
            .await
            .unwrap()
            .admitted,
        1
    );
    let cursor: Value =
        sqlx::query_scalar("SELECT cursor FROM raw.positions WHERE binding='synthetic-refusal'")
            .fetch_one(&mut reader)
            .await
            .unwrap();
    assert_eq!(cursor["offset"], bytes.len());
    drop(handle);
    writer.await.unwrap();
}

#[devkit::test]
async fn temporary_database_errors_remain_retryable(db: devkit::pg::TestDatabase) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    sqlx::query("SET statement_timeout = '5ms'")
        .execute(&mut conn)
        .await
        .unwrap();
    let timeout = sqlx::query("SELECT pg_sleep(1)")
        .execute(&mut conn)
        .await
        .unwrap_err();
    assert!(matches!(
        admission_error(timeout.into()),
        WriteError::Unavailable(_)
    ));
    sqlx::query("SET statement_timeout = 0")
        .execute(&mut conn)
        .await
        .unwrap();
    let invalid = sqlx::query("SELECT 'invalid'::integer")
        .execute(&mut conn)
        .await
        .unwrap_err();
    assert!(matches!(
        admission_error(invalid.into()),
        WriteError::Refused(_)
    ));
    let outcome = admit_checked(
        &mut conn,
        &batch("after-timeout"),
        &sinex_store::NoDerivations,
        None,
        &|| Ok(()),
    )
    .await
    .unwrap();
    assert_eq!(outcome.admitted, 1);
}

#[devkit::test]
async fn only_transient_sqlstates_are_retryable(db: devkit::pg::TestDatabase) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    for (code, temporary) in [
        ("08006", true),
        ("40001", true),
        ("40P01", true),
        ("53100", true),
        ("57014", true),
        ("58030", true),
        ("22021", false),
        ("23505", false),
        ("54000", false),
        ("42601", false),
    ] {
        let query = format!(
            "DO $$ BEGIN RAISE EXCEPTION USING ERRCODE = '{code}', MESSAGE = 'synthetic admission failure'; END $$"
        );
        let error = sqlx::query(&query).execute(&mut conn).await.unwrap_err();
        assert_eq!(
            matches!(admission_error(error.into()), WriteError::Unavailable(_)),
            temporary,
            "{code}"
        );
    }
    for error in [
        sqlx::Error::Io(std::io::Error::other("synthetic")),
        sqlx::Error::PoolClosed,
        sqlx::Error::PoolTimedOut,
        sqlx::Error::WorkerCrashed,
    ] {
        assert!(matches!(
            admission_error(error.into()),
            WriteError::Unavailable(_)
        ));
    }
}

#[devkit::test]
async fn schema_is_validated_after_waiting_for_the_writer_claim(db: devkit::pg::TestDatabase) {
    let url = db.url(None);
    let mut upgrade = PgConnection::connect(&url).await.unwrap();
    let mut tx = upgrade.begin().await.unwrap();
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(sinex_schema::WRITER_LOCK)
        .execute(&mut *tx)
        .await
        .unwrap();
    let waiting =
        tokio::spawn(async move { claim(PgConnection::connect(&url).await.unwrap()).await });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert!(!waiting.is_finished());
    sqlx::query("UPDATE sinex_schema.applied SET version=version+1")
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let error = waiting.await.unwrap().unwrap_err();
    assert!(error.contains("schema version"));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM privacy.settings")
            .fetch_one(&mut upgrade)
            .await
            .unwrap(),
        0
    );
    // A refused version check closes its claimed session.
    let won: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
        .bind(sinex_schema::WRITER_LOCK)
        .fetch_one(&mut upgrade)
        .await
        .unwrap();
    assert!(won);
}

#[devkit::test]
async fn reconnect_reclaims_and_reloads_privacy(db: devkit::pg::TestDatabase) {
    let url = db.url(None);
    let mut connection = claim(PgConnection::connect(&url).await.unwrap())
        .await
        .unwrap();
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut connection)
        .await
        .unwrap();
    let (handle, queues) = channels();
    let writer = tokio::spawn(run(
        url.clone(),
        connection,
        queues,
        Arc::new(sinex_store::NoDerivations),
        None,
    ));
    handle.submit(Priority::Live, batch("warm")).await.unwrap();
    let mut changes = PgConnection::connect(&url).await.unwrap();
    sinex_store::privacy::set_source_class(&mut changes, "t", sinex_model::Class::Guarded, "test")
        .await
        .unwrap();
    sqlx::query("SELECT pg_terminate_backend($1, 5000)")
        .bind(pid)
        .execute(&mut changes)
        .await
        .unwrap();
    assert!(
        handle
            .submit(Priority::Live, batch("lost connection"))
            .await
            .is_err()
    );
    let outcome = handle
        .submit(Priority::Live, batch("new connection"))
        .await
        .unwrap();
    let class: String = sqlx::query_scalar("SELECT class FROM core.events WHERE id=$1")
        .bind(outcome.admitted_ids[0].0)
        .fetch_one(&mut changes)
        .await
        .unwrap();
    assert_eq!(class, "guarded");
    assert!(outcome.timings.contains_key("queue"));
    assert!(outcome.timings.contains_key("commit"));
    drop(handle);
    writer.await.unwrap();
}
