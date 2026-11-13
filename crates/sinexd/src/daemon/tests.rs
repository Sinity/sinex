use super::*;

#[tokio::test]
async fn shutdown_summary_keeps_unjoined_completions_and_actual_cancellations() {
    let mut tasks = JoinSet::new();
    let writer = tasks.spawn(async { ("writer", Ok(())) });
    let (entered, ready) = tokio::sync::oneshot::channel();
    tasks.spawn(async move {
        entered.send(()).unwrap();
        std::future::pending::<()>().await;
        ("held", Ok(()))
    });
    ready.await.unwrap();
    while !writer.is_finished() {
        tokio::task::yield_now().await;
    }
    let mut summary = ShutdownSummary {
        requested: tasks.len(),
        ..Default::default()
    };
    assert_eq!(summary.requested, 2, "completed but unjoined tasks count");
    summary.record(tasks.join_next().await.unwrap());
    summary.abort_requested = tasks.len();
    tasks.abort_all();
    while let Some(done) = tasks.join_next().await {
        summary.record(done);
    }
    assert_eq!(summary.joined, 2);
    assert!(summary.writer_joined);
    assert_eq!(summary.abort_requested, 1);
    assert_eq!(summary.cancelled, 1);
    assert_eq!(summary.failed, 0);
    assert_eq!(summary.panicked, 0);
    let retained = serde_json::to_value(summary).unwrap();
    assert_eq!(retained["tasks"][0]["name"], "writer");
    assert_eq!(retained["tasks"][1]["outcome"], "cancelled");
    assert!(retained["tasks"][1]["name"].is_null());
}

#[devkit::test]
async fn shutdown_cleanup_releases_the_writer_claim_even_on_failure(db: devkit::pg::TestDatabase) {
    let url = db.url(None);
    let dir = tempfile::tempdir().unwrap();
    let blobs = Blobs::open(dir.path()).unwrap();
    forget_admitted(&url, &blobs).await.unwrap();
    let mut conn = connect(&url).await.unwrap();
    assert!(
        sinex_store::writer_lock::try_claim(&mut conn)
            .await
            .unwrap()
    );
    conn.close().await.unwrap();
    // Exercise the real extent flush error, with bytes queued for shutdown.
    blobs.put(b"synthetic pending shutdown bytes").unwrap();
    let mut fault = connect(&url).await.unwrap();
    sqlx::query("DROP TABLE core.blob_extents")
        .execute(&mut fault)
        .await
        .unwrap();
    fault.close().await.unwrap();
    assert!(forget_admitted(&url, &blobs).await.is_err());
    let mut conn = connect(&url).await.unwrap();
    assert!(
        sinex_store::writer_lock::try_claim(&mut conn)
            .await
            .unwrap()
    );
    conn.close().await.unwrap();
}

fn shutdown_batch(tag: &str) -> sinex_model::Batch {
    use sinex_model::{
        Batch, BatchKind, Class, Event, IngressFrame, MaterialId, Origin, ReadingOrder,
        RecordLocation, RecordOutcome, RunId, Timestamp, event_types::CommandImported,
    };
    let event = Event::build(
        &CommandImported {
            command: tag.into(),
            hostname: "synthetic".into(),
            started_at: Some(Timestamp::from_unix_secs(1).unwrap()),
            header_elapsed: Some(0),
            paths: vec![],
            n: 0,
        },
        "synthetic-shutdown",
        Origin::Record {
            location: RecordLocation {
                material: MaterialId::new(),
                anchor: 0,
            },
            output_index: 0,
        },
        ReadingOrder(1),
        *blake3::hash(tag.as_bytes()).as_bytes(),
        Class::Open,
    )
    .unwrap();
    Batch {
        run: RunId::new(),
        kind: BatchKind::Ingress(Box::new(IngressFrame {
            content_hash: Some(blake3::hash(tag.as_bytes()).to_hex().to_string()),
            receipt_number: None,
            coverage: None,
            native_source: None,
            source_fingerprint: None,
            host: None,
            family: None,
            write_kind_prefixes: vec![],
            assign_orders: vec![],
            producer: "synthetic-shutdown".into(),
            frame: tag.into(),
            material: None,
            refusal: None,
            declarations: vec![],
            source: None,
            event_types: vec![],
        })),
        records: vec![RecordOutcome::Events {
            location: None,
            events: vec![event],
        }],
        archive: vec![],
    }
}

#[devkit::test]
async fn interrupted_writer_rolls_back_before_shutdown_and_exact_frame_retry(
    db: devkit::pg::TestDatabase,
) {
    use sinex_store::NoDerivations;
    let url = db.url(None);
    let dir = tempfile::tempdir().unwrap();
    let blobs = Blobs::open(dir.path()).unwrap();
    let mut observer = connect(&url).await.unwrap();
    sqlx::raw_sql(
        "CREATE TABLE public.shutdown_probe(id uuid PRIMARY KEY);
        CREATE FUNCTION public.hold_shutdown_insert() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          INSERT INTO public.shutdown_probe VALUES(NEW.id);
          PERFORM pg_advisory_xact_lock(927161);
          RETURN NEW;
        END $$;
        CREATE TRIGGER hold_shutdown_insert AFTER INSERT ON core.events
        FOR EACH ROW WHEN (NEW.payload->>'command' LIKE 'held-%')
        EXECUTE FUNCTION public.hold_shutdown_insert()",
    )
    .execute(&mut observer)
    .await
    .unwrap();
    let committed = shutdown_batch("acknowledged");
    let mut connection = writer::claim(connect(&url).await.unwrap()).await.unwrap();
    let mut tx = connection.begin().await.unwrap();
    let acknowledged = tx.admit(&committed, &NoDerivations).await.unwrap();
    tx.commit().await.unwrap();
    connection.close().await.unwrap();
    let receipt:serde_json::Value=sqlx::query_scalar("SELECT to_jsonb(f) FROM core.ingress_frames f WHERE producer='synthetic-shutdown' AND frame='acknowledged'")
        .fetch_one(&mut observer).await.unwrap();
    for detects_disconnect in [false, true] {
        let mut blocker = connect(&url).await.unwrap();
        sqlx::query("SELECT pg_advisory_lock(927161)")
            .execute(&mut blocker)
            .await
            .unwrap();
        let mut connection = writer::claim(connect(&url).await.unwrap()).await.unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, String>("SHOW client_connection_check_interval")
                .fetch_one(&mut *connection)
                .await
                .unwrap(),
            "1s"
        );
        if !detects_disconnect {
            // Reproduce the preceding connection behavior, even after joining
            // the aborted future: its server statement still owns the claim.
            sqlx::query("SET client_connection_check_interval=0")
                .execute(&mut *connection)
                .await
                .unwrap();
        }
        let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *connection)
            .await
            .unwrap();
        let (handle, queues) = writer::channels();
        let mut tasks = JoinSet::new();
        let database = url.clone();
        tasks.spawn(async move {
            writer::run(database, connection, queues, Arc::new(NoDerivations), None).await;
            ("writer", Ok(()))
        });
        let pending = shutdown_batch(if detects_disconnect {
            "held-new"
        } else {
            "held-old"
        });
        let submitted = pending.clone();
        let waiting =
            tokio::spawn(async move { handle.submit(writer::Priority::Live, submitted).await });
        tokio::time::timeout(Duration::from_secs(5),async {
            loop {
                let held:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND wait_event_type='Lock')")
                    .bind(pid).fetch_one(&mut observer).await.unwrap();
                if held {break;}
                tokio::task::yield_now().await;
            }
        }).await.expect("actual writer statement reached its held transaction");
        if detects_disconnect {
            finish_tasks(&mut tasks, Duration::ZERO, &url, &blobs, true)
                .await
                .unwrap();
        } else {
            tasks.abort_all();
            while tasks.join_next().await.is_some() {}
            assert!(sqlx::query_scalar::<_,bool>("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND wait_event_type='Lock')")
                .bind(pid).fetch_one(&mut observer).await.unwrap(),"old disconnect left the native writer alive");
            assert!(
                !sinex_store::writer_lock::try_claim(&mut observer)
                    .await
                    .unwrap()
            );
            assert!(
                sqlx::query_scalar::<_, bool>("SELECT pg_cancel_backend($1)")
                    .bind(pid)
                    .fetch_one(&mut observer)
                    .await
                    .unwrap()
            );
        }
        assert!(
            matches!(waiting.await.unwrap(), Err(writer::WriteError::Stopped)),
            "an interrupted frame has no acknowledgment"
        );
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let exists: bool = sqlx::query_scalar(
                    "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1)",
                )
                .bind(pid)
                .fetch_one(&mut observer)
                .await
                .unwrap();
                if !exists {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("native writer backend exited");
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM public.shutdown_probe")
                .fetch_one(&mut observer)
                .await
                .unwrap(),
            if detects_disconnect { 1 } else { 0 }
        );
        let after:serde_json::Value=sqlx::query_scalar("SELECT to_jsonb(f) FROM core.ingress_frames f WHERE producer='synthetic-shutdown' AND frame='acknowledged'")
            .fetch_one(&mut observer).await.unwrap();
        assert_eq!(
            after, receipt,
            "prior committed acknowledgment is unchanged"
        );
        let frame = match &pending.kind {
            sinex_model::BatchKind::Ingress(f) => &f.frame,
            _ => unreachable!(),
        };
        assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM core.ingress_frames WHERE producer='synthetic-shutdown' AND frame=$1")
            .bind(frame).fetch_one(&mut observer).await.unwrap(),0,"unfinished frame and receipt rolled back");
        sqlx::query("SELECT pg_advisory_unlock(927161)")
            .execute(&mut blocker)
            .await
            .unwrap();
        blocker.close().await.unwrap();
        let mut fresh = writer::claim(connect(&url).await.unwrap()).await.unwrap();
        let mut tx = fresh.begin().await.unwrap();
        let retried = tx.admit(&pending, &NoDerivations).await.unwrap();
        assert_eq!(retried.admitted, 1);
        let duplicate = tx.admit(&pending, &NoDerivations).await.unwrap();
        assert_eq!(retried.commit_seq, duplicate.commit_seq);
        assert_eq!(retried.admitted_ids, duplicate.admitted_ids);
        tx.commit().await.unwrap();
        fresh.close().await.unwrap();
    }
    assert_eq!(acknowledged.admitted, 1);
}

#[devkit::test]
async fn shutdown_cannot_succeed_when_native_writer_cleanup_fails(db: devkit::pg::TestDatabase) {
    let url = db.url(None);
    let dir = tempfile::tempdir().unwrap();
    let blobs = Blobs::open(dir.path()).unwrap();
    blobs.put(b"synthetic pending shutdown bytes").unwrap();
    let mut conn = connect(&url).await.unwrap();
    sqlx::query("DROP TABLE core.blob_extents")
        .execute(&mut conn)
        .await
        .unwrap();
    conn.close().await.unwrap();
    let mut tasks = JoinSet::new();
    assert!(
        finish_tasks(&mut tasks, Duration::ZERO, &url, &blobs, true)
            .await
            .is_err()
    );
    let fresh = writer::claim(connect(&url).await.unwrap()).await.unwrap();
    fresh.close().await.unwrap();
}
