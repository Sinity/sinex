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
    let mut conn = connect(&url, "sinex.test").await.unwrap();
    assert!(
        sinex_store::writer_lock::try_claim(&mut conn)
            .await
            .unwrap()
    );
    conn.close().await.unwrap();
    // Exercise the real extent flush error, with bytes queued for shutdown.
    blobs.put(b"synthetic pending shutdown bytes").unwrap();
    let mut fault = connect(&url, "sinex.test").await.unwrap();
    sqlx::query("DROP TABLE core.blob_extents")
        .execute(&mut fault)
        .await
        .unwrap();
    fault.close().await.unwrap();
    assert!(forget_admitted(&url, &blobs).await.is_err());
    let mut conn = connect(&url, "sinex.test").await.unwrap();
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
    let mut observer = connect(&url, "sinex.test").await.unwrap();
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
    let mut connection = writer::claim(connect(&url, "sinex.test").await.unwrap())
        .await
        .unwrap();
    let mut tx = connection.begin().await.unwrap();
    let acknowledged = tx.admit(&committed, &NoDerivations).await.unwrap();
    tx.commit().await.unwrap();
    connection.close().await.unwrap();
    let receipt:serde_json::Value=sqlx::query_scalar("SELECT to_jsonb(f) FROM core.ingress_frames f WHERE producer='synthetic-shutdown' AND frame='acknowledged'")
        .fetch_one(&mut observer).await.unwrap();
    for detects_disconnect in [false, true] {
        let mut blocker = connect(&url, "sinex.test").await.unwrap();
        sqlx::query("SELECT pg_advisory_lock(927161)")
            .execute(&mut blocker)
            .await
            .unwrap();
        let mut connection = writer::claim(connect(&url, "sinex.test").await.unwrap())
            .await
            .unwrap();
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
        let mut fresh = writer::claim(connect(&url, "sinex.test").await.unwrap())
            .await
            .unwrap();
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
    let mut conn = connect(&url, "sinex.test").await.unwrap();
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
    let fresh = writer::claim(connect(&url, "sinex.test").await.unwrap())
        .await
        .unwrap();
    fresh.close().await.unwrap();
}

#[devkit::test]
async fn shutdown_releases_source_and_backup_waiting_on_publication(db: devkit::pg::TestDatabase) {
    use futures::FutureExt;
    use sinex_store::NoDerivations;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let url = db.url(None);
    let root = tempfile::tempdir().unwrap();
    let blobs = Blobs::open(&root.path().join("store")).unwrap();
    let original = b"synthetic retained source record\n";
    let blob = blobs.put(original).unwrap();
    let mut observer = connect(&url, "sinex.test").await.unwrap();
    sqlx::raw_sql(
        "CREATE TABLE public.backup_shutdown_probe(id uuid PRIMARY KEY);
        CREATE FUNCTION public.hold_backup_shutdown() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN INSERT INTO public.backup_shutdown_probe VALUES(NEW.id);
          PERFORM pg_advisory_xact_lock(927162); RETURN NEW; END $$;
        CREATE TRIGGER hold_backup_shutdown AFTER INSERT ON core.events
        FOR EACH ROW EXECUTE FUNCTION public.hold_backup_shutdown()",
    )
    .execute(&mut observer)
    .await
    .unwrap();
    let mut blocker = connect(&url, "sinex.test").await.unwrap();
    sqlx::query("SELECT pg_advisory_lock(927162)")
        .execute(&mut blocker)
        .await
        .unwrap();
    let mut connection = writer::claim(connect(&url, "sinex.test").await.unwrap())
        .await
        .unwrap();
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *connection)
        .await
        .unwrap();
    let (handle, queues) = writer::channels();
    let publication = handle.source_publication();
    let mut tasks = JoinSet::new();
    let database = url.clone();
    let store = blobs.clone();
    tasks.spawn(async move {
        writer::run(
            database,
            connection,
            queues,
            Arc::new(NoDerivations),
            Some(store),
        )
        .await;
        ("writer", Ok(()))
    });
    let mut pending = shutdown_batch("held-publication");
    let event = pending.records.first_mut().unwrap();
    let sinex_model::RecordOutcome::Events { events, .. } = event else {
        unreachable!()
    };
    let material = match events[0].origin {
        sinex_model::Origin::Record { location, .. } => location.material,
        _ => unreachable!(),
    };
    let progress = sinex_model::SourceProgress {
        source: None,
        host: None,
        binding: "synthetic-backup-shutdown".into(),
        data_keys: vec![],
        input: sinex_model::InputRef {
            kind: "file".into(),
            locator: "synthetic".into(),
            host: "synthetic".into(),
        },
        observation: Default::default(),
        revision: sinex_model::RevisionChange::Appended,
        material: Some(sinex_model::MaterialRef {
            id: material,
            input: None,
            blob: blob.clone(),
            length: original.len() as u64,
            part_index: 0,
            previous_part: None,
            parser: "synthetic".into(),
            parser_version: 1,
            manifest: Default::default(),
        }),
        cursor: serde_json::json!({"offset": original.len()}),
        carry: serde_json::Value::Null,
        memory: None,
    };
    let sinex_model::BatchKind::Ingress(frame) = &mut pending.kind else {
        unreachable!()
    };
    frame.material = Some(progress);
    let submitted = pending.clone();
    let source_handle = handle.clone();
    let (source_done, source_result) = tokio::sync::oneshot::channel();
    tasks.spawn(async move {
        let result = source_handle
            .submit_source(writer::Priority::Bulk, submitted, Arc::new(|| Ok(())), None)
            .await;
        let _ = source_done.send(result);
        ("source", Ok(()))
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let held: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND wait_event_type='Lock')")
                .bind(pid).fetch_one(&mut observer).await.unwrap();
            if held { break; }
            tokio::task::yield_now().await;
        }
    }).await.expect("source owns an admitted writer and publication read guard");
    let broker = devkit::nats::test_nats().unwrap();
    let client = async_nats::connect(broker.url()).await.unwrap();
    let subjects = sinex_protocol::Subjects::sinex();
    let producer = broker.prefix().to_owned();
    use futures::StreamExt;
    let mut requests = client.subscribe(subjects.backup(&producer)).await.unwrap();
    // An ordered broker response proves the subscription is installed.
    async_nats::jetstream::new(client.clone())
        .query_account()
        .await
        .unwrap();
    let responder = tokio::spawn({
        let client = client.clone();
        async move {
            let request = requests.next().await.unwrap();
            client
                .publish(
                    request.reply.unwrap(),
                    serde_json::to_vec(&sinex_protocol::backup::Reply::Unavailable {
                        reason: "synthetic producer unavailable".into(),
                    })
                    .unwrap()
                    .into(),
                )
                .await
                .unwrap();
        }
    });
    let reads = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
        .unwrap();
    let (stop, stopped) = watch::channel(false);
    let api = Api {
        backup_producers: Some((client, subjects, vec![producer])),
        source_config: Default::default(),
        read_failures: Default::default(),
        running_stages: Default::default(),
        sql_cursors: Default::default(),
        stop: stopped.clone(),
        reads,
        writer: Some(handle.clone()),
        blobs: blobs.clone(),
        zone: jiff::tz::TimeZone::get("UTC").unwrap(),
        day_start: jiff::civil::Time::constant(4, 0, 0, 0),
        derivations: Default::default(),
        agent_grants: vec![],
        unlock: Default::default(),
        host: "synthetic".into(),
        library_roots: vec![],
    };
    let socket = root.path().join("api.sock");
    let config = api::Config {
        socket: socket.clone(),
        listen: None,
        tokens: api::Tokens::new(None, None, Some("operator".into())),
    };
    let (_commits, commits) = watch::channel(0);
    let (_models, models) = watch::channel(());
    let mut api_stop = stopped;
    tasks.spawn(async move {
        (
            "api",
            api::serve(config, api, commits, models, async move {
                let _ = api_stop.wait_for(|s| *s).await;
            })
            .await,
        )
    });
    let mut request = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match tokio::net::UnixStream::connect(&socket).await {
                Ok(stream) => break stream,
                Err(_) => tokio::task::yield_now().await,
            }
        }
    })
    .await
    .expect("real API listener");
    let body = serde_json::json!({"method":"backup","action":"prepare",
        "output":root.path().join("backup"),"deadline":null})
    .to_string();
    request.write_all(format!("POST /v1 HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer operator\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(),body).as_bytes()).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            // This new reader can wait only once the backup's write guard is queued.
            if publication.enter().now_or_never().is_none() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("real admitted backup queued on source publication");
    assert!(!root.path().join("backup/boundary.json").exists());
    drop(handle);
    stop.send_replace(true);
    assert!(
        sqlx::query_scalar::<_, bool>("SELECT pg_cancel_backend($1)")
            .bind(pid)
            .fetch_one(&mut observer)
            .await
            .unwrap()
    );
    tokio::time::timeout(
        Duration::from_secs(5),
        finish_tasks(&mut tasks, Duration::from_millis(100), &url, &blobs, true),
    )
    .await
    .expect("shutdown must join source, writer and backup API")
    .unwrap();
    assert!(
        source_result.await.unwrap().is_err(),
        "no acknowledgment on canceled source"
    );
    let mut response = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), request.read_to_end(&mut response))
        .await
        .expect("owned HTTP request released")
        .unwrap();
    responder.await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM public.backup_shutdown_probe")
            .fetch_one(&mut observer)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM raw.positions WHERE binding='synthetic-backup-shutdown'"
        )
        .fetch_one(&mut observer)
        .await
        .unwrap(),
        0
    );
    assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM core.ingress_frames WHERE producer='synthetic-shutdown' AND frame='held-publication'")
        .fetch_one(&mut observer).await.unwrap(), 0);
    assert!(
        !sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1)"
        )
        .bind(pid)
        .fetch_one(&mut observer)
        .await
        .unwrap()
    );
    sqlx::query("SELECT pg_advisory_unlock(927162)")
        .execute(&mut blocker)
        .await
        .unwrap();
    assert_eq!(
        blobs.read(&blob).unwrap(),
        original,
        "pending native raw bytes remain available"
    );
    let mut fresh = writer::claim(connect(&url, "sinex.test").await.unwrap())
        .await
        .unwrap();
    let mut tx = fresh.begin().await.unwrap();
    let admitted = tx.admit(&pending, &NoDerivations).await.unwrap();
    assert_eq!(admitted.admitted, 1);
    let duplicate = tx.admit(&pending, &NoDerivations).await.unwrap();
    assert_eq!(duplicate.commit_seq, admitted.commit_seq);
    tx.commit().await.unwrap();
    fresh.close().await.unwrap();
}

#[devkit::test]
async fn source_copy_cancellation_keeps_pending_frame_and_progress_for_retry(
    db: devkit::pg::TestDatabase,
) {
    use sinex_store::NoDerivations;
    let url = db.url(None);
    let root = tempfile::tempdir().unwrap();
    let blobs = Blobs::open(root.path()).unwrap();
    let original = b"synthetic pending source bytes\n";
    let blob = blobs.put(original).unwrap();
    let mut observer = connect(&url, "sinex.test").await.unwrap();
    sqlx::raw_sql("CREATE TABLE public.source_copy_mutation(ord bigint); CREATE FUNCTION public.hold_source_copy() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO public.source_copy_mutation VALUES(NEW.ord); PERFORM pg_advisory_xact_lock(927164); RETURN NEW; END $$; CREATE TRIGGER hold_source_copy AFTER INSERT ON core.admission_stage FOR EACH ROW WHEN (NEW.ord=0) EXECUTE FUNCTION public.hold_source_copy()")
        .execute(&mut observer).await.unwrap();
    let mut blocker = connect(&url, "sinex.test").await.unwrap();
    sqlx::query("SELECT pg_advisory_lock(927164)")
        .execute(&mut blocker)
        .await
        .unwrap();
    let mut connection = writer::claim(connect(&url, "sinex.test").await.unwrap())
        .await
        .unwrap();
    let pid = sqlx::query_scalar::<_, i32>("SELECT pg_backend_pid()")
        .fetch_one(&mut *connection)
        .await
        .unwrap();
    let (handle, queues) = writer::channels();
    let mut tasks = JoinSet::new();
    let database = url.clone();
    let store = blobs.clone();
    tasks.spawn(async move {
        writer::run(
            database,
            connection,
            queues,
            Arc::new(NoDerivations),
            Some(store),
        )
        .await;
        ("writer", Ok(()))
    });
    let mut pending = shutdown_batch("copy-pending");
    let sinex_model::RecordOutcome::Events { events, .. } = &mut pending.records[0] else {
        unreachable!()
    };
    // First COPY row reaches a native lock; the escaped second row fills the
    // socket so task cancellation exercises the streaming encoder itself.
    let mut large = events[0].clone();
    large.id = sinex_model::EventId::new();
    large.payload["command"] = serde_json::Value::String("\"\\\n🦀".repeat(2 << 20));
    if let sinex_model::Origin::Record { location, .. } = &mut large.origin {
        location.anchor = 1;
    }
    events.push(large);
    let material = match events[0].origin {
        sinex_model::Origin::Record { location, .. } => location.material,
        _ => unreachable!(),
    };
    let sinex_model::BatchKind::Ingress(frame) = &mut pending.kind else {
        unreachable!()
    };
    frame.material = Some(sinex_model::SourceProgress {
        source: None,
        host: None,
        binding: "synthetic-copy-pending".into(),
        data_keys: vec![],
        input: sinex_model::InputRef {
            kind: "file".into(),
            locator: "synthetic".into(),
            host: "synthetic".into(),
        },
        observation: Default::default(),
        revision: sinex_model::RevisionChange::Appended,
        material: Some(sinex_model::MaterialRef {
            id: material,
            input: None,
            blob: blob.clone(),
            length: original.len() as u64,
            part_index: 0,
            previous_part: None,
            parser: "synthetic".into(),
            parser_version: 1,
            manifest: Default::default(),
        }),
        cursor: serde_json::json!({"offset":original.len()}),
        carry: serde_json::Value::Null,
        memory: None,
    });
    let offered = pending.clone();
    let waiting = tokio::spawn(async move {
        handle
            .submit_source(writer::Priority::Bulk, offered, Arc::new(|| Ok(())), None)
            .await
    });
    tokio::time::timeout(Duration::from_secs(5),async {loop {
        let held=sqlx::query_scalar::<_,bool>("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND query LIKE 'COPY core.admission_stage%' AND wait_event_type='Lock')").bind(pid).fetch_one(&mut observer).await.unwrap();
        if held {break;} tokio::task::yield_now().await;
    }}).await.expect("real source COPY reached its held first row");
    tokio::time::timeout(
        Duration::from_secs(5),
        finish_tasks(&mut tasks, Duration::ZERO, &url, &blobs, true),
    )
    .await
    .expect("streaming COPY yields to writer cancellation")
    .unwrap();
    assert!(
        matches!(waiting.await.unwrap(), Err(writer::WriteError::Stopped)),
        "aborted source has no final refusal or ACK"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM public.source_copy_mutation")
            .fetch_one(&mut observer)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM raw.positions WHERE binding='synthetic-copy-pending'"
        )
        .fetch_one(&mut observer)
        .await
        .unwrap(),
        0
    );
    assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM core.ingress_frames WHERE producer='synthetic-shutdown' AND frame='copy-pending'").fetch_one(&mut observer).await.unwrap(),0);
    assert_eq!(blobs.read(&blob).unwrap(), original);
    sqlx::query("SELECT pg_advisory_unlock(927164)")
        .execute(&mut blocker)
        .await
        .unwrap();
    let mut fresh = writer::claim(connect(&url, "sinex.test").await.unwrap())
        .await
        .unwrap();
    let mut tx = fresh.begin().await.unwrap();
    let first = tx.admit(&pending, &NoDerivations).await.unwrap();
    let repeated = tx.admit(&pending, &NoDerivations).await.unwrap();
    assert_eq!(first.admitted, 2);
    assert_eq!(first.commit_seq, repeated.commit_seq);
    assert_eq!(first.admitted_ids, repeated.admitted_ids);
    tx.commit().await.unwrap();
    fresh.close().await.unwrap();
}
