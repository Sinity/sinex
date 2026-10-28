use super::*;
use sinex_model::Timestamp;
use sinex_model::event_types::forecast::{ForecastRecord, Issued};
use sinex_sources::sources::forecast;

fn acquisition(id: &str) -> drivers::Acquired {
    let at = Timestamp::from_unix_secs(1_700_000_000).unwrap();
    forecast::request_material(
        ForecastRecord::Issued(Issued {
            forecast: id.into(),
            target: "synthetic event".into(),
            probability: 0.5,
            criteria: "synthetic completion".into(),
            horizon: Timestamp::from_unix_secs(1_800_000_000).unwrap(),
            information_cutoff: at,
            information: Vec::new(),
            information_position: 0,
            by: "untrusted author".into(),
            at,
        }),
        "operator",
        "synthetic",
        at,
    )
}

#[devkit::test]
async fn request_refuses_all_records_when_one_does_not_parse(db: devkit::pg::TestDatabase) {
    let dir = tempfile::tempdir().unwrap();
    let mut ctx = crate::reader_context::ReaderContext::open(&db.url(None), dir.path())
        .await
        .unwrap();
    let mut acquired = acquisition("first");
    let material = acquired.material.as_mut().unwrap();
    let start = material.bytes.len();
    material
        .bytes
        .extend_from_slice(b"{\"record\":\"not_a_forecast\"}\n");
    material.records.push((start, material.bytes.len()));
    let error = request(
        &mut ctx.read_conn,
        &ctx.writer,
        &ctx.blobs,
        Arc::new(forecast::request_source()),
        "synthetic-request",
        acquired,
    )
    .await
    .err()
    .unwrap();
    assert!(error.contains("failed to read back"), "{error}");
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM core.events")
        .fetch_one(&mut ctx.read_conn)
        .await
        .unwrap();
    assert_eq!(count, 0);
    assert!(
        sinex_store::positions::load(&mut ctx.read_conn, "synthetic-request")
            .await
            .unwrap()
            .is_none()
    );
    ctx.close().await.unwrap();
}

#[devkit::test]
async fn request_commits_its_records_together_with_the_source_class(db: devkit::pg::TestDatabase) {
    let dir = tempfile::tempdir().unwrap();
    let mut ctx = crate::reader_context::ReaderContext::open(&db.url(None), dir.path())
        .await
        .unwrap();
    let mut acquired = acquisition("first");
    let second = acquisition("second").material.unwrap();
    let material = acquired.material.as_mut().unwrap();
    let start = material.bytes.len();
    material.bytes.extend(second.bytes);
    material.records.push((start, material.bytes.len()));
    let submitted = request(
        &mut ctx.read_conn,
        &ctx.writer,
        &ctx.blobs,
        Arc::new(forecast::request_source()),
        "synthetic-request",
        acquired,
    )
    .await
    .unwrap();
    assert_eq!(submitted.outcome.admitted, 2);
    let rows: Vec<(uuid::Uuid, String, serde_json::Value)> =
        sqlx::query_as("SELECT material_id, class::text, payload FROM core.events ORDER BY anchor")
            .fetch_all(&mut ctx.read_conn)
            .await
            .unwrap();
    assert_eq!(rows.len(), 2);
    for (material, class, payload) in rows {
        assert_eq!(Some(MaterialId(material)), submitted.material);
        assert_eq!(class, "guarded");
        assert_eq!(payload["by"], "operator");
    }
    ctx.close().await.unwrap();
}

#[devkit::test]
async fn submission_checks_again_inside_the_writer(db: devkit::pg::TestDatabase) {
    use std::sync::atomic::{AtomicBool, Ordering};
    let dir = tempfile::tempdir().unwrap();
    let mut ctx = crate::reader_context::ReaderContext::open(&db.url(None), dir.path())
        .await
        .unwrap();
    let stopped = Arc::new(AtomicBool::new(false));
    let checked_stop = stopped.clone();
    let result = Intake {
        writer: &ctx.writer,
        source: Arc::new(forecast::request_source()),
        binding: "synthetic-check",
        run: RunId::new(),
        material: materials(ctx.blobs.clone()),
        check: Arc::new(move || {
            if checked_stop.load(Ordering::SeqCst) {
                Err("capture stopped".into())
            } else {
                Ok(())
            }
        }),
        priority: Priority::Bulk,
        atomic: false,
        registered: None,
    }
    .submit_using(
        acquisition("first"),
        Value::Null,
        &mut Totals::default(),
        |batch, check| {
            // Cancellation after the caller's final check must still refuse admission.
            stopped.store(true, Ordering::SeqCst);
            let writer = &ctx.writer;
            async move {
                writer
                    .submit_checked(Priority::Bulk, batch, check)
                    .await
                    .map_err(|e| e.to_string())
            }
        },
    )
    .await;
    assert!(result.err().unwrap().contains("capture stopped"));
    assert!(
        sinex_store::positions::load(&mut ctx.read_conn, "synthetic-check")
            .await
            .unwrap()
            .is_none()
    );
    ctx.close().await.unwrap();
}

mod explicit_imports;

struct CountedSource {
    source: Box<dyn Source>,
    parsed: Arc<std::sync::atomic::AtomicUsize>,
    second: Arc<tokio::sync::Notify>,
    closed: Arc<std::sync::atomic::AtomicBool>,
}

impl Source for CountedSource {
    fn spec(&self) -> &sinex_source_core::SourceSpec {
        self.source.spec()
    }
    fn drainer<'a>(
        &'a self,
        mut input: sinex_source_core::DrainInput<'a>,
    ) -> Result<
        Box<
            dyn Iterator<
                    Item = Result<sinex_source_core::DrainOutput, sinex_source_core::RecordError>,
                > + Send
                + 'a,
        >,
        sinex_source_core::RecordError,
    > {
        input.limits.max_events = 1;
        let batches = self.source.drainer(input)?;
        struct Counted<'a> {
            batches: Box<
                dyn Iterator<
                        Item = Result<
                            sinex_source_core::DrainOutput,
                            sinex_source_core::RecordError,
                        >,
                    > + Send
                    + 'a,
            >,
            source: &'a CountedSource,
        }
        impl Iterator for Counted<'_> {
            type Item = Result<sinex_source_core::DrainOutput, sinex_source_core::RecordError>;
            fn next(&mut self) -> Option<Self::Item> {
                let batch = self.batches.next()?;
                if self
                    .source
                    .parsed
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                    == 1
                {
                    self.source.second.notify_one();
                }
                Some(batch)
            }
        }
        impl Drop for Counted<'_> {
            fn drop(&mut self) {
                self.source
                    .closed
                    .store(true, std::sync::atomic::Ordering::SeqCst);
            }
        }
        Ok(Box::new(Counted {
            batches,
            source: self,
        }))
    }
}

fn three_records() -> drivers::Acquired {
    let mut acquired = acquisition("first");
    for id in ["second", "third"] {
        let next = acquisition(id).material.unwrap();
        let material = acquired.material.as_mut().unwrap();
        let start = material.bytes.len();
        material.bytes.extend(next.bytes);
        material.records.push((start, material.bytes.len()));
    }
    acquired
}

async fn overlap(fail_first: bool) {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    let parsed = Arc::new(AtomicUsize::new(0));
    let closed = Arc::new(AtomicBool::new(false));
    let second = Arc::new(tokio::sync::Notify::new());
    let source = CountedSource {
        source: Box::new(forecast::request_source()),
        parsed: parsed.clone(),
        second: second.clone(),
        closed: closed.clone(),
    };
    let dir = tempfile::tempdir().unwrap();
    let blobs = Blobs::open(dir.path()).unwrap();
    let (writer, _queues) = crate::writer::channels();
    let intake = Intake {
        writer: &writer,
        source: Arc::new(source),
        binding: "synthetic-overlap",
        run: RunId::new(),
        material: materials(blobs),
        check: Arc::new(|| Ok(())),
        priority: Priority::Bulk,
        atomic: false,
        registered: None,
    };
    let mut submitted = 0;
    let mut anchors = Vec::new();
    let mut totals = Totals::default();
    let result = intake
        .submit_using(three_records(), Value::Null, &mut totals, |batch, check| {
            submitted += 1;
            let first = submitted == 1;
            let second = second.clone();
            let parsed = parsed.clone();
            let location = match &batch.records[0] {
                RecordOutcome::Events { location, .. } => location.unwrap(),
                RecordOutcome::Failure { .. } => panic!("synthetic records must parse"),
            };
            anchors.push(location.anchor);
            async move {
                check()?;
                if first {
                    // The admission answer is held until the blocking parser produces batch two.
                    tokio::time::timeout(std::time::Duration::from_secs(5), second.notified())
                        .await
                        .unwrap();
                    assert_eq!(parsed.load(Ordering::SeqCst), 2);
                    if fail_first {
                        return Err("first commit failed".into());
                    }
                }
                check()?;
                Ok(Outcome {
                    admitted: batch.event_count(),
                    ..Outcome::default()
                })
            }
        })
        .await;
    assert!(
        closed.load(Ordering::SeqCst),
        "the parser must be joined before returning"
    );
    if fail_first {
        assert_eq!(result.err().unwrap(), "first commit failed");
        assert_eq!(submitted, 1);
        assert_eq!(parsed.load(Ordering::SeqCst), 2);
    } else {
        assert_eq!(result.unwrap().outcome.admitted, 3);
        assert_eq!(submitted, 3);
        assert!(anchors.windows(2).all(|a| a[0] < a[1]));
    }
}

#[tokio::test(flavor = "current_thread")]
async fn parsing_the_next_batch_overlaps_the_commit_answer() {
    overlap(false).await;
}

#[tokio::test(flavor = "current_thread")]
async fn failed_commit_discards_the_batch_ahead_and_joins_the_parser() {
    overlap(true).await;
}

struct SlowSink {
    entered: std::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
    sink: crate::material_access::Sink,
}
impl SlowSink {
    fn wait(&self) -> Result<(), String> {
        let entered = self.entered.lock().unwrap().take();
        if let Some(entered) = entered {
            entered.send(()).unwrap();
            self.release
                .lock()
                .unwrap()
                .recv_timeout(std::time::Duration::from_secs(5))
                .map_err(|_| "runtime did not progress during storage".to_owned())?;
        }
        Ok(())
    }
}
impl drivers::MaterialSink for SlowSink {
    fn put_file(&self, path: &std::path::Path) -> Result<String, String> {
        self.wait()?;
        drivers::MaterialSink::put_file(&self.sink, path)
    }

    fn put(&self, bytes: &[u8]) -> Result<String, String> {
        self.wait()?;
        drivers::MaterialSink::put(&self.sink, bytes)
    }
}

#[tokio::test(flavor = "current_thread")]
async fn synchronous_storage_does_not_stop_other_runtime_tasks() {
    let dir = tempfile::tempdir().unwrap();
    let blobs = Blobs::open(dir.path()).unwrap();
    let (entered, waiting) = tokio::sync::oneshot::channel();
    let (release, resume) = std::sync::mpsc::channel();
    let sink = SlowSink {
        entered: std::sync::Mutex::new(Some(entered)),
        release: std::sync::Mutex::new(resume),
        sink: crate::material_access::Sink(blobs),
    };
    let mut batches = blocking::Batches::start(Priority::Live, move |send| {
        let started = Instant::now();
        let source = forecast::request_source();
        let prepared = drivers::prepare(
            &source,
            "synthetic-slow-sink",
            RunId::new(),
            acquisition("first"),
            Value::Null,
            &sink,
            Limits::default(),
        )
        .map_err(|e| e.to_string())?;
        let permit = send.permit().ok_or("submission cancelled")?;
        let batch = prepared
            .batches()
            .map_err(|e| e.to_string())?
            .next()
            .unwrap()
            .map_err(|e| e.to_string())?;
        send.send(blocking::Parsed {
            batch,
            elapsed: started.elapsed(),
            material_length: Some(prepared.material_len()),
            permit,
        });
        Ok(())
    })
    .await
    .unwrap();
    let unrelated = async {
        waiting.await.unwrap();
        release.send(()).unwrap();
    };
    let consume = async {
        let parsed = batches.next().await.unwrap().unwrap();
        let count = parsed.batch.event_count();
        let mut permit = parsed.permit;
        permit.committed();
        drop(permit);
        assert!(batches.next().await.is_none());
        batches.finish().await.unwrap();
        count
    };
    let (count, ()) = tokio::join!(consume, unrelated);
    assert_eq!(count, 1);
}

async fn queued_submission(db: devkit::pg::TestDatabase, cancel: bool) {
    use sqlx::Connection;
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    let dir = tempfile::tempdir().unwrap();
    let blobs = Blobs::open(dir.path()).unwrap();
    let url = db.url(None);
    let connection = sqlx::PgConnection::connect(&url).await.unwrap();
    let (writer, queues) = crate::writer::channels();
    let worker_writer = writer.clone();
    let worker_blobs = blobs.clone();
    let queued = Arc::new(tokio::sync::Notify::new());
    let worker_queued = queued.clone();
    let second = Arc::new(tokio::sync::Notify::new());
    let source = CountedSource {
        source: Box::new(forecast::request_source()),
        parsed: Arc::new(AtomicUsize::new(0)),
        second: second.clone(),
        closed: Arc::new(AtomicBool::new(false)),
    };
    let mut submission = tokio::spawn(async move {
        let intake = Intake {
            writer: &worker_writer,
            source: Arc::new(source),
            binding: "synthetic-queued",
            run: RunId::new(),
            material: materials(worker_blobs),
            check: Arc::new(|| Ok(())),
            priority: Priority::Bulk,
            atomic: false,
            registered: None,
        };
        let mut first = true;
        intake
            .submit_using(
                three_records(),
                Value::Null,
                &mut Totals::default(),
                |batch, check| {
                    let writer = worker_writer.clone();
                    let queued = worker_queued.clone();
                    let first = std::mem::take(&mut first);
                    async move {
                        let mut answer =
                            Box::pin(writer.submit_checked(Priority::Bulk, batch, check));
                        if first {
                            // The empty queue accepts the batch; the writer has not started to answer it.
                            assert!(matches!(
                                futures::poll!(answer.as_mut()),
                                std::task::Poll::Pending
                            ));
                            queued.notify_one();
                        }
                        answer.await.map_err(|e| e.to_string())
                    }
                },
            )
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::join!(queued.notified(), second.notified());
    })
    .await
    .unwrap();
    if cancel {
        submission.abort();
        assert!((&mut submission).await.err().unwrap().is_cancelled());
    }
    let worker = tokio::spawn(crate::writer::run(
        url.clone(),
        connection,
        queues,
        Arc::new(sinex_store::NoDerivations),
        Some(blobs),
    ));
    if !cancel {
        assert_eq!(submission.await.unwrap().unwrap().outcome.admitted, 3);
    }
    drop(writer);
    worker.await.unwrap();
    let mut read = sqlx::PgConnection::connect(&url).await.unwrap();
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM core.events")
        .fetch_one(&mut read)
        .await
        .unwrap();
    assert_eq!(count, if cancel { 0 } else { 3 });
    assert_eq!(
        sinex_store::positions::load(&mut read, "synthetic-queued")
            .await
            .unwrap()
            .is_some(),
        !cancel
    );
}

#[devkit::test]
async fn queued_batches_commit_in_order_after_the_writer_starts(db: devkit::pg::TestDatabase) {
    queued_submission(db, false).await;
}

#[devkit::test]
async fn dropping_a_submission_refuses_its_queued_batch(db: devkit::pg::TestDatabase) {
    queued_submission(db, true).await;
}

fn materials(blobs: Blobs) -> crate::material_access::MaterialAccess {
    crate::material_access::MaterialAccess::new(
        blobs,
        Class::Guarded,
        crate::material_access::Access::default(),
    )
    .unwrap()
}
