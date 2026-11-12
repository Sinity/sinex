use super::*;
use sinex_source_core::{DrainOutput, RecordError, SourceSpec};
use std::sync::Mutex;
use tokio::sync::oneshot;

/// Pause after more batches than the former read-ahead channel could hold.
/// The old streaming writer had to own its transaction to reach this point.
struct PausedParser {
    source: Box<dyn Source>,
    reached: Mutex<Option<oneshot::Sender<()>>>,
    release: Mutex<Option<oneshot::Receiver<()>>>,
    _alive: oneshot::Sender<()>,
}

impl Source for PausedParser {
    fn spec(&self) -> &SourceSpec {
        self.source.spec()
    }

    fn drainer<'a>(
        &'a self,
        input: DrainInput<'a>,
    ) -> std::result::Result<
        Box<dyn Iterator<Item = std::result::Result<DrainOutput, RecordError>> + Send + 'a>,
        RecordError,
    > {
        let chunks = self.source.drainer(input)?;
        Ok(Box::new(chunks.enumerate().map(move |(index, chunk)| {
            if index == 5
                && let Some(reached) = self.reached.lock().unwrap().take()
            {
                let _ = reached.send(());
                let _ = self.release.lock().unwrap().take().unwrap().blocking_recv();
            }
            chunk
        })))
    }
}

async fn context(db: &devkit::pg::TestDatabase) -> (tempfile::TempDir, ReaderContext) {
    let directory = tempfile::tempdir().unwrap();
    let mut context = ReaderContext::open(
        &db.url(None),
        &directory.path().join("store"),
        sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0).unwrap(),
    )
    .await
    .unwrap();
    let text: String = (1..=10)
        .map(|i| format!(": {i}:0;original-{i}\n"))
        .collect();
    let path = history(directory.path(), &text);
    ingest(&mut context, &path, &mut AppendState::default()).await;
    (directory, context)
}

async fn reader(context: &mut ReaderContext) -> (SequenceReader, Value) {
    let scope = Scope {
        batch_events: Some(1),
        ..whole_binding()
    };
    let materials = replay::select(
        &mut context.read_conn,
        &[],
        scope.binding.as_deref(),
        None,
        None,
        None,
    )
    .await
    .unwrap();
    let inputs = replay::inputs_of(&mut context.read_conn, &materials)
        .await
        .unwrap();
    let sequence = replay::materials_of(&mut context.read_conn, &inputs, &materials)
        .await
        .unwrap();
    let parsers = sources_of(&mut context.read_conn, &sequence).await.unwrap();
    let mut saved_scope = scope.to_json();
    saved_scope["identity"] = build_identity().unwrap();
    saved_scope["selected_materials"] = json!(materials);
    saved_scope["sealed"] = json!(false);
    (
        SequenceReader {
            blobs: context.blobs.clone(),
            sequence,
            parsers,
            in_scope: materials.into_iter().collect(),
            scope,
            operation: OperationId::new(),
            keys: HashMap::new(),
            unlock: None,
        },
        saved_scope,
    )
}

fn pause(
    reader: &mut SequenceReader,
) -> (
    oneshot::Receiver<()>,
    oneshot::Sender<()>,
    oneshot::Receiver<()>,
) {
    let (reached, wait) = oneshot::channel();
    let (release, resume) = oneshot::channel();
    let (alive, dropped) = oneshot::channel();
    let parser = reader.parsers.remove(0);
    reader.parsers.insert(
        0,
        NativeReplaySource {
            host: parser.host,
            source: Box::new(PausedParser {
                source: parser.source,
                reached: Mutex::new(Some(reached)),
                release: Mutex::new(Some(resume)),
                _alive: alive,
            }),
        },
    );
    (wait, release, dropped)
}

#[devkit::test]
async fn live_admission_commits_while_replay_parser_is_paused_then_replay_resumes_after_restart(
    db: devkit::pg::TestDatabase,
) {
    let (directory, mut context) = context(&db).await;
    let original = live_ids(&mut context.read_conn).await;
    let (mut reader, scope) = reader(&mut context).await;
    let operation = reader.operation;
    let (reached, release, _) = pause(&mut reader);
    let writer = context.writer.clone();
    let replaying =
        tokio::spawn(async move { reader.replace(&writer, "test".into(), scope, false).await });
    reached.await.unwrap();
    let live_path = history(
        &directory.path().join("live"),
        ": 20:0;live-during-replay\n",
    );
    let source = zsh::source("h".into());
    let acquired = zsh::driver(live_path, "h".into())
        .poll(&Value::Null, &mut AppendState::default())
        .unwrap();
    let sink = Sink(context.blobs.clone());
    for batch in drivers::batches(
        &source,
        "live@h",
        RunId::new(),
        acquired,
        Value::Null,
        &sink,
        Limits::default(),
    )
    .unwrap()
    {
        context.writer.submit(Priority::Live, batch).await.unwrap();
    }
    let during = live_ids(&mut context.read_conn).await;
    assert!(original.iter().all(|id| during.contains(id)));
    assert_eq!(during.len(), original.len() + 1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM audit.archived WHERE operation_id=$1")
            .bind(operation.0)
            .fetch_one(&mut context.read_conn)
            .await
            .unwrap(),
        0,
        "preparation cannot expose a partial replacement"
    );
    release.send(()).unwrap();
    let (read, _, _) = replaying.await.unwrap().unwrap();
    assert_eq!(read.events_read, 10);
    let after = live_ids(&mut context.read_conn).await;
    assert_eq!(after.len(), 11);
    assert!(original.iter().all(|id| !after.contains(id)));
    assert!(
        live_commands(&mut context.read_conn)
            .await
            .contains(&"live-during-replay".to_owned())
    );
    context.close().await.unwrap();
    let mut context = ReaderContext::open(
        &db.url(None),
        &directory.path().join("store"),
        sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0).unwrap(),
    )
    .await
    .unwrap();
    let mut request = options(whole_binding(), false);
    request.resume = Some(operation);
    let resumed = run(
        &mut context.read_conn,
        &context.blobs,
        &context.writer,
        request,
    )
    .await
    .unwrap();
    assert_eq!((resumed.sequences, resumed.sequences_skipped), (0, 1));
    assert_eq!(live_ids(&mut context.read_conn).await, after);
    context.close().await.unwrap();
}

#[devkit::test]
async fn cancelled_replay_preparation_keeps_all_old_readings_and_can_retry(
    db: devkit::pg::TestDatabase,
) {
    let (_directory, mut context) = context(&db).await;
    let original = live_ids(&mut context.read_conn).await;
    let (mut reader, scope) = reader(&mut context).await;
    let (reached, release, dropped) = pause(&mut reader);
    let writer = context.writer.clone();
    let replaying =
        tokio::spawn(async move { reader.replace(&writer, "test".into(), scope, false).await });
    reached.await.unwrap();
    replaying.abort();
    assert!(replaying.await.unwrap_err().is_cancelled());
    release.send(()).unwrap();
    let _ = dropped.await;
    assert_eq!(live_ids(&mut context.read_conn).await, original);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM core.replayed_sequences")
            .fetch_one(&mut context.read_conn)
            .await
            .unwrap(),
        0
    );
    let replayed = run(
        &mut context.read_conn,
        &context.blobs,
        &context.writer,
        options(whole_binding(), false),
    )
    .await
    .unwrap();
    assert_eq!(replayed.events_read, 10);
    let after = live_ids(&mut context.read_conn).await;
    assert!(original.iter().all(|id| !after.contains(id)));
    context.close().await.unwrap();
}

#[devkit::test]
async fn sealed_replay_preparation_preserves_ciphertext_and_requires_its_unlock(
    db: devkit::pg::TestDatabase,
) {
    let (directory, mut context) = context(&db).await;
    let path = history(
        &directory.path().join("sealed"),
        ": 30:0;synthetic-sealed-one\n: 31:0;synthetic-sealed-two\n",
    );
    let identity = sinex_sealing::Identity::generate();
    let recipient = identity.to_public();
    let unlock = crate::material_access::sealing::Unlock::new(
        identity,
        std::time::Duration::from_secs(3600),
    )
    .unwrap();
    let sink = crate::material_access::sealing::SealingSink::new(context.blobs.clone(), recipient);
    let source = zsh::source("h".into());
    let acquired = zsh::driver(path, "h".into())
        .poll(&Value::Null, &mut AppendState::default())
        .unwrap();
    let prepared = drivers::prepare(
        &source,
        "sealed@h",
        RunId::new(),
        acquired,
        Value::Null,
        &sink,
        Limits {
            max_events: 1,
            ..Limits::default()
        },
    )
    .unwrap();
    for batch in prepared.batches().unwrap() {
        let mut batch = [batch.unwrap()];
        sink.seal(&mut batch).unwrap();
        context
            .writer
            .submit(Priority::Bulk, batch.into_iter().next().unwrap())
            .await
            .unwrap();
    }
    let materials = replay::select(
        &mut context.read_conn,
        &[],
        Some("sealed@h"),
        None,
        None,
        None,
    )
    .await
    .unwrap();
    let scope = Scope {
        materials: materials.clone(),
        batch_events: Some(1),
        ..Scope::default()
    };
    let before = live_ids(&mut context.read_conn).await;
    let locked = run(
        &mut context.read_conn,
        &context.blobs,
        &context.writer,
        options(scope.clone(), false),
    )
    .await
    .unwrap_err();
    assert!(
        locked
            .to_string()
            .contains("sealed replay needs an operator unlock")
    );
    assert_eq!(live_ids(&mut context.read_conn).await, before);
    let key = unlock
        .data_key(&mut context.read_conn, MaterialId(materials[0]))
        .await
        .unwrap();
    let mut request = options(scope.clone(), false);
    request.unlock = Some(unlock.clone());
    let replayed = run(
        &mut context.read_conn,
        &context.blobs,
        &context.writer,
        request,
    )
    .await
    .unwrap();
    assert_eq!(replayed.events_read, 2);
    let payloads: Vec<Value> =
        sqlx::query_scalar("SELECT payload FROM core.events WHERE run_id=$1 ORDER BY ts_orig,id")
            .bind(replayed.operation.0)
            .fetch_all(&mut context.read_conn)
            .await
            .unwrap();
    assert_eq!(payloads.len(), 2);
    let commands: Vec<_> = payloads
        .iter()
        .map(|payload| {
            assert!(sinex_sealing::is_sealed(payload));
            assert!(!payload.to_string().contains("synthetic-sealed"));
            sinex_sealing::unseal_payload(payload, &key.key).unwrap()["command"].clone()
        })
        .collect();
    assert_eq!(
        commands,
        vec![json!("synthetic-sealed-one"), json!("synthetic-sealed-two")]
    );
    let after = live_ids(&mut context.read_conn).await;
    unlock.revoke();
    let mut request = options(scope, false);
    request.unlock = Some(unlock);
    assert!(
        run(
            &mut context.read_conn,
            &context.blobs,
            &context.writer,
            request,
        )
        .await
        .is_err()
    );
    assert_eq!(live_ids(&mut context.read_conn).await, after);
    context.close().await.unwrap();
}
