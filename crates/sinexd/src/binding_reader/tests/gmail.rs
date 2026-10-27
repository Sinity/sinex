use super::*;
use crate::intake::request;
use serde_json::json;
use sinex_source_core::Reader;
use sinex_sources::sources::gmail::{self, GmailReader, api};
use std::collections::VecDeque;
use std::sync::Mutex;

type ExpectedResponse = (&'static str, Result<Value, api::Error>);

struct Provider(Arc<Mutex<VecDeque<ExpectedResponse>>>);
impl api::Transport for Provider {
    fn get(&self, path: &str, _: &[(&str, &str)]) -> Result<Value, api::Error> {
        let (expected, response) = self
            .0
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected Gmail request");
        assert_eq!(path, expected);
        response
    }
}

fn reader(steps: Vec<ExpectedResponse>) -> GmailReader<Provider> {
    GmailReader::new(
        Provider(Arc::new(Mutex::new(steps.into()))),
        "owner@example.test".into(),
        "synthetic".into(),
    )
}

async fn cursor(ctx: &mut crate::reader_context::ReaderContext) -> Value {
    sinex_store::positions::load(&mut ctx.read_conn, "synthetic-gmail")
        .await
        .unwrap()
        .unwrap()
        .0
}

async fn submit(
    ctx: &mut crate::reader_context::ReaderContext,
    reader: &mut GmailReader<Provider>,
    previous: &Value,
) {
    let acquired = reader.poll(previous).unwrap();
    request(
        &mut ctx.read_conn,
        &ctx.writer,
        &ctx.blobs,
        Arc::new(gmail::source()),
        "synthetic-gmail",
        acquired,
    )
    .await
    .unwrap();
}

#[devkit::test]
async fn gmail_restart_failure_gap_and_replay_use_stored_bytes(db: devkit::pg::TestDatabase) {
    let dir = tempfile::tempdir().unwrap();
    let mut ctx = crate::reader_context::ReaderContext::open(&db.url(None), dir.path())
        .await
        .unwrap();
    let mut first = reader(vec![
        (
            "/profile",
            Ok(json!({"emailAddress":"owner@example.test", "historyId":"baseline"})),
        ),
        (
            "/messages",
            Ok(json!({"messages":[{"id":"one"},{"id":"two"}]})),
        ),
        (
            "/labels",
            Ok(json!({"labels":[{"id":"SENT","name":"Sent"}]})),
        ),
        (
            "/messages/one",
            Ok(
                json!({"id":"one","historyId":"message-history","threadId":"thread","labelIds":["SENT"],"raw":"TWVzc2FnZS1JRDogPHJmY0BleGFtcGxlLnRlc3Q-DQpTdWJqZWN0OiBTeW50aGV0aWMNCg0KYm9keQ"}),
            ),
        ),
        ("/messages/two", Err(api::Error::Status(503))),
    ]);
    submit(&mut ctx, &mut first, &Value::Null).await;
    let baseline = cursor(&mut ctx).await;
    assert!(baseline["material"].is_string());
    submit(&mut ctx, &mut first, &baseline).await;
    let listed = cursor(&mut ctx).await;
    submit(&mut ctx, &mut first, &listed).await;
    let previous = cursor(&mut ctx).await;
    assert!(first.poll(&previous).is_err());
    assert_eq!(cursor(&mut ctx).await, previous);
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM core.events")
        .fetch_one(&mut ctx.read_conn)
        .await
        .unwrap();
    assert_eq!(count, 2);
    drop(first);
    let mut restarted = reader(vec![
        ("/messages/two", Err(api::Error::Status(404))),
        (
            "/history",
            Ok(
                json!({"historyId":"next","history":[{"id":"change","messagesDeleted":[{"message":{"id":"one"}}]}]}),
            ),
        ),
        ("/labels", Ok(json!({"labels":[]}))),
        ("/history", Err(api::Error::Status(404))),
    ]);
    submit(&mut ctx, &mut restarted, &previous).await;
    let after_missing = cursor(&mut ctx).await;
    assert_eq!(after_missing["api"]["mode"], "history");
    assert_eq!(after_missing["api"]["start_history_id"], "baseline");
    submit(&mut ctx, &mut restarted, &after_missing).await;
    let after_history = cursor(&mut ctx).await;
    assert_eq!(after_history["api"]["completed_history_id"], "next");
    submit(&mut ctx, &mut restarted, &after_history).await;
    let gap = cursor(&mut ctx).await;
    assert_eq!(gap["api"]["mode"], "baseline");
    drop(restarted);
    let mut reconciled = reader(vec![("/profile", Ok(json!({"emailAddress":"owner@example.test", "historyId":"new-baseline"})))]);
    submit(&mut ctx, &mut reconciled, &gap).await;
    assert_eq!(cursor(&mut ctx).await["api"]["mode"], "list");
    let before: Vec<(String, Value, String)> = sqlx::query_as(
        "SELECT event_type,payload,class::text FROM core.events ORDER BY event_type",
    )
    .fetch_all(&mut ctx.read_conn)
    .await
    .unwrap();
    assert_eq!(before.len(), 5);
    assert!(before.iter().all(|(_, _, class)| class == "guarded"));
    let mail = before
        .iter()
        .find(|(kind, _, _)| kind == "mail.message")
        .unwrap();
    assert_eq!(mail.1["message_id"], "rfc@example.test");
    assert_eq!(mail.1["labels"], json!(["Sent"]));
    let changed = before
        .iter()
        .find(|(kind, _, _)| kind == "mail.changed")
        .unwrap();
    assert_eq!(changed.1["deleted"][0]["message"]["id"], "one");
    let replay = crate::replay::run(
        &mut ctx.read_conn,
        &ctx.blobs,
        &ctx.writer,
        crate::replay::Options {
            scope: crate::replay::Scope::default(),
            compare: false,
            resume: None,
            actor: "synthetic".into(),
            unlock: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(replay.failures, 0);
    assert_eq!(replay.events_read, 5);
    let after: Vec<(String, Value, String)> = sqlx::query_as(
        "SELECT event_type,payload,class::text FROM core.events ORDER BY event_type",
    )
    .fetch_all(&mut ctx.read_conn)
    .await
    .unwrap();
    assert_eq!(after.len(), 10);
    for event in before {
        assert_eq!(after.iter().filter(|e| **e == event).count(), 2);
    }
    ctx.close().await.unwrap();
}

#[devkit::test]
async fn gmail_backfill_reads_past_baseline_and_empty_pages_in_one_invocation(
    db: devkit::pg::TestDatabase,
) {
    let dir = tempfile::tempdir().unwrap();
    let mut ctx = crate::reader_context::ReaderContext::open(&db.url(None), dir.path())
        .await
        .unwrap();
    let steps = Arc::new(Mutex::new(VecDeque::from(vec![
        (
            "/profile",
            Ok(json!({"emailAddress":"owner@example.test","historyId":"baseline"})),
        ),
        ("/messages", Ok(json!({"nextPageToken":"next"}))),
        ("/labels", Ok(json!({"labels":[]}))),
        ("/messages", Ok(json!({"messages":[{"id":"one"}]}))),
        (
            "/labels",
            Ok(json!({"labels":[{"id":"SENT","name":"Sent"}]})),
        ),
        (
            "/messages/one",
            Ok(
                json!({"id":"one","historyId":"message-history","threadId":"thread","labelIds":["SENT"],"raw":"TWVzc2FnZS1JRDogPHJmY0BleGFtcGxlLnRlc3Q-DQpTdWJqZWN0OiBTeW50aGV0aWMNCg0KYm9keQ"}),
            ),
        ),
        ("/history", Ok(json!({"historyId":"baseline"}))),
        ("/labels", Ok(json!({"labels":[]}))),
    ])));
    let reader = GmailReader::new(
        Provider(steps.clone()),
        "owner@example.test".into(),
        "synthetic".into(),
    );
    sinex_store::bindings::save(
        &mut ctx.read_conn,
        &sinex_store::bindings::Stored {
            binding: "synthetic-gmail-drive".into(),
            source: gmail::SPEC.name.into(),
            input: "owner@example.test".into(),
            settings: json!({"host":"synthetic"}),
        },
    )
    .await
    .unwrap();
    let (_keep, stop) = tokio::sync::watch::channel(false);
    crate::backfill::drive(
        &mut ctx.read_conn,
        &ctx.writer,
        RunId::new(),
        Arc::new(gmail::source()),
        "synthetic-gmail-drive",
        &crate::sealing::Sink(ctx.blobs.clone()),
        crate::sealing::Access::default(),
        Class::Open,
        &mut crate::binding_reader::polled::PolledReader::new(Box::new(reader)),
        stop,
    )
    .await
    .unwrap();
    assert!(steps.lock().unwrap().is_empty());
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM core.events")
        .fetch_one(&mut ctx.read_conn)
        .await
        .unwrap();
    assert_eq!(count, 2);
    let (cursor, _) = sinex_store::positions::load(&mut ctx.read_conn, "synthetic-gmail-drive")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cursor["api"]["mode"], "history");
    assert_eq!(cursor["api"]["page_index"], 0);
    ctx.close().await.unwrap();
}

#[devkit::test]
async fn a_reader_that_changes_only_its_cursor_stops_after_one_poll(db: devkit::pg::TestDatabase) {
    struct Progress(std::sync::Arc<std::sync::atomic::AtomicUsize>);
    impl Reader for Progress {
        fn poll(&mut self, _: &Value) -> Result<sinex_source_core::drivers::Acquired, sinex_source_core::drivers::DriverError> {
            let next = self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
            Ok(sinex_source_core::drivers::Acquired {
                input: sinex_model::InputRef { kind: "synthetic".into(), locator: "progress".into(), host: "synthetic".into() },
                observation: Default::default(),
                revision: sinex_model::RevisionChange::Same,
                material: None,
                cursor_without_material: json!({"next": next}),
            })
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let mut ctx = ReaderContext::open(&db.url(None), dir.path()).await.unwrap();
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut reader = PolledReader::new(Box::new(Progress(calls.clone())));
    sinex_store::bindings::save(&mut ctx.read_conn, &sinex_store::bindings::Stored {
        binding: "synthetic-progress".into(), source: gmail::SPEC.name.into(), input: "progress".into(), settings: json!({"host":"synthetic"}),
    }).await.unwrap();
    let (_keep, stop) = tokio::sync::watch::channel(false);
    crate::backfill::drive(&mut ctx.read_conn, &ctx.writer, RunId::new(), Arc::new(gmail::source()), "synthetic-progress", &crate::sealing::Sink(ctx.blobs.clone()), crate::sealing::Access::default(), Class::Open, &mut reader, stop).await.unwrap();
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    let (cursor, _) = sinex_store::positions::load(&mut ctx.read_conn, "synthetic-progress").await.unwrap().unwrap();
    assert_eq!(cursor["next"], 1);
    ctx.close().await.unwrap();
}
