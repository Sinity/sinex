use super::{EventBatcher, EventBatcherConfig, EventTransport};
use crate::runtime::nats_publisher::NatsPublisher;
use async_nats::jetstream;
use futures::StreamExt;
use sinex_primitives::{
    DynamicPayload, Id, JsonValue, Uuid,
    events::{Event, EventId},
};
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;
use tokio::sync::{mpsc, oneshot};
use xtask::sandbox::{TestResult, sinex_test};

async fn remove_if_exists(path: &Path) -> TestResult<()> {
    match tokio::fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

async fn ensure_events_stream(
    client: &async_nats::Client,
    env: &sinex_primitives::environment::SinexEnvironment,
) -> TestResult<()> {
    jetstream::new(client.clone())
        .get_or_create_stream(jetstream::stream::Config {
            name: env.nats_stream_name("EVENTS"),
            subjects: vec![env.nats_subject("events.raw.>")],
            storage: jetstream::stream::StorageType::Memory,
            ..Default::default()
        })
        .await?;
    Ok(())
}

fn test_event(name: &str, ok: bool) -> sinex_primitives::Result<Event<JsonValue>> {
    let mut event = DynamicPayload::new("dlq.test", name, serde_json::json!({ "ok": ok }))
        .from_parents([EventId::from_uuid(Uuid::now_v7())])?
        .build()?;
    event.id = Some(Id::new());
    Ok(event)
}

#[sinex_test]
async fn recovery_spool_write_failure_is_propagated() -> TestResult<()> {
    let temp_dir = tempdir()?;
    let recovery_spool_path = temp_dir.path().join("sinex_event_recovery_spool.jsonl");
    let original_permissions = fs::metadata(temp_dir.path())?.permissions();
    let mut read_only = original_permissions.clone();
    read_only.set_readonly(true);
    fs::set_permissions(temp_dir.path(), read_only)?;

    let event = DynamicPayload::new(
        "dlq.test",
        "recovery_spool.failure",
        serde_json::json!({"ok": true}),
    )
    .from_parents([EventId::from_uuid(Uuid::now_v7())])?
    .build()
    .expect("infallible: test provenance set");
    let result =
        EventBatcher::store_recovery_spool_events_at_path(&[event], &recovery_spool_path).await;

    fs::set_permissions(temp_dir.path(), original_permissions)?;
    assert!(result.is_err());
    Ok(())
}

#[sinex_test]
async fn recovery_spool_write_uses_provided_work_directory() -> TestResult<()> {
    let temp_dir = tempdir()?;
    let work_dir = temp_dir.path().to_path_buf();
    let recovery_spool_path = work_dir.join("sinex_event_recovery_spool.jsonl");

    let event = DynamicPayload::new(
        "dlq.test",
        "recovery_spool.path",
        serde_json::json!({"ok": true}),
    )
    .from_parents([EventId::from_uuid(Uuid::now_v7())])?
    .build()
    .expect("infallible: test provenance set");

    remove_if_exists(&recovery_spool_path).await?;
    EventBatcher::store_recovery_spool_events(&[event], &recovery_spool_path).await?;
    assert!(
        recovery_spool_path.exists(),
        "expected recovery spool at {recovery_spool_path:?}"
    );
    Ok(())
}

#[sinex_test]
async fn leftover_recovery_spool_events_are_republished_on_startup(
    ctx: xtask::sandbox::TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    ensure_events_stream(&ctx.nats_client(), ctx.env()).await?;

    let work_dir = tempdir()?;
    let recovery_spool_path = work_dir.path().join("sinex_event_recovery_spool.jsonl");
    let event = test_event("recovery_spool.recovered", true)?;
    let subject = ctx.env().nats_raw_event_subject_with_namespace(
        None,
        event.source.as_str(),
        event.event_type.as_str(),
    );
    let mut subscription = ctx.nats_client().subscribe(subject).await?;

    EventBatcher::store_recovery_spool_events_at_path(&[event], &recovery_spool_path).await?;

    let (_sender, receiver) = mpsc::channel(1);
    let (_shutdown_tx, shutdown_rx) = oneshot::channel();
    let batcher = EventBatcher::new(
        EventTransport::Nats(Arc::new(NatsPublisher::new(ctx.nats_client()))),
        EventBatcherConfig::default(),
        receiver,
        shutdown_rx,
        work_dir.path().to_path_buf(),
    );
    batcher.recover_recovery_spool_events().await?;

    let message = tokio::time::timeout(Duration::from_secs(5), subscription.next())
        .await?
        .expect("replayed recovery-spool event should be published");
    // The recovery path publishes via `publish_intent`, so the message
    // payload is an `EventIntent` envelope — the event_type lives under
    // `events[0]`, not at the top level. (Inherited assertion bug: the
    // raw-event-to-intent switch in #1653 left this asserting `event_type`
    // at the envelope root, where it is always null.)
    let payload: JsonValue = serde_json::from_slice(&message.payload)?;
    assert_eq!(
        payload["events"][0]["event_type"],
        "recovery_spool.recovered"
    );
    assert!(
        tokio::fs::metadata(&recovery_spool_path).await.is_err(),
        "fully replayed recovery spool should be removed"
    );
    Ok(())
}

#[sinex_test]
async fn malformed_recovery_spool_entries_are_preserved_during_replay(
    ctx: xtask::sandbox::TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    ensure_events_stream(&ctx.nats_client(), ctx.env()).await?;

    let work_dir = tempdir()?;
    let recovery_spool_path = work_dir.path().join("sinex_event_recovery_spool.jsonl");
    let event = test_event("recovery_spool.partial_recovery", true)?;
    let valid_line = serde_json::to_string(&event)?;
    EventBatcher::rewrite_recovery_spool_file(
        &[valid_line, "{not-json".to_string()],
        &recovery_spool_path,
    )
    .await?;

    let (_sender, receiver) = mpsc::channel(1);
    let (_shutdown_tx, shutdown_rx) = oneshot::channel();
    let batcher = EventBatcher::new(
        EventTransport::Nats(Arc::new(NatsPublisher::new(ctx.nats_client()))),
        EventBatcherConfig::default(),
        receiver,
        shutdown_rx,
        work_dir.path().to_path_buf(),
    );
    batcher.recover_recovery_spool_events().await?;

    let contents = tokio::fs::read_to_string(&recovery_spool_path).await?;
    assert!(
        contents.contains("{not-json"),
        "malformed recovery-spool entry should remain for manual inspection"
    );
    assert!(
        !contents.contains("recovery_spool.partial_recovery"),
        "successfully replayed entries should be removed from the preserved recovery spool"
    );
    Ok(())
}

/// Proves the `Direct` transport routes a batch synchronously to its
/// admission closure without any NATS infrastructure: the closure captures
/// the delivered events, and after `send_batch` the captured set matches the
/// sent set by both count and event identity, and the input batch is drained.
#[sinex_test]
async fn direct_transport_send_batch_delivers_to_closure() -> TestResult<()> {
    use std::sync::Mutex;

    let delivered: Arc<Mutex<Vec<Event<JsonValue>>>> = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&delivered);
    let transport = EventTransport::new_direct(move |events| {
        let sink = Arc::clone(&sink);
        Box::pin(async move {
            sink.lock()
                .expect("delivered-events mutex should not be poisoned")
                .extend(events);
            Ok(())
        })
    });

    let work_dir = tempdir()?;
    let (_sender, receiver) = mpsc::channel(1);
    let (_shutdown_tx, shutdown_rx) = oneshot::channel();
    let mut batcher = EventBatcher::new(
        transport,
        EventBatcherConfig::default(),
        receiver,
        shutdown_rx,
        work_dir.path().to_path_buf(),
    );

    let first = test_event("direct.first", true)?;
    let second = test_event("direct.second", true)?;
    let expected_ids = vec![first.id, second.id];
    let mut batch = vec![first, second];

    batcher.send_batch(&mut batch).await?;

    assert!(
        batch.is_empty(),
        "Direct send_batch must drain the input batch on success"
    );
    let captured = delivered
        .lock()
        .expect("delivered-events mutex should not be poisoned");
    assert_eq!(
        captured.len(),
        2,
        "Direct path must deliver every event in the batch"
    );
    let captured_ids: Vec<_> = captured.iter().map(|event| event.id).collect();
    assert_eq!(
        captured_ids, expected_ids,
        "Direct path must deliver the same events (by identity) that were sent"
    );
    Ok(())
}

/// Proves a `Direct` admission closure that returns an error does not drop
/// silently: the events are routed to the local recovery spool so they can be
/// replayed, exactly as the NATS publish-failure path does.
#[sinex_test]
async fn direct_transport_failure_routes_to_recovery_spool() -> TestResult<()> {
    let transport = EventTransport::new_direct(|_events| {
        Box::pin(async {
            Err(sinex_primitives::SinexError::processing(
                "admission rejected",
            ))
        })
    });

    let work_dir = tempdir()?;
    let recovery_spool_path = work_dir.path().join("sinex_event_recovery_spool.jsonl");
    let (_sender, receiver) = mpsc::channel(1);
    let (_shutdown_tx, shutdown_rx) = oneshot::channel();
    let mut batcher = EventBatcher::new(
        transport,
        EventBatcherConfig::default(),
        receiver,
        shutdown_rx,
        work_dir.path().to_path_buf(),
    );

    let mut batch = vec![test_event("direct.failed", true)?];
    // send_batch swallows the failure and spools; it returns Ok once spooled.
    batcher.send_batch(&mut batch).await?;

    assert!(
        tokio::fs::metadata(&recovery_spool_path).await.is_ok(),
        "Direct admission failure must persist events to the recovery spool"
    );
    let contents = tokio::fs::read_to_string(&recovery_spool_path).await?;
    assert!(
        contents.contains("direct.failed"),
        "recovery spool must contain the undelivered Direct event"
    );
    Ok(())
}

#[sinex_test]
async fn direct_transport_reports_nats_required_operations() -> TestResult<()> {
    let transport = EventTransport::new_noop_direct();

    let error = transport
        .nats_publisher()
        .expect_err("Direct transport should not expose a NATS publisher");
    let message = error.to_string();

    assert!(
        message.contains("Direct transport does not provide a NATS publisher"),
        "NATS-required call sites should receive an explicit Direct-transport error: {message}"
    );
    Ok(())
}
