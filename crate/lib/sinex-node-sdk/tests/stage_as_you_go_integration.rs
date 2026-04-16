use std::sync::Arc;
use std::time::Duration;
use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};

use async_nats::jetstream;
use serde_json::json;
use sinex_db::models::Event;
use sinex_node_sdk::acquisition_manager::AcquisitionManager;
use sinex_node_sdk::nats_publisher::NatsPublisher;
use sinex_node_sdk::stage_as_you_go::{LogFileStageNode, StageAsYouGoContext, StageAsYouGoNode};
use sinex_node_sdk::{EventBatcherConfig, EventTransport, spawn_event_batcher};
// Channel size constant - not in sinex_primitives::constants, use local
const DEFAULT_EVENT_CHANNEL_SIZE: usize = 1000;
use sinex_primitives::JsonValue;
use tokio::io::{AsyncRead, ReadBuf};
use tokio::sync::{mpsc, oneshot};
use tracing::info;
use uuid::Uuid;
use xtask::sandbox::prelude::*;
use xtask::sandbox::timing::{Timeouts, WaitHelpers};
use xtask::sandbox::{TestIngestdConfig, start_test_ingestd_with_config};

struct FailingReader {
    bytes: Vec<u8>,
    offset: usize,
    failed: bool,
}

impl FailingReader {
    fn new(bytes: impl Into<Vec<u8>>) -> Self {
        Self {
            bytes: bytes.into(),
            offset: 0,
            failed: false,
        }
    }
}

impl AsyncRead for FailingReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.offset < self.bytes.len() {
            let remaining = &self.bytes[self.offset..];
            let to_copy = remaining.len().min(buf.remaining());
            buf.put_slice(&remaining[..to_copy]);
            self.offset += to_copy;
            return Poll::Ready(Ok(()));
        }

        if !self.failed {
            self.failed = true;
            return Poll::Ready(Err(io::Error::other("synthetic read failure")));
        }

        Poll::Ready(Ok(()))
    }
}

#[sinex_test]
async fn stage_as_you_go_pipeline_end_to_end(ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_nats().shared().await?;
    let nats_client = ctx.nats_client();
    let jetstream: jetstream::Context = ctx.jetstream().await?;
    let work_dir = tempfile::tempdir()?;
    let namespace = ctx.pipeline_namespace().prefix().to_string();
    AcquisitionManager::bootstrap_streams_with_namespace(&nats_client, Some(&namespace)).await?;
    let ingest_config = TestIngestdConfig {
        nats: ctx.nats_handle()?.connection_config(),
        database_url: ctx.database_url().to_string(),
        work_dir: Some(work_dir.path().to_path_buf()),
        namespace: Some(namespace.clone()),
        ..Default::default()
    };

    let mut ingest_handle = start_test_ingestd_with_config(ingest_config, Some(&ctx)).await?;
    WaitHelpers::wait_for_condition(
        || {
            let js = jetstream.clone();
            let stream_name = ingest_handle.stream_name.clone();
            async move {
                Ok::<bool, xtask::sandbox::SinexError>(js.get_stream(&stream_name).await.is_ok())
            }
        },
        Timeouts::STANDARD,
    )
    .await?;

    let (event_tx, event_rx) = mpsc::channel::<Event<JsonValue>>(DEFAULT_EVENT_CHANNEL_SIZE);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let publisher = Arc::new(NatsPublisher::with_namespace(
        nats_client.clone(),
        Some(namespace.clone()),
    ));
    let node_batch_config = EventBatcherConfig {
        batch_size: 1,
        batch_timeout_ms: 100,
    };
    let batcher_handle = spawn_event_batcher(
        EventTransport::Nats(publisher),
        node_batch_config,
        event_rx,
        shutdown_rx,
        std::env::temp_dir(),
    );

    let context = StageAsYouGoContext::from_sender(
        Arc::new(AcquisitionManager::new_with_namespace(
            nats_client.clone(),
            sinex_node_sdk::RotationPolicy::default(),
            "integration-log".to_string(),
            Some(namespace.clone()),
        )),
        event_tx,
        false,
    );
    let mut node = LogFileStageNode::new(context, "integration-log");

    let content = b"alpha\nbeta\ngamma\n";
    let metadata = serde_json::json!({ "integration": true });

    let result = node
        .process_with_staging(content, Some("file:///tmp/integration.log"), metadata)
        .await?;

    assert_eq!(result.bytes_processed, content.len());
    assert_eq!(result.event_ids.len(), 3, "expected one event per line");

    WaitHelpers::wait_for_condition(
        || {
            let pool = ctx.pool.clone();
            let material_id = result.source_material_id;
            async move {
                let row = sqlx::query!(
                    r#"
                        SELECT status
                        FROM raw.source_material_registry
                        WHERE id = $1
                    "#,
                    material_id
                )
                .fetch_one(&pool)
                .await?;
                Ok::<bool, xtask::sandbox::SinexError>(row.status == "completed")
            }
        },
        Timeouts::STANDARD,
    )
    .await?;

    let material_row = sqlx::query!(
        r#"
            SELECT
                status,
                (metadata->>'total_bytes')::bigint AS "total_bytes?",
                metadata->>'encoding' AS encoding
            FROM raw.source_material_registry
            WHERE id = $1
        "#,
        Uuid::from(result.source_material_id)
    )
    .fetch_one(&ctx.pool)
    .await?;

    assert_eq!(material_row.status.as_str(), "completed");
    assert_eq!(material_row.total_bytes, Some(content.len() as i64));
    assert_eq!(material_row.encoding.as_deref(), Some("utf-8"));

    WaitHelpers::wait_for_condition(
        || {
            let pool = ctx.pool.clone();
            let material_id = result.source_material_id;
            let expected = result.event_ids.len() as i64;
            async move {
                let count: Option<i64> = sqlx::query_scalar!(
                    "SELECT COUNT(*) FROM core.events WHERE source_material_id = $1::uuid",
                    material_id
                )
                .fetch_one(&pool)
                .await?;
                Ok::<bool, xtask::sandbox::SinexError>(count.unwrap_or(0) == expected)
            }
        },
        Timeouts::STANDARD,
    )
    .await?;

    let observed_events: i64 = sqlx::query_scalar!(
        "SELECT COUNT(*) FROM core.events WHERE source_material_id = $1::uuid",
        Uuid::from(result.source_material_id)
    )
    .fetch_one(&ctx.pool)
    .await?
    .unwrap_or(0);
    assert_eq!(observed_events, result.event_ids.len() as i64);

    WaitHelpers::wait_for_condition(
        || {
            let js = jetstream.clone();
            let stream_name = ingest_handle.stream_name.clone();
            let expected = result.event_ids.len() as u64;
            async move {
                let mut stream = js
                    .get_stream(&stream_name)
                    .await
                    .map_err(|e| SinexError::network(e.to_string()))?;
                let info = stream
                    .info()
                    .await
                    .map_err(|e| SinexError::network(e.to_string()))?;
                Ok::<bool, SinexError>(info.state.messages >= expected)
            }
        },
        Timeouts::STANDARD,
    )
    .await?;

    let _ = shutdown_tx.send(());
    batcher_handle.await??;
    ingest_handle.stop().await?;
    Ok(())
}

#[sinex_test]
async fn stage_as_you_go_reconciliation_cancels_stale_materials(ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_nats().shared().await?;
    let nats_client = ctx.nats_client();
    AcquisitionManager::bootstrap_streams(&nats_client).await?;

    let acquisition = Arc::new(AcquisitionManager::with_defaults(
        nats_client.clone(),
        "reconciliation-log",
    ));

    let (event_tx, mut event_rx) = mpsc::channel::<Event<JsonValue>>(DEFAULT_EVENT_CHANNEL_SIZE);
    let context = StageAsYouGoContext::from_sender(acquisition, event_tx, false);
    let material_id = context
        .register_in_flight("reconciliation", None, json!({}))
        .await?;

    let summary = context
        .reconcile_inflight_older_than(Duration::from_millis(0))
        .await?;

    assert_eq!(summary.cancelled, 1);
    assert_eq!(summary.errors, 0);
    assert_eq!(summary.skipped, 0);

    // No events should have been emitted for the cancelled material.
    assert!(event_rx.try_recv().is_err());

    // Subsequent reconciliation runs should do nothing.
    let second_summary = context
        .reconcile_inflight_older_than(Duration::from_millis(0))
        .await?;
    assert_eq!(second_summary.cancelled, 0);

    info!(material_id = %material_id, "Reconciliation cancelled stale material");
    Ok(())
}

#[sinex_test]
async fn stage_as_you_go_reconciliation_ignores_unrepresentable_stale_ttl(
    ctx: TestContext,
) -> Result<()> {
    let ctx = ctx.with_nats().shared().await?;
    let nats_client = ctx.nats_client();
    AcquisitionManager::bootstrap_streams(&nats_client).await?;

    let acquisition = Arc::new(AcquisitionManager::with_defaults(
        nats_client.clone(),
        "reconciliation-max-ttl-log",
    ));

    let (event_tx, mut event_rx) = mpsc::channel::<Event<JsonValue>>(DEFAULT_EVENT_CHANNEL_SIZE);
    let context = StageAsYouGoContext::from_sender(acquisition, event_tx, false);
    let material_id = context
        .register_in_flight("reconciliation-max-ttl", None, json!({}))
        .await?;

    let summary = context.reconcile_inflight_older_than(Duration::MAX).await?;

    assert_eq!(summary.cancelled, 0);
    assert_eq!(summary.errors, 0);
    assert_eq!(summary.skipped, 0);
    assert!(
        context.material_started_at(material_id).await.is_some(),
        "overflowing stale TTL must not immediately cancel in-flight material"
    );
    assert!(event_rx.try_recv().is_err());

    Ok(())
}

#[sinex_test]
async fn stage_as_you_go_stream_failure_retains_reconcilable_state(ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_nats().shared().await?;
    let nats_client = ctx.nats_client();
    AcquisitionManager::bootstrap_streams(&nats_client).await?;

    let acquisition = Arc::new(AcquisitionManager::with_defaults(
        nats_client.clone(),
        "stream-failure-log",
    ));

    let (event_tx, _event_rx) = mpsc::channel::<Event<JsonValue>>(DEFAULT_EVENT_CHANNEL_SIZE);
    let context = StageAsYouGoContext::from_sender(acquisition, event_tx, false);
    let material_id = context
        .register_in_flight("stream-failure", None, json!({ "case": "reader-error" }))
        .await?;

    let mut reader = FailingReader::new(b"partial payload".to_vec());
    let error = context
        .finalize_source_material_stream(material_id, &mut reader, Some("text/plain"), None)
        .await
        .expect_err("reader failure should abort streaming finalization");
    assert!(
        error.to_string().contains("synthetic read failure"),
        "unexpected error: {error}"
    );

    assert!(
        context.material_started_at(material_id).await.is_some(),
        "failed stream finalization must retain the acquisition handle for reconciliation"
    );

    let summary = context
        .reconcile_inflight_older_than(Duration::from_millis(0))
        .await?;
    assert_eq!(summary.cancelled, 1);
    assert_eq!(summary.errors, 0);
    assert_eq!(summary.skipped, 0);

    Ok(())
}

#[sinex_test]
async fn stage_as_you_go_reconciliation_preserves_state_when_cancel_fails(
    ctx: TestContext,
) -> Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let nats_client = ctx.nats_client();
    AcquisitionManager::bootstrap_streams(&nats_client).await?;

    let acquisition = Arc::new(AcquisitionManager::with_defaults(
        nats_client,
        "cancel-failure-log",
    ));

    let (event_tx, mut event_rx) = mpsc::channel::<Event<JsonValue>>(DEFAULT_EVENT_CHANNEL_SIZE);
    let context = StageAsYouGoContext::from_sender(acquisition, event_tx, false);
    let material_id = context
        .register_in_flight("cancel-failure", None, json!({ "case": "nats-down" }))
        .await?;

    ctx.nats_handle()?.shutdown().await?;

    let summary = context
        .reconcile_inflight_older_than(Duration::from_millis(0))
        .await?;

    assert_eq!(summary.cancelled, 0);
    assert_eq!(summary.errors, 1);
    assert_eq!(summary.skipped, 0);
    assert!(
        context.material_started_at(material_id).await.is_some(),
        "failed stale cancellation must preserve the acquisition handle for retry"
    );
    assert!(event_rx.try_recv().is_err());

    Ok(())
}
