//! `JetStream` stress/regression tests for ingestd pipeline throughput.

use async_nats::jetstream;
use serde_json::json;
use sinex_db::DbPoolExt;
use sinex_primitives::{EventSource, Uuid, error::SinexError};
use xtask::sandbox::prelude::*;
use xtask::sandbox::timing::WaitHelpers;

fn is_stream_not_found<E: std::fmt::Display>(err: &E) -> bool {
    let message = err.to_string();
    message.contains("stream not found") || message.contains("error code 10059")
}

async fn dlq_message_count(
    js: &jetstream::Context,
    stream: &str,
) -> Result<Option<u64>, SinexError> {
    match js.get_stream(stream).await {
        Ok(mut stream) => {
            let info = stream
                .info()
                .await
                .map_err(|e| SinexError::network(e.to_string()))?;
            Ok(Some(info.state.messages))
        }
        Err(err) if is_stream_not_found(&err) => Ok(None),
        Err(err) => Err(SinexError::network(err.to_string())),
    }
}

/// Helper to publish raw bytes directly to `JetStream` (for DLQ testing)
async fn publish_raw_bytes(
    nats_client: &async_nats::Client,
    namespace: &str,
    source: &str,
    event_type: &str,
    bytes: &[u8],
) -> TestResult<()> {
    let env = sinex_primitives::environment();
    let subject = env.nats_subject_with_namespace(
        Some(namespace),
        &format!(
            "events.raw.{}.{}",
            source.replace('.', "_"),
            event_type.replace('.', "_")
        ),
    );
    nats_client.publish(subject, bytes.to_vec().into()).await?;
    nats_client.flush().await?;
    Ok(())
}

#[sinex_test]
async fn jetstream_pipeline_handles_burst_without_timeouts() -> TestResult<()> {
    let ctx = TestContext::new().await?;
    let ctx = ctx.with_nats().shared().await?;
    let pipeline = ctx.pipeline().await?;

    let source = "stress.pipeline";
    let event_type = "burst.event";

    let total = 200usize;
    pipeline
        .publish_batch_simple(total, source, event_type)
        .await?;

    pipeline.shutdown().await?;
    Ok(())
}

#[sinex_test]
async fn jetstream_pipeline_restart_keeps_dlq_flowing() -> TestResult<()> {
    let ctx = TestContext::new().await?;
    let ctx = ctx.with_nats().shared().await?;
    let namespace = ctx.pipeline_namespace().prefix().to_string();
    let base_stream = ctx.pipeline_namespace().stream("SINEX_RAW_EVENTS");
    let dlq_stream = format!("{base_stream}_DLQ");

    let nats = ctx.nats_handle()?;
    let js = nats.jetstream_with_client(ctx.nats_client());

    let pipeline = ctx.pipeline().await?;
    publish_raw_bytes(
        &ctx.nats_client(),
        &namespace,
        "restart.dlq",
        "restart.bad",
        b"{not-json",
    )
    .await?;

    WaitHelpers::wait_for_condition(
        || {
            let js = js.clone();
            let dlq_stream = dlq_stream.clone();
            async move {
                match dlq_message_count(&js, &dlq_stream).await? {
                    Some(count) => Ok::<bool, SinexError>(count >= 1),
                    None => Ok::<bool, SinexError>(false),
                }
            }
        },
        20,
    )
    .await?;
    pipeline.shutdown().await?;

    let pipeline = ctx.pipeline().await?;
    let start_count = ctx
        .pool
        .events()
        .count_by_source(&EventSource::from_static("restart.dlq"))
        .await? as usize;
    pipeline
        .publish(DynamicPayload::new(
            "restart.dlq",
            "restart.ok",
            json!({"seq": 1}),
        ))
        .await?;
    WaitHelpers::wait_for_source_events(&ctx.pool, "restart.dlq", start_count + 1, 20).await?;
    pipeline.shutdown().await?;

    nats.assert_log_does_not_contain(&["[ERR]", "[FTL]"], 200)?;
    Ok(())
}

#[sinex_test]
async fn jetstream_pipeline_dedupes_duplicate_event_ids() -> TestResult<()> {
    let ctx = TestContext::new().await?;
    let ctx = ctx.with_nats().shared().await?;
    let pipeline = ctx.pipeline().await?;

    let source = "stress.dedupe";
    let event_type = "dedupe.event";
    let event_id = Uuid::now_v7();
    let overrides = EventOverrides {
        id: Some(event_id),
        ..Default::default()
    };

    let start_count = ctx
        .pool
        .events()
        .count_by_source(&EventSource::new(source)?)
        .await? as usize;

    // Publish 50 events with the same event_id through the pipeline.
    // Each gets a unique NATS message, but the DB should dedup on event_id.
    for _idx in 0..50 {
        pipeline
            .publish_with_overrides(
                DynamicPayload::new(source, event_type, json!({"dedup": true})),
                overrides.clone(),
            )
            .await?;
    }

    WaitHelpers::wait_for_source_events(&ctx.pool, source, start_count + 1, 20).await?;
    let count = ctx
        .pool
        .events()
        .count_by_source(&EventSource::new(source)?)
        .await?;
    assert_eq!(
        count as usize,
        start_count + 1,
        "duplicate event IDs should only be persisted once"
    );

    pipeline.shutdown().await?;
    Ok(())
}

#[sinex_test]
async fn jetstream_pipeline_routes_invalid_burst_to_dlq() -> TestResult<()> {
    let ctx = TestContext::new().await?;
    let ctx = ctx.with_nats().shared().await?;
    let namespace = ctx.pipeline_namespace().prefix().to_string();
    let base_stream = ctx.pipeline_namespace().stream("SINEX_RAW_EVENTS");
    let dlq_stream = format!("{base_stream}_DLQ");

    let nats = ctx.nats_handle()?;
    let js = nats.jetstream_with_client(ctx.nats_client());

    let pipeline = ctx.pipeline().await?;
    let start_count = dlq_message_count(&js, &dlq_stream).await?.unwrap_or(0);

    let total = 25u64;
    for idx in 0..total {
        let payload = format!("{{bad:{idx}}}");
        publish_raw_bytes(
            &ctx.nats_client(),
            &namespace,
            "stress.dlq",
            "burst.bad",
            payload.as_bytes(),
        )
        .await?;
    }

    WaitHelpers::wait_for_condition(
        || {
            let js = js.clone();
            let dlq_stream = dlq_stream.clone();
            async move {
                match dlq_message_count(&js, &dlq_stream).await? {
                    Some(count) => Ok::<bool, SinexError>(count >= start_count + total),
                    None => Ok::<bool, SinexError>(false),
                }
            }
        },
        20,
    )
    .await?;

    pipeline.shutdown().await?;
    Ok(())
}

#[sinex_test]
async fn jetstream_pipeline_handles_mixed_valid_and_invalid_bursts() -> TestResult<()> {
    let ctx = TestContext::new().await?;
    let ctx = ctx.with_nats().shared().await?;
    let pipeline = ctx.pipeline().await?;

    let namespace = ctx.pipeline_namespace().prefix().to_string();
    let base_stream = ctx.pipeline_namespace().stream("SINEX_RAW_EVENTS");
    let dlq_stream = format!("{base_stream}_DLQ");
    let source = "stress.mixed";
    let event_type = "mixed.ok";

    let nats = ctx.nats_handle()?;
    let js = nats.jetstream_with_client(ctx.nats_client());

    let start_count = ctx
        .pool
        .events()
        .count_by_source(&EventSource::new(source)?)
        .await? as usize;
    let dlq_start = dlq_message_count(&js, &dlq_stream).await?.unwrap_or(0);

    let valid_total = 100usize;
    let invalid_total = 20u64;

    for idx in 0..valid_total {
        pipeline
            .publish(DynamicPayload::new(source, event_type, json!({"seq": idx})))
            .await?;
    }
    for idx in 0..invalid_total {
        let payload = format!("{{bad:{idx}}}");
        publish_raw_bytes(
            &ctx.nats_client(),
            &namespace,
            source,
            "mixed.bad",
            payload.as_bytes(),
        )
        .await?;
    }

    WaitHelpers::wait_for_source_events(&ctx.pool, source, start_count + valid_total, 20).await?;
    WaitHelpers::wait_for_condition(
        || {
            let js = js.clone();
            let dlq_stream = dlq_stream.clone();
            async move {
                match dlq_message_count(&js, &dlq_stream).await? {
                    Some(count) => Ok::<bool, SinexError>(count >= dlq_start + invalid_total),
                    None => Ok::<bool, SinexError>(false),
                }
            }
        },
        20,
    )
    .await?;

    pipeline.shutdown().await?;
    Ok(())
}
