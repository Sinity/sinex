//! JetStream stress/regression tests for ingestd pipeline throughput.

use async_nats::{jetstream, HeaderMap};
use chrono::Utc;
use serde_json::json;
use sinex_core::{DbPoolExt, EventSource, SinexError, Ulid};
use sinex_test_utils::timing_utils::WaitHelpers;
use sinex_test_utils::{sinex_test, TestContext, TestNodePublisher};

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

#[sinex_test]
async fn jetstream_pipeline_handles_burst_without_timeouts() -> sinex_test_utils::TestResult<()> {
    let ctx = TestContext::new().await?;
    let ctx = ctx.with_nats().shared().await?;
    let pipeline = ctx.pipeline_scope().await?;

    let source = "stress.pipeline";
    let event_type = "burst.event";
    let namespace = ctx.pipeline_namespace().prefix().to_string();
    let publisher =
        TestNodePublisher::with_namespace(ctx.nats_client(), source.to_string(), Some(namespace));

    let start_count = ctx
        .pool
        .events()
        .count_by_source(&EventSource::new(source))
        .await? as usize;

    let total = 200usize;
    for idx in 0..total {
        publisher
            .publish_event(event_type, json!({"seq": idx}))
            .await?;
    }

    WaitHelpers::wait_for_source_events(&ctx.pool, source, start_count + total, 20).await?;

    pipeline.shutdown().await?;
    Ok(())
}

#[sinex_test]
async fn jetstream_pipeline_restart_keeps_dlq_flowing() -> sinex_test_utils::TestResult<()> {
    let ctx = TestContext::new().await?;
    let ctx = ctx.with_nats().shared().await?;
    let namespace = ctx.pipeline_namespace().prefix().to_string();
    let base_stream = ctx.pipeline_namespace().stream("SINEX_RAW_EVENTS");
    let dlq_stream = format!("{base_stream}_DLQ");

    let nats = ctx.nats_handle()?;
    let js = nats.jetstream_with_client(ctx.nats_client());
    let publisher = TestNodePublisher::with_namespace(
        ctx.nats_client(),
        "restart.dlq".to_string(),
        Some(namespace.clone()),
    );

    let pipeline = ctx.pipeline_scope().await?;
    publisher
        .publish_raw_event_bytes("restart.bad", b"{not-json", None)
        .await?;

    WaitHelpers::wait_for_condition(
        || {
            let js = js.clone();
            let dlq_stream = dlq_stream.clone();
            async move {
                match dlq_message_count(&js, &dlq_stream).await? {
                    Some(count) => Ok(count >= 1),
                    None => Ok(false),
                }
            }
        },
        20,
    )
    .await?;
    pipeline.shutdown().await?;

    let pipeline = ctx.pipeline_scope().await?;
    let start_count = ctx
        .pool
        .events()
        .count_by_source(&EventSource::new("restart.dlq"))
        .await? as usize;
    publisher
        .publish_event("restart.ok", json!({"seq": 1}))
        .await?;
    WaitHelpers::wait_for_source_events(&ctx.pool, "restart.dlq", start_count + 1, 20).await?;
    pipeline.shutdown().await?;

    nats.assert_log_does_not_contain(&["[ERR]", "[FTL]"], 200)?;
    Ok(())
}

#[sinex_test]
async fn jetstream_pipeline_dedupes_duplicate_event_ids() -> sinex_test_utils::TestResult<()> {
    let ctx = TestContext::new().await?;
    let ctx = ctx.with_nats().shared().await?;
    let pipeline = ctx.pipeline_scope().await?;

    let namespace = ctx.pipeline_namespace().prefix().to_string();
    let source = "stress.dedupe";
    let event_type = "dedupe.event";
    let event_id = Ulid::new();
    let subject = ctx.env().nats_subject_with_namespace(
        Some(&namespace),
        &format!("events.raw.{source}.{event_type}"),
    );

    let start_count = ctx
        .pool
        .events()
        .count_by_source(&EventSource::new(source))
        .await? as usize;
    let js = ctx.nats_handle()?.jetstream_with_client(ctx.nats_client());
    for idx in 0..50 {
        let message = json!({
            "id": event_id.to_string(),
            "source": source,
            "event_type": event_type,
            "ts_orig": Utc::now().to_rfc3339(),
            "host": "test-host",
            "payload": { "seq": idx },
            "ingestor_version": "test-node"
        });

        let mut headers = HeaderMap::new();
        headers.insert("Nats-Msg-Id", Ulid::new().to_string().as_str());
        js.publish_with_headers(
            subject.clone(),
            headers,
            serde_json::to_vec(&message)?.into(),
        )
        .await?
        .await?;
    }

    WaitHelpers::wait_for_source_events(&ctx.pool, source, start_count + 1, 20).await?;
    let count = ctx
        .pool
        .events()
        .count_by_source(&EventSource::new(source))
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
async fn jetstream_pipeline_routes_invalid_burst_to_dlq() -> sinex_test_utils::TestResult<()> {
    let ctx = TestContext::new().await?;
    let ctx = ctx.with_nats().shared().await?;
    let namespace = ctx.pipeline_namespace().prefix().to_string();
    let base_stream = ctx.pipeline_namespace().stream("SINEX_RAW_EVENTS");
    let dlq_stream = format!("{base_stream}_DLQ");

    let nats = ctx.nats_handle()?;
    let js = nats.jetstream_with_client(ctx.nats_client());
    let publisher = TestNodePublisher::with_namespace(
        ctx.nats_client(),
        "stress.dlq".to_string(),
        Some(namespace),
    );

    let pipeline = ctx.pipeline_scope().await?;
    let start_count = dlq_message_count(&js, &dlq_stream).await?.unwrap_or(0);

    let total = 25u64;
    for idx in 0..total {
        let payload = format!("{{bad:{idx}}}");
        publisher
            .publish_raw_event_bytes("burst.bad", payload.as_bytes(), None)
            .await?;
    }

    WaitHelpers::wait_for_condition(
        || {
            let js = js.clone();
            let dlq_stream = dlq_stream.clone();
            async move {
                match dlq_message_count(&js, &dlq_stream).await? {
                    Some(count) => Ok(count >= start_count + total),
                    None => Ok(false),
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
async fn jetstream_pipeline_handles_mixed_valid_and_invalid_bursts(
) -> sinex_test_utils::TestResult<()> {
    let ctx = TestContext::new().await?;
    let ctx = ctx.with_nats().shared().await?;
    let pipeline = ctx.pipeline_scope().await?;

    let namespace = ctx.pipeline_namespace().prefix().to_string();
    let base_stream = ctx.pipeline_namespace().stream("SINEX_RAW_EVENTS");
    let dlq_stream = format!("{base_stream}_DLQ");
    let source = "stress.mixed";
    let event_type = "mixed.ok";

    let nats = ctx.nats_handle()?;
    let js = nats.jetstream_with_client(ctx.nats_client());
    let publisher =
        TestNodePublisher::with_namespace(ctx.nats_client(), source.to_string(), Some(namespace));

    let start_count = ctx
        .pool
        .events()
        .count_by_source(&EventSource::new(source))
        .await? as usize;
    let dlq_start = dlq_message_count(&js, &dlq_stream).await?.unwrap_or(0);

    let valid_total = 100usize;
    let invalid_total = 20u64;

    for idx in 0..valid_total {
        publisher
            .publish_event(event_type, json!({"seq": idx}))
            .await?;
    }
    for idx in 0..invalid_total {
        let payload = format!("{{bad:{idx}}}");
        publisher
            .publish_raw_event_bytes("mixed.bad", payload.as_bytes(), None)
            .await?;
    }

    WaitHelpers::wait_for_source_events(&ctx.pool, source, start_count + valid_total, 20).await?;
    WaitHelpers::wait_for_condition(
        || {
            let js = js.clone();
            let dlq_stream = dlq_stream.clone();
            async move {
                match dlq_message_count(&js, &dlq_stream).await? {
                    Some(count) => Ok(count >= dlq_start + invalid_total),
                    None => Ok(false),
                }
            }
        },
        20,
    )
    .await?;

    pipeline.shutdown().await?;
    Ok(())
}
