use super::{
    RedeliveryDecision, RedeliveryErrorKind, apply_redelivery_decision, bootstrap_streams,
    decode_begin_message, parse_material_id, parse_slice_material_id, parse_slice_offset,
};
use crate::event_engine::durable_failure::DURABLE_FAILURE_ID_HEADER;
use crate::event_engine::material_assembler::test_support::build_test_assembler;
use async_nats::HeaderMap;
use futures::StreamExt;
use serde_json::json;
use sinex_primitives::{SinexError, Uuid};
use tokio::time::{Duration, timeout};
use xtask::sandbox::prelude::*;

const SUBJECT: &str = "dev.source_material.frames.slices.test.00000000-0000-7000-8000-000000000001";

// Inline because these exercise private malformed-slice parsing helpers.
#[sinex_test]
async fn parse_slice_offset_accepts_valid_header() -> TestResult<()> {
    let mut headers = HeaderMap::new();
    headers.insert("Offset", "42");
    let offset = parse_slice_offset(SUBJECT, Some(&headers)).map_err(SinexError::validation)?;
    assert_eq!(offset, 42);
    Ok(())
}

#[sinex_test]
async fn parse_slice_offset_rejects_missing_header() -> TestResult<()> {
    let error = parse_slice_offset(SUBJECT, None).expect_err("missing offset header should fail");
    assert!(error.contains("missing Offset header"));
    Ok(())
}

#[sinex_test]
async fn parse_slice_offset_rejects_non_numeric_header() -> TestResult<()> {
    let mut headers = HeaderMap::new();
    headers.insert("Offset", "nope");
    let error =
        parse_slice_offset(SUBJECT, Some(&headers)).expect_err("non-numeric offset should fail");
    assert!(error.contains("invalid Offset header"));
    Ok(())
}

#[sinex_test]
async fn parse_slice_offset_rejects_negative_header() -> TestResult<()> {
    let mut headers = HeaderMap::new();
    headers.insert("Offset", "-1");
    let error =
        parse_slice_offset(SUBJECT, Some(&headers)).expect_err("negative offset should fail");
    assert!(error.contains("negative Offset header"));
    Ok(())
}

#[sinex_test]
async fn parse_material_id_reports_context() -> TestResult<()> {
    let error = parse_material_id("not-a-uuid", "test material_id")
        .expect_err("invalid material id should fail");
    assert!(error.contains("test material_id"));
    assert!(error.contains("not-a-uuid"));
    Ok(())
}

#[sinex_test]
async fn decode_begin_message_rejects_invalid_payload() -> TestResult<()> {
    let error = decode_begin_message(br#"{"material_id":"oops""#)
        .expect_err("invalid begin payload should fail");
    assert!(error.contains("invalid begin payload"));
    Ok(())
}

#[sinex_test]
async fn decode_begin_message_rejects_invalid_material_id() -> TestResult<()> {
    let error = decode_begin_message(
        serde_json::to_vec(&json!({
            "material_id": "not-a-uuid",
            "material_kind": "shell-history",
            "source_identifier": "history.db",
            "metadata": {},
            "started_at": "2026-03-28T08:00:00Z"
        }))?
        .as_slice(),
    )
    .expect_err("invalid begin material id should fail");
    assert!(error.contains("begin material_id"));
    Ok(())
}

#[sinex_test]
async fn decode_begin_message_accepts_valid_payload() -> TestResult<()> {
    let material_id = "00000000-0000-7000-8000-000000000001";
    let (begin, parsed_material_id) = decode_begin_message(
        serde_json::to_vec(&json!({
            "material_id": material_id,
            "material_kind": "shell-history",
            "source_identifier": "history.db",
            "metadata": {},
            "started_at": "2026-03-28T08:00:00Z"
        }))?
        .as_slice(),
    )
    .map_err(SinexError::validation)?;
    assert_eq!(begin.material_kind, "shell-history");
    assert_eq!(parsed_material_id, material_id.parse::<Uuid>()?);
    Ok(())
}

#[sinex_test]
async fn parse_slice_material_id_rejects_invalid_subject() -> TestResult<()> {
    let error = parse_slice_material_id("dev.source_material.frames.slices.test.not-a-uuid")
        .expect_err("invalid slice subject material id should fail");
    assert!(error.contains("slice subject material_id"));
    Ok(())
}

/// Exercise the production settlement route for a malformed frame whose
/// payload contains no material ID. The frame must be confirmed into the
/// material DLQ before its source-stream message is ACKed; an unconditional
/// ACK here would make this test observe neither a DLQ record nor a durable
/// failure witness.
#[sinex_test]
async fn malformed_material_frame_without_id_is_durable_dlq_settled(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let (assembler, _content_store_dir, _state_dir) =
        build_test_assembler(&ctx, "pipeline-malformed-frame").await?;
    bootstrap_streams(&assembler).await?;

    let js = async_nats::jetstream::new(ctx.nats_client());
    let dlq_stream_name = ctx.env().nats_stream_name_with_namespace(
        Some(ctx.pipeline_namespace().prefix()),
        "SINEX_RAW_EVENTS_DLQ",
    );
    js.create_or_update_stream(async_nats::jetstream::stream::Config {
        name: dlq_stream_name.clone(),
        subjects: vec![assembler.dlq_subject.clone()],
        retention: async_nats::jetstream::stream::RetentionPolicy::Limits,
        storage: async_nats::jetstream::stream::StorageType::Memory,
        max_age: Duration::from_secs(300),
        allow_direct: true,
        ..Default::default()
    })
    .await?;

    let source_stream_name = ctx.env().nats_stream_name_with_namespace(
        Some(ctx.pipeline_namespace().prefix()),
        "SOURCE_MATERIAL",
    );
    let mut source_stream = js.get_stream(&source_stream_name).await?;
    let consumer_name = format!("malformed-frame-{}", Uuid::now_v7());
    let consumer = source_stream
        .create_consumer(async_nats::jetstream::consumer::pull::Config {
            name: Some(consumer_name.clone()),
            durable_name: Some(consumer_name),
            filter_subject: ctx.pipeline_namespace().subject("source_material.frames.>"),
            ack_policy: async_nats::jetstream::consumer::AckPolicy::Explicit,
            ..Default::default()
        })
        .await?;
    let mut messages = consumer.messages().await?;

    js.publish(
        ctx.pipeline_namespace()
            .subject("source_material.frames.begin"),
        b"not-json".to_vec().into(),
    )
    .await?
    .await?;
    let message = timeout(Duration::from_secs(2), messages.next())
        .await?
        .ok_or_else(|| SinexError::processing("malformed source frame was not delivered"))??;

    apply_redelivery_decision(
        &assembler,
        &message,
        RedeliveryDecision::for_error(
            RedeliveryErrorKind::MalformedFrame {
                reason: "begin_payload_invalid".to_string(),
            },
            1,
        ),
        None,
        json!({"fixture": "malformed-begin"}),
    )
    .await?;

    let mut dlq_stream = js.get_stream(&dlq_stream_name).await?;
    let state = dlq_stream.info().await?.state.clone();
    assert_eq!(
        state.messages, 1,
        "malformed frame must be retained in the DLQ"
    );
    let entry = dlq_stream.direct_get(state.first_sequence).await?;
    let witness_id = entry
        .headers
        .get(DURABLE_FAILURE_ID_HEADER)
        .ok_or_else(|| SinexError::processing("material DLQ witness header is missing"))?
        .to_string()
        .parse::<Uuid>()?;
    let evidence = sqlx::query!(
        "SELECT failed_event_id FROM sinex_schemas.dlq_events WHERE dlq_id = $1",
        witness_id,
    )
    .fetch_one(ctx.pool())
    .await?;
    assert_ne!(evidence.failed_event_id, Uuid::nil());

    let source_state = source_stream.info().await?.state.clone();
    assert_eq!(
        source_state.messages, 0,
        "confirmed DLQ route may ACK the source frame"
    );
    Ok(())
}
