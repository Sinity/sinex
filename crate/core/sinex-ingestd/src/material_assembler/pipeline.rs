//! Pipeline management for material assembler consumers.
//!
//! This module contains the `JetStream` consumer spawning logic and stream
//! bootstrapping for the ordered material assembly frame stream.

use super::redelivery_decision::{RedeliveryDecision, RedeliveryErrorKind};
use super::state::MaterialBeginMessage;
use super::{MaterialAssembler, MaterialEndMessage, Uuid};

use async_nats::jetstream;
use futures::{FutureExt, StreamExt};
use serde_json::json;
use sinex_node_sdk::{
    SOURCE_MATERIAL_BEGIN_SUBJECT, SOURCE_MATERIAL_END_SUBJECT, SOURCE_MATERIAL_FRAMES_SUBJECT,
    SOURCE_MATERIAL_SLICE_SUBJECT_PREFIX, SOURCE_MATERIAL_STREAM,
};
use std::str::FromStr;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

use crate::{IngestdResult, SinexError};

const MATERIAL_CONSUMER_SHUTDOWN_POLL: std::time::Duration = std::time::Duration::from_millis(100);
// Keep SOURCE_MATERIAL stream caps aligned with the Nix bootstrap path. The current
// nats CLI rejects --max-bytes values above signed 32-bit range.
const JETSTREAM_BOOTSTRAP_MAX_BYTES: i64 = 2_147_483_647;

async fn ack_with_warning(
    message: &jetstream::Message,
    reason: &'static str,
    material_id: Option<&Uuid>,
) -> IngestdResult<()> {
    message.ack().await.map_err(|error| {
        warn!(
            subject = %message.subject,
            material_id = ?material_id,
            error = %error,
            reason,
            "Failed to ack material assembler message"
        );
        SinexError::network("failed to ack material assembler message")
            .with_context("subject", message.subject.to_string())
            .with_context("reason", reason)
            .with_context(
                "material_id",
                material_id.map(Uuid::to_string).unwrap_or_default(),
            )
            .with_source(error.to_string())
    })
}

async fn nak_with_warning(
    message: &jetstream::Message,
    delay: Option<std::time::Duration>,
    reason: &'static str,
    material_id: Option<&Uuid>,
) -> IngestdResult<()> {
    message
        .ack_with(jetstream::AckKind::Nak(delay))
        .await
        .map_err(|error| {
            warn!(
                subject = %message.subject,
                material_id = ?material_id,
                error = %error,
                reason,
                retry_delay_ms = delay.map(|value| value.as_millis() as u64),
                "Failed to NAK material assembler message"
            );
            SinexError::network("failed to NAK material assembler message")
                .with_context("subject", message.subject.to_string())
                .with_context("reason", reason)
                .with_context(
                    "material_id",
                    material_id.map(Uuid::to_string).unwrap_or_default(),
                )
                .with_source(error.to_string())
        })
}

fn message_delivery_attempt(message: &jetstream::Message) -> IngestdResult<i64> {
    message.info().map(|info| info.delivered).map_err(|error| {
        SinexError::processing("failed to inspect material frame delivery metadata")
            .with_context("subject", message.subject.to_string())
            .with_source(error.to_string())
    })
}

async fn apply_redelivery_decision(
    assembler: &MaterialAssembler,
    message: &jetstream::Message,
    decision: RedeliveryDecision,
    material_id: Option<Uuid>,
    dlq_context: serde_json::Value,
) -> IngestdResult<()> {
    match decision {
        RedeliveryDecision::Ack { reason } => {
            ack_with_warning(message, reason, material_id.as_ref()).await
        }
        RedeliveryDecision::Nak { reason, delay } => {
            nak_with_warning(message, Some(delay), reason, material_id.as_ref()).await
        }
        RedeliveryDecision::Dlq { reason } => {
            if let Some(material_id) = material_id {
                assembler
                    .route_material_error(material_id, reason.clone(), dlq_context)
                    .await;
                assembler
                    .finalize_failed_material(material_id, &reason)
                    .await;
                ack_with_warning(message, "material_frame_routed_to_dlq", Some(&material_id)).await
            } else {
                warn!(
                    subject = %message.subject,
                    reason,
                    "Material frame was classified as DLQ but no material_id was available"
                );
                ack_with_warning(message, "material_frame_routed_to_dlq", None).await
            }
        }
    }
}

/// Bootstrap the ordered `JetStream` stream for material lifecycle frames.
pub(super) async fn bootstrap_streams(assembler: &MaterialAssembler) -> IngestdResult<()> {
    info!("Bootstrapping material streams");

    assembler
        .js
        .create_or_update_stream(jetstream::stream::Config {
            name: namespaced_stream(assembler, SOURCE_MATERIAL_STREAM),
            subjects: vec![namespaced_subject(
                assembler,
                SOURCE_MATERIAL_FRAMES_SUBJECT,
            )],
            retention: jetstream::stream::RetentionPolicy::WorkQueue,
            storage: jetstream::stream::StorageType::File,
            max_age: tokio::time::Duration::from_hours(72),
            max_bytes: JETSTREAM_BOOTSTRAP_MAX_BYTES,
            max_message_size: 512 * 1024,
            ..Default::default()
        })
        .await
        .map_err(|e| SinexError::network("Failed to create material stream").with_source(e))?;

    info!("Material streams bootstrapped successfully");
    Ok(())
}

enum MaterialFrame {
    Begin {
        material_id: Uuid,
        message: MaterialBeginMessage,
    },
    Slice {
        material_id: Uuid,
        offset: i64,
        payload: Vec<u8>,
    },
    End {
        material_id: Uuid,
        message: MaterialEndMessage,
    },
}

impl MaterialFrame {
    fn material_id(&self) -> Uuid {
        match self {
            Self::Begin { material_id, .. }
            | Self::Slice { material_id, .. }
            | Self::End { material_id, .. } => *material_id,
        }
    }

    fn kind(&self) -> &'static str {
        match self {
            Self::Begin { .. } => "begin",
            Self::Slice { .. } => "slice",
            Self::End { .. } => "end",
        }
    }

    fn offset(&self) -> Option<i64> {
        match self {
            Self::Slice { offset, .. } => Some(*offset),
            Self::Begin { .. } | Self::End { .. } => None,
        }
    }
}

#[derive(Debug)]
struct MaterialFrameDecodeError {
    reason: &'static str,
    material_id: Option<Uuid>,
    message: String,
}

impl MaterialFrameDecodeError {
    fn new(reason: &'static str, material_id: Option<Uuid>, message: String) -> Self {
        Self {
            reason,
            material_id,
            message,
        }
    }
}

/// Spawn the single ordered consumer for material lifecycle frames.
pub(super) async fn spawn_material_consumer(
    assembler: &MaterialAssembler,
    shutdown_flag: Arc<AtomicBool>,
) -> IngestdResult<JoinHandle<IngestdResult<()>>> {
    let js = assembler.js.clone();
    let assembler = assembler.clone_for_task();

    let stream_name = namespaced_stream(&assembler, SOURCE_MATERIAL_STREAM);
    let stream = js
        .get_stream(&stream_name)
        .await
        .map_err(|e| SinexError::network("Failed to get material stream").with_source(e))?;

    let consumer_name = namespaced_consumer(&assembler, "ingestd_material_frames");
    let consumer = stream
        .get_or_create_consumer(
            consumer_name.as_str(),
            jetstream::consumer::pull::Config {
                durable_name: Some(consumer_name.clone()),
                ack_policy: jetstream::consumer::AckPolicy::Explicit,
                deliver_policy: jetstream::consumer::DeliverPolicy::All,
                max_ack_pending: assembler.slices_max_ack_pending,
                ..Default::default()
            },
        )
        .await
        .map_err(|e| SinexError::network("Failed to create material consumer").with_source(e))?;

    let mut messages = consumer.messages().await.map_err(|e| {
        SinexError::network("Failed to open material frame consumer").with_source(e)
    })?;

    Ok(tokio::spawn(async move {
        loop {
            if shutdown_flag.load(Ordering::Acquire) {
                break;
            }

            let message = tokio::select! {
                maybe = messages.next() => maybe,
                () = tokio::time::sleep(MATERIAL_CONSUMER_SHUTDOWN_POLL) => {
                    continue;
                }
            };

            let Some(message) = message else {
                break;
            };
            let message = match message {
                Ok(msg) => msg,
                Err(e) => {
                    warn!("Error receiving material frame message: {}", e);
                    continue;
                }
            };

            let frame = match decode_material_frame(
                message.subject.as_str(),
                message.headers.as_ref(),
                &message.payload,
            ) {
                Ok(frame) => frame,
                Err(error) => {
                    let material_id = error.material_id;
                    warn!(
                        subject = %message.subject,
                        material_id = ?material_id,
                        error = %error.message,
                        payload_len = message.payload.len(),
                        "Rejecting malformed material frame"
                    );
                    let decision = RedeliveryDecision::for_error(
                        RedeliveryErrorKind::MalformedFrame {
                            reason: error.reason.to_string(),
                        },
                        message_delivery_attempt(&message)?,
                    );
                    apply_redelivery_decision(
                        &assembler,
                        &message,
                        decision,
                        material_id,
                        json!({
                            "error": error.message,
                            "subject": message.subject.as_str(),
                        }),
                    )
                    .await?;
                    continue;
                }
            };
            let material_id = frame.material_id();
            let frame_kind = frame.kind();
            let frame_offset = frame.offset();

            let result = std::panic::AssertUnwindSafe(async {
                match frame {
                    MaterialFrame::Begin {
                        material_id,
                        message,
                    } => assembler.handle_begin(material_id, message).await,
                    MaterialFrame::Slice {
                        material_id,
                        offset,
                        payload,
                    } => assembler.handle_slice(material_id, offset, payload).await,
                    MaterialFrame::End { message, .. } => assembler.handle_end(message).await,
                }
            })
            .catch_unwind()
            .await;

            match result {
                Ok(Ok(())) => {}
                Ok(Err(err)) => {
                    error!(
                        material_id = %material_id,
                        frame_kind,
                        "Failed to process material frame: {}",
                        err
                    );
                    let decision = RedeliveryDecision::for_processing_error(
                        &err,
                        message_delivery_attempt(&message)?,
                    );
                    apply_redelivery_decision(
                        &assembler,
                        &message,
                        decision,
                        Some(material_id),
                        json!({
                            "error": err.to_string(),
                            "frame_kind": frame_kind,
                            "offset": frame_offset,
                        }),
                    )
                    .await?;
                    continue;
                }
                Err(panic) => {
                    let panic_msg = describe_panic(&*panic);
                    error!(
                        material_id = %material_id,
                        frame_kind,
                        "Material frame consumer panicked: {}",
                        panic_msg
                    );
                    let decision = RedeliveryDecision::for_error(
                        RedeliveryErrorKind::ConsumerPanic {
                            panic: panic_msg.clone(),
                        },
                        message_delivery_attempt(&message)?,
                    );
                    apply_redelivery_decision(
                        &assembler,
                        &message,
                        decision,
                        Some(material_id),
                        json!({ "panic": panic_msg, "frame_kind": frame_kind }),
                    )
                    .await?;
                    continue;
                }
            }

            apply_redelivery_decision(
                &assembler,
                &message,
                RedeliveryDecision::processed(),
                Some(material_id),
                json!({}),
            )
            .await?;
        }

        Ok::<(), SinexError>(())
    }))
}

fn namespaced_subject(assembler: &MaterialAssembler, base: &str) -> String {
    assembler
        .env
        .nats_subject_with_namespace(assembler.namespace.as_deref(), base)
}

fn namespaced_stream(assembler: &MaterialAssembler, base: &str) -> String {
    assembler
        .env
        .nats_stream_name_with_namespace(assembler.namespace.as_deref(), base)
}

fn namespaced_consumer(assembler: &MaterialAssembler, base: &str) -> String {
    match assembler.namespace.as_deref() {
        Some(ns) => format!("{}_{}", sanitize_namespace(ns), base),
        None => base.to_string(),
    }
}

fn sanitize_namespace(namespace: &str) -> String {
    namespace
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_uppercase()
            } else {
                '_'
            }
        })
        .collect()
}

fn describe_panic(panic: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = panic.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = panic.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic".to_string()
    }
}

fn subject_has_suffix(subject: &str, suffix: &str) -> bool {
    subject == suffix
        || subject
            .strip_suffix(suffix)
            .is_some_and(|prefix| prefix.ends_with('.'))
}

fn subject_is_slice(subject: &str) -> bool {
    subject.starts_with(SOURCE_MATERIAL_SLICE_SUBJECT_PREFIX)
        || subject.contains(&format!(".{SOURCE_MATERIAL_SLICE_SUBJECT_PREFIX}"))
}

fn decode_material_frame(
    subject: &str,
    headers: Option<&async_nats::HeaderMap>,
    payload: &[u8],
) -> Result<MaterialFrame, MaterialFrameDecodeError> {
    if subject_has_suffix(subject, SOURCE_MATERIAL_BEGIN_SUBJECT) {
        let (message, material_id) = decode_begin_message(payload).map_err(|message| {
            MaterialFrameDecodeError::new("begin_payload_invalid", None, message)
        })?;
        return Ok(MaterialFrame::Begin {
            material_id,
            message,
        });
    }

    if subject_has_suffix(subject, SOURCE_MATERIAL_END_SUBJECT) {
        let message = serde_json::from_slice::<MaterialEndMessage>(payload).map_err(|error| {
            MaterialFrameDecodeError::new(
                "end_payload_invalid",
                None,
                format!("invalid end payload: {error}"),
            )
        })?;
        let material_id = parse_material_id(&message.material_id, "end message material_id")
            .map_err(|message| {
                MaterialFrameDecodeError::new("end_material_id_invalid", None, message)
            })?;
        return Ok(MaterialFrame::End {
            material_id,
            message,
        });
    }

    if subject_is_slice(subject) {
        let material_id = parse_slice_material_id(subject).map_err(|message| {
            MaterialFrameDecodeError::new("slice_subject_invalid", None, message)
        })?;
        let offset = parse_slice_offset(subject, headers).map_err(|message| {
            MaterialFrameDecodeError::new("slice_offset_invalid", Some(material_id), message)
        })?;
        return Ok(MaterialFrame::Slice {
            material_id,
            offset,
            payload: payload.to_vec(),
        });
    }

    Err(MaterialFrameDecodeError::new(
        "material_frame_subject_invalid",
        None,
        format!("unexpected material frame subject '{subject}'"),
    ))
}

fn parse_material_id(raw: &str, context: &str) -> Result<Uuid, String> {
    Uuid::from_str(raw).map_err(|error| format!("invalid {context} '{raw}': {error}"))
}

fn decode_begin_message(payload: &[u8]) -> Result<(MaterialBeginMessage, Uuid), String> {
    let begin = serde_json::from_slice::<MaterialBeginMessage>(payload)
        .map_err(|error| format!("invalid begin payload: {error}"))?;
    let material_id = parse_material_id(&begin.material_id, "begin material_id")?;
    Ok((begin, material_id))
}

fn parse_slice_material_id(subject: &str) -> Result<Uuid, String> {
    let raw = subject
        .split('.')
        .next_back()
        .ok_or_else(|| format!("slice subject '{subject}' is missing material id"))?;
    parse_material_id(raw, "slice subject material_id")
}

fn parse_slice_offset(
    subject: &str,
    headers: Option<&async_nats::HeaderMap>,
) -> Result<i64, String> {
    let Some(raw_offset) = headers.and_then(|headers| headers.get("Offset")) else {
        return Err("missing Offset header".to_string());
    };
    let offset = raw_offset
        .as_str()
        .parse::<i64>()
        .map_err(|error| format!("invalid Offset header '{}': {error}", raw_offset.as_str()))?;
    if offset < 0 {
        return Err(format!(
            "negative Offset header '{}' is invalid",
            raw_offset.as_str()
        ));
    }
    if !subject_is_slice(subject) {
        return Err(format!("unexpected slice subject '{subject}'"));
    }
    Ok(offset)
}

#[cfg(test)]
mod tests {
    use super::{
        decode_begin_message, parse_material_id, parse_slice_material_id, parse_slice_offset,
    };
    use async_nats::HeaderMap;
    use serde_json::json;
    use uuid::Uuid;
    use xtask::sandbox::sinex_test;

    const SUBJECT: &str =
        "dev.source_material.frames.slices.test.00000000-0000-7000-8000-000000000001";

    // Inline because these exercise private malformed-slice parsing helpers.
    #[sinex_test]
    async fn parse_slice_offset_accepts_valid_header() -> TestResult<()> {
        let mut headers = HeaderMap::new();
        headers.insert("Offset", "42");
        let offset = parse_slice_offset(SUBJECT, Some(&headers))
            .map_err(|error| color_eyre::eyre::eyre!(error))?;
        assert_eq!(offset, 42);
        Ok(())
    }

    #[sinex_test]
    async fn parse_slice_offset_rejects_missing_header() -> TestResult<()> {
        let error =
            parse_slice_offset(SUBJECT, None).expect_err("missing offset header should fail");
        assert!(error.contains("missing Offset header"));
        Ok(())
    }

    #[sinex_test]
    async fn parse_slice_offset_rejects_non_numeric_header() -> TestResult<()> {
        let mut headers = HeaderMap::new();
        headers.insert("Offset", "nope");
        let error = parse_slice_offset(SUBJECT, Some(&headers))
            .expect_err("non-numeric offset should fail");
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
        .map_err(|error| color_eyre::eyre::eyre!(error))?;
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
}
