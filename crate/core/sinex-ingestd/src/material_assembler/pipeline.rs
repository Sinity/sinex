//! Pipeline management for material assembler consumers.
//!
//! This module contains the `JetStream` consumer spawning logic and stream
//! bootstrapping for the three material assembly streams: begin, slices, and end.

use super::state::MaterialBeginMessage;
use super::{MaterialAssembler, MaterialEndMessage, Uuid};

use async_nats::jetstream;
use futures::{FutureExt, StreamExt};
use serde_json::json;
use std::str::FromStr;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

use crate::{IngestdResult, SinexError};

const BATCH_PROCESSING_SEMAPHORE_PERMITS: usize = 4; // Allow up to 4 concurrent batches

/// Handles for the three material consumer tasks
pub(super) struct MaterialConsumerHandles {
    pub(crate) begin: JoinHandle<IngestdResult<()>>,
    pub(crate) slices: JoinHandle<IngestdResult<()>>,
    pub(crate) end: JoinHandle<IngestdResult<()>>,
}

impl Drop for MaterialConsumerHandles {
    fn drop(&mut self) {
        self.begin.abort();
        self.slices.abort();
        self.end.abort();
    }
}

/// Bootstrap `JetStream` streams for materials
pub(super) async fn bootstrap_streams(assembler: &MaterialAssembler) -> IngestdResult<()> {
    info!("Bootstrapping material streams");

    assembler
        .js
        .get_or_create_stream(jetstream::stream::Config {
            name: namespaced_stream(assembler, "SOURCE_MATERIAL_BEGIN"),
            subjects: vec![namespaced_subject(assembler, "source_material.begin")],
            storage: jetstream::stream::StorageType::File,
            ..Default::default()
        })
        .await
        .map_err(|e| SinexError::network("Failed to create begin stream").with_source(e))?;

    assembler
        .js
        .get_or_create_stream(jetstream::stream::Config {
            name: namespaced_stream(assembler, "SOURCE_MATERIAL_SLICES"),
            subjects: vec![namespaced_subject(assembler, "source_material.slices.>")],
            storage: jetstream::stream::StorageType::File,
            max_age: tokio::time::Duration::from_hours(168),
            max_message_size: 512 * 1024,
            ..Default::default()
        })
        .await
        .map_err(|e| SinexError::network("Failed to create slices stream").with_source(e))?;

    assembler
        .js
        .get_or_create_stream(jetstream::stream::Config {
            name: namespaced_stream(assembler, "SOURCE_MATERIAL_END"),
            subjects: vec![namespaced_subject(assembler, "source_material.end")],
            storage: jetstream::stream::StorageType::File,
            ..Default::default()
        })
        .await
        .map_err(|e| SinexError::network("Failed to create end stream").with_source(e))?;

    info!("Material streams bootstrapped successfully");
    Ok(())
}

/// Spawn consumer for begin messages
pub(super) fn spawn_begin_consumer(
    assembler: &MaterialAssembler,
    shutdown_flag: Arc<AtomicBool>,
) -> JoinHandle<IngestdResult<()>> {
    let js = assembler.js.clone();
    let assembler = assembler.clone_for_task();

    tokio::spawn(async move {
        let stream_name = namespaced_stream(&assembler, "SOURCE_MATERIAL_BEGIN");
        let stream = js
            .get_stream(&stream_name)
            .await
            .map_err(|e| SinexError::network("Failed to get begin stream").with_source(e))?;

        let consumer_name = namespaced_consumer(&assembler, "ingestd_material_begin");
        let consumer = stream
            .get_or_create_consumer(
                consumer_name.as_str(),
                jetstream::consumer::pull::Config {
                    durable_name: Some(consumer_name.clone()),
                    ack_policy: jetstream::consumer::AckPolicy::Explicit,
                    // Critical for correctness: tests (and real systems) may publish before this
                    // consumer is created on first startup; don't silently skip earlier messages.
                    deliver_policy: jetstream::consumer::DeliverPolicy::All,
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| SinexError::network("Failed to create begin consumer").with_source(e))?;

        loop {
            if shutdown_flag.load(Ordering::Relaxed) {
                break;
            }
            let mut messages = consumer
                .batch()
                .max_messages(50)
                .messages()
                .await
                .map_err(|e| {
                    SinexError::network("Failed to fetch begin messages").with_source(e)
                })?;

            while let Some(message) = messages.next().await {
                if shutdown_flag.load(Ordering::Relaxed) {
                    break;
                }
                let message = match message {
                    Ok(msg) => msg,
                    Err(e) => {
                        warn!("Error receiving begin message: {}", e);
                        continue;
                    }
                };

                let material_id = parse_begin_material_id(&message.payload);

                let result = std::panic::AssertUnwindSafe(async {
                    assembler.handle_begin(message.clone()).await
                })
                .catch_unwind()
                .await;

                match result {
                    Ok(Ok(())) => {}
                    Ok(Err(err)) => {
                        error!("Failed to process begin message: {}", err);
                        let _ = message.ack_with(jetstream::AckKind::Nak(None)).await;
                        continue;
                    }
                    Err(panic) => {
                        let panic_msg = describe_panic(&*panic);
                        error!(
                            material_id = ?material_id.as_ref().ok(),
                            material_id_error = ?material_id.as_ref().err(),
                            "Begin consumer panicked: {}",
                            panic_msg
                        );
                        if let Ok(material_id) = material_id {
                            assembler
                                .route_material_error(
                                    material_id,
                                    "begin_consumer_panic",
                                    json!({ "panic": panic_msg }),
                                )
                                .await;
                            assembler
                                .finalize_failed_material(material_id, "begin_consumer_panic")
                                .await;
                        }
                        let _ = message
                            .ack_with(jetstream::AckKind::Nak(Some(
                                std::time::Duration::from_millis(200),
                            )))
                            .await;
                        continue;
                    }
                }

                if let Err(e) = message.ack().await {
                    warn!("Failed to ack begin message: {}", e);
                }
            }
        }

        Ok::<(), SinexError>(())
    })
}

/// Spawn consumer for slice messages
pub(super) fn spawn_slices_consumer(
    assembler: &MaterialAssembler,
    shutdown_flag: Arc<AtomicBool>,
) -> JoinHandle<IngestdResult<()>> {
    let js = assembler.js.clone();
    let assembler = assembler.clone_for_task();

    // Semaphore to limit concurrent batch processing and prevent memory exhaustion
    let batch_semaphore = Arc::new(tokio::sync::Semaphore::new(
        BATCH_PROCESSING_SEMAPHORE_PERMITS,
    ));

    tokio::spawn(async move {
        let stream_name = namespaced_stream(&assembler, "SOURCE_MATERIAL_SLICES");
        let stream = js
            .get_stream(&stream_name)
            .await
            .map_err(|e| SinexError::network("Failed to get slices stream").with_source(e))?;

        let consumer_name = namespaced_consumer(&assembler, "ingestd_material_slices");
        let consumer = stream
            .get_or_create_consumer(
                consumer_name.as_str(),
                jetstream::consumer::pull::Config {
                    durable_name: Some(consumer_name.clone()),
                    ack_policy: jetstream::consumer::AckPolicy::Explicit,
                    // Same reasoning as begin/end: don't skip slices published before consumer creation.
                    deliver_policy: jetstream::consumer::DeliverPolicy::All,
                    max_ack_pending: assembler.slices_max_ack_pending,
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| SinexError::network("Failed to create slices consumer").with_source(e))?;

        loop {
            if shutdown_flag.load(Ordering::Relaxed) {
                break;
            }

            // Acquire semaphore permit before fetching next batch (backpressure)
            let _permit = batch_semaphore.acquire().await.map_err(|e| {
                SinexError::service(format!("Failed to acquire batch semaphore: {e}"))
            })?;

            let mut messages = consumer
                .batch()
                .max_messages(200)
                .messages()
                .await
                .map_err(|e| {
                    SinexError::network("Failed to fetch slice messages").with_source(e)
                })?;

            while let Some(message) = messages.next().await {
                if shutdown_flag.load(Ordering::Relaxed) {
                    break;
                }
                let message = match message {
                    Ok(msg) => msg,
                    Err(e) => {
                        warn!("Error receiving slice message: {}", e);
                        continue;
                    }
                };

                let material_id = parse_slice_material_id(message.subject.as_str());

                let Ok(material_id) = material_id else {
                    warn!(
                        subject = %message.subject,
                        error = %material_id.unwrap_err(),
                        "Rejecting malformed slice message subject"
                    );
                    let _ = message.ack().await;
                    continue;
                };

                let offset = match parse_slice_offset(message.subject.as_str(), message.headers.as_ref()) {
                    Ok(offset) => offset,
                    Err(error) => {
                        warn!(
                            material_id = %material_id,
                            subject = %message.subject,
                            error = %error,
                            "Rejecting malformed slice message"
                        );
                        assembler
                            .route_material_error(
                                material_id,
                                "slice_offset_invalid",
                                json!({
                                    "error": error,
                                    "subject": message.subject.as_str(),
                                }),
                            )
                            .await;
                        assembler
                            .finalize_failed_material(material_id, "slice_offset_invalid")
                            .await;
                        if let Err(ack_err) = message.ack().await {
                            warn!(
                                material_id = %material_id,
                                error = %ack_err,
                                "Failed to ack malformed slice message"
                            );
                        }
                        continue;
                    }
                };

                let result = std::panic::AssertUnwindSafe(async {
                    assembler
                        .handle_slice(material_id, offset, message.payload.to_vec())
                        .await
                })
                .catch_unwind()
                .await;

                match result {
                    Ok(Ok(())) => {}
                    Ok(Err(err)) => {
                        error!(
                            material_id = %material_id,
                            "Failed to process slice message: {}",
                            err
                        );
                        assembler
                            .route_material_error(
                                material_id,
                                "slice_processing_failed",
                                json!({ "error": err.to_string(), "offset": offset }),
                            )
                            .await;
                        let _ = message.ack_with(jetstream::AckKind::Nak(None)).await;
                        continue;
                    }
                    Err(panic) => {
                        let panic_msg = describe_panic(&*panic);
                        error!(
                            material_id = %material_id,
                            "Slice consumer panicked: {}",
                            panic_msg
                        );
                        assembler
                            .route_material_error(
                                material_id,
                                "slice_consumer_panic",
                                json!({ "panic": panic_msg, "offset": offset }),
                            )
                            .await;
                        assembler
                            .finalize_failed_material(material_id, "slice_consumer_panic")
                            .await;
                        let _ = message
                            .ack_with(jetstream::AckKind::Nak(Some(
                                std::time::Duration::from_millis(200),
                            )))
                            .await;
                        continue;
                    }
                }

                if let Err(e) = message.ack().await {
                    warn!("Failed to ack slice message: {}", e);
                }
            }
            // Permit automatically dropped here, releasing semaphore
        }

        Ok::<(), SinexError>(())
    })
}

/// Spawn consumer for end messages
pub(super) fn spawn_end_consumer(
    assembler: &MaterialAssembler,
    shutdown_flag: Arc<AtomicBool>,
) -> JoinHandle<IngestdResult<()>> {
    let js = assembler.js.clone();
    let assembler = assembler.clone_for_task();

    tokio::spawn(async move {
        let stream_name = namespaced_stream(&assembler, "SOURCE_MATERIAL_END");
        let stream = js
            .get_stream(&stream_name)
            .await
            .map_err(|e| SinexError::network("Failed to get end stream").with_source(e))?;

        let consumer_name = namespaced_consumer(&assembler, "ingestd_material_end");
        let consumer = stream
            .get_or_create_consumer(
                consumer_name.as_str(),
                jetstream::consumer::pull::Config {
                    durable_name: Some(consumer_name.clone()),
                    ack_policy: jetstream::consumer::AckPolicy::Explicit,
                    // Ensure end messages published before consumer creation are still processed.
                    deliver_policy: jetstream::consumer::DeliverPolicy::All,
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| SinexError::network("Failed to create end consumer").with_source(e))?;

        loop {
            if shutdown_flag.load(Ordering::Relaxed) {
                break;
            }
            let mut messages = consumer
                .batch()
                .max_messages(50)
                .messages()
                .await
                .map_err(|e| SinexError::network("Failed to fetch end messages").with_source(e))?;

            while let Some(message) = messages.next().await {
                if shutdown_flag.load(Ordering::Relaxed) {
                    break;
                }
                let message = match message {
                    Ok(msg) => msg,
                    Err(e) => {
                        warn!("Error receiving end message: {}", e);
                        continue;
                    }
                };

                let end_message: MaterialEndMessage = match serde_json::from_slice(&message.payload)
                {
                    Ok(msg) => msg,
                    Err(e) => {
                        warn!("Failed to decode end message payload: {}", e);
                        if let Err(ack_err) = message.ack().await {
                            warn!("Failed to ack malformed end message: {}", ack_err);
                        }
                        continue;
                    }
                };

                let material_id = parse_material_id(&end_message.material_id, "end message material_id");

                let result =
                    std::panic::AssertUnwindSafe(async { assembler.handle_end(end_message).await })
                        .catch_unwind()
                        .await;

                match result {
                    Ok(Ok(())) => {}
                    Ok(Err(err)) => {
                        error!("Failed to process end message: {}", err);
                        let _ = message
                            .ack_with(jetstream::AckKind::Nak(Some(
                                std::time::Duration::from_millis(200),
                            )))
                            .await;
                        continue;
                    }
                    Err(panic) => {
                        let panic_msg = describe_panic(&*panic);
                        error!(
                            material_id = ?material_id.as_ref().ok(),
                            material_id_error = ?material_id.as_ref().err(),
                            "End consumer panicked: {}",
                            panic_msg
                        );
                        if let Ok(material_id) = material_id {
                            assembler
                                .route_material_error(
                                    material_id,
                                    "end_consumer_panic",
                                    json!({ "panic": panic_msg }),
                                )
                                .await;
                            assembler
                                .finalize_failed_material(material_id, "end_consumer_panic")
                                .await;
                        }
                        let _ = message
                            .ack_with(jetstream::AckKind::Nak(Some(
                                std::time::Duration::from_millis(200),
                            )))
                            .await;
                        continue;
                    }
                }

                if let Err(e) = message.ack().await {
                    warn!("Failed to ack end message: {}", e);
                }
            }
        }

        Ok::<(), SinexError>(())
    })
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

fn parse_material_id(raw: &str, context: &str) -> Result<Uuid, String> {
    Uuid::from_str(raw).map_err(|error| format!("invalid {context} '{raw}': {error}"))
}

fn parse_begin_material_id(payload: &[u8]) -> Result<Uuid, String> {
    let begin = serde_json::from_slice::<MaterialBeginMessage>(payload)
        .map_err(|error| format!("invalid begin payload: {error}"))?;
    parse_material_id(&begin.material_id, "begin material_id")
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
        return Err(format!("negative Offset header '{}' is invalid", raw_offset.as_str()));
    }
    if !subject.contains(".source_material.slices.") {
        return Err(format!("unexpected slice subject '{subject}'"));
    }
    Ok(offset)
}

#[cfg(test)]
mod tests {
    use super::{
        parse_begin_material_id, parse_material_id, parse_slice_material_id, parse_slice_offset,
    };
    use async_nats::HeaderMap;
    use serde_json::json;
    use uuid::Uuid;
    use xtask::sandbox::sinex_test;

    const SUBJECT: &str = "dev.source_material.slices.test.00000000-0000-7000-8000-000000000001";

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
        let error = parse_slice_offset(SUBJECT, None)
            .expect_err("missing offset header should fail");
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
        let error = parse_slice_offset(SUBJECT, Some(&headers))
            .expect_err("negative offset should fail");
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
    async fn parse_begin_material_id_rejects_invalid_payload() -> TestResult<()> {
        let error = parse_begin_material_id(
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
    async fn parse_begin_material_id_accepts_valid_payload() -> TestResult<()> {
        let material_id = "00000000-0000-7000-8000-000000000001";
        let parsed = parse_begin_material_id(
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
        assert_eq!(parsed, material_id.parse::<Uuid>()?);
        Ok(())
    }

    #[sinex_test]
    async fn parse_slice_material_id_rejects_invalid_subject() -> TestResult<()> {
        let error = parse_slice_material_id("dev.source_material.slices.test.not-a-uuid")
            .expect_err("invalid slice subject material id should fail");
        assert!(error.contains("slice subject material_id"));
        Ok(())
    }
}
