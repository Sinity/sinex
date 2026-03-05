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
    atomic::{AtomicBool, Ordering},
    Arc,
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

                let material_id = serde_json::from_slice::<MaterialBeginMessage>(&message.payload)
                    .ok()
                    .and_then(|msg| Uuid::from_str(&msg.material_id).ok());

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
                            material_id = ?material_id,
                            "Begin consumer panicked: {}",
                            panic_msg
                        );
                        if let Some(material_id) = material_id {
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

                let offset = message
                    .headers
                    .as_ref()
                    .and_then(|h| h.get("Offset"))
                    .and_then(|v| v.as_str().parse::<i64>().ok())
                    .unwrap_or(0);

                let material_id = message
                    .subject
                    .split('.')
                    .next_back()
                    .and_then(|part| Uuid::from_str(part).ok());

                let Some(material_id) = material_id else {
                    warn!(
                        "Slice message missing material id in subject {}",
                        message.subject
                    );
                    let _ = message.ack().await;
                    continue;
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

                let material_id = Uuid::from_str(&end_message.material_id).ok();

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
                            material_id = ?material_id,
                            "End consumer panicked: {}",
                            panic_msg
                        );
                        if let Some(material_id) = material_id {
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
