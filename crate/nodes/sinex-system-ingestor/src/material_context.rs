use async_trait::async_trait;
use sinex_db::models::{Event, OffsetKind, Provenance, SourceMaterial};
use sinex_node_sdk::acquisition_manager::{AcquisitionManager, SourceMaterialHandle};
use sinex_node_sdk::{NodeResult, SinexError};
use sinex_primitives::{Id, JsonValue};
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{
    mpsc::{self, error::TryRecvError},
    oneshot,
};
use tracing::warn;

#[async_trait]
pub trait MaterialContext: Send + Sync + fmt::Debug {
    fn initial_provenance(&self) -> Provenance;
    async fn decorate_event(&self, event: &mut Event<JsonValue>) -> NodeResult<()>;
    async fn finalize(&self, reason: &str) -> NodeResult<()>;
    fn event_count(&self) -> u64;
}

pub type WatcherMaterialContext = Arc<dyn MaterialContext>;

fn send_material_reply(
    reply: oneshot::Sender<NodeResult<Option<(i64, i64)>>>,
    result: NodeResult<Option<(i64, i64)>>,
    phase: &str,
) -> bool {
    if reply.send(result).is_err() {
        warn!(phase, "System material writer reply receiver dropped");
        false
    } else {
        true
    }
}

/// Request sent to the background writer task.
///
/// `payload = None` is the finalize sentinel: the writer drains pending requests,
/// finalizes the `SourceMaterialHandle`, and exits.
struct MaterialWriteRequest {
    /// `Some(bytes)` → append; `None` → finalize sentinel
    payload: Option<Vec<u8>>,
    /// Reason string only used when `payload` is `None`.
    reason: Option<String>,
    /// Reply channel.
    ///
    /// On append: `Ok(Some((offset_start, offset_end)))`.
    /// On finalize sentinel: `Ok(None)` after finalization completes.
    reply: oneshot::Sender<NodeResult<Option<(i64, i64)>>>,
}

const WRITER_BATCH_MAX_RECORDS: usize = 64;
const WRITER_BATCH_MAX_BYTES: usize = 128 * 1024;

async fn append_material_batch(
    acquisition: &Arc<AcquisitionManager>,
    handle: &mut SourceMaterialHandle,
    batch: Vec<(Vec<u8>, oneshot::Sender<NodeResult<Option<(i64, i64)>>>)>,
) {
    let records: Vec<Vec<u8>> = batch.iter().map(|(payload, _)| payload.clone()).collect();
    let result = acquisition.append_record_batch(handle, &records).await;

    match result {
        Ok(anchors) => {
            for ((_, reply), anchor) in batch.into_iter().zip(anchors) {
                send_material_reply(
                    reply,
                    Ok(Some((anchor.offset_start, anchor.offset_end))),
                    "append",
                );
            }
        }
        Err(error) => {
            let message = error.to_string();
            for (_, reply) in batch {
                send_material_reply(
                    reply,
                    Err(SinexError::processing(format!(
                        "Failed to append system payload batch: {message}"
                    ))),
                    "append",
                );
            }
        }
    }
}

/// Background task that owns the `SourceMaterialHandle`.
///
/// Serializes all NATS I/O through a single task so callers never hold a lock
/// across an async NATS write.  The task exits (and finalizes the handle) when
/// it receives a finalize sentinel (`payload == None`).
#[allow(clippy::needless_pass_by_value)]
async fn material_writer_task(
    acquisition: Arc<AcquisitionManager>,
    mut handle: SourceMaterialHandle,
    mut rx: mpsc::Receiver<MaterialWriteRequest>,
) {
    let mut pending_request: Option<MaterialWriteRequest> = None;

    loop {
        let req = match pending_request.take() {
            Some(req) => req,
            None => match rx.recv().await {
                Some(req) => req,
                None => break,
            },
        };

        #[allow(
            clippy::single_match_else,
            reason = "Two-arm match makes the append/finalize dichotomy visible; the finalize arm returns"
        )]
        match req.payload {
            // ── normal append ────────────────────────────────────────────
            Some(payload_bytes) => {
                let mut batch_bytes = payload_bytes.len();
                let mut batch = vec![(payload_bytes, req.reply)];

                while batch.len() < WRITER_BATCH_MAX_RECORDS {
                    match rx.try_recv() {
                        Ok(next) => match next.payload {
                            Some(next_payload) => {
                                let projected_bytes =
                                    batch_bytes.saturating_add(next_payload.len());
                                if projected_bytes > WRITER_BATCH_MAX_BYTES {
                                    pending_request = Some(MaterialWriteRequest {
                                        payload: Some(next_payload),
                                        reason: next.reason,
                                        reply: next.reply,
                                    });
                                    break;
                                }
                                batch_bytes = projected_bytes;
                                batch.push((next_payload, next.reply));
                            }
                            None => {
                                pending_request = Some(MaterialWriteRequest {
                                    payload: None,
                                    reason: next.reason,
                                    reply: next.reply,
                                });
                                break;
                            }
                        },
                        Err(TryRecvError::Empty) => break,
                        Err(TryRecvError::Disconnected) => break,
                    }
                }

                append_material_batch(&acquisition, &mut handle, batch).await;
            }

            // ── finalize sentinel ─────────────────────────────────────────
            None => {
                let reason = req.reason.as_deref().unwrap_or("material writer shutdown");
                let finalize_result = acquisition
                    .finalize(handle, reason)
                    .await
                    .map(|()| None)
                    .map_err(|e| {
                        SinexError::lifecycle(format!(
                            "Failed to finalize system watcher material: {e}"
                        ))
                    });

                // Notify caller that finalization completed (or failed).
                send_material_reply(req.reply, finalize_result, "finalize");

                // Exit the loop — the handle has been consumed.
                return;
            }
        }
    }

    // Channel closed without a finalize sentinel (e.g. all senders dropped without
    // calling `finalize`).  Perform a best-effort finalize so the material is not
    // left open.
    if let Err(e) = acquisition
        .finalize(handle, "material writer task: channel closed")
        .await
    {
        warn!(error = %e, "Failed to finalize system watcher material in writer task");
    }
}

/// Writer-task–based material context.
///
/// Ownership of `SourceMaterialHandle` and the running byte-offset counter lives
/// exclusively inside [`material_writer_task`].  `append_payload` and `finalize`
/// communicate with that task via an `mpsc` channel + `oneshot` reply, so no
/// tokio `Mutex` is held across NATS I/O.
///
/// `Clone` is cheap: all fields are `Arc`- or channel-handle–based.
#[derive(Clone)]
pub struct RealWatcherMaterialContext {
    material_id: Id<SourceMaterial>,
    writer_tx: mpsc::Sender<MaterialWriteRequest>,
    event_count: Arc<AtomicU64>,
}

impl fmt::Debug for RealWatcherMaterialContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RealWatcherMaterialContext")
            .field("material_id", &self.material_id)
            .finish()
    }
}

/// Channel capacity for the writer task inbox.
///
/// Sized to buffer a burst of concurrent callers without back-pressure stalls;
/// the writer drains quickly because NATS I/O is fast in the happy path.
const WRITER_CHANNEL_CAPACITY: usize = 256;

impl RealWatcherMaterialContext {
    pub(crate) async fn new(
        acquisition: Arc<AcquisitionManager>,
        source_identifier: &str,
        metadata: JsonValue,
    ) -> NodeResult<Self> {
        let handle = acquisition
            .build_material(source_identifier)
            .with_metadata(metadata)
            .begin()
            .await
            .map_err(|e| {
                SinexError::lifecycle(format!("Failed to begin system watcher material: {e}"))
            })?;
        let material_id = Id::from_uuid(handle.material_id);

        let (writer_tx, writer_rx) = mpsc::channel(WRITER_CHANNEL_CAPACITY);
        tokio::spawn(material_writer_task(acquisition, handle, writer_rx));

        Ok(Self {
            material_id,
            writer_tx,
            event_count: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Send an append request and await the writer task's reply.
    ///
    /// Does **not** hold any mutex across the NATS write.
    async fn append_payload(&self, payload_bytes: &[u8]) -> NodeResult<(i64, i64)> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.writer_tx
            .send(MaterialWriteRequest {
                payload: Some(payload_bytes.to_vec()),
                reason: None,
                reply: reply_tx,
            })
            .await
            .map_err(|_| {
                SinexError::processing("Material writer task has shut down".to_string())
            })?;

        reply_rx
            .await
            .map_err(|_| {
                SinexError::processing("Material writer task dropped reply channel".to_string())
            })?
            .and_then(|opt| {
                opt.ok_or_else(|| {
                    SinexError::processing(
                        "Material writer task returned finalize response for append request"
                            .to_string(),
                    )
                })
            })
    }
}

#[async_trait]
impl MaterialContext for RealWatcherMaterialContext {
    fn initial_provenance(&self) -> Provenance {
        Provenance::Material {
            id: self.material_id,
            anchor_byte: 0,
            offset_start: None,
            offset_end: None,
            offset_kind: OffsetKind::Byte,
        }
    }

    async fn decorate_event(&self, event: &mut Event<JsonValue>) -> NodeResult<()> {
        let payload_bytes = serde_json::to_vec(&event.payload).map_err(|e| {
            SinexError::processing(format!("Failed to serialize system payload: {e}"))
        })?;

        let (offset_start, offset_end) = self.append_payload(&payload_bytes).await?;
        event.provenance = Provenance::Material {
            id: self.material_id,
            anchor_byte: offset_start,
            offset_start: Some(offset_start),
            offset_end: Some(offset_end),
            offset_kind: OffsetKind::Byte,
        };

        self.event_count.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    /// Send a finalize sentinel to the writer task and await its completion.
    ///
    /// The writer will process any requests already enqueued before the sentinel,
    /// finalize the `SourceMaterialHandle`, and reply.  After this call the
    /// writer task has exited.
    async fn finalize(&self, reason: &str) -> NodeResult<()> {
        let (reply_tx, reply_rx) = oneshot::channel();
        // If the writer is already gone, treat it as a no-op (already finalized).
        let send_result = self
            .writer_tx
            .send(MaterialWriteRequest {
                payload: None,
                reason: Some(reason.to_owned()),
                reply: reply_tx,
            })
            .await;

        if send_result.is_err() {
            // Writer already exited — nothing to do.
            return Ok(());
        }

        reply_rx
            .await
            .map_err(|_| {
                SinexError::processing(
                    "Material writer task dropped finalize reply channel".to_string(),
                )
            })?
            // The writer sends `Ok(None)` on successful finalize.
            .map(|_| ())
    }

    fn event_count(&self) -> u64 {
        self.event_count.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        MaterialWriteRequest, RealWatcherMaterialContext, material_writer_task, send_material_reply,
    };
    use sinex_db::models::SourceMaterial;
    use sinex_node_sdk::acquisition_manager::{AcquisitionManager, RotationPolicy};
    use sinex_primitives::Uuid;
    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;
    use tokio::sync::{mpsc, oneshot};
    use xtask::sandbox::prelude::*;

    #[sinex_test]
    async fn send_material_reply_reports_dropped_receiver() -> TestResult<()> {
        let (tx, rx) = oneshot::channel();
        drop(rx);

        assert!(!send_material_reply(tx, Ok(None), "append"));
        Ok(())
    }

    #[sinex_test]
    async fn send_material_reply_delivers_payload() -> TestResult<()> {
        let (tx, rx) = oneshot::channel();

        assert!(send_material_reply(tx, Ok(Some((1, 4))), "append"));
        assert_eq!(rx.await??, Some((1, 4)));
        Ok(())
    }

    #[sinex_test]
    async fn append_payload_rejects_finalize_reply_for_append() -> TestResult<()> {
        let (writer_tx, mut writer_rx) = mpsc::channel(1);
        let context = RealWatcherMaterialContext {
            material_id: Id::<SourceMaterial>::new(),
            writer_tx,
            event_count: Arc::new(AtomicU64::new(0)),
        };

        tokio::spawn(async move {
            let request = writer_rx
                .recv()
                .await
                .expect("append request should reach synthetic writer");
            let _ = request.reply.send(Ok(None));
        });

        let error = context
            .append_payload(br#"{"message":"hello"}"#)
            .await
            .expect_err("append requests must not fabricate offsets from finalize replies");

        assert!(
            error
                .to_string()
                .contains("finalize response for append request")
        );
        Ok(())
    }

    #[sinex_test]
    async fn material_writer_preserves_offsets_for_queued_payloads(
        ctx: TestContext,
    ) -> TestResult<()> {
        let ctx = ctx.with_nats().shared().await?;
        let namespace = format!("system-writer-batch-{}", Uuid::now_v7());
        let work_dir = std::env::temp_dir().join(format!("sinex-system-writer-{namespace}"));
        tokio::fs::create_dir_all(&work_dir).await?;
        let acquisition = Arc::new(
            AcquisitionManager::new_with_namespace(
                ctx.nats_client(),
                RotationPolicy::default(),
                "system-test".to_string(),
                Some(namespace.clone()),
            )
            .with_work_dir(&work_dir),
        );
        let handle = acquisition.begin_material("test://system-writer").await?;
        let (writer_tx, writer_rx) = mpsc::channel(8);
        let writer = tokio::spawn(material_writer_task(
            Arc::clone(&acquisition),
            handle,
            writer_rx,
        ));

        let mut replies = Vec::new();
        for payload in [b"one".to_vec(), b"two".to_vec(), b"three".to_vec()] {
            let (reply, rx) = oneshot::channel();
            writer_tx
                .send(MaterialWriteRequest {
                    payload: Some(payload),
                    reason: None,
                    reply,
                })
                .await?;
            replies.push(rx);
        }

        let (finalize_reply, finalize_rx) = oneshot::channel();
        writer_tx
            .send(MaterialWriteRequest {
                payload: None,
                reason: Some("test-complete".to_string()),
                reply: finalize_reply,
            })
            .await?;

        let mut anchors = Vec::new();
        for reply in replies {
            anchors.push(reply.await??.ok_or_else(|| {
                SinexError::invalid_state("append request returned finalize response")
            })?);
        }
        assert_eq!(finalize_rx.await??, None);
        writer.await?;

        assert_eq!(anchors, vec![(0, 3), (3, 6), (6, 11)]);
        let _ = tokio::fs::remove_dir_all(&work_dir).await;
        Ok(())
    }
}
