//! Buffered batching for metadata-only events (observation materialization).
//!
//! This module abstracts the buffering pattern seen in `sinex-system-source`:
//! accumulate small JSON-serializable records, flush when thresholds are reached,
//! and serialize to JSON Lines format suitable for material staging.
//!
//! # Example
//!
//! ```ignore
//! #[derive(Serialize)]
//! struct Record { id: i32, data: String }
//!
//! let mut mat = ObservationMaterializer::<Record>::new(
//!     ObservationMaterializerConfig {
//!         batch_coalesce_window_ms: 20,
//!         max_records: 100,
//!         max_bytes: 128 * 1024,
//!     },
//! );
//!
//! mat.append(&record1).await?;
//! mat.append(&record2).await?;
//! // On max_records or window_ms elapsed, automatically flushes
//! ```

use crate::runtime::RuntimeResult;
use serde::Serialize;
use sinex_primitives::SinexError;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{Duration, Instant, interval};
use tracing::{debug, error, warn};

/// Configuration for [`ObservationMaterializer`] behavior.
#[derive(Debug, Clone)]
pub struct ObservationMaterializerConfig {
    /// Time window (ms) before flushing even if thresholds not reached.
    pub batch_coalesce_window_ms: u64,
    /// Maximum number of records per batch before forced flush.
    pub max_records: usize,
    /// Maximum bytes per batch before forced flush.
    pub max_bytes: usize,
}

impl Default for ObservationMaterializerConfig {
    fn default() -> Self {
        Self {
            batch_coalesce_window_ms: 20,
            max_records: 100,
            max_bytes: 128 * 1024,
        }
    }
}

/// Serialized batch ready for flush.
#[derive(Debug)]
pub struct SerializedBatch {
    /// JSON Lines content (one record per line, newline-terminated)
    pub data: Vec<u8>,
    /// Number of records in this batch
    pub record_count: usize,
}

/// Future returned by a [`FlushCallback`]. Pinned, boxed, `Send + 'static` so
/// async-block return values from `Box::pin(async move {...})` coerce cleanly.
pub type FlushFuture =
    std::pin::Pin<Box<dyn std::future::Future<Output = RuntimeResult<()>> + Send + 'static>>;

/// Callback invoked when a batch is ready to flush.
///
/// Receives the serialized JSON Lines data and record count.
/// Implementers are responsible for staging the data as material.
pub type FlushCallback = dyn Fn(SerializedBatch) -> FlushFuture + Send + Sync + 'static;

/// Buffered materializer for metadata-only observations.
///
/// Accumulates records of type `R: Serialize` and flushes to JSON Lines format
/// when thresholds (`max_records`, `max_bytes`, or coalesce window) are reached.
pub struct ObservationMaterializer<R: Serialize + Send> {
    tx: mpsc::Sender<AppendRequest<R>>,
    handle: tokio::task::JoinHandle<()>,
}

/// Internal message for the buffer task.
struct AppendRequest<R: Serialize + Send> {
    record: R,
    reply: oneshot::Sender<RuntimeResult<()>>,
}

impl<R: Serialize + Send + 'static> ObservationMaterializer<R> {
    /// Create a new materializer with the given configuration.
    #[must_use]
    pub fn new(config: ObservationMaterializerConfig) -> Self {
        Self::with_callback(config, Arc::new(|_| Box::pin(async { Ok(()) })))
    }

    /// Create a new materializer with a custom flush callback.
    ///
    /// The callback is invoked whenever a batch is ready, and should stage
    /// the serialized JSON Lines data as source material.
    pub fn with_callback(
        config: ObservationMaterializerConfig,
        on_flush: Arc<FlushCallback>,
    ) -> Self {
        let (tx, rx) = mpsc::channel(config.max_records * 2);
        let config_clone = config.clone();

        let handle = tokio::spawn(buffer_task(rx, config_clone, on_flush));

        Self { tx, handle }
    }

    /// Append a record to the buffer.
    ///
    /// Returns `Ok(())` if the record was successfully buffered.
    /// Returns `Err` if the buffer channel is closed or capacity exceeded.
    pub async fn append(&mut self, record: R) -> RuntimeResult<()> {
        let (tx, rx): (
            oneshot::Sender<RuntimeResult<()>>,
            oneshot::Receiver<RuntimeResult<()>>,
        ) = oneshot::channel();
        self.tx
            .send(AppendRequest { record, reply: tx })
            .await
            .map_err(|_| SinexError::lifecycle("Observation buffer channel closed"))?;

        rx.await
            .map_err(|_| SinexError::lifecycle("Observation buffer reply dropped"))?
    }

    /// Wait for the materializer task to complete.
    ///
    /// Consumes the materializer and joins the internal buffer task.
    pub async fn join(self) -> RuntimeResult<()> {
        self.handle.await.map_err(|e| {
            SinexError::lifecycle(format!("Observation materializer task panicked: {e}"))
        })
    }
}

/// Internal buffer task that accumulates records and flushes on threshold.
async fn buffer_task<R: Serialize + Send + 'static>(
    mut rx: mpsc::Receiver<AppendRequest<R>>,
    config: ObservationMaterializerConfig,
    on_flush: Arc<FlushCallback>,
) {
    let mut buffer: Vec<R> = Vec::with_capacity(config.max_records);
    let mut buffer_bytes = 0usize;
    let mut last_flush = Instant::now();
    let coalesce_window = Duration::from_millis(config.batch_coalesce_window_ms);
    let mut timer = interval(coalesce_window);
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            Some(req) = rx.recv() => {
                // Try to serialize the record to estimate its size
                match serde_json::to_string(&req.record) {
                    Ok(line) => {
                        let line_size = line.len() + 1; // +1 for newline
                        buffer_bytes += line_size;
                        buffer.push(req.record);

                        // Determine if we need to flush
                        let should_flush = buffer.len() >= config.max_records
                            || buffer_bytes >= config.max_bytes;

                        if should_flush {
                            // Propagate the flush result to the caller: if the sink
                            // fails (disk full, CAS write error, …) the caller learns
                            // about it rather than silently receiving Ok(()).
                            let flush_result = flush_internal(
                                &mut buffer,
                                &mut buffer_bytes,
                                on_flush.as_ref(),
                            )
                            .await;
                            last_flush = Instant::now();
                            let _ = req.reply.send(flush_result);
                        } else {
                            let _ = req.reply.send(Ok(()));
                        }
                    }
                    Err(e) => {
                        let err = SinexError::processing(format!("Failed to serialize observation: {e}"));
                        let _ = req.reply.send(Err(err));
                    }
                }
            }
            _ = timer.tick() => {
                if last_flush.elapsed() >= coalesce_window && !buffer.is_empty() {
                    if let Err(e) = flush_internal(&mut buffer, &mut buffer_bytes, on_flush.as_ref()).await {
                        error!(
                            error = %e,
                            "Observation materializer: timer-triggered flush failed; \
                             records may not have been staged as source material"
                        );
                    }
                    last_flush = Instant::now();
                }
            }
            else => break,
        }
    }

    // Flush any remaining records on shutdown
    if !buffer.is_empty()
        && let Err(e) = flush_internal(&mut buffer, &mut buffer_bytes, on_flush.as_ref()).await
    {
        error!(
            error = %e,
            "Observation materializer: shutdown flush failed; \
             records may not have been staged as source material"
        );
    }
}

/// Internal helper to serialize and flush buffered records.
async fn flush_internal<R: Serialize>(
    buffer: &mut Vec<R>,
    buffer_bytes: &mut usize,
    on_flush: &FlushCallback,
) -> RuntimeResult<()> {
    if buffer.is_empty() {
        return Ok(());
    }

    // Capture before drain: buffer.len() is always 0 after drain(..).
    let record_count = buffer.len();
    let mut data = Vec::with_capacity(*buffer_bytes);
    let mut serialize_failures = 0usize;
    for record in buffer.drain(..) {
        match serde_json::to_string(&record) {
            Ok(line) => {
                data.extend_from_slice(line.as_bytes());
                data.push(b'\n');
            }
            Err(e) => {
                // A record that cannot be serialized cannot be durably staged.
                // Log it so the operator can investigate; do not silently drop it,
                // as that would corrupt the record count and hide data loss.
                warn!(
                    error = %e,
                    "Observation materializer: failed to serialize record; \
                     dropping from batch (data loss)"
                );
                serialize_failures += 1;
            }
        }
    }
    *buffer_bytes = 0;

    let staged_count = record_count - serialize_failures;
    debug!(
        staged = staged_count,
        dropped = serialize_failures,
        "Flushing observation records"
    );

    if staged_count == 0 {
        // Nothing serialized successfully; don't call on_flush with empty data.
        return Ok(());
    }

    let batch = SerializedBatch {
        data,
        record_count: staged_count,
    };
    on_flush(batch).await
}

#[cfg(test)]
#[path = "observation_test.rs"]
mod tests;
