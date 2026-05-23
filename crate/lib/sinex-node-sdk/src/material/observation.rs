//! Buffered batching for metadata-only events (observation materialization).
//!
//! This module abstracts the buffering pattern seen in `sinex-system-ingestor`:
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

use crate::NodeResult;
use serde::Serialize;
use sinex_primitives::SinexError;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{Duration, Instant, interval};
use tracing::debug;

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
    std::pin::Pin<Box<dyn std::future::Future<Output = NodeResult<()>> + Send + 'static>>;

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
    reply: oneshot::Sender<NodeResult<()>>,
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
    pub async fn append(&mut self, record: R) -> NodeResult<()> {
        let (tx, rx): (
            oneshot::Sender<NodeResult<()>>,
            oneshot::Receiver<NodeResult<()>>,
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
    pub async fn join(self) -> NodeResult<()> {
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
                            let _ = flush_internal(&mut buffer, &mut buffer_bytes, on_flush.as_ref()).await;
                            last_flush = Instant::now();
                        }

                        let _ = req.reply.send(Ok(()));
                    }
                    Err(e) => {
                        let err = SinexError::processing(format!("Failed to serialize observation: {e}"));
                        let _ = req.reply.send(Err(err));
                    }
                }
            }
            _ = timer.tick() => {
                if last_flush.elapsed() >= coalesce_window && !buffer.is_empty() {
                    let _ = flush_internal(&mut buffer, &mut buffer_bytes, on_flush.as_ref()).await;
                    last_flush = Instant::now();
                }
            }
            else => break,
        }
    }

    // Flush any remaining records on shutdown
    if !buffer.is_empty() {
        let _ = flush_internal(&mut buffer, &mut buffer_bytes, on_flush.as_ref()).await;
    }
}

/// Internal helper to serialize and flush buffered records.
async fn flush_internal<R: Serialize>(
    buffer: &mut Vec<R>,
    buffer_bytes: &mut usize,
    on_flush: &FlushCallback,
) -> NodeResult<()> {
    if buffer.is_empty() {
        return Ok(());
    }

    let mut data = Vec::with_capacity(*buffer_bytes);
    for record in buffer.drain(..) {
        if let Ok(line) = serde_json::to_string(&record) {
            data.extend_from_slice(line.as_bytes());
            data.push(b'\n');
        }
    }

    let record_count = buffer.len();
    *buffer_bytes = 0;

    let batch = SerializedBatch { data, record_count };

    debug!("Flushing {} observation records", record_count);
    on_flush(batch).await
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc as StdArc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::time::sleep;
    use xtask::sandbox::prelude::sinex_test;

    #[derive(Serialize, Clone)]
    struct TestRecord {
        id: usize,
        value: String,
    }

    #[sinex_test]
    async fn test_append_single_record() -> xtask::sandbox::TestResult<()> {
        let mut mat =
            ObservationMaterializer::<TestRecord>::new(ObservationMaterializerConfig::default());

        let record = TestRecord {
            id: 1,
            value: "test".to_string(),
        };

        let result = mat.append(record).await;
        assert!(result.is_ok());
        Ok(())
    }

    #[sinex_test]
    async fn test_flush_on_max_records() -> xtask::sandbox::TestResult<()> {
        let flush_count = StdArc::new(AtomicUsize::new(0));
        let flush_count_clone = flush_count.clone();

        let on_flush: Arc<FlushCallback> = Arc::new(move |batch: SerializedBatch| -> FlushFuture {
            let fc = flush_count_clone.clone();
            Box::pin(async move {
                fc.fetch_add(1, Ordering::SeqCst);
                assert!(!batch.data.is_empty());
                Ok(())
            })
        });

        let config = ObservationMaterializerConfig {
            batch_coalesce_window_ms: 1000,
            max_records: 3,
            max_bytes: 128 * 1024,
        };

        let mut mat = ObservationMaterializer::<TestRecord>::with_callback(config, on_flush);

        for i in 0..3 {
            let record = TestRecord {
                id: i,
                value: format!("test{i}"),
            };
            let _ = mat.append(record).await;
        }

        // After appending 3 records with max_records=3, should have flushed
        sleep(Duration::from_millis(50)).await;
        assert_eq!(flush_count.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[sinex_test]
    async fn test_flush_on_window_timeout() -> xtask::sandbox::TestResult<()> {
        let flush_count = StdArc::new(AtomicUsize::new(0));
        let flush_count_clone = flush_count.clone();

        let on_flush: Arc<FlushCallback> =
            Arc::new(move |_batch: SerializedBatch| -> FlushFuture {
                let fc = flush_count_clone.clone();
                Box::pin(async move {
                    fc.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
            });

        let config = ObservationMaterializerConfig {
            batch_coalesce_window_ms: 50,
            max_records: 100,
            max_bytes: 128 * 1024,
        };

        let mut mat = ObservationMaterializer::<TestRecord>::with_callback(config, on_flush);

        let record = TestRecord {
            id: 1,
            value: "test".to_string(),
        };
        let _ = mat.append(record).await;

        // Wait for window timeout
        sleep(Duration::from_millis(150)).await;

        // Should have flushed due to timeout
        assert_eq!(flush_count.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[sinex_test]
    async fn test_empty_flush_is_noop() -> xtask::sandbox::TestResult<()> {
        let flush_count = StdArc::new(AtomicUsize::new(0));
        let flush_count_clone = flush_count.clone();

        let on_flush: Arc<FlushCallback> =
            Arc::new(move |_batch: SerializedBatch| -> FlushFuture {
                let fc = flush_count_clone.clone();
                Box::pin(async move {
                    fc.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
            });

        let config = ObservationMaterializerConfig {
            batch_coalesce_window_ms: 50,
            max_records: 100,
            max_bytes: 128 * 1024,
        };

        let _mat = ObservationMaterializer::<TestRecord>::with_callback(config, on_flush);

        // Don't append anything, just let it sit
        sleep(Duration::from_millis(150)).await;

        // Should not flush if buffer is empty
        assert_eq!(flush_count.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[sinex_test]
    async fn test_flush_on_max_bytes() -> xtask::sandbox::TestResult<()> {
        let flush_count = StdArc::new(AtomicUsize::new(0));
        let flush_count_clone = flush_count.clone();

        let on_flush: Arc<FlushCallback> =
            Arc::new(move |_batch: SerializedBatch| -> FlushFuture {
                let fc = flush_count_clone.clone();
                Box::pin(async move {
                    fc.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
            });

        let config = ObservationMaterializerConfig {
            batch_coalesce_window_ms: 1000,
            max_records: 1000,
            max_bytes: 100, // Small threshold to trigger quickly
        };

        let mut mat = ObservationMaterializer::<TestRecord>::with_callback(config, on_flush);

        // Add records until we exceed max_bytes
        for i in 0..10 {
            let record = TestRecord {
                id: i,
                value: "x".repeat(30), // ~30 bytes per record
            };
            let _ = mat.append(record).await;
        }

        sleep(Duration::from_millis(50)).await;

        // Should have flushed due to exceeding max_bytes
        let flushes = flush_count.load(Ordering::SeqCst);
        assert!(flushes > 0, "Expected at least 1 flush, got {flushes}");
        Ok(())
    }

    #[sinex_test]
    async fn test_serialization_error_propagates() -> xtask::sandbox::TestResult<()> {
        let flush_count = StdArc::new(AtomicUsize::new(0));
        let flush_count_clone = flush_count.clone();

        let on_flush: Arc<FlushCallback> =
            Arc::new(move |_batch: SerializedBatch| -> FlushFuture {
                let fc = flush_count_clone.clone();
                Box::pin(async move {
                    fc.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
            });

        let config = ObservationMaterializerConfig::default();
        let mut mat = ObservationMaterializer::<TestRecord>::with_callback(config, on_flush);

        let record = TestRecord {
            id: 1,
            value: "test".to_string(),
        };

        let result = mat.append(record).await;
        assert!(result.is_ok());
        Ok(())
    }

    #[sinex_test]
    async fn test_multiple_flushes_accumulate() -> xtask::sandbox::TestResult<()> {
        let flush_count = StdArc::new(AtomicUsize::new(0));
        let total_records = StdArc::new(AtomicUsize::new(0));
        let flush_count_clone = flush_count.clone();
        let total_records_clone = total_records.clone();

        let on_flush: Arc<FlushCallback> = Arc::new(move |batch: SerializedBatch| -> FlushFuture {
            let fc = flush_count_clone.clone();
            let tr = total_records_clone.clone();
            Box::pin(async move {
                fc.fetch_add(1, Ordering::SeqCst);
                tr.fetch_add(batch.record_count, Ordering::SeqCst);
                Ok(())
            })
        });

        let config = ObservationMaterializerConfig {
            batch_coalesce_window_ms: 50,
            max_records: 2,
            max_bytes: 128 * 1024,
        };

        let mut mat = ObservationMaterializer::<TestRecord>::with_callback(config, on_flush);

        // Append 5 records in batches of 2
        for i in 0..5 {
            let record = TestRecord {
                id: i,
                value: format!("test{i}"),
            };
            let _ = mat.append(record).await;
        }

        sleep(Duration::from_millis(150)).await;

        let flushes = flush_count.load(Ordering::SeqCst);
        assert!(flushes >= 2, "Expected at least 2 flushes, got {flushes}");
        Ok(())
    }
}
