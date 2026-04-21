use async_trait::async_trait;
use sinex_db::models::{Event, OffsetKind, Provenance, SourceMaterial};
use sinex_node_sdk::acquisition_manager::{
    AcquisitionManager, BufferedAppendStreamWriterConfig, SourceRecordAnchor,
};
use sinex_node_sdk::{BufferedRecordSink, NodeResult, RecordMaterializer, SinexError};
use sinex_primitives::{Id, JsonValue};
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

#[async_trait]
pub trait MaterialContext: Send + Sync + fmt::Debug {
    fn initial_provenance(&self) -> Provenance;
    async fn decorate_event(&self, event: &mut Event<JsonValue>) -> NodeResult<()>;
    async fn finalize(&self, reason: &str) -> NodeResult<()>;
    fn event_count(&self) -> u64;
}

pub type WatcherMaterialContext = Arc<dyn MaterialContext>;

const WRITER_BATCH_MAX_RECORDS: usize = 64;
const WRITER_BATCH_MAX_BYTES: usize = 128 * 1024;
const WRITER_BATCH_COALESCE_WINDOW: std::time::Duration = std::time::Duration::from_millis(20);

/// Writer-task–based material context.
///
/// Ownership of `SourceMaterialHandle` and the running byte-offset counter lives
/// exclusively inside the SDK buffered append writer, so no tokio `Mutex` is
/// held across NATS I/O.
///
/// `Clone` is cheap: all fields are `Arc`- or channel-handle–based.
#[derive(Clone)]
pub struct RealWatcherMaterialContext {
    material_id: Id<SourceMaterial>,
    materializer: RecordMaterializer<BufferedRecordSink>,
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
        let materializer = RecordMaterializer::new(BufferedRecordSink::from_active_handle(
            Arc::clone(&acquisition),
            handle,
            source_identifier.to_string(),
            BufferedAppendStreamWriterConfig {
                channel_capacity: WRITER_CHANNEL_CAPACITY,
                batch_max_records: WRITER_BATCH_MAX_RECORDS,
                batch_max_bytes: WRITER_BATCH_MAX_BYTES,
                batch_coalesce_window: WRITER_BATCH_COALESCE_WINDOW,
            },
        ));

        Ok(Self {
            material_id,
            materializer,
            event_count: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Send an append request and await the writer task's reply.
    ///
    /// Does **not** hold any mutex across the NATS write.
    async fn append_payload(&self, payload_bytes: &[u8]) -> NodeResult<SourceRecordAnchor> {
        self.materializer
            .append_stable_bytes(payload_bytes.to_vec())
            .await
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

        let anchor = self.append_payload(&payload_bytes).await?;
        let material_id = Id::<SourceMaterial>::from_uuid(anchor.material_id);
        event.provenance = Provenance::Material {
            id: material_id,
            anchor_byte: anchor.offset_start,
            offset_start: Some(anchor.offset_start),
            offset_end: Some(anchor.offset_end),
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
        self.materializer.finalize(reason).await
    }

    fn event_count(&self) -> u64 {
        self.event_count.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::{MaterialContext, RealWatcherMaterialContext};
    use serde_json::json;
    use sinex_node_sdk::acquisition_manager::{AcquisitionManager, RotationPolicy};
    use sinex_primitives::events::DynamicPayload;
    use sinex_primitives::{Bytes, Seconds, Uuid};
    use std::sync::Arc;
    use xtask::sandbox::prelude::*;

    #[sinex_test]
    async fn watcher_material_context_rotates_at_sdk_policy(ctx: TestContext) -> TestResult<()> {
        let ctx = ctx.with_nats().shared().await?;
        let namespace = format!("system-material-rotation-{}", Uuid::now_v7());
        let work_dir = std::env::temp_dir().join(format!("sinex-system-material-{namespace}"));
        tokio::fs::create_dir_all(&work_dir).await?;
        let acquisition = Arc::new(
            AcquisitionManager::new_with_namespace(
                ctx.nats_client(),
                RotationPolicy {
                    max_bytes: Bytes::from(8),
                    max_age_seconds: Seconds::from_secs(3600),
                },
                "system-test".to_string(),
                Some(namespace),
            )
            .with_work_dir(&work_dir),
        );
        let context =
            RealWatcherMaterialContext::new(acquisition, "test://rotating-system", json!({}))
                .await?;

        let mut first = DynamicPayload::new(
            "system.test",
            "system.test.payload",
            json!({ "record": "first-record" }),
        )
        .from_material(context.material_id)
        .build()?
        .to_json_event()?;
        let mut second = DynamicPayload::new(
            "system.test",
            "system.test.payload",
            json!({ "record": "second-record" }),
        )
        .from_material(context.material_id)
        .build()?
        .to_json_event()?;

        context.decorate_event(&mut first).await?;
        context.decorate_event(&mut second).await?;
        context.finalize("test-complete").await?;

        let Provenance::Material { id: first_id, .. } = first.provenance else {
            panic!("expected material provenance");
        };
        let Provenance::Material { id: second_id, .. } = second.provenance else {
            panic!("expected material provenance");
        };

        assert_eq!(
            first_id, context.material_id,
            "first event should use the initially exposed material"
        );
        assert_ne!(
            first_id, second_id,
            "hot watcher streams should rotate through the SDK stream acquirer"
        );
        let _ = tokio::fs::remove_dir_all(&work_dir).await;
        Ok(())
    }
}
