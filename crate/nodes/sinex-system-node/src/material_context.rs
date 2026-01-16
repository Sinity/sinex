use color_eyre::eyre::eyre;
use sinex_core::db::models::event::OffsetKind;
use sinex_core::db::models::{Event, Provenance, SourceMaterial};
use sinex_core::{Id, JsonValue};
use sinex_node_sdk::acquisition_manager::{AcquisitionManager, SourceMaterialHandle};
use sinex_node_sdk::{NodeError, NodeResult};
use std::fmt;
use std::sync::Arc;
use tokio::sync::Mutex;

struct MaterialState {
    handle: Option<SourceMaterialHandle>,
    bytes_written: i64,
}

#[derive(Clone)]
pub(crate) struct WatcherMaterialContext {
    acquisition: Arc<AcquisitionManager>,
    material_id: Id<SourceMaterial>,
    state: Arc<Mutex<MaterialState>>,
}

impl fmt::Debug for WatcherMaterialContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WatcherMaterialContext")
            .field("material_id", &self.material_id)
            .finish()
    }
}

impl WatcherMaterialContext {
    pub(crate) async fn new(
        acquisition: Arc<AcquisitionManager>,
        source_identifier: &str,
        metadata: JsonValue,
    ) -> NodeResult<Self> {
        let handle = acquisition
            .begin_material_with_metadata(source_identifier, metadata)
            .await
            .map_err(|e| {
                NodeError::General(eyre!("Failed to begin system watcher material: {}", e))
            })?;
        let material_id = Id::from_ulid(handle.material_id);

        Ok(Self {
            acquisition,
            material_id,
            state: Arc::new(Mutex::new(MaterialState {
                handle: Some(handle),
                bytes_written: 0,
            })),
        })
    }

    pub(crate) fn initial_provenance(&self) -> Provenance {
        Provenance::Material {
            id: self.material_id,
            anchor_byte: 0,
            offset_start: None,
            offset_end: None,
            offset_kind: OffsetKind::Byte,
        }
    }

    pub(crate) async fn decorate_event(&self, event: &mut Event<JsonValue>) -> NodeResult<()> {
        let payload_bytes = serde_json::to_vec(&event.payload).map_err(|e| {
            NodeError::Processing(format!("Failed to serialize system payload: {}", e))
        })?;

        let (offset_start, offset_end) = self.append_payload(&payload_bytes).await?;
        event.provenance = Provenance::Material {
            id: self.material_id,
            anchor_byte: offset_start,
            offset_start: Some(offset_start),
            offset_end: Some(offset_end),
            offset_kind: OffsetKind::Byte,
        };

        if let Some(obj) = event.payload.as_object_mut() {
            obj.insert(
                "_source_material_id".to_string(),
                serde_json::json!(self.material_id.to_string()),
            );
        }

        Ok(())
    }

    pub(crate) async fn finalize(&self, reason: &str) -> NodeResult<()> {
        let handle = {
            let mut guard = self.state.lock().await;
            guard.handle.take()
        };

        if let Some(handle) = handle {
            self.acquisition
                .finalize(handle, reason)
                .await
                .map_err(|e| {
                    NodeError::General(eyre!(
                        "Failed to finalize system watcher material: {}",
                        e
                    ))
                })?;
        }

        Ok(())
    }

    async fn append_payload(&self, payload_bytes: &[u8]) -> NodeResult<(i64, i64)> {
        let mut guard = self.state.lock().await;
        let offset_start = guard.bytes_written;
        let offset_end = offset_start + payload_bytes.len() as i64;

        let handle = guard.handle.as_mut().ok_or_else(|| {
            NodeError::Processing("System watcher material already finalized".to_string())
        })?;

        if !payload_bytes.is_empty() {
            self.acquisition
                .append_slice(handle, payload_bytes)
                .await
                .map_err(|e| {
                    NodeError::General(eyre!("Failed to append system payload: {}", e))
                })?;
        }

        guard.bytes_written = offset_end;

        Ok((offset_start, offset_end))
    }
}
