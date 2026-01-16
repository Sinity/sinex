use super::{EventEmitter, EventSender, ProcessorHandles, ServiceInfo};
use crate::{
    acquisition_manager::{AcquisitionManager, RotationPolicy},
    checkpoint::CheckpointManager,
    confirmation_handler::ConfirmationBuffer,
    coordination::NodeCoordination,
    event_processor::EventTransport,
    heartbeat::HeartbeatEmitter,
    lifecycle::LifecycleManager,
    NodeResult,
};
use camino::Utf8PathBuf;
use serde_json::Value;
use sinex_core::db::models::Event;
use sinex_core::{db::SqlxPgPool as PgPool, types::Seconds, JsonValue};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

/// Captures runtime dependencies supplied to processors during initialization.
#[derive(Clone)]
pub struct ProcessorRuntimeState {
    service_info: ServiceInfo,
    handles: ProcessorHandles,
    raw_config: HashMap<String, Value>,
    work_dir_utf8: Utf8PathBuf,
}

impl ProcessorRuntimeState {
    pub fn new(
        service_info: ServiceInfo,
        handles: ProcessorHandles,
        raw_config: HashMap<String, Value>,
        work_dir_utf8: Utf8PathBuf,
    ) -> Self {
        Self {
            service_info,
            handles,
            raw_config,
            work_dir_utf8,
        }
    }

    pub fn service_info(&self) -> &ServiceInfo {
        &self.service_info
    }

    pub fn handles(&self) -> &ProcessorHandles {
        &self.handles
    }

    pub fn db_pool(&self) -> &PgPool {
        self.handles.require_db_pool()
    }

    pub fn checkpoint_manager(&self) -> Arc<CheckpointManager> {
        self.handles.checkpoint_manager()
    }

    pub fn event_emitter(&self) -> &EventEmitter {
        self.handles.emitter()
    }

    pub fn event_sender(&self) -> EventSender {
        (*self.handles.emitter().sender()).clone()
    }

    pub fn transport(&self) -> &EventTransport {
        self.handles.transport()
    }

    pub fn nats_client(&self) -> Option<async_nats::Client> {
        match self.handles.transport() {
            EventTransport::Nats(publisher) => Some(publisher.nats_client().clone()),
        }
    }

    pub fn confirmation_buffer(&self) -> Option<Arc<ConfirmationBuffer>> {
        self.handles.confirmation_buffer()
    }

    pub fn config_value<T: serde::de::DeserializeOwned>(&self, key: &str) -> Option<T> {
        self.raw_config
            .get(key)
            .and_then(|value| serde_json::from_value(value.clone()).ok())
    }

    pub fn raw_config_value(&self, key: &str) -> Option<&Value> {
        self.raw_config.get(key)
    }

    pub async fn emit_event(&self, event: Event<JsonValue>) -> crate::NodeResult<()> {
        self.event_emitter().emit(event).await
    }

    pub fn acquisition_manager(
        &self,
        rotation_policy: RotationPolicy,
        source_type: impl Into<String>,
        source_path: impl Into<String>,
    ) -> crate::NodeResult<AcquisitionManager> {
        Ok(AcquisitionManager::from_handles(
            self.handles(),
            rotation_policy,
            source_type,
            source_path,
        )?
        .with_work_dir(self.work_dir()))
    }

    pub fn heartbeat_emitter(&self, interval_seconds: Seconds) -> HeartbeatEmitter {
        HeartbeatEmitter::from_runtime(self, interval_seconds)
    }

    pub fn lifecycle_manager(&self) -> LifecycleManager {
        LifecycleManager::from_runtime(self)
    }

    pub async fn coordination(
        &self,
        instance_id: impl Into<String>,
    ) -> NodeResult<NodeCoordination> {
        NodeCoordination::from_runtime(self, instance_id.into()).await
    }

    pub fn raw_config(&self) -> &HashMap<String, Value> {
        &self.raw_config
    }

    pub fn work_dir(&self) -> &Path {
        self.work_dir_utf8.as_std_path()
    }

    pub fn work_dir_utf8(&self) -> &Utf8PathBuf {
        &self.work_dir_utf8
    }

    pub fn into_parts(
        self,
    ) -> (
        ServiceInfo,
        ProcessorHandles,
        HashMap<String, Value>,
        Utf8PathBuf,
    ) {
        (
            self.service_info,
            self.handles,
            self.raw_config,
            self.work_dir_utf8,
        )
    }
}
