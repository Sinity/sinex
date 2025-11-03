use super::{EventEmitter, EventSender, ProcessorHandles, ServiceInfo};
use crate::{
    checkpoint::CheckpointManager, confirmation_handler::ConfirmationBuffer,
    event_processor::EventTransport, lease_manager::LeaseManager,
};
use camino::Utf8PathBuf;
use serde_json::Value;
use sinex_core::db::SqlxPgPool as PgPool;
use std::collections::HashMap;
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
        self.handles.db_pool()
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

    pub fn lease_manager(&self) -> Option<Arc<LeaseManager>> {
        self.handles.lease_manager()
    }

    pub fn confirmation_buffer(&self) -> Option<Arc<ConfirmationBuffer>> {
        self.handles.confirmation_buffer()
    }

    pub fn raw_config(&self) -> &HashMap<String, Value> {
        &self.raw_config
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

    pub fn to_stream_context(&self) -> super::StreamProcessorContext {
        super::StreamProcessorContext::from_runtime(
            &self.service_info,
            &self.handles,
            self.raw_config.clone(),
            self.work_dir_utf8.clone(),
        )
    }
}
