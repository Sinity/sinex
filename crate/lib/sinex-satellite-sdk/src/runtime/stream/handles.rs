use super::runtime_state::ProcessorRuntimeState;
use crate::{
    checkpoint::CheckpointManager, confirmation_handler::ConfirmationBuffer,
    event_processor::EventTransport, lease_manager::LeaseManager, SatelliteError,
};
use camino::Utf8PathBuf;
use sinex_core::db::models::Event;
use sinex_core::db::SqlxPgPool as PgPool;
use sinex_core::JsonValue;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::info;

pub type EventSender = mpsc::UnboundedSender<Event<JsonValue>>;
pub type EventStream = mpsc::UnboundedReceiver<Event<JsonValue>>;

/// Basic metadata about the running service.
#[derive(Debug, Clone)]
pub struct ServiceInfo {
    service_name: String,
    host: String,
    work_dir: PathBuf,
    dry_run: bool,
}

impl ServiceInfo {
    pub fn new(service_name: String, host: String, work_dir: PathBuf, dry_run: bool) -> Self {
        Self {
            service_name,
            host,
            work_dir,
            dry_run,
        }
    }

    pub fn service_name(&self) -> &str {
        &self.service_name
    }

    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn work_dir(&self) -> &PathBuf {
        &self.work_dir
    }

    pub fn dry_run(&self) -> bool {
        self.dry_run
    }
}

/// Emit events while respecting dry-run semantics.
#[derive(Clone)]
pub struct EventEmitter {
    sender: Arc<EventSender>,
    dry_run: bool,
}

impl EventEmitter {
    pub fn new(sender: EventSender, dry_run: bool) -> Self {
        Self {
            sender: Arc::new(sender),
            dry_run,
        }
    }

    pub fn dry_run(&self) -> bool {
        self.dry_run
    }

    pub fn sender(&self) -> Arc<EventSender> {
        Arc::clone(&self.sender)
    }

    pub async fn emit(&self, event: Event<JsonValue>) -> Result<(), SatelliteError> {
        let event_type = event.event_type.clone();
        if self.dry_run {
            info!(
                source = %event.source,
                event_type = %event_type,
                "DRY RUN: Would emit event"
            );
            return Ok(());
        }

        self.sender
            .send(event)
            .map_err(|_| SatelliteError::Processing("Event channel closed".to_string()))
    }
}

/// Handles made available to processors during initialization and runtime.
#[derive(Clone)]
pub struct ProcessorHandles {
    db_pool: PgPool,
    checkpoint_manager: Arc<CheckpointManager>,
    emitter: EventEmitter,
    transport: EventTransport,
    lease_manager: Option<Arc<LeaseManager>>,
    confirmation_buffer: Option<Arc<ConfirmationBuffer>>,
}

impl ProcessorHandles {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        db_pool: PgPool,
        checkpoint_manager: Arc<CheckpointManager>,
        emitter: EventEmitter,
        transport: EventTransport,
        lease_manager: Option<Arc<LeaseManager>>,
        confirmation_buffer: Option<Arc<ConfirmationBuffer>>,
    ) -> Self {
        Self {
            db_pool,
            checkpoint_manager,
            emitter,
            transport,
            lease_manager,
            confirmation_buffer,
        }
    }

    pub fn db_pool(&self) -> &PgPool {
        &self.db_pool
    }

    pub fn checkpoint_manager(&self) -> Arc<CheckpointManager> {
        Arc::clone(&self.checkpoint_manager)
    }

    pub fn emitter(&self) -> &EventEmitter {
        &self.emitter
    }

    pub fn transport(&self) -> &EventTransport {
        &self.transport
    }

    pub fn lease_manager(&self) -> Option<Arc<LeaseManager>> {
        self.lease_manager.as_ref().map(Arc::clone)
    }

    pub fn confirmation_buffer(&self) -> Option<Arc<ConfirmationBuffer>> {
        self.confirmation_buffer.as_ref().map(Arc::clone)
    }
}

/// Initialization context passed to processors.
pub struct ProcessorInitContext<C> {
    config: C,
    raw_config: std::collections::HashMap<String, serde_json::Value>,
    service: ServiceInfo,
    handles: ProcessorHandles,
    work_dir_utf8: Utf8PathBuf,
}

impl<C> ProcessorInitContext<C> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: C,
        raw_config: std::collections::HashMap<String, serde_json::Value>,
        service: ServiceInfo,
        handles: ProcessorHandles,
        work_dir_utf8: Utf8PathBuf,
    ) -> Self {
        Self {
            config,
            raw_config,
            service,
            handles,
            work_dir_utf8,
        }
    }

    pub fn config(&self) -> &C {
        &self.config
    }

    pub fn raw_config(&self) -> &std::collections::HashMap<String, serde_json::Value> {
        &self.raw_config
    }

    pub fn service_info(&self) -> &ServiceInfo {
        &self.service
    }

    pub fn handles(&self) -> &ProcessorHandles {
        &self.handles
    }

    pub fn work_dir_utf8(&self) -> &Utf8PathBuf {
        &self.work_dir_utf8
    }

    pub fn into_parts(
        self,
    ) -> (
        C,
        std::collections::HashMap<String, serde_json::Value>,
        ServiceInfo,
        ProcessorHandles,
        Utf8PathBuf,
    ) {
        (
            self.config,
            self.raw_config,
            self.service,
            self.handles,
            self.work_dir_utf8,
        )
    }

    /// Construct a runtime snapshot without consuming the context.
    pub fn runtime_state(&self) -> ProcessorRuntimeState {
        ProcessorRuntimeState::new(
            self.service.clone(),
            self.handles.clone(),
            self.raw_config.clone(),
            self.work_dir_utf8.clone(),
        )
    }

    /// Consume the context, yielding processor config and its runtime state.
    pub fn into_runtime(self) -> (C, ProcessorRuntimeState) {
        let runtime = ProcessorRuntimeState::new(
            self.service,
            self.handles,
            self.raw_config,
            self.work_dir_utf8,
        );
        (self.config, runtime)
    }
}
