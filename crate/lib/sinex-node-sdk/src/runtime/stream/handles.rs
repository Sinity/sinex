use super::runtime_state::NodeRuntimeState;
use crate::{
    EventTransport, SinexError, checkpoint::CheckpointManager,
    confirmation_handler::ConfirmationBuffer,
};
use camino::Utf8PathBuf;
#[cfg(feature = "db")]
use sinex_db::DbPool as PgPool;
use sinex_primitives::JsonValue;
use sinex_primitives::events::Event;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::info;

pub type EventSender = mpsc::Sender<Event<JsonValue>>;
pub type EventStream = mpsc::Receiver<Event<JsonValue>>;

/// Basic metadata about the running service.
#[derive(Debug, Clone)]
pub struct ServiceInfo {
    service_name: String,
    host: String,
    work_dir: PathBuf,
    dry_run: bool,
}

impl ServiceInfo {
    #[must_use]
    pub fn new(service_name: String, host: String, work_dir: PathBuf, dry_run: bool) -> Self {
        Self {
            service_name,
            host,
            work_dir,
            dry_run,
        }
    }

    #[must_use]
    pub fn service_name(&self) -> &str {
        &self.service_name
    }

    #[must_use]
    pub fn host(&self) -> &str {
        &self.host
    }

    #[must_use]
    pub fn work_dir(&self) -> &PathBuf {
        &self.work_dir
    }

    #[must_use]
    pub fn dry_run(&self) -> bool {
        self.dry_run
    }
}

/// Emit events while respecting dry-run semantics.
#[derive(Clone)]
pub struct EventEmitter {
    sender: Arc<EventSender>,
    dry_run: bool,
    #[cfg(feature = "messaging")]
    validator: Option<Arc<crate::schema_validator::NodeSchemaValidator>>,
}

impl EventEmitter {
    #[must_use]
    pub fn new(sender: EventSender, dry_run: bool) -> Self {
        Self {
            sender: Arc::new(sender),
            dry_run,
            #[cfg(feature = "messaging")]
            validator: None,
        }
    }

    /// Create `EventEmitter` with schema validation enabled
    #[cfg(feature = "messaging")]
    #[must_use]
    pub fn with_validator(
        sender: EventSender,
        dry_run: bool,
        validator: Arc<crate::schema_validator::NodeSchemaValidator>,
    ) -> Self {
        Self {
            sender: Arc::new(sender),
            dry_run,
            validator: Some(validator),
        }
    }

    #[must_use]
    pub fn dry_run(&self) -> bool {
        self.dry_run
    }

    #[must_use]
    pub fn sender(&self) -> Arc<EventSender> {
        Arc::clone(&self.sender)
    }

    /// Rebuild this emitter around a different sender while preserving validation and dry-run policy.
    #[must_use]
    pub fn clone_with_sender(&self, sender: EventSender) -> Self {
        Self {
            sender: Arc::new(sender),
            dry_run: self.dry_run,
            #[cfg(feature = "messaging")]
            validator: self.validator.clone(),
        }
    }

    pub async fn emit(&self, event: Event<JsonValue>) -> Result<(), SinexError> {
        // Validate before emitting (if validator present)
        if let Some(validator) = &self.validator {
            validator
                .validate(
                    event.source.as_ref(),
                    event.event_type.as_ref(),
                    &event.payload,
                )
                .await
                .map_err(|e| SinexError::validation(e.to_string()))?;
        }

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
            .await
            .map_err(|_| SinexError::processing("Event channel closed".to_string()))
    }
}

/// Handles made available to nodes during initialization and runtime.
#[derive(Clone)]
pub struct NodeHandles {
    #[cfg(feature = "db")]
    db_pool: Option<PgPool>,
    checkpoint_manager: Arc<CheckpointManager>,
    emitter: EventEmitter,
    transport: EventTransport,
    confirmation_buffer: Option<Arc<ConfirmationBuffer>>,
    schema_cache: Option<Arc<crate::runtime::stream::SchemaBroadcastCache>>,
}

impl NodeHandles {
    #[allow(clippy::too_many_arguments)]
    #[cfg(feature = "db")]
    #[must_use]
    pub fn new(
        db_pool: PgPool,
        checkpoint_manager: Arc<CheckpointManager>,
        emitter: EventEmitter,
        transport: EventTransport,
        confirmation_buffer: Option<Arc<ConfirmationBuffer>>,
        schema_cache: Option<Arc<crate::runtime::stream::SchemaBroadcastCache>>,
    ) -> Self {
        Self {
            db_pool: Some(db_pool),
            checkpoint_manager,
            emitter,
            transport,
            confirmation_buffer,
            schema_cache,
        }
    }

    /// Create `NodeHandles` for Edge Mode (no database)
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new_edge(
        checkpoint_manager: Arc<CheckpointManager>,
        emitter: EventEmitter,
        transport: EventTransport,
        confirmation_buffer: Option<Arc<ConfirmationBuffer>>,
        schema_cache: Option<Arc<crate::runtime::stream::SchemaBroadcastCache>>,
    ) -> Self {
        Self {
            #[cfg(feature = "db")]
            db_pool: None,
            checkpoint_manager,
            emitter,
            transport,
            confirmation_buffer,
            schema_cache,
        }
    }

    /// Get database pool if available (Edge Mode returns None)
    #[cfg(feature = "db")]
    #[must_use]
    pub fn db_pool(&self) -> Option<&PgPool> {
        self.db_pool.as_ref()
    }

    /// Get database pool or panic with a helpful error message
    #[cfg(feature = "db")]
    #[allow(clippy::expect_used)] // Intentional: "require" methods panic by contract
    #[must_use]
    pub fn require_db_pool(&self) -> &PgPool {
        self.db_pool.as_ref().expect(
            "Database pool required but not available. \
             This node cannot run in Edge Mode (SINEX_EDGE_MODE=1). \
             Either provide DATABASE_URL or refactor to use NATS-only data flow.",
        )
    }

    #[must_use]
    pub fn checkpoint_manager(&self) -> Arc<CheckpointManager> {
        Arc::clone(&self.checkpoint_manager)
    }

    #[must_use]
    pub fn emitter(&self) -> &EventEmitter {
        &self.emitter
    }

    #[must_use]
    pub fn transport(&self) -> &EventTransport {
        &self.transport
    }

    pub fn confirmation_buffer(&self) -> Option<Arc<ConfirmationBuffer>> {
        self.confirmation_buffer.as_ref().map(Arc::clone)
    }

    pub fn schema_cache(&self) -> Option<Arc<crate::runtime::stream::SchemaBroadcastCache>> {
        self.schema_cache.as_ref().map(Arc::clone)
    }
}

/// Initialization context passed to nodes.
pub struct NodeInitContext<C> {
    config: C,
    raw_config: std::collections::HashMap<String, serde_json::Value>,
    service: ServiceInfo,
    handles: NodeHandles,
    work_dir_utf8: Utf8PathBuf,
}

impl<C> NodeInitContext<C> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: C,
        raw_config: std::collections::HashMap<String, serde_json::Value>,
        service: ServiceInfo,
        handles: NodeHandles,
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

    pub fn handles(&self) -> &NodeHandles {
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
        NodeHandles,
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
    pub fn runtime_state(&self) -> NodeRuntimeState {
        NodeRuntimeState::new(
            self.service.clone(),
            self.handles.clone(),
            self.raw_config.clone(),
            self.work_dir_utf8.clone(),
        )
    }

    /// Consume the context, yielding node config and runtime state.
    pub fn into_runtime(self) -> (C, NodeRuntimeState) {
        let runtime = NodeRuntimeState::new(
            self.service,
            self.handles,
            self.raw_config,
            self.work_dir_utf8,
        );
        (self.config, runtime)
    }
}
