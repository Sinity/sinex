use super::runtime_state::NodeRuntimeState;
use crate::{
    EventTransport, SinexError, checkpoint::CheckpointManager,
    confirmation_handler::ConfirmationBuffer,
};
use camino::Utf8PathBuf;
#[cfg(feature = "db")]
use sinex_db::DbPool as PgPool;
use sinex_primitives::events::Event;
use sinex_primitives::{HostName, Id, JsonValue, Uuid};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{mpsc, watch};
use tracing::info;

pub type EventSender = mpsc::Sender<Event<JsonValue>>;
pub type EventStream = mpsc::Receiver<Event<JsonValue>>;

/// Shared one-way drain controller for a running node service.
///
/// The command listener raises the drain signal, long-running node loops
/// subscribe to it, and the runner can register abort handles for runtime-owned
/// background tasks that must stop accepting new work immediately.
#[derive(Clone)]
pub struct RuntimeDrainController {
    drain_tx: Arc<watch::Sender<bool>>,
    runtime_abort: Arc<std::sync::Mutex<Option<tokio::task::AbortHandle>>>,
}

impl RuntimeDrainController {
    #[must_use]
    pub fn new() -> Self {
        let (drain_tx, _drain_rx) = watch::channel(false);
        Self {
            drain_tx: Arc::new(drain_tx),
            runtime_abort: Arc::new(std::sync::Mutex::new(None)),
        }
    }

    #[must_use]
    pub fn subscribe(&self) -> watch::Receiver<bool> {
        self.drain_tx.subscribe()
    }

    #[must_use]
    pub fn is_requested(&self) -> bool {
        *self.drain_tx.borrow()
    }

    pub fn request_drain(&self) -> bool {
        if self.is_requested() {
            return false;
        }
        self.drain_tx.send_replace(true);
        true
    }

    pub fn register_runtime_abort(&self, abort_handle: tokio::task::AbortHandle) {
        let mut guard = self
            .runtime_abort
            .lock()
            .expect("runtime abort handle mutex poisoned");
        *guard = Some(abort_handle);
    }

    pub fn clear_runtime_abort(&self) {
        let mut guard = self
            .runtime_abort
            .lock()
            .expect("runtime abort handle mutex poisoned");
        *guard = None;
    }

    pub fn abort_runtime_work(&self) -> bool {
        let guard = self
            .runtime_abort
            .lock()
            .expect("runtime abort handle mutex poisoned");
        if let Some(handle) = guard.as_ref() {
            handle.abort();
            true
        } else {
            false
        }
    }
}

impl Default for RuntimeDrainController {
    fn default() -> Self {
        Self::new()
    }
}

/// Basic metadata about the running service.
#[derive(Debug, Clone)]
pub struct ServiceInfo {
    service_name: String,
    node_name: String,
    source_unit_id: Option<String>,
    runner_pack: Option<String>,
    host: HostName,
    work_dir: PathBuf,
    dry_run: bool,
    instance_id: String,
    version: String,
    node_run_id: Option<Uuid>,
}

impl ServiceInfo {
    #[must_use]
    pub fn new(
        service_name: String,
        node_name: String,
        host: HostName,
        work_dir: PathBuf,
        dry_run: bool,
        instance_id: String,
        version: String,
        node_run_id: Option<Uuid>,
    ) -> Self {
        Self {
            service_name,
            node_name,
            source_unit_id: None,
            runner_pack: None,
            host,
            work_dir,
            dry_run,
            instance_id,
            version,
            node_run_id,
        }
    }

    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_runtime_identity(
        service_name: String,
        node_name: String,
        source_unit_id: Option<String>,
        runner_pack: Option<String>,
        host: HostName,
        work_dir: PathBuf,
        dry_run: bool,
        instance_id: String,
        version: String,
        node_run_id: Option<Uuid>,
    ) -> Self {
        Self {
            service_name,
            node_name,
            source_unit_id,
            runner_pack,
            host,
            work_dir,
            dry_run,
            instance_id,
            version,
            node_run_id,
        }
    }

    #[must_use]
    pub fn service_name(&self) -> &str {
        &self.service_name
    }

    #[must_use]
    pub fn node_name(&self) -> &str {
        &self.node_name
    }

    #[must_use]
    pub fn source_unit_id(&self) -> Option<&str> {
        self.source_unit_id.as_deref()
    }

    #[must_use]
    pub fn runner_pack(&self) -> Option<&str> {
        self.runner_pack.as_deref()
    }

    #[must_use]
    pub fn checkpoint_identity(&self) -> &str {
        self.source_unit_id
            .as_deref()
            .unwrap_or(self.service_name.as_str())
    }

    #[must_use]
    pub fn control_identity(&self) -> &str {
        self.source_unit_id
            .as_deref()
            .unwrap_or(self.node_name.as_str())
    }

    #[must_use]
    pub fn host(&self) -> &HostName {
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

    #[must_use]
    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    #[must_use]
    pub fn version(&self) -> &str {
        &self.version
    }

    #[must_use]
    pub fn node_run_id(&self) -> Option<Uuid> {
        self.node_run_id
    }
}

/// Emit events while respecting dry-run semantics.
#[derive(Clone)]
pub struct EventEmitter {
    sender: Arc<EventSender>,
    dry_run: bool,
    default_node_run_id: Option<Uuid>,
    default_created_by_operation_id: Option<Uuid>,
    #[cfg(feature = "messaging")]
    validator: Option<Arc<crate::schema_validator::NodeSchemaValidator>>,
}

impl EventEmitter {
    #[must_use]
    pub fn new(sender: EventSender, dry_run: bool) -> Self {
        Self {
            sender: Arc::new(sender),
            dry_run,
            default_node_run_id: None,
            default_created_by_operation_id: None,
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
            default_node_run_id: None,
            default_created_by_operation_id: None,
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

    #[must_use]
    pub fn with_default_node_run_id(mut self, node_run_id: Uuid) -> Self {
        self.default_node_run_id = Some(node_run_id);
        self
    }

    #[must_use]
    pub fn with_default_created_by_operation_id(mut self, operation_id: Uuid) -> Self {
        self.default_created_by_operation_id = Some(operation_id);
        self
    }

    /// Rebuild this emitter around a different sender while preserving validation and dry-run policy.
    #[must_use]
    pub fn clone_with_sender(&self, sender: EventSender) -> Self {
        Self {
            sender: Arc::new(sender),
            dry_run: self.dry_run,
            default_node_run_id: self.default_node_run_id,
            default_created_by_operation_id: self.default_created_by_operation_id,
            #[cfg(feature = "messaging")]
            validator: self.validator.clone(),
        }
    }

    pub async fn emit(&self, mut event: Event<JsonValue>) -> Result<(), SinexError> {
        if event.id.is_none() {
            event.id = Some(Id::new());
        }

        if event.node_run_id.is_none() {
            event.node_run_id = self.default_node_run_id;
        }

        if event.created_by_operation_id.is_none() {
            event.created_by_operation_id = self.default_created_by_operation_id;
        }

        // Validate before emitting (if validator present)
        if let Some(validator) = &self.validator {
            let schema_id = validator
                .validate(
                    event.source.as_ref(),
                    event.event_type.as_ref(),
                    &event.payload,
                )
                .await?;
            if event.payload_schema_id.is_none() {
                event.payload_schema_id = Some(schema_id);
            }
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

#[cfg(test)]
mod tests {
    use super::EventEmitter;
    use sinex_primitives::events::{Event, Provenance};
    use sinex_primitives::{EventSource, EventType, HostName, Id, OffsetKind, Timestamp, Uuid};
    use xtask::sandbox::sinex_test;

    #[cfg(feature = "messaging")]
    #[sinex_test]
    async fn emit_stamps_payload_schema_id_from_validator() -> TestResult<()> {
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        let validator = std::sync::Arc::new(crate::schema_validator::NodeSchemaValidator::new());
        let schema_id = Uuid::now_v7();
        validator.register_test_schema(
            schema_id,
            "runtime-test-source",
            "runtime.test",
            &serde_json::json!({
                "type": "object",
                "required": ["ok"],
                "properties": {
                    "ok": { "type": "boolean" }
                },
                "additionalProperties": false
            }),
        )?;

        let emitter = EventEmitter::with_validator(sender, false, validator);
        let event = Event {
            id: Some(Id::new()),
            source: EventSource::new("runtime-test-source")?,
            event_type: EventType::new("runtime.test")?,
            payload: serde_json::json!({"ok": true}),
            ts_orig: Some(Timestamp::now()),
            host: HostName::from_static("runtime-test-host"),
            node_run_id: None,
            payload_schema_id: None,
            provenance: Provenance::Material {
                id: Id::from_uuid(Uuid::now_v7()),
                anchor_byte: 0,
                offset_start: None,
                offset_end: None,
                offset_kind: OffsetKind::Byte,
            },
            associated_blob_ids: None,
            temporal_policy: None,
            semantics_version: None,
            scope_key: None,
            equivalence_key: None,
            created_by_operation_id: None,
            node_model: None,
        };

        emitter.emit(event).await?;
        let emitted = receiver
            .recv()
            .await
            .ok_or_else(|| color_eyre::eyre::eyre!("missing emitted event"))?;
        assert_eq!(emitted.payload_schema_id, Some(schema_id));
        Ok(())
    }

    #[cfg(feature = "messaging")]
    #[sinex_test]
    async fn emit_preserves_existing_payload_schema_id() -> TestResult<()> {
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        let validator = std::sync::Arc::new(crate::schema_validator::NodeSchemaValidator::new());
        let cached_schema_id = Uuid::now_v7();
        validator.register_test_schema(
            cached_schema_id,
            "runtime-test-source",
            "runtime.test",
            &serde_json::json!({
                "type": "object",
                "required": ["ok"],
                "properties": {
                    "ok": { "type": "boolean" }
                },
                "additionalProperties": false
            }),
        )?;

        let emitter = EventEmitter::with_validator(sender, false, validator);
        let explicit_schema_id = Uuid::now_v7();
        let event = Event {
            id: Some(Id::new()),
            source: EventSource::new("runtime-test-source")?,
            event_type: EventType::new("runtime.test")?,
            payload: serde_json::json!({"ok": true}),
            ts_orig: Some(Timestamp::now()),
            host: HostName::from_static("runtime-test-host"),
            node_run_id: None,
            payload_schema_id: Some(explicit_schema_id),
            provenance: Provenance::Material {
                id: Id::from_uuid(Uuid::now_v7()),
                anchor_byte: 0,
                offset_start: None,
                offset_end: None,
                offset_kind: OffsetKind::Byte,
            },
            associated_blob_ids: None,
            temporal_policy: None,
            semantics_version: None,
            scope_key: None,
            equivalence_key: None,
            created_by_operation_id: None,
            node_model: None,
        };

        emitter.emit(event).await?;
        let emitted = receiver
            .recv()
            .await
            .ok_or_else(|| color_eyre::eyre::eyre!("missing emitted event"))?;
        assert_eq!(emitted.payload_schema_id, Some(explicit_schema_id));
        Ok(())
    }

    #[sinex_test]
    async fn emit_stamps_missing_event_id() -> TestResult<()> {
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        let emitter = EventEmitter::new(sender, false);

        let event = Event {
            id: None,
            source: EventSource::new("runtime-test-source")?,
            event_type: EventType::new("runtime.test")?,
            payload: serde_json::json!({"ok": true}),
            ts_orig: Some(Timestamp::now()),
            host: HostName::from_static("runtime-test-host"),
            node_run_id: None,
            payload_schema_id: None,
            provenance: Provenance::Material {
                id: Id::from_uuid(Uuid::now_v7()),
                anchor_byte: 0,
                offset_start: None,
                offset_end: None,
                offset_kind: OffsetKind::Byte,
            },
            associated_blob_ids: None,
            temporal_policy: None,
            semantics_version: None,
            scope_key: None,
            equivalence_key: None,
            created_by_operation_id: None,
            node_model: None,
        };

        emitter.emit(event).await?;
        let emitted = receiver
            .recv()
            .await
            .ok_or_else(|| color_eyre::eyre::eyre!("missing emitted event"))?;
        assert!(emitted.id.is_some());
        Ok(())
    }

    #[sinex_test]
    async fn emit_stamps_default_created_by_operation_id() -> TestResult<()> {
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        let operation_id = Uuid::now_v7();
        let emitter =
            EventEmitter::new(sender, false).with_default_created_by_operation_id(operation_id);

        let event = Event {
            id: Some(Id::new()),
            source: EventSource::new("runtime-test-source")?,
            event_type: EventType::new("runtime.test")?,
            payload: serde_json::json!({"ok": true}),
            ts_orig: Some(Timestamp::now()),
            host: HostName::from_static("runtime-test-host"),
            node_run_id: None,
            payload_schema_id: None,
            provenance: Provenance::Material {
                id: Id::from_uuid(Uuid::now_v7()),
                anchor_byte: 0,
                offset_start: None,
                offset_end: None,
                offset_kind: OffsetKind::Byte,
            },
            associated_blob_ids: None,
            temporal_policy: None,
            semantics_version: None,
            scope_key: None,
            equivalence_key: None,
            created_by_operation_id: None,
            node_model: None,
        };

        emitter.emit(event).await?;
        let emitted = receiver
            .recv()
            .await
            .ok_or_else(|| color_eyre::eyre::eyre!("missing emitted event"))?;
        assert_eq!(emitted.created_by_operation_id, Some(operation_id));
        Ok(())
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
    runtime_drain: Arc<RuntimeDrainController>,
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
            runtime_drain: Arc::new(RuntimeDrainController::new()),
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
            runtime_drain: Arc::new(RuntimeDrainController::new()),
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

    #[must_use]
    pub fn runtime_drain(&self) -> Arc<RuntimeDrainController> {
        Arc::clone(&self.runtime_drain)
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
