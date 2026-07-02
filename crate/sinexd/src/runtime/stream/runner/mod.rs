//! `RuntimeRunner<T>` and its associated lifecycle/runtime helpers.
//!
//! This is the long-lived runtime kernel of stream modules. Keeping it isolated
//! from wire types, listener plumbing, and control-message helpers makes the
//! file navigable; further role splits inside this module are tracked as
//! follow-up work.

#[cfg(feature = "messaging")]
use super::control_protocol::{ControlCommandKind, RuntimeDrainComplete, control_command_kind};
use super::listener::{
    CONFIRMED_EVENT_CHANNEL_CAPACITY, LISTENER_RETRY_DELAY, RunnerConfirmedEventHandler,
    TASK_SHUTDOWN_GRACE_PERIOD, create_checkpoint_kv, maybe_start_schema_listener,
    run_resubscribing_listener,
};
use super::{
    Checkpoint, EventEmitter, ModuleKind, RunnerLifecycle, RuntimeCapabilities, RuntimeContext,
    RuntimeDrainController, RuntimeHandles, RuntimeInitContext, RuntimeModule, ScanArgs,
    ScanEstimate, ScanReport, ServiceInfo, SourceScanAck, SourceScanCommand, SourceScanProgress,
    TimeHorizon,
};
use crate::runtime::{
    RuntimeResult, SinexError,
    checkpoint::CheckpointManager,
    confirmation_handler::{ProcessingModel, ProvisionalEvent},
    event_transport::{EventBatcherConfig, EventTransport, spawn_event_batcher},
    jetstream_consumer::{JetStreamEventConsumer, JetStreamEventConsumerConfig},
    systemd_notify,
};
use camino::Utf8PathBuf;
use serde::Deserialize;
#[cfg(feature = "db")]
use sinex_db::DbPool as PgPool;
use sinex_db::models::SourceMaterial;
use sinex_db::repositories::DbPoolExt;
use sinex_primitives::events::Event;
use sinex_primitives::events::builder::{EventId, Provenance};
use sinex_primitives::{
    EventSource, EventType, HostName, Id, JsonValue, OffsetKind, Timestamp, Uuid,
    domain::ModuleState, non_empty::NonEmptyVec,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio_stream::StreamExt;
use tracing::{debug, error, info, warn};

const DEFAULT_EVENT_CHANNEL_SIZE: usize = 1024;

/// Unified runner for source drivers and automata.
type SourceFactory<T> = Arc<dyn Fn() -> T + Send + Sync>;

pub struct RuntimeRunner<T: RuntimeModule> {
    module: T,
    source_factory: Option<SourceFactory<T>>,
    lifecycle: RunnerLifecycle,
    handles: Option<RuntimeHandles>,
    service_info: Option<ServiceInfo>,
    raw_config: Option<HashMap<String, serde_json::Value>>,
    work_dir_utf8: Option<Utf8PathBuf>,
    event_batcher_handle: Option<tokio::task::JoinHandle<RuntimeResult<()>>>,
    event_batcher_shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    schema_listener_shutdown: Option<watch::Sender<bool>>,
    schema_listener_handle: Option<tokio::task::JoinHandle<()>>,
    checkpoint_cleanup_shutdown: Option<watch::Sender<bool>>,
    checkpoint_cleanup_handle: Option<tokio::task::JoinHandle<()>>,
    consumer_handle: Option<tokio::task::JoinHandle<()>>,
    command_listener_shutdown: Option<watch::Sender<bool>>,
    command_listener_handle: Option<tokio::task::JoinHandle<()>>,
    /// Per-source parse listener join handle (#1780). Started in service mode for
    /// source modules; aborted on shutdown. No shutdown channel: the listener
    /// holds a NATS subscription and is aborted directly (like `consumer_handle`).
    parse_listener_handle: Option<tokio::task::JoinHandle<()>>,
    processing_model: ProcessingModel,
    leader_state: Option<LeaderState>,
}

struct LeaderState {
    kv_client: sinex_primitives::coordination::CoordinationKvClient,
    instance_id: String,
    heartbeat_shutdown: tokio::sync::oneshot::Sender<()>,
    heartbeat_handle: tokio::task::JoinHandle<()>,
}

/// Batch of events resolved from provisional confirmations.
#[cfg(feature = "messaging")]
struct ResolvedBatch {
    events: Vec<Event<JsonValue>>,
    last_event_id: Option<Uuid>,
}

#[cfg(feature = "messaging")]
struct DispatchedScanOutcome {
    report: ScanReport,
    events_emitted: u64,
}

#[cfg(feature = "messaging")]
struct FailedDispatchedScanOutcome {
    error: SinexError,
    events_emitted: u64,
}

mod automaton_runtime;
mod batch;
mod command_listener;
mod construct;
mod control_messages;
mod dispatch;
mod initialize;
mod provisional;
mod registration;
mod service;
mod shutdown_helpers;
mod source_startup;

impl<T: RuntimeModule + 'static> RuntimeRunner<T> {
    /// Get module capabilities
    pub fn get_capabilities(&self) -> RuntimeCapabilities {
        self.module.capabilities()
    }

    /// Get scan estimate
    pub async fn estimate_scan_scope(
        &self,
        from: &Checkpoint,
        until: &TimeHorizon,
        args: &ScanArgs,
    ) -> RuntimeResult<ScanEstimate> {
        self.module.estimate_scan_scope(from, until, args).await
    }
}

mod shutdown;

#[cfg(test)]
use super::control_protocol::encode_control_message;
#[cfg(test)]
use super::{ContinuousStart, ProcessingStats};

#[cfg(test)]
#[path = "runner_test.rs"]
mod tests;
