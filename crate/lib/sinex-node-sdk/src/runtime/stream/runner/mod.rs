//! `NodeRunner<T>` and its associated lifecycle/runtime helpers.
//!
//! This is the long-lived runtime kernel of stream nodes. Keeping it isolated
//! from wire types, listener plumbing, and control-message helpers makes the
//! file navigable; further role splits inside this module are tracked as
//! follow-up work.

use super::{
    Checkpoint, ContinuousStart, EventEmitter, EventSender, EventStream, MaterialReplayContext,
    Node, NodeCapabilities, NodeHandles, NodeInitContext, NodeRuntimeState, NodeScanAck,
    NodeScanCommand, NodeScanProgress, NodeType, ProcessingStats, ResolvedReplayMaterial,
    RunnerLifecycle, RuntimeDrainController, ScanArgs, ScanEstimate, ScanReport,
    SchemaBroadcastCache, SchemaBroadcastEntry, ServiceInfo, TimeHorizon,
};
use super::control_protocol::{
    ensure_control_payload_fits, encode_control_message, MAX_CONTROL_MESSAGE_BYTES,
};
#[cfg(feature = "messaging")]
use super::control_protocol::{ControlCommandKind, NodeDrainComplete, control_command_kind};
use super::listener::{
    CONFIRMED_EVENT_CHANNEL_CAPACITY, LISTENER_RETRY_DELAY, LISTENER_STARTUP_GRACE_PERIOD,
    RunnerConfirmedEventHandler, TASK_SHUTDOWN_GRACE_PERIOD, create_checkpoint_kv,
    maybe_start_schema_listener, run_resubscribing_listener,
};
use crate::{
    NodeResult, SinexError,
    checkpoint::CheckpointManager,
    confirmation_handler::{ConfirmedEventHandler, ProcessingModel, ProvisionalEvent},
    error_helpers::env_parse_with_default,
    event_node::{EventBatcherConfig, EventTransport, spawn_event_batcher},
    jetstream_consumer::{JetStreamEventConsumer, JetStreamEventConsumerConfig},
    systemd_notify,
};
use async_nats::jetstream::kv;
use camino::Utf8PathBuf;
use serde::{Deserialize, Serialize};
#[cfg(feature = "db")]
use sinex_db::DbPool as PgPool;
use sinex_db::SourceMaterialRecord;
use sinex_db::models::SourceMaterial;
use sinex_db::repositories::DbPoolExt;
use sinex_primitives::events::Event;
use sinex_primitives::events::builder::{EventId, Provenance};
use sinex_primitives::nats::{
    NatsTrafficClass, create_or_open_kv_store, insert_traffic_class_header,
};
use sinex_primitives::{
    EventSource, EventType, HostName, Id, JsonValue, OffsetKind, Timestamp, Uuid,
    domain::{NodeName, NodeState},
    non_empty::NonEmptyVec,
};
use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::{RwLock, oneshot, watch};
use tokio_stream::StreamExt;
use tracing::{debug, error, info, warn};

const DEFAULT_EVENT_CHANNEL_SIZE: usize = 1024;

/// Unified runner for nodes
type NodeFactory<T> = Arc<dyn Fn() -> T + Send + Sync>;

pub struct NodeRunner<T: Node> {
    node: T,
    node_factory: Option<NodeFactory<T>>,
    lifecycle: RunnerLifecycle,
    handles: Option<NodeHandles>,
    service_info: Option<ServiceInfo>,
    raw_config: Option<HashMap<String, serde_json::Value>>,
    work_dir_utf8: Option<Utf8PathBuf>,
    event_batcher_handle: Option<tokio::task::JoinHandle<NodeResult<()>>>,
    event_batcher_shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    schema_listener_shutdown: Option<watch::Sender<bool>>,
    schema_listener_handle: Option<tokio::task::JoinHandle<()>>,
    checkpoint_cleanup_shutdown: Option<watch::Sender<bool>>,
    checkpoint_cleanup_handle: Option<tokio::task::JoinHandle<()>>,
    consumer_handle: Option<tokio::task::JoinHandle<()>>,
    command_listener_shutdown: Option<watch::Sender<bool>>,
    command_listener_handle: Option<tokio::task::JoinHandle<()>>,
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

mod shutdown_helpers;
mod control_messages;
mod registration;
mod construct;
mod initialize;
mod service;
mod command_listener;
mod dispatch;
mod ingestor_startup;
mod automaton_runtime;
mod provisional;
mod batch;

impl<T: Node + 'static> NodeRunner<T> {






    /// Get node capabilities
    pub fn get_capabilities(&self) -> NodeCapabilities {
        self.node.capabilities()
    }

    /// Get scan estimate
    pub async fn estimate_scan_scope(
        &self,
        from: &Checkpoint,
        until: &TimeHorizon,
        args: &ScanArgs,
    ) -> NodeResult<ScanEstimate> {
        self.node.estimate_scan_scope(from, until, args).await
    }

}

#[cfg(test)]
mod tests;
mod shutdown;
