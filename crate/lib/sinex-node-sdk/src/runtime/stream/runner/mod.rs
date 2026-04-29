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

impl<T: Node + 'static> NodeRunner<T> {




    #[cfg(feature = "messaging")]
    async fn load_bridge_checkpoint_state(
        checkpoint_manager: &CheckpointManager,
    ) -> NodeResult<crate::checkpoint::CheckpointState> {
        checkpoint_manager.load_checkpoint().await.map_err(|error| {
            SinexError::checkpoint("Failed to load checkpoint state for automaton bridge")
                .with_source(error)
        })
    }

    #[cfg(feature = "db")]
    async fn fetch_persisted_event(
        pool: &PgPool,
        event_id: &EventId,
    ) -> NodeResult<Option<Event<JsonValue>>> {
        let event_id_str = event_id.to_string();
        pool.events().get_by_id(*event_id).await.map_err(|err| {
            SinexError::processing(format!(
                "Failed to load confirmed event {event_id_str} from database: {err}"
            ))
        })
    }

    fn parse_uuid(value: &str, field: &str) -> NodeResult<Uuid> {
        value.parse::<Uuid>().map_err(|err| {
            SinexError::processing(format!("Invalid UUID for {field}: {value} ({err})"))
        })
    }

    fn parse_offset_kind(kind: Option<&str>) -> OffsetKind {
        match kind {
            Some("line") => OffsetKind::Line,
            Some("rowid") => OffsetKind::Record,
            Some("logical") => OffsetKind::Character,
            Some("byte") | None => OffsetKind::Byte,
            Some(_) => OffsetKind::Byte,
        }
    }

    fn build_event_from_provisional(
        provisional: &ProvisionalEvent,
    ) -> NodeResult<Event<JsonValue>> {
        #[derive(Deserialize)]
        struct PublishedEventPayload {
            source: String,
            event_type: String,
            host: String,
            #[serde(rename = "payload")]
            event_payload: JsonValue,
            node_run_id: Option<String>,
            payload_schema_id: Option<String>,
            associated_blob_ids: Option<Vec<String>>,
            source_material_id: Option<String>,
            anchor_byte: Option<i64>,
            offset_start: Option<i64>,
            offset_end: Option<i64>,
            offset_kind: Option<String>,
            source_event_ids: Option<Vec<String>>,
        }

        let published: PublishedEventPayload = serde_json::from_value(provisional.payload.clone())
            .map_err(|err| {
                SinexError::processing(format!("Failed to parse provisional event payload: {err}"))
            })?;

        // Parse provenance fields for flat Event struct
        let provenance = match (published.source_material_id, published.source_event_ids) {
            (Some(material_id), None) => {
                let anchor_byte = published.anchor_byte.ok_or_else(|| {
                    SinexError::processing("Material provenance missing anchor_byte".to_string())
                })?;
                let material_uuid = Self::parse_uuid(&material_id, "source_material_id")?;
                Provenance::Material {
                    id: Id::<SourceMaterial>::from_uuid(material_uuid),
                    anchor_byte,
                    offset_start: published.offset_start,
                    offset_end: published.offset_end,
                    offset_kind: Self::parse_offset_kind(published.offset_kind.as_deref()),
                }
            }
            (None, Some(source_ids)) => {
                let mut ids = Vec::new();
                for raw_id in source_ids {
                    let source_uuid = Self::parse_uuid(&raw_id, "source_event_ids")?;
                    ids.push(EventId::from_uuid(source_uuid));
                }
                let source_event_ids = NonEmptyVec::from_vec(ids).ok_or_else(|| {
                    SinexError::processing(
                        "Synthesis provenance missing source_event_ids".to_string(),
                    )
                })?;
                Provenance::Synthesis {
                    source_event_ids,
                    operation_id: None,
                }
            }
            (Some(_), Some(_)) => {
                return Err(SinexError::processing(
                    "Provisional event contains both material and synthesis provenance".to_string(),
                ));
            }
            (None, None) => {
                return Err(SinexError::processing(
                    "Provisional event missing provenance".to_string(),
                ));
            }
        };

        let payload_schema_id = published
            .payload_schema_id
            .map(|value| Self::parse_uuid(&value, "payload_schema_id"))
            .transpose()?;
        let associated_blob_ids = match published.associated_blob_ids {
            Some(ids) => {
                let mut parsed = Vec::with_capacity(ids.len());
                for raw_id in ids {
                    parsed.push(Self::parse_uuid(&raw_id, "associated_blob_ids")?);
                }
                Some(parsed)
            }
            None => None,
        };
        let node_run_id = published
            .node_run_id
            .as_deref()
            .map(|value| Self::parse_uuid(value, "node_run_id"))
            .transpose()?;

        Ok(Event {
            id: Some(provisional.event_id),
            source: EventSource::from(published.source),
            event_type: EventType::from(published.event_type),
            payload: published.event_payload,
            ts_orig: Some(provisional.ts_orig),
            host: HostName::new(published.host).map_err(|error| {
                SinexError::processing("Invalid host in provisional event payload")
                    .with_source(error)
            })?,
            node_run_id,
            payload_schema_id,
            provenance,
            associated_blob_ids,
            temporal_policy: None,
            semantics_version: None,
            scope_key: None,
            equivalence_key: None,
            created_by_operation_id: None,
            node_model: None,
        })
    }

    // ── Helper methods extracted from run_automaton_event_bridge ──

    /// Resolve provisional event confirmations into full `Event` values.
    ///
    /// With `db` feature: fetches persisted events from the database when a pool
    /// is available, falling back to parsing the provisional payload directly.
    /// Without `db`: always parses from the provisional payload.
    #[cfg(feature = "messaging")]
    async fn resolve_provisionals_to_events(
        provisionals: &[ProvisionalEvent],
        #[cfg(feature = "db")] db_pool: &Option<PgPool>,
    ) -> NodeResult<ResolvedBatch> {
        let mut events = Vec::with_capacity(provisionals.len());
        let mut last_event_id = None;

        for provisional in provisionals {
            let event_id = &provisional.event_id;
            let event = {
                #[cfg(feature = "db")]
                {
                    match db_pool {
                        Some(pool) => {
                            if let Some(event) = Self::fetch_persisted_event(pool, event_id).await?
                            {
                                Some(event)
                            } else {
                                return Err(Self::confirmed_event_missing_error(event_id));
                            }
                        }
                        None => Some(
                            Self::build_event_from_provisional(provisional)
                                .map_err(|error| Self::provisional_decode_error(event_id, error))?,
                        ),
                    }
                }
                #[cfg(not(feature = "db"))]
                {
                    Some(
                        Self::build_event_from_provisional(provisional)
                            .map_err(|error| Self::provisional_decode_error(event_id, error))?,
                    )
                }
            };

            if let Some(event) = event {
                last_event_id = Some(*event_id.as_uuid());
                events.push(event);
            }
        }

        Ok(ResolvedBatch {
            events,
            last_event_id,
        })
    }

    #[cfg(feature = "messaging")]
    fn confirmed_event_missing_error(event_id: &EventId) -> SinexError {
        SinexError::processing("Confirmed event missing from database")
            .with_context("event_id", event_id.to_string())
    }

    #[cfg(feature = "messaging")]
    fn provisional_decode_error(event_id: &EventId, error: SinexError) -> SinexError {
        SinexError::processing(
            "Confirmed event could not be reconstructed from provisional payload",
        )
        .with_context("event_id", event_id.to_string())
        .with_source(error)
    }

    /// Process a batch of events, falling back to per-event processing with DLQ
    /// routing if the batch fails. Returns the total number of events processed
    /// (including those routed to the DLQ).
    #[cfg(feature = "messaging")]
    async fn process_batch_with_dlq_fallback(
        node: &mut T,
        transport: &EventTransport,
        events: Vec<Event<JsonValue>>,
    ) -> NodeResult<u64> {
        let batch_size = events.len();
        let events_backup = events.clone();

        match node.process_event_batch(events).await {
            Ok(stats) => {
                if batch_size > 1 {
                    debug!(
                        batch_size,
                        processed = stats.processed,
                        "Processed event batch"
                    );
                }
                Ok(stats.processed as u64)
            }
            Err(batch_err) => {
                // Fatal errors (NodeFatal, TransportDegraded) apply to the
                // entire node, not to any one event. Per-event DLQ fallback
                // would route every event in the batch — and every subsequent
                // batch — to DLQ while the node keeps consuming, generating
                // an unbounded log/IO storm. Issue #581 observed 221K
                // consecutive failures producing 1.6M journal entries and
                // 54 GB of NATS traffic on sinnix-prime before I/O saturation
                // halted the host.
                //
                // Use the new error_class() classification instead of
                // hardcoding individual variants. Checkpoint, Lifecycle,
                // Configuration, PermissionDenied, and live-context
                // ChannelSend are all NodeFatal.
                let error_class = batch_err.error_class();
                if error_class.is_fatal() {
                    error!(
                        error = %batch_err,
                        ?error_class,
                        batch_size,
                        "Fatal error in batch processing; halting node (per-event DLQ fallback would loop on every event)"
                    );
                    return Err(batch_err);
                }
                warn!(
                    error = %batch_err,
                    ?error_class,
                    batch_size,
                    "Batch processing failed; falling back to per-event processing with DLQ routing"
                );
                let node_name = node.node_name().to_string();
                let mut succeeded = 0u64;
                for event in events_backup {
                    match node.process_event_batch(vec![event.clone()]).await {
                        Ok(stats) => {
                            succeeded += stats.processed as u64;
                        }
                        Err(event_err) => {
                            // Same defense as the batch path — fatal errors
                            // are not data errors. Halt immediately.
                            if event_err.error_class().is_fatal() {
                                error!(
                                    error = %event_err,
                                    "Checkpoint error during per-event fallback; halting node"
                                );
                                return Err(event_err);
                            }
                            let event_id = event.id;
                            warn!(
                                error = %event_err,
                                ?event_id,
                                "Event processing failed; routing to DLQ"
                            );
                            if let Err(dlq_err) = transport
                                .send_to_processing_failure_queue(
                                    &event,
                                    &event_err.to_string(),
                                    &node_name,
                                )
                                .await
                            {
                                return Err(SinexError::processing(
                                    "failed to route failed automaton event to processing-failure stream",
                                )
                                .with_context("node", node_name.clone())
                                .with_context(
                                    "event_id",
                                    event_id.as_ref().map_or_else(
                                        || "missing".to_string(),
                                        std::string::ToString::to_string,
                                    ),
                                )
                                .with_context("source", event.source.as_str().to_string())
                                .with_context("event_type", event.event_type.as_str().to_string())
                                .with_context("processing_error", event_err.to_string())
                                .with_source(dlq_err));
                            }
                        }
                    }
                }
                let dlq_count = batch_size as u64 - succeeded;
                info!(succeeded, dlq_count, "Per-event fallback complete");
                // Count DLQ'd events as processed for checkpoint advancement
                Ok(batch_size as u64)
            }
        }
    }

    /// Save a checkpoint if `last_event_id` is `Some`. Returns the new revision
    /// on success, or `None` if there was nothing to save or the save failed.
    ///
    /// Tracks consecutive failures in `consecutive_failures`. Resets to 0 on success.
    /// Returns a hard error after 3 consecutive failures to prevent silent progress loss
    /// on crash+restart (which would cause duplicate event processing).
    #[cfg(feature = "messaging")]
    async fn try_save_checkpoint(
        checkpoint_manager: &CheckpointManager,
        checkpoint_state: &mut crate::checkpoint::CheckpointState,
        last_event_id: Option<Uuid>,
        processed_events: u64,
        consecutive_failures: &mut u32,
    ) -> NodeResult<Option<u64>> {
        let Some(eid) = last_event_id else {
            return Ok(None);
        };
        checkpoint_state.checkpoint = Checkpoint::Internal {
            event_id: eid,
            message_count: processed_events,
        };
        checkpoint_state.processed_count = processed_events;
        checkpoint_state.last_activity = sinex_primitives::temporal::Timestamp::now();
        match checkpoint_manager.save_checkpoint(checkpoint_state).await {
            Ok(revision) => {
                *consecutive_failures = 0;
                debug!(processed_events, revision, "Checkpoint saved");
                Ok(Some(revision))
            }
            Err(err) => {
                *consecutive_failures += 1;
                error!(
                    error = %err,
                    consecutive_failures = *consecutive_failures,
                    "Failed to save checkpoint; will retry next interval"
                );
                if *consecutive_failures >= 3 {
                    return Err(SinexError::checkpoint(format!(
                        "Checkpoint save failed {} consecutive times; halting to prevent \
                         silent progress loss on crash+restart",
                        *consecutive_failures
                    )));
                }
                Ok(None)
            }
        }
    }

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

    /// Graceful shutdown.
    ///
    /// Idempotent: safe to call multiple times or on a never-initialized runner.
    pub async fn shutdown(&mut self) -> NodeResult<()> {
        if matches!(self.lifecycle, RunnerLifecycle::ShutDown) {
            debug!("shutdown() called on already shut-down runner; no-op");
            return Ok(());
        }
        if matches!(self.lifecycle, RunnerLifecycle::Created) {
            debug!("shutdown() called on never-initialized runner; no-op");
            self.lifecycle = RunnerLifecycle::ShutDown;
            return Ok(());
        }

        info!("Shutting down stream node runner");

        let mut shutdown_errors = Vec::new();
        Self::push_shutdown_error(
            &mut shutdown_errors,
            "schema broadcast listener",
            Self::shutdown_task(
                &mut self.schema_listener_handle,
                self.schema_listener_shutdown.take(),
                "schema broadcast listener",
            )
            .await,
        );
        Self::push_shutdown_error(
            &mut shutdown_errors,
            "command listener",
            Self::shutdown_task(
                &mut self.command_listener_handle,
                self.command_listener_shutdown.take(),
                "command listener",
            )
            .await,
        );
        Self::push_shutdown_error(
            &mut shutdown_errors,
            "coordination",
            self.shutdown_leader_state().await,
        );
        Self::push_shutdown_error(
            &mut shutdown_errors,
            "automaton consumer",
            Self::shutdown_task(&mut self.consumer_handle, None, "automaton consumer").await,
        );
        // Save checkpoint BEFORE draining the event batcher. This ensures the
        // checkpoint reflects the last fully-processed position. Events still in
        // the batcher channel will be published during drain but are "ahead" of
        // the checkpoint — on restart they'll be re-processed (at-least-once).
        // The previous order (batcher first, then checkpoint) could silently drop
        // events if the batcher's 250ms grace period expired mid-flush.
        Self::push_shutdown_error(
            &mut shutdown_errors,
            "node shutdown",
            self.node.shutdown().await,
        );
        Self::push_shutdown_error(
            &mut shutdown_errors,
            "event batcher",
            self.shutdown_event_batcher().await,
        );
        Self::push_shutdown_error(
            &mut shutdown_errors,
            "checkpoint cleanup",
            Self::shutdown_task(
                &mut self.checkpoint_cleanup_handle,
                self.checkpoint_cleanup_shutdown.take(),
                "checkpoint cleanup",
            )
            .await,
        );

        match Self::collapse_shutdown_errors(shutdown_errors) {
            Ok(()) => {
                self.lifecycle = RunnerLifecycle::ShutDown;
                Ok(())
            }
            Err(error) => {
                self.lifecycle = RunnerLifecycle::ShutdownFailed;
                Err(error)
            }
        }
    }

    async fn shutdown_task(
        handle: &mut Option<tokio::task::JoinHandle<()>>,
        shutdown_tx: Option<watch::Sender<bool>>,
        name: &str,
    ) -> NodeResult<()> {
        if let Some(shutdown_tx) = shutdown_tx {
            Self::signal_watch_shutdown(shutdown_tx, name);
        }
        if let Some(mut h) = handle.take() {
            if let Ok(result) = tokio::time::timeout(TASK_SHUTDOWN_GRACE_PERIOD, &mut h).await {
                Self::shutdown_join_result(name, result)
            } else {
                debug!(
                    task = name,
                    grace_period_ms = TASK_SHUTDOWN_GRACE_PERIOD.as_millis(),
                    "Task did not exit within shutdown grace period; aborting"
                );
                h.abort();
                Self::shutdown_join_result(name, h.await)
            }
        } else {
            Ok(())
        }
    }

    async fn shutdown_leader_state(&mut self) -> NodeResult<()> {
        if let Some(state) = self.leader_state.take() {
            let mut shutdown_errors = Vec::new();
            Self::signal_shutdown_channel(state.heartbeat_shutdown, "coordination heartbeat");
            Self::push_shutdown_error(
                &mut shutdown_errors,
                "coordination heartbeat",
                Self::shutdown_join_result("coordination heartbeat", state.heartbeat_handle.await),
            );
            Self::push_shutdown_error(
                &mut shutdown_errors,
                "coordination leadership release",
                Self::leadership_release_result(
                    &state.instance_id,
                    state.kv_client.release_leadership(&state.instance_id).await,
                ),
            );
            Self::collapse_shutdown_errors(shutdown_errors)
        } else {
            Ok(())
        }
    }

    async fn shutdown_event_batcher(&mut self) -> NodeResult<()> {
        if let Some(shutdown_tx) = self.event_batcher_shutdown.take() {
            Self::signal_shutdown_channel(shutdown_tx, "event batcher");
        }
        if let Some(handle) = self.event_batcher_handle.take() {
            Self::event_batcher_shutdown_result(handle.await)
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests;
