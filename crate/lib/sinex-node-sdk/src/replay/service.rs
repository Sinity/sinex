use super::{
    MetricsSnapshot, ProgressTracker, ReplayController, ReplayMetrics, ReplayPhase, ReplayProgress,
};
use crate::event_node::{EventBatcherConfig, spawn_event_batcher};
use crate::runtime::stream::{EventEmitter, NodeHandles, NodeRuntimeState};
use crate::{NodeResult, SinexError};
use serde::{Deserialize, Serialize};
use sinex_db::{DbPool as PgPool, repositories::DbPoolExt};
use sinex_primitives::events::Event;
const DEFAULT_EVENT_CHANNEL_SIZE: usize = 1024;
use sinex_primitives::JsonValue;
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::temporal::Timestamp;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tracing::{info, warn};

/// Get the Unix epoch timestamp.
fn epoch_timestamp() -> Timestamp {
    Timestamp::UNIX_EPOCH
}

/// Replay mode configuration
#[derive(Debug, Clone)]
pub enum ReplayMode {
    /// No replay, process live events only
    Live,
    /// Replay all events from `start_time` to `end_time`
    TimeRange {
        start_time: Timestamp,
        end_time: Option<Timestamp>,
    },
    /// Replay events from a specific source
    Source {
        source: String,
        start_time: Option<Timestamp>,
        end_time: Option<Timestamp>,
    },
    /// Replay specific event types
    EventTypes {
        event_types: Vec<String>,
        start_time: Option<Timestamp>,
        end_time: Option<Timestamp>,
    },
    /// Custom replay with flexible filters
    Custom { filters: ReplayFilters },
}

/// Flexible filters for custom replay
#[derive(Debug, Clone, Serialize, Deserialize, bon::Builder)]
pub struct ReplayFilters {
    /// Source patterns (supports wildcards)
    pub sources: Option<Vec<String>>,

    /// Event type patterns (supports wildcards)
    pub event_types: Option<Vec<String>>,

    /// Host patterns (supports wildcards)
    pub hosts: Option<Vec<String>>,

    /// Time range
    pub start_time: Option<Timestamp>,
    pub end_time: Option<Timestamp>,

    /// Limit number of events
    pub limit: Option<u64>,

    /// Additional JSON filters for payload
    pub payload_filters: Option<HashMap<String, serde_json::Value>>,
}

/// Replay manager for processing historical events
pub struct ReplayService {
    handles: NodeHandles,
    mode: ReplayMode,
    batch_size: usize,
    controller: Option<ReplayController>,
    metrics: Option<ReplayMetrics>,
    replay_namespace: Option<String>,
}

impl ReplayService {
    /// Create a new replay service from node handles
    #[must_use]
    pub fn new(handles: NodeHandles, mode: ReplayMode) -> Self {
        Self {
            handles,
            mode,
            batch_size: 1000,
            controller: None,
            metrics: None,
            replay_namespace: replay_namespace_from_env(),
        }
    }

    /// Create a replay service from a node runtime snapshot
    #[must_use]
    pub fn from_runtime(runtime: &NodeRuntimeState, mode: ReplayMode) -> Self {
        Self::new(runtime.handles().clone(), mode)
    }

    /// Clone handles from an existing node handle set
    #[must_use]
    pub fn from_handles(handles: &NodeHandles, mode: ReplayMode) -> Self {
        Self::new(handles.clone(), mode)
    }

    fn db_pool(&self) -> &PgPool {
        self.handles.require_db_pool()
    }

    /// Set batch size for replay processing
    #[must_use]
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Set replay controller for pause/resume/cancel functionality
    #[must_use]
    pub fn with_controller(mut self, controller: ReplayController) -> Self {
        self.controller = Some(controller);
        self
    }

    /// Get the replay controller (if set)
    #[must_use]
    pub fn controller(&self) -> Option<&ReplayController> {
        self.controller.as_ref()
    }

    /// Set metrics collector
    #[must_use]
    pub fn with_metrics(mut self, metrics: ReplayMetrics) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Get the metrics collector (if set)
    #[must_use]
    pub fn metrics(&self) -> Option<&ReplayMetrics> {
        self.metrics.as_ref()
    }

    /// Override the namespace used for replay traffic when publishing to NATS.
    pub fn with_replay_namespace(mut self, namespace: impl Into<Option<String>>) -> Self {
        self.replay_namespace = namespace.into();
        self
    }

    /// Replay events into the provided event emitter using the current mode.
    pub async fn replay_into_emitter(
        &mut self,
        emitter: &EventEmitter,
        progress_callback: Option<impl Fn(&ReplayProgress) + Send + Sync + 'static>,
    ) -> NodeResult<ReplayResult> {
        let replay_handle = self.build_replay_emitter()?;
        let emitter = replay_handle
            .as_ref()
            .map_or_else(|| emitter.clone(), |handle| handle.emitter.clone());

        let result = self
            .replay_events_with_progress(
                move |events| {
                    let emitter = emitter.clone();
                    async move {
                        let mut processed = 0;
                        for event in events {
                            emitter.emit(event).await?;
                            processed += 1;
                        }
                        Ok(processed)
                    }
                },
                progress_callback,
            )
            .await;

        if let Some(handle) = replay_handle
            && let Err(err) = handle.finish().await
        {
            warn!(error = %err, "Replay transport shutdown failed");
            if result.is_ok() {
                return Err(err);
            }
        }

        result
    }

    /// Check if replay mode is enabled
    #[must_use]
    pub fn is_replay_enabled(&self) -> bool {
        !matches!(self.mode, ReplayMode::Live)
    }

    /// Get replay statistics
    pub async fn get_replay_stats(&self) -> NodeResult<ReplayStats> {
        let total_events = match &self.mode {
            ReplayMode::Live => 0,
            ReplayMode::TimeRange {
                start_time,
                end_time,
            } => {
                let end_time = end_time.unwrap_or_else(Timestamp::now);

                self.db_pool()
                    .events()
                    .estimate_count_by_time_range(*start_time, end_time)
                    .await? as u64
            }
            ReplayMode::Source {
                source,
                start_time,
                end_time,
            } => {
                if start_time.is_none() && end_time.is_none() {
                    let event_source = EventSource::new(source)?;
                    self.db_pool()
                        .events()
                        .estimate_count_by_source(&event_source)
                        .await? as u64
                } else {
                    // Use a complex query for source with time range
                    let start_time = start_time.unwrap_or_else(epoch_timestamp);
                    let end_time = end_time.unwrap_or_else(Timestamp::now);

                    let event_source = EventSource::new(source)?;
                    self.db_pool()
                        .events()
                        .estimate_count_by_source_and_time_range(
                            &event_source,
                            start_time,
                            end_time,
                        )
                        .await? as u64
                }
            }
            ReplayMode::EventTypes {
                event_types,
                start_time,
                end_time,
            } => {
                if event_types.len() == 1 && start_time.is_none() && end_time.is_none() {
                    let event_type = EventType::new(&event_types[0])?;
                    self.db_pool()
                        .events()
                        .estimate_count_by_event_type(&event_type)
                        .await? as u64
                } else {
                    self.db_pool().events().count_all_estimate().await? as u64
                }
            }
            ReplayMode::Custom { .. } => self.db_pool().events().count_all_estimate().await? as u64,
        };

        Ok(ReplayStats {
            total_events: total_events as u64,
            batch_size: self.batch_size,
            estimated_batches: (total_events as usize).div_ceil(self.batch_size),
        })
    }

    /// Process events in replay mode with progress tracking
    pub async fn replay_events_with_progress<F, Fut>(
        &mut self,
        mut handler: F,
        progress_callback: Option<impl Fn(&ReplayProgress) + Send + Sync + 'static>,
    ) -> NodeResult<ReplayResult>
    where
        F: FnMut(Vec<Event<JsonValue>>) -> Fut,
        Fut: std::future::Future<Output = NodeResult<usize>>,
    {
        if matches!(self.mode, ReplayMode::Live) {
            return Ok(ReplayResult {
                total_processed: 0,
                total_batches: 0,
                errors: Vec::new(),
                metrics: None,
                aggregated_data: None,
            });
        }

        info!("Starting replay mode processing with progress tracking");

        if let Some(ref mut metrics) = self.metrics {
            metrics.start();
        }

        let stats = self.get_replay_stats().await?;
        info!(
            total_events = stats.total_events,
            estimated_batches = stats.estimated_batches,
            batch_size = stats.batch_size,
            "Replay statistics"
        );

        let mut tracker = ProgressTracker::new(stats.total_events, stats.estimated_batches);
        if let Some(callback) = progress_callback {
            tracker = tracker.with_callback(callback);
        }

        tracker.set_phase(ReplayPhase::Initializing).await;

        let mut total_processed = 0;
        let mut total_batches: usize = 0;
        let mut errors = Vec::new();
        let mut offset = 0;

        tracker.set_phase(ReplayPhase::Processing).await;

        loop {
            if let Some(ref controller) = self.controller {
                controller.wait_if_paused().await?;
                controller.check_cancelled()?;
            }

            let events = self.fetch_batch_for_mode(offset).await?;

            if events.is_empty() {
                info!("Replay complete: no more events to process");
                break;
            }

            let batch_size = events.len();
            match handler(events).await {
                Ok(processed) => {
                    total_processed += processed;
                    total_batches += 1;
                    if let Some(ref mut metrics) = self.metrics {
                        metrics.record_batch(processed as u64);
                    }
                }
                Err(err) => {
                    warn!(error = %err, "Replay batch failed");
                    errors.push(err.to_string());
                }
            }

            offset += batch_size;
            tracker.update(batch_size as u64).await;
        }

        tracker.set_phase(ReplayPhase::Completed).await;

        let metrics = self
            .metrics
            .as_ref()
            .map(super::metrics::ReplayMetrics::snapshot);

        Ok(ReplayResult {
            total_processed,
            total_batches,
            errors,
            metrics,
            aggregated_data: None,
        })
    }

    /// Fetch a batch of events according to the current replay mode.
    async fn fetch_batch_for_mode(&self, offset: usize) -> NodeResult<Vec<Event<JsonValue>>> {
        let page =
            sinex_primitives::Pagination::new(Some(self.batch_size as i64), Some(offset as i64));

        match &self.mode {
            ReplayMode::TimeRange {
                start_time,
                end_time,
            } => {
                let end = end_time.unwrap_or_else(Timestamp::now);
                self.db_pool()
                    .events()
                    .get_by_time_range(*start_time, end, page)
                    .await
            }
            ReplayMode::Source {
                source,
                start_time,
                end_time,
            } => {
                let event_source = EventSource::new(source)?;
                if start_time.is_none() && end_time.is_none() {
                    self.db_pool()
                        .events()
                        .get_by_source(&event_source, page)
                        .await
                } else {
                    let start = start_time.unwrap_or_else(epoch_timestamp);
                    let end = end_time.unwrap_or_else(Timestamp::now);
                    self.db_pool()
                        .events()
                        .get_by_source_and_time_range(&event_source, start, end, page)
                        .await
                }
            }
            ReplayMode::EventTypes {
                event_types,
                start_time,
                end_time,
            } => {
                if event_types.len() == 1 && start_time.is_none() && end_time.is_none() {
                    let event_type = EventType::new(&event_types[0])?;
                    return self
                        .db_pool()
                        .events()
                        .get_by_event_type(&event_type, page)
                        .await;
                }
                let start = start_time.unwrap_or_else(epoch_timestamp);
                let end = end_time.unwrap_or_else(Timestamp::now);
                let limit = (offset + self.batch_size) as i64;
                let events = self
                    .db_pool()
                    .events()
                    .get_by_time_range(
                        start,
                        end,
                        sinex_primitives::Pagination::new(Some(limit), None),
                    )
                    .await?;
                let allowed: HashSet<&str> = event_types
                    .iter()
                    .map(std::string::String::as_str)
                    .collect();
                Ok(events
                    .into_iter()
                    .filter(|event| allowed.contains(event.event_type.as_str()))
                    .skip(offset)
                    .take(self.batch_size)
                    .collect())
            }
            ReplayMode::Custom { filters } => {
                let start = filters.start_time.unwrap_or_else(epoch_timestamp);
                let end = filters.end_time.unwrap_or_else(Timestamp::now);
                let limit = (offset + self.batch_size) as i64;
                let events = self
                    .db_pool()
                    .events()
                    .get_by_time_range(
                        start,
                        end,
                        sinex_primitives::Pagination::new(Some(limit), None),
                    )
                    .await?;
                Ok(apply_custom_filters(
                    events,
                    filters,
                    offset,
                    self.batch_size,
                ))
            }
            ReplayMode::Live => unreachable!(),
        }
    }
}

/// Apply custom replay filters to a pre-fetched event list.
fn apply_custom_filters(
    events: Vec<Event<JsonValue>>,
    filters: &ReplayFilters,
    offset: usize,
    batch_size: usize,
) -> Vec<Event<JsonValue>> {
    let source_filter: Option<HashSet<&str>> = filters
        .sources
        .as_ref()
        .map(|items| items.iter().map(std::string::String::as_str).collect());
    let type_filter: Option<HashSet<&str>> = filters
        .event_types
        .as_ref()
        .map(|items| items.iter().map(std::string::String::as_str).collect());
    let host_filter: Option<HashSet<&str>> = filters
        .hosts
        .as_ref()
        .map(|items| items.iter().map(std::string::String::as_str).collect());

    events
        .into_iter()
        .filter(|event| {
            source_filter
                .as_ref()
                .is_none_or(|s| s.contains(event.source.as_str()))
                && type_filter
                    .as_ref()
                    .is_none_or(|t| t.contains(event.event_type.as_str()))
                && host_filter
                    .as_ref()
                    .is_none_or(|h| h.contains(event.host.as_str()))
        })
        .skip(offset)
        .take(batch_size)
        .collect()
}

fn replay_namespace_from_env() -> Option<String> {
    std::env::var("SINEX_REPLAY_NAMESPACE")
        .ok()
        .and_then(|raw| {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        })
}

struct ReplayEmitterHandle {
    emitter: EventEmitter,
    shutdown_tx: Option<oneshot::Sender<()>>,
    join: JoinHandle<NodeResult<()>>,
}

impl ReplayEmitterHandle {
    async fn finish(mut self) -> NodeResult<()> {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        match self.join.await {
            Ok(result) => result,
            Err(err) => Err(SinexError::processing(format!(
                "Replay event batcher join failed: {err}"
            ))),
        }
    }
}

impl ReplayService {
    fn build_replay_emitter(&self) -> NodeResult<Option<ReplayEmitterHandle>> {
        let Some(namespace) = self.replay_namespace.as_ref() else {
            return Ok(None);
        };

        let transport = match self.handles.transport() {
            crate::event_node::EventTransport::Nats(publisher) => {
                let client = publisher.nats_client().clone();
                crate::event_node::EventTransport::Nats(Arc::new(
                    crate::NatsPublisher::with_namespace(client, Some(namespace.clone())),
                ))
            }
        };

        let (sender, receiver) = mpsc::channel(DEFAULT_EVENT_CHANNEL_SIZE);
        let emitter = EventEmitter::new(sender, self.handles.emitter().dry_run());
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let join = spawn_event_batcher(
            transport,
            EventBatcherConfig::default(),
            receiver,
            shutdown_rx,
        );

        Ok(Some(ReplayEmitterHandle {
            emitter,
            shutdown_tx: Some(shutdown_tx),
            join,
        }))
    }
}

/// Summary of replay execution
#[derive(Debug, Clone, Serialize)]
pub struct ReplayResult {
    pub total_processed: usize,
    pub total_batches: usize,
    pub errors: Vec<String>,
    pub metrics: Option<MetricsSnapshot>,
    pub aggregated_data: Option<serde_json::Value>,
}

/// Replay statistics prior to execution
#[derive(Debug, Clone, Serialize)]
pub struct ReplayStats {
    pub total_events: u64,
    pub batch_size: usize,
    pub estimated_batches: usize,
}
