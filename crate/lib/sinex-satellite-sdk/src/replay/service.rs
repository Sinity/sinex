use super::{
    MetricsSnapshot, ProgressTracker, ReplayController, ReplayMetrics, ReplayPhase, ReplayProgress,
};
use crate::stream_processor::{EventEmitter, ProcessorHandles, ProcessorRuntimeState};
use crate::SatelliteResult;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sinex_core::db::models::Event;
use sinex_core::db::{repositories::DbPoolExt, DbPool as PgPool};
use sinex_core::JsonValue;
use sinex_core::{EventSource, EventType};
use std::collections::{HashMap, HashSet};
use tracing::{info, warn};

/// Get epoch timestamp as a safe fallback
fn epoch_timestamp() -> DateTime<Utc> {
    DateTime::from_timestamp(0, 0).unwrap_or(DateTime::<Utc>::MIN_UTC)
}

/// Replay mode configuration
#[derive(Debug, Clone)]
pub enum ReplayMode {
    /// No replay, process live events only
    Live,
    /// Replay all events from start_time to end_time
    TimeRange {
        start_time: DateTime<Utc>,
        end_time: Option<DateTime<Utc>>,
    },
    /// Replay events from a specific source
    Source {
        source: String,
        start_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
    },
    /// Replay specific event types
    EventTypes {
        event_types: Vec<String>,
        start_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
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
    pub start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,

    /// Limit number of events
    pub limit: Option<u64>,

    /// Additional JSON filters for payload
    pub payload_filters: Option<HashMap<String, serde_json::Value>>,
}

/// Replay manager for processing historical events
pub struct ReplayService {
    handles: ProcessorHandles,
    mode: ReplayMode,
    batch_size: usize,
    controller: Option<ReplayController>,
    metrics: Option<ReplayMetrics>,
}

impl ReplayService {
    /// Create a new replay service from processor handles
    pub fn new(handles: ProcessorHandles, mode: ReplayMode) -> Self {
        Self {
            handles,
            mode,
            batch_size: 1000,
            controller: None,
            metrics: None,
        }
    }

    /// Create a replay service from a processor runtime snapshot
    pub fn from_runtime(runtime: &ProcessorRuntimeState, mode: ReplayMode) -> Self {
        Self::new(runtime.handles().clone(), mode)
    }

    /// Clone handles from an existing processor handle set
    pub fn from_handles(handles: &ProcessorHandles, mode: ReplayMode) -> Self {
        Self::new(handles.clone(), mode)
    }

    fn db_pool(&self) -> &PgPool {
        self.handles.db_pool()
    }

    /// Set batch size for replay processing
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Set replay controller for pause/resume/cancel functionality
    pub fn with_controller(mut self, controller: ReplayController) -> Self {
        self.controller = Some(controller);
        self
    }

    /// Get the replay controller (if set)
    pub fn controller(&self) -> Option<&ReplayController> {
        self.controller.as_ref()
    }

    /// Set metrics collector
    pub fn with_metrics(mut self, metrics: ReplayMetrics) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Get the metrics collector (if set)
    pub fn metrics(&self) -> Option<&ReplayMetrics> {
        self.metrics.as_ref()
    }

    /// Replay events into the provided event emitter using the current mode.
    pub async fn replay_into_emitter(
        &mut self,
        emitter: &EventEmitter,
        progress_callback: Option<impl Fn(&ReplayProgress) + Send + Sync + 'static>,
    ) -> SatelliteResult<ReplayResult> {
        let emitter = emitter.clone();
        self.replay_events_with_progress(
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
        .await
    }

    /// Check if replay mode is enabled
    pub fn is_replay_enabled(&self) -> bool {
        !matches!(self.mode, ReplayMode::Live)
    }

    /// Get replay statistics
    pub async fn get_replay_stats(&self) -> SatelliteResult<ReplayStats> {
        let total_events = match &self.mode {
            ReplayMode::Live => 0,
            ReplayMode::TimeRange {
                start_time,
                end_time,
            } => {
                let end_time = end_time.unwrap_or_else(Utc::now);

                self.db_pool()
                    .events()
                    .count_by_time_range(*start_time, end_time)
                    .await? as u64
            }
            ReplayMode::Source {
                source,
                start_time,
                end_time,
            } => {
                if start_time.is_none() && end_time.is_none() {
                    let event_source = EventSource::new(source);
                    self.db_pool()
                        .events()
                        .count_by_source(&event_source)
                        .await? as u64
                } else {
                    // Use a complex query for source with time range
                    let start_time = start_time.unwrap_or_else(epoch_timestamp);
                    let end_time = end_time.unwrap_or_else(Utc::now);

                    let event_source = EventSource::new(source);
                    self.db_pool()
                        .events()
                        .count_by_source_and_time_range(&event_source, start_time, end_time)
                        .await? as u64
                }
            }
            ReplayMode::EventTypes {
                event_types,
                start_time,
                end_time,
            } => {
                if event_types.len() == 1 && start_time.is_none() && end_time.is_none() {
                    let event_type = EventType::new(&event_types[0]);
                    self.db_pool()
                        .events()
                        .count_by_event_type(&event_type)
                        .await? as u64
                } else {
                    self.db_pool().events().count_all().await? as u64
                }
            }
            ReplayMode::Custom { .. } => self.db_pool().events().count_all().await? as u64,
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
        mut processor: F,
        progress_callback: Option<impl Fn(&ReplayProgress) + Send + Sync + 'static>,
    ) -> SatelliteResult<ReplayResult>
    where
        F: FnMut(Vec<Event<JsonValue>>) -> Fut,
        Fut: std::future::Future<Output = SatelliteResult<usize>>,
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

            let events: Vec<Event<JsonValue>> = match &self.mode {
                ReplayMode::TimeRange {
                    start_time,
                    end_time,
                } => {
                    let end_time = end_time.unwrap_or_else(Utc::now);

                    self.db_pool()
                        .events()
                        .get_by_time_range(
                            *start_time,
                            end_time,
                            Some(self.batch_size as i64),
                            Some(offset as i64),
                        )
                        .await?
                }
                ReplayMode::Source {
                    source,
                    start_time,
                    end_time,
                } => {
                    if start_time.is_none() && end_time.is_none() {
                        let event_source = EventSource::new(source);
                        self.db_pool()
                            .events()
                            .get_by_source(
                                &event_source,
                                Some(self.batch_size as i64),
                                Some(offset as i64),
                            )
                            .await?
                    } else {
                        let start_time = start_time.unwrap_or_else(epoch_timestamp);
                        let end_time = end_time.unwrap_or_else(Utc::now);

                        let event_source = EventSource::new(source);
                        self.db_pool()
                            .events()
                            .get_by_source_and_time_range(
                                &event_source,
                                start_time,
                                end_time,
                                Some(self.batch_size as i64),
                                Some(offset as i64),
                            )
                            .await?
                    }
                }
                ReplayMode::EventTypes {
                    event_types,
                    start_time,
                    end_time,
                } => {
                    if event_types.len() == 1 && start_time.is_none() && end_time.is_none() {
                        let event_type = EventType::new(&event_types[0]);
                        self.db_pool()
                            .events()
                            .get_by_event_type(
                                &event_type,
                                Some(self.batch_size as i64),
                                Some(offset as i64),
                            )
                            .await?
                    } else {
                        let start = start_time.unwrap_or_else(epoch_timestamp);
                        let end = end_time.unwrap_or_else(Utc::now);
                        let limit = (offset + self.batch_size) as i64;
                        let events = self
                            .db_pool()
                            .events()
                            .get_by_time_range(start, end, Some(limit), None)
                            .await?;
                        let allowed: HashSet<&str> =
                            event_types.iter().map(|s| s.as_str()).collect();
                        events
                            .into_iter()
                            .filter(|event| allowed.contains(event.event_type.as_str()))
                            .skip(offset)
                            .take(self.batch_size)
                            .collect()
                    }
                }
                ReplayMode::Custom { filters } => {
                    let start = filters.start_time.unwrap_or_else(epoch_timestamp);
                    let end = filters.end_time.unwrap_or_else(Utc::now);
                    let limit = (offset + self.batch_size) as i64;
                    let events = self
                        .db_pool()
                        .events()
                        .get_by_time_range(start, end, Some(limit), None)
                        .await?;

                    let source_filter: Option<HashSet<&str>> = filters
                        .sources
                        .as_ref()
                        .map(|items| items.iter().map(|s| s.as_str()).collect());
                    let type_filter: Option<HashSet<&str>> = filters
                        .event_types
                        .as_ref()
                        .map(|items| items.iter().map(|s| s.as_str()).collect());
                    let host_filter: Option<HashSet<&str>> = filters
                        .hosts
                        .as_ref()
                        .map(|items| items.iter().map(|s| s.as_str()).collect());

                    events
                        .into_iter()
                        .filter(|event| {
                            if let Some(sources) = &source_filter {
                                if !sources.contains(event.source.as_str()) {
                                    return false;
                                }
                            }

                            if let Some(types) = &type_filter {
                                if !types.contains(event.event_type.as_str()) {
                                    return false;
                                }
                            }

                            if let Some(hosts) = &host_filter {
                                if !hosts.contains(event.host.as_str()) {
                                    return false;
                                }
                            }

                            true
                        })
                        .skip(offset)
                        .take(self.batch_size)
                        .collect()
                }
                ReplayMode::Live => unreachable!(),
            };

            if events.is_empty() {
                info!("Replay complete: no more events to process");
                break;
            }

            let batch_size = events.len();
            match processor(events).await {
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

        let metrics = self.metrics.as_ref().map(|m| m.snapshot());

        Ok(ReplayResult {
            total_processed,
            total_batches,
            errors,
            metrics,
            aggregated_data: None,
        })
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
