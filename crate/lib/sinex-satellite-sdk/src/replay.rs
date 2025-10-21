//! Replay mode for historical event processing

use crate::replay_control::ReplayController;
use crate::replay_metrics::ReplayMetrics;
use crate::replay_progress::{ProgressTracker, ReplayPhase};
use crate::SatelliteResult;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sinex_core::db::models::Event;
use sinex_core::db::{repositories::DbPoolExt, DbPool as PgPool};
use sinex_core::JsonValue;
use sinex_core::{EventSource, EventType};
use std::collections::HashMap;
use tracing::{debug, info, warn};

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
pub struct ReplayManager {
    pool: PgPool,
    mode: ReplayMode,
    batch_size: usize,
    controller: Option<ReplayController>,
    metrics: Option<ReplayMetrics>,
}

impl ReplayManager {
    /// Create a new replay manager
    pub fn new(pool: PgPool, mode: ReplayMode) -> Self {
        Self {
            pool,
            mode,
            batch_size: 1000,
            controller: None,
            metrics: None,
        }
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

                self.pool
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
                    self.pool.events().count_by_source(&event_source).await? as u64
                } else {
                    // Use a complex query for source with time range
                    let start_time = start_time.unwrap_or_else(epoch_timestamp);
                    let end_time = end_time.unwrap_or_else(Utc::now);

                    let event_source = EventSource::new(source);
                    self.pool
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
                    self.pool.events().count_by_event_type(&event_type).await? as u64
                } else {
                    // For complex event type queries, fall back to count all (simplified)

                    self.pool.events().count_all().await? as u64
                }
            }
            ReplayMode::Custom { .. } => {
                // For custom filters, use count all as approximation

                self.pool.events().count_all().await? as u64
            }
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
        progress_callback: Option<
            impl Fn(&crate::replay_progress::ReplayProgress) + Send + Sync + 'static,
        >,
    ) -> SatelliteResult<ReplayResult>
    where
        F: FnMut(Vec<Event<JsonValue>>) -> Fut,
        Fut: std::future::Future<Output = SatelliteResult<usize>>,
    {
        if matches!(self.mode, ReplayMode::Live) {
            return Ok(ReplayResult {
                total_processed: 0,
                total_batches: 0,
                errors: vec![],
                metrics: None,
                aggregated_data: None,
            });
        }

        info!("Starting replay mode processing with progress tracking");

        // Start metrics collection if enabled
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

        // Create progress tracker
        let mut tracker = ProgressTracker::new(stats.total_events, stats.estimated_batches);
        if let Some(callback) = progress_callback {
            tracker = tracker.with_callback(callback);
        }

        // Initialize phase
        tracker.set_phase(ReplayPhase::Initializing).await;

        let mut total_processed = 0;
        let mut total_batches: usize = 0;
        let mut errors = Vec::new();
        let mut offset = 0;

        // Start processing phase
        tracker.set_phase(ReplayPhase::Processing).await;

        loop {
            // Check for pause/cancel before fetching next batch
            if let Some(ref controller) = self.controller {
                controller.wait_if_paused().await?;
                controller.check_cancelled()?;
            }

            // Fetch events using the query system based on mode
            let events: Vec<Event<JsonValue>> = match &self.mode {
                ReplayMode::TimeRange {
                    start_time,
                    end_time,
                } => {
                    let end_time = end_time.unwrap_or_else(Utc::now);

                    // Use the existing time range query method
                    self.pool
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
                        self.pool
                            .events()
                            .get_by_source(
                                &event_source,
                                Some(self.batch_size as i64),
                                Some(offset as i64),
                            )
                            .await?
                    } else {
                        // For source with time range, use time range query and filter
                        let start_time = start_time.unwrap_or_else(epoch_timestamp);
                        let end_time = end_time.unwrap_or_else(Utc::now);

                        // Use the new source + time range query method
                        let event_source = EventSource::new(source);
                        self.pool
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
                        self.pool
                            .events()
                            .get_by_event_type(
                                &event_type,
                                Some(self.batch_size as i64),
                                Some(offset as i64),
                            )
                            .await?
                    } else {
                        // For complex queries, use get_recent and filter

                        // get_recent doesn't support offset, use search instead
                        use sinex_core::EventSearchFilters;
                        self.pool
                            .events()
                            .search(EventSearchFilters {
                                limit: Some(self.batch_size as u64),
                                offset: Some(offset as u64),
                                ..Default::default()
                            })
                            .await?
                            .into_iter()
                            .filter(|event: &Event<JsonValue>| {
                                let type_matches =
                                    event_types.iter().any(|t| t == event.event_type.as_str());
                                let start_matches = start_time.is_none_or(|start| {
                                    event.ts_orig.as_ref().is_some_and(|ts| *ts >= start)
                                });
                                let end_matches = end_time.is_none_or(|end| {
                                    event.ts_orig.as_ref().is_some_and(|ts| *ts <= end)
                                });
                                type_matches && start_matches && end_matches
                            })
                            .collect()
                    }
                }
                ReplayMode::Custom { filters } => {
                    // Use get_recent as base query and apply filters

                    // get_recent doesn't support offset, use search instead
                    use sinex_core::EventSearchFilters;
                    self.pool
                        .events()
                        .search(EventSearchFilters {
                            limit: Some(self.batch_size as u64),
                            offset: Some(offset as u64),
                            ..Default::default()
                        })
                        .await?
                        .into_iter()
                        .filter(|event| self.apply_custom_filters(event, filters))
                        .collect()
                }
                ReplayMode::Live => {
                    // Live mode means no historical replay - return empty to exit loop
                    Vec::new()
                }
            };

            if events.is_empty() {
                break;
            }

            let batch_size = events.len();
            let batch_start = std::time::Instant::now();

            // Calculate batch size in bytes (approximate)
            let batch_bytes = events
                .iter()
                .map(|e| e.payload.to_string().len() as u64 + 100) // +100 for metadata overhead
                .sum::<u64>();

            {
                // Process the batch
                match processor(events).await {
                    Ok(processed) => {
                        total_processed += processed;
                        debug!(
                            batch = total_batches + 1,
                            processed = processed,
                            total = total_processed,
                            "Processed replay batch"
                        );

                        // Update progress tracker
                        tracker.increment_processed(processed as u64).await;

                        // Record metrics
                        if let Some(ref metrics) = self.metrics {
                            metrics.record_batch(
                                processed as u64,
                                batch_start.elapsed(),
                                batch_bytes,
                            );
                        }
                    }
                    Err(e) => {
                        warn!(
                            batch = total_batches + 1,
                            error = %e,
                            "Failed to process replay batch"
                        );
                        errors.push(format!("Batch {} error: {}", total_batches + 1, e));

                        // Update failed count
                        tracker.increment_failed(batch_size as u64).await;

                        // Record failure metrics
                        if let Some(ref metrics) = self.metrics {
                            metrics.record_failures(batch_size as u64);
                            metrics.record_error("batch_processing");
                        }
                    }
                }
            }

            total_batches += 1;
            offset += self.batch_size;

            // Update batch completion in tracker
            tracker.complete_batch().await;

            // Save checkpoint periodically (every 10 batches)
            if total_batches != 0 && total_batches % 10 == 0 {
                let last_event_id = None; // Would need to extract from events
                tracker
                    .save_checkpoint(
                        last_event_id,
                        offset as u64,
                        serde_json::json!({
                            "mode": format!("{:?}", self.mode),
                            "batch_size": self.batch_size,
                        }),
                    )
                    .await;
            }
        }

        // Set completion phase
        tracker.set_phase(ReplayPhase::Completed).await;

        info!(
            total_processed = total_processed,
            total_batches = total_batches,
            errors = errors.len(),
            "Replay processing completed"
        );

        // Get final summary
        let summary = tracker.get_summary().await;
        info!("{}", summary.format_report());

        // Get final metrics report
        if let Some(ref metrics) = self.metrics {
            let snapshot = metrics.snapshot();
            info!("Metrics report:\n{}", snapshot.format_report());
        }

        Ok(ReplayResult {
            total_processed,
            total_batches,
            errors,
            metrics: self.metrics.as_ref().map(|m| m.snapshot()),
            aggregated_data: None, // Would need to be collected during processing
        })
    }

    /// Process events in replay mode (backwards compatibility)
    pub async fn replay_events<F, Fut>(&mut self, processor: F) -> SatelliteResult<ReplayResult>
    where
        F: FnMut(Vec<Event<JsonValue>>) -> Fut,
        Fut: std::future::Future<Output = SatelliteResult<usize>>,
    {
        self.replay_events_with_progress(
            processor,
            None::<fn(&crate::replay_progress::ReplayProgress)>,
        )
        .await
    }

    /// Apply custom filters to an event
    fn apply_custom_filters(&self, event: &Event<JsonValue>, filters: &ReplayFilters) -> bool {
        // Check source patterns (simple wildcard matching)
        if let Some(sources) = &filters.sources {
            let source_matches = sources.iter().any(|pattern| {
                if pattern.contains('*') {
                    // Simple wildcard matching - just check prefix/suffix
                    if pattern.starts_with('*') && pattern.ends_with('*') {
                        let middle = &pattern[1..pattern.len() - 1];
                        event.source.contains(middle)
                    } else if let Some(suffix) = pattern.strip_prefix('*') {
                        event.source.ends_with(suffix)
                    } else if let Some(prefix) = pattern.strip_suffix('*') {
                        event.source.starts_with(prefix)
                    } else {
                        event.source.as_str() == pattern
                    }
                } else {
                    event.source.as_str() == pattern
                }
            });
            if !source_matches {
                return false;
            }
        }

        // Check event type patterns (simple wildcard matching)
        if let Some(event_types) = &filters.event_types {
            let type_matches = event_types.iter().any(|pattern| {
                if pattern.contains('*') {
                    // Simple wildcard matching - just check prefix/suffix
                    if pattern.starts_with('*') && pattern.ends_with('*') {
                        let middle = &pattern[1..pattern.len() - 1];
                        event.event_type.contains(middle)
                    } else if let Some(suffix) = pattern.strip_prefix('*') {
                        event.event_type.ends_with(suffix)
                    } else if let Some(prefix) = pattern.strip_suffix('*') {
                        event.event_type.starts_with(prefix)
                    } else {
                        event.event_type.as_str() == pattern
                    }
                } else {
                    event.event_type.as_str() == pattern
                }
            });
            if !type_matches {
                return false;
            }
        }

        // Check host patterns (simple wildcard matching)
        if let Some(hosts) = &filters.hosts {
            let host_matches = hosts.iter().any(|pattern| {
                if pattern.contains('*') {
                    // Simple wildcard matching - just check prefix/suffix
                    if pattern.starts_with('*') && pattern.ends_with('*') {
                        let middle = &pattern[1..pattern.len() - 1];
                        event.host.contains(middle)
                    } else if let Some(suffix) = pattern.strip_prefix('*') {
                        event.host.ends_with(suffix)
                    } else if let Some(prefix) = pattern.strip_suffix('*') {
                        event.host.starts_with(prefix)
                    } else {
                        event.host.as_str() == pattern
                    }
                } else {
                    event.host.as_str() == pattern
                }
            });
            if !host_matches {
                return false;
            }
        }

        // Check time range
        if let Some(start_time) = filters.start_time {
            if event.ts_orig.as_ref().is_some_and(|ts| *ts < start_time) {
                return false;
            }
        }

        if let Some(end_time) = filters.end_time {
            if event.ts_orig.as_ref().is_some_and(|ts| *ts > end_time) {
                return false;
            }
        }

        // Apply payload filters
        if let Some(payload_filters) = &filters.payload_filters {
            // Check each filter against the event payload
            for (key, expected_value) in payload_filters {
                // Use JSON pointer syntax for nested field access (e.g., "/field/nested")
                let actual_value = if key.starts_with('/') {
                    // JSON pointer style access
                    event.payload.pointer(key)
                } else {
                    // Direct field access
                    event.payload.get(key)
                };

                match actual_value {
                    Some(actual) => {
                        // Check if values match (handles different JSON value types)
                        if !json_values_match(actual, expected_value) {
                            return false;
                        }
                    }
                    None => {
                        // If field doesn't exist and we're not checking for null, filter out
                        if !expected_value.is_null() {
                            return false;
                        }
                    }
                }
            }
        }

        true
    }
}

/// Helper function to compare JSON values with type coercion
fn json_values_match(actual: &serde_json::Value, expected: &serde_json::Value) -> bool {
    use serde_json::Value;

    match (actual, expected) {
        // Exact matches
        (a, e) if a == e => true,

        // String contains matching for partial searches
        (Value::String(a), Value::String(e)) if e.starts_with('*') && e.ends_with('*') => {
            let pattern = &e[1..e.len() - 1];
            a.contains(pattern)
        }
        (Value::String(a), Value::String(e)) if e.starts_with('*') => {
            let suffix = &e[1..];
            a.ends_with(suffix)
        }
        (Value::String(a), Value::String(e)) if e.ends_with('*') => {
            let prefix = &e[..e.len() - 1];
            a.starts_with(prefix)
        }

        // Number comparisons (handle int/float differences)
        (Value::Number(a), Value::Number(e)) => {
            // Try to compare as floats for compatibility
            match (a.as_f64(), e.as_f64()) {
                (Some(av), Some(ev)) => (av - ev).abs() < f64::EPSILON,
                _ => false,
            }
        }

        // Array contains check
        (Value::Array(arr), single) => arr.iter().any(|item| json_values_match(item, single)),

        // Default to false for type mismatches
        _ => false,
    }
}

/// Replay statistics
#[derive(Debug, Clone)]
pub struct ReplayStats {
    pub total_events: u64,
    pub batch_size: usize,
    pub estimated_batches: usize,
}

/// Replay processing result with aggregation support
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayResult {
    pub total_processed: usize,
    pub total_batches: usize,
    pub errors: Vec<String>,
    pub metrics: Option<crate::replay_metrics::MetricsSnapshot>,
    pub aggregated_data: Option<AggregatedResults>,
}

impl ReplayResult {
    /// Check if replay was successful
    pub fn is_success(&self) -> bool {
        self.errors.is_empty()
    }

    /// Get error count
    pub fn error_count(&self) -> usize {
        self.errors.len()
    }

    /// Merge with another replay result (for aggregation)
    pub fn merge(&mut self, other: ReplayResult) {
        self.total_processed += other.total_processed;
        self.total_batches += other.total_batches;
        self.errors.extend(other.errors);

        // Merge aggregated data if present
        if let Some(other_data) = other.aggregated_data {
            if let Some(ref mut our_data) = self.aggregated_data {
                our_data.merge(other_data);
            } else {
                self.aggregated_data = Some(other_data);
            }
        }
    }

    /// Create empty result
    pub fn empty() -> Self {
        Self {
            total_processed: 0,
            total_batches: 0,
            errors: vec![],
            metrics: None,
            aggregated_data: None,
        }
    }
}

/// Aggregated results from replay processing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregatedResults {
    /// Count of events by type
    pub events_by_type: HashMap<String, usize>,

    /// Count of events by source
    pub events_by_source: HashMap<String, usize>,

    /// Count of events by hour
    pub events_by_hour: HashMap<String, usize>,

    /// Unique hosts encountered
    pub unique_hosts: std::collections::HashSet<String>,

    /// Processing statistics
    pub processing_stats: ProcessingStats,
}

impl AggregatedResults {
    /// Create new aggregated results
    pub fn new() -> Self {
        Self {
            events_by_type: HashMap::new(),
            events_by_source: HashMap::new(),
            events_by_hour: HashMap::new(),
            unique_hosts: std::collections::HashSet::new(),
            processing_stats: ProcessingStats::default(),
        }
    }

    /// Add event to aggregation
    pub fn add_event(&mut self, event: &Event<JsonValue>) {
        // Count by type
        *self
            .events_by_type
            .entry(event.event_type.to_string())
            .or_insert(0) += 1;

        // Count by source
        *self
            .events_by_source
            .entry(event.source.to_string())
            .or_insert(0) += 1;

        // Count by hour
        if let Some(ts) = event.ts_orig {
            let hour_key = ts.format("%Y-%m-%d %H:00").to_string();
            *self.events_by_hour.entry(hour_key).or_insert(0) += 1;
        }

        // Track unique hosts
        self.unique_hosts.insert(event.host.to_string());

        // Update stats
        self.processing_stats.event_count += 1;
        let payload_size = event.payload.to_string().len();
        self.processing_stats.total_bytes += payload_size;
        self.processing_stats.max_payload_size =
            self.processing_stats.max_payload_size.max(payload_size);
        self.processing_stats.min_payload_size =
            self.processing_stats.min_payload_size.min(payload_size);
    }

    /// Merge with another aggregated result
    pub fn merge(&mut self, other: AggregatedResults) {
        // Merge event counts
        for (k, v) in other.events_by_type {
            *self.events_by_type.entry(k).or_insert(0) += v;
        }

        for (k, v) in other.events_by_source {
            *self.events_by_source.entry(k).or_insert(0) += v;
        }

        for (k, v) in other.events_by_hour {
            *self.events_by_hour.entry(k).or_insert(0) += v;
        }

        // Merge hosts
        self.unique_hosts.extend(other.unique_hosts);

        // Merge stats
        self.processing_stats.merge(other.processing_stats);
    }
}

impl Default for AggregatedResults {
    fn default() -> Self {
        Self::new()
    }
}

/// Processing statistics for aggregation
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ProcessingStats {
    pub event_count: usize,
    pub total_bytes: usize,
    pub max_payload_size: usize,
    pub min_payload_size: usize,
}

impl ProcessingStats {
    /// Merge with another stats object
    pub fn merge(&mut self, other: ProcessingStats) {
        self.event_count += other.event_count;
        self.total_bytes += other.total_bytes;
        self.max_payload_size = self.max_payload_size.max(other.max_payload_size);
        if other.min_payload_size > 0 {
            if self.min_payload_size == 0 {
                self.min_payload_size = other.min_payload_size;
            } else {
                self.min_payload_size = self.min_payload_size.min(other.min_payload_size);
            }
        }
    }

    /// Get average payload size
    pub fn avg_payload_size(&self) -> usize {
        if self.event_count > 0 {
            self.total_bytes / self.event_count
        } else {
            0
        }
    }
}
