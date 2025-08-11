//! Replay mode for historical event processing

use crate::replay_progress::{ProgressTracker, ReplayPhase};
use crate::SatelliteResult;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sinex_core::db::models::RawEvent;
use sinex_core::db::{repositories::DbPoolExt, DbPool as PgPool};
use sinex_core::types::domain::{EventSource, EventType};
use std::collections::HashMap;
use tracing::{debug, info, warn};

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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
}

impl ReplayManager {
    /// Create a new replay manager
    pub fn new(pool: PgPool, mode: ReplayMode) -> Self {
        Self {
            pool,
            mode,
            batch_size: 1000,
        }
    }

    /// Set batch size for replay processing
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
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
                    let start_time =
                        start_time.unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap());
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
        &self,
        mut processor: F,
        progress_callback: Option<
            impl Fn(&crate::replay_progress::ReplayProgress) + Send + Sync + 'static,
        >,
    ) -> SatelliteResult<ReplayResult>
    where
        F: FnMut(Vec<RawEvent>) -> Fut,
        Fut: std::future::Future<Output = SatelliteResult<usize>>,
    {
        if matches!(self.mode, ReplayMode::Live) {
            return Ok(ReplayResult {
                total_processed: 0,
                total_batches: 0,
                errors: vec![],
            });
        }

        info!("Starting replay mode processing with progress tracking");

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
        let mut total_batches = 0;
        let mut errors = Vec::new();
        let mut offset = 0;

        // Start processing phase
        tracker.set_phase(ReplayPhase::Processing).await;

        loop {
            // Fetch events using the query system based on mode
            let events: Vec<RawEvent> = match &self.mode {
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
                        let start_time =
                            start_time.unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap());
                        let end_time = end_time.unwrap_or_else(Utc::now);

                        // TODO: Add time range query with source filter to repository
                        // For now, use search with filters
                        use sinex_core::db::repositories::EventSearchFilters;
                        self.pool
                            .events()
                            .search(EventSearchFilters {
                                source: Some(EventSource::new(source)),
                                after: Some(start_time),
                                before: Some(end_time),
                                limit: Some(self.batch_size as u64),
                                offset: Some(offset as u64),
                                ..Default::default()
                            })
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
                        use sinex_core::db::repositories::EventSearchFilters;
                        self.pool
                            .events()
                            .search(EventSearchFilters {
                                limit: Some(self.batch_size as u64),
                                offset: Some(offset as u64),
                                ..Default::default()
                            })
                            .await?
                            .into_iter()
                            .filter(|event: &RawEvent| {
                                let type_matches =
                                    event_types.iter().any(|t| t == event.event_type.as_str());
                                let start_matches = start_time.map_or(true, |start| {
                                    event.ts_orig.as_ref().map_or(false, |ts| *ts >= start)
                                });
                                let end_matches = end_time.map_or(true, |end| {
                                    event.ts_orig.as_ref().map_or(false, |ts| *ts <= end)
                                });
                                type_matches && start_matches && end_matches
                            })
                            .collect()
                    }
                }
                ReplayMode::Custom { filters } => {
                    // Use get_recent as base query and apply filters

                    // get_recent doesn't support offset, use search instead
                    use sinex_core::db::repositories::EventSearchFilters;
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
                    }
                }
            }

            total_batches += 1;
            offset += self.batch_size;

            // Update batch completion in tracker
            tracker.complete_batch().await;

            // Save checkpoint periodically (every 10 batches)
            if total_batches % 10 == 0 {
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

        Ok(ReplayResult {
            total_processed,
            total_batches,
            errors,
        })
    }

    /// Process events in replay mode (backwards compatibility)
    pub async fn replay_events<F, Fut>(&self, processor: F) -> SatelliteResult<ReplayResult>
    where
        F: FnMut(Vec<RawEvent>) -> Fut,
        Fut: std::future::Future<Output = SatelliteResult<usize>>,
    {
        self.replay_events_with_progress(
            processor,
            None::<fn(&crate::replay_progress::ReplayProgress)>,
        )
        .await
    }

    /// Apply custom filters to an event
    fn apply_custom_filters(&self, event: &RawEvent, filters: &ReplayFilters) -> bool {
        // Check source patterns (simple wildcard matching)
        if let Some(sources) = &filters.sources {
            let source_matches = sources.iter().any(|pattern| {
                if pattern.contains('*') {
                    // Simple wildcard matching - just check prefix/suffix
                    if pattern.starts_with('*') && pattern.ends_with('*') {
                        let middle = &pattern[1..pattern.len() - 1];
                        event.source.contains(middle)
                    } else if pattern.starts_with('*') {
                        let suffix = &pattern[1..];
                        event.source.ends_with(suffix)
                    } else if pattern.ends_with('*') {
                        let prefix = &pattern[..pattern.len() - 1];
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
                    } else if pattern.starts_with('*') {
                        let suffix = &pattern[1..];
                        event.event_type.ends_with(suffix)
                    } else if pattern.ends_with('*') {
                        let prefix = &pattern[..pattern.len() - 1];
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
                    } else if pattern.starts_with('*') {
                        let suffix = &pattern[1..];
                        event.host.ends_with(suffix)
                    } else if pattern.ends_with('*') {
                        let prefix = &pattern[..pattern.len() - 1];
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
            if event.ts_orig.as_ref().map_or(false, |ts| *ts < start_time) {
                return false;
            }
        }

        if let Some(end_time) = filters.end_time {
            if event.ts_orig.as_ref().map_or(false, |ts| *ts > end_time) {
                return false;
            }
        }

        // TODO: Implement payload filters when needed
        // if let Some(payload_filters) = &filters.payload_filters {
        //     // Complex JSON filtering logic would go here
        // }

        true
    }
}

/// Replay statistics
#[derive(Debug, Clone)]
pub struct ReplayStats {
    pub total_events: u64,
    pub batch_size: usize,
    pub estimated_batches: usize,
}

/// Replay processing result
#[derive(Debug, Clone)]
pub struct ReplayResult {
    pub total_processed: usize,
    pub total_batches: usize,
    pub errors: Vec<String>,
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
}
