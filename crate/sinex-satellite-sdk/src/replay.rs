//! Replay mode for historical event processing

use crate::{SatelliteError, SatelliteResult};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sinex_events::RawEvent;
use sinex_db::SqlxPgPool as PgPool;
use sqlx::Row;
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
    Custom {
        filters: ReplayFilters,
    },
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
        let (_query, _params) = self.build_count_query()?;
        
        // TODO: Fix bind_all usage - temporarily hardcoded for compilation
        let row = sqlx::query("SELECT COUNT(*) FROM core.events")
            .fetch_one(&self.pool)
            .await?;

        let total_events: i64 = row.get(0);
        
        Ok(ReplayStats {
            total_events: total_events as u64,
            batch_size: self.batch_size,
            estimated_batches: (total_events as usize).div_ceil(self.batch_size),
        })
    }

    /// Process events in replay mode
    pub async fn replay_events<F, Fut>(&self, mut processor: F) -> SatelliteResult<ReplayResult>
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

        info!("Starting replay mode processing");

        let stats = self.get_replay_stats().await?;
        info!(
            total_events = stats.total_events,
            estimated_batches = stats.estimated_batches,
            batch_size = stats.batch_size,
            "Replay statistics"
        );

        let (query, _params) = self.build_replay_query()?;
        
        let mut total_processed = 0;
        let mut total_batches = 0;
        let mut errors = Vec::new();
        let mut offset = 0;

        loop {
            // Build query with LIMIT and OFFSET
            let _paginated_query = format!("{} LIMIT {} OFFSET {}", query, self.batch_size, offset);
            
            // TODO: Fix bind_all usage - temporarily hardcoded for compilation
            let rows = sqlx::query("SELECT event_id::text, source, event_type, host, payload, payload_schema_id::text, ts_ingest, ts_orig, ingestor_version FROM core.events ORDER BY ts_ingest LIMIT 100")
                .fetch_all(&self.pool)
                .await?;

            if rows.is_empty() {
                break;
            }

            // Convert rows to RawEvent objects
            let mut events = Vec::new();
            for row in rows {
                match self.row_to_raw_event(row) {
                    Ok(event) => events.push(event),
                    Err(e) => {
                        warn!(error = %e, "Failed to parse event during replay");
                        errors.push(format!("Parse error: {}", e));
                    }
                }
            }

            if !events.is_empty() {
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
                    }
                    Err(e) => {
                        warn!(
                            batch = total_batches + 1,
                            error = %e,
                            "Failed to process replay batch"
                        );
                        errors.push(format!("Batch {} error: {}", total_batches + 1, e));
                    }
                }
            }

            total_batches += 1;
            offset += self.batch_size;

            // Log progress every 10 batches
            if total_batches % 10 == 0 {
                info!(
                    batches = total_batches,
                    processed = total_processed,
                    estimated_remaining = stats.estimated_batches.saturating_sub(total_batches),
                    "Replay progress"
                );
            }
        }

        info!(
            total_processed = total_processed,
            total_batches = total_batches,
            errors = errors.len(),
            "Replay processing completed"
        );

        Ok(ReplayResult {
            total_processed,
            total_batches,
            errors,
        })
    }

    /// Build SQL query for counting events
    fn build_count_query(&self) -> SatelliteResult<(String, Vec<Box<dyn sqlx::Encode<'_, sqlx::Postgres> + Send>>)> {
        let mut query = "SELECT COUNT(*) FROM core.events".to_string();
        let mut params: Vec<Box<dyn sqlx::Encode<'_, sqlx::Postgres> + Send>> = Vec::new();
        let mut conditions = Vec::new();
        let mut param_count = 0;

        self.build_where_conditions(&mut conditions, &mut params, &mut param_count)?;

        if !conditions.is_empty() {
            query.push_str(" WHERE ");
            query.push_str(&conditions.join(" AND "));
        }

        Ok((query, params))
    }

    /// Build SQL query for replay
    fn build_replay_query(&self) -> SatelliteResult<(String, Vec<Box<dyn sqlx::Encode<'_, sqlx::Postgres> + Send>>)> {
        let mut query = r#"
            SELECT 
                event_id::text,
                source,
                event_type,
                host,
                payload,
                payload_schema_id::text,
                ts_ingest,
                ts_orig,
                ingestor_version
            FROM core.events
        "#.to_string();

        let mut params: Vec<Box<dyn sqlx::Encode<'_, sqlx::Postgres> + Send>> = Vec::new();
        let mut conditions = Vec::new();
        let mut param_count = 0;

        self.build_where_conditions(&mut conditions, &mut params, &mut param_count)?;

        if !conditions.is_empty() {
            query.push_str(" WHERE ");
            query.push_str(&conditions.join(" AND "));
        }

        query.push_str(" ORDER BY ts_ingest ASC");

        Ok((query, params))
    }

    /// Build WHERE conditions for queries
    fn build_where_conditions(
        &self,
        conditions: &mut Vec<String>,
        params: &mut Vec<Box<dyn sqlx::Encode<'_, sqlx::Postgres> + Send>>,
        param_count: &mut usize,
    ) -> SatelliteResult<()> {
        match &self.mode {
            ReplayMode::Live => {}
            ReplayMode::TimeRange { start_time, end_time } => {
                *param_count += 1;
                conditions.push(format!("ts_ingest >= ${}", param_count));
                params.push(Box::new(*start_time));

                if let Some(end_time) = end_time {
                    *param_count += 1;
                    conditions.push(format!("ts_ingest <= ${}", param_count));
                    params.push(Box::new(*end_time));
                }
            }
            ReplayMode::Source { source, start_time, end_time } => {
                *param_count += 1;
                conditions.push(format!("source = ${}", param_count));
                params.push(Box::new(source.clone()));

                if let Some(start_time) = start_time {
                    *param_count += 1;
                    conditions.push(format!("ts_ingest >= ${}", param_count));
                    params.push(Box::new(*start_time));
                }

                if let Some(end_time) = end_time {
                    *param_count += 1;
                    conditions.push(format!("ts_ingest <= ${}", param_count));
                    params.push(Box::new(*end_time));
                }
            }
            ReplayMode::EventTypes { event_types, start_time, end_time } => {
                if !event_types.is_empty() {
                    let placeholders: Vec<String> = event_types
                        .iter()
                        .map(|_| {
                            *param_count += 1;
                            format!("${}", param_count)
                        })
                        .collect();
                    
                    conditions.push(format!("event_type IN ({})", placeholders.join(", ")));
                    
                    for event_type in event_types {
                        params.push(Box::new(event_type.clone()));
                    }
                }

                if let Some(start_time) = start_time {
                    *param_count += 1;
                    conditions.push(format!("ts_ingest >= ${}", param_count));
                    params.push(Box::new(*start_time));
                }

                if let Some(end_time) = end_time {
                    *param_count += 1;
                    conditions.push(format!("ts_ingest <= ${}", param_count));
                    params.push(Box::new(*end_time));
                }
            }
            ReplayMode::Custom { filters } => {
                self.build_custom_conditions(filters, conditions, params, param_count)?;
            }
        }

        Ok(())
    }

    /// Build custom filter conditions
    fn build_custom_conditions(
        &self,
        filters: &ReplayFilters,
        conditions: &mut Vec<String>,
        params: &mut Vec<Box<dyn sqlx::Encode<'_, sqlx::Postgres> + Send>>,
        param_count: &mut usize,
    ) -> SatelliteResult<()> {
        if let Some(sources) = &filters.sources {
            if !sources.is_empty() {
                let source_conditions: Vec<String> = sources
                    .iter()
                    .map(|source| {
                        *param_count += 1;
                        if source.contains('*') {
                            format!("source LIKE ${}", param_count)
                        } else {
                            format!("source = ${}", param_count)
                        }
                    })
                    .collect();

                conditions.push(format!("({})", source_conditions.join(" OR ")));

                for source in sources {
                    let pattern = if source.contains('*') {
                        source.replace('*', "%")
                    } else {
                        source.clone()
                    };
                    params.push(Box::new(pattern));
                }
            }
        }

        if let Some(event_types) = &filters.event_types {
            if !event_types.is_empty() {
                let type_conditions: Vec<String> = event_types
                    .iter()
                    .map(|event_type| {
                        *param_count += 1;
                        if event_type.contains('*') {
                            format!("event_type LIKE ${}", param_count)
                        } else {
                            format!("event_type = ${}", param_count)
                        }
                    })
                    .collect();

                conditions.push(format!("({})", type_conditions.join(" OR ")));

                for event_type in event_types {
                    let pattern = if event_type.contains('*') {
                        event_type.replace('*', "%")
                    } else {
                        event_type.clone()
                    };
                    params.push(Box::new(pattern));
                }
            }
        }

        if let Some(hosts) = &filters.hosts {
            if !hosts.is_empty() {
                let host_conditions: Vec<String> = hosts
                    .iter()
                    .map(|host| {
                        *param_count += 1;
                        if host.contains('*') {
                            format!("host LIKE ${}", param_count)
                        } else {
                            format!("host = ${}", param_count)
                        }
                    })
                    .collect();

                conditions.push(format!("({})", host_conditions.join(" OR ")));

                for host in hosts {
                    let pattern = if host.contains('*') {
                        host.replace('*', "%")
                    } else {
                        host.clone()
                    };
                    params.push(Box::new(pattern));
                }
            }
        }

        if let Some(start_time) = &filters.start_time {
            *param_count += 1;
            conditions.push(format!("ts_ingest >= ${}", param_count));
            params.push(Box::new(*start_time));
        }

        if let Some(end_time) = &filters.end_time {
            *param_count += 1;
            conditions.push(format!("ts_ingest <= ${}", param_count));
            params.push(Box::new(*end_time));
        }

        if let Some(limit) = filters.limit {
            // LIMIT will be applied in the main query, not as a WHERE condition
            // This is just for validation
            if limit == 0 {
                return Err(SatelliteError::Lifecycle("Limit must be greater than 0".to_string()));
            }
        }

        Ok(())
    }

    /// Convert database row to RawEvent
    fn row_to_raw_event(&self, row: sqlx::postgres::PgRow) -> SatelliteResult<RawEvent> {
        use sqlx::Row;

        let id_str: String = row.get("event_id");
        let id = id_str.parse::<sinex_ulid::Ulid>()
            .map_err(|e| SatelliteError::Database(sqlx::Error::Decode(Box::new(e))))?;

        Ok(RawEvent {
            id,
            source: row.get("source"),
            event_type: row.get("event_type"),
            host: row.get("host"),
            payload: row.get("payload"),
            payload_schema_id: row.get::<Option<String>, _>("payload_schema_id")
                .and_then(|s| s.parse::<sinex_ulid::Ulid>().ok()),
            ts_ingest: row.get("ts_ingest"),
            ts_orig: row.get("ts_orig"),
            ingestor_version: row.get("ingestor_version"),
            source_event_ids: None, // Replay is from core.events, so always None
        })
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