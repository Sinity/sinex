use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use sinex_db::{RawEvent, DbPoolRef, Timestamp, OptionalTimestamp};
use sinex_ulid::Ulid;
use std::collections::HashMap;
use tracing::{debug, info};

/// Configuration for the event scanner
#[derive(Debug, Clone)]
pub struct ScannerConfig {
    /// Maximum number of events to process in a single batch
    pub batch_size: usize,
    /// How far back to look for events on first run
    pub initial_lookback: Duration,
    /// Whether to process all historical events or just new ones
    pub process_historical: bool,
}

impl Default for ScannerConfig {
    fn default() -> Self {
        Self {
            batch_size: 1000,
            initial_lookback: Duration::hours(24),
            process_historical: false,
        }
    }
}

/// Tracks scanning progress across runs
#[derive(Debug, Clone)]
pub struct ScannerState {
    /// Last processed event ID for each source
    pub last_event_ids: HashMap<String, Ulid>,
    /// Timestamp of last scan
    pub last_scan_ts: OptionalTimestamp,
}

impl Default for ScannerState {
    fn default() -> Self {
        Self {
            last_event_ids: HashMap::new(),
            last_scan_ts: None,
        }
    }
}

/// Scans for new events to promote
pub struct EventScanner {
    config: ScannerConfig,
    state: ScannerState,
}

impl EventScanner {
    pub fn new(config: ScannerConfig) -> Self {
        Self {
            config,
            state: ScannerState::default(),
        }
    }
    
    /// Create scanner with default configuration
    pub fn with_defaults() -> Self {
        Self::new(ScannerConfig::default())
    }
    
    /// Get current state (for persistence)
    pub fn state(&self) -> &ScannerState {
        &self.state
    }
    
    /// Restore state from previous run
    pub fn restore_state(&mut self, state: ScannerState) {
        self.state = state;
    }
    
    /// Scan for new events that need promotion
    pub async fn scan_new_events(&mut self, pool: DbPoolRef<'_>) -> Result<Vec<RawEvent>> {
        let start_time = if let Some(last_scan) = self.state.last_scan_ts {
            last_scan
        } else if self.config.process_historical {
            DateTime::<Utc>::MIN_UTC
        } else {
            Utc::now() - self.config.initial_lookback
        };
        
        debug!(
            start_time = %start_time,
            batch_size = self.config.batch_size,
            "Scanning for new events"
        );
        
        let events = self.fetch_new_events(pool, start_time).await?;
        
        // Update state with new event IDs
        for event in &events {
            self.state.last_event_ids
                .entry(event.source.clone())
                .and_modify(|id| {
                    if event.id > *id {
                        *id = event.id;
                    }
                })
                .or_insert(event.id);
        }
        
        if !events.is_empty() {
            self.state.last_scan_ts = Some(Utc::now());
            info!(
                count = events.len(),
                sources = ?events.iter().map(|e| &e.source).collect::<std::collections::HashSet<_>>(),
                "Found new events"
            );
        }
        
        Ok(events)
    }
    
    /// Fetch events newer than the given timestamp
    async fn fetch_new_events(
        &self,
        pool: DbPoolRef<'_>,
        since: Timestamp,
    ) -> Result<Vec<RawEvent>> {
        // Build dynamic query based on whether we have last event IDs
        if self.state.last_event_ids.is_empty() {
            // First run - use timestamp only
            self.fetch_by_timestamp(pool, since).await
        } else {
            // Subsequent runs - use event IDs for each source
            self.fetch_by_event_ids(pool, since).await
        }
    }
    
    /// Fetch events using timestamp filter
    async fn fetch_by_timestamp(
        &self,
        pool: DbPoolRef<'_>,
        since: Timestamp,
    ) -> Result<Vec<RawEvent>> {
        let records = sqlx::query!(
            r#"
            SELECT 
                id::uuid as "id!",
                source as "source!",
                event_type as "event_type!",
                ts_ingest as "ts_ingest!",
                ts_orig,
                host as "host!",
                ingestor_version,
                payload_schema_id::uuid as "payload_schema_id",
                payload as "payload!"
            FROM raw.events
            WHERE ts_ingest > $1
            ORDER BY id ASC
            LIMIT $2
            "#,
            since,
            self.config.batch_size as i64
        )
        .fetch_all(pool)
        .await?;
        
        let events = records
            .into_iter()
            .map(|r| RawEvent {
                id: Ulid::from_uuid(r.id),
                source: r.source,
                event_type: r.event_type,
                ts_ingest: r.ts_ingest,
                ts_orig: r.ts_orig,
                host: r.host,
                ingestor_version: r.ingestor_version,
                payload_schema_id: r.payload_schema_id.map(Ulid::from_uuid),
                payload: r.payload,
            })
            .collect();
        
        Ok(events)
    }
    
    /// Fetch events using last known event IDs per source
    async fn fetch_by_event_ids(
        &self,
        pool: DbPoolRef<'_>,
        since: Timestamp,
    ) -> Result<Vec<RawEvent>> {
        // For simplicity, we'll fetch all events newer than any of our last IDs
        // In production, you might want per-source queries for efficiency
        let min_id = self.state.last_event_ids
            .values()
            .min()
            .copied()
            .unwrap_or_else(Ulid::new);
        
        let records = sqlx::query!(
            r#"
            SELECT 
                id::uuid as "id!",
                source as "source!",
                event_type as "event_type!",
                ts_ingest as "ts_ingest!",
                ts_orig,
                host as "host!",
                ingestor_version,
                payload_schema_id::uuid as "payload_schema_id",
                payload as "payload!"
            FROM raw.events
            WHERE id > $1::uuid::ulid
                AND ts_ingest > $2
            ORDER BY id ASC
            LIMIT $3
            "#,
            min_id.to_uuid(),
            since,
            self.config.batch_size as i64
        )
        .fetch_all(pool)
        .await?;
        
        let events: Vec<RawEvent> = records
            .into_iter()
            .filter_map(|r| {
                let event = RawEvent {
                    id: Ulid::from_uuid(r.id),
                    source: r.source,
                    event_type: r.event_type,
                    ts_ingest: r.ts_ingest,
                    ts_orig: r.ts_orig,
                    host: r.host,
                    ingestor_version: r.ingestor_version,
                    payload_schema_id: r.payload_schema_id.map(Ulid::from_uuid),
                    payload: r.payload,
                };
                
                // Only include if newer than last known ID for this source
                if let Some(&last_id) = self.state.last_event_ids.get(&event.source) {
                    if event.id > last_id {
                        Some(event)
                    } else {
                        None
                    }
                } else {
                    // New source
                    Some(event)
                }
            })
            .collect();
        
        Ok(events)
    }
    
    /// Get events that don't have work queue entries yet
    pub async fn get_unqueued_events(
        &self,
        pool: DbPoolRef<'_>,
        limit: usize,
    ) -> Result<Vec<RawEvent>> {
        let records = sqlx::query!(
            r#"
            SELECT
                e.id::uuid as "id!",
                e.source as "source!",
                e.event_type as "event_type!",
                e.ts_ingest as "ts_ingest!",
                e.ts_orig,
                e.host as "host!",
                e.ingestor_version,
                e.payload_schema_id::uuid as "payload_schema_id",
                e.payload as "payload!"
            FROM raw.events e
            WHERE NOT EXISTS (
                SELECT 1 
                FROM sinex_schemas.work_queue pq 
                WHERE pq.raw_event_id = e.id
            )
            ORDER BY e.id ASC
            LIMIT $1
            "#,
            limit as i64
        )
        .fetch_all(pool)
        .await?;
        
        let events = records
            .into_iter()
            .map(|r| RawEvent {
                id: Ulid::from_uuid(r.id),
                source: r.source,
                event_type: r.event_type,
                ts_ingest: r.ts_ingest,
                ts_orig: r.ts_orig,
                host: r.host,
                ingestor_version: r.ingestor_version,
                payload_schema_id: r.payload_schema_id.map(Ulid::from_uuid),
                payload: r.payload,
            })
            .collect();
        
        Ok(events)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_scanner_config_default() {
        let config = ScannerConfig::default();
        assert_eq!(config.batch_size, 1000);
        assert_eq!(config.initial_lookback, Duration::hours(24));
        assert!(!config.process_historical);
    }
    
    #[test]
    fn test_scanner_state_tracking() {
        let mut scanner = EventScanner::with_defaults();
        assert!(scanner.state.last_event_ids.is_empty());
        assert!(scanner.state.last_scan_ts.is_none());
        
        // Simulate state update
        let event_id = Ulid::new();
        scanner.state.last_event_ids.insert("test.source".to_string(), event_id);
        scanner.state.last_scan_ts = Some(Utc::now());
        
        assert_eq!(scanner.state.last_event_ids.get("test.source"), Some(&event_id));
        assert!(scanner.state.last_scan_ts.is_some());
    }
    
    #[test]
    fn test_scanner_state_restore() {
        let mut scanner = EventScanner::with_defaults();
        
        let mut state = ScannerState::default();
        let event_id = Ulid::new();
        state.last_event_ids.insert("test.source".to_string(), event_id);
        state.last_scan_ts = Some(Utc::now());
        
        scanner.restore_state(state.clone());
        
        assert_eq!(scanner.state.last_event_ids.get("test.source"), Some(&event_id));
        assert_eq!(scanner.state.last_scan_ts, state.last_scan_ts);
    }
}