//! Mock database implementation for testing
//!
//! Provides a controllable database substitute that can simulate:
//! - Connection failures
//! - Query timeouts
//! - Data corruption
//! - Constraint violations
//! - Transaction failures

use crate::common::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tokio::time::{Duration, Instant};

/// Configuration for MockDatabase behavior
#[derive(Debug, Clone)]
pub struct MockDatabaseConfig {
    /// Maximum number of concurrent connections
    pub max_connections: usize,
    /// Probability of connection failure
    pub connection_failure_rate: f64,
    /// Probability of query failure
    pub query_failure_rate: f64,
    /// Simulated query latency
    pub query_latency: Duration,
    /// Maximum query timeout
    pub query_timeout: Duration,
    /// Whether to enforce constraints
    pub enforce_constraints: bool,
    /// Whether to simulate slow queries
    pub simulate_slow_queries: bool,
    /// Memory limit for result sets
    pub memory_limit: usize,
    /// Failure patterns to inject
    pub failure_patterns: Vec<super::failure_injector::FailurePattern>,
}

impl Default for MockDatabaseConfig {
    fn default() -> Self {
        Self {
            max_connections: 20,
            connection_failure_rate: 0.0,
            query_failure_rate: 0.0,
            query_latency: Duration::from_millis(1),
            query_timeout: Duration::from_secs(30),
            enforce_constraints: true,
            simulate_slow_queries: false,
            memory_limit: 100 * 1024 * 1024, // 100MB
            failure_patterns: Vec::new(),
        }
    }
}

/// Mock database implementation
pub struct MockDatabase {
    config: MockDatabaseConfig,
    events: Arc<RwLock<Vec<MockEvent>>>,
    checkpoints: Arc<RwLock<HashMap<String, MockCheckpoint>>>,
    connections: Arc<Mutex<usize>>,
    query_count: Arc<Mutex<usize>>,
    failure_injector: Arc<Mutex<super::failure_injector::FailureInjector>>,
    start_time: Instant,
}

#[derive(Debug, Clone)]
struct MockEvent {
    id: sinex_ulid::Ulid,
    source: String,
    event_type: String,
    payload: serde_json::Value,
    ts_ingest: chrono::DateTime<chrono::Utc>,
    ts_orig: Option<chrono::DateTime<chrono::Utc>>,
    host: String,
    ingestor_version: Option<String>,
}

#[derive(Debug, Clone)]
struct MockCheckpoint {
    automaton_name: String,
    consumer_group: String,
    consumer_name: String,
    processed_count: u64,
    last_processed_id: Option<String>,
    last_activity: chrono::DateTime<chrono::Utc>,
    state_data: Option<serde_json::Value>,
}

impl MockDatabase {
    pub fn new(config: MockDatabaseConfig) -> Self {
        let failure_injector = super::failure_injector::FailureInjector::new(
            super::failure_injector::FailureConfig {
                patterns: config.failure_patterns.clone(),
                enabled: true,
            }
        );

        Self {
            config,
            events: Arc::new(RwLock::new(Vec::new())),
            checkpoints: Arc::new(RwLock::new(HashMap::new())),
            connections: Arc::new(Mutex::new(0)),
            query_count: Arc::new(Mutex::new(0)),
            failure_injector: Arc::new(Mutex::new(failure_injector)),
            start_time: Instant::now(),
        }
    }

    pub async fn connect(&self) -> Result<MockDatabaseConnection, MockDatabaseError> {
        // Check connection limit
        let mut connections = self.connections.lock().await;
        if *connections >= self.config.max_connections {
            return Err(MockDatabaseError::ConnectionLimitExceeded);
        }

        // Simulate connection failure
        if self.should_fail_connection().await {
            return Err(MockDatabaseError::ConnectionFailed);
        }

        *connections += 1;
        drop(connections);

        // Simulate connection latency
        tokio::time::sleep(self.config.query_latency).await;

        Ok(MockDatabaseConnection {
            database: self.clone(),
            connected: true,
        })
    }

    async fn should_fail_connection(&self) -> bool {
        let mut injector = self.failure_injector.lock().await;
        injector.should_fail("connection").await
            || fastrand::f64() < self.config.connection_failure_rate
    }

    async fn should_fail_query(&self, query_type: &str) -> bool {
        let mut injector = self.failure_injector.lock().await;
        injector.should_fail(query_type).await
            || fastrand::f64() < self.config.query_failure_rate
    }

    pub async fn get_stats(&self) -> MockDatabaseStats {
        let events = self.events.read().await;
        let checkpoints = self.checkpoints.read().await;
        let connections = *self.connections.lock().await;
        let query_count = *self.query_count.lock().await;

        MockDatabaseStats {
            events_count: events.len(),
            checkpoints_count: checkpoints.len(),
            connections_count: connections,
            query_count,
            uptime: self.start_time.elapsed(),
        }
    }

    pub async fn reset(&self) {
        let mut events = self.events.write().await;
        events.clear();
        let mut checkpoints = self.checkpoints.write().await;
        checkpoints.clear();
        let mut connections = self.connections.lock().await;
        *connections = 0;
        let mut query_count = self.query_count.lock().await;
        *query_count = 0;
    }

    pub async fn inject_failure(&self, pattern: super::failure_injector::FailurePattern) {
        let mut injector = self.failure_injector.lock().await;
        injector.add_pattern(pattern).await;
    }

    /// Simulate database corruption
    pub async fn simulate_corruption(&self, percentage: f64) {
        let mut events = self.events.write().await;
        let corrupt_count = (events.len() as f64 * percentage) as usize;
        
        for i in 0..corrupt_count.min(events.len()) {
            // Corrupt event data
            events[i].payload = serde_json::json!({"corrupted": true});
        }
    }

    /// Simulate slow query performance
    pub async fn simulate_slow_queries(&self, enabled: bool) {
        // This would modify the config in a real implementation
        // For now, this is a placeholder
    }

    /// Simulate constraint violations
    pub async fn simulate_constraint_violations(&self, enabled: bool) {
        // This would modify constraint enforcement in a real implementation
        // For now, this is a placeholder
    }
}

impl Clone for MockDatabase {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            events: self.events.clone(),
            checkpoints: self.checkpoints.clone(),
            connections: self.connections.clone(),
            query_count: self.query_count.clone(),
            failure_injector: self.failure_injector.clone(),
            start_time: self.start_time,
        }
    }
}

/// Mock database connection
pub struct MockDatabaseConnection {
    database: MockDatabase,
    connected: bool,
}

impl MockDatabaseConnection {
    pub async fn insert_event(&mut self, event: &RawEvent) -> Result<(), MockDatabaseError> {
        if !self.connected {
            return Err(MockDatabaseError::NotConnected);
        }

        if self.database.should_fail_query("insert").await {
            return Err(MockDatabaseError::QueryFailed("Insert failed".to_string()));
        }

        // Simulate query latency
        tokio::time::sleep(self.database.config.query_latency).await;

        // Increment query count
        let mut query_count = self.database.query_count.lock().await;
        *query_count += 1;

        // Validate constraints if enabled
        if self.database.config.enforce_constraints {
            if event.source.is_empty() {
                return Err(MockDatabaseError::ConstraintViolation("Source cannot be empty".to_string()));
            }
            if event.event_type.is_empty() {
                return Err(MockDatabaseError::ConstraintViolation("Event type cannot be empty".to_string()));
            }
        }

        // Insert event
        let mut events = self.database.events.write().await;
        let mock_event = MockEvent {
            id: event.id,
            source: event.source.clone(),
            event_type: event.event_type.clone(),
            payload: event.payload.clone(),
            ts_ingest: event.ts_ingest,
            ts_orig: event.ts_orig,
            host: event.host.clone(),
            ingestor_version: event.ingestor_version.clone(),
        };

        events.push(mock_event);

        Ok(())
    }

    pub async fn query_events(&mut self, limit: Option<usize>) -> Result<Vec<RawEvent>, MockDatabaseError> {
        if !self.connected {
            return Err(MockDatabaseError::NotConnected);
        }

        if self.database.should_fail_query("select").await {
            return Err(MockDatabaseError::QueryFailed("Select failed".to_string()));
        }

        // Simulate query latency
        if self.database.config.simulate_slow_queries {
            tokio::time::sleep(Duration::from_millis(100)).await;
        } else {
            tokio::time::sleep(self.database.config.query_latency).await;
        }

        // Increment query count
        let mut query_count = self.database.query_count.lock().await;
        *query_count += 1;

        let events = self.database.events.read().await;
        let mut result = Vec::new();
        let take_count = limit.unwrap_or(events.len());

        for event in events.iter().take(take_count) {
            result.push(RawEvent {
                id: event.id,
                source: event.source.clone(),
                event_type: event.event_type.clone(),
                payload: event.payload.clone(),
                ts_ingest: event.ts_ingest,
                ts_orig: event.ts_orig,
                host: event.host.clone(),
                ingestor_version: event.ingestor_version.clone(),
                payload_schema_id: None,
                source_event_ids: None,
            });
        }

        Ok(result)
    }

    pub async fn count_events(&mut self) -> Result<usize, MockDatabaseError> {
        if !self.connected {
            return Err(MockDatabaseError::NotConnected);
        }

        if self.database.should_fail_query("count").await {
            return Err(MockDatabaseError::QueryFailed("Count failed".to_string()));
        }

        // Simulate query latency
        tokio::time::sleep(self.database.config.query_latency).await;

        // Increment query count
        let mut query_count = self.database.query_count.lock().await;
        *query_count += 1;

        let events = self.database.events.read().await;
        Ok(events.len())
    }

    pub async fn upsert_checkpoint(
        &mut self,
        automaton_name: &str,
        consumer_group: &str,
        consumer_name: &str,
        processed_count: u64,
        last_processed_id: Option<&str>,
        state_data: Option<&serde_json::Value>,
    ) -> Result<(), MockDatabaseError> {
        if !self.connected {
            return Err(MockDatabaseError::NotConnected);
        }

        if self.database.should_fail_query("upsert").await {
            return Err(MockDatabaseError::QueryFailed("Upsert failed".to_string()));
        }

        // Simulate query latency
        tokio::time::sleep(self.database.config.query_latency).await;

        // Increment query count
        let mut query_count = self.database.query_count.lock().await;
        *query_count += 1;

        let checkpoint = MockCheckpoint {
            automaton_name: automaton_name.to_string(),
            consumer_group: consumer_group.to_string(),
            consumer_name: consumer_name.to_string(),
            processed_count,
            last_processed_id: last_processed_id.map(|s| s.to_string()),
            last_activity: chrono::Utc::now(),
            state_data: state_data.cloned(),
        };

        let mut checkpoints = self.database.checkpoints.write().await;
        let key = format!("{}:{}:{}", automaton_name, consumer_group, consumer_name);
        checkpoints.insert(key, checkpoint);

        Ok(())
    }

    pub async fn get_checkpoint(
        &mut self,
        automaton_name: &str,
        consumer_group: &str,
        consumer_name: &str,
    ) -> Result<Option<(u64, Option<String>, Option<serde_json::Value>)>, MockDatabaseError> {
        if !self.connected {
            return Err(MockDatabaseError::NotConnected);
        }

        if self.database.should_fail_query("select").await {
            return Err(MockDatabaseError::QueryFailed("Select failed".to_string()));
        }

        // Simulate query latency
        tokio::time::sleep(self.database.config.query_latency).await;

        // Increment query count
        let mut query_count = self.database.query_count.lock().await;
        *query_count += 1;

        let checkpoints = self.database.checkpoints.read().await;
        let key = format!("{}:{}:{}", automaton_name, consumer_group, consumer_name);
        
        match checkpoints.get(&key) {
            Some(checkpoint) => Ok(Some((
                checkpoint.processed_count,
                checkpoint.last_processed_id.clone(),
                checkpoint.state_data.clone(),
            ))),
            None => Ok(None),
        }
    }

    pub async fn transaction<F, T>(&mut self, f: F) -> Result<T, MockDatabaseError>
    where
        F: FnOnce(&mut Self) -> Result<T, MockDatabaseError>,
    {
        if !self.connected {
            return Err(MockDatabaseError::NotConnected);
        }

        if self.database.should_fail_query("transaction").await {
            return Err(MockDatabaseError::TransactionFailed);
        }

        // Simulate transaction overhead
        tokio::time::sleep(self.database.config.query_latency * 2).await;

        // Execute transaction
        f(self)
    }

    pub async fn disconnect(&mut self) -> Result<(), MockDatabaseError> {
        if self.connected {
            self.connected = false;
            let mut connections = self.database.connections.lock().await;
            *connections = connections.saturating_sub(1);
        }
        Ok(())
    }
}

impl Drop for MockDatabaseConnection {
    fn drop(&mut self) {
        if self.connected {
            self.connected = false;
            // In a real implementation, we'd need to handle connection cleanup asynchronously
        }
    }
}

/// Mock database errors
#[derive(Debug, Clone)]
pub enum MockDatabaseError {
    NotConnected,
    ConnectionFailed,
    ConnectionLimitExceeded,
    QueryFailed(String),
    ConstraintViolation(String),
    TransactionFailed,
    Timeout,
    Corrupted,
}

impl std::fmt::Display for MockDatabaseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MockDatabaseError::NotConnected => write!(f, "Database not connected"),
            MockDatabaseError::ConnectionFailed => write!(f, "Failed to connect to database"),
            MockDatabaseError::ConnectionLimitExceeded => write!(f, "Connection limit exceeded"),
            MockDatabaseError::QueryFailed(msg) => write!(f, "Query failed: {}", msg),
            MockDatabaseError::ConstraintViolation(msg) => write!(f, "Constraint violation: {}", msg),
            MockDatabaseError::TransactionFailed => write!(f, "Transaction failed"),
            MockDatabaseError::Timeout => write!(f, "Database operation timed out"),
            MockDatabaseError::Corrupted => write!(f, "Database corrupted"),
        }
    }
}

impl std::error::Error for MockDatabaseError {}

/// Statistics for MockDatabase
#[derive(Debug, Clone)]
pub struct MockDatabaseStats {
    pub events_count: usize,
    pub checkpoints_count: usize,
    pub connections_count: usize,
    pub query_count: usize,
    pub uptime: Duration,
}

/// Test utilities for MockDatabase
impl MockDatabase {
    pub fn for_testing() -> Self {
        Self::new(MockDatabaseConfig::default())
    }

    pub fn with_failures(failure_rate: f64) -> Self {
        let config = MockDatabaseConfig {
            connection_failure_rate: failure_rate,
            query_failure_rate: failure_rate,
            ..Default::default()
        };
        Self::new(config)
    }

    pub fn with_constraints(enforce: bool) -> Self {
        let config = MockDatabaseConfig {
            enforce_constraints: enforce,
            ..Default::default()
        };
        Self::new(config)
    }

    pub fn with_slow_queries() -> Self {
        let config = MockDatabaseConfig {
            simulate_slow_queries: true,
            query_latency: Duration::from_millis(100),
            ..Default::default()
        };
        Self::new(config)
    }

    pub async fn verify_events(&self, expected_count: usize) -> bool {
        let events = self.events.read().await;
        events.len() == expected_count
    }

    pub async fn verify_checkpoints(&self, expected_count: usize) -> bool {
        let checkpoints = self.checkpoints.read().await;
        checkpoints.len() == expected_count
    }

    pub async fn get_events_by_source(&self, source: &str) -> Vec<RawEvent> {
        let events = self.events.read().await;
        events.iter()
            .filter(|e| e.source == source)
            .map(|e| RawEvent {
                id: e.id,
                source: e.source.clone(),
                event_type: e.event_type.clone(),
                payload: e.payload.clone(),
                ts_ingest: e.ts_ingest,
                ts_orig: e.ts_orig,
                host: e.host.clone(),
                ingestor_version: e.ingestor_version.clone(),
                payload_schema_id: None,
                source_event_ids: None,
            })
            .collect()
    }

    pub async fn get_checkpoint_info(&self, automaton_name: &str) -> Option<(u64, Option<String>)> {
        let checkpoints = self.checkpoints.read().await;
        for (key, checkpoint) in checkpoints.iter() {
            if key.starts_with(&format!("{}:", automaton_name)) {
                return Some((checkpoint.processed_count, checkpoint.last_processed_id.clone()));
            }
        }
        None
    }
}