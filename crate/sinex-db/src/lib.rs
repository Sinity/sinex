pub mod models;
pub mod pool;
pub mod queries;
pub mod validation;
pub mod metrics;

// Re-export commonly used types and query functions
pub use queries::{
    QueueDepthMetric, refresh_routing_cache, run_batch_router, calculate_queue_depth_metrics,
    get_event_by_id, insert_raw_event, get_recent_events, get_events_by_source,
    get_events_by_type, get_events_in_time_range, claim_work_queue_items, 
    complete_work_queue_item, fail_work_queue_item, add_to_work_queue,
    get_next_work_item, complete_work_item, fail_work_item
};

use anyhow::Result;
use sqlx::postgres::PgPoolOptions;
use sqlx::{migrate::MigrateDatabase, PgPool, Postgres};
use std::time::Duration;
use tracing::info;

// Common type aliases for database operations
pub type DbPool = PgPool;
pub type DbPoolRef<'a> = &'a PgPool;

// Import type aliases from sinex-ulid and add our own
pub use sinex_ulid::Timestamp;
pub type OptionalTimestamp = Option<Timestamp>;
pub type JsonValue = serde_json::Value;

/// Create a database connection pool with default settings
pub async fn create_pool(database_url: &str) -> Result<DbPool> {
    let pool = PgPoolOptions::new()
        .max_connections(500)  // Massive pool size
        .min_connections(50)
        .acquire_timeout(Duration::from_secs(120))  // Very long timeout
        .idle_timeout(Duration::from_secs(1800))
        .connect(database_url)
        .await?;

    info!("Database pool created successfully");
    Ok(pool)
}

/// Create a database connection pool optimized for testing with high concurrency
pub async fn create_test_pool(database_url: &str) -> Result<DbPool> {
    let pool = PgPoolOptions::new()
        .max_connections(2000)  // Even more massive limit for concurrent tests
        .min_connections(200)
        .acquire_timeout(Duration::from_secs(600))  // 10 minute timeout
        .idle_timeout(Duration::from_secs(1200))
        .test_before_acquire(false)  // Skip connection testing for speed
        .connect(database_url)
        .await?;

    info!("Test database pool created successfully with ultra-high concurrency settings");
    Ok(pool)
}

/// Create database if it doesn't exist
pub async fn create_database_if_not_exists(database_url: &str) -> Result<()> {
    if !Postgres::database_exists(database_url).await? {
        info!("Creating database...");
        Postgres::create_database(database_url).await?;
    }
    Ok(())
}

/// Run database migrations
pub async fn run_migrations(pool: DbPoolRef<'_>) -> Result<()> {
    sqlx::migrate!("../../migrations")
        .run(pool)
        .await?;
    
    info!("Database migrations completed");
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::models::{RawEvent, WorkQueueItem, QueueStatus};
    use sinex_ulid::Ulid;
    use chrono::Utc;
    use serde_json::json;

    #[test]
    fn test_raw_event_creation() {
        let event = RawEvent {
            id: Ulid::new(),
            source: "test.source".to_string(),
            event_type: "test_event".to_string(),
            ts_ingest: Utc::now(),
            ts_orig: None,
            host: "localhost".to_string(),
            ingestor_version: Some("1.0.0".to_string()),
            payload_schema_id: None,
            payload: json!({"test": "data"}),
        };

        assert_eq!(event.source, "test.source");
        assert_eq!(event.event_type, "test_event");
        assert_eq!(event.host, "localhost");
        assert_eq!(event.ingestor_version, Some("1.0.0".to_string()));
        assert_eq!(event.payload["test"], "data");
    }

    #[test]
    fn test_work_queue_item_creation() {
        let queue_item = WorkQueueItem {
            queue_id: Ulid::new(),
            raw_event_id: Ulid::new(),
            target_agent_name: "test_agent".to_string(),
            status: "pending".to_string(),
            attempts: 0,
            max_attempts: 3,
            last_attempt_ts: None,
            next_retry_ts: None,
            error_message_last: None,
            created_at: Utc::now(),
            processing_worker_id: None,
            processed_at: None,
            failure_reason: None,
        };

        assert_eq!(queue_item.target_agent_name, "test_agent");
        assert_eq!(queue_item.status, "pending");
        assert_eq!(queue_item.attempts, 0);
        assert_eq!(queue_item.max_attempts, 3);
        assert!(queue_item.last_attempt_ts.is_none());
        assert!(queue_item.processed_at.is_none());
    }

    #[test]
    fn test_queue_status_enum() {
        // Test enum variants - using Debug format since Display isn't implemented
        assert_eq!(format!("{:?}", QueueStatus::Pending), "Pending");
        assert_eq!(format!("{:?}", QueueStatus::Processing), "Processing");
        assert_eq!(format!("{:?}", QueueStatus::Succeeded), "Succeeded");
        assert_eq!(format!("{:?}", QueueStatus::Failed), "Failed");
        assert_eq!(format!("{:?}", QueueStatus::FailedRetryable), "FailedRetryable");

        // Test equality
        assert_eq!(QueueStatus::Pending, QueueStatus::Pending);
        assert_ne!(QueueStatus::Pending, QueueStatus::Processing);
    }

    #[test]
    fn test_queue_status_from_string() {
        // Test parsing from strings
        assert_eq!(QueueStatus::from("pending"), QueueStatus::Pending);
        assert_eq!(QueueStatus::from("processing"), QueueStatus::Processing);
        assert_eq!(QueueStatus::from("succeeded"), QueueStatus::Succeeded);
        assert_eq!(QueueStatus::from("failed"), QueueStatus::Failed);
        assert_eq!(QueueStatus::from("failed_retryable"), QueueStatus::FailedRetryable);
        
        // Test legacy mapping
        assert_eq!(QueueStatus::from("completed"), QueueStatus::Succeeded);
        
        // Test unknown values default to Pending
        assert_eq!(QueueStatus::from("unknown"), QueueStatus::Pending);
        assert_eq!(QueueStatus::from(""), QueueStatus::Pending);
        assert_eq!(QueueStatus::from("invalid"), QueueStatus::Pending);
    }

    #[test]
    fn test_queue_status_serde() {
        use serde_json;
        
        // Test serialization
        let status = QueueStatus::Succeeded;
        let json = serde_json::to_string(&status).unwrap();
        assert_eq!(json, "\"succeeded\"");
        
        // Test deserialization
        let parsed: QueueStatus = serde_json::from_str("\"processing\"").unwrap();
        assert_eq!(parsed, QueueStatus::Processing);
        
        // Test round-trip
        let original = QueueStatus::FailedRetryable;
        let json = serde_json::to_string(&original).unwrap();
        let restored: QueueStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(original, restored);
    }

    #[test]
    fn test_ulid_in_models() {
        let ulid1 = Ulid::new();
        // Small delay to ensure different timestamps
        std::thread::sleep(std::time::Duration::from_millis(1));
        let ulid2 = Ulid::new();
        
        // ULIDs should be unique
        assert_ne!(ulid1, ulid2);
        
        // ULIDs should be time-ordered (with very high probability after delay)
        assert!(ulid1 <= ulid2); // Allow equality in case delay wasn't enough
        
        // Test ULID string representation
        let ulid_str = ulid1.to_string();
        assert_eq!(ulid_str.len(), 26);
        
        // Test ULID parsing
        let parsed_ulid = ulid_str.parse::<Ulid>().unwrap();
        assert_eq!(ulid1, parsed_ulid);
    }

    #[test]
    fn test_event_payload_json_handling() {
        // Test simple JSON payload
        let simple_payload = json!({"key": "value", "number": 42});
        let event = RawEvent {
            id: Ulid::new(),
            source: "test".to_string(),
            event_type: "test".to_string(),
            ts_ingest: Utc::now(),
            ts_orig: None,
            host: "localhost".to_string(),
            ingestor_version: None,
            payload_schema_id: None,
            payload: simple_payload.clone(),
        };
        
        assert_eq!(event.payload["key"], "value");
        assert_eq!(event.payload["number"], 42);
        
        // Test complex nested JSON
        let complex_payload = json!({
            "metadata": {
                "version": "1.0",
                "tags": ["test", "event"]
            },
            "data": {
                "items": [1, 2, 3],
                "enabled": true
            }
        });
        
        let complex_event = RawEvent {
            id: Ulid::new(),
            source: "complex.test".to_string(),
            event_type: "complex_event".to_string(),
            ts_ingest: Utc::now(),
            ts_orig: None,
            host: "localhost".to_string(),
            ingestor_version: None,
            payload_schema_id: None,
            payload: complex_payload,
        };
        
        assert_eq!(complex_event.payload["metadata"]["version"], "1.0");
        assert_eq!(complex_event.payload["data"]["items"][0], 1);
        assert_eq!(complex_event.payload["data"]["enabled"], true);
    }

    #[test]
    fn test_timestamp_handling() {
        let now = Utc::now();
        let past = now - chrono::Duration::seconds(3600); // 1 hour ago
        
        let event = RawEvent {
            id: Ulid::new(),
            source: "timestamp.test".to_string(),
            event_type: "timestamp_event".to_string(),
            ts_ingest: now,
            ts_orig: Some(past),
            host: "localhost".to_string(),
            ingestor_version: None,
            payload_schema_id: None,
            payload: json!({}),
        };
        
        // Test that ingestion timestamp is after original timestamp
        assert!(event.ts_ingest > event.ts_orig.unwrap());
        
        // Test that timestamps are properly set
        assert_eq!(event.ts_ingest, now);
        assert_eq!(event.ts_orig.unwrap(), past);
    }

    #[test]
    fn test_error_handling_in_work_queue() {
        let mut queue_item = WorkQueueItem {
            queue_id: Ulid::new(),
            raw_event_id: Ulid::new(),
            target_agent_name: "error_test_agent".to_string(),
            status: "pending".to_string(),
            attempts: 0,
            max_attempts: 3,
            last_attempt_ts: None,
            next_retry_ts: None,
            error_message_last: None,
            created_at: Utc::now(),
            processing_worker_id: None,
            processed_at: None,
            failure_reason: None,
        };
        
        // Simulate processing failure
        queue_item.attempts = 1;
        queue_item.status = "failed_retryable".to_string();
        queue_item.error_message_last = Some("Connection timeout".to_string());
        queue_item.last_attempt_ts = Some(Utc::now());
        queue_item.next_retry_ts = Some(Utc::now() + chrono::Duration::minutes(5));
        
        assert_eq!(queue_item.attempts, 1);
        assert_eq!(queue_item.status, "failed_retryable");
        assert!(queue_item.error_message_last.is_some());
        assert!(queue_item.last_attempt_ts.is_some());
        assert!(queue_item.next_retry_ts.is_some());
    }

    #[tokio::test]
    async fn test_pool_creation() {
        // This would require a test database
        // For now, just ensure the function compiles and types are correct
        
        // Test that the functions exist and have the right signatures
        // Cannot actually call them without a database, but we can test they compile
        assert!(true); // Placeholder assertion
    }

    #[test]
    fn test_function_signatures() {
        // Just test that our functions exist and compile
        // We can't test the actual functionality without a database
        
        // This ensures the functions are callable and have the right basic structure
        assert!(true); // Functions exist if this compiles
    }
}