use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{PgPool, Row};
use std::time::Duration;
use tracing::{debug, info};
use uuid::Uuid;

use crate::config::DatabaseConfig;
use crate::error::{IngestorError, Result};

/// Database service for managing connections and operations
#[derive(Clone)]
pub struct DatabaseService {
    pool: PgPool,
    config: DatabaseConfig,
}

/// Represents an event to be stored in the database
#[derive(Debug, Clone)]
pub struct EventRecord {
    pub id: Uuid,
    pub source: String,
    pub ts_ingest: DateTime<Utc>,
    pub payload: Value,
    pub provenance: Value,
}

impl DatabaseService {
    /// Create a new database service with connection pool
    pub async fn new(config: DatabaseConfig) -> Result<Self> {
        info!("Connecting to database: {}", mask_database_url(&config.url));

        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(config.max_connections)
            .acquire_timeout(Duration::from_secs(config.connection_timeout_secs))
            .connect(&config.url)
            .await
            .map_err(|e| IngestorError::database_connection(format!(
                "Failed to connect to database: {}", e
            )))?;

        info!("Database connection established successfully");

        let service = Self { pool, config };
        service.verify_connection().await?;
        service.verify_schema().await?;

        Ok(service)
    }

    /// Verify database connection and retrieve version
    async fn verify_connection(&self) -> Result<()> {
        debug!("Verifying database connection");

        let row = sqlx::query("SELECT version()")
            .fetch_one(&self.pool)
            .await?;

        let version: String = row.get(0);
        info!("Connected to PostgreSQL: {}", version);

        Ok(())
    }

    /// Verify that the required schema exists
    async fn verify_schema(&self) -> Result<()> {
        debug!("Verifying database schema");

        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = 'raw' AND table_name = 'events')"
        )
        .fetch_one(&self.pool)
        .await?;

        if !exists {
            return Err(IngestorError::application(
                "Required table 'raw.events' does not exist. Please run schema migrations."
            ));
        }

        info!("Database schema verified successfully");
        Ok(())
    }

    /// Insert a single event into the database
    pub async fn insert_event(&self, event: EventRecord) -> Result<()> {
        debug!("Inserting event: id={}, source={}, type={}", 
               event.id, event.source, 
               event.payload.get("type").and_then(|v| v.as_str()).unwrap_or("unknown"));

        let _query_timeout = Duration::from_secs(self.config.query_timeout_secs);

        sqlx::query(
            r#"
            INSERT INTO raw.events (id, source, ts_ingest, payload, provenance)
            VALUES ($1, $2, $3, $4, $5)
            "#,
        )
        .bind(&event.id)
        .bind(&event.source)
        .bind(&event.ts_ingest)
        .bind(&event.payload)
        .bind(&event.provenance)
        .execute(&self.pool)
        .await
        .map_err(|e| IngestorError::event_ingestion(&event.source, e))?;

        debug!("Event inserted successfully: {}", event.id);
        Ok(())
    }

    /// Insert multiple events in a batch for better performance
    pub async fn insert_events_batch(&self, events: Vec<EventRecord>) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }

        info!("Inserting batch of {} events", events.len());

        let mut transaction = self.pool.begin().await?;

        for event in events {
            sqlx::query(
                r#"
                INSERT INTO raw.events (id, source, ts_ingest, payload, provenance)
                VALUES ($1, $2, $3, $4, $5)
                "#,
            )
            .bind(&event.id)
            .bind(&event.source)
            .bind(&event.ts_ingest)
            .bind(&event.payload)
            .bind(&event.provenance)
            .execute(&mut *transaction)
            .await?;
        }

        transaction.commit().await?;
        info!("Batch insert completed successfully");
        Ok(())
    }

    /// Get database statistics
    pub async fn get_stats(&self) -> Result<DatabaseStats> {
        debug!("Retrieving database statistics");

        let total_events: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM raw.events")
            .fetch_one(&self.pool)
            .await?;

        let sources: Vec<String> = sqlx::query_scalar("SELECT DISTINCT source FROM raw.events ORDER BY source")
            .fetch_all(&self.pool)
            .await?;

        let oldest_event: Option<DateTime<Utc>> = sqlx::query_scalar(
            "SELECT MIN(ts_ingest) FROM raw.events"
        )
        .fetch_optional(&self.pool)
        .await?;

        let newest_event: Option<DateTime<Utc>> = sqlx::query_scalar(
            "SELECT MAX(ts_ingest) FROM raw.events"
        )
        .fetch_optional(&self.pool)
        .await?;

        Ok(DatabaseStats {
            total_events,
            sources,
            oldest_event,
            newest_event,
        })
    }

    /// Health check for the database connection
    pub async fn health_check(&self) -> Result<()> {
        sqlx::query("SELECT 1")
            .fetch_one(&self.pool)
            .await?;
        Ok(())
    }

    /// Close the database connection pool
    pub async fn close(&self) {
        info!("Closing database connection pool");
        self.pool.close().await;
    }
}

/// Database statistics
#[derive(Debug)]
pub struct DatabaseStats {
    pub total_events: i64,
    pub sources: Vec<String>,
    pub oldest_event: Option<DateTime<Utc>>,
    pub newest_event: Option<DateTime<Utc>>,
}

impl EventRecord {
    /// Create a new event record
    pub fn new(
        source: impl Into<String>,
        payload: Value,
        provenance: Value,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            source: source.into(),
            ts_ingest: Utc::now(),
            payload,
            provenance,
        }
    }

    /// Create event record with custom timestamp
    pub fn with_timestamp(
        source: impl Into<String>,
        payload: Value,
        provenance: Value,
        timestamp: DateTime<Utc>,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            source: source.into(),
            ts_ingest: timestamp,
            payload,
            provenance,
        }
    }
}

/// Mask sensitive information in database URLs for logging
fn mask_database_url(url: &str) -> String {
    if let Ok(parsed) = url::Url::parse(url) {
        let mut masked = parsed.clone();
        if masked.password().is_some() {
            let _ = masked.set_password(Some("***"));
        }
        masked.to_string()
    } else {
        // If parsing fails, mask everything after :// until @
        if let Some(scheme_end) = url.find("://") {
            if let Some(at_pos) = url[scheme_end + 3..].find('@') {
                let scheme = &url[..scheme_end + 3];
                let after_auth = &url[scheme_end + 3 + at_pos..];
                format!("{}***{}", scheme, after_auth)
            } else {
                url.to_string()
            }
        } else {
            url.to_string()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mask_database_url() {
        assert_eq!(
            mask_database_url("postgresql://user:password@localhost/db"),
            "postgresql://user:***@localhost/db"
        );
        assert_eq!(
            mask_database_url("postgresql://localhost/db"),
            "postgresql://localhost/db"
        );
    }

    #[test]
    fn test_event_record_creation() {
        let payload = serde_json::json!({"type": "test", "data": "value"});
        let provenance = serde_json::json!({"source": "test"});
        
        let event = EventRecord::new("test-source", payload.clone(), provenance.clone());
        
        assert_eq!(event.source, "test-source");
        assert_eq!(event.payload, payload);
        assert_eq!(event.provenance, provenance);
    }
}