use anyhow::{Context, Result};
use sqlx::postgres::{PgPool, PgPoolOptions};
use std::time::Duration;
use tracing::{debug, info};
use sinex_db::models::RawEvent;
use uuid::Uuid;

/// Database connection configuration
#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    pub url: String,
    pub max_connections: u32,
    pub min_connections: u32,
    pub acquire_timeout: Duration,
    pub idle_timeout: Duration,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            url: "postgresql://localhost/sinex".to_string(),
            max_connections: 10,
            min_connections: 2,
            acquire_timeout: Duration::from_secs(10),
            idle_timeout: Duration::from_secs(600),
        }
    }
}

/// Enhanced database service with retry logic and validation
pub struct DatabaseService {
    pool: PgPool,
    validator: Option<crate::validation::EventValidator>,
}

impl DatabaseService {
    /// Create a new database service
    pub async fn new(config: DatabaseConfig) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(config.max_connections)
            .min_connections(config.min_connections)
            .acquire_timeout(config.acquire_timeout)
            .idle_timeout(config.idle_timeout)
            .connect(&config.url)
            .await
            .context("Failed to create database connection pool")?;

        info!("Database connection pool established");
        Ok(Self { 
            pool,
            validator: Some(crate::validation::EventValidator::new()),
        })
    }

    /// Create from existing pool (useful for testing)
    pub fn from_pool(pool: PgPool) -> Self {
        Self { 
            pool,
            validator: Some(crate::validation::EventValidator::new()),
        }
    }
    
    /// Create without validation (for testing invalid events)
    pub fn from_pool_no_validation(pool: PgPool) -> Self {
        Self {
            pool,
            validator: None,
        }
    }
    
    /// Enable or disable validation
    pub fn set_validation(&mut self, enabled: bool) {
        self.validator = if enabled {
            Some(crate::validation::EventValidator::new())
        } else {
            None
        };
    }

    /// Get the underlying connection pool
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Insert a raw event into the database
    pub async fn insert_event(&self, event: &RawEvent) -> Result<Uuid> {
        // Validate event if validator is enabled
        if let Some(ref validator) = self.validator {
            validator.validate(&event.source, &event.event_type, &event.payload)
                .map_err(|e| anyhow::anyhow!("Event validation failed: {}", e))?;
        }
        
        // Let database generate the ULID
        let result = sqlx::query!(
            r#"
            INSERT INTO raw.events 
                (source, event_type, ts_orig, host, ingestor_version, 
                 payload_schema_id, payload)
            VALUES ($1, $2, $3, $4, $5, $6::uuid::ulid, $7)
            RETURNING id::uuid as "id!"
            "#,
            event.source,
            event.event_type,
            event.ts_orig,
            event.host,
            event.ingestor_version,
            event.payload_schema_id,
            event.payload
        )
        .fetch_one(&self.pool)
        .await
        .context("Failed to insert event")?;

        debug!(
            "Inserted event: {} {} (id: {})",
            event.source, event.event_type, result.id
        );

        Ok(result.id)
    }

    /// Insert multiple events in a batch
    pub async fn insert_events_batch(&self, events: &[RawEvent]) -> Result<Vec<Uuid>> {
        // Validate all events first if validator is enabled
        if let Some(ref validator) = self.validator {
            for (i, event) in events.iter().enumerate() {
                validator.validate(&event.source, &event.event_type, &event.payload)
                    .map_err(|e| anyhow::anyhow!("Event {} validation failed: {}", i, e))?;
            }
        }
        
        let mut tx = self.pool.begin().await?;
        let mut ids = Vec::new();

        for event in events {
            let result = sqlx::query!(
                r#"
                INSERT INTO raw.events 
                    (source, event_type, ts_orig, host, ingestor_version, 
                     payload_schema_id, payload)
                VALUES ($1, $2, $3, $4, $5, $6::uuid::ulid, $7)
                RETURNING id::uuid as "id!"
                "#,
                event.source,
                event.event_type,
                event.ts_orig,
                event.host,
                event.ingestor_version,
                event.payload_schema_id,
                event.payload
            )
            .fetch_one(&mut *tx)
            .await
            .context("Failed to insert event in batch")?;

            ids.push(result.id);
        }

        tx.commit().await?;

        debug!("Inserted batch of {} events", events.len());
        Ok(ids)
    }

    /// Health check
    pub async fn health_check(&self) -> Result<()> {
        sqlx::query!("SELECT 1 as check")
            .fetch_one(&self.pool)
            .await
            .context("Database health check failed")?;
        Ok(())
    }

    /// Close the database connection pool
    pub async fn close(&self) {
        self.pool.close().await;
        info!("Database connection pool closed");
    }
}

/// Retry configuration for database operations
#[derive(Debug, Clone)]
pub struct RetryConfig {
    pub max_retries: u32,
    pub initial_delay: Duration,
    pub max_delay: Duration,
    pub exponential_base: u32,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(5),
            exponential_base: 2,
        }
    }
}

/// Retry a database operation with exponential backoff
pub async fn retry_db_operation<F, Fut, T>(
    config: &RetryConfig,
    mut operation: F,
) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let mut delay = config.initial_delay;
    let mut last_error = None;

    for attempt in 0..=config.max_retries {
        match operation().await {
            Ok(result) => return Ok(result),
            Err(e) => {
                last_error = Some(e);
                
                if attempt < config.max_retries {
                    info!("Database operation failed (attempt {}), retrying...", attempt + 1);
                    tokio::time::sleep(delay).await;
                    
                    // Exponential backoff
                    delay = std::cmp::min(
                        delay * config.exponential_base,
                        config.max_delay
                    );
                }
            }
        }
    }

    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("No retry attempts were made")))
}