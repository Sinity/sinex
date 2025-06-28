//! Test database isolation through separate databases
//!
//! Each test gets its own database for perfect isolation.
//! This approach ensures complete test independence at the cost
//! of increased setup time and resource usage.
//!
//! # Usage
//! ```rust
//! let test_db = TestDatabase::create("my_test").await?;
//! // Use test_db.pool for database operations
//! // Database is automatically cleaned up on drop
//! ```

use crate::common::prelude::*;
use sqlx::postgres::PgConnection;
use sqlx::Connection;
use std::sync::atomic::{AtomicU32, Ordering};

static DB_COUNTER: AtomicU32 = AtomicU32::new(0);

/// A test database that provides complete isolation
pub struct TestDatabase {
    pub pool: DbPool,
    pub name: String,
    admin_url: String,
}

impl TestDatabase {
    /// Create a new test database
    pub async fn create(test_name: &str) -> Result<Self> {
        // Get admin connection URL (to main database)
        let base_url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgresql:///sinex_dev?host=/run/postgresql".to_string());

        // Parse and modify to connect to postgres database for admin operations
        let admin_url = base_url.replace("/sinex_dev", "/postgres");

        // Generate unique database name with safety checks
        let counter = DB_COUNTER.fetch_add(1, Ordering::SeqCst);
        let sanitized_name = test_name
            .replace("::", "_")
            .replace(" ", "_")
            .replace("-", "_")
            .replace(".", "_")
            .to_lowercase();
        let name = format!(
            "test_{}_{}_{}",
            sanitized_name.chars().take(20).collect::<String>(), // Limit length
            std::process::id(),
            counter
        );

        // Create the database
        let mut admin_conn = PgConnection::connect(&admin_url).await?;

        // Drop if exists (in case of previous failed test)
        let drop_query = format!("DROP DATABASE IF EXISTS {}", name);
        sqlx::query(&drop_query).execute(&mut admin_conn).await?;

        // Create fresh database
        let create_query = format!("CREATE DATABASE {}", name);
        sqlx::query(&create_query).execute(&mut admin_conn).await?;

        admin_conn.close().await?;

        // Connect to the new database
        let db_url = base_url.replace("/sinex_dev", &format!("/{}", name));
        let pool: DbPool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(5)
            .connect(&db_url)
            .await?;

        // Run migrations
        sinex_db::run_migrations(&pool).await?;

        Ok(TestDatabase {
            pool,
            name,
            admin_url,
        })
    }
}

impl Drop for TestDatabase {
    fn drop(&mut self) {
        // Schedule database cleanup
        let admin_url = self.admin_url.clone();
        let db_name = self.name.clone();

        // We can't do async in drop, so spawn a task
        tokio::spawn(async move {
            if let Ok(mut conn) = PgConnection::connect(&admin_url).await {
                // Force disconnect all connections with retry logic
                for attempt in 0..3 {
                    let disconnect_query = format!(
                        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{}' AND pid <> pg_backend_pid()",
                        db_name
                    );
                    let _ = sqlx::query(&disconnect_query).execute(&mut conn).await;

                    // Wait a bit for connections to close
                    tokio::time::sleep(std::time::Duration::from_millis(100 * (attempt + 1))).await;

                    // Try to drop the database
                    let drop_query = format!("DROP DATABASE IF EXISTS {}", db_name);
                    if sqlx::query(&drop_query).execute(&mut conn).await.is_ok() {
                        break;
                    }
                }

                let _ = conn.close().await;
            }
        });
    }
}

/// Utility functions for test database management
impl TestDatabase {
    /// Check if the database is healthy
    pub async fn check_health(&self) -> Result<bool> {
        match sqlx::query!("SELECT 1 as health_check")
            .fetch_one(&self.pool)
            .await
        {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }

    /// Get database statistics
    pub async fn get_stats(&self) -> Result<DatabaseStats> {
        let row = sqlx::query!(
            r#"
            SELECT
                (SELECT COUNT(*) FROM raw.events) as event_count,
                (SELECT COUNT(*) FROM sinex_schemas.agent_manifests) as agent_count,
                (SELECT COUNT(*) FROM sinex_schemas.work_queue) as work_queue_count
            "#
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(DatabaseStats {
            event_count: row.event_count.unwrap_or(0),
            agent_count: row.agent_count.unwrap_or(0),
            work_queue_count: row.work_queue_count.unwrap_or(0),
        })
    }
}

/// Database statistics for debugging
#[derive(Debug, Clone)]
pub struct DatabaseStats {
    pub event_count: i64,
    pub agent_count: i64,
    pub work_queue_count: i64,
}
