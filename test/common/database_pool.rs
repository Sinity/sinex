// Database pool management for tests
// Provides TestDatabase type and utilities for test database isolation

use crate::common::prelude::*;
use std::sync::Arc;
use tokio::sync::Mutex;

/// A test database handle that provides isolated database access for tests
pub struct TestDatabase {
    pool: DbPool,
    name: String,
}

impl TestDatabase {
    /// Get the underlying connection pool
    pub fn pool(&self) -> &DbPool {
        &self.pool
    }
    
    /// Get the database name
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// Acquire a test database for isolated testing
/// 
/// For now, this returns a shared test database pool.
/// In a full implementation, this would create isolated databases.
pub async fn acquire_test_database() -> AnyhowResult<TestDatabase> {
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql:///sinex_test?host=/run/postgresql".to_string());
    
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;
    
    Ok(TestDatabase {
        pool,
        name: "sinex_test".to_string(),
    })
}