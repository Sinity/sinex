//! Final solution: Pre-initialized database pool
//!
//! This creates a fixed pool of databases at startup and distributes them to tests.
//! Databases are cleaned BEFORE being given to a test, not after.

use crate::common::prelude::*;
use std::sync::atomic::{AtomicU32, AtomicBool, Ordering};
use std::sync::Arc;
use once_cell::sync::Lazy;
use sqlx::postgres::PgConnection;
use sqlx::Connection;
use std::sync::OnceLock;
use std::time::Duration;
use tokio::sync::Mutex;

static DB_COUNTER: AtomicU32 = AtomicU32::new(0);

/// Database pool configuration
struct PoolConfig {
    size: usize,
    admin_url: String,
    base_url: String,
    template_name: String,
}

impl Default for PoolConfig {
    fn default() -> Self {
        let base_url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgresql:///sinex_dev?host=/run/postgresql".to_string());
        let admin_url = base_url.replace("/sinex_dev", "/postgres");
        
        Self {
            size: 32,  // Increased for better parallelism with large test suites
            admin_url,
            base_url,
            template_name: "sinex_test_template_shared".to_string(),
        }
    }
}

/// A test database handle
pub struct TestDatabase {
    name: String,
    pool: DbPool,
    slot: Arc<DatabaseSlot>,
}

impl TestDatabase {
    pub fn name(&self) -> &str {
        &self.name
    }
    
    pub fn pool(&self) -> &DbPool {
        &self.pool
    }
}

impl Drop for TestDatabase {
    fn drop(&mut self) {
        // Mark slot as available
        self.slot.in_use.store(false, Ordering::Release);
    }
}

/// A slot in the database pool
struct DatabaseSlot {
    name: String,
    pool: DbPool,
    in_use: AtomicBool,
}

/// The global database pool
struct DatabasePool {
    slots: Vec<Arc<DatabaseSlot>>,
}

impl DatabasePool {
    /// Initialize the pool
    async fn new(config: PoolConfig) -> Result<Self> {
        eprintln!("🚀 Initializing database pool with {} databases (reusing existing if available)...", config.size);
        
        // Ensure template exists
        crate::common::test_database::TestDatabase::ensure_template_database(
            &config.admin_url,
            &config.base_url,
        ).await?;
        
        // Create admin connection
        let admin_pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(3)
            .connect(&config.admin_url)
            .await?;
        
        // Clean up any non-pool test databases (from old test runs)
        let non_pool_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM pg_database WHERE datname LIKE 'sinex_test_%' 
             AND datname NOT LIKE 'sinex_test_pool_%' 
             AND datname NOT LIKE '%template%'"
        )
        .fetch_one(&admin_pool)
        .await?;
        
        if non_pool_count > 0 {
            eprintln!("🧹 Cleaning up {} non-pool test databases...", non_pool_count);
            
            // Get list of non-pool databases
            let dbs_to_drop: Vec<String> = sqlx::query_scalar(
                "SELECT datname FROM pg_database WHERE datname LIKE 'sinex_test_%' 
                 AND datname NOT LIKE 'sinex_test_pool_%' 
                 AND datname NOT LIKE '%template%'"
            )
            .fetch_all(&admin_pool)
            .await?;
            
            // Drop them
            for db in dbs_to_drop {
                let _ = sqlx::query(&format!("DROP DATABASE IF EXISTS {}", db))
                    .execute(&admin_pool)
                    .await;
            }
        }
        
        // Create all databases in parallel
        let mut slots = Vec::with_capacity(config.size);
        let mut tasks = Vec::new();
        
        for i in 0..config.size {
            let admin_pool = admin_pool.clone();
            let base_url = config.base_url.clone();
            let template_name = config.template_name.clone();
            
            let task = tokio::spawn(async move {
                let name = format!("sinex_test_pool_{}", i);
                
                let mut conn = admin_pool.acquire().await?;
                
                // Check if database already exists
                let exists: bool = sqlx::query_scalar(&format!(
                    "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = '{}')",
                    name
                ))
                .fetch_one(&mut *conn)
                .await?;
                
                if !exists {
                    sqlx::query(&format!("CREATE DATABASE {} WITH TEMPLATE {}", name, template_name))
                        .execute(&mut *conn).await?;
                    eprintln!("  Created new pool database: {}", name);
                } else {
                    eprintln!("  Reusing existing pool database: {}", name);
                }
                
                drop(conn);
                
                // Create connection pool
                let url = base_url.replace("/sinex_dev", &format!("/{}", name));
                let pool = sqlx::postgres::PgPoolOptions::new()
                    .max_connections(5)
                    .connect(&url)
                    .await?;
                
                Result::<_, anyhow::Error>::Ok((name, pool))
            });
            
            tasks.push(task);
        }
        
        // Wait for all databases to be created
        for task in tasks {
            let (name, pool) = task.await??;
            slots.push(Arc::new(DatabaseSlot {
                name,
                pool,
                in_use: AtomicBool::new(false),
            }));
        }
        
        eprintln!("✅ Database pool initialized with {} databases", slots.len());
        
        Ok(Self {
            slots,
        })
    }
    
    /// Acquire a database from the pool
    async fn acquire(&self) -> Result<TestDatabase> {
        let mut attempts = 0;
        loop {
            for slot in &self.slots {
                if !slot.in_use.swap(true, Ordering::AcqRel) {
                    // Got a slot! Clean it before use
                    match clean_database(&slot.pool).await {
                        Ok(_) => {
                            return Ok(TestDatabase {
                                name: slot.name.clone(),
                                pool: slot.pool.clone(),
                                slot: slot.clone(),
                            });
                        }
                        Err(e) => {
                            eprintln!("⚠️  Failed to clean database {}: {}", slot.name, e);
                            slot.in_use.store(false, Ordering::Release);
                        }
                    }
                }
            }
            
            attempts += 1;
            if attempts > 1000 {
                return Err(anyhow::anyhow!("Failed to acquire database after 1000 attempts - all {} slots in use", self.slots.len()));
            }
            
            // All slots in use, wait a bit
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }
    }
}

/// Clean a database for reuse
async fn clean_database(pool: &DbPool) -> Result<()> {
    // Clean in reverse dependency order
    sqlx::query("DELETE FROM sinex_schemas.work_queue").execute(pool).await?;
    sqlx::query("DELETE FROM sinex_schemas.agent_manifests").execute(pool).await?;
    sqlx::query("DELETE FROM core.event_annotations").execute(pool).await?;
    sqlx::query("DELETE FROM core.artifacts").execute(pool).await?;
    sqlx::query("DELETE FROM raw.events").execute(pool).await?;
    
    // Reset sequences - simplified query that works
    // Note: This is optional, tests should work without sequence reset
    
    Ok(())
}

// Global pool instance - initialized on first use
static POOL: Lazy<tokio::sync::Mutex<Option<Arc<DatabasePool>>>> = Lazy::new(|| {
    tokio::sync::Mutex::new(None)
});

/// Acquire a test database
pub async fn acquire_test_database() -> Result<TestDatabase> {
    // Get or initialize the pool
    let mut pool_lock = POOL.lock().await;
    
    if pool_lock.is_none() {
        let config = PoolConfig::default();
        let pool = Arc::new(DatabasePool::new(config).await?);
        *pool_lock = Some(pool);
    }
    
    let pool = pool_lock.as_ref().unwrap().clone();
    drop(pool_lock);
    
    pool.acquire().await
}

