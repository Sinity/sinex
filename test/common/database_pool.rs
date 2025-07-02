//! Universal database pool system for Sinex tests
//!
//! This module provides a high-performance database pooling system that:
//! - Pre-creates test databases from template for instant availability
//! - Cleans databases with TRUNCATE CASCADE (not DROP) for speed
//! - Provides <5ms database acquisition and cleanup
//! - Handles pool exhaustion gracefully with automatic expansion
//!
//! All tests should use this system through the #[sinex_test] macro.

use crate::common::prelude::*;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::OnceLock;
use tokio::sync::{Mutex, Semaphore};
use anyhow::Result;
use sqlx::Connection;

/// Global database pool manager
static POOL_MANAGER: OnceLock<DatabasePoolManager> = OnceLock::new();

/// Counter for database naming
static DB_COUNTER: AtomicU32 = AtomicU32::new(0);

/// Statistics for monitoring
static STATS: PoolStatistics = PoolStatistics {
    total_created: AtomicUsize::new(0),
    total_acquisitions: AtomicUsize::new(0),
    total_cleanups: AtomicUsize::new(0),
    peak_usage: AtomicUsize::new(0),
    failed_cleanups: AtomicUsize::new(0),
};

/// Pool statistics for debugging and monitoring
struct PoolStatistics {
    total_created: AtomicUsize,
    total_acquisitions: AtomicUsize,
    total_cleanups: AtomicUsize,
    peak_usage: AtomicUsize,
    failed_cleanups: AtomicUsize,
}

/// Configuration for the database pool
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Minimum number of databases to maintain
    pub min_size: usize,
    /// Maximum number of databases to create
    pub max_size: usize,
    /// Connection string template
    pub base_url: String,
    /// Admin connection URL
    pub admin_url: String,
    /// Template database name
    pub template_name: String,
    /// Enable verbose logging
    pub verbose: bool,
}

impl Default for PoolConfig {
    fn default() -> Self {
        let base_url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgresql:///sinex_dev?host=/run/postgresql".to_string());
        let admin_url = base_url.replace("/sinex_dev", "/postgres");
        
        // Default to CPU count for optimal parallelism
        let cpu_count = num_cpus::get();
        let min_size = cpu_count; // One database per CPU for parallel test execution
        let max_size = (cpu_count * 2).min(48); // Allow bursting up to 2x CPUs, cap at 48
        
        Self {
            min_size,
            max_size,
            base_url,
            admin_url,
            template_name: "sinex_test_template_shared".to_string(),
            verbose: std::env::var("SINEX_TEST_VERBOSE").is_ok(),
        }
    }
}

/// Manager for the global database pool
pub(crate) struct DatabasePoolManager {
    /// Configuration
    config: PoolConfig,
    /// Available clean databases
    available: Mutex<VecDeque<TestDatabaseInfo>>,
    /// Databases currently in use
    in_use: Mutex<Vec<TestDatabaseInfo>>,
    /// Semaphore to limit total databases
    permits: Semaphore,
    /// Admin connection pool for management operations
    admin_pool: DbPool,
}

/// Information about a test database
#[derive(Debug, Clone)]
struct TestDatabaseInfo {
    /// Database name
    name: String,
    /// Connection URL
    url: String,
    /// Connection pool
    pool: DbPool,
    /// Last cleanup timestamp
    last_cleanup: std::time::Instant,
    /// Number of times used
    use_count: usize,
}

/// Handle to a database from the pool
pub struct PooledDatabase {
    info: TestDatabaseInfo,
    returned: bool,
}

impl PooledDatabase {
    /// Get the database connection pool
    pub fn pool(&self) -> &DbPool {
        &self.info.pool
    }
    
    /// Get the database name
    pub fn name(&self) -> &str {
        &self.info.name
    }
    
    /// Get usage statistics
    pub fn use_count(&self) -> usize {
        self.info.use_count
    }
}

impl Drop for PooledDatabase {
    fn drop(&mut self) {
        if !self.returned {
            // Return database to pool
            let info = self.info.clone();
            tokio::spawn(async move {
                if let Some(manager) = POOL_MANAGER.get() {
                    manager.return_database(info).await;
                }
            });
        }
    }
}

impl DatabasePoolManager {
    /// Cleanup old test databases at startup
    async fn cleanup_old_test_databases(admin_url: &str) -> Result<()> {
        eprintln!("🧹 Cleaning up old test databases...");
        
        let mut conn = sqlx::postgres::PgConnection::connect(admin_url).await?;
        
        // Get count of old test databases
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM pg_database WHERE datname LIKE 'sinex_test_%' AND datname NOT LIKE '%template%'"
        )
        .fetch_one(&mut conn)
        .await?;
        
        if count > 50 {  // Only cleanup if there are many
            eprintln!("   Found {} old test databases, cleaning up first 50...", count);
            
            // Get list of databases to drop - limit to 50 to avoid timeout
            let databases: Vec<String> = sqlx::query_scalar(
                "SELECT datname FROM pg_database WHERE datname LIKE 'sinex_test_%' AND datname NOT LIKE '%template%' LIMIT 50"
            )
            .fetch_all(&mut conn)
            .await?;
            
            let mut dropped = 0;
            for db_name in databases {
                // Terminate connections
                let _ = sqlx::query(&format!(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{}' AND pid <> pg_backend_pid()",
                    db_name
                ))
                .execute(&mut conn)
                .await;
                
                // Drop database
                if let Ok(_) = sqlx::query(&format!("DROP DATABASE IF EXISTS {}", db_name))
                    .execute(&mut conn)
                    .await
                {
                    dropped += 1;
                    if dropped % 100 == 0 {
                        eprintln!("   Dropped {} databases...", dropped);
                    }
                }
            }
            
            eprintln!("   Cleaned up {} test databases", dropped);
        }
        
        Ok(())
    }

    /// Initialize the global pool manager
    async fn initialize(config: PoolConfig) -> Result<Self> {
        eprintln!("🚀 Initializing database pool (size: {})", config.min_size);
        
        // Clean up old databases first - disabled during tests to avoid timeouts
        if std::env::var("SINEX_CLEANUP_OLD_DBS").unwrap_or_else(|_| "0".to_string()) == "1" {
            if let Err(e) = Self::cleanup_old_test_databases(&config.admin_url).await {
                eprintln!("⚠️  Failed to cleanup old databases: {}", e);
            }
        }
        
        // Create admin connection pool with timeout and retries
        let admin_pool = tokio::time::timeout(
            Duration::from_secs(10),
            sqlx::postgres::PgPoolOptions::new()
                .max_connections(5)
                .min_connections(1)
                .acquire_timeout(Duration::from_secs(5))
                .connect(&config.admin_url)
        ).await
        .map_err(|_| CoreError::database("Admin pool connection timeout").build())??;
        
        // Test admin connection
        sqlx::query("SELECT 1")
            .fetch_one(&admin_pool)
            .await
            .map_err(|e| CoreError::database("Admin pool health check failed").with_source(e).build())?;
        
        // Ensure template database exists with proper error handling
        match crate::common::test_database::TestDatabase::ensure_template_database(
            &config.admin_url,
            &config.base_url,
        ).await {
            Ok(_) => {},
            Err(e) => {
                eprintln!("❌ Failed to create template database: {}", e);
                eprintln!("   This usually means:");
                eprintln!("   - PostgreSQL extensions are missing (pgx_ulid, timescaledb, pg_jsonschema, pgvector)");
                eprintln!("   - PostgreSQL connection limits are exhausted");
                eprintln!("   - Database permissions are insufficient");
                eprintln!("");
                eprintln!("   Run ./check_postgresql_setup.sh to diagnose the issue.");
                return Err(e);
            }
        }
        
        let manager = Self {
            permits: Semaphore::new(config.max_size),
            config: config.clone(),
            available: Mutex::new(VecDeque::new()),
            in_use: Mutex::new(Vec::new()),
            admin_pool,
        };
        
        // Pre-create initial databases with better error handling
        let create_start = std::time::Instant::now();
        let mut initial_dbs = Vec::new();
        let mut failed_count = 0;
        
        for i in 0..config.min_size {
            match tokio::time::timeout(
                Duration::from_secs(5),
                manager.create_database(i)
            ).await {
                Ok(Ok(db_info)) => initial_dbs.push(db_info),
                Ok(Err(e)) => {
                    eprintln!("⚠️  Failed to create initial database {}: {}", i, e);
                    failed_count += 1;
                }
                Err(_) => {
                    eprintln!("⚠️  Timeout creating initial database {}", i);
                    failed_count += 1;
                }
            }
        }
        
        // Ensure we have at least some databases
        if initial_dbs.is_empty() {
            return Err(anyhow::anyhow!(
                "Failed to create any test databases. Check PostgreSQL connection and permissions."
            ));
        }
        
        // Add to available pool
        {
            let mut available = manager.available.lock().await;
            for db in initial_dbs {
                available.push_back(db);
            }
        }
        
        let elapsed = create_start.elapsed();
        let created_count = config.min_size - failed_count;
        eprintln!(
            "✅ Database pool initialized with {} databases in {:?} ({:.1?} per db)",
            created_count,
            elapsed,
            elapsed / created_count.max(1) as u32
        );
        
        if failed_count > 0 {
            eprintln!(
                "⚠️  Warning: {} databases failed to create. Pool will expand on demand.",
                failed_count
            );
        }
        
        Ok(manager)
    }
    
    /// Try to reuse an existing clean database
    async fn try_reuse_existing_database(&self) -> Result<Option<TestDatabaseInfo>> {
        // Look for existing test databases that might be reusable
        let mut admin_conn = self.admin_pool.acquire().await?;
        
        // Find a candidate database
        let candidate: Option<String> = sqlx::query_scalar(
            "SELECT datname FROM pg_database 
             WHERE datname LIKE 'sinex_test_%' 
             AND datname NOT LIKE '%template%'
             AND NOT EXISTS (
                 SELECT 1 FROM pg_stat_activity 
                 WHERE pg_stat_activity.datname = pg_database.datname
                 AND pid <> pg_backend_pid()
             )
             LIMIT 1"
        )
        .fetch_optional(&mut *admin_conn)
        .await?;
        
        drop(admin_conn);
        
        if let Some(db_name) = candidate {
            // Try to connect and verify it's clean
            let url = self.config.base_url.replace("/sinex_dev", &format!("/{}", db_name));
            
            match sqlx::postgres::PgPoolOptions::new()
                .max_connections(5)
                .min_connections(1)
                .acquire_timeout(Duration::from_secs(2))
                .connect(&url)
                .await
            {
                Ok(pool) => {
                    // Verify it's clean by checking event count
                    let event_count = sqlx::query_scalar::<_, i64>(
                        "SELECT COUNT(*) FROM raw.events"
                    )
                    .fetch_one(&pool)
                    .await
                    .ok();
                    
                    if event_count == Some(0) {
                        // Clean database, reuse it!
                        return Ok(Some(TestDatabaseInfo {
                            name: db_name,
                            url,
                            pool,
                            last_cleanup: std::time::Instant::now(),
                            use_count: 0,
                        }));
                    } else {
                        // Has data, clean it
                        if let Ok(_) = self.clean_database(&TestDatabaseInfo {
                            name: db_name.clone(),
                            url: url.clone(),
                            pool: pool.clone(),
                            last_cleanup: std::time::Instant::now(),
                            use_count: 0,
                        }).await {
                            return Ok(Some(TestDatabaseInfo {
                                name: db_name,
                                url,
                                pool,
                                last_cleanup: std::time::Instant::now(),
                                use_count: 0,
                            }));
                        }
                    }
                }
                Err(_) => {
                    // Connection failed, database might be corrupted
                }
            }
        }
        
        Ok(None)
    }
    
    /// Create a new test database
    async fn create_database(&self, index: usize) -> Result<TestDatabaseInfo> {
        // Try to reuse an existing clean database first
        if let Some(existing_db) = self.try_reuse_existing_database().await? {
            if self.config.verbose {
                eprintln!("  Reusing existing database: {}", existing_db.name);
            }
            return Ok(existing_db);
        }
        
        // Otherwise create a new one
        let counter = DB_COUNTER.fetch_add(1, Ordering::SeqCst);
        let name = format!("sinex_test_{}_{}", std::process::id(), counter);
        
        if self.config.verbose {
            eprintln!("  Creating database {} (index {})", name, index);
        }
        
        // Create from template
        let mut admin_conn = self.admin_pool.acquire().await?;
        
        // Drop if exists (cleanup from previous runs)
        sqlx::query(&format!("DROP DATABASE IF EXISTS {}", name))
            .execute(&mut *admin_conn)
            .await?;
        
        // Create from template
        sqlx::query(&format!(
            "CREATE DATABASE {} WITH TEMPLATE {}",
            name, self.config.template_name
        ))
        .execute(&mut *admin_conn)
        .await?;
        
        drop(admin_conn);
        
        // Create connection pool for this database
        let url = self.config.base_url.replace("/sinex_dev", &format!("/{}", name));
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(5)
            .min_connections(1)
            .acquire_timeout(Duration::from_secs(5))
            .connect(&url)
            .await?;
        
        // Apply session optimizations
        // Note: apply_test_session_optimizations is private, but we can apply them directly
        if std::env::var("SINEX_TEST_OPTIMIZATIONS").is_ok() {
            let optimizations = vec![
                "SET work_mem = '64MB'",
                "SET maintenance_work_mem = '256MB'", 
                "SET synchronous_commit = off",
                "SET random_page_cost = 1.1",
                "SET effective_cache_size = '1GB'",
                "SET temp_buffers = '32MB'",
                "SET statement_timeout = '30s'",  // Prevent runaway queries in tests
            ];
            
            for setting in optimizations {
                let _ = sqlx::query(setting).execute(&pool).await;
            }
        }
        
        STATS.total_created.fetch_add(1, Ordering::Relaxed);
        
        Ok(TestDatabaseInfo {
            name,
            url,
            pool,
            last_cleanup: std::time::Instant::now(),
            use_count: 0,
        })
    }
    
    /// Acquire a database from the pool
    pub async fn acquire(&self) -> Result<PooledDatabase> {
        let acquire_start = std::time::Instant::now();
        STATS.total_acquisitions.fetch_add(1, Ordering::Relaxed);
        
        // Try to get an available database
        let mut db_info = {
            let mut available = self.available.lock().await;
            available.pop_front()
        };
        
        // If none available, try to create a new one
        if db_info.is_none() {
            // Check if we can create more
            if let Ok(permit) = self.permits.try_acquire() {
                // We got a permit, create a new database
                permit.forget(); // Don't return it, we're expanding the pool
                
                if self.config.verbose {
                    eprintln!("📈 Expanding pool - no databases available");
                }
                
                match self.create_database(STATS.total_created.load(Ordering::Relaxed)).await {
                    Ok(new_db) => db_info = Some(new_db),
                    Err(e) => {
                        eprintln!("⚠️  Failed to expand pool: {}", e);
                        // Return the permit since we failed
                        self.permits.add_permits(1);
                    }
                }
            }
        }
        
        // If still none, wait for one to become available
        if db_info.is_none() {
            if self.config.verbose {
                eprintln!("⏳ Waiting for available database...");
            }
            
            // Poll until one becomes available
            loop {
                tokio::time::sleep(Duration::from_millis(10)).await;
                
                let mut available = self.available.lock().await;
                if let Some(db) = available.pop_front() {
                    db_info = Some(db);
                    break;
                }
            }
        }
        
        let mut info = db_info.unwrap();
        info.use_count += 1;
        
        // Verify database is healthy
        if let Err(e) = self.verify_database_health(&info).await {
            eprintln!("⚠️  Database {} failed health check: {}", info.name, e);
            // Try to recreate it
            match self.recreate_database(info).await {
                Ok(new_info) => info = new_info,
                Err(e) => return Err(e),
            }
        }
        
        // Track it as in-use
        {
            let mut in_use = self.in_use.lock().await;
            in_use.push(info.clone());
            
            let current_usage = in_use.len();
            let mut peak = STATS.peak_usage.load(Ordering::Relaxed);
            while current_usage > peak {
                match STATS.peak_usage.compare_exchange(
                    peak,
                    current_usage,
                    Ordering::Release,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(actual) => peak = actual,
                }
            }
        }
        
        let acquire_time = acquire_start.elapsed();
        if self.config.verbose || acquire_time > Duration::from_millis(10) {
            eprintln!(
                "🗄️  Acquired database {} (use #{}) in {:?}",
                info.name, info.use_count, acquire_time
            );
        }
        
        Ok(PooledDatabase {
            info,
            returned: false,
        })
    }
    
    /// Return a database to the pool
    async fn return_database(&self, mut info: TestDatabaseInfo) {
        let return_start = std::time::Instant::now();
        
        // Remove from in-use list
        {
            let mut in_use = self.in_use.lock().await;
            in_use.retain(|db| db.name != info.name);
        }
        
        // Clean the database
        match self.clean_database(&info).await {
            Ok(_) => {
                STATS.total_cleanups.fetch_add(1, Ordering::Relaxed);
                info.last_cleanup = std::time::Instant::now();
                
                // Return to available pool
                let mut available = self.available.lock().await;
                available.push_back(info);
                
                let cleanup_time = return_start.elapsed();
                if self.config.verbose || cleanup_time > Duration::from_millis(10) {
                    eprintln!("♻️  Returned database in {:?}", cleanup_time);
                }
            }
            Err(e) => {
                eprintln!("❌ Failed to clean database {}: {}", info.name, e);
                STATS.failed_cleanups.fetch_add(1, Ordering::Relaxed);
                
                // Drop the database since it's corrupted
                if let Err(e) = self.drop_database(&info).await {
                    eprintln!("❌ Failed to drop corrupted database: {}", e);
                }
                
                // Return the permit so a new database can be created
                self.permits.add_permits(1);
            }
        }
    }
    
    /// Clean a database using TRUNCATE CASCADE
    async fn clean_database(&self, info: &TestDatabaseInfo) -> Result<()> {
        // Get list of tables to truncate (in dependency order)
        let tables = vec![
            // Clean in reverse dependency order
            "sinex_schemas.work_queue",
            "sinex_schemas.agent_manifests",
            "core.event_annotations",
            "core.artifacts",
            "raw.events",
        ];
        
        // Execute TRUNCATE CASCADE for each table
        for table in tables {
            let query = format!("TRUNCATE TABLE {} CASCADE", table);
            if let Err(e) = sqlx::query(&query).execute(&info.pool).await {
                // Some tables might not exist in all test scenarios
                if self.config.verbose {
                    eprintln!("  Note: Could not truncate {}: {}", table, e);
                }
            }
        }
        
        // Reset sequences
        sqlx::query("SELECT setval(s.oid, 1, false) FROM pg_class c JOIN pg_sequence s ON c.oid = s.seqrelid WHERE c.relnamespace::regnamespace::text LIKE 'sinex_%'")
            .execute(&info.pool)
            .await?;
        
        // Vacuum to reclaim space (fast since tables are empty)
        sqlx::query("VACUUM ANALYZE").execute(&info.pool).await?;
        
        Ok(())
    }
    
    /// Verify database is healthy and connections work
    async fn verify_database_health(&self, info: &TestDatabaseInfo) -> Result<()> {
        // Quick health check query
        sqlx::query("SELECT 1")
            .fetch_one(&info.pool)
            .await
            .map_err(|e| CoreError::database("Health check failed").with_source(e).build())?;
        
        // Verify critical tables exist
        let critical_tables = ["raw.events", "sinex_schemas.work_queue"];
        for table in critical_tables {
            let parts: Vec<&str> = table.split('.').collect();
            let (schema, table_name) = (parts[0], parts[1]);
            
            let exists: bool = sqlx::query_scalar(
                "SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables 
                    WHERE table_schema = $1 AND table_name = $2
                )"
            )
            .bind(schema)
            .bind(table_name)
            .fetch_one(&info.pool)
            .await?;
            
            if !exists {
                return Err(anyhow::anyhow!("Critical table missing: {}", table));
            }
        }
        
        Ok(())
    }
    
    /// Recreate a corrupted database
    async fn recreate_database(&self, old_info: TestDatabaseInfo) -> Result<TestDatabaseInfo> {
        eprintln!("🔄 Recreating corrupted database {}", old_info.name);
        
        // Close connections
        old_info.pool.close().await;
        
        // Drop and recreate
        self.drop_database(&old_info).await?;
        self.create_database(old_info.use_count).await
    }
    
    /// Drop a database
    async fn drop_database(&self, info: &TestDatabaseInfo) -> Result<()> {
        let mut admin_conn = self.admin_pool.acquire().await?;
        
        // Force disconnect all connections
        let disconnect_query = format!(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity 
             WHERE datname = '{}' AND pid <> pg_backend_pid()",
            info.name
        );
        sqlx::query(&disconnect_query).execute(&mut *admin_conn).await?;
        
        // Drop the database
        tokio::time::sleep(Duration::from_millis(50)).await;
        sqlx::query(&format!("DROP DATABASE IF EXISTS {}", info.name))
            .execute(&mut *admin_conn)
            .await?;
        
        Ok(())
    }
    
    /// Get current pool statistics
    pub fn stats(&self) -> String {
        format!(
            "Pool Stats: created={}, acquisitions={}, cleanups={}, peak_usage={}, failed_cleanups={}",
            STATS.total_created.load(Ordering::Relaxed),
            STATS.total_acquisitions.load(Ordering::Relaxed),
            STATS.total_cleanups.load(Ordering::Relaxed),
            STATS.peak_usage.load(Ordering::Relaxed),
            STATS.failed_cleanups.load(Ordering::Relaxed),
        )
    }
}

/// Get or initialize the global database pool
pub async fn get_pool_manager() -> Result<&'static DatabasePoolManager> {
    if let Some(manager) = POOL_MANAGER.get() {
        return Ok(manager);
    }
    
    // Register cleanup hook on first use
    crate::common::cleanup_hook::register_cleanup_hook();
    
    // Initialize with default config
    let config = PoolConfig::default();
    let manager = DatabasePoolManager::initialize(config).await?;
    
    POOL_MANAGER.set(manager)
        .map_err(|_| CoreError::Other("Failed to initialize pool manager".to_string()))?;
    
    Ok(POOL_MANAGER.get().unwrap())
}

/// Acquire a database from the global pool
pub async fn acquire_database() -> Result<PooledDatabase> {
    let manager = get_pool_manager().await?;
    manager.acquire().await
}

/// Clean up the global pool (called at process exit)
pub async fn cleanup_global_pool() -> Result<()> {
    if let Some(manager) = POOL_MANAGER.get() {
        eprintln!("\n📊 Final {}", manager.stats());
        
        // Clean up all databases
        let (available, in_use) = {
            let available = manager.available.lock().await.clone();
            let in_use = manager.in_use.lock().await.clone();
            (available, in_use)
        };
        
        eprintln!("🧹 Cleaning up {} databases...", available.len() + in_use.len());
        
        for db in available.into_iter().chain(in_use.into_iter()) {
            if let Err(e) = manager.drop_database(&db).await {
                eprintln!("⚠️  Failed to drop {}: {}", db.name, e);
            }
        }
        
        // Also clean up template database
        crate::common::test_database::TestDatabase::cleanup_template_database().await?;
    }
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_pool_initialization() {
        let config = PoolConfig {
            min_size: 2,
            max_size: 4,
            ..Default::default()
        };
        
        let manager = DatabasePoolManager::initialize(config).await.unwrap();
        let available = manager.available.lock().await;
        assert_eq!(available.len(), 2);
    }
    
    #[tokio::test]
    async fn test_acquire_and_return() {
        let manager = get_pool_manager().await.unwrap();
        
        // Acquire a database
        let db = manager.acquire().await.unwrap();
        let name = db.name().to_string();
        
        // Verify we can use it
        sqlx::query("SELECT 1").execute(db.pool()).await.unwrap();
        
        // Return it
        drop(db);
        
        // Give it time to return
        tokio::time::sleep(Duration::from_millis(100)).await;
        
        // Verify it's back in the pool
        let available = manager.available.lock().await;
        assert!(available.iter().any(|db| db.name == name));
    }
}
