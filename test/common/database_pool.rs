//! Unified Database Pool for Test Isolation
//!
//! This is the single source of truth for database pool management in Sinex tests.
//! Features:
//! - Global, lazy static pool of pre-warmed, migrated databases
//! - PostgreSQL advisory locks for inter-process coordination
//! - Automatic cleanup on TestDatabase Drop
//! - High-performance architecture with 64 pre-warmed databases
//! - Clean-before-use strategy for optimal performance
//!
//! # Usage
//! ```rust
//! let test_db = acquire_test_database().await?;
//! // Use test_db.pool() for database operations
//! // Database automatically returns to pool on drop
//! ```

use crate::common::prelude::*;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use once_cell::sync::Lazy;
use std::time::{Duration, Instant};
use sqlx::postgres::PgConnection;
use sqlx::Connection;
use sinex_core::timeouts;
use parking_lot::Mutex;

static DB_COUNTER: AtomicU32 = AtomicU32::new(0);
static SLOT_COUNTER: AtomicUsize = AtomicUsize::new(0);

/// Pool performance metrics
static POOL_METRICS: Lazy<PoolMetrics> = Lazy::new(|| PoolMetrics::new());

/// Pool performance metrics for monitoring
struct PoolMetrics {
    acquisitions: AtomicUsize,
    total_wait_time: AtomicU64,
    cleanup_failures: AtomicUsize,
    template_recreations: AtomicUsize,
}

impl PoolMetrics {
    fn new() -> Self {
        Self {
            acquisitions: AtomicUsize::new(0),
            total_wait_time: AtomicU64::new(0),
            cleanup_failures: AtomicUsize::new(0),
            template_recreations: AtomicUsize::new(0),
        }
    }
    
    fn record_acquisition(&self, wait_time: Duration) {
        self.acquisitions.fetch_add(1, Ordering::Relaxed);
        self.total_wait_time.fetch_add(wait_time.as_millis() as u64, Ordering::Relaxed);
    }
    
    fn record_cleanup_failure(&self) {
        self.cleanup_failures.fetch_add(1, Ordering::Relaxed);
    }
    
    fn record_template_recreation(&self) {
        self.template_recreations.fetch_add(1, Ordering::Relaxed);
    }
    
    fn get_stats(&self) -> PoolStats {
        let acquisitions = self.acquisitions.load(Ordering::Relaxed);
        let total_wait = self.total_wait_time.load(Ordering::Relaxed);
        
        PoolStats {
            total_acquisitions: acquisitions,
            average_wait_time_ms: if acquisitions > 0 { total_wait / acquisitions as u64 } else { 0 },
            cleanup_failures: self.cleanup_failures.load(Ordering::Relaxed),
            template_recreations: self.template_recreations.load(Ordering::Relaxed),
        }
    }
}

/// Pool statistics for monitoring
#[derive(Debug, Clone)]
pub struct PoolStats {
    pub total_acquisitions: usize,
    pub average_wait_time_ms: u64,
    pub cleanup_failures: usize,
    pub template_recreations: usize,
}

/// Get current pool statistics
pub fn get_pool_stats() -> PoolStats {
    POOL_METRICS.get_stats()
}

/// Template database name cached for the current test process  
static TEMPLATE_DB_NAME: OnceLock<String> = OnceLock::new();

/// Mutex to ensure only one thread creates the template database
use lazy_static::lazy_static;

lazy_static! {
    static ref TEMPLATE_CREATION_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::new(());
}

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
            size: 64,  // Large pool to minimize contention on high-core systems
            admin_url,
            base_url,
            template_name: "sinex_test_template_shared".to_string(),
        }
    }
}

/// Pool configuration with customizable parameters
impl PoolConfig {
    /// Create config with custom pool size
    pub fn with_size(size: usize) -> Self {
        let mut config = Self::default();
        config.size = size;
        config
    }
    
    /// Create config with custom template name
    pub fn with_template(template_name: &str) -> Self {
        let mut config = Self::default();
        config.template_name = template_name.to_string();
        config
    }
}

/// A test database handle that automatically returns to pool on Drop
/// This is the primary interface for test database access
pub struct TestDatabase {
    name: String,
    pool: DbPool,
    slot: Arc<DatabaseSlot>,
    lock_id: i64, // Store advisory lock ID for cleanup
    acquired_at: Instant,
    acquisition_process_id: u32,
}

impl TestDatabase {
    /// Get the database name
    pub fn name(&self) -> &str {
        &self.name
    }
    
    /// Get the database pool for operations
    pub fn pool(&self) -> &DbPool {
        &self.pool
    }
    
    /// Get acquisition timestamp for diagnostics
    pub fn acquired_at(&self) -> Instant {
        self.acquired_at
    }
    
    /// Get the process ID that acquired this database
    pub fn acquisition_process_id(&self) -> u32 {
        self.acquisition_process_id
    }
    
    /// Check if the database is healthy
    pub async fn check_health(&self) -> Result<bool> {
        match sqlx::query("SELECT 1 as health_check")
            .fetch_one(&self.pool)
            .await
        {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }
    
    /// Get database statistics for debugging
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
    
    /// Force cleanup of this database (for testing)
    pub async fn force_cleanup(&self) -> Result<()> {
        clean_database(&self.pool, &self.name).await
    }
}

/// Database statistics for debugging
#[derive(Debug, Clone)]
pub struct DatabaseStats {
    pub event_count: i64,
    pub agent_count: i64,
    pub work_queue_count: i64,
}

impl Drop for TestDatabase {
    fn drop(&mut self) {
        // Release the PostgreSQL advisory lock
        let lock_id = self.lock_id;
        let pool_clone = self.pool.clone();
        
        eprintln!("🔓 Releasing database slot: {} (lock_id: {})", self.name, lock_id);
        
        // We need to release the advisory lock before closing the pool
        // Use a blocking task to handle this
        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                // Try to release the advisory lock with a short timeout
                match tokio::time::timeout(
                    timeouts::DEFAULT_TERMINAL_POLL_INTERVAL,
                    sqlx::query("SELECT pg_advisory_unlock($1)")
                        .bind(lock_id)
                        .execute(&pool_clone)
                ).await {
                    Ok(Ok(_)) => eprintln!("✅ Released advisory lock {}", lock_id),
                    Ok(Err(e)) => eprintln!("⚠️  Failed to release advisory lock {}: {}", lock_id, e),
                    Err(_) => eprintln!("⚠️  Timeout releasing advisory lock {} (pool shutting down)", lock_id),
                }
                
                // Then close the pool
                pool_clone.close().await;
                tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
            });
        }).join().unwrap_or_else(|_| eprintln!("⚠️  Cleanup thread panicked"));
        
        // Clear the pool reference
        let mut pool_opt = self.slot.pool.lock();
        *pool_opt = None;
        
        // Record when this slot was released
        {
            let mut last_released = self.slot.last_released.lock();
            *last_released = Some(std::time::Instant::now());
        }
        
        // Mark as not in use (for intra-process coordination)
        self.slot.in_use.store(false, Ordering::Release);
    }
}

/// A slot in the database pool
struct DatabaseSlot {
    name: String,
    url: String,  // Store URL instead of pool to create fresh connections
    pool: Mutex<Option<DbPool>>,  // Current pool if in use
    in_use: AtomicBool,
    // Track when the slot was acquired to help debug issues
    last_acquired: Mutex<Option<std::time::Instant>>,
    // Track when the slot was released for cooldown
    last_released: Mutex<Option<std::time::Instant>>,
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
        ensure_template_database(
            &config.admin_url,
            &config.base_url,
        ).await?;
        
        // Create admin connection
        let admin_pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(10)  // Increased for parallel database creation
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
                
                // Store URL for later pool creation
                let url = base_url.replace("/sinex_dev", &format!("/{}", name));
                
                Result::<_, anyhow::Error>::Ok((name, url))
            });
            
            tasks.push(task);
        }
        
        // Wait for all databases to be created
        for task in tasks {
            let (name, url) = task.await??;
            slots.push(Arc::new(DatabaseSlot {
                name,
                url,
                pool: Mutex::new(None),
                in_use: AtomicBool::new(false),
                last_acquired: Mutex::new(None),
                last_released: Mutex::new(None),
            }));
        }
        
        eprintln!("✅ Database pool initialized with {} databases", slots.len());
        
        Ok(Self {
            slots,
        })
    }
    
    /// Acquire a database from the pool
    async fn acquire(&self) -> Result<TestDatabase> {
        let start_time = std::time::Instant::now();
        let mut attempts = 0;
        
        // Use process ID and random offset to reduce contention
        let pid = std::process::id();
        let random_offset = rand::random::<usize>();
        let start_index = (pid as usize + random_offset) % self.slots.len();
        eprintln!("🎲 Process {} starting from index: {}", pid, start_index);
        
        // We need to try to acquire databases with PostgreSQL advisory locks
        // to ensure inter-process coordination
        loop {
            // Iterate through slots starting from our position
            for i in 0..self.slots.len() {
                let slot_index = (start_index + i) % self.slots.len();
                let slot = &self.slots[slot_index];
                
                // Try to connect to this database
                let pool = match sqlx::postgres::PgPoolOptions::new()
                    .max_connections(15)
                    .acquire_timeout(Duration::from_secs(2))  // Shorter timeout for faster iteration
                    .connect(&slot.url)
                    .await 
                {
                    Ok(pool) => pool,
                    Err(_) => continue, // Try next slot
                };
                
                // Try to acquire an advisory lock for this database
                // Use a unique lock ID based on the slot index
                let lock_id = 1000 + slot_index as i64;
                let lock_acquired: bool = sqlx::query_scalar(
                    "SELECT pg_try_advisory_lock($1)"
                )
                .bind(lock_id)
                .fetch_one(&pool)
                .await?;
                
                if !lock_acquired {
                    // Another process has this database, try next
                    pool.close().await;
                    continue;
                }
                
                // We got the lock! This database is ours for the duration of the test
                eprintln!("🔑 Process {} acquired database slot: {} with advisory lock {}", 
                          pid, slot.name, lock_id);
                
                // Store lock info in the slot for cleanup
                slot.in_use.store(true, Ordering::Release);
                {
                    let mut pool_opt = slot.pool.lock();
                    *pool_opt = Some(pool.clone());
                }
                
                // Clean it before use
                let clean_start = std::time::Instant::now();
                match clean_database(&pool, &slot.name).await {
                    Ok(_) => {
                        let clean_time = clean_start.elapsed();
                        if clean_time.as_millis() > 100 {
                            eprintln!("🔧 Database {} cleaned in {:.1?}", slot.name, clean_time);
                        }
                        
                        let acquisition_time = start_time.elapsed();
                        POOL_METRICS.record_acquisition(acquisition_time);
                        
                        return Ok(TestDatabase {
                            name: slot.name.clone(),
                            pool: pool.clone(),
                            slot: slot.clone(),
                            lock_id,
                            acquired_at: Instant::now(),
                            acquisition_process_id: pid,
                        });
                    }
                    Err(e) => {
                        eprintln!("⚠️  Failed to clean database {}: {}", slot.name, e);
                        POOL_METRICS.record_cleanup_failure();
                        
                        // Release the advisory lock
                        let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
                            .bind(lock_id)
                            .execute(&pool)
                            .await;
                        pool.close().await;
                        {
                            let mut pool_opt = slot.pool.lock();
                            *pool_opt = None;
                        }
                        slot.in_use.store(false, Ordering::Release);
                    }
                }
            }
            
            attempts += 1;
            if attempts > 100 {
                let total_time = start_time.elapsed();
                return Err(anyhow::anyhow!("Failed to acquire database after {} attempts ({:.1?})", attempts, total_time));
            }
            
            // Log warning after many attempts
            if attempts % 10 == 0 {
                let elapsed = start_time.elapsed();
                eprintln!("⚠️  Process {} waiting for database slot (attempt {}, {:.1?} elapsed)", pid, attempts, elapsed);
            }
            
            // All slots in use, wait a bit before retrying
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

/// Clean a database for reuse with comprehensive cleanup strategies
async fn clean_database(pool: &DbPool, db_name: &str) -> Result<()> {
    eprintln!("🧹 Cleaning database: {}", db_name);
    
    // First, disable FK checks for the cleanup session
    sqlx::query("SET session_replication_role = 'replica'")
        .execute(pool)
        .await?;
    
    // Try to use TRUNCATE for non-hypertables (much faster and more thorough)
    let truncate_result = sqlx::query(r#"
        TRUNCATE TABLE 
            core.event_annotations,
            core.event_artifact_refs,
            core.event_relations,
            core.event_cluster_members,
            core.artifact_event_sources,
            core.event_embeddings,
            core.entity_relations,
            core.artifact_embeddings,
            core.artifact_contents,
            core.artifact_tags,
            core.artifact_relations,
            core.entities,
            core.artifacts,
            core.event_clusters,
            sinex_schemas.work_queue,
            sinex_schemas.agent_manifests
        CASCADE
    "#)
    .execute(pool)
    .await;
    
    if let Err(e) = truncate_result {
        eprintln!("  ⚠️  TRUNCATE failed ({}), falling back to DELETE", e);
        
        // Fall back to DELETE in dependency order
        let delete_queries = [
            // First, delete from tables that reference raw.events
            "DELETE FROM core.event_annotations",
            "DELETE FROM core.event_artifact_refs",
            "DELETE FROM core.event_relations",
            "DELETE FROM core.event_cluster_members",
            "DELETE FROM core.artifact_event_sources",
            "DELETE FROM core.event_embeddings",
            
            // Delete from tables that reference entities
            "DELETE FROM core.entity_relations",
            
            // Delete from artifact-related tables
            "DELETE FROM core.artifact_embeddings",
            "DELETE FROM core.artifact_contents",
            "DELETE FROM core.artifact_tags",
            "DELETE FROM core.artifact_relations",
            
            // Delete from work queue
            "DELETE FROM sinex_schemas.work_queue",
            "DELETE FROM sinex_schemas.agent_manifests",
            
            // Finally, delete from primary tables
            "DELETE FROM core.entities",
            "DELETE FROM core.artifacts",
            "DELETE FROM core.event_clusters",
        ];
        
        for query in delete_queries {
            match sqlx::query(query).execute(pool).await {
                Ok(result) => {
                    let rows = result.rows_affected();
                    if rows > 0 {
                        let table_name = query.split_whitespace().nth(2).unwrap_or("unknown");
                        eprintln!("  🧹 Deleted {} rows from {}", rows, table_name);
                    }
                }
                Err(e) => {
                    let table_name = query.split_whitespace().nth(2).unwrap_or("unknown");
                    eprintln!("  ⚠️  Failed to delete from {}: {}", table_name, e);
                }
            }
        }
    } else {
        eprintln!("  ✅ Tables truncated successfully");
    }
    
    // Handle raw.events separately (hypertable cannot be truncated)
    match sqlx::query("DELETE FROM raw.events").execute(pool).await {
        Ok(result) => {
            let rows = result.rows_affected();
            if rows > 0 {
                eprintln!("  🧹 Deleted {} rows from raw.events", rows);
            }
        }
        Err(e) => {
            eprintln!("  ⚠️  Failed to delete from raw.events: {}", e);
            // Try TimescaleDB-specific cleanup
            match sqlx::query("SELECT drop_chunks('raw.events', older_than => INTERVAL '0 seconds')")
                .execute(pool)
                .await 
            {
                Ok(_) => eprintln!("  🧹 Dropped all chunks from raw.events"),
                Err(e2) => eprintln!("  ⚠️  Failed to drop chunks: {}", e2),
            }
        }
    }
    
    // Re-enable FK checks
    sqlx::query("SET session_replication_role = 'origin'")
        .execute(pool)
        .await?;
    
    // Verification with detailed output
    let verification_queries = [
        ("raw.events", "SELECT COUNT(*) FROM raw.events"),
        ("core.event_annotations", "SELECT COUNT(*) FROM core.event_annotations"),
        ("core.entities", "SELECT COUNT(*) FROM core.entities"),
        ("core.entity_relations", "SELECT COUNT(*) FROM core.entity_relations"),
        ("core.artifacts", "SELECT COUNT(*) FROM core.artifacts"),
        ("core.artifact_relations", "SELECT COUNT(*) FROM core.artifact_relations"),
        ("core.artifact_contents", "SELECT COUNT(*) FROM core.artifact_contents"),
    ];
    
    let mut all_clean = true;
    let mut remaining_counts = Vec::new();
    
    for (table_name, query) in verification_queries {
        let count: i64 = sqlx::query_scalar(query)
            .fetch_one(pool)
            .await
            .unwrap_or(0);
        
        if count > 0 {
            all_clean = false;
            remaining_counts.push((table_name, count));
        }
    }
    
    if !all_clean {
        eprintln!("  ❌ CRITICAL: Database {} not fully cleaned!", db_name);
        for (table, count) in &remaining_counts {
            eprintln!("     - {} has {} rows remaining", table, count);
        }
        
        // Try one final aggressive cleanup
        eprintln!("  🔧 Final cleanup attempt...");
        
        // Disable all constraints
        sqlx::query("SET session_replication_role = 'replica'")
            .execute(pool)
            .await?;
        
        // Use CASCADE DELETE on primary tables to force cleanup
        let cascade_queries = [
            "DELETE FROM raw.events CASCADE",
            "DELETE FROM core.entities CASCADE",
            "DELETE FROM core.artifacts CASCADE",
            "DELETE FROM core.event_clusters CASCADE",
        ];
        
        for query in cascade_queries {
            match sqlx::query(query).execute(pool).await {
                Ok(result) => {
                    if result.rows_affected() > 0 {
                        eprintln!("     🧹 CASCADE deleted from {}", 
                                 query.split_whitespace().nth(2).unwrap_or("unknown"));
                    }
                }
                Err(e) => {
                    // CASCADE might not be supported, try without
                    let non_cascade = query.replace(" CASCADE", "");
                    let _ = sqlx::query(&non_cascade).execute(pool).await;
                }
            }
        }
        
        // Re-enable constraints
        sqlx::query("SET session_replication_role = 'origin'")
            .execute(pool)
            .await?;
        
        // Final verification
        all_clean = true;
        for (table_name, query) in verification_queries {
            let count: i64 = sqlx::query_scalar(query)
                .fetch_one(pool)
                .await
                .unwrap_or(0);
            
            if count > 0 {
                all_clean = false;
                eprintln!("     ❌ {} STILL has {} rows!", table_name, count);
            }
        }
        
        if !all_clean {
            POOL_METRICS.record_cleanup_failure();
            return Err(anyhow::anyhow!(
                "Database {} cleanup failed - tables still contain data", 
                db_name
            ));
        }
    }
    
    eprintln!("  ✅ Database cleanup verified - all tables empty");
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

/// Ensure we have a template database with all migrations applied
/// This is created once per test process and reused for all test databases
async fn ensure_template_database(admin_url: &str, base_url: &str) -> Result<String> {
    // Check if we already have a template database cached
    if let Some(template_name) = TEMPLATE_DB_NAME.get() {
        return Ok(template_name.clone());
    }

    // Acquire lock to prevent race condition between parallel tests
    let _lock = TEMPLATE_CREATION_LOCK.lock().await;
    
    if let Some(template_name) = TEMPLATE_DB_NAME.get() {
        return Ok(template_name.clone());
    }

    // Create the template database name - use a shared name based on migrations hash
    // This allows multiple test processes to share the same template
    let template_name = "sinex_test_template_shared";
    
    eprintln!("🔧 Checking template database {} ...", template_name);
    let template_start = std::time::Instant::now();

    // Create template database with aggressive connection handling
    let admin_conn_future = async {
        let mut admin_conn = tokio::time::timeout(
            Duration::from_secs(5),
            PgConnection::connect(admin_url)
        ).await
        .map_err(|_| CoreError::database("Admin connection timeout").build())??;
        
        // Check if template already exists
        let exists: bool = sqlx::query_scalar(&format!(
            "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = '{}')",
            template_name
        ))
        .fetch_one(&mut admin_conn)
        .await?;
        
        if exists {
            eprintln!("✅ Template database already exists, reusing it");
            admin_conn.close().await?;
            return Ok::<bool, anyhow::Error>(false);  // false = no migrations needed
        }
        
        eprintln!("🔧 Creating template database {} (one-time setup)...", template_name);

        // First, aggressively terminate any existing connections to the template database
        let terminate_query = format!(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity 
             WHERE datname = '{}' AND pid <> pg_backend_pid()",
            template_name
        );
        let _ = sqlx::query(&terminate_query).execute(&mut admin_conn).await;
        
        // Wait a bit for connections to close
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Drop if exists (cleanup from previous runs) with CASCADE to force
        let drop_query = format!("DROP DATABASE IF EXISTS {} WITH (FORCE)", template_name);
        match sqlx::query(&drop_query).execute(&mut admin_conn).await {
            Ok(_) => {},
            Err(_) => {
                // Fallback to regular DROP if FORCE not supported
                let drop_query = format!("DROP DATABASE IF EXISTS {}", template_name);
                sqlx::query(&drop_query).execute(&mut admin_conn).await?;
            }
        }

        // Create fresh template database
        let create_query = format!("CREATE DATABASE {}", template_name);
        tokio::time::timeout(
            Duration::from_secs(10),
            sqlx::query(&create_query).execute(&mut admin_conn)
        ).await
        .map_err(|_| CoreError::database("Create database timeout").build())??;

        admin_conn.close().await?;
        Ok::<bool, anyhow::Error>(true)  // true = needs migrations
    };

    // Execute admin operations with timeout
    let needs_migrations = tokio::time::timeout(Duration::from_secs(20), admin_conn_future).await
        .map_err(|_| CoreError::database("Admin operations timeout").build())??;

    // If template already exists, we're done
    if !needs_migrations {
        // Cache the template name for future use
        TEMPLATE_DB_NAME.set(template_name.to_string())
            .map_err(|_| CoreError::Other("Failed to cache template database name".to_string()))?;
        return Ok(template_name.to_string());
    }
    
    // Track template recreation
    POOL_METRICS.record_template_recreation();

    // Connect to template database and run all migrations
    let template_url = base_url.replace("/sinex_dev", &format!("/{}", template_name));
    
    let template_pool_future = async {
        let template_pool: DbPool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(15)  // Increased for template database setup
            .min_connections(1)
            .max_lifetime(Duration::from_secs(300))
            .idle_timeout(Duration::from_secs(10))
            .acquire_timeout(Duration::from_secs(15))  // Increased for parallel template operations
            .connect(&template_url)
            .await?;

        // Apply test-specific optimizations for this session only
        apply_test_session_optimizations(&template_pool).await?;

        // Run all migrations on template (this is the expensive part, but only once!)
        eprintln!("  📋 Running migrations on template database...");
        
        // Check for required extensions first
        match check_required_extensions(&template_pool).await {
            Ok(_) => {},
            Err(e) => {
                eprintln!("❌ Missing required PostgreSQL extensions: {}", e);
                eprintln!("   Check NixOS PostgreSQL configuration and required extensions.");
                return Err(e);
            }
        }
        
        tokio::time::timeout(
            Duration::from_secs(30),
            sinex_db::run_migrations(&template_pool)
        ).await
        .map_err(|_| CoreError::database("Migration timeout - check if all required extensions are installed").build())??;
        
        // Optimize template for faster copying
        optimize_template_for_tests(&template_pool).await?;
        
        template_pool.close().await;
        Ok::<(), anyhow::Error>(())
    };

    // Execute template setup with timeout
    tokio::time::timeout(Duration::from_secs(45), template_pool_future).await
        .map_err(|_| CoreError::database("Template setup timeout").build())??;

    let template_elapsed = template_start.elapsed();
    eprintln!("✅ Template database created in {:?}", template_elapsed);

    // Cache the template name for future use
    TEMPLATE_DB_NAME.set(template_name.to_string())
        .map_err(|_| CoreError::Other("Failed to cache template database name".to_string()))?;

    Ok(template_name.to_string())
}

/// Check if required PostgreSQL extensions are available
async fn check_required_extensions(pool: &DbPool) -> Result<()> {
    let required_extensions = vec![
        ("ulid", "pgx_ulid for ULID primary keys"),
        ("timescaledb", "TimescaleDB for hypertable partitioning"),
        ("pg_jsonschema", "pg_jsonschema for JSON validation"),
        ("vector", "pgvector for vector similarity search"),
    ];
    
    let mut missing = Vec::new();
    
    for (ext_name, description) in required_extensions {
        let available: Option<String> = sqlx::query_scalar(
            "SELECT name FROM pg_available_extensions WHERE name = $1"
        )
        .bind(ext_name)
        .fetch_optional(pool)
        .await?;
        
        if available.is_none() {
            missing.push(format!("{} ({})", ext_name, description));
        }
    }
    
    if !missing.is_empty() {
        return Err(CoreError::database(
            format!("Missing required PostgreSQL extensions: {}", missing.join(", "))
        ).build().into());
    }
    
    Ok(())
}
    
/// Apply test-specific PostgreSQL optimizations (session-level only)
async fn apply_test_session_optimizations(pool: &DbPool) -> Result<()> {
    if std::env::var("SINEX_TEST_OPTIMIZATIONS").is_ok() {
        eprintln!("⚡ Applying test session optimizations...");
        
        // These settings only affect this session/connection, not the global server
        // NOTE: Only use session-level settings, not server-level ones
        let optimizations = vec![
            "SET work_mem = '64MB'",
            "SET maintenance_work_mem = '256MB'", 
            "SET synchronous_commit = off",
            "SET random_page_cost = 1.1",  // Assume SSD/fast storage for tests
            "SET effective_cache_size = '1GB'",
            "SET temp_buffers = '32MB'",
            "SET statement_timeout = '30s'",  // Prevent runaway queries
        ];
        
        for setting in optimizations {
            if let Err(e) = sqlx::query(setting).execute(pool).await {
                eprintln!("⚠️  Could not apply setting '{}': {}", setting, e);
            }
        }
    }
    Ok(())
}

/// Optimize template database for faster test copying
async fn optimize_template_for_tests(pool: &DbPool) -> Result<()> {
    eprintln!("🔧 Optimizing template database for test performance...");
    
    // Add a timeout to prevent hanging
    let optimization_future = async {
    
    // Drop unnecessary indexes that slow down copying
    let expensive_indexes = vec![
        // Vector indexes are expensive to copy
        "idx_artifact_embeddings_vector",
        "idx_event_embeddings_vector", 
        "idx_embedding_cache_vector",
        
        // Full-text search indexes
        "idx_artifacts_search",
        "idx_ai_content_search",
        
        // Complex multi-column indexes for test data
        "idx_event_annotations_complex",
        "idx_artifact_relations_complex",
    ];
    
    for index in expensive_indexes {
        let drop_sql = format!("DROP INDEX IF EXISTS {}", index);
        if let Err(e) = sqlx::query(&drop_sql).execute(pool).await {
            // Don't fail if index doesn't exist
            eprintln!("⚠️  Could not drop index {}: {}", index, e);
        }
    }
    
    // CRITICAL: Disable TimescaleDB continuous aggregate policies in tests
    // These consume all background workers and cause timeouts
    eprintln!("  🔧 Disabling TimescaleDB continuous aggregate policies...");
    let disable_policies_sql = r#"
        SELECT alter_job(job_id, scheduled => false) 
        FROM timescaledb_information.jobs 
        WHERE application_name LIKE '%Continuous Aggregate%'
           OR application_name LIKE '%Telemetry%'
    "#;
    
    if let Err(e) = sqlx::query(disable_policies_sql).execute(pool).await {
        eprintln!("  ⚠️  Could not disable TimescaleDB policies: {}", e);
    }
    
    // Disable autovacuum on template (tests don't need it)
    let disable_autovacuum_tables = vec![
        "raw.events",
        "core.artifacts", 
        "core.event_annotations",
        "sinex_schemas.work_queue",
    ];
    
    for table in disable_autovacuum_tables {
        let disable_sql = format!("ALTER TABLE {} SET (autovacuum_enabled = false)", table);
        if let Err(e) = sqlx::query(&disable_sql).execute(pool).await {
            eprintln!("⚠️  Could not disable autovacuum on {}: {}", table, e);
        }
    }
    
    // Set test-friendly table settings
    sqlx::query("ALTER TABLE raw.events SET (fillfactor = 100)")
        .execute(pool)
        .await
        .unwrap_or_else(|_| {
            eprintln!("⚠️  Could not set fillfactor on raw.events");
            Default::default()
        });
    
    // Clean up any test data that might have snuck in
    sqlx::query("DELETE FROM raw.events WHERE source LIKE 'test_%'")
        .execute(pool)
        .await
        .unwrap_or_else(|_| {
            eprintln!("⚠️  Could not clean test data");
            Default::default()
        });
    
        eprintln!("✅ Template database optimized for test performance");
        Ok::<(), anyhow::Error>(())
    };
    
    // Apply a reasonable timeout
    match tokio::time::timeout(Duration::from_secs(20), optimization_future).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(e),
        Err(_) => {
            eprintln!("⚠️  Template optimization timed out after 20s, continuing anyway");
            Ok(()) // Don't fail, optimizations are optional
        }
    }
}

/// Health check for the entire pool
pub async fn check_pool_health() -> Result<PoolHealthReport> {
    let pool_lock = POOL.lock().await;
    
    if let Some(pool) = pool_lock.as_ref() {
        let mut healthy_slots = 0;
        let mut unhealthy_slots = 0;
        let mut total_slots = 0;
        
        for slot in &pool.slots {
            total_slots += 1;
            
            if slot.in_use.load(Ordering::Acquire) {
                // Skip in-use slots
                continue;
            }
            
            // Try to connect to this slot's database
            match sqlx::postgres::PgPoolOptions::new()
                .max_connections(1)
                .acquire_timeout(Duration::from_secs(2))
                .connect(&slot.url)
                .await 
            {
                Ok(pool) => {
                    match sqlx::query("SELECT 1").fetch_one(&pool).await {
                        Ok(_) => healthy_slots += 1,
                        Err(_) => unhealthy_slots += 1,
                    }
                    pool.close().await;
                }
                Err(_) => unhealthy_slots += 1,
            }
        }
        
        Ok(PoolHealthReport {
            total_slots,
            healthy_slots,
            unhealthy_slots,
            stats: POOL_METRICS.get_stats(),
        })
    } else {
        Ok(PoolHealthReport {
            total_slots: 0,
            healthy_slots: 0,
            unhealthy_slots: 0,
            stats: POOL_METRICS.get_stats(),
        })
    }
}

/// Pool health report
#[derive(Debug, Clone)]
pub struct PoolHealthReport {
    pub total_slots: usize,
    pub healthy_slots: usize,
    pub unhealthy_slots: usize,
    pub stats: PoolStats,
}

/// Emergency pool reset function (for testing/debugging)
pub async fn reset_pool() -> Result<()> {
    let mut pool_lock = POOL.lock().await;
    
    if let Some(pool) = pool_lock.take() {
        // Close all connections
        for slot in &pool.slots {
            {
                let mut pool_opt = slot.pool.lock();
                if let Some(pool) = pool_opt.take() {
                    pool.close().await;
                }
            }
        }
    }
    
    // Force reinitialize on next acquisition
    *pool_lock = None;
    
    Ok(())
}

/// Initialize pool with custom configuration (for testing)
pub async fn init_pool_with_config(config: PoolConfig) -> Result<()> {
    let mut pool_lock = POOL.lock().await;
    let pool = Arc::new(DatabasePool::new(config).await?);
    *pool_lock = Some(pool);
    Ok(())
}

/// Get pool configuration (for debugging)
pub fn get_pool_config() -> PoolConfig {
    PoolConfig::default()
}

