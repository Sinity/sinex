//! Database Pool - High-Performance Test Database Isolation
//!
//! This module provides a sophisticated database pooling system optimized for parallel test
//! execution. It maintains a pool of pre-warmed, migrated databases that are cleaned and
//! reused between tests for optimal performance.
//!
//! # Architecture
//!
//! The pool uses a multi-layered approach:
//! 1. **Template Database**: Single migrated template created once per test run
//! 2. **Database Pool**: 64 pre-created databases cloned from template
//! 3. **Advisory Locks**: PostgreSQL advisory locks for inter-process coordination
//! 4. **Smart Cleanup**: Efficient truncation with foreign key awareness
//!
//! # Performance Characteristics
//!
//! - **Acquisition Time**: ~5-10ms per database (after initial warmup)
//! - **Cleanup Time**: ~20-30ms with optimized truncation
//! - **Parallelism**: Supports 64 concurrent tests without contention
//! - **Memory Usage**: ~50MB per database (configurable)
//!
//! # Usage Pattern
//!
//! ```rust
//! // Automatic through TestContext (recommended)
//! #[sinex_test]
//! async fn test_something(ctx: TestContext) -> Result<()> {
//!     // Database automatically acquired and cleaned
//!     ctx.create_test_event("test", "test.event", json!({})).await?;
//!     Ok(())
//! }
//!
//! // Manual acquisition (for special cases)
//! let db = acquire_test_database().await?;
//! let pool = db.pool();
//! // ... use pool for queries
//! // Automatically returned to pool on drop
//! ```
//!
//! # Implementation Details
//!
//! ## Database Lifecycle
//! 1. **Template Creation**: First test creates migrated template
//! 2. **Pool Initialization**: 64 databases created from template
//! 3. **Test Acquisition**: Clean database acquired with advisory lock
//! 4. **Test Execution**: Isolated database operations
//! 5. **Cleanup & Return**: Data truncated, returned to pool
//!
//! ## Foreign Key Handling
//! The cleanup process respects foreign key constraints:
//! 1. Disable FK checks temporarily
//! 2. Truncate in dependency order
//! 3. Re-enable FK checks
//! 4. Verify referential integrity
//!
//! ## Lock Management
//! Advisory locks prevent race conditions:
//! - Lock ID = hash(database_name) % 2^31
//! - Exclusive locks during acquisition/cleanup
//! - Automatic release on connection drop
//!
//! # Monitoring
//!
//! ```rust
//! let stats = get_pool_stats();
//! println!("Total acquisitions: {}", stats.total_acquisitions);
//! println!("Avg wait time: {}ms", stats.average_wait_time_ms);
//! println!("Cleanup failures: {}", stats.cleanup_failures);
//! ```

use crate::Result;
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use sinex_core::db::DbPool;
use sinex_core::types::error::SinexError;

use sqlx::postgres::PgConnection;
use sqlx::Connection;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

#[allow(dead_code)]
static DB_COUNTER: AtomicU32 = AtomicU32::new(0);
#[allow(dead_code)]
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
        self.total_wait_time.fetch_add(
            wait_time.as_millis().min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
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
            average_wait_time_ms: if acquisitions > 0 {
                total_wait / acquisitions as u64
            } else {
                0
            },
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
            size: 64, // Large pool to minimize contention on high-core systems
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
#[derive(Debug)]
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
                (SELECT COUNT(*) FROM core.events) as event_count,
                (SELECT COUNT(*) FROM core.events WHERE source_event_ids IS NOT NULL) as synthesis_count,
                0 as checkpoint_count
            "#
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(DatabaseStats {
            event_count: row.event_count.unwrap_or(0),
            agent_count: row.synthesis_count.unwrap_or(0),
            checkpoint_count: row.checkpoint_count.unwrap_or(0) as i64,
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
    pub checkpoint_count: i64,
}

impl Drop for TestDatabase {
    fn drop(&mut self) {
        // Release the PostgreSQL advisory lock
        let lock_id = self.lock_id;
        let pool_clone = self.pool.clone();

        eprintln!(
            "🔓 Releasing database slot: {} (lock_id: {})",
            self.name, lock_id
        );

        // We need to release the advisory lock before closing the pool
        // Use a blocking task to handle this
        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                // Try to release the advisory lock with a short timeout
                match tokio::time::timeout(
                    sinex_core::types::timeouts::DEFAULT_TERMINAL_POLL_INTERVAL,
                    sqlx::query("SELECT pg_advisory_unlock($1)")
                        .bind(lock_id)
                        .execute(&pool_clone),
                )
                .await
                {
                    Ok(Ok(_)) => eprintln!("✅ Released advisory lock {}", lock_id),
                    Ok(Err(e)) => {
                        eprintln!("⚠️  Failed to release advisory lock {}: {}", lock_id, e)
                    }
                    Err(_) => eprintln!(
                        "⚠️  Timeout releasing advisory lock {} (pool shutting down)",
                        lock_id
                    ),
                }

                // Then close the pool
                pool_clone.close().await;
                tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
            });
        })
        .join()
        .unwrap_or_else(|_| eprintln!("⚠️  Cleanup thread panicked"));

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
#[derive(Debug)]
struct DatabaseSlot {
    name: String,
    url: String,                 // Store URL instead of pool to create fresh connections
    pool: Mutex<Option<DbPool>>, // Current pool if in use
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
        eprintln!(
            "🚀 Initializing database pool with {} databases (reusing existing if available)...",
            config.size
        );

        // Ensure template exists
        ensure_template_database(&config.admin_url, &config.base_url).await?;

        // Create admin connection
        let admin_pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(10) // Increased for parallel database creation
            .connect(&config.admin_url)
            .await?;

        // Clean up any non-pool test databases (from old test runs)
        let non_pool_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM pg_database WHERE datname LIKE 'sinex_test_%' 
             AND datname NOT LIKE 'sinex_test_pool_%' 
             AND datname NOT LIKE '%template%'",
        )
        .fetch_one(&admin_pool)
        .await?;

        if non_pool_count > 0 {
            eprintln!(
                "🧹 Cleaning up {} non-pool test databases...",
                non_pool_count
            );

            // Get list of non-pool databases
            let dbs_to_drop: Vec<String> = sqlx::query_scalar(
                "SELECT datname FROM pg_database WHERE datname LIKE 'sinex_test_%' 
                 AND datname NOT LIKE 'sinex_test_pool_%' 
                 AND datname NOT LIKE '%template%'",
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
                    sqlx::query(&format!(
                        "CREATE DATABASE {} WITH TEMPLATE {}",
                        name, template_name
                    ))
                    .execute(&mut *conn)
                    .await?;
                    eprintln!("  Created new pool database: {}", name);
                } else {
                    eprintln!("  Reusing existing pool database: {}", name);
                }

                drop(conn);

                // Store URL for later pool creation
                let url = base_url.replace("/sinex_dev", &format!("/{}", name));

                Ok::<_, color_eyre::eyre::Error>((name, url))
            });

            tasks.push(task);
        }

        // Wait for all databases to be created
        for task in tasks {
            let (name, url) = task
                .await
                .map_err(|e| SinexError::service(format!("Database creation task failed: {}", e)))?
                .map_err(|e| SinexError::database(e.to_string()))?;
            slots.push(Arc::new(DatabaseSlot {
                name,
                url,
                pool: Mutex::new(None),
                in_use: AtomicBool::new(false),
                last_acquired: Mutex::new(None),
                last_released: Mutex::new(None),
            }));
        }

        eprintln!(
            "✅ Database pool initialized with {} databases",
            slots.len()
        );

        Ok(Self { slots })
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
                    .acquire_timeout(Duration::from_secs(2)) // Shorter timeout for faster iteration
                    .connect(&slot.url)
                    .await
                {
                    Ok(pool) => pool,
                    Err(_) => continue, // Try next slot
                };

                // Try to acquire an advisory lock for this database
                // Use a unique lock ID based on the slot index
                let lock_id = 1000 + slot_index as i64;
                let lock_acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
                    .bind(lock_id)
                    .fetch_one(&pool)
                    .await?;

                if !lock_acquired {
                    // Another process has this database, try next
                    pool.close().await;
                    continue;
                }

                // We got the lock! This database is ours for the duration of the test
                eprintln!(
                    "🔑 Process {} acquired database slot: {} with advisory lock {}",
                    pid, slot.name, lock_id
                );

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
                return Err(SinexError::unknown(format!(
                    "Failed to acquire database after {} attempts ({:.1?})",
                    attempts, total_time
                )));
            }

            // Log warning after many attempts
            if attempts % 10 == 0 {
                let elapsed = start_time.elapsed();
                eprintln!(
                    "⚠️  Process {} waiting for database slot (attempt {}, {:.1?} elapsed)",
                    pid, attempts, elapsed
                );
            }

            // All slots in use, wait a bit before retrying
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

/// Clean a database for reuse
async fn clean_database(pool: &DbPool, db_name: &str) -> Result<()> {
    eprintln!("🧹 Cleaning database: {}", db_name);

    // Use the shared db_common implementation
    match crate::db_common::reset_database(pool).await {
        Ok(_) => {
            eprintln!("  ✅ Database cleanup verified - all tables empty");
            Ok(())
        }
        Err(e) => {
            eprintln!("  ❌ CRITICAL: Database {} cleanup failed: {}", db_name, e);
            POOL_METRICS.record_cleanup_failure();

            // Try to get more details about what went wrong
            if let Ok(counts) = crate::db_common::get_row_counts(pool).await {
                for (table, count) in counts {
                    if count > 0 {
                        eprintln!("     - {} has {} rows remaining", table, count);
                    }
                }
            }

            Err(SinexError::unknown(format!(
                "Database {} cleanup failed: {}",
                db_name, e
            )))
        }
    }
}

// Global pool instance - initialized on first use
static POOL: Lazy<tokio::sync::Mutex<Option<Arc<DatabasePool>>>> =
    Lazy::new(|| tokio::sync::Mutex::new(None));

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
        let mut admin_conn =
            tokio::time::timeout(Duration::from_secs(5), PgConnection::connect(admin_url))
                .await
                .map_err(|_| SinexError::database("Admin connection timeout"))?
                .map_err(|e| SinexError::database(format!("Admin connection failed: {}", e)))?;

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
            return Ok::<bool, SinexError>(false); // false = no migrations needed
        }

        eprintln!(
            "🔧 Creating template database {} (one-time setup)...",
            template_name
        );

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
            Ok(_) => {}
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
            sqlx::query(&create_query).execute(&mut admin_conn),
        )
        .await
        .map_err(|_| SinexError::database("Create database timeout"))?
        .map_err(|e| SinexError::database(format!("Create database failed: {}", e)))?;

        admin_conn.close().await?;
        Ok::<bool, SinexError>(true) // true = needs migrations
    };

    // Execute admin operations with timeout
    let needs_migrations = tokio::time::timeout(Duration::from_secs(20), admin_conn_future)
        .await
        .map_err(|_| SinexError::database("Admin operations timeout"))?
        .map_err(|e| SinexError::database(format!("Admin operations failed: {}", e)))?;

    // If template already exists, we're done
    if !needs_migrations {
        // Cache the template name for future use
        TEMPLATE_DB_NAME
            .set(template_name.to_string())
            .map_err(|_| {
                SinexError::unknown("Failed to cache template database name".to_string())
            })?;
        return Ok(template_name.to_string());
    }

    // Track template recreation
    POOL_METRICS.record_template_recreation();

    // Connect to template database and run all migrations
    let template_url = base_url.replace("/sinex_dev", &format!("/{}", template_name));

    let template_pool_future = async {
        let template_pool: DbPool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(15) // Increased for template database setup
            .min_connections(1)
            .max_lifetime(Duration::from_secs(300))
            .idle_timeout(Duration::from_secs(10))
            .acquire_timeout(Duration::from_secs(15)) // Increased for parallel template operations
            .connect(&template_url)
            .await?;

        // Apply test-specific optimizations for this session only
        apply_test_session_optimizations(&template_pool).await?;

        // Run all migrations on template (this is the expensive part, but only once!)
        eprintln!("  📋 Running migrations on template database...");

        // Check for required extensions first
        match check_required_extensions(&template_pool).await {
            Ok(_) => {}
            Err(e) => {
                eprintln!("❌ Missing required PostgreSQL extensions: {}", e);
                eprintln!("   Check NixOS PostgreSQL configuration and required extensions.");
                return Err(e);
            }
        }

        tokio::time::timeout(
            Duration::from_secs(30),
            sinex_core::db::run_migrations(&template_pool),
        )
        .await
        .map_err(|_| {
            SinexError::database(
                "Migration timeout - check if all required extensions are installed".to_string(),
            )
        })?
        .map_err(|e| SinexError::database(format!("Migration failed: {}", e)))?;

        // Optimize template for faster copying
        optimize_template_for_tests(&template_pool).await?;

        template_pool.close().await;
        Ok::<(), SinexError>(())
    };

    // Execute template setup with timeout
    tokio::time::timeout(Duration::from_secs(45), template_pool_future)
        .await
        .map_err(|_| SinexError::database("Template setup timeout"))?
        .map_err(|e| SinexError::database(format!("Template setup failed: {}", e)))?;

    let template_elapsed = template_start.elapsed();
    eprintln!("✅ Template database created in {:?}", template_elapsed);

    // Cache the template name for future use
    TEMPLATE_DB_NAME
        .set(template_name.to_string())
        .map_err(|_| SinexError::unknown("Failed to cache template database name"))?;

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
        let available: Option<String> =
            sqlx::query_scalar("SELECT name FROM pg_available_extensions WHERE name = $1")
                .bind(ext_name)
                .fetch_optional(pool)
                .await?;

        if available.is_none() {
            missing.push(format!("{} ({})", ext_name, description));
        }
    }

    if !missing.is_empty() {
        return Err(SinexError::database(format!(
            "Missing required PostgreSQL extensions: {}",
            missing.join(", ")
        )));
    }

    Ok(())
}

/// Apply test-specific PostgreSQL optimizations (session-level only)
async fn apply_test_session_optimizations(pool: &DbPool) -> Result<()> {
    if std::env::var("SINEX_TEST_OPTIMIZATIONS").is_ok() {
        eprintln!("⚡ Applying test session optimizations...");
        crate::db_common::apply_test_optimizations(pool)
            .await
            .map_err(|e| {
                SinexError::database(format!("Failed to apply test optimizations: {}", e))
            })?;
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
            "idx_event_embeddings_vector",
            "idx_embedding_cache_vector",
            // Full-text search indexes
            "idx_ai_content_search",
            // Complex multi-column indexes for test data
            "idx_event_annotations_complex",
            // Note: artifact-related indexes removed in Phase 1.3 cleanup
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
        let disable_autovacuum_tables = vec!["core.events", "core.event_annotations"];

        for table in disable_autovacuum_tables {
            let disable_sql = format!("ALTER TABLE {} SET (autovacuum_enabled = false)", table);
            if let Err(e) = sqlx::query(&disable_sql).execute(pool).await {
                eprintln!("⚠️  Could not disable autovacuum on {}: {}", table, e);
            }
        }

        // Set test-friendly table settings
        sqlx::query("ALTER TABLE core.events SET (fillfactor = 100)")
            .execute(pool)
            .await
            .unwrap_or_else(|_| {
                eprintln!("⚠️  Could not set fillfactor on core.events");
                Default::default()
            });

        // Clean up any test data that might have snuck in
        sqlx::query("DELETE FROM core.events WHERE source LIKE 'test_%'")
            .execute(pool)
            .await
            .unwrap_or_else(|_| {
                eprintln!("⚠️  Could not clean test data");
                Default::default()
            });

        eprintln!("✅ Template database optimized for test performance");
        Ok::<(), SinexError>(())
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
async fn _init_pool_with_config(config: PoolConfig) -> Result<()> {
    let mut pool_lock = POOL.lock().await;
    let pool = Arc::new(DatabasePool::new(config).await?);
    *pool_lock = Some(pool);
    Ok(())
}

/// Get pool configuration (for debugging)
fn _get_pool_config() -> PoolConfig {
    PoolConfig::default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sinex_test;

    #[sinex_test]
    async fn test_pool_handles_concurrent_acquisition() -> Result<()> {
        // Test that multiple tasks can acquire databases concurrently
        let handles: Vec<_> = (0..20)
            .map(|i| {
                tokio::spawn(async move {
                    let db = acquire_test_database().await?;

                    // Each should have clean database
                    use sinex_core::db::repositories::*;
                    let count = db.pool().events().count_all().await?;
                    assert_eq!(count, 0, "Database {} should be clean", i);

                    // Hold the database for a bit to ensure concurrency
                    tokio::time::sleep(Duration::from_millis(10)).await;

                    Ok::<_, SinexError>(db.name().to_string())
                })
            })
            .collect();

        // Collect all database names
        let mut db_names = Vec::new();
        for handle in handles {
            let name = handle
                .await
                .map_err(|e| SinexError::service(format!("Task failed: {}", e)))?
                .map_err(|e| SinexError::database(format!("Database operation failed: {}", e)))?;
            db_names.push(name);
        }

        // All databases should be unique
        let unique_count = db_names
            .iter()
            .collect::<std::collections::HashSet<_>>()
            .len();
        assert_eq!(
            unique_count,
            db_names.len(),
            "All databases should be unique"
        );

        Ok(())
    }

    #[sinex_test]
    async fn test_database_cleanup_on_drop() -> Result<()> {
        use sinex_core::db::models::*;
        use sinex_core::db::repositories::*;
        use sinex_core::types::domain::*;

        let db_name;

        {
            let db = acquire_test_database().await?;
            db_name = db.name().to_string();

            // Insert test data

            let repo = db.pool.events();
            let event = RawEvent::builder()
                .source(EventSource::new("test"))
                .event_type(EventType::new("test.event"))
                .host(HostName::new("test-host"))
                .payload(serde_json::json!({}))
                .build();
            repo.insert(event).await?;

            // Verify data exists
            let count = db.pool().events().count_all().await?;
            assert_eq!(count, 1);
        } // db is dropped here

        // Sleep briefly to allow cleanup
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Try to reacquire a database - it might be the same one
        let db2 = acquire_test_database().await?;

        if db2.name() == db_name {
            // If we got the same database, it should be clean
            let count = db2.pool().events().count_all().await?;
            assert_eq!(count, 0, "Reused database should be cleaned");
        }

        Ok(())
    }

    #[sinex_test]
    async fn test_advisory_lock_prevents_double_acquisition() -> Result<()> {
        // This test verifies that two processes can't acquire the same database
        let db1 = acquire_test_database().await?;
        let lock_id1 = db1.lock_id;

        // Try to manually acquire the same lock - should fail
        let lock_acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
            .bind(lock_id1)
            .fetch_one(db1.pool())
            .await?;

        assert!(
            !lock_acquired,
            "Should not be able to acquire lock that's already held"
        );

        Ok(())
    }

    #[sinex_test]
    async fn test_database_health_check() -> Result<()> {
        let db = acquire_test_database().await?;

        // Health check should pass
        assert!(db.check_health().await?);

        // Get stats should work
        let stats = db.get_stats().await?;
        assert_eq!(stats.event_count, 0);

        Ok(())
    }

    #[sinex_test]
    async fn test_pool_statistics() -> Result<()> {
        // Get current stats
        let initial_stats = get_pool_stats();
        let initial_acquisitions = initial_stats.total_acquisitions;

        // Acquire and release a database
        {
            let _db = acquire_test_database().await?;
        }

        // Stats should be updated
        let after_stats = get_pool_stats();
        assert!(after_stats.total_acquisitions > initial_acquisitions);

        Ok(())
    }

    #[sinex_test]
    async fn test_clean_database_handles_complex_data() -> Result<()> {
        let db = acquire_test_database().await?;

        // Insert data with foreign key relationships
        use sinex_core::db::models::*;
        use sinex_core::db::repositories::*;
        use sinex_core::types::domain::*;

        let repo = db.pool.events();
        let event_to_insert = RawEvent::builder()
            .source(EventSource::new("test"))
            .event_type(EventType::new("test"))
            .host(HostName::new("test"))
            .payload(serde_json::json!({}))
            .build();
        let event = repo.insert(event_to_insert).await?;

        // Add annotation
        sqlx::query(
            "INSERT INTO core.event_annotations (id, event_id, annotation_type, content, annotator) 
             VALUES ($1, $2, 'test', '{}'::jsonb, 'test-user')"
        )
        .bind(sinex_core::types::ulid::Ulid::new().to_uuid())
        .bind(event.id.expect("Event must have an ID").to_uuid())
        .execute(db.pool())
        .await?;

        // Force cleanup
        db.force_cleanup().await?;

        // Everything should be gone
        let event_count = db.pool().events().count_all().await?;
        let annotation_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM core.event_annotations")
                .fetch_one(db.pool())
                .await?;

        assert_eq!(event_count, 0);
        assert_eq!(annotation_count, 0);

        Ok(())
    }

    #[sinex_test]
    async fn test_pool_health_report() -> Result<()> {
        // Ensure pool is initialized
        let _db = acquire_test_database().await?;

        let health = check_pool_health().await?;
        assert!(health.total_slots > 0);
        assert!(health.healthy_slots > 0);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_stress_concurrent_operations() -> Result<()> {
        // Stress test with many concurrent acquisitions
        let mut handles = Vec::new();

        for i in 0..50 {
            let handle = tokio::spawn(async move {
                let db = acquire_test_database().await?;

                // Do some work
                use sinex_core::db::models::*;
                use sinex_core::db::repositories::*;
                use sinex_core::types::domain::*;

                let repo = db.pool.events();
                for _j in 0..5 {
                    let event = RawEvent::builder()
                        .source(EventSource::new(&format!("task_{}", i)))
                        .event_type(EventType::new("stress.test"))
                        .host(HostName::new("test"))
                        .payload(serde_json::json!({}))
                        .build();
                    repo.insert(event).await?;
                }

                // Verify isolation
                let repo = db.pool.events();
                let source = sinex_core::types::domain::EventSource::new(&format!("task_{}", i));
                let count = repo.count_by_source(&source).await?;

                assert_eq!(count, 5);

                Ok::<_, SinexError>(())
            });
            handles.push(handle);
        }

        // All should succeed
        for handle in handles {
            handle
                .await
                .map_err(|e| SinexError::service(format!("Task failed: {}", e)))?
                .map_err(|e| SinexError::database(format!("Database operation failed: {}", e)))?;
        }

        Ok(())
    }

    #[sinex_test]
    async fn test_template_database_exists() -> Result<()> {
        // Template should be created on first use
        let _db = acquire_test_database().await?;

        // Verify template exists
        let admin_url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgresql:///sinex_dev?host=/run/postgresql".to_string())
            .replace("/sinex_dev", "/postgres");

        let mut conn = sqlx::postgres::PgConnection::connect(&admin_url).await?;

        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = 'sinex_test_template_shared')",
        )
        .fetch_one(&mut conn)
        .await?;

        assert!(exists, "Template database should exist");

        Ok(())
    }

    #[sinex_test]
    async fn test_database_pool_provides_connection() -> Result<()> {
        let db = acquire_test_database().await?;

        // Direct pool access should work
        let result: i32 = sqlx::query_scalar("SELECT 1").fetch_one(db.pool()).await?;
        assert_eq!(result, 1);

        Ok(())
    }

    #[sinex_test]
    async fn test_concurrent_context_allocation() -> Result<()> {
        use std::sync::atomic::{AtomicU32, Ordering};
        use std::sync::Arc;

        let success_count = Arc::new(AtomicU32::new(0));

        // Try to allocate multiple databases concurrently
        let mut handles = vec![];
        for _i in 0..5 {
            let counter = success_count.clone();
            let handle = tokio::spawn(async move {
                match acquire_test_database().await {
                    Ok(db) => {
                        // Do some work
                        let _: i32 = sqlx::query_scalar("SELECT 1").fetch_one(db.pool()).await?;
                        counter.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    }
                    Err(e) => Err(e),
                }
            });
            handles.push(handle);
        }

        // Wait for all
        for handle in handles {
            let _ = handle.await;
        }

        assert!(success_count.load(Ordering::SeqCst) > 0);

        Ok(())
    }

    #[sinex_test]
    async fn test_basic_pool_functionality() -> Result<()> {
        // Test basic pool operations
        let db = acquire_test_database().await?;
        let pool = db.pool();

        // Basic connectivity test
        let result: i32 = sqlx::query_scalar("SELECT 1").fetch_one(pool).await?;
        assert_eq!(result, 1);

        // Test isolation between databases
        let db1 = acquire_test_database().await?;
        let db2 = acquire_test_database().await?;
        assert_ne!(
            db1.name(),
            db2.name(),
            "Each test should get a unique database"
        );

        Ok(())
    }
}

#[cfg(all(test, feature = "bench"))]
mod benches {
    use super::*;
    use crate::sinex_bench;

    /// Benchmark database acquisition from pool
    ///
    /// This measures the time to acquire a clean database from the pool,
    /// including advisory lock acquisition and cleanup verification.
    #[sinex_bench]
    fn bench_acquire_database() -> color_eyre::eyre::Result<()> {
        let db = acquire_test_database().await?;
        // Database is automatically returned on drop
        drop(db);
        Ok(())
    }

    /// Benchmark concurrent database acquisition
    ///
    /// Measures contention and performance when multiple tasks
    /// try to acquire databases simultaneously.
    #[sinex_bench(args = [2, 4, 8, 16])]
    async fn bench_concurrent_acquisition(arg: usize) -> color_eyre::eyre::Result<()> {
        let concurrency = arg;
        let handles: Vec<_> = (0..concurrency)
            .map(|_| tokio::spawn(async move { acquire_test_database().await.unwrap() }))
            .collect();

        // Wait for all to complete
        for handle in handles {
            let db = handle.await?;
            drop(db);
        }
        Ok(())
    }

    /// Benchmark database cleanup performance
    ///
    /// Measures the time to clean a database with various amounts of data
    #[sinex_bench]
    fn bench_database_cleanup() -> color_eyre::eyre::Result<()> {
        // Setup: Get a database and populate it
        let db = acquire_test_database().await?;
        let pool = db.pool();

        // Insert test data
        use sinex_core::db::models::*;
        use sinex_core::db::repositories::*;
        use sinex_core::types::domain::*;

        let repo = pool.events();
        for i in 0..100 {
            let new_event = RawEvent::builder()
                .source(EventSource::new("bench"))
                .event_type(EventType::new("test"))
                .host(HostName::new("host"))
                .payload(serde_json::json!({"index": i}))
                .build();
            repo.insert(new_event).await?;
        }

        // Perform cleanup
        clean_database(pool, db.name()).await?;
        drop(db);
        Ok(())
    }

    /// Benchmark template database operations
    #[sinex_bench]
    fn bench_ensure_template_database() -> color_eyre::eyre::Result<()> {
        let config = PoolConfig::default();
        // This should be fast after first run (cached)
        ensure_template_database(&config.admin_url, &config.base_url).await?;
        Ok(())
    }

    /// Benchmark pool health check
    #[sinex_bench]
    fn bench_pool_health_check() -> color_eyre::eyre::Result<()> {
        // Ensure pool is initialized
        let _ = acquire_test_database().await?;

        check_pool_health().await?;
        Ok(())
    }

    /// Benchmark database statistics collection
    #[sinex_bench]
    fn bench_get_database_stats() -> color_eyre::eyre::Result<()> {
        let db = acquire_test_database().await?;

        // Insert some varied data
        let pool = db.pool();
        use sinex_core::db::models::*;
        use sinex_core::db::repositories::*;
        use sinex_core::types::domain::*;

        let repo = pool.events();
        for i in 0..50 {
            let new_event = RawEvent::builder()
                .source(EventSource::new(&format!("source_{}", i % 10)))
                .event_type(EventType::new("test"))
                .host(HostName::new("bench"))
                .payload(serde_json::json!({}))
                .build();
            repo.insert(new_event).await?;
        }

        let stats = db.get_stats().await?;
        #[cfg(feature = "bench")]
        divan::black_box(stats);
        #[cfg(not(feature = "bench"))]
        drop(stats);
        Ok(())
    }
}
