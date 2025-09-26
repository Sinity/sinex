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
use serde::{Deserialize, Serialize};
use sinex_core::db::DbPool;
use sinex_core::types::error::SinexError;

use sha2::{Digest, Sha256};
use sqlx::postgres::{PgConnection, PgPoolOptions};
use sqlx::Connection;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::fs;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use tracing::warn;

#[allow(dead_code)]
static DB_COUNTER: AtomicU32 = AtomicU32::new(0);
#[allow(dead_code)]
static SLOT_COUNTER: AtomicUsize = AtomicUsize::new(0);

/// Pool performance metrics
static POOL_METRICS: Lazy<PoolMetrics> = Lazy::new(PoolMetrics::new);

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

#[derive(Debug, Serialize, Deserialize)]
struct TemplateStamp {
    template_name: String,
    fingerprint: String,
    extensions: HashMap<String, String>,
}

fn template_stamp_path() -> Option<PathBuf> {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace_root = manifest_dir
        .parent()
        .and_then(|p| p.parent())
        .and_then(|p| p.parent())
        .map(PathBuf::from)
        .unwrap_or(manifest_dir);

    let target_dir = std::env::var("CARGO_TARGET_DIR")
        .ok()
        .map(PathBuf::from)
        .unwrap_or_else(|| workspace_root.join("target"));

    Some(
        target_dir
            .join("sinex-test-utils")
            .join("template_stamp.json"),
    )
}

fn load_template_stamp() -> Option<TemplateStamp> {
    let path = template_stamp_path()?;
    let data = fs::read(path).ok()?;
    serde_json::from_slice(&data).ok()
}

fn store_template_stamp(stamp: &TemplateStamp) {
    if let Some(path) = template_stamp_path() {
        if let Some(parent) = path.parent() {
            let _ = fs::create_dir_all(parent);
        }
        match serde_json::to_vec_pretty(stamp) {
            Ok(payload) => {
                if let Err(err) = fs::write(&path, payload) {
                    warn!("Failed to write template stamp to {:?}: {}", path, err);
                }
            }
            Err(err) => warn!("Failed to serialize template stamp: {}", err),
        }
    }
}

fn migrations_fingerprint() -> Option<String> {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let schema_dir = crate_dir.join("../sinex-schema");
    let migrations_dir = schema_dir.join("src/migrations").canonicalize().ok()?;

    let mut entries: Vec<PathBuf> = fs::read_dir(&migrations_dir)
        .ok()?
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .collect();
    entries.sort();

    let mut hasher = Sha256::new();
    for path in entries {
        if path.is_file() {
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                hasher.update(name.as_bytes());
            }
            if let Ok(bytes) = fs::read(&path) {
                hasher.update(bytes);
            }
        }
    }

    for extra in ["DDL.sql", "monitoring.sql"] {
        let file = schema_dir.join(extra);
        if let Ok(bytes) = fs::read(&file) {
            hasher.update(extra.as_bytes());
            hasher.update(bytes);
        }
    }

    Some(format!("{:x}", hasher.finalize()))
}

/// Database pool configuration
struct PoolConfig {
    size: usize,
    admin_url: String,
    base_url: String,
    template_name: String,
    slot_max_connections: u32,
    admin_max_connections: u32,
}

impl Default for PoolConfig {
    fn default() -> Self {
        let base_url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgresql:///sinex_dev?host=/run/postgresql".to_string());
        let admin_url = base_url.replace("/sinex_dev", "/postgres");
        let size = std::env::var("SINEX_TESTUTILS_POOL_SIZE")
            .ok()
            .and_then(|s| s.parse().ok())
            .filter(|&s: &usize| s > 0)
            .unwrap_or(64);

        let mut config = Self {
            size,
            admin_url,
            base_url,
            template_name: "sinex_test_template_shared".to_string(),
            slot_max_connections: 0,
            admin_max_connections: 0,
        };

        config.recompute_connection_limits();
        config
    }
}

/// Pool configuration with customizable parameters
impl PoolConfig {
    /// Create config with custom pool size
    pub fn with_size(size: usize) -> Self {
        let mut config = Self::default();
        if size > 0 {
            config.size = size;
        }
        config.recompute_connection_limits();
        config
    }

    /// Create config with custom template name
    pub fn with_template(template_name: &str) -> Self {
        let mut config = Self {
            template_name: template_name.to_string(),
            ..Self::default()
        };
        config.recompute_connection_limits();
        config
    }

    fn recompute_connection_limits(&mut self) {
        fn parse_env_u32(name: &str) -> Option<u32> {
            std::env::var(name).ok().and_then(|v| v.parse().ok())
        }

        let size_u32 = self.size.max(1) as u32;
        let conn_budget = parse_env_u32("SINEX_TESTUTILS_CONN_BUDGET").unwrap_or(96);

        let mut slot_default = (conn_budget / size_u32).max(1);
        slot_default = slot_default.clamp(1, 8);
        if slot_default < 2 {
            slot_default = 2;
        }

        let slot_max = parse_env_u32("SINEX_TESTUTILS_SLOT_MAX_CONNECTIONS")
            .map(|v| v.clamp(1, 32))
            .unwrap_or(slot_default);
        self.slot_max_connections = slot_max;

        let admin_default = self
            .slot_max_connections
            .saturating_mul(2)
            .max(2)
            .clamp(2, 24);
        let admin_max = parse_env_u32("SINEX_TESTUTILS_ADMIN_MAX_CONNECTIONS")
            .map(|v| v.clamp(1, 32))
            .unwrap_or(admin_default);
        self.admin_max_connections = admin_max;

        // Ensure pool size respects the connection budget
        let per_slot = self.slot_max_connections.max(1);
        let usable_budget = conn_budget.saturating_sub(self.admin_max_connections);
        let max_size = (usable_budget / per_slot).max(1);
        if (self.size as u32) > max_size {
            self.size = max_size as usize;
        }
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

    /// Connection URL for opening ad-hoc connections
    pub fn url(&self) -> &str {
        &self.slot.url
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

/// Cleanup task for background processing
#[derive(Debug)]
struct CleanupTask {
    lock_id: i64,
    pool: DbPool,
    slot_name: String,
}

/// Background cleanup manager to handle resource cleanup safely
struct CleanupManager {
    sender: tokio::sync::mpsc::UnboundedSender<CleanupTask>,
}

impl CleanupManager {
    fn new() -> Self {
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel::<CleanupTask>();

        // Spawn background cleanup task
        tokio::spawn(async move {
            while let Some(task) = receiver.recv().await {
                Self::process_cleanup_task(task).await;
            }
        });

        Self { sender }
    }

    fn schedule_cleanup(&self, task: CleanupTask) {
        if self.sender.send(task).is_err() {
            eprintln!("⚠️  Cleanup manager channel closed, cannot schedule cleanup");
        }
    }

    async fn process_cleanup_task(task: CleanupTask) {
        // Try to release the advisory lock with a timeout
        match tokio::time::timeout(
            Duration::from_secs(5),
            sqlx::query("SELECT pg_advisory_unlock($1)")
                .bind(task.lock_id)
                .execute(&task.pool),
        )
        .await
        {
            Ok(Ok(_)) => eprintln!(
                "✅ Released advisory lock {} for {}",
                task.lock_id, task.slot_name
            ),
            Ok(Err(e)) => {
                eprintln!(
                    "⚠️  Failed to release advisory lock {} for {}: {}",
                    task.lock_id, task.slot_name, e
                )
            }
            Err(_) => eprintln!(
                "⚠️  Timeout releasing advisory lock {} for {} (pool may be shutting down)",
                task.lock_id, task.slot_name
            ),
        }

        // Close the pool with a timeout
        let close_future = task.pool.close();
        if tokio::time::timeout(Duration::from_secs(2), close_future)
            .await
            .is_err()
        {
            eprintln!("⚠️  Timeout closing pool for {}", task.slot_name);
        }
    }
}

/// Global cleanup manager
static CLEANUP_MANAGER: Lazy<CleanupManager> = Lazy::new(CleanupManager::new);

impl Drop for TestDatabase {
    fn drop(&mut self) {
        // Safe, non-blocking cleanup that doesn't create runtimes
        let lock_id = self.lock_id;

        eprintln!(
            "🔓 Releasing database slot: {} (lock_id: {})",
            self.name, lock_id
        );

        // Schedule cleanup via the cleanup manager instead of blocking Drop
        CLEANUP_MANAGER.schedule_cleanup(CleanupTask {
            lock_id,
            pool: self.pool.clone(),
            slot_name: self.name.clone(),
        });

        // Clear the pool reference immediately
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
    slot_max_connections: u32,
}

impl DatabasePool {
    /// Initialize the pool
    async fn new(config: PoolConfig) -> Result<Self> {
        eprintln!(
            "🚀 Initializing database pool with {} databases (reusing existing if available)...",
            config.size
        );
        eprintln!(
            "   slot max connections per DB: {}, admin pool max connections: {}",
            config.slot_max_connections, config.admin_max_connections
        );

        // Ensure template exists
        ensure_template_database(
            &config.admin_url,
            &config.base_url,
            config.slot_max_connections,
        )
        .await?;

        // Create admin connection
        let admin_pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(config.admin_max_connections)
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
            eprintln!("🧹 Cleaning up {non_pool_count} non-pool test databases...");

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
                let _ = sqlx::query(&format!("DROP DATABASE IF EXISTS {db}"))
                    .execute(&admin_pool)
                    .await;
            }
        }

        // Create all databases in parallel
        let mut slots = Vec::with_capacity(config.size);
        let mut tasks = Vec::new();

        // Compute template URL and capture extension versions from the fresh template
        let template_url = config
            .base_url
            .replace("/sinex_dev", &format!("/{}", config.template_name));
        // Fetch extension versions from template for drift detection
        let template_ext_versions: std::collections::HashMap<String, String> = {
            let mut map = std::collections::HashMap::new();
            if let Ok(pool) = sqlx::postgres::PgPoolOptions::new()
                .max_connections(1)
                .connect(&template_url)
                .await
            {
                if let Ok(rows) = sqlx::query!(
                    r#"SELECT extname, extversion FROM pg_extension WHERE extname IN ('timescaledb','ulid','pg_jsonschema','vector')"#
                )
                .fetch_all(&pool)
                .await
                {
                    for row in rows {
                        map.insert(row.extname, row.extversion);
                    }
                }
                let _ = pool.close().await;
            }
            map
        };

        let slot_max_conns = config.slot_max_connections;

        for i in 0..config.size {
            let admin_pool = admin_pool.clone();
            let base_url = config.base_url.clone();
            let template_name = config.template_name.clone();
            let template_ext_versions = template_ext_versions.clone();

            let task = tokio::spawn(async move {
                let name = format!("sinex_test_pool_{i}");

                let mut conn = admin_pool.acquire().await?;

                // Check if database already exists
                let exists: bool = sqlx::query_scalar(&format!(
                    "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = '{name}')"
                ))
                .fetch_one(&mut *conn)
                .await?;

                if !exists {
                    match sqlx::query(&format!(
                        "CREATE DATABASE {name} WITH TEMPLATE {template_name}"
                    ))
                    .execute(&mut *conn)
                    .await
                    {
                        Ok(_) => eprintln!("  Created new pool database: {name}"),
                        Err(err) => {
                            let err_str = err.to_string();
                            if err_str.contains("already exists") {
                                eprintln!(
                                    "  Database {name} already exists after creation race; reusing"
                                );
                            } else {
                                return Err(err.into());
                            }
                        }
                    }
                } else {
                    // Check extension versions against the template; drop/recreate if drifted
                    let db_url = base_url.replace("/sinex_dev", &format!("/{name}"));
                    let mut needs_recreate = false;

                    if let Ok(db_pool) = sqlx::postgres::PgPoolOptions::new()
                        .max_connections(slot_max_conns.max(1))
                        .acquire_timeout(Duration::from_secs(2))
                        .connect(&db_url)
                        .await
                    {
                        if let Ok(rows) = sqlx::query!(
                            r#"SELECT extname, extversion FROM pg_extension WHERE extname IN ('timescaledb','ulid','pg_jsonschema','vector')"#
                        )
                        .fetch_all(&db_pool)
                        .await
                        {
                            for row in rows {
                                if let Some(t_ver) = template_ext_versions.get(&row.extname) {
                                    if &row.extversion != t_ver {
                                        needs_recreate = true;
                                        eprintln!(
                                            "  Drift detected in {ext} ({found} != {expected}), recreating {name}",
                                            ext = row.extname,
                                            found = row.extversion,
                                            expected = t_ver,
                                        );
                                        break;
                                    }
                                }
                            }
                            // Additionally ensure core schema exists (e.g., core.events table)
                            if !needs_recreate {
                                if let Ok(exists) = sqlx::query_scalar::<_, Option<String>>(
                                    "SELECT to_regclass('core.events')::text"
                                )
                                .fetch_one(&db_pool)
                                .await
                                {
                                    if exists.as_deref() != Some("core.events") {
                                        needs_recreate = true;
                                        eprintln!(
                                            "  Missing schema in {name} (core.events), recreating"
                                        );
                                    }
                                } else {
                                    needs_recreate = true;
                                    eprintln!("  Failed to verify schema in {name}, recreating");
                                }
                            }

                            if !needs_recreate {
                                match sqlx::query_scalar::<_, bool>(
                                    "SELECT EXISTS (SELECT 1 FROM information_schema.columns \
                                     WHERE table_schema = 'core' AND table_name = 'events' AND column_name = 'associated_blob_ids')",
                                )
                                .fetch_one(&db_pool)
                                .await
                                {
                                    Ok(true) => {}
                                    Ok(false) => {
                                        needs_recreate = true;
                                        eprintln!(
                                            "  Database {name} missing core.events.associated_blob_ids; recreating"
                                        );
                                    }
                                    Err(err) => {
                                        needs_recreate = true;
                                        eprintln!(
                                            "  Failed to inspect columns in {name} ({err}); recreating"
                                        );
                                    }
                                }
                            }
                        } else {
                            // Unable to query extensions; assume drift and recreate
                            needs_recreate = true;
                            eprintln!(
                                "  Unable to query extensions for {name}, forcing recreation"
                    );
                        }
                        let _ = db_pool.close().await;
                    } else {
                        // Can't connect to DB quickly; play it safe and recreate
                        needs_recreate = true;
                        eprintln!("  Unable to connect to {name}, forcing recreation");
                    }

                    if needs_recreate {
                        // Terminate connections and drop the database
                        let _ = sqlx::query(&format!(
                                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{name}' AND pid <> pg_backend_pid()"
                            ))
                        .execute(&mut *conn)
                        .await;

                        let drop_force =
                            sqlx::query(&format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)"))
                                .execute(&mut *conn)
                                .await;
                        if drop_force.is_err() {
                            let _ = sqlx::query(&format!("DROP DATABASE IF EXISTS {name}"))
                                .execute(&mut *conn)
                                .await;
                        }

                        // Recreate from the fresh template
                        match sqlx::query(&format!(
                            "CREATE DATABASE {name} WITH TEMPLATE {template_name}"
                        ))
                        .execute(&mut *conn)
                        .await
                        {
                            Ok(_) => eprintln!("  Recreated pool database from template: {name}"),
                            Err(err) => {
                                let err_str = err.to_string();
                                if err_str.contains("already exists") {
                                    eprintln!(
                                        "  Database {name} was recreated by another task; reusing"
                                    );
                                } else {
                                    return Err(err.into());
                                }
                            }
                        }
                    } else {
                        eprintln!("  Reusing existing pool database: {name}");
                    }
                }

                drop(conn);

                // Store URL for later pool creation
                let url = base_url.replace("/sinex_dev", &format!("/{name}"));

                Ok::<_, color_eyre::eyre::Error>((name, url))
            });

            tasks.push(task);
        }

        // Wait for all databases to be created
        for task in tasks {
            let (name, url) = task
                .await
                .map_err(|e| SinexError::service(format!("Database creation task failed: {e}")))?
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

        Ok(Self {
            slots,
            slot_max_connections: slot_max_conns.max(1),
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
        eprintln!("🎲 Process {pid} starting from index: {start_index}");

        // We need to try to acquire databases with PostgreSQL advisory locks
        // to ensure inter-process coordination
        loop {
            // Iterate through slots starting from our position
            for i in 0..self.slots.len() {
                let slot_index = (start_index + i) % self.slots.len();
                let slot = &self.slots[slot_index];

                // Try to connect to this database
                let pool = match sqlx::postgres::PgPoolOptions::new()
                    .max_connections(self.slot_max_connections)
                    .acquire_timeout(Duration::from_secs(2)) // Shorter timeout for faster iteration
                    .connect(&slot.url)
                    .await
                {
                    Ok(pool) => pool,
                    Err(_) => continue, // Try next slot
                };

                // Try to acquire an advisory lock for this database
                // Use a compound lock ID: slot_index + process_id for better uniqueness
                let lock_id = (1000 + slot_index as i64) * 100000 + (pid as i64);
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
                // Immediately mark as in use to prevent intra-process races\n                slot.in_use.store(true, Ordering::SeqCst);\n\n                // Verify we still hold the lock after setting in_use flag\n                let lock_verified: bool = sqlx::query_scalar(\n                    \"SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND objid = $1 AND pid = pg_backend_pid())\"\n                )\n                .bind(lock_id)\n                .fetch_one(&pool)\n                .await?;\n\n                if !lock_verified {\n                    // We lost the lock somehow, mark as not in use and try next\n                    slot.in_use.store(false, Ordering::SeqCst);\n                    pool.close().await;\n                    eprintln!(\"⚠️  Lock verification failed for slot {}, trying next\", slot.name);\n                    continue;\n                }
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
            if attempts > 250 {
                let total_time = start_time.elapsed();
                return Err(SinexError::unknown(format!(
                    "Failed to acquire database after {attempts} attempts ({total_time:.1?})"
                )));
            }

            // Log warning after many attempts
            if attempts % 10 == 0 {
                let elapsed = start_time.elapsed();
                eprintln!(
                    "⚠️  Process {pid} waiting for database slot (attempt {attempts}, {elapsed:.1?} elapsed)"
                );
            }

            // All slots in use, wait a bit before retrying
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

/// Clean a database for reuse
async fn clean_database(pool: &DbPool, db_name: &str) -> Result<()> {
    eprintln!("🧹 Cleaning database: {db_name}");

    // Use the shared db_common implementation
    // Relax strict FK that can block synthetic test IDs
    let _ = sqlx::query(
        "ALTER TABLE core.processor_checkpoints DROP CONSTRAINT IF EXISTS processor_checkpoints_last_processed_id_fkey",
    )
    .execute(pool)
    .await;

    match crate::db_common::reset_database(pool).await {
        Ok(_) => {
            eprintln!("  ✅ Database cleanup verified - all tables empty");
            Ok(())
        }
        Err(e) => {
            eprintln!("  ❌ CRITICAL: Database {db_name} cleanup failed: {e}");
            POOL_METRICS.record_cleanup_failure();

            // Try to get more details about what went wrong
            if let Ok(counts) = crate::db_common::get_row_counts(pool).await {
                for (table, count) in counts {
                    if count > 0 {
                        eprintln!("     - {table} has {count} rows remaining");
                    }
                }
            }

            Err(SinexError::unknown(format!(
                "Database {db_name} cleanup failed: {e}"
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

    let pool = pool_lock
        .as_ref()
        .ok_or_else(|| SinexError::service("Database pool not initialized".to_string()))?
        .clone();
    drop(pool_lock);

    pool.acquire().await
}

/// Ensure we have a template database with all migrations applied
/// This is created once per test process and reused for all test databases
fn advisory_lock_key(name: &str) -> i64 {
    let mut hasher = DefaultHasher::new();
    name.hash(&mut hasher);
    // mask to positive i64 to match PostgreSQL advisory lock expectations
    (hasher.finish() & 0x7FFF_FFFF_FFFF_FFFF) as i64
}

async fn connect_admin_with_retry(admin_url: &str) -> Result<PgConnection> {
    let mut delay = Duration::from_millis(100);
    let mut last_error: Option<sqlx::Error> = None;

    for attempt in 0..5 {
        match tokio::time::timeout(Duration::from_secs(5), PgConnection::connect(admin_url)).await {
            Ok(Ok(conn)) => return Ok(conn),
            Ok(Err(err)) => {
                let err_str = err.to_string();
                if !err_str.to_lowercase().contains("too many clients") {
                    return Err(SinexError::database(format!(
                        "Admin connection failed: {err_str}"
                    )));
                }
                last_error = Some(err);
                eprintln!(
                    "⚠️  Admin connection refused (too many clients); retrying in {:?} (attempt {}/{})",
                    delay,
                    attempt + 1,
                    5
                );
            }
            Err(_) => {
                return Err(SinexError::database("Admin connection timeout"));
            }
        }

        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(Duration::from_secs(1));
    }

    Err(SinexError::database(format!(
        "Admin connection failed after retries: {}",
        last_error
            .map(|e| e.to_string())
            .unwrap_or_else(|| "unknown error".to_string())
    )))
}

async fn ensure_template_database(
    admin_url: &str,
    base_url: &str,
    slot_max_connections: u32,
) -> Result<String> {
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

    eprintln!("🔧 Checking template database {template_name} ...");
    let template_start = std::time::Instant::now();

    let desired_fingerprint = migrations_fingerprint();
    if desired_fingerprint.is_none() {
        eprintln!(
            "⚠️  Unable to compute migrations fingerprint; template caching disabled for this run"
        );
    }
    let cached_stamp = load_template_stamp();
    if cached_stamp.is_none() {
        eprintln!("ℹ️  No template stamp found; first build or stamp unavailable");
    }

    // Connect to admin database with timeout
    let mut admin_conn = connect_admin_with_retry(admin_url).await?;

    let lock_key = advisory_lock_key(template_name);
    tokio::time::timeout(
        Duration::from_secs(120),
        sqlx::query("SELECT pg_advisory_lock($1)")
            .bind(lock_key)
            .execute(&mut admin_conn),
    )
    .await
    .map_err(|_| SinexError::database("Template advisory lock timeout"))?
    .map_err(|e| SinexError::database(format!("Template advisory lock failed: {e}")))?;

    let slot_max_connections = slot_max_connections.max(1);
    let template_pool_max = slot_max_connections.saturating_mul(2).max(8);

    let template_url = base_url.replace("/sinex_dev", &format!("/{template_name}"));

    // Check if template already exists
    let exists: bool = sqlx::query_scalar(&format!(
        "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = '{template_name}')"
    ))
    .fetch_one(&mut admin_conn)
    .await?;

    // Determine if we can reuse the existing template without rebuild
    let mut reuse_allowed = false;
    if exists {
        if let (Some(fp), Some(stamp)) = (&desired_fingerprint, cached_stamp.as_ref()) {
            if stamp.template_name == template_name && stamp.fingerprint == *fp {
                if let Ok(pool) = PgPoolOptions::new()
                    .max_connections(1)
                    .acquire_timeout(Duration::from_secs(5))
                    .connect(&template_url)
                    .await
                {
                    match collect_extension_versions(&pool).await {
                        Ok(current_exts) => {
                            if current_exts == stamp.extensions {
                                match sqlx::query_scalar::<_, bool>(
                                    "SELECT EXISTS (SELECT 1 FROM information_schema.columns \
                                     WHERE table_schema = 'core' AND table_name = 'events' AND column_name = 'associated_blob_ids')",
                                )
                                .fetch_one(&pool)
                                .await
                                {
                                    Ok(true) => {
                                        eprintln!(
                                            "✅ Template database {template_name} reused (migrations unchanged)"
                                        );
                                        reuse_allowed = true;
                                    }
                                    Ok(false) => {
                                        eprintln!(
                                            "♻️  Template {template_name} missing core.events.associated_blob_ids; recreating"
                                        );
                                    }
                                    Err(err) => {
                                        eprintln!(
                                            "⚠️  Failed to inspect template schema ({err}); forcing recreation"
                                        );
                                    }
                                }
                            } else {
                                eprintln!(
                                    "♻️  Template database '{template_name}' extensions drifted; recreating"
                                );
                            }
                        }
                        Err(err) => {
                            eprintln!(
                                "⚠️  Failed to inspect template extensions ({err}); forcing recreation"
                            );
                        }
                    }
                    let _ = pool.close().await;
                }
            } else {
                eprintln!(
                    "♻️  Migration fingerprint changed ({} -> {}); recreating template",
                    stamp.fingerprint, fp
                );
            }
        }
    }

    if reuse_allowed {
        let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
            .bind(lock_key)
            .execute(&mut admin_conn)
            .await;
        admin_conn.close().await?;
        TEMPLATE_DB_NAME
            .set(template_name.to_string())
            .map_err(|_| SinexError::unknown("Failed to cache template database name"))?;
        return Ok(template_name.to_string());
    }

    // We need to rebuild the template
    POOL_METRICS.record_template_recreation();
    eprintln!(
        "♻️  Template database '{template_name}' requires recreation; rebuilding from scratch"
    );

    // Terminate connections and drop if necessary
    let terminate_query = format!(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
         WHERE datname = '{template_name}' AND pid <> pg_backend_pid()"
    );
    let _ = sqlx::query(&terminate_query).execute(&mut admin_conn).await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let drop_query = format!("DROP DATABASE IF EXISTS {template_name} WITH (FORCE)");
    match sqlx::query(&drop_query).execute(&mut admin_conn).await {
        Ok(_) => {}
        Err(_) => {
            let fallback = format!("DROP DATABASE IF EXISTS {template_name}");
            sqlx::query(&fallback).execute(&mut admin_conn).await?;
        }
    }

    let create_query = format!("CREATE DATABASE {template_name}");
    tokio::time::timeout(
        Duration::from_secs(10),
        sqlx::query(&create_query).execute(&mut admin_conn),
    )
    .await
    .map_err(|_| SinexError::database("Create database timeout"))?
    .map_err(|e| SinexError::database(format!("Create database failed: {e}")))?;

    // Connect to template database and run all migrations
    let template_pool_future = async {
        let template_pool: DbPool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(template_pool_max)
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
                eprintln!("❌ Missing required PostgreSQL extensions: {e}");
                eprintln!("   Check NixOS PostgreSQL configuration and required extensions.");
                return Err(e);
            }
        }

        // Run migrations against the template database. The sinex-core migration helper
        // reads DATABASE_URL, so temporarily point it at the template DB.
        let prev_db_url = std::env::var("DATABASE_URL").ok();
        std::env::set_var("DATABASE_URL", &template_url);

        let migrate_result = tokio::time::timeout(
            Duration::from_secs(30),
            sinex_core::db::run_migrations_for_url(&template_url),
        )
        .await
        .map_err(|_| {
            SinexError::database(
                "Migration timeout - check if all required extensions are installed".to_string(),
            )
        })
        .and_then(|res| res.map_err(|e| SinexError::database(format!("Migration failed: {e}"))));

        // Restore original DATABASE_URL
        if let Some(url) = prev_db_url {
            std::env::set_var("DATABASE_URL", url);
        }

        // Propagate migration result
        migrate_result?;

        // Seed a canonical test source material so Event::test_event() inserts pass FK checks.
        sqlx::query(
            r#"
            INSERT INTO raw.source_material_registry (
                id,
                material_kind,
                source_identifier,
                status,
                timing_info_type,
                metadata
            ) VALUES (
                $1::text::ulid,
                'annex',
                'test-material-bootstrap',
                'completed',
                'realtime',
                '{}'::jsonb
            )
            ON CONFLICT (id) DO NOTHING
            "#,
        )
        .bind("014D2PF2DBSQQZXQ5TK1V58CGG")
        .execute(&template_pool)
        .await?;

        // Optimize template for faster copying
        optimize_template_for_tests(&template_pool).await?;

        let extensions = collect_extension_versions(&template_pool).await?;

        template_pool.close().await;
        Ok::<HashMap<String, String>, SinexError>(extensions)
    };

    let migration_result: Result<HashMap<String, String>> =
        tokio::time::timeout(Duration::from_secs(45), template_pool_future)
            .await
            .map_err(|_| SinexError::database("Template setup timeout"))?;

    if let Err(e) = sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(lock_key)
        .execute(&mut admin_conn)
        .await
    {
        eprintln!("⚠️  Failed to release template advisory lock for {template_name}: {e}");
    }

    admin_conn.close().await?;

    let extensions = migration_result?;

    let template_elapsed = template_start.elapsed();
    eprintln!("✅ Template database created in {template_elapsed:?}");

    if let Some(fp) = desired_fingerprint {
        let stamp = TemplateStamp {
            template_name: template_name.to_string(),
            fingerprint: fp,
            extensions,
        };
        store_template_stamp(&stamp);
    }

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
            missing.push(format!("{ext_name} ({description})"));
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

async fn collect_extension_versions(pool: &DbPool) -> Result<HashMap<String, String>> {
    let rows = sqlx::query!(
        r#"SELECT extname, extversion FROM pg_extension WHERE extname IN ('timescaledb','ulid','pg_jsonschema','vector')"#
    )
    .fetch_all(pool)
    .await?;

    let mut map = HashMap::new();
    for row in rows {
        map.insert(row.extname, row.extversion);
    }
    Ok(map)
}

/// Apply test-specific PostgreSQL optimizations (session-level only)
async fn apply_test_session_optimizations(pool: &DbPool) -> Result<()> {
    if std::env::var("SINEX_TEST_OPTIMIZATIONS").is_ok() {
        eprintln!("⚡ Applying test session optimizations...");
        crate::db_common::apply_test_optimizations(pool)
            .await
            .map_err(|e| {
                SinexError::database(format!("Failed to apply test optimizations: {e}"))
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
            let drop_sql = format!("DROP INDEX IF EXISTS {index}");
            if let Err(e) = sqlx::query(&drop_sql).execute(pool).await {
                // Don't fail if index doesn't exist
                eprintln!("⚠️  Could not drop index {index}: {e}");
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
            eprintln!("  ⚠️  Could not disable TimescaleDB policies: {e}");
        }

        // Disable autovacuum on template (tests don't need it)
        let disable_autovacuum_tables = vec!["core.events", "core.event_annotations"];

        for table in disable_autovacuum_tables {
            let disable_sql = format!("ALTER TABLE {table} SET (autovacuum_enabled = false)");
            if let Err(e) = sqlx::query(&disable_sql).execute(pool).await {
                eprintln!("⚠️  Could not disable autovacuum on {table}: {e}");
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

        // Relax strict FKs that make synthetic test IDs cumbersome
        let _ = sqlx::query(
            "ALTER TABLE core.processor_checkpoints DROP CONSTRAINT IF EXISTS processor_checkpoints_last_processed_id_fkey",
        )
        .execute(pool)
        .await;

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
            let pool_to_close = {
                let mut pool_opt = slot.pool.lock();
                pool_opt.take()
            };

            if let Some(pool) = pool_to_close {
                pool.close().await;
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
    #![allow(unused_imports)]
    use super::*;
    use crate::db_common::verify_clean_state;
    use crate::sinex_test;
    use sinex_core::DbPoolExt;

    #[sinex_test]
    async fn test_pool_handles_concurrent_acquisition() -> Result<()> {
        // Establish baseline event count for a clean database
        // Test that multiple tasks can acquire databases concurrently
        let handles: Vec<_> = (0..20)
            .map(|_i| {
                tokio::spawn(async move {
                    let db = acquire_test_database().await?;

                    // Each should have clean database according to clean-state verification
                    verify_clean_state(db.pool()).await?;

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
                .map_err(|e| SinexError::service(format!("Task failed: {e}")))?
                .map_err(|e| SinexError::database(format!("Database operation failed: {e}")))?;
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
        use sinex_core::*;
        use sinex_core::{
            Blob, BlobRecord, CheckpointRecord, Entity, EntityRecord, EntityRelation, Event,
            JsonValue, Operation, OperationRecord, Provenance, SourceMaterial,
        };

        let db_name;

        {
            let db = acquire_test_database().await?;
            let baseline = db.pool().events().count_all().await?;
            db_name = db.name().to_string();

            // Insert test data

            let repo = db.pool.events();
            let event = Event::<JsonValue>::test_event(
                EventSource::new("test"),
                EventType::new("test.event"),
                serde_json::json!({}),
            )
            .with_host(HostName::new("test-host"));
            repo.insert(event).await?;

            // Verify data exists
            let count = db.pool().events().count_all().await?;
            assert_eq!(count, baseline + 1);
        } // db is dropped here

        // Sleep briefly to allow cleanup
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Try to reacquire a database - it might be the same one
        let db2 = acquire_test_database().await?;
        let baseline = db2.pool().events().count_all().await?;

        if db2.name() == db_name {
            // If we got the same database, it should be clean
            let count = db2.pool().events().count_all().await?;
            assert_eq!(count, baseline, "Reused database should be cleaned");
        }

        Ok(())
    }

    #[sinex_test]
    async fn test_advisory_lock_prevents_double_acquisition() -> Result<()> {
        // This test verifies that two processes can't acquire the same database
        let db1 = acquire_test_database().await?;
        let lock_id1 = db1.lock_id;

        // Try to manually acquire the same lock - should fail
        let mut probe_conn = PgConnection::connect(db1.url()).await?;
        let lock_acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
            .bind(lock_id1)
            .fetch_one(&mut probe_conn)
            .await?;

        assert!(
            !lock_acquired,
            "Should not be able to acquire lock that's already held"
        );

        Ok(())
    }

    #[sinex_test]
    async fn test_database_health_check() -> Result<()> {
        use sinex_core::DbPoolExt;

        let db = acquire_test_database().await?;
        let baseline = db.pool().events().count_all().await?;

        // Health check should pass
        assert!(db.check_health().await?);

        // Get stats should work
        let stats = db.get_stats().await?;
        assert_eq!(stats.event_count, baseline);

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
        use sinex_core::*;
        use sinex_core::*;
        use sinex_core::{
            Blob, BlobRecord, CheckpointRecord, Entity, EntityRecord, EntityRelation, Event,
            JsonValue, Operation, OperationRecord, Provenance, SourceMaterial,
        };

        let repo = db.pool.events();
        let event_to_insert = Event::<JsonValue>::test_event(
            EventSource::new("test"),
            EventType::new("test"),
            serde_json::json!({}),
        )
        .with_host(HostName::new("test"));
        let event = repo.insert(event_to_insert).await?;

        // Add annotation
        sqlx::query(
            "INSERT INTO core.event_annotations (id, event_id, annotation_type, content, metadata, created_by) \
             VALUES ($1, $2, 'test', 'test-content', '{}'::jsonb, 'test-user')"
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

    #[allow(clippy::result_large_err)]
    #[cfg_attr(not(feature = "slow-tests"), ignore = "slow fixture")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_stress_concurrent_operations() -> Result<()> {
        // Stress test with many concurrent acquisitions
        let mut handles = Vec::new();

        for i in 0..50 {
            let handle = tokio::spawn(async move {
                let db = acquire_test_database().await?;

                // Do some work
                use sinex_core::{
                    db::repositories::source_materials::legacy_material_types, Event, EventSource,
                    EventType, HostName, JsonValue, Provenance, SourceMaterial,
                };

                let material_record = db
                    .pool()
                    .source_materials()
                    .register_in_flight(
                        legacy_material_types::STREAM,
                        Some(&format!("stress-fixture-{i}")),
                        serde_json::json!({ "test": "stress" }),
                    )
                    .await?;
                let material_id = sinex_core::Id::<SourceMaterial>::from_ulid(material_record.id);

                let repo = db.pool.events();
                for _j in 0..5 {
                    let mut event = Event::<JsonValue>::test_event(
                        EventSource::new(format!("task_{i}")),
                        EventType::new("stress.test"),
                        serde_json::json!({}),
                    )
                    .with_host(HostName::new("test"));
                    event.provenance = Provenance::from_material(material_id, 0, None, None);
                    repo.insert(event).await?;
                }

                // Verify isolation
                let repo = db.pool.events();
                let source = EventSource::new(format!("task_{i}"));
                let count = repo.count_by_source(&source).await?;
                assert!(count >= 5, "expected at least 5 events for {source}");

                db.force_cleanup().await?;

                Ok::<_, SinexError>(())
            });
            handles.push(handle);
        }

        // All should succeed
        for handle in handles {
            handle
                .await
                .map_err(|e| SinexError::service(format!("Task failed: {e}")))?
                .map_err(|e| SinexError::database(format!("Database operation failed: {e}")))?;
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
            .map(|_| {
                tokio::spawn(async move {
                    acquire_test_database().await.map_err(|e| {
                        tracing::error!("Benchmark database acquisition failed: {}", e);
                        e
                    })
                })
            })
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
        use sinex_core::*;
        use sinex_core::*;
        use sinex_core::{
            Blob, BlobRecord, CheckpointRecord, Entity, EntityRecord, EntityRelation, Event,
            JsonValue, Operation, OperationRecord, Provenance, SourceMaterial,
        };

        let repo = pool.events();
        for i in 0..100 {
            let new_event = Event::<JsonValue>::test_event(
                EventSource::new("bench"),
                EventType::new("test"),
                serde_json::json!({"index": i}),
            )
            .with_host(HostName::new("host"));
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
        ensure_template_database(
            &config.admin_url,
            &config.base_url,
            config.slot_max_connections,
        )
        .await?;
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
        use sinex_core::*;
        use sinex_core::*;
        use sinex_core::{
            Blob, BlobRecord, CheckpointRecord, Entity, EntityRecord, EntityRelation, Event,
            JsonValue, Operation, OperationRecord, Provenance, SourceMaterial,
        };

        let repo = pool.events();
        for i in 0..50 {
            let new_event = Event::<JsonValue>::test_event(
                EventSource::new(&format!("source_{}", i % 10)),
                EventType::new("test"),
                serde_json::json!({}),
            )
            .with_host(HostName::new("bench"));
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
