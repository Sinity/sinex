#![doc = include_str!("../docs/database_pool.md")]

use crate::Result;
use futures::Future;
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use sinex_core::db::DbPool;
use sinex_core::types::error::SinexError;
use sinex_core::types::ulid::Ulid;

use sha2::{Digest, Sha256};
use sqlx::pool::PoolConnection;
use sqlx::postgres::{PgConnection, PgPoolOptions};
use sqlx::{Connection, Error, Postgres};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::fs;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use tracing::warn;
use url::Url;

#[allow(dead_code)]
static DB_COUNTER: AtomicU32 = AtomicU32::new(0);
#[allow(dead_code)]
static SLOT_COUNTER: AtomicUsize = AtomicUsize::new(0);

/// Pool performance metrics
static POOL_METRICS: Lazy<PoolMetrics> = Lazy::new(PoolMetrics::new);
static OPTIONAL_EXTENSION_MISSING: Lazy<Mutex<HashMap<String, String>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));
static BOOTSTRAP_MATERIAL_ID: Lazy<Ulid> = Lazy::new(|| {
    Ulid::from_str("014D2PF2DBSQQZXQ5TK1V58CGG").expect("valid bootstrap material id")
});

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
#[derive(Debug, Clone, Serialize)]
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
lazy_static! {
    static ref DATABASE_POOL_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::new(());
}

pub type DatabasePoolTestGuard = tokio::sync::MutexGuard<'static, ()>;

/// Acquire a global guard to run database pool tests exclusively.
pub async fn acquire_pool_test_guard() -> DatabasePoolTestGuard {
    DATABASE_POOL_TEST_LOCK.lock().await
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
        let admin_url = std::env::var("DATABASE_URL_SUPERUSER")
            .or_else(|_| std::env::var("SINEX_TESTUTILS_ADMIN_URL"))
            .unwrap_or_else(|_| force_user(&replace_db_name(&base_url, "postgres"), "postgres"));
        let size = std::env::var("SINEX_TESTUTILS_POOL_SIZE")
            .ok()
            .and_then(|s| s.parse().ok())
            .filter(|&s: &usize| s > 0)
            .unwrap_or(32);

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

fn force_user(url: &str, user: &str) -> String {
    if let Ok(mut parsed) = Url::parse(url) {
        let _ = parsed.set_username(user);
        return parsed.to_string();
    }

    if url.contains('?') {
        format!("{url}&user={user}")
    } else {
        format!("{url}?user={user}")
    }
}

fn replace_db_name(url: &str, db: &str) -> String {
    if let Ok(mut parsed) = Url::parse(url) {
        parsed.set_path(&format!("/{db}"));
        return parsed.to_string();
    }

    url.replace("/sinex_dev", &format!("/{db}"))
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

        // Default to 480 to work with the NixOS module's 500 max_connections minimum
        // Leaves 20 connections for admin/other processes
        let conn_budget = parse_env_u32("SINEX_TESTUTILS_CONN_BUDGET").unwrap_or(480);

        let slot_max = parse_env_u32("SINEX_TESTUTILS_SLOT_MAX_CONNECTIONS")
            .map(|v| v.clamp(1, 32))
            .unwrap_or(4);
        self.slot_max_connections = slot_max;

        let admin_default = self.slot_max_connections.max(1).clamp(1, 8);
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

    /// Advisory lock identifier associated with this database slot
    pub fn lock_id(&self) -> i64 {
        self.lock_id
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
#[derive(Debug, Clone)]
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
        if self.sender.send(task.clone()).is_err() {
            eprintln!("⚠️  Cleanup manager channel closed, running cleanup inline");
            tokio::spawn(async move {
                CleanupManager::process_cleanup_task(task).await;
            });
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
                let exists = database_exists(&mut conn, &name).await?;

                if !exists {
                    match create_database_from_template(&mut conn, &name, &template_name).await? {
                        CreateDatabaseOutcome::Created => {
                            eprintln!("  Created new pool database: {name}");
                        }
                        CreateDatabaseOutcome::AlreadyExists => {
                            eprintln!(
                                "  Database {name} already exists after creation race; reusing"
                            );
                            // Ensure permissions are granted even when database was created concurrently
                            let _ = grant_pool_database_permissions(&name).await;
                        }
                    }
                } else {
                    // Ensure permissions are granted on pre-existing databases (CI restarts, etc)
                    let _ = grant_pool_database_permissions(&name).await;
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
                    r#"SELECT extname, extversion FROM pg_extension WHERE extname IN ('timescaledb','pgx_ulid','pg_jsonschema','vector')"#
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
                                let events_has_blobs = sqlx::query_scalar::<_, bool>(
                                    "SELECT EXISTS (SELECT 1 FROM information_schema.columns \
                                     WHERE table_schema = 'core' AND table_name = 'events' AND column_name = 'associated_blob_ids')",
                                )
                                .fetch_one(&db_pool)
                                .await;

                                let events_has_subnano = sqlx::query_scalar::<_, bool>(
                                    "SELECT EXISTS (SELECT 1 FROM information_schema.columns \
                                     WHERE table_schema = 'core' AND table_name = 'events' AND column_name = 'ts_orig_subnano')",
                                )
                                .fetch_one(&db_pool)
                                .await;

                                let checkpoints_has_metadata = sqlx::query_scalar::<_, bool>(
                                    "SELECT COUNT(*) = 2 FROM information_schema.columns \
                                     WHERE table_schema = 'core' AND table_name = 'processor_checkpoints' \
                                       AND column_name IN ('checkpoint_version', 'created_at')",
                                )
                                .fetch_one(&db_pool)
                                .await;

                                let payload_has_updated_at = sqlx::query_scalar::<_, bool>(
                                    "SELECT EXISTS (SELECT 1 FROM information_schema.columns \
                                     WHERE table_schema = 'sinex_schemas' AND table_name = 'event_payload_schemas' \
                                       AND column_name = 'updated_at')",
                                )
                                .fetch_one(&db_pool)
                                .await;

                                match (
                                    events_has_blobs,
                                    events_has_subnano,
                                    checkpoints_has_metadata,
                                    payload_has_updated_at,
                                ) {
                                    (Ok(true), Ok(true), Ok(true), Ok(true)) => {}
                                    (Ok(false), _, _, _) => {
                                        needs_recreate = true;
                                        eprintln!(
                                            "  Database {name} missing core.events.associated_blob_ids; recreating"
                                        );
                                    }
                                    (_, Ok(false), _, _) => {
                                        needs_recreate = true;
                                        eprintln!(
                                            "  Database {name} missing core.events.ts_orig_subnano; recreating"
                                        );
                                    }
                                    (_, _, Ok(false), _) => {
                                        needs_recreate = true;
                                        eprintln!(
                                            "  Database {name} missing core.processor_checkpoints metadata columns; recreating"
                                        );
                                    }
                                    (_, _, _, Ok(false)) => {
                                        needs_recreate = true;
                                        eprintln!(
                                            "  Database {name} missing sinex_schemas.event_payload_schemas.updated_at; recreating"
                                        );
                                    }
                                    (Err(err), _, _, _)
                                    | (_, Err(err), _, _)
                                    | (_, _, Err(err), _)
                                    | (_, _, _, Err(err)) => {
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

                        drop_database_if_exists(&mut conn, &name).await?;
                        wait_for_database_absence(&mut conn, &name).await?;

                        // Recreate from the fresh template
                        match create_database_from_template(&mut conn, &name, &template_name)
                            .await?
                        {
                            CreateDatabaseOutcome::Created => {
                                eprintln!("  Recreated pool database from template: {name}");
                            }
                            CreateDatabaseOutcome::AlreadyExists => {
                                eprintln!(
                                    "  Database {name} was recreated by another task; reusing"
                                );
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CreateDatabaseOutcome {
    Created,
    AlreadyExists,
}

async fn database_exists(conn: &mut PoolConnection<Postgres>, name: &str) -> Result<bool> {
    let exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)")
            .bind(name)
            .fetch_one(conn.as_mut())
            .await?;
    Ok(exists)
}

async fn drop_database_if_exists(conn: &mut PoolConnection<Postgres>, name: &str) -> Result<()> {
    let drop_force = sqlx::query(&format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)"))
        .execute(conn.as_mut())
        .await;

    if let Err(force_err) = drop_force {
        let fallback = sqlx::query(&format!("DROP DATABASE IF EXISTS {name}"))
            .execute(conn.as_mut())
            .await;

        if let Err(drop_err) = fallback {
            return Err(SinexError::database(format!(
                "Failed to drop database {name}: {force_err}; fallback error: {drop_err}"
            )));
        }
    }

    Ok(())
}

async fn wait_for_database_absence(conn: &mut PoolConnection<Postgres>, name: &str) -> Result<()> {
    const MAX_ATTEMPTS: usize = 20;
    for attempt in 0..MAX_ATTEMPTS {
        if !database_exists(conn, name).await? {
            return Ok(());
        }

        let delay = Duration::from_millis(50 + (attempt as u64 * 10));
        tokio::time::sleep(delay).await;
    }

    Err(SinexError::database(format!(
        "Database {name} still present after drop attempts"
    )))
}

/// Grant schema permissions to app user on a newly created pool database.
///
/// This uses the centralized permissions module which automatically grants on ALL
/// schemas including public (for seaql_migrations), eliminating hardcoded schema lists.
async fn grant_pool_database_permissions(db_name: &str) -> Result<()> {
    crate::permissions::grant_pool_database_permissions(db_name).await
}

async fn create_database_from_template(
    conn: &mut PoolConnection<Postgres>,
    name: &str,
    template_name: &str,
) -> Result<CreateDatabaseOutcome> {
    match sqlx::query(&format!(
        "CREATE DATABASE {name} WITH TEMPLATE {template_name}"
    ))
    .execute(conn.as_mut())
    .await
    {
        Ok(_) => {
            // Grant permissions on the newly created database in CI environment
            let _ = grant_pool_database_permissions(name).await;
            Ok(CreateDatabaseOutcome::Created)
        }
        Err(err) => {
            if let Error::Database(db_err) = &err {
                let duplicate_code = db_err
                    .code()
                    .as_ref()
                    .map(|c| {
                        let code = c.as_ref();
                        code == "42P04" || code == "23505"
                    })
                    .unwrap_or(false);
                if duplicate_code || db_err.message().contains("already exists") {
                    return Ok(CreateDatabaseOutcome::AlreadyExists);
                }
            }

            Err(SinexError::database(err.to_string()))
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
            if let Err(verify_err) = crate::db_common::verify_clean_state(pool).await {
                eprintln!(
                    "  ⚠️ Database {db_name} failed clean-state verification: {verify_err}. Retrying cleanup once."
                );

                // Retry once more to avoid transient race conditions
                match crate::db_common::reset_database(pool).await {
                    Ok(_) => {
                        if let Err(second_verify) = crate::db_common::verify_clean_state(pool).await
                        {
                            eprintln!(
                                "  ❌ Database {db_name} still dirty after retry: {second_verify}"
                            );
                            // Last-resort forced cleanup to clear FK-linked rows.
                            if let Err(force_err) = force_event_material_cleanup(pool).await {
                                POOL_METRICS.record_cleanup_failure();
                                log_remaining_rows(pool).await;
                                return Err(SinexError::unknown(format!(
                                    "Database {db_name} cleanup verification failed after retry: {second_verify}; forced cleanup failed: {force_err}"
                                )));
                            }
                            if let Err(final_verify) =
                                crate::db_common::verify_clean_state(pool).await
                            {
                                POOL_METRICS.record_cleanup_failure();
                                log_remaining_rows(pool).await;
                                return Err(SinexError::unknown(format!(
                                    "Database {db_name} cleanup verification failed after forced cleanup: {final_verify}"
                                )));
                            }
                        }
                    }
                    Err(retry_err) => {
                        eprintln!("  ❌ Retry cleanup for {db_name} failed: {retry_err}");
                        if let Err(force_err) = force_event_material_cleanup(pool).await {
                            POOL_METRICS.record_cleanup_failure();
                            log_remaining_rows(pool).await;
                            return Err(SinexError::unknown(format!(
                                "Database {db_name} cleanup retry failed: {retry_err}; forced cleanup failed: {force_err}"
                            )));
                        }
                        if let Err(final_verify) = crate::db_common::verify_clean_state(pool).await
                        {
                            POOL_METRICS.record_cleanup_failure();
                            log_remaining_rows(pool).await;
                            return Err(SinexError::unknown(format!(
                                "Database {db_name} cleanup failed after forced cleanup: {final_verify}"
                            )));
                        }
                    }
                }
            }

            eprintln!("  ✅ Database cleanup verified - all tables empty");
            Ok(())
        }
        Err(e) => {
            eprintln!("  ❌ CRITICAL: Database {db_name} cleanup failed: {e}");
            POOL_METRICS.record_cleanup_failure();
            log_remaining_rows(pool).await;

            // Attempt one last forced cleanup focusing on stubborn event/material rows.
            if let Err(force_err) = force_event_material_cleanup(pool).await {
                return Err(SinexError::unknown(format!(
                    "Database {db_name} cleanup failed: {e}; forced cleanup also failed: {force_err}"
                )));
            }

            if let Err(verify_err) = crate::db_common::verify_clean_state(pool).await {
                return Err(SinexError::unknown(format!(
                    "Database {db_name} cleanup failed after forced cleanup: {verify_err}"
                )));
            }

            eprintln!("  ✅ Database cleanup recovered after forced truncation");
            Ok(())
        }
    }
}

async fn log_remaining_rows(pool: &DbPool) {
    if let Ok(counts) = crate::db_common::get_row_counts(pool).await {
        for (table, count) in counts {
            if count > 0 {
                eprintln!("     - {table} has {count} rows remaining");
            }
        }
    }
}

/// Final backstop cleanup when standard reset fails (e.g., FK contention).
async fn force_event_material_cleanup(pool: &DbPool) -> Result<()> {
    let mut conn = pool.acquire().await?;
    let replication_disabled = sqlx::query("SET session_replication_role = 'replica'")
        .execute(conn.as_mut())
        .await
        .is_ok();
    let _ = sqlx::query("SET row_security = off")
        .execute(conn.as_mut())
        .await;
    let _ = sqlx::query("ALTER TABLE core.events DISABLE TRIGGER ALL")
        .execute(conn.as_mut())
        .await;
    let _ = sqlx::query("ALTER TABLE raw.temporal_ledger DISABLE TRIGGER ALL")
        .execute(conn.as_mut())
        .await;

    let mut attempts = 0;
    let mut last_events = 0_i64;
    let mut last_materials = 0_i64;

    while attempts < 3 {
        attempts += 1;

        let tables = [
            "core.event_annotations",
            "core.event_relations",
            "core.event_cluster_members",
            "core.event_embeddings",
            "core.entity_relations",
            "core.revisions",
            "core.processor_checkpoints",
            "core.operations_log",
            "core.transactional_outbox",
            "core.tags",
            "core.tagged_items",
            "core.blobs",
            "raw.temporal_ledger",
            "core.event_clusters",
            "core.entities",
        ];

        for table in tables {
            let _ = sqlx::query(&format!("DELETE FROM {table}"))
                .execute(conn.as_mut())
                .await;
        }

        // Hypertable cleanup via DELETE + drop_chunks for events and explicit material purge.
        let _ = sqlx::query("DELETE FROM core.events")
            .execute(conn.as_mut())
            .await;
        let _ =
            sqlx::query("SELECT drop_chunks('core.events', older_than => INTERVAL '0 seconds')")
                .execute(pool)
                .await;
        let _ = sqlx::query("DELETE FROM raw.source_material_registry")
            .execute(conn.as_mut())
            .await;

        let counts = crate::db_common::get_row_counts(pool)
            .await
            .unwrap_or_default();
        last_events = *counts.get("core.events").unwrap_or(&0);
        last_materials = *counts.get("raw.source_material_registry").unwrap_or(&0);
        if last_events == 0 && last_materials <= 1 {
            break;
        }
    }

    if last_events != 0 || last_materials > 1 {
        // Final aggressive delete before giving up.
        let _ = sqlx::query("DELETE FROM core.events")
            .execute(conn.as_mut())
            .await;
        let _ = sqlx::query("DELETE FROM raw.source_material_registry")
            .execute(conn.as_mut())
            .await;
    }

    let _ = sqlx::query("ALTER TABLE core.events ENABLE TRIGGER ALL")
        .execute(conn.as_mut())
        .await;
    let _ = sqlx::query("ALTER TABLE raw.temporal_ledger ENABLE TRIGGER ALL")
        .execute(conn.as_mut())
        .await;
    let _ = sqlx::query("SET row_security = on")
        .execute(conn.as_mut())
        .await;
    if replication_disabled {
        if let Err(err) = sqlx::query("SET session_replication_role = 'origin'")
            .execute(conn.as_mut())
            .await
        {
            warn!(
                "Failed to reset session_replication_role to origin after forced cleanup: {}",
                err
            );
        }
    }

    Ok(())
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
    const MAX_ATTEMPTS: usize = 10;

    for attempt in 0..MAX_ATTEMPTS {
        match tokio::time::timeout(Duration::from_secs(5), PgConnection::connect(admin_url)).await {
            Ok(Ok(conn)) => return Ok(conn),
            Ok(Err(err)) => {
                let err_str = err.to_string();
                if !err_str.to_lowercase().contains("too many clients") {
                    return Err(SinexError::database(format!(
                        "Admin connection failed: {err_str}. Ensure the local PostgreSQL instance is running and accessible (try `just db-setup`, `pg_ctl start`, or set DATABASE_URL to a reachable server)."
                    )));
                }
                last_error = Some(err);
                eprintln!(
                    "⚠️  Admin connection refused (too many clients); retrying in {:?} (attempt {}/{})",
                    delay,
                    attempt + 1,
                    MAX_ATTEMPTS
                );
            }
            Err(_) => {
                return Err(SinexError::database(
                    "Admin connection timeout. Ensure PostgreSQL is running locally.",
                ));
            }
        }

        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(Duration::from_secs(2));
    }

    Err(SinexError::database(format!(
        "Admin connection failed after retries: {}. Ensure PostgreSQL is running and reachable for tests.",
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
    // Fast-path reuse if cached template is reachable.
    if let Some(template_name) = TEMPLATE_DB_NAME.get() {
        let template_url = replace_db_name(base_url, template_name);
        if PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_millis(750))
            .connect(&template_url)
            .await
            .is_ok()
        {
            return Ok(template_name.clone());
        } else {
            eprintln!("♻️  Cached template {template_name} is inaccessible; forcing rebuild");
        }
    }

    // Acquire lock to prevent race condition between parallel tests
    let _lock = TEMPLATE_CREATION_LOCK.lock().await;

    if let Some(template_name) = TEMPLATE_DB_NAME.get() {
        let template_url = replace_db_name(base_url, template_name);
        if PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_millis(750))
            .connect(&template_url)
            .await
            .is_ok()
        {
            return Ok(template_name.clone());
        }
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
    let template_pool_max = slot_max_connections.saturating_mul(2).max(4);

    let template_url = replace_db_name(base_url, template_name);
    let template_admin_url = replace_db_name(admin_url, template_name);

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
                                let events_has_blobs = sqlx::query_scalar::<_, bool>(
                                    "SELECT EXISTS (SELECT 1 FROM information_schema.columns \
                                     WHERE table_schema = 'core' AND table_name = 'events' AND column_name = 'associated_blob_ids')",
                                )
                                .fetch_one(&pool)
                                .await;

                                let events_has_subnano = sqlx::query_scalar::<_, bool>(
                                    "SELECT EXISTS (SELECT 1 FROM information_schema.columns \
                                     WHERE table_schema = 'core' AND table_name = 'events' AND column_name = 'ts_orig_subnano')",
                                )
                                .fetch_one(&pool)
                                .await;

                                let checkpoints_has_metadata = sqlx::query_scalar::<_, bool>(
                                    "SELECT COUNT(*) = 2 FROM information_schema.columns \
                                     WHERE table_schema = 'core' AND table_name = 'processor_checkpoints' \
                                       AND column_name IN ('checkpoint_version', 'created_at')",
                                )
                                .fetch_one(&pool)
                                .await;

                                let payload_has_updated_at = sqlx::query_scalar::<_, bool>(
                                    "SELECT EXISTS (SELECT 1 FROM information_schema.columns \
                                     WHERE table_schema = 'sinex_schemas' AND table_name = 'event_payload_schemas' \
                                       AND column_name = 'updated_at')",
                                )
                                .fetch_one(&pool)
                                .await;

                                match (
                                    events_has_blobs,
                                    events_has_subnano,
                                    checkpoints_has_metadata,
                                    payload_has_updated_at,
                                ) {
                                    (Ok(true), Ok(true), Ok(true), Ok(true)) => {
                                        eprintln!(
                                            "✅ Template database {template_name} reused (migrations unchanged)"
                                        );
                                        reuse_allowed = true;
                                    }
                                    (Ok(false), _, _, _) => {
                                        eprintln!(
                                            "♻️  Template {template_name} missing core.events.associated_blob_ids; recreating"
                                        );
                                    }
                                    (_, Ok(false), _, _) => {
                                        eprintln!(
                                            "♻️  Template {template_name} missing core.events.ts_orig_subnano; recreating"
                                        );
                                    }
                                    (_, _, Ok(false), _) => {
                                        eprintln!(
                                            "♻️  Template {template_name} missing core.processor_checkpoints metadata columns; recreating"
                                        );
                                    }
                                    (_, _, _, Ok(false)) => {
                                        eprintln!(
                                            "♻️  Template {template_name} missing sinex_schemas.event_payload_schemas.updated_at; recreating"
                                        );
                                    }
                                    (Err(err), _, _, _)
                                    | (_, Err(err), _, _)
                                    | (_, _, Err(err), _)
                                    | (_, _, _, Err(err)) => {
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
    match tokio::time::timeout(
        Duration::from_secs(10),
        sqlx::query(&create_query).execute(&mut admin_conn),
    )
    .await
    {
        Ok(Ok(_)) => {}
        Ok(Err(err)) => {
            let err_str = err.to_string();
            if err_str.contains("already exists") || err_str.contains("duplicate key value") {
                eprintln!(
                    "  Template database {template_name} already exists; reusing existing instance"
                );
            } else {
                return Err(SinexError::database(format!(
                    "Create database failed: {err}"
                )));
            }
        }
        Err(_) => {
            return Err(SinexError::database("Create database timeout"));
        }
    }

    // Connect to template database and run all migrations
    let template_pool_future = async {
        // Use DATABASE_URL_SUPERUSER if available (CI environment), otherwise use admin URL
        let template_migration_url = std::env::var("DATABASE_URL_SUPERUSER")
            .unwrap_or_else(|_| template_admin_url.clone())
            .replace("/sinex_dev", &format!("/{}", template_name));

        let template_pool: DbPool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(template_pool_max)
            .min_connections(1)
            .max_lifetime(Duration::from_secs(300))
            .idle_timeout(Duration::from_secs(10))
            .acquire_timeout(Duration::from_secs(15)) // Increased for parallel template operations
            .connect(&template_migration_url)
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
        // reads DATABASE_URL, so temporarily point it at the template DB with superuser credentials.
        let prev_db_url = std::env::var("DATABASE_URL").ok();
        std::env::set_var("DATABASE_URL", &template_migration_url);

        let migrate_result = tokio::time::timeout(
            Duration::from_secs(30),
            sinex_core::db::run_migrations_for_url(&template_migration_url),
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

        // Grant schema permissions to the non-superuser role for template database operations
        // Uses centralized permissions module which grants on ALL schemas (including public)
        if let Some(granter) = crate::permissions::PermissionGranter::from_env()? {
            if let Some(username) = std::env::var("DATABASE_URL_APP")
                .ok()
                .and_then(|url| url.split("://").nth(1).and_then(|s| s.split('@').next().map(|u| u.to_string())))
            {
                eprintln!("  🔑 Granting schema permissions to user '{username}' in template database");

                // Use the centralized granter to grant all schemas
                use sinex_schema::schema_registry;
                for schema in schema_registry::SINEX_SCHEMAS {
                    if let Err(e) = granter.grant_schema_access(&template_pool, schema.name).await {
                        tracing::warn!(
                            error = %e,
                            schema = schema.name,
                            "Failed to grant permissions on schema in template database"
                        );
                    }
                }
            }
        }

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
                $1::uuid::ulid,
                'annex',
                'test-material-bootstrap',
                'completed',
                'realtime',
                '{}'::jsonb
            )
            ON CONFLICT (id) DO NOTHING
            "#,
        )
        .bind(BOOTSTRAP_MATERIAL_ID.as_uuid())
        .execute(&template_pool)
        .await?;

        // Ensure canonical bootstrap material exists for test events
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
                $1::uuid::ulid,
                'annex',
                'test-material-bootstrap',
                'completed',
                'realtime',
                '{}'::jsonb
            )
            ON CONFLICT (source_identifier) DO UPDATE
            SET id = EXCLUDED.id,
                status = EXCLUDED.status,
                timing_info_type = EXCLUDED.timing_info_type,
                metadata = EXCLUDED.metadata
            "#,
        )
        .bind(BOOTSTRAP_MATERIAL_ID.as_uuid())
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
    if TEMPLATE_DB_NAME.get().is_none() {
        TEMPLATE_DB_NAME
            .set(template_name.to_string())
            .map_err(|_| SinexError::unknown("Failed to cache template database name"))?;
    }

    if let Err(e) = sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(lock_key)
        .execute(&mut admin_conn)
        .await
    {
        eprintln!("⚠️  Failed to release template advisory lock for {template_name}: {e}");
    }

    admin_conn.close().await?;

    Ok(template_name.to_string())
}

/// Check if required PostgreSQL extensions are available
async fn check_required_extensions(pool: &DbPool) -> Result<()> {
    let required_extensions = [
        ("ulid", "ULID extension for primary keys"),
        ("timescaledb", "TimescaleDB for hypertable partitioning"),
    ];
    let optional_extensions = [
        ("pg_jsonschema", "pg_jsonschema for JSON validation"),
        ("vector", "pgvector for vector similarity search"),
    ];

    let mut missing_required = Vec::new();
    for (ext_name, description) in required_extensions {
        let available: Option<String> =
            sqlx::query_scalar("SELECT name FROM pg_available_extensions WHERE name = $1")
                .bind(ext_name)
                .fetch_optional(pool)
                .await?;

        if available.is_none() {
            if ext_name == "ulid" {
                install_ulid_compat_layer(pool).await?;
                continue;
            }
            missing_required.push(format!("{ext_name} ({description})"));
            continue;
        }

        ensure_extension_installed(pool, ext_name).await?;
    }

    if !missing_required.is_empty() {
        return Err(SinexError::database(format!(
            "Missing required PostgreSQL extensions: {}",
            missing_required.join(", ")
        )));
    }

    let mut missing_optional = Vec::new();
    for (ext_name, description) in optional_extensions {
        let available: Option<String> =
            sqlx::query_scalar("SELECT name FROM pg_available_extensions WHERE name = $1")
                .bind(ext_name)
                .fetch_optional(pool)
                .await?;

        if available.is_none() {
            missing_optional.push((ext_name.to_string(), description.to_string()));
            continue;
        }

        if let Err(err) = ensure_extension_installed(pool, ext_name).await {
            warn!(
                "Failed to auto-install optional extension '{}': {}",
                ext_name, err
            );
            missing_optional.push((ext_name.to_string(), description.to_string()));
        }
    }

    if !missing_optional.is_empty() {
        let mut guard = OPTIONAL_EXTENSION_MISSING.lock();
        for (ext_name, description) in missing_optional {
            if guard
                .insert(ext_name.clone(), description.clone())
                .is_none()
            {
                warn!(
                    "Optional PostgreSQL extension '{}' unavailable; related features/tests will be skipped ({})",
                    ext_name, description
                );
            }
        }
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

/// Check whether an optional database extension was unavailable during setup.
pub fn optional_extension_missing(name: &str) -> bool {
    OPTIONAL_EXTENSION_MISSING.lock().contains_key(name)
}

async fn ensure_extension_installed(pool: &DbPool, extension: &str) -> Result<()> {
    let installed: Option<String> =
        sqlx::query_scalar("SELECT extname FROM pg_extension WHERE extname = $1")
            .bind(extension)
            .fetch_optional(pool)
            .await?;

    if installed.is_some() {
        return Ok(());
    }

    let available: Option<String> =
        sqlx::query_scalar("SELECT name FROM pg_available_extensions WHERE name = $1")
            .bind(extension)
            .fetch_optional(pool)
            .await?;

    if available.is_none() && extension == "ulid" {
        install_ulid_compat_layer(pool).await?;
        return Ok(());
    } else if available.is_none() {
        return Err(SinexError::database(format!(
            "Extension {extension} is not available in the current PostgreSQL installation"
        )));
    }

    let create_stmt = format!("CREATE EXTENSION IF NOT EXISTS {extension}");
    sqlx::query(&create_stmt).execute(pool).await.map_err(|e| {
        SinexError::database(format!("Failed to create extension {extension}: {e}"))
    })?;

    Ok(())
}

async fn install_ulid_compat_layer(pool: &DbPool) -> Result<()> {
    warn!("ULID extension unavailable; installing compatibility shim for tests");
    sqlx::query("CREATE EXTENSION IF NOT EXISTS pgcrypto")
        .execute(pool)
        .await
        .map_err(|e| SinexError::database(format!("Failed to enable pgcrypto: {e}")))?;

    sqlx::query(
        r#"
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'ulid') THEN
                EXECUTE 'CREATE DOMAIN ulid AS uuid';
            END IF;
        END
        $$;
        "#,
    )
    .execute(pool)
    .await
    .map_err(|e| SinexError::database(format!("Failed to create ULID domain shim: {e}")))?;

    sqlx::query(
        r#"
        CREATE OR REPLACE FUNCTION gen_ulid() RETURNS uuid
        LANGUAGE SQL
        STABLE
        AS $$ SELECT gen_random_uuid() $$;
        "#,
    )
    .execute(pool)
    .await
    .map_err(|e| SinexError::database(format!("Failed to create gen_ulid() shim: {e}")))?;

    sqlx::query(
        r#"
        CREATE OR REPLACE FUNCTION pgx_ulid_generate() RETURNS uuid
        LANGUAGE SQL
        STABLE
        AS $$ SELECT gen_ulid() $$;
        "#,
    )
    .execute(pool)
    .await
    .map_err(|e| SinexError::database(format!("Failed to create pgx_ulid_generate() shim: {e}")))?;

    Ok(())
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
        // Set operation_id for RLS policies
        if let Err(e) =
            sqlx::query("SELECT set_config('sinex.operation_id', 'template-setup', false)")
                .execute(pool)
                .await
        {
            eprintln!("⚠️  Could not set operation_id: {e}");
        }

        sqlx::query("DELETE FROM core.events WHERE source LIKE 'test_%'")
            .execute(pool)
            .await
            .unwrap_or_else(|_| {
                eprintln!("⚠️  Could not clean test data");
                Default::default()
            });

        // Reset operation_id
        let _ = sqlx::query("RESET sinex.operation_id").execute(pool).await;

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

/// Current number of slots available in the database pool.
pub async fn pool_slot_count() -> usize {
    let pool_lock = POOL.lock().await;
    pool_lock.as_ref().map(|pool| pool.slots.len()).unwrap_or(0)
}

/// Acquire a connection to the Postgres admin database with retry logic.
pub async fn acquire_admin_connection() -> Result<PgConnection> {
    let config = PoolConfig::default();
    connect_admin_with_retry(&config.admin_url).await
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

/// Execute a future with a temporary pool size, restoring the original configuration afterwards.
pub async fn with_pool_size<F, Fut, T>(size: usize, f: F) -> Result<T>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let previous = std::env::var("SINEX_TESTUTILS_POOL_SIZE").ok();
    std::env::set_var("SINEX_TESTUTILS_POOL_SIZE", size.to_string());
    reset_pool().await?;

    let result = f().await;

    if let Some(prev) = previous {
        std::env::set_var("SINEX_TESTUTILS_POOL_SIZE", prev);
    } else {
        std::env::remove_var("SINEX_TESTUTILS_POOL_SIZE");
    }

    reset_pool().await?;
    result
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
