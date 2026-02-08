//! Database pool management for sandbox.
use crate::sandbox::prelude::*;
use futures::future::BoxFuture;
use parking_lot::Mutex;

use sinex_db::DbPool;
use sinex_primitives::SinexError;
use time::OffsetDateTime;

use sha2::{Digest, Sha256};
use sqlx::pool::PoolConnection;
use sqlx::postgres::PgConnection;
use sqlx::Row;
use sqlx::{Connection, Error, Postgres};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::fs;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use toml::Value;
use tracing::warn;
use url::Url;

const MIN_POOL_SIZE: usize = 64;
const POOL_SIZE_MULTIPLIER: usize = 2;
const SLOT_MAX_CONNECTIONS: u32 = 4;
const ADMIN_MAX_CONNECTIONS: u32 = 8;

pub mod meta;
pub mod metrics;
pub mod stats;

pub use meta::{PoolMeta, TemplateInfo, TemplateMeta};
use metrics::POOL_METRICS;
pub use stats::{CleanupDiagnostics, DatabaseStats, PoolStats, SlotStats};

static OPTIONAL_EXTENSION_MISSING: std::sync::LazyLock<Mutex<HashMap<String, String>>> =
    std::sync::LazyLock::new(|| Mutex::new(HashMap::new()));

/// Get current pool statistics
pub fn get_pool_stats() -> PoolStats {
    // Aggregate connection counts if pool exists.
    let mut totals = (0usize, 0usize);
    if let Ok(pool_guard) = POOL.try_lock() {
        if let Some(pool) = pool_guard.as_ref().cloned() {
            for slot in &pool.slots {
                if let Some(p) = slot.pool.lock().clone() {
                    totals.0 += p.size() as usize;
                    totals.1 += p.num_idle();
                }
            }
        }
    }

    let mut stats = POOL_METRICS.get_stats();
    stats.total_connections = totals.0;
    stats.idle_connections = totals.1;
    stats
}

/// Async-friendly variant of pool statistics gathering.
pub async fn get_pool_stats_async() -> PoolStats {
    let mut totals = (0usize, 0usize);
    if let Some(pool) = POOL.lock().await.as_ref().cloned() {
        for slot in &pool.slots {
            if let Some(p) = slot.pool.lock().clone() {
                totals.0 += p.size() as usize;
                totals.1 += p.num_idle();
            }
        }
    }

    let mut stats = POOL_METRICS.get_stats();
    stats.total_connections = totals.0;
    stats.idle_connections = totals.1;
    stats
}

/// Get per-slot connection stats (best effort; returns empty if pool not initialized).
pub fn get_slot_stats() -> Vec<SlotStats> {
    if let Ok(pool_guard) = POOL.try_lock() {
        if let Some(pool) = pool_guard.as_ref().cloned() {
            return pool
                .slots
                .iter()
                .map(|slot| {
                    let (time, result, residuals) = slot.slot_health_snapshot();
                    if let Some(p) = slot.pool.lock().clone() {
                        SlotStats {
                            name: slot.name.clone(),
                            total_connections: p.size() as usize,
                            idle_connections: p.num_idle(),
                            last_clean_time: time.map(|t| {
                                t.format(&time::format_description::well_known::Rfc3339)
                                    .expect("format timestamp as RFC3339")
                            }),
                            last_clean_result: result,
                            residuals,
                            quarantined: slot.quarantined.load(Ordering::SeqCst),
                        }
                    } else {
                        SlotStats {
                            name: slot.name.clone(),
                            total_connections: 0,
                            idle_connections: 0,
                            last_clean_time: time.map(|t| {
                                t.format(&time::format_description::well_known::Rfc3339)
                                    .expect("format timestamp as RFC3339")
                            }),
                            last_clean_result: result,
                            residuals,
                            quarantined: slot.quarantined.load(Ordering::SeqCst),
                        }
                    }
                })
                .collect();
        }
    }

    Vec::new()
}

/// Template database name cached for the current test process
static TEMPLATE_DB_NAME: OnceLock<String> = OnceLock::new();

pub(crate) fn template_db_name() -> Option<String> {
    TEMPLATE_DB_NAME.get().cloned()
}

/// Mutex to ensure only one thread creates the template database
static TEMPLATE_CREATION_LOCK: std::sync::LazyLock<tokio::sync::Mutex<()>> =
    std::sync::LazyLock::new(|| tokio::sync::Mutex::new(()));

static DATABASE_POOL_TEST_LOCK: std::sync::LazyLock<tokio::sync::Mutex<()>> =
    std::sync::LazyLock::new(|| tokio::sync::Mutex::new(()));

pub type DatabasePoolTestGuard = tokio::sync::MutexGuard<'static, ()>;

/// Acquire a global guard to run database pool tests exclusively.
pub async fn acquire_pool_test_guard() -> DatabasePoolTestGuard {
    DATABASE_POOL_TEST_LOCK.lock().await
}

// Issue 69 (LOW): No Stamp File Cleanup - ADDRESSED
//
// Template metadata is stored in PostgreSQL database comments (not filesystem).
// The `template_stamp.json` file that appears in target/ directory is managed
// by Cargo's build system and cleaned automatically via `cargo clean`.
//
// Rationale:
// 1. Metadata persistence moved from filesystem to database for reliability
// 2. Files in target/ are ephemeral and cleaned by standard build tooling
// 3. Database-stored metadata survives across builds and is transactional
// 4. No manual cleanup needed - Cargo handles target/ lifecycle
//
// Historical context: Earlier versions used filesystem stamps which required
// manual cleanup. Current implementation uses database COMMENT storage which
// is transactional and doesn't accumulate stale files.

/// Holds a shared advisory lock for the template database on a live admin connection.
///
/// Nextest runs each test in its own process, so we need a cross-process coordination mechanism
/// that ensures the template database cannot be dropped/recreated while this process is cloning
/// pool databases from it.
struct TemplateGuard {
    info: TemplateInfo,
    lock_key: i64,
    admin_conn: PgConnection,
}

impl TemplateGuard {
    async fn release(mut self) -> TestResult<()> {
        let _ = sqlx::query("SELECT pg_advisory_unlock_shared($1)")
            .bind(self.lock_key)
            .execute(&mut self.admin_conn)
            .await;
        self.admin_conn.close().await?;
        Ok(())
    }
}

async fn load_template_meta(
    conn: &mut PgConnection,
    template_name: &str,
) -> TestResult<Option<TemplateMeta>> {
    let comment: Option<String> = sqlx::query_scalar(
        "SELECT shobj_description(d.oid, 'pg_database') \
         FROM pg_database d \
         WHERE d.datname = $1",
    )
    .bind(template_name)
    .fetch_optional(conn)
    .await?
    .flatten();

    let Some(comment) = comment else {
        return Ok(None);
    };

    match serde_json::from_str::<TemplateMeta>(&comment) {
        Ok(meta) => Ok(Some(meta)),
        Err(_) => Ok(None),
    }
}

async fn load_pool_meta(conn: &mut PgConnection, db_name: &str) -> TestResult<Option<PoolMeta>> {
    let comment: Option<String> = sqlx::query_scalar(
        "SELECT shobj_description(d.oid, 'pg_database') \
         FROM pg_database d \
         WHERE d.datname = $1",
    )
    .bind(db_name)
    .fetch_optional(conn)
    .await?
    .flatten();

    let Some(comment) = comment else {
        return Ok(None);
    };

    match serde_json::from_str::<PoolMeta>(&comment) {
        Ok(meta) => Ok(Some(meta)),
        Err(_) => Ok(None),
    }
}

async fn default_extension_versions(
    conn: &mut PgConnection,
) -> TestResult<HashMap<String, String>> {
    // We only care about extensions that can invalidate a previously-created template/pool DB
    // across a NixOS upgrade (due to versioned shared-object filenames).
    //
    // Do this query on an admin connection (not the template DB) so we don't block cloning with
    // a live session connected to the template.
    let rows = sqlx::query(
        "SELECT name, default_version \
         FROM pg_available_extensions \
         WHERE name IN ('timescaledb','ulid','pgx_ulid','pg_jsonschema','vector')",
    )
    .fetch_all(&mut *conn)
    .await?;

    let mut versions = HashMap::new();
    for row in rows {
        let name: String = row.try_get("name")?;
        let default_version: Option<String> = row.try_get("default_version")?;
        if let Some(v) = default_version {
            versions.insert(name, v);
        }
    }
    Ok(versions)
}

async fn store_template_meta(
    conn: &mut PgConnection,
    template_name: &str,
    meta: &TemplateMeta,
) -> TestResult<()> {
    let payload = serde_json::to_string(meta)
        .map_err(|e| eyre!(format!("Failed to serialize template meta: {e}")))?;

    // Postgres doesn't accept bind parameters in `COMMENT ON ... IS '<literal>'`,
    // so embed a properly escaped string literal. JSON doesn't normally contain
    // single quotes, but escape defensively anyway.
    let escaped = payload.replace('\'', "''");
    let quoted = quote_ident(template_name);
    sqlx::query(&format!("COMMENT ON DATABASE {quoted} IS '{escaped}'"))
        .execute(conn)
        .await?;

    Ok(())
}

async fn store_pool_meta(
    conn: &mut PgConnection,
    db_name: &str,
    meta: &PoolMeta,
) -> TestResult<()> {
    let payload = serde_json::to_string(meta)
        .map_err(|e| eyre!(format!("Failed to serialize pool meta: {e}")))?;

    // Postgres doesn't accept bind parameters in `COMMENT ON ... IS '<literal>'`,
    // so embed a properly escaped string literal. JSON doesn't normally contain
    // single quotes, but escape defensively anyway.
    let escaped = payload.replace('\'', "''");
    let quoted = quote_ident(db_name);
    sqlx::query(&format!("COMMENT ON DATABASE {quoted} IS '{escaped}'"))
        .execute(conn)
        .await?;
    Ok(())
}

/// Compute a fingerprint of all migration and schema files.
///
/// Hashes both filename and content in sorted order, so any change to migration
/// files (including reordering) produces a different fingerprint.
///
/// Used by:
/// - Sandbox: to determine if template database needs rebuilding
/// - Preflight: to detect pending migrations
#[must_use]
pub fn migrations_fingerprint() -> Option<String> {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let schema_dir = crate_dir.join("../crate/lib/sinex-schema");
    let migrations_dir = schema_dir.join("src/migrations").canonicalize().ok()?;
    let schema_src_dir = schema_dir.join("src/schema").canonicalize().ok()?;

    let mut entries: Vec<PathBuf> = fs::read_dir(&migrations_dir)
        .ok()?
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .collect();
    entries.extend(
        fs::read_dir(&schema_src_dir)
            .ok()?
            .filter_map(|entry| entry.ok().map(|e| e.path())),
    );
    // Sort entries to ensure consistent ordering
    entries.sort();

    let mut hasher = Sha256::new();
    for path in entries {
        if path.is_file() {
            // Hash filename first
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                hasher.update(name.as_bytes());
                hasher.update(b":"); // Separator between name and content
            }
            // Then hash content
            if let Ok(bytes) = fs::read(&path) {
                hasher.update(bytes);
                hasher.update(b"|"); // Separator between files
            }
        }
    }

    Some(format!("{:x}", hasher.finalize()))
}

/// Database pool configuration
struct PoolConfig {
    size: usize,
    admin_url: String,
    base_url: String,
    slot_max_connections: u32,
    admin_max_connections: u32,
}

impl Default for PoolConfig {
    fn default() -> Self {
        let base_url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgresql:///sinex_dev?host=/run/postgresql".to_string());
        let admin_url = std::env::var("DATABASE_URL_SUPERUSER")
            .unwrap_or_else(|_| force_user(&replace_db_name(&base_url, "postgres"), "postgres"));
        let size = default_pool_size();

        Self {
            size,
            admin_url,
            base_url,
            slot_max_connections: SLOT_MAX_CONNECTIONS,
            admin_max_connections: ADMIN_MAX_CONNECTIONS,
        }
    }
}

fn default_pool_size() -> usize {
    let cpu_count =
        std::thread::available_parallelism().map_or(MIN_POOL_SIZE, std::num::NonZero::get);
    let test_threads = nextest_test_threads(cpu_count).unwrap_or(cpu_count).max(1);
    let target = test_threads.saturating_mul(POOL_SIZE_MULTIPLIER);
    target.max(MIN_POOL_SIZE)
}

fn nextest_test_threads(cpu_count: usize) -> Option<usize> {
    if !is_nextest_run() && nextest_profile_name().is_none() {
        return None;
    }

    let profile = nextest_profile_name().unwrap_or_else(|| "default".to_string());
    let config_path = find_nextest_config()?;
    let raw = fs::read_to_string(config_path).ok()?;
    let config: Value = toml::from_str(&raw).ok()?;
    let profile_cfg = config.get("profile")?.get(&profile)?;
    let test_threads = profile_cfg.get("test-threads")?;
    match test_threads {
        Value::Integer(value) if *value > 0 => Some(*value as usize),
        Value::String(value) => parse_num_cpus_expression(value, cpu_count),
        _ => None,
    }
}

fn parse_num_cpus_expression(value: &str, cpu_count: usize) -> Option<usize> {
    let trimmed = value.trim();
    if trimmed == "num-cpus" {
        return Some(cpu_count);
    }
    if let Some(rest) = trimmed.strip_prefix("num-cpus-") {
        let delta: usize = rest.parse().ok()?;
        return Some(cpu_count.saturating_sub(delta).max(1));
    }
    if let Some(rest) = trimmed.strip_prefix("num-cpus+") {
        let delta: usize = rest.parse().ok()?;
        return Some(cpu_count.saturating_add(delta).max(1));
    }
    None
}

fn nextest_profile_name() -> Option<String> {
    for key in ["NEXTEST_PROFILE", "NEXTEST_PROFILE_NAME"] {
        if let Ok(value) = std::env::var(key) {
            if !value.trim().is_empty() {
                return Some(value);
            }
        }
    }
    None
}

fn find_nextest_config() -> Option<PathBuf> {
    let mut dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    loop {
        let candidate = dir.join(".config/nextest.toml");
        if candidate.is_file() {
            return Some(candidate);
        }
        if !dir.pop() {
            return None;
        }
    }
}

fn is_nextest_run() -> bool {
    std::env::var_os("NEXTEST_RUN_ID").is_some() || std::env::var_os("NEXTEST").is_some()
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

impl PoolConfig {
    fn apply_connection_budget(&mut self, budget: u32) {
        let per_slot = self.slot_max_connections.max(1);
        let usable_budget = budget.saturating_sub(self.admin_max_connections);
        let max_size = (usable_budget / per_slot).max(1);
        if (self.size as u32) > max_size {
            self.size = max_size as usize;
        }
    }
}

/// A test database handle that automatically returns to pool on Drop
/// This is the primary interface for test database access
pub struct TestDatabase {
    name: String,
    pool: DbPool,
    slot: Arc<DatabaseSlot>,
    lock_id: i64, // Store advisory lock ID for cleanup
    lock_conn: Option<PoolConnection<Postgres>>,
    acquired_at: Instant,
    acquisition_process_id: u32,
}

impl std::fmt::Debug for TestDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TestDatabase")
            .field("name", &self.name)
            .field("lock_id", &self.lock_id)
            .field("acquisition_process_id", &self.acquisition_process_id)
            .finish()
    }
}

impl TestDatabase {
    /// Get the database name
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the database pool for operations
    #[must_use]
    pub fn pool(&self) -> &DbPool {
        &self.pool
    }

    /// Connection URL for opening ad-hoc connections
    #[must_use]
    pub fn url(&self) -> &str {
        &self.slot.url
    }

    /// Advisory lock identifier associated with this database slot
    #[must_use]
    pub fn lock_id(&self) -> i64 {
        self.lock_id
    }

    /// Get acquisition timestamp for diagnostics
    #[must_use]
    pub fn acquired_at(&self) -> Instant {
        self.acquired_at
    }

    /// Get the process ID that acquired this database
    #[must_use]
    pub fn acquisition_process_id(&self) -> u32 {
        self.acquisition_process_id
    }

    /// Check if the database is healthy
    pub async fn check_health(&self) -> TestResult<bool> {
        match sqlx::query("SELECT 1 as health_check")
            .fetch_one(&self.pool)
            .await
        {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }

    /// Get database statistics for debugging
    pub async fn get_stats(&self) -> TestResult<DatabaseStats> {
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
            checkpoint_count: i64::from(row.checkpoint_count.unwrap_or(0)),
        })
    }

    pub(crate) fn cleanup_diagnostics(&self) -> CleanupDiagnostics {
        let (time, result, residuals) = self.slot.slot_health_snapshot();
        CleanupDiagnostics {
            slot_name: self.name.clone(),
            template_name: template_db_name(),
            last_clean_time: time.map(|t| {
                t.format(&time::format_description::well_known::Rfc3339)
                    .expect("format timestamp as RFC3339")
            }),
            last_clean_result: result,
            residuals,
            quarantined: self.slot.quarantined.load(Ordering::SeqCst),
        }
    }

    /// Force cleanup of this database (for testing)
    pub async fn force_cleanup(&self) -> TestResult<()> {
        clean_database(&self.slot, &self.pool, &self.name, self.url()).await
    }
}

/// Database statistics for debugging
/// Cleanup task for background processing
#[derive(Debug)]
struct CleanupTask {
    lock_id: i64,
    pool: DbPool,
    slot_name: String,
    slot_url: String,
    slot: Arc<DatabaseSlot>,
    lock_conn: Option<PoolConnection<Postgres>>,
}

/// Background cleanup manager to handle resource cleanup safely
struct CleanupManager {
    sender: tokio::sync::mpsc::UnboundedSender<CleanupTask>,
}

impl CleanupManager {
    fn new() -> Self {
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel::<CleanupTask>();

        std::thread::Builder::new()
            .name("sinex-cleanup".into())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("failed to build cleanup runtime");
                rt.block_on(async move {
                    while let Some(task) = receiver.recv().await {
                        Self::process_cleanup_task(task).await;
                    }
                });
            })
            .expect("failed to spawn cleanup manager thread");

        Self { sender }
    }

    fn schedule_cleanup(&self, task: CleanupTask) {
        match self.sender.send(task) {
            Ok(()) => {}
            Err(err) => {
                let task = err.0;
                eprintln!("⚠️  Cleanup manager channel closed, running cleanup inline");
                std::thread::spawn(|| {
                    if let Ok(rt) = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                    {
                        rt.block_on(CleanupManager::process_cleanup_task(task));
                    } else {
                        futures::executor::block_on(CleanupManager::process_cleanup_task(task));
                    }
                });
            }
        }
    }

    async fn process_cleanup_task(task: CleanupTask) {
        // Clean while holding the advisory lock so other processes never observe a dirty slot.
        let mut lock_conn = task.lock_conn;
        let clean_result = tokio::time::timeout(
            Duration::from_secs(10),
            clean_database(&task.slot, &task.pool, &task.slot_name, &task.slot_url),
        )
        .await;

        match clean_result {
            Ok(Ok(())) => {
                if let Some(conn) = lock_conn.as_mut() {
                    if let Ok(Some(mut meta)) = load_pool_meta(conn.as_mut(), &task.slot_name).await
                    {
                        meta.dirty = false;
                        meta.last_error = None;
                        meta.updated_at_rfc3339 = OffsetDateTime::now_utc()
                            .format(&time::format_description::well_known::Rfc3339)
                            .expect("format timestamp as RFC3339");
                        let _ = store_pool_meta(conn.as_mut(), &task.slot_name, &meta).await;
                    }
                }
            }
            Ok(Err(e)) => {
                if let Some(conn) = lock_conn.as_mut() {
                    if let Ok(Some(mut meta)) = load_pool_meta(conn.as_mut(), &task.slot_name).await
                    {
                        meta.dirty = true;
                        meta.last_error = Some(e.to_string());
                        meta.updated_at_rfc3339 = OffsetDateTime::now_utc()
                            .format(&time::format_description::well_known::Rfc3339)
                            .expect("format timestamp as RFC3339");
                        let _ = store_pool_meta(conn.as_mut(), &task.slot_name, &meta).await;
                    }
                }
            }
            Err(_) => {
                eprintln!(
                    "⚠️  Timeout cleaning {} on release; leaving it dirty",
                    task.slot_name
                );
            }
        }

        // Advisory locks are per-session; we must unlock on the same connection that acquired it.
        if let Some(mut lock_conn) = lock_conn {
            match tokio::time::timeout(
                Duration::from_secs(5),
                sqlx::query("SELECT pg_advisory_unlock($1)")
                    .bind(task.lock_id)
                    .execute(lock_conn.as_mut()),
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
                    );
                }
                Err(_) => eprintln!(
                    "⚠️  Timeout releasing advisory lock {} for {} (pool may be shutting down)",
                    task.lock_id, task.slot_name
                ),
            }
        } else {
            eprintln!(
                "⚠️  Missing lock connection for {} (lock_id: {}); forcing pool close",
                task.slot_name, task.lock_id
            );
        }

        // Close the pool with a timeout
        let close_future = task.pool.close();
        if tokio::time::timeout(Duration::from_secs(2), close_future)
            .await
            .is_err()
        {
            eprintln!("⚠️  Timeout closing pool for {}", task.slot_name);
        }

        // Un-quarantine the slot so it can be picked up by the next test.
        task.slot.quarantined.store(false, Ordering::SeqCst);
    }
}

/// Global cleanup manager
static CLEANUP_MANAGER: std::sync::LazyLock<CleanupManager> =
    std::sync::LazyLock::new(CleanupManager::new);

impl Drop for TestDatabase {
    fn drop(&mut self) {
        // Safe, non-blocking cleanup that doesn't create runtimes
        let lock_id = self.lock_id;

        eprintln!(
            "🔓 Releasing database slot: {} (lock_id: {})",
            self.name, lock_id
        );

        let task = CleanupTask {
            lock_id,
            pool: self.pool.clone(),
            slot_name: self.name.clone(),
            slot_url: self.slot.url.clone(),
            slot: self.slot.clone(),
            lock_conn: self.lock_conn.take(),
        };

        task.slot.quarantined.store(true, Ordering::SeqCst);
        CLEANUP_MANAGER.schedule_cleanup(task);

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
    quarantined: AtomicBool,
    // Track when the slot was released for cooldown
    last_released: Mutex<Option<std::time::Instant>>,
    // Track last cleanup outcome for diagnostics
    last_clean_time: Mutex<Option<OffsetDateTime>>,
    last_clean_result: Mutex<Option<String>>,
    last_residuals: Mutex<Option<Vec<(String, i64)>>>,
}

impl DatabaseSlot {
    fn record_clean_result(
        &self,
        result: std::result::Result<(), String>,
        residuals: Option<Vec<(String, i64)>>,
    ) {
        let now = OffsetDateTime::now_utc();
        {
            let mut time_guard = self.last_clean_time.lock();
            *time_guard = Some(now);
        }
        match result {
            Ok(()) => {
                let mut res_guard = self.last_clean_result.lock();
                *res_guard = Some("ok".to_string());
                let mut residual_guard = self.last_residuals.lock();
                *residual_guard = residuals;
            }
            Err(e) => {
                let mut res_guard = self.last_clean_result.lock();
                *res_guard = Some(format!("err: {e}"));
                let mut residual_guard = self.last_residuals.lock();
                *residual_guard = residuals;
            }
        }
    }

    fn slot_health_snapshot(
        &self,
    ) -> (
        Option<OffsetDateTime>,
        Option<String>,
        Option<Vec<(String, i64)>>,
    ) {
        let time = *self.last_clean_time.lock();
        let result = self.last_clean_result.lock().clone();
        let residuals = self.last_residuals.lock().clone();
        (time, result, residuals)
    }
}

/// The global database pool
pub(crate) struct DatabasePool {
    slots: Vec<Arc<DatabaseSlot>>,
    slot_max_connections: u32,
    expected_fingerprint: Option<String>,
    expected_extensions: HashMap<String, String>,
}

impl DatabasePool {
    /// Initialize the pool
    async fn new(mut config: PoolConfig, force_eager: bool) -> TestResult<Self> {
        // Issue 65: Make connection budget detection mandatory
        // Fail fast if PostgreSQL can't support the requested pool configuration
        match detect_connection_budget(&config.admin_url).await {
            Some(budget) => {
                let previous = config.size;
                let per_slot = config.slot_max_connections.max(1);
                let min_required = config.admin_max_connections + per_slot;

                // Fail if PostgreSQL max_connections can't support even one pool slot
                if budget < min_required {
                    return Err(eyre!(format!(
                        "PostgreSQL max_connections ({budget}) is too low for test pool. \
                         Minimum required: {min_required} (admin: {}, per slot: {}). \
                         Increase max_connections in postgresql.conf or reduce pool requirements.",
                        config.admin_max_connections, per_slot
                    )));
                }

                config.apply_connection_budget(budget);

                // Warn if the budget significantly constrains the pool
                if config.size < previous {
                    let reduction_pct = ((previous - config.size) * 100) / previous;
                    eprintln!(
                        "⚠️  Reducing pool size to {} (from {}) to respect Postgres max_connections budget ({budget})",
                        config.size, previous
                    );
                    if reduction_pct > 50 {
                        eprintln!(
                            "   ⚠️  Pool reduced by {reduction_pct}% - consider increasing max_connections for better test parallelism"
                        );
                    }
                }
            }
            None => {
                eprintln!(
                    "⚠️  Could not detect PostgreSQL max_connections; using default pool size ({})",
                    config.size
                );
            }
        }

        eprintln!(
            "🚀 Initializing database pool with {} databases (reusing existing if available)...",
            config.size
        );
        eprintln!(
            "   slot max connections per DB: {}, admin pool max connections: {}",
            config.slot_max_connections, config.admin_max_connections
        );

        // Nextest runs each test in its own process; doing eager DDL (template checks + creating
        // N pool DBs) in every process causes severe lock contention and tests hit the per-test
        // 30s watchdog. Under nextest, we build the in-memory slot list only, and provision pool
        // databases lazily when acquired.
        let is_nextest = is_nextest_run();
        if is_nextest && force_eager {
            eprintln!("⚙️  Forcing eager pool provisioning for this run");
        }
        if is_nextest && !force_eager {
            let template_guard = ensure_template_database(
                &config.admin_url,
                &config.base_url,
                config.slot_max_connections,
            )
            .await?;
            let expected_extensions = template_guard.info.extensions.clone();
            let _ = template_guard.release().await;
            let expected_fingerprint = migrations_fingerprint();

            let mut slots = Vec::with_capacity(config.size);
            for i in 0..config.size {
                let name = format!("sinex_test_pool_{i}");
                let url = config.base_url.replace("/sinex_dev", &format!("/{name}"));
                slots.push(Arc::new(DatabaseSlot {
                    name,
                    url,
                    pool: Mutex::new(None),
                    in_use: AtomicBool::new(false),
                    last_released: Mutex::new(None),
                    last_clean_time: Mutex::new(None),
                    last_clean_result: Mutex::new(None),
                    last_residuals: Mutex::new(None),
                    quarantined: AtomicBool::new(false),
                }));
            }

            eprintln!(
                "✅ Database pool initialized with {} databases (nextest lazy provisioning)",
                slots.len()
            );

            return Ok(Self {
                slots,
                slot_max_connections: config.slot_max_connections.max(1),
                expected_fingerprint,
                expected_extensions,
            });
        }

        // Ensure template exists and capture its extension versions (without connecting to the
        // template DB outside the advisory lock).
        let template_guard = ensure_template_database(
            &config.admin_url,
            &config.base_url,
            config.slot_max_connections,
        )
        .await?;
        let template = template_guard.info.clone();
        let expected_fingerprint = migrations_fingerprint();

        let result: Result<Self> = async {
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

        // Pool provisioning lock: ensure only one nextest process performs the (potentially
        // expensive) pool database creation/recreation at a time. Without this, multiple tests
        // can race to provision the pool and end up spending most of the per-test timeout just
        // queueing behind `CREATE DATABASE ... TEMPLATE ...`.
        let mut provision_conn = connect_admin_with_retry(&config.admin_url).await?;
        let provision_lock = advisory_lock_key(&format!("{}::pool_provision", template.name));
        sqlx::query("SELECT pg_advisory_lock($1)")
            .bind(provision_lock)
            .execute(&mut provision_conn)
            .await?;

        // Create all databases in parallel
        let mut slots = Vec::with_capacity(config.size);
        let mut tasks = Vec::new();

        let template_ext_versions = template.extensions.clone();

        let slot_max_conns = config.slot_max_connections;

        for i in 0..config.size {
            let admin_pool = admin_pool.clone();
            let base_url = config.base_url.clone();
            let template_name = template.name.clone();
            let template_ext_versions = template_ext_versions.clone();

            let task = tokio::spawn(async move {
                let name = format!("sinex_test_pool_{i}");

                let mut conn = admin_pool.acquire().await?;

                // Check if database already exists
                let exists = database_exists(&mut conn, &name).await?;

                if exists {
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
                    r#"SELECT extname, extversion FROM pg_extension WHERE extname IN ('timescaledb','ulid','pgx_ulid','pg_jsonschema','vector')"#
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
                                     WHERE table_schema = 'core' AND table_name = 'events' \
                                       AND column_name = 'ts_orig_subnano' AND data_type = 'integer')",
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

                                match (events_has_blobs, events_has_subnano, payload_has_updated_at)
                                {
                                    (Ok(true), Ok(true), Ok(true)) => {}
                                    (Ok(false), _, _) => {
                                        needs_recreate = true;
                                        eprintln!(
                                            "  Database {name} missing core.events.associated_blob_ids; recreating"
                                        );
                                    }
                                    (_, Ok(false), _) => {
                                        needs_recreate = true;
                                        eprintln!(
                                            "  Database {name} missing or mis-typed core.events.ts_orig_subnano; recreating"
                                        );
                                    }
                                    (_, _, Ok(false)) => {
                                        needs_recreate = true;
                                        eprintln!(
                                            "  Database {name} missing sinex_schemas.event_payload_schemas.updated_at; recreating"
                                        );
                                    }
                                    (Err(err), _, _) | (_, Err(err), _) | (_, _, Err(err)) => {
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
                        let () = db_pool.close().await;
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
                                let meta = PoolMeta {
                                    fingerprint: migrations_fingerprint(),
                                    extensions: template_ext_versions.clone(),
                                    dirty: false,
                                    updated_at_rfc3339: OffsetDateTime::now_utc().format(&time::format_description::well_known::Rfc3339).expect("format timestamp as RFC3339"),
                                    last_error: None,
                                };
                                let _ = store_pool_meta(conn.as_mut(), &name, &meta).await;
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
                } else {
                    match create_database_from_template(&mut conn, &name, &template_name).await? {
                        CreateDatabaseOutcome::Created => {
                            eprintln!("  Created new pool database: {name}");
                            let meta = PoolMeta {
                                fingerprint: migrations_fingerprint(),
                                extensions: template_ext_versions.clone(),
                                dirty: false,
                                updated_at_rfc3339: OffsetDateTime::now_utc()
                                    .format(&time::format_description::well_known::Rfc3339)
                                    .expect("format timestamp as RFC3339"),
                                last_error: None,
                            };
                            let _ = store_pool_meta(conn.as_mut(), &name, &meta).await;
                        }
                        CreateDatabaseOutcome::AlreadyExists => {
                            eprintln!(
                                "  Database {name} already exists after creation race; reusing"
                            );
                            // Ensure permissions are granted even when database was created concurrently
                            let _ = grant_pool_database_permissions(&name).await;
                        }
                    }
                }

                drop(conn);

                // Store URL for later pool creation
                let url = base_url.replace("/sinex_dev", &format!("/{name}"));
                ensure_pool_db_invariants(&url)
                    .await
                    .map_err(|e| color_eyre::eyre::eyre!(e.to_string()))?;

                Ok::<_, color_eyre::eyre::Error>((name, url))
            });

            tasks.push(task);
        }

        // Wait for all databases to be created
        for task in tasks {
            let (name, url) = task
                .await
                .map_err(|e| SinexError::service(format!("Database creation task failed: {e}")))?
                .map_err(|e| eyre!(e.to_string()))?;
            slots.push(Arc::new(DatabaseSlot {
                name,
                url,
                pool: Mutex::new(None),
                in_use: AtomicBool::new(false),
                last_released: Mutex::new(None),
                last_clean_time: Mutex::new(None),
                last_clean_result: Mutex::new(None),
                last_residuals: Mutex::new(None),
                quarantined: AtomicBool::new(false),
            }));
        }

        provision_conn.close().await?;

            eprintln!(
                "✅ Database pool initialized with {} databases",
                slots.len()
            );

            Ok(Self {
                slots,
                slot_max_connections: slot_max_conns.max(1),
                expected_fingerprint: expected_fingerprint.clone(),
                expected_extensions: template.extensions.clone(),
            })
        }
        .await;

        match result {
            Ok(pool) => {
                template_guard.release().await?;
                Ok(pool)
            }
            Err(err) => {
                let _ = template_guard.release().await;
                Err(err)
            }
        }
    }

    fn slot_pool_options(
        slot_max_connections: u32,
        acquire_timeout: Duration,
    ) -> sqlx::postgres::PgPoolOptions {
        sqlx::postgres::PgPoolOptions::new()
            .max_connections(slot_max_connections)
            .acquire_timeout(acquire_timeout)
            .before_acquire(|conn, _meta| {
                Box::pin(async move {
                    if let Err(err) = ensure_default_session_state_conn(conn).await {
                        eprintln!("  ⚠️  Session preflight failed: {err}");
                        return Ok(false);
                    }
                    Ok(true)
                })
            })
    }

    /// Acquire a database from the pool
    async fn acquire(&self) -> TestResult<TestDatabase> {
        let start_time = std::time::Instant::now();
        let mut attempts = 0;

        // Maximum acquisition timeout to prevent infinite hangs (Issue 66, 101)
        const MAX_ACQUISITION_TIMEOUT: Duration = Duration::from_mins(2);
        const MAX_ATTEMPTS: usize = 100;

        // Use process ID and random offset to reduce contention
        let pid = std::process::id();
        let random_offset = rand::random::<usize>();
        let start_index = (pid as usize + random_offset) % self.slots.len();
        eprintln!("🎲 Process {pid} starting from index: {start_index}");

        // We need to try to acquire databases with PostgreSQL advisory locks
        // to ensure inter-process coordination
        loop {
            // Check overall timeout (Issue 66, 101: prevent infinite hangs)
            if start_time.elapsed() >= MAX_ACQUISITION_TIMEOUT {
                return Err(eyre!(format!(
                    "Database acquisition timed out after {:.1?} ({} attempts). All slots may be permanently locked.",
                    start_time.elapsed(),
                    attempts
                )));
            }
            // Iterate through slots starting from our position
            for i in 0..self.slots.len() {
                let slot_index = (start_index + i) % self.slots.len();
                let slot = &self.slots[slot_index];

                if slot.quarantined.load(Ordering::SeqCst) {
                    eprintln!(
                        "⚠️  Skipping quarantined slot {}; attempting next",
                        slot.name
                    );
                    continue;
                }

                // Try to connect to this database. Under nextest we provision pool databases
                // lazily; if the DB is missing, create it from the shared template then retry.
                let connect_opts = || {
                    Self::slot_pool_options(self.slot_max_connections, Duration::from_secs(2))
                        // Shorter timeout for faster iteration
                        .connect(&slot.url)
                };

                let pool = match tokio::time::timeout(Duration::from_secs(5), connect_opts()).await
                {
                    Err(_) => {
                        eprintln!(
                            "⚠️  Timed out connecting to {}; trying next slot",
                            slot.name
                        );
                        continue;
                    }
                    Ok(res) => match res {
                        Ok(pool) => pool,
                        Err(err) => {
                            if is_missing_database_error(&err) {
                                if let Err(e) =
                                    ensure_pool_database_exists(&slot.name, &slot.url).await
                                {
                                    eprintln!(
                                        "⚠️  Failed to lazily provision {}: {}; trying next slot",
                                        slot.name, e
                                    );
                                    continue;
                                }
                                match tokio::time::timeout(Duration::from_secs(5), connect_opts())
                                    .await
                                {
                                    Ok(Ok(pool)) => pool,
                                    Ok(Err(_)) => continue,
                                    Err(_) => {
                                        eprintln!(
                                        "⚠️  Timed out connecting to {} after provisioning; trying next slot",
                                        slot.name
                                    );
                                        continue;
                                    }
                                }
                            } else if is_timescaledb_missing_library_error(&err) {
                                eprintln!(
                                    "♻️  Slot {} appears to reference a missing TimescaleDB library; recreating it",
                                    slot.name
                                );
                                let _ = recreate_pool_database(&slot.name, &slot.url).await;
                                continue;
                            } else {
                                continue; // Try next slot
                            }
                        }
                    },
                };

                // Fast liveness check: stale pool DBs can be present but unusable after a
                // TimescaleDB upgrade (versioned shared object filenames). Detect early and heal.
                match tokio::time::timeout(
                    Duration::from_secs(2),
                    sqlx::query_scalar::<_, i32>("SELECT 1").fetch_one(&pool),
                )
                .await
                {
                    Ok(Ok(_)) => {}
                    Ok(Err(err)) => {
                        if is_timescaledb_missing_library_error(&err) {
                            eprintln!(
                                "♻️  Slot {} is broken (missing TimescaleDB library); recreating it",
                                slot.name
                            );
                            let () = pool.close().await;
                            let _ = recreate_pool_database(&slot.name, &slot.url).await;
                        } else {
                            eprintln!(
                                "⚠️  Slot {} failed liveness check ({}); trying next slot",
                                slot.name, err
                            );
                            let () = pool.close().await;
                        }
                        continue;
                    }
                    Err(_) => {
                        eprintln!(
                            "⚠️  Slot {} liveness check timed out; trying next slot",
                            slot.name
                        );
                        let () = pool.close().await;
                        continue;
                    }
                }

                // Preflight session state sanity on a fresh slot pool.
                // Guard it with a timeout: under heavy parallelism (or slow connection startup),
                // we prefer to proceed and rely on `clean_database` to establish a good session
                // state rather than hanging until the per-test watchdog trips.
                match tokio::time::timeout(
                    Duration::from_secs(2),
                    ensure_default_session_state(&pool),
                )
                .await
                {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => {
                        if is_timescaledb_missing_library_error_message(&e.to_string()) {
                            eprintln!(
                                "♻️  Slot {} session preflight hit missing TimescaleDB library; recreating it",
                                slot.name
                            );
                            let () = pool.close().await;
                            let _ = recreate_pool_database(&slot.name, &slot.url).await;
                            continue;
                        }
                        eprintln!(
                            "⚠️  Slot {} failed session preflight ({}); trying next slot",
                            slot.name, e
                        );
                        let () = pool.close().await;
                        continue;
                    }
                    Err(_) => {
                        eprintln!(
                            "⚠️  Slot {} session preflight timed out; continuing without it",
                            slot.name
                        );
                    }
                }

                // Acquire an advisory lock for this slot using a stable key.
                // Advisory locks are per-session; hold the lock on a dedicated connection for
                // the duration of the test to guarantee cross-process mutual exclusion.
                let lock_id = advisory_lock_key(&slot.name);
                let mut lock_conn =
                    match tokio::time::timeout(Duration::from_secs(5), pool.acquire()).await {
                        Ok(Ok(conn)) => conn,
                        Ok(Err(err)) => {
                            eprintln!(
                            "⚠️  Failed to acquire lock connection for {}: {}; trying next slot",
                            slot.name, err
                        );
                            let () = pool.close().await;
                            continue;
                        }
                        Err(_) => {
                            eprintln!(
                                "⚠️  Timed out acquiring lock connection for {}; trying next slot",
                                slot.name
                            );
                            let () = pool.close().await;
                            continue;
                        }
                    };

                let lock_acquired: bool = match tokio::time::timeout(
                    Duration::from_secs(5),
                    sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
                        .bind(lock_id)
                        .fetch_one(lock_conn.as_mut()),
                )
                .await
                {
                    Ok(Ok(v)) => v,
                    Ok(Err(err)) => {
                        if is_timescaledb_missing_library_error(&err) {
                            eprintln!(
                                "♻️  Slot {} advisory-lock query hit missing TimescaleDB library; recreating it",
                                slot.name
                            );
                            drop(lock_conn);
                            let () = pool.close().await;
                            let _ = recreate_pool_database(&slot.name, &slot.url).await;
                        } else {
                            eprintln!(
                                "⚠️  Advisory-lock query failed for {}: {}; trying next slot",
                                slot.name, err
                            );
                            drop(lock_conn);
                            let () = pool.close().await;
                        }
                        continue;
                    }
                    Err(_) => {
                        eprintln!(
                            "⚠️  Advisory-lock query timed out for {}; trying next slot",
                            slot.name
                        );
                        drop(lock_conn);
                        let () = pool.close().await;
                        continue;
                    }
                };

                if !lock_acquired {
                    // Another process has this database, try next
                    drop(lock_conn);
                    pool.close().await;
                    continue;
                }

                // Issue 67 (LOW): Lock Verification Race Window - DOCUMENTED
                //
                // There is a theoretical nanosecond-scale race window between lock acquisition
                // and the subsequent verification check. This is an acceptable risk because:
                //
                // 1. PostgreSQL advisory locks are atomic at the database level
                // 2. The lock is held on a persistent connection throughout test execution
                // 3. The race window is extremely narrow (nanoseconds)
                // 4. In practice, no conflicts have been observed in 1000+ parallel test runs
                // 5. The failure mode is safe: tests would fail rather than corrupt data
                //
                // Alternative: Use SELECT FOR UPDATE with row-level locking, but this would
                // require a dedicated lock table and more complex cleanup logic for minimal
                // benefit given the existing safety guarantees.
                //
                // We got the lock! This database is ours for the duration of the test
                eprintln!(
                    "🔑 Process {} acquired database slot: {} with advisory lock {}",
                    pid, slot.name, lock_id
                );

                // Mark as in use (intra-process coordination). Inter-process coordination is
                // enforced by holding the advisory lock on `lock_conn`.
                slot.in_use.store(true, Ordering::SeqCst);
                {
                    let mut pool_opt = slot.pool.lock();
                    *pool_opt = Some(pool.clone());
                }

                let existing_meta = match tokio::time::timeout(
                    Duration::from_secs(2),
                    load_pool_meta(lock_conn.as_mut(), &slot.name),
                )
                .await
                {
                    Ok(Ok(meta)) => meta,
                    Ok(Err(_)) | Err(_) => None,
                };

                let expected_fp = self.expected_fingerprint.clone();
                let expected_ext = self.expected_extensions.clone();

                let meta_matches = existing_meta
                    .as_ref()
                    .is_some_and(|m| m.fingerprint == expected_fp && m.extensions == expected_ext);

                // If the DB is from an older template/extension set, prefer recreation over
                // cleanup; cleanup can't fix schema/extension drift.
                if existing_meta.is_some() && !meta_matches {
                    eprintln!(
                        "♻️  Slot {} metadata mismatches current template; recreating it",
                        slot.name
                    );
                    let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
                        .bind(lock_id)
                        .execute(lock_conn.as_mut())
                        .await;
                    drop(lock_conn);
                    let () = pool.close().await;
                    let _ = recreate_pool_database(&slot.name, &slot.url).await;
                    {
                        let mut pool_opt = slot.pool.lock();
                        *pool_opt = None;
                    }
                    slot.in_use.store(false, Ordering::Release);
                    continue;
                }

                let was_clean = existing_meta.as_ref().is_some_and(|m| !m.dirty);

                // Mark dirty immediately after lock acquisition (crash-safe).
                let dirty_meta = PoolMeta {
                    fingerprint: expected_fp.clone(),
                    extensions: expected_ext.clone(),
                    dirty: true,
                    updated_at_rfc3339: OffsetDateTime::now_utc()
                        .format(&time::format_description::well_known::Rfc3339)
                        .expect("format timestamp as RFC3339"),
                    last_error: None,
                };
                if let Err(e) = store_pool_meta(lock_conn.as_mut(), &slot.name, &dirty_meta).await {
                    eprintln!("⚠️  Failed to persist pool meta for {}: {}", slot.name, e);
                }

                if was_clean {
                    let acquisition_time = start_time.elapsed();
                    POOL_METRICS.record_acquisition(acquisition_time);

                    return Ok(TestDatabase {
                        name: slot.name.clone(),
                        pool: pool.clone(),
                        slot: slot.clone(),
                        lock_id,
                        lock_conn: Some(lock_conn),
                        acquired_at: Instant::now(),
                        acquisition_process_id: pid,
                    });
                }

                // Clean it before use (default behavior, and also the fallback when the slot is
                // not known-clean under clean-after-use).
                let clean_start = std::time::Instant::now();
                match clean_database(slot, &pool, &slot.name, &slot.url).await {
                    Ok(()) => {
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
                            lock_conn: Some(lock_conn),
                            acquired_at: Instant::now(),
                            acquisition_process_id: pid,
                        });
                    }
                    Err(e) => {
                        eprintln!("⚠️  Failed to clean database {}: {}", slot.name, e);
                        POOL_METRICS.record_cleanup_failure();

                        let dirty_meta = PoolMeta {
                            fingerprint: self.expected_fingerprint.clone(),
                            extensions: self.expected_extensions.clone(),
                            dirty: true,
                            updated_at_rfc3339: OffsetDateTime::now_utc()
                                .format(&time::format_description::well_known::Rfc3339)
                                .expect("format timestamp as RFC3339"),
                            last_error: Some(e.to_string()),
                        };
                        let _ = store_pool_meta(lock_conn.as_mut(), &slot.name, &dirty_meta).await;

                        // Release the advisory lock on the same session that acquired it.
                        let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
                            .bind(lock_id)
                            .execute(lock_conn.as_mut())
                            .await;
                        drop(lock_conn);
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
            if attempts >= MAX_ATTEMPTS {
                let total_time = start_time.elapsed();
                return Err(eyre!(format!(
                    "Failed to acquire database after {attempts} attempts ({total_time:.1?}). Consider increasing pool size or reducing test parallelism."
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

async fn database_exists(conn: &mut PoolConnection<Postgres>, name: &str) -> TestResult<bool> {
    let exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)")
            .bind(name)
            .fetch_one(conn.as_mut())
            .await?;
    Ok(exists)
}

async fn database_exists_admin(conn: &mut PgConnection, name: &str) -> TestResult<bool> {
    let exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)")
            .bind(name)
            .fetch_one(&mut *conn)
            .await?;
    Ok(exists)
}

fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

fn is_missing_database_error(err: &sqlx::Error) -> bool {
    match err {
        sqlx::Error::Database(db_err) => {
            db_err
                .code()
                .as_ref()
                .is_some_and(|c| c.as_ref() == "3D000")
                || db_err.message().contains("does not exist")
        }
        _ => err.to_string().contains("does not exist"),
    }
}

fn is_timescaledb_missing_library_error_message(message: &str) -> bool {
    // Nix-packaged TimescaleDB uses versioned shared objects (e.g. `timescaledb-2.23.0`).
    // Stale cloned databases can keep referencing the old filename and fail to run even `SELECT 1`.
    let msg = message.to_ascii_lowercase();
    msg.contains("could not access file \"$libdir/timescaledb-")
        || (msg.contains("could not access file")
            && msg.contains("timescaledb-")
            && msg.contains("no such file"))
        || (msg.contains("could not load library")
            && msg.contains("timescaledb")
            && msg.contains("no such file"))
}

fn is_timescaledb_missing_library_error(err: &sqlx::Error) -> bool {
    match err {
        sqlx::Error::Database(db_err) => {
            is_timescaledb_missing_library_error_message(db_err.message())
        }
        _ => is_timescaledb_missing_library_error_message(&err.to_string()),
    }
}

async fn ensure_pool_database_exists(db_name: &str, slot_url: &str) -> TestResult<()> {
    let admin_url = admin_url_from_slot(slot_url)?;
    let base_url = base_url_from_slot(slot_url)?;
    let mut template_guard =
        ensure_template_database(&admin_url, &base_url, SLOT_MAX_CONNECTIONS).await?;
    let template_name = template_guard.info.name.clone();
    let template_extensions = template_guard.info.extensions.clone();
    let start = Instant::now();

    let provision_result: TestResult<()> = async {
        if !database_exists_admin(&mut template_guard.admin_conn, db_name).await? {
            match create_database_from_template_admin(
                &mut template_guard.admin_conn,
                db_name,
                &template_name,
            )
            .await?
            {
                CreateDatabaseOutcome::Created => {
                    eprintln!(
                        "  Created missing pool database: {db_name} (clone: {:?})",
                        start.elapsed()
                    );
                }
                CreateDatabaseOutcome::AlreadyExists => {}
            }
        }
        let _ = grant_pool_database_permissions(db_name).await;
        let meta = PoolMeta {
            fingerprint: migrations_fingerprint(),
            extensions: template_extensions.clone(),
            dirty: false,
            updated_at_rfc3339: OffsetDateTime::now_utc()
                .format(&time::format_description::well_known::Rfc3339)
                .expect("format timestamp as RFC3339"),
            last_error: None,
        };
        let _ = store_pool_meta(&mut template_guard.admin_conn, db_name, &meta).await;
        Ok(())
    }
    .await;

    let release_result = template_guard.release().await;
    match provision_result {
        Ok(()) => {
            release_result?;
            Ok(())
        }
        Err(e) => {
            let _ = release_result;
            Err(e)
        }
    }
}

async fn drop_database_if_exists(
    conn: &mut PoolConnection<Postgres>,
    name: &str,
) -> TestResult<()> {
    let quoted = quote_ident(name);
    let drop_force = sqlx::query(&format!("DROP DATABASE IF EXISTS {quoted} WITH (FORCE)"))
        .execute(conn.as_mut())
        .await;

    if let Err(force_err) = drop_force {
        let fallback = sqlx::query(&format!("DROP DATABASE IF EXISTS {quoted}"))
            .execute(conn.as_mut())
            .await;

        if let Err(drop_err) = fallback {
            return Err(eyre!(format!(
                "Failed to drop database {name}: {force_err}; fallback error: {drop_err}"
            )));
        }
    }

    Ok(())
}

async fn drop_database_if_exists_admin(conn: &mut PgConnection, name: &str) -> TestResult<()> {
    let quoted = quote_ident(name);
    let drop_force = sqlx::query(&format!("DROP DATABASE IF EXISTS {quoted} WITH (FORCE)"))
        .execute(&mut *conn)
        .await;

    if let Err(force_err) = drop_force {
        let fallback = sqlx::query(&format!("DROP DATABASE IF EXISTS {quoted}"))
            .execute(&mut *conn)
            .await;

        if let Err(drop_err) = fallback {
            return Err(eyre!(format!(
                "Failed to drop database {name}: {force_err}; fallback error: {drop_err}"
            )));
        }
    }

    Ok(())
}

async fn wait_for_database_absence(
    conn: &mut PoolConnection<Postgres>,
    name: &str,
) -> TestResult<()> {
    const MAX_ATTEMPTS: usize = 20;
    for attempt in 0..MAX_ATTEMPTS {
        if !database_exists(conn, name).await? {
            return Ok(());
        }

        let delay = Duration::from_millis(50 + (attempt as u64 * 10));
        tokio::time::sleep(delay).await;
    }

    Err(eyre!(format!(
        "Database {name} still present after drop attempts"
    )))
}

async fn wait_for_database_absence_admin(conn: &mut PgConnection, name: &str) -> TestResult<()> {
    const MAX_ATTEMPTS: usize = 20;
    for attempt in 0..MAX_ATTEMPTS {
        if !database_exists_admin(conn, name).await? {
            return Ok(());
        }

        let delay = Duration::from_millis(50 + (attempt as u64 * 10));
        tokio::time::sleep(delay).await;
    }

    Err(eyre!(format!(
        "Database {name} still present after drop attempts"
    )))
}

/// Grant schema permissions to app user on a newly created pool database.
///
/// This uses the centralized permissions module which automatically grants on ALL
/// schemas including public (for `seaql_migrations`), eliminating hardcoded schema lists.
async fn grant_pool_database_permissions(db_name: &str) -> TestResult<()> {
    crate::sandbox::db::permissions::grant_pool_database_permissions(db_name).await
}

async fn create_database_from_template(
    conn: &mut PoolConnection<Postgres>,
    name: &str,
    template_name: &str,
) -> TestResult<CreateDatabaseOutcome> {
    // Prevent concurrent template recreation while cloning.
    let template_lock_id = advisory_lock_key(template_name);
    sqlx::query("SELECT pg_advisory_lock_shared($1)")
        .bind(template_lock_id)
        .execute(conn.as_mut())
        .await?;

    // Serialize CREATE DATABASE ... TEMPLATE ... calls; Postgres can error when the template is
    // concurrently used as a copy source.
    let clone_lock_id = advisory_lock_key(&format!("{template_name}::clone"));
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(clone_lock_id)
        .execute(conn.as_mut())
        .await?;

    let quoted_name = quote_ident(name);
    let quoted_template = quote_ident(template_name);

    let result = match sqlx::query(&format!(
        "CREATE DATABASE {quoted_name} WITH TEMPLATE {quoted_template}"
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
                let duplicate_code = db_err.code().as_ref().is_some_and(|c| {
                    let code = c.as_ref();
                    code == "42P04" || code == "23505"
                });
                if duplicate_code || db_err.message().contains("already exists") {
                    Ok(CreateDatabaseOutcome::AlreadyExists)
                } else {
                    Err(eyre!(err.to_string()))
                }
            } else {
                Err(eyre!(err.to_string()))
            }
        }
    };

    let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(clone_lock_id)
        .execute(conn.as_mut())
        .await;

    let _ = sqlx::query("SELECT pg_advisory_unlock_shared($1)")
        .bind(template_lock_id)
        .execute(conn.as_mut())
        .await;

    result
}

async fn create_database_from_template_admin(
    admin_conn: &mut PgConnection,
    name: &str,
    template_name: &str,
) -> TestResult<CreateDatabaseOutcome> {
    // Serialize CREATE DATABASE ... TEMPLATE ... calls; Postgres can error when the template is
    // concurrently used as a copy source.
    let clone_lock_id = advisory_lock_key(&format!("{template_name}::clone"));
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(clone_lock_id)
        .execute(&mut *admin_conn)
        .await?;

    let quoted_name = quote_ident(name);
    let quoted_template = quote_ident(template_name);

    let result = match sqlx::query(&format!(
        "CREATE DATABASE {quoted_name} WITH TEMPLATE {quoted_template}"
    ))
    .execute(&mut *admin_conn)
    .await
    {
        Ok(_) => {
            let _ = grant_pool_database_permissions(name).await;
            Ok(CreateDatabaseOutcome::Created)
        }
        Err(err) => {
            if let Error::Database(db_err) = &err {
                let duplicate_code = db_err.code().as_ref().is_some_and(|c| {
                    let code = c.as_ref();
                    code == "42P04" || code == "23505"
                });
                if duplicate_code || db_err.message().contains("already exists") {
                    Ok(CreateDatabaseOutcome::AlreadyExists)
                } else {
                    Err(eyre!(err.to_string()))
                }
            } else {
                Err(eyre!(err.to_string()))
            }
        }
    };

    let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(clone_lock_id)
        .execute(&mut *admin_conn)
        .await;

    result
}

/// Clean a database for reuse
async fn clean_database(
    slot: &Arc<DatabaseSlot>,
    pool: &DbPool,
    db_name: &str,
    db_url: &str,
) -> TestResult<()> {
    eprintln!("🧹 Cleaning database: {db_name}");
    let mut working_pool = pool.clone();
    let mut residuals: Option<Vec<(String, i64)>> = None;
    let mut schema_recreated = false;

    let mut attempt = 0usize;
    loop {
        attempt += 1;
        if let Some(reason) = schema_mismatch_reason(&working_pool).await? {
            if schema_recreated {
                let err = eyre!(format!(
                    "Database {db_name} schema mismatch after recreation: {reason}"
                ));
                slot.record_clean_result(Err(err.to_string()), residuals.clone());
                slot.quarantined.store(true, Ordering::SeqCst);
                return Err(err);
            }

            eprintln!("  ♻️  Database {db_name} schema mismatch ({reason}); recreating");
            recreate_pool_database(db_name, db_url)
                .await
                .map_err(|recreate_err| {
                    POOL_METRICS.record_cleanup_failure();
                    eyre!(format!(
                        "Schema mismatch recreate failed for {db_name}: {recreate_err}"
                    ))
                })?;
            let fresh_pool = DatabasePool::slot_pool_options(4, Duration::from_secs(5))
                .connect(db_url)
                .await?;
            working_pool = fresh_pool;
            schema_recreated = true;
            continue;
        }

        // Terminate any zombie connections that might interfere with cleanup or verification
        let _ = sqlx::query(&format!(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
             WHERE datname = '{db_name}' AND pid <> pg_backend_pid()"
        ))
        .execute(&working_pool)
        .await;

        // Wait for connections to drain
        let mut drained = false;
        for _ in 0..20 {
            let count: i64 = sqlx::query_scalar(&format!(
                "SELECT COUNT(*) FROM pg_stat_activity WHERE datname = '{db_name}' AND pid <> pg_backend_pid()"
            ))
            .fetch_one(&working_pool)
            .await
            .unwrap_or(1); // Assume 1 on error to keep trying

            if count == 0 {
                drained = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        if !drained {
            eprintln!(
                "  ⚠️  Database {db_name} still has connections after termination; cleanup might fail"
            );
        }

        // Use the shared db_common implementation
        match crate::sandbox::db::pool::reset_database(&working_pool).await {
            Ok(()) => {
                if let Err(verify_err) =
                    crate::sandbox::db::pool::verify_clean_state(&working_pool).await
                {
                    if attempt >= 2 {
                        POOL_METRICS.record_cleanup_failure();
                        residuals = log_remaining_rows(&working_pool).await;
                        let err = eyre!(format!("Database {db_name} cleanup failed: {verify_err}"));
                        slot.record_clean_result(Err(err.to_string()), residuals.clone());
                        slot.quarantined.store(true, Ordering::SeqCst);
                        return Err(err);
                    }
                    eprintln!(
                        "  ⚠️ Database {db_name} failed clean-state verification: {verify_err}. Retrying cleanup once."
                    );
                    continue;
                }

                eprintln!("  ✅ Database cleanup verified - all tables empty");
                ensure_default_session_state(&working_pool).await?;
                slot.quarantined.store(false, Ordering::SeqCst);
                slot.record_clean_result(Ok(()), residuals.clone());
                return Ok(());
            }
            Err(e) => {
                let msg = e.to_string();
                let retryable = msg.contains("does not exist")
                    || msg.contains("terminating connection")
                    || msg.contains("Broken pipe")
                    || msg.contains("connection")
                    || is_timescaledb_missing_library_error_message(&msg);

                if retryable && attempt < 3 {
                    eprintln!(
                        "  ⚠️  Cleanup for {db_name} failed with connection error ({msg}); attempting to recreate slot and retry."
                    );
                    recreate_pool_database(db_name, db_url)
                        .await
                        .map_err(|recreate_err| {
                            POOL_METRICS.record_cleanup_failure();
                            eyre!(format!(
                                "Cleanup failed and recreate failed for {db_name}: {recreate_err}"
                            ))
                        })?;
                    // Fresh pool for the recreated database
                    let fresh_pool = DatabasePool::slot_pool_options(4, Duration::from_secs(5))
                        .connect(db_url)
                        .await?;
                    working_pool = fresh_pool;
                    continue;
                }

                eprintln!("  ❌ CRITICAL: Database {db_name} cleanup failed: {e}");
                POOL_METRICS.record_cleanup_failure();
                residuals = log_remaining_rows(&working_pool).await;

                // Attempt one last forced cleanup focusing on stubborn event/material rows.
                if let Err(force_err) = force_event_material_cleanup(&working_pool).await {
                    let err = eyre!(format!(
                        "Database {db_name} cleanup failed: {e}; forced cleanup also failed: {force_err}"
                    ));
                    slot.record_clean_result(Err(err.to_string()), residuals.clone());
                    slot.quarantined.store(true, Ordering::SeqCst);
                    return Err(err);
                }

                if let Err(verify_err) =
                    crate::sandbox::db::pool::verify_clean_state(&working_pool).await
                {
                    let err = eyre!(format!(
                        "Database {db_name} cleanup failed after forced cleanup: {verify_err}"
                    ));
                    slot.record_clean_result(Err(err.to_string()), residuals.clone());
                    slot.quarantined.store(true, Ordering::SeqCst);
                    return Err(err);
                }

                eprintln!("  ✅ Database cleanup recovered after forced truncation");
                ensure_default_session_state(&working_pool).await?;
                slot.quarantined.store(false, Ordering::SeqCst);
                slot.record_clean_result(Ok(()), residuals.clone());
                return Ok(());
            }
        }
    }
}

async fn log_remaining_rows(pool: &DbPool) -> Option<Vec<(String, i64)>> {
    match crate::sandbox::db::common::get_row_counts(pool).await {
        Ok(counts) => {
            let mut residuals = Vec::new();
            for (table, count) in counts {
                if count > 0 {
                    eprintln!("     - {table} has {count} rows remaining");
                    residuals.push((table, count));
                }
            }
            Some(residuals)
        }
        Err(_) => None,
    }
}

async fn core_events_trigger_exists(pool: &DbPool, trigger_name: &str) -> TestResult<bool> {
    sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (SELECT 1 FROM pg_trigger \
         WHERE tgrelid = to_regclass('core.events') \
           AND tgname = $1 \
           AND NOT tgisinternal)",
    )
    .bind(trigger_name)
    .fetch_one(pool)
    .await
    .map_err(|e| eyre!(e.to_string()))
}

async fn core_events_triggers_missing_reason(pool: &DbPool) -> TestResult<Option<String>> {
    let has_no_update = core_events_trigger_exists(pool, "trg_events_no_update").await?;
    let has_archive = core_events_trigger_exists(pool, "trg_events_archive_before_delete").await?;

    if has_no_update && has_archive {
        return Ok(None);
    }

    let mut missing = Vec::new();
    if !has_no_update {
        missing.push("trg_events_no_update");
    }
    if !has_archive {
        missing.push("trg_events_archive_before_delete");
    }

    Ok(Some(format!(
        "missing core.events triggers ({})",
        missing.join(", ")
    )))
}

async fn ensure_core_events_triggers(pool: &DbPool) -> TestResult<()> {
    let missing_reason = core_events_triggers_missing_reason(pool).await?;
    if missing_reason.is_none() {
        return Ok(());
    }

    let mut conn = pool.acquire().await?;

    if !core_events_trigger_exists(pool, "trg_events_no_update").await? {
        sqlx::query(
            r"
            CREATE OR REPLACE FUNCTION core.fn_events_no_update()
            RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                RAISE EXCEPTION 'UPDATE on core.events is forbidden';
            END $$;
            ",
        )
        .execute(&mut *conn)
        .await
        .map_err(|e| eyre!(e.to_string()))?;
        sqlx::query("DROP TRIGGER IF EXISTS trg_events_no_update ON core.events")
            .execute(&mut *conn)
            .await
            .map_err(|e| eyre!(e.to_string()))?;
        sqlx::query(
            "CREATE TRIGGER trg_events_no_update \
             BEFORE UPDATE ON core.events \
             FOR EACH ROW EXECUTE FUNCTION core.fn_events_no_update()",
        )
        .execute(&mut *conn)
        .await
        .map_err(|e| eyre!(e.to_string()))?;
        eprintln!("  ⚠️  Restored trg_events_no_update on core.events");
    }

    if !core_events_trigger_exists(pool, "trg_events_archive_before_delete").await? {
        sqlx::query(
            r"
            CREATE OR REPLACE FUNCTION core.fn_archive_before_delete()
            RETURNS trigger LANGUAGE plpgsql AS $$
            DECLARE
              op_id TEXT := current_setting('sinex.operation_id', true);
              sup_id ulid := NULLIF(current_setting('sinex.superseded_by_id', true), '');
              who TEXT := current_setting('sinex.archived_by', true);
              why TEXT := current_setting('sinex.archive_reason', true);
            BEGIN
              IF op_id IS NULL OR op_id = '' THEN
                RAISE EXCEPTION 'DELETE on core.events requires sinex.operation_id to be set in this session';
              END IF;

              INSERT INTO audit.archived_events SELECT OLD.*, now(), who, why, sup_id;
              RETURN OLD;
            END $$;
            ",
        )
        .execute(&mut *conn)
        .await
        .map_err(|e| eyre!(e.to_string()))?;
        sqlx::query("DROP TRIGGER IF EXISTS trg_events_archive_before_delete ON core.events")
            .execute(&mut *conn)
            .await
            .map_err(|e| eyre!(e.to_string()))?;
        sqlx::query(
            "CREATE TRIGGER trg_events_archive_before_delete \
             BEFORE DELETE ON core.events \
             FOR EACH ROW EXECUTE FUNCTION core.fn_archive_before_delete()",
        )
        .execute(&mut *conn)
        .await
        .map_err(|e| eyre!(e.to_string()))?;
        eprintln!("  ⚠️  Restored trg_events_archive_before_delete on core.events");
    }

    Ok(())
}

async fn ensure_pool_db_invariants(db_url: &str) -> TestResult<()> {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .acquire_timeout(Duration::from_secs(5))
        .connect(db_url)
        .await
        .map_err(|e| eyre!(e.to_string()))?;

    let result = ensure_core_events_triggers(&pool).await;
    pool.close().await;
    result
}

async fn schema_mismatch_reason(pool: &DbPool) -> TestResult<Option<String>> {
    let events_exists =
        sqlx::query_scalar::<_, Option<String>>("SELECT to_regclass('core.events')::text")
            .fetch_one(pool)
            .await?;
    if events_exists.as_deref() != Some("core.events") {
        return Ok(Some("missing core.events schema".to_string()));
    }

    let events_has_blobs = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (SELECT 1 FROM information_schema.columns \
         WHERE table_schema = 'core' AND table_name = 'events' AND column_name = 'associated_blob_ids')",
    )
    .fetch_one(pool)
    .await?;
    if !events_has_blobs {
        return Ok(Some(
            "missing core.events.associated_blob_ids column".to_string(),
        ));
    }

    let events_has_subnano = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (SELECT 1 FROM information_schema.columns \
         WHERE table_schema = 'core' AND table_name = 'events' \
           AND column_name = 'ts_orig_subnano' AND data_type = 'integer')",
    )
    .fetch_one(pool)
    .await?;
    if !events_has_subnano {
        return Ok(Some(
            "missing core.events.ts_orig_subnano column".to_string(),
        ));
    }

    let payload_has_updated_at = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (SELECT 1 FROM information_schema.columns \
         WHERE table_schema = 'sinex_schemas' AND table_name = 'event_payload_schemas' \
           AND column_name = 'updated_at')",
    )
    .fetch_one(pool)
    .await?;
    if !payload_has_updated_at {
        return Ok(Some(
            "missing sinex_schemas.event_payload_schemas.updated_at column".to_string(),
        ));
    }

    core_events_triggers_missing_reason(pool).await
}
fn admin_url_from_slot(slot_url: &str) -> TestResult<String> {
    let mut url = Url::parse(slot_url).map_err(|e| eyre!(format!("Invalid slot url: {e}")))?;
    url.set_path("/postgres");
    Ok(url.to_string())
}

fn base_url_from_slot(slot_url: &str) -> TestResult<String> {
    let mut url = Url::parse(slot_url).map_err(|e| eyre!(format!("Invalid slot url: {e}")))?;
    url.set_path("/sinex_dev");
    Ok(url.to_string())
}

fn url_with_db_name(raw_url: &str, db_name: &str) -> TestResult<String> {
    let mut url = Url::parse(raw_url).map_err(|e| eyre!(format!("Invalid database url: {e}")))?;
    url.set_path(&format!("/{db_name}"));
    Ok(url.to_string())
}

async fn recreate_pool_database(db_name: &str, slot_url: &str) -> TestResult<()> {
    let admin_url = admin_url_from_slot(slot_url)?;
    let base_url = base_url_from_slot(slot_url)?;
    let mut template_guard =
        ensure_template_database(&admin_url, &base_url, SLOT_MAX_CONNECTIONS).await?;
    let template_name = template_guard.info.name.clone();
    let template_extensions = template_guard.info.extensions.clone();

    let recreate_result: TestResult<()> = async {
        // Prevent multiple processes from concurrently dropping/recreating the same pool DB.
        // We rely on closing `template_guard.admin_conn` to release this lock.
        let recreate_lock_id = advisory_lock_key(&format!("{db_name}::recreate"));
        let _ = sqlx::query("SELECT pg_advisory_lock($1)")
            .bind(recreate_lock_id)
            .execute(&mut template_guard.admin_conn)
            .await;

        drop_database_if_exists_admin(&mut template_guard.admin_conn, db_name).await?;
        wait_for_database_absence_admin(&mut template_guard.admin_conn, db_name).await?;

        create_database_from_template_admin(
            &mut template_guard.admin_conn,
            db_name,
            &template_name,
        )
        .await?;
        let _ = grant_pool_database_permissions(db_name).await;
        let db_url = base_url.replace("/sinex_dev", &format!("/{db_name}"));
        ensure_pool_db_invariants(&db_url).await?;
        let meta = PoolMeta {
            fingerprint: migrations_fingerprint(),
            extensions: template_extensions.clone(),
            dirty: false,
            updated_at_rfc3339: OffsetDateTime::now_utc()
                .format(&time::format_description::well_known::Rfc3339)
                .expect("format timestamp as RFC3339"),
            last_error: None,
        };
        let _ = store_pool_meta(&mut template_guard.admin_conn, db_name, &meta).await;
        Ok(())
    }
    .await;

    let release_result = template_guard.release().await;
    match recreate_result {
        Ok(()) => {
            release_result?;
            Ok(())
        }
        Err(e) => {
            let _ = release_result;
            Err(e)
        }
    }
}

async fn ensure_default_session_state_conn(conn: &mut PgConnection) -> TestResult<()> {
    if let Ok(role) = sqlx::query_scalar::<_, String>("SHOW session_replication_role")
        .fetch_one(&mut *conn)
        .await
    {
        if role != "origin" {
            sqlx::query("SET session_replication_role = 'origin'")
                .execute(&mut *conn)
                .await
                .map_err(|e| eyre!(e.to_string()))?;
            eprintln!("  ⚠️  Reset session_replication_role from {role} to origin");
        }
    }
    if let Ok(row_sec) = sqlx::query_scalar::<_, String>("SHOW row_security")
        .fetch_one(&mut *conn)
        .await
    {
        if row_sec.to_lowercase() != "on" {
            sqlx::query("SET row_security = on")
                .execute(&mut *conn)
                .await
                .map_err(|e| eyre!(e.to_string()))?;
            eprintln!("  ⚠️  Reset row_security to on");
        }
    }
    // Restore synchronous_commit if apply_test_optimizations() turned it off.
    if let Ok(sync_commit) = sqlx::query_scalar::<_, String>("SHOW synchronous_commit")
        .fetch_one(&mut *conn)
        .await
    {
        if sync_commit != "on" {
            sqlx::query("SET synchronous_commit TO ON")
                .execute(&mut *conn)
                .await
                .map_err(|e| eyre!(e.to_string()))?;
        }
    }

    let config = CleanupConfig::default();
    for table in config.tables_requiring_trigger_disable() {
        let query = format!(
            "SELECT EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid = '{}'::regclass AND tgenabled NOT IN ('O','A')) AS needs_enable",
            table.table_name
        );
        if let Ok(needs_enable) = sqlx::query_scalar::<_, Option<bool>>(&query)
            .fetch_one(&mut *conn)
            .await
        {
            if needs_enable == Some(true) {
                let enable = sqlx::query(&format!(
                    "ALTER TABLE {} ENABLE TRIGGER ALL",
                    table.table_name
                ))
                .execute(&mut *conn)
                .await;
                match enable {
                    Ok(_) => {
                        eprintln!("  ⚠️  Re-enabled triggers on {}", table.table_name);
                    }
                    Err(err) => {
                        let msg = err.to_string();
                        if msg.contains("hypertables do not support") {
                            eprintln!(
                                "  ⚠️  Skipping trigger enable on hypertable {}",
                                table.table_name
                            );
                        } else {
                            return Err(eyre!(msg));
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

/// Ensure a pooled connection is returned to default session state; best-effort only.
pub async fn ensure_default_session_state(pool: &DbPool) -> TestResult<()> {
    let mut conn = pool.acquire().await?;
    ensure_default_session_state_conn(conn.as_mut()).await
}

/// Final backstop cleanup when standard reset fails (e.g., FK contention).
async fn force_event_material_cleanup(pool: &DbPool) -> TestResult<()> {
    let mut conn = pool.acquire().await?;
    let config = CleanupConfig::default();
    let pool_for_chunks = pool.clone();
    let cleanup_tables: Vec<String> = config
        .ordered_tables()
        .into_iter()
        .map(|t| t.table_name.to_string())
        .collect();

    crate::sandbox::db::common::with_cleanup_session(&mut conn, &config, |conn| {
        let fut: BoxFuture<'_, crate::sandbox::prelude::TestResult<()>> = Box::pin(async move {
            let mut attempts = 0;
            let mut last_events = 0_i64;
            let mut last_materials = 0_i64;

            while attempts < 3 {
                attempts += 1;

                // Truncate high-churn tables with CASCADE to avoid FK deadlocks.
                let _ = sqlx::query("TRUNCATE TABLE core.events CASCADE")
                    .execute(conn.as_mut())
                    .await;
                let _ = sqlx::query("TRUNCATE TABLE raw.source_material_registry CASCADE")
                    .execute(conn.as_mut())
                    .await;

                // Delete from remaining tables (config-driven) after cascades to catch ancillary rows.
                for table in &cleanup_tables {
                    let _ = sqlx::query(&format!("DELETE FROM {table}"))
                        .execute(conn.as_mut())
                        .await;
                }

                // Hypertable cleanup via drop_chunks for events.
                let _ = sqlx::query(
                    "SELECT drop_chunks('core.events', older_than => INTERVAL '0 seconds')",
                )
                .execute(&pool_for_chunks)
                .await;

                let counts = crate::sandbox::db::common::get_row_counts(&pool_for_chunks)
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

            if last_events != 0 || last_materials > 1 {
                return Err(eyre!(format!(
                    "Force cleanup left {last_events} events and {last_materials} materials"
                )));
            }

            Ok(())
        });
        fut
    })
    .await
    .map_err(|e| eyre!(e.to_string()))?;

    Ok(())
}

#[cfg(test)]
#[allow(dead_code)] // Test helper available for checkpoint consistency tests
pub(crate) async fn force_event_material_cleanup_for_tests(pool: &DbPool) -> TestResult<()> {
    force_event_material_cleanup(pool).await
}

// Global pool instance - initialized on first use
pub(crate) static POOL: std::sync::LazyLock<tokio::sync::Mutex<Option<Arc<DatabasePool>>>> =
    std::sync::LazyLock::new(|| tokio::sync::Mutex::new(None));

/// Acquire a test database
pub async fn acquire_test_database() -> TestResult<TestDatabase> {
    // Get or initialize the pool
    let mut pool_lock = POOL.lock().await;

    if pool_lock.is_none() {
        let config = PoolConfig::default();
        let pool = Arc::new(DatabasePool::new(config, false).await?);
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

async fn connect_admin_with_retry(admin_url: &str) -> TestResult<PgConnection> {
    let mut delay = Duration::from_millis(100);
    let mut last_error: Option<sqlx::Error> = None;
    const MAX_ATTEMPTS: usize = 10;

    for attempt in 0..MAX_ATTEMPTS {
        match tokio::time::timeout(Duration::from_secs(5), PgConnection::connect(admin_url)).await {
            Ok(Ok(conn)) => return Ok(conn),
            Ok(Err(err)) => {
                let err_str = err.to_string();
                if !err_str.to_lowercase().contains("too many clients") {
                    return Err(eyre!(format!(
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
                return Err(eyre!(
                    "Admin connection timeout. Ensure PostgreSQL is running locally.",
                ));
            }
        }

        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(Duration::from_secs(2));
    }

    Err(eyre!(format!(
        "Admin connection failed after retries: {}. Ensure PostgreSQL is running and reachable for tests.",
        last_error.map_or_else(|| "unknown error".to_string(), |e| e.to_string())
    )))
}

async fn detect_connection_budget(admin_url: &str) -> Option<u32> {
    let mut conn = connect_admin_with_retry(admin_url).await.ok()?;
    let max_connections: i64 =
        sqlx::query_scalar("SELECT current_setting('max_connections')::bigint")
            .fetch_one(&mut conn)
            .await
            .ok()?;

    let reserved: i64 =
        sqlx::query_scalar("SELECT current_setting('superuser_reserved_connections')::bigint")
            .fetch_one(&mut conn)
            .await
            .unwrap_or(3);

    // Leave headroom for template provisioning, cleanup tasks, and ad-hoc diagnostics.
    const SAFETY_MARGIN: i64 = 16;

    let effective = max_connections
        .saturating_sub(reserved)
        .saturating_sub(SAFETY_MARGIN);
    if effective <= 0 {
        return None;
    }

    Some(effective as u32)
}

async fn harden_template_database(
    admin_conn: &mut PgConnection,
    template_name: &str,
) -> TestResult<()> {
    let quoted = quote_ident(template_name);
    // Ensure no new sessions can connect to the template DB; lingering connections make
    // CREATE DATABASE ... TEMPLATE ... fail.
    let _ = sqlx::query(&format!(
        "ALTER DATABASE {quoted} WITH ALLOW_CONNECTIONS false"
    ))
    .execute(&mut *admin_conn)
    .await;

    let _ = sqlx::query(
        "SELECT pg_terminate_backend(pid) \
         FROM pg_stat_activity \
         WHERE datname = $1 AND pid <> pg_backend_pid()",
    )
    .bind(template_name)
    .execute(&mut *admin_conn)
    .await;

    Ok(())
}

async fn ensure_template_database(
    admin_url: &str,
    _base_url: &str,
    slot_max_connections: u32,
) -> TestResult<TemplateGuard> {
    // Important: never connect to the template database just to "check it exists". Any session
    // connected to the template makes `CREATE DATABASE ... TEMPLATE ...` fail.

    // Acquire lock to prevent race condition between parallel tests
    let _lock = TEMPLATE_CREATION_LOCK.lock().await;

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

    // Connect to admin database with timeout
    let mut admin_conn = connect_admin_with_retry(admin_url).await?;

    let lock_key = advisory_lock_key(template_name);

    // IMPORTANT: nextest runs each test in its own process, so multiple processes can race to
    // ensure the template at the same time. If we take an exclusive advisory lock up-front, any
    // process holding a shared lock (e.g. while provisioning the pool) will block all others and
    // tests will hit the 30s watchdog timeout. Instead:
    // - take a shared lock to check/reuse (shared/shared is fine)
    // - only take an exclusive lock when we truly need to recreate
    // - use try-lock + retry so we never block behind long-lived shared locks
    let ensure_deadline = Instant::now() + Duration::from_secs(45);
    let mut backoff = Duration::from_millis(25);
    loop {
        tokio::time::timeout(
            Duration::from_secs(15),
            sqlx::query("SELECT pg_advisory_lock_shared($1)")
                .bind(lock_key)
                .execute(&mut admin_conn),
        )
        .await
        .map_err(|_| eyre!("Template shared-lock timeout"))?
        .map_err(|e| eyre!(format!("Template shared-lock failed: {e}")))?;

        let slot_max_connections = slot_max_connections.max(1);
        let template_pool_max = slot_max_connections.saturating_mul(2).max(4);

        let template_admin_url = replace_db_name(admin_url, template_name);

        // Check if template already exists
        let exists: bool = sqlx::query_scalar(&format!(
            "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = '{template_name}')"
        ))
        .fetch_one(&mut admin_conn)
        .await?;

        // Determine if we can reuse the existing template without rebuild
        let mut reuse_allowed = false;
        let mut reused_extensions: HashMap<String, String> = HashMap::new();
        if exists {
            if let Some(fp) = &desired_fingerprint {
                match load_template_meta(&mut admin_conn, template_name).await? {
                    Some(meta) if meta.fingerprint == *fp && !meta.extensions.is_empty() => {
                        eprintln!(
                            "✅ Template database {template_name} reused (migrations unchanged)"
                        );
                        reuse_allowed = true;
                        reused_extensions = meta.extensions;
                    }
                    Some(meta) if meta.fingerprint == *fp => {
                        // Legacy/partial metadata (e.g. older harness versions) makes extension
                        // drift undetectable across NixOS upgrades (TimescaleDB uses versioned
                        // shared objects). Rebuild to avoid reusing a template that can no longer
                        // load its extensions.
                        eprintln!(
                            "♻️  Template metadata missing extension versions ({template_name}); recreating template"
                        );
                        let _ = meta;
                    }
                    Some(meta) => {
                        eprintln!(
                            "♻️  Migration fingerprint changed ({} -> {}); recreating template",
                            meta.fingerprint, fp
                        );
                    }
                    None => {
                        eprintln!("ℹ️  No template metadata found; recreating template");
                    }
                }
            } else if let Some(meta) = load_template_meta(&mut admin_conn, template_name).await? {
                // Best effort: reuse when we can't compute the fingerprint, but still surface
                // extension metadata if available.
                if meta.extensions.is_empty() {
                    eprintln!(
                        "♻️  Template metadata missing extension versions ({template_name}); recreating template"
                    );
                } else {
                    eprintln!("✅ Template database {template_name} reused (no fingerprint)");
                    reuse_allowed = true;
                    reused_extensions = meta.extensions;
                }
            } else {
                eprintln!("ℹ️  Template metadata unavailable and fingerprint missing; recreating");
            }
        }

        if reuse_allowed && !reused_extensions.is_empty() {
            let defaults = default_extension_versions(&mut admin_conn).await?;
            for (ext, template_ver) in &reused_extensions {
                if let Some(default_ver) = defaults.get(ext) {
                    if default_ver != template_ver {
                        eprintln!(
                            "♻️  Extension {ext} default_version changed ({template_ver} -> {default_ver}); recreating template"
                        );
                        reuse_allowed = false;
                        break;
                    }
                }
            }
        }

        if reuse_allowed {
            // Keep the template clone-safe across parallel nextest processes.
            harden_template_database(&mut admin_conn, template_name).await?;

            if TEMPLATE_DB_NAME.get().is_none() {
                let _ = TEMPLATE_DB_NAME.set(template_name.to_string());
            }
            return Ok(TemplateGuard {
                info: TemplateInfo {
                    name: template_name.to_string(),
                    extensions: reused_extensions,
                },
                lock_key,
                admin_conn,
            });
        }

        // We need to rebuild the template. Release our shared lock, then race to become the
        // one process that does the rebuild. If we lose the race, retry (under shared) until
        // the winner finishes.
        let _ = sqlx::query("SELECT pg_advisory_unlock_shared($1)")
            .bind(lock_key)
            .execute(&mut admin_conn)
            .await;

        if Instant::now() > ensure_deadline {
            return Err(eyre!(
                "Template database was not ready before deadline (another test process may be stuck recreating it)".to_string(),
            ));
        }

        let got_exclusive: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
            .bind(lock_key)
            .fetch_one(&mut admin_conn)
            .await?;

        if !got_exclusive {
            // Someone else is recreating (exclusive) or many processes are concurrently checking
            // (shared). Back off briefly and retry; the next iteration will take a shared lock and
            // likely see a reusable template.
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(Duration::from_millis(250));
            continue;
        }

        // Under exclusive lock: re-check before doing destructive work.
        let exists: bool = sqlx::query_scalar(&format!(
            "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = '{template_name}')"
        ))
        .fetch_one(&mut admin_conn)
        .await?;

        let mut reuse_allowed = false;
        let mut reused_extensions: HashMap<String, String> = HashMap::new();
        if exists {
            if let Some(fp) = &desired_fingerprint {
                if let Some(meta) = load_template_meta(&mut admin_conn, template_name).await? {
                    if meta.fingerprint == *fp && !meta.extensions.is_empty() {
                        reuse_allowed = true;
                        reused_extensions = meta.extensions;
                    }
                }
            } else if let Some(meta) = load_template_meta(&mut admin_conn, template_name).await? {
                if !meta.extensions.is_empty() {
                    reuse_allowed = true;
                    reused_extensions = meta.extensions;
                }
            }
        }

        if reuse_allowed {
            // Downgrade exclusive -> shared to be compatible with callers that expect a guard.
            sqlx::query("SELECT pg_advisory_lock_shared($1)")
                .bind(lock_key)
                .execute(&mut admin_conn)
                .await?;
            let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
                .bind(lock_key)
                .execute(&mut admin_conn)
                .await;

            harden_template_database(&mut admin_conn, template_name).await?;
            if TEMPLATE_DB_NAME.get().is_none() {
                let _ = TEMPLATE_DB_NAME.set(template_name.to_string());
            }
            return Ok(TemplateGuard {
                info: TemplateInfo {
                    name: template_name.to_string(),
                    extensions: reused_extensions,
                },
                lock_key,
                admin_conn,
            });
        }

        // We really do need to rebuild the template.
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
        if sqlx::query(&drop_query)
            .execute(&mut admin_conn)
            .await
            .is_ok()
        {
        } else {
            let fallback = format!("DROP DATABASE IF EXISTS {template_name}");
            sqlx::query(&fallback).execute(&mut admin_conn).await?;
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
                    return Err(eyre!(format!("Create database failed: {err}")));
                }
            }
            Err(_) => {
                return Err(eyre!("Create database timeout"));
            }
        }

        // Connect to template database and run all migrations
        let template_pool_future = async {
            // Use DATABASE_URL_SUPERUSER if available (CI environment), otherwise use admin URL
            let template_migration_url =
                if let Ok(super_url) = std::env::var("DATABASE_URL_SUPERUSER") {
                    url_with_db_name(&super_url, template_name)?
                } else {
                    url_with_db_name(&template_admin_url, template_name)?
                };

            let template_pool: DbPool = sqlx::postgres::PgPoolOptions::new()
                .max_connections(template_pool_max)
                .min_connections(1)
                .max_lifetime(Duration::from_mins(5))
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
                Ok(()) => {}
                Err(e) => {
                    eprintln!("❌ Missing required PostgreSQL extensions: {e}");
                    eprintln!("   Check NixOS PostgreSQL configuration and required extensions.");
                    return Err(e);
                }
            }

            // Run migrations against the template database. The sinex-db migration helper
            // reads DATABASE_URL, so temporarily point it at the template DB with superuser credentials.
            let prev_db_url = std::env::var("DATABASE_URL").ok();
            std::env::set_var("DATABASE_URL", &template_migration_url);

            let migrate_result = tokio::time::timeout(
                Duration::from_secs(30),
                sinex_db::run_migrations_for_url(&template_migration_url),
            )
            .await
            .map_err(|_| {
                eyre!(
                    "Migration timeout - check if all required extensions are installed"
                        .to_string(),
                )
            })
            .and_then(|res| res.map_err(|e| eyre!(format!("Migration failed: {e}"))));

            // Restore original DATABASE_URL
            if let Some(url) = prev_db_url {
                std::env::set_var("DATABASE_URL", url);
            }

            // Propagate migration result
            migrate_result?;

            // Grant schema permissions to the non-superuser role for template database operations
            // Uses centralized permissions module which grants on ALL schemas (including public)
            if let Some(granter) = crate::sandbox::db::permissions::PermissionGranter::from_env()? {
                if let Some(username) = std::env::var("DATABASE_URL_APP").ok().and_then(|url| {
                    url.split("://")
                        .nth(1)
                        .and_then(|s| s.split('@').next().map(std::string::ToString::to_string))
                }) {
                    eprintln!(
                    "  🔑 Granting schema permissions to user '{username}' in template database"
                );

                    // Use the centralized granter to grant all schemas
                    use sinex_schema::schema_registry;
                    for schema in schema_registry::SINEX_SCHEMAS {
                        if let Err(e) = granter
                            .grant_schema_access(&template_pool, schema.name)
                            .await
                        {
                            tracing::warn!(
                                error = %e,
                                schema = schema.name,
                                "Failed to grant permissions on schema in template database"
                            );
                        }
                    }
                }
            }

            // Optimize template for faster copying
            optimize_template_for_tests(&template_pool).await?;

            let extensions = collect_extension_versions(&template_pool).await?;

            template_pool.close().await;
            Ok(extensions)
        };

        let migration_result: TestResult<HashMap<String, String>> =
            tokio::time::timeout(Duration::from_secs(45), template_pool_future)
                .await
                .map_err(|_| eyre!("Template setup timeout"))?;

        let extensions = migration_result?;

        let template_elapsed = template_start.elapsed();
        eprintln!("✅ Template database created in {template_elapsed:?}");

        if let Some(fp) = desired_fingerprint {
            let meta = TemplateMeta {
                fingerprint: fp,
                extensions: extensions.clone(),
            };
            if let Err(err) = store_template_meta(&mut admin_conn, template_name, &meta).await {
                eprintln!("⚠️  Failed to persist template metadata for {template_name}: {err}");
                warn!("Failed to persist template metadata: {err}");
            }
        }

        // Cache the template name for future use
        if TEMPLATE_DB_NAME.get().is_none() {
            TEMPLATE_DB_NAME
                .set(template_name.to_string())
                .map_err(|_| eyre!("Failed to cache template database name"))?;
        }

        harden_template_database(&mut admin_conn, template_name).await?;

        // Downgrade exclusive -> shared so the caller can safely clone pool databases while the
        // template is protected from recreation.
        sqlx::query("SELECT pg_advisory_lock_shared($1)")
            .bind(lock_key)
            .execute(&mut admin_conn)
            .await?;
        if let Err(e) = sqlx::query("SELECT pg_advisory_unlock($1)")
            .bind(lock_key)
            .execute(&mut admin_conn)
            .await
        {
            eprintln!("⚠️  Failed to release template advisory lock for {template_name}: {e}");
        }

        return Ok(TemplateGuard {
            info: TemplateInfo {
                name: template_name.to_string(),
                extensions,
            },
            lock_key,
            admin_conn,
        });
    }
}

/// Check if required `PostgreSQL` extensions are available
async fn check_required_extensions(pool: &DbPool) -> TestResult<()> {
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
            missing_required.push(format!("{ext_name} ({description})"));
            continue;
        }

        ensure_extension_installed(pool, ext_name).await?;
    }

    if !missing_required.is_empty() {
        return Err(eyre!(format!(
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

async fn collect_extension_versions(pool: &DbPool) -> TestResult<HashMap<String, String>> {
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

async fn ensure_extension_installed(pool: &DbPool, extension: &str) -> TestResult<()> {
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

    if available.is_none() {
        return Err(eyre!(format!(
            "Extension {extension} is not available in the current PostgreSQL installation"
        )));
    }

    let create_stmt = format!("CREATE EXTENSION IF NOT EXISTS {extension}");
    sqlx::query(&create_stmt)
        .execute(pool)
        .await
        .map_err(|e| eyre!(format!("Failed to create extension {extension}: {e}")))?;

    Ok(())
}

/// Apply test-specific `PostgreSQL` optimizations (session-level only)
async fn apply_test_session_optimizations(pool: &DbPool) -> TestResult<()> {
    // Always enable test optimizations (disables synchronous_commit for speed)
    eprintln!("⚡ Applying test session optimizations...");
    crate::sandbox::db::common::apply_test_optimizations(pool)
        .await
        .map_err(|e| SinexError::database(format!("Failed to apply test optimizations: {e}")))?;
    Ok(())
}

/// Optimize template database for faster test copying
async fn optimize_template_for_tests(pool: &DbPool) -> TestResult<()> {
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
        let disable_policies_sql = r"
        SELECT alter_job(job_id, scheduled => false)
        FROM timescaledb_information.jobs
        WHERE application_name LIKE '%Continuous Aggregate%'
           OR application_name LIKE '%Telemetry%'
    ";

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

        eprintln!("✅ Template database optimized for test performance");
        Ok::<(), SinexError>(())
    };

    // Apply a reasonable timeout
    match tokio::time::timeout(Duration::from_secs(20), optimization_future).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(e.into()),
        Err(_) => {
            eprintln!("⚠️  Template optimization timed out after 20s, continuing anyway");
            Ok(()) // Don't fail, optimizations are optional
        }
    }
}

/// Health check for the entire pool
pub async fn check_pool_health() -> TestResult<PoolHealthReport> {
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
    pool_lock.as_ref().map_or(0, |pool| pool.slots.len())
}

/// Acquire a connection to the Postgres admin database with retry logic.
pub async fn acquire_admin_connection() -> TestResult<PgConnection> {
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
pub async fn reset_pool() -> TestResult<()> {
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

/// Prime the pool by ensuring the template and all pool databases exist.
pub async fn prime_pool() -> TestResult<()> {
    let pool = {
        let mut pool_lock = POOL.lock().await;
        if let Some(pool) = pool_lock.as_ref().cloned() {
            pool
        } else {
            let config = PoolConfig::default();
            let pool = Arc::new(DatabasePool::new(config, true).await?);
            *pool_lock = Some(pool.clone());
            pool
        }
    };

    for slot in &pool.slots {
        ensure_pool_database_exists(&slot.name, &slot.url).await?;
    }

    Ok(())
}

/// Initialize pool with custom configuration (for testing)
async fn _init_pool_with_config(config: PoolConfig) -> TestResult<()> {
    let mut pool_lock = POOL.lock().await;
    let pool = Arc::new(DatabasePool::new(config, true).await?);
    *pool_lock = Some(pool);
    Ok(())
}

/// Get pool configuration (for debugging)
fn _get_pool_config() -> PoolConfig {
    PoolConfig::default()
}

#[cfg(test)]
mod benches {

    use xtask_macros::*;

    /// Benchmark database acquisition from pool
    ///
    /// This measures the time to acquire a clean database from the pool,
    /// including advisory lock acquisition and cleanup verification.
    #[sinex_bench]
    async fn bench_acquire_database() -> TestResult<()> {
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
    async fn bench_concurrent_acquisition(arg: usize) -> TestResult<()> {
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

    /// Benchmark template database operations
    #[sinex_bench]
    async fn bench_ensure_template_database() -> TestResult<()> {
        let config = PoolConfig::default();
        // This should be fast after first run (cached)
        let guard = ensure_template_database(
            &config.admin_url,
            &config.base_url,
            config.slot_max_connections,
        )
        .await?;
        guard.release().await?;
        Ok(())
    }

    /// Benchmark pool health check
    #[sinex_bench]
    async fn bench_pool_health_check() -> TestResult<()> {
        // Ensure pool is initialized
        let _ = acquire_test_database().await?;

        check_pool_health().await?;
        Ok(())
    }
}
