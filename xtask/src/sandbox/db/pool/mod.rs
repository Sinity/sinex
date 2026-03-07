//! Database pool management for sandbox.

use crate::sandbox::prelude::*;
use crate::sandbox::slog::{Level, slog};
use parking_lot::Mutex;

use sinex_primitives::SinexError;
use sinex_primitives::temporal::Timestamp;

use sqlx::Connection;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

// ── Submodules ──────────────────────────────────────────────────────────────

mod cleanup;
mod config;
mod health;
mod provisioning;
mod reset;
mod slot;
mod template;
mod test_database;

pub mod meta;
pub mod metrics;
pub mod stats;

// ── Re-exports (preserve public API) ────────────────────────────────────────

pub use health::{
    PoolHealthReport, acquire_admin_connection, check_pool_health, pool_slot_count, prime_pool,
    reset_pool,
};
pub use meta::{PoolMeta, TemplateInfo, TemplateMeta};
pub use reset::{ensure_default_session_state, seed_test_fixtures};
pub use stats::{CleanupDiagnostics, DatabaseStats, PoolStats, SlotStats};
pub use template::{optional_extension_missing, schema_fingerprint};
pub use test_database::TestDatabase;

use config::{PoolConfig, is_nextest_run};
use metrics::POOL_METRICS;
use provisioning::{
    CreateDatabaseOutcome, advisory_lock_key, connect_admin_with_retry,
    create_database_from_template, database_exists, detect_connection_budget,
    drop_database_if_exists, ensure_pool_database_exists, grant_pool_database_permissions,
    is_missing_database_error, is_timescaledb_missing_library_error,
    load_pool_meta, recreate_pool_database, store_pool_meta, url_with_db_name,
    wait_for_database_absence,
};
use slot::DatabaseSlot;
use template::{ensure_template_database, template_db_name};

// ── Pool test guard ─────────────────────────────────────────────────────────

static DATABASE_POOL_TEST_LOCK: std::sync::LazyLock<tokio::sync::Mutex<()>> =
    std::sync::LazyLock::new(|| tokio::sync::Mutex::new(()));

/// Acquire a global guard to run database pool tests exclusively.
pub async fn acquire_pool_test_guard() -> tokio::sync::MutexGuard<'static, ()> {
    DATABASE_POOL_TEST_LOCK.lock().await
}

fn format_acquisition_timeout_message(
    elapsed: Duration,
    attempts: usize,
    lock_holders: &str,
) -> String {
    format!(
        "Database acquisition timed out after {elapsed:.1?} ({attempts} attempts). \
         All slots may be permanently locked.\
         {lock_holders}"
    )
}

fn is_timescaledb_missing_library_schema_apply(err: &SinexError) -> bool {
    err.context_map()
        .get("error_class")
        .is_some_and(|value| value == "timescaledb_missing_library")
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
// Current implementation uses database COMMENT storage, so no manual
// stamp-file cleanup is required in the test pool lifecycle.

// ── Pool stats ──────────────────────────────────────────────────────────────

/// Get current pool statistics
pub fn get_pool_stats() -> PoolStats {
    // Aggregate connection counts if pool exists.
    let mut totals = (0usize, 0usize);
    if let Ok(pool_guard) = POOL.try_lock()
        && let Some(pool) = pool_guard.as_ref().cloned()
    {
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
    if let Ok(pool_guard) = POOL.try_lock()
        && let Some(pool) = pool_guard.as_ref().cloned()
    {
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
                        last_clean_time: time.map(|t| Timestamp::new(t).format_rfc3339()),
                        last_clean_result: result,
                        residuals,
                        quarantined: slot.quarantined.load(Ordering::SeqCst),
                    }
                } else {
                    SlotStats {
                        name: slot.name.clone(),
                        total_connections: 0,
                        idle_connections: 0,
                        last_clean_time: time.map(|t| Timestamp::new(t).format_rfc3339()),
                        last_clean_result: result,
                        residuals,
                        quarantined: slot.quarantined.load(Ordering::SeqCst),
                    }
                }
            })
            .collect();
    }

    Vec::new()
}

// ── Test database cleanup re-export ─────────────────────────────────────────

// force_event_material_cleanup is pub(crate) on reset.rs, accessible via reset:: path

// ── Global pool ─────────────────────────────────────────────────────────────

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

// ── Slot acquisition helpers ────────────────────────────────────────────────

/// Build pool options for connecting to a slot database.
pub(super) fn slot_pool_options(
    slot_max_connections: u32,
    acquire_timeout: Duration,
) -> sqlx::postgres::PgPoolOptions {
    sqlx::postgres::PgPoolOptions::new()
        .max_connections(slot_max_connections)
        .acquire_timeout(acquire_timeout)
        .before_acquire(|conn, _meta| {
            Box::pin(async move {
                if let Err(err) = reset::ensure_default_session_state_conn_pub(conn).await {
                    slog!(Level::Warn, "session_preflight_failed", error = err);
                    return Ok(false);
                }
                Ok(true)
            })
        })
}

/// Try to connect to a slot database, handling missing databases and broken shared libraries.
/// Returns `Some(pool)` on success, `None` if the slot should be skipped.
async fn try_connect_to_slot(
    slot: &DatabaseSlot,
    slot_max_connections: u32,
) -> Option<sinex_db::DbPool> {
    let connect =
        || slot_pool_options(slot_max_connections, Duration::from_secs(5)).connect(&slot.url);

    match tokio::time::timeout(Duration::from_secs(5), connect()).await {
        Err(_) => {
            slog!(Level::Warn, "connect_timeout", slot = slot.name);
            None
        }
        Ok(Ok(pool)) => Some(pool),
        Ok(Err(err)) => try_recover_slot_connection(slot, err, slot_max_connections).await,
    }
}

/// Attempt recovery when the initial connection to a slot fails.
async fn try_recover_slot_connection(
    slot: &DatabaseSlot,
    err: sqlx::Error,
    slot_max_connections: u32,
) -> Option<sinex_db::DbPool> {
    if is_missing_database_error(&err) {
        if let Err(e) = ensure_pool_database_exists(&slot.name, &slot.url).await {
            slog!(Level::Warn, "provision_failed", slot = slot.name, error = e);
            return None;
        }
        let connect =
            || slot_pool_options(slot_max_connections, Duration::from_secs(5)).connect(&slot.url);
        match tokio::time::timeout(Duration::from_secs(5), connect()).await {
            Ok(Ok(pool)) => return Some(pool),
            Ok(Err(_)) => return None,
            Err(_) => {
                slog!(
                    Level::Warn,
                    "connect_timeout_post_provision",
                    slot = slot.name
                );
                return None;
            }
        }
    }

    if is_timescaledb_missing_library_error(&err) {
        slog!(Level::Warn, "timescaledb_library_missing", slot = slot.name);
        if let Err(recreate_err) = recreate_pool_database(&slot.name, &slot.url).await {
            slog!(
                Level::Error,
                "timescaledb_recreate_failed",
                slot = slot.name,
                error = recreate_err
            );
            slot.quarantined.store(true, Ordering::SeqCst);
        }
    }
    None
}

/// Query pg_stat_activity to identify processes holding advisory locks.
/// Returns a formatted string (empty if query fails) for inclusion in timeout errors.
async fn query_advisory_lock_holders() -> String {
    // Best-effort: connect to the admin DB and query lock holders.
    // If this fails, return empty string — the timeout error is still useful without it.
    let base_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        crate::infra::stack::StackConfig::for_current_checkout()
            .map(|c| c.database_url())
            .unwrap_or_default()
    });
    if base_url.is_empty() {
        return String::new();
    }

    let query = "SELECT pid, usename, application_name, state, query_start::text, left(query, 80) \
                 FROM pg_stat_activity a \
                 JOIN pg_locks l ON l.pid = a.pid \
                 WHERE l.locktype = 'advisory' AND a.pid <> pg_backend_pid() \
                 LIMIT 10";

    let result = tokio::time::timeout(Duration::from_secs(3), async {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(&base_url)
            .await?;
        let rows = sqlx::query(query).fetch_all(&pool).await?;
        pool.close().await;
        Ok::<_, sqlx::Error>(rows)
    })
    .await;

    match result {
        Ok(Ok(rows)) if !rows.is_empty() => {
            use sqlx::Row;
            let entries: Vec<String> = rows
                .iter()
                .map(|r| {
                    format!(
                        "  pid={} user={} state={} query={}",
                        r.try_get::<i32, _>("pid").unwrap_or(0),
                        r.try_get::<&str, _>("usename").unwrap_or("?"),
                        r.try_get::<&str, _>("state").unwrap_or("?"),
                        r.try_get::<&str, _>("left").unwrap_or("?"),
                    )
                })
                .collect();
            format!("\nAdvisory lock holders:\n{}", entries.join("\n"))
        }
        _ => String::new(),
    }
}

/// Try to acquire a PostgreSQL advisory lock on a slot.
/// Returns `Some(lock_conn)` on success, `None` if the slot should be skipped.
/// Caller must close the pool if this returns `None`.
async fn try_advisory_lock_slot(
    pool: &sinex_db::DbPool,
    slot: &DatabaseSlot,
) -> Option<sqlx::pool::PoolConnection<sqlx::Postgres>> {
    let lock_id = advisory_lock_key(&slot.name);

    let mut lock_conn = match tokio::time::timeout(Duration::from_secs(5), pool.acquire()).await {
        Ok(Ok(conn)) => conn,
        Ok(Err(err)) => {
            slog!(
                Level::Warn,
                "lock_conn_failed",
                slot = slot.name,
                error = err
            );
            let () = pool.close().await;
            return None;
        }
        Err(_) => {
            slog!(Level::Warn, "lock_conn_timeout", slot = slot.name);
            let () = pool.close().await;
            return None;
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
                slog!(Level::Warn, "timescaledb_library_lock", slot = slot.name);
                drop(lock_conn);
                let () = pool.close().await;
                if let Err(recreate_err) = recreate_pool_database(&slot.name, &slot.url).await {
                    slog!(
                        Level::Error,
                        "timescaledb_recreate_failed",
                        slot = slot.name,
                        error = recreate_err
                    );
                    slot.quarantined.store(true, Ordering::SeqCst);
                }
            } else {
                slog!(
                    Level::Warn,
                    "lock_query_failed",
                    slot = slot.name,
                    error = err
                );
                drop(lock_conn);
                let () = pool.close().await;
            }
            return None;
        }
        Err(_) => {
            slog!(Level::Warn, "lock_query_timeout", slot = slot.name);
            drop(lock_conn);
            let () = pool.close().await;
            return None;
        }
    };

    if !lock_acquired {
        drop(lock_conn);
        pool.close().await;
        return None;
    }

    Some(lock_conn)
}

/// Release a slot: unlock advisory lock, close pool, mark unused.
async fn release_slot(
    slot: &DatabaseSlot,
    pool: &sinex_db::DbPool,
    lock_conn: &mut sqlx::pool::PoolConnection<sqlx::Postgres>,
    lock_id: i64,
) {
    let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(lock_id)
        .execute(lock_conn.as_mut())
        .await;
    let () = pool.close().await;
    {
        let mut pool_opt = slot.pool.lock();
        *pool_opt = None;
    }
    slot.in_use.store(false, Ordering::Release);
}

// ── DatabasePool ────────────────────────────────────────────────────────────

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
            let expected_fingerprint = schema_fingerprint();

            let mut slots = Vec::with_capacity(config.size);
            for i in 0..config.size {
                let name = format!("sinex_test_pool_{i}");
                let url =
                    url_with_db_name(&config.base_url, &name).map_err(|e| eyre!(e.to_string()))?;
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
                    schema_verified: AtomicBool::new(false),
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
        let expected_fingerprint = schema_fingerprint();

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
            let provision_lock =
                advisory_lock_key(&format!("{}::pool_provision", template.name));
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
                        // Existing DBs are reconciled declaratively; recreate only when unrecoverable
                        // drift is detected (e.g. stale Timescale shared library).
                        let db_url = url_with_db_name(&base_url, &name)
                            .map_err(|e| color_eyre::eyre::eyre!(e.to_string()))?;
                        let mut needs_recreate = false;

                        if let Ok(db_pool) = sqlx::postgres::PgPoolOptions::new()
                            .max_connections(slot_max_conns.max(1))
                            .acquire_timeout(Duration::from_secs(5))
                            .connect(&db_url)
                            .await
                        {
                            if let Ok(rows) = sqlx::query(
                        r"SELECT extname, extversion FROM pg_extension WHERE extname IN ('timescaledb','pg_jsonschema','vector','pg_trgm')"
                            )
                            .fetch_all(&db_pool)
                            .await
                            {
                                for row in rows {
                                    let extname: String = sqlx::Row::get(&row, "extname");
                                    let extversion: String = sqlx::Row::get(&row, "extversion");
                                    if let Some(t_ver) = template_ext_versions.get(&extname)
                                        && &extversion != t_ver {
                                            needs_recreate = true;
                                            eprintln!(
                                                "  Drift detected in {extname} ({extversion} != {t_ver}), recreating {name}",
                                            );
                                            break;
                                        }
                                }
                                if !needs_recreate
                                    && let Err(err) = sinex_db::apply_schema_for_url(&db_url).await
                                {
                                    if is_timescaledb_missing_library_schema_apply(&err) {
                                        needs_recreate = true;
                                        eprintln!(
                                            "  Stale TimescaleDB library reference in {name}; recreating"
                                        );
                                    } else {
                                        needs_recreate = true;
                                        eprintln!(
                                            "  Declarative schema apply failed for {name} ({err}); recreating"
                                        );
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
                            match create_database_from_template(
                                &mut conn,
                                &name,
                                &template_name,
                            )
                            .await?
                            {
                                CreateDatabaseOutcome::Created => {
                                    eprintln!(
                                        "  Recreated pool database from template: {name}"
                                    );
                                    let meta = PoolMeta {
                                        fingerprint: schema_fingerprint(),
                                        extensions: template_ext_versions.clone(),
                                        dirty: false,
                                        updated_at_rfc3339: Timestamp::now().format_rfc3339(),
                                        last_error: None,
                                    };
                                    let _ =
                                        store_pool_meta(conn.as_mut(), &name, &meta).await;
                                }
                                CreateDatabaseOutcome::AlreadyExists => {
                                    eprintln!(
                                        "  Database {name} was recreated by another task; reusing"
                                    );
                                }
                            }
                        } else {
                            let meta = PoolMeta {
                                fingerprint: schema_fingerprint(),
                                extensions: template_ext_versions.clone(),
                                dirty: false,
                                updated_at_rfc3339: Timestamp::now().format_rfc3339(),
                                last_error: None,
                            };
                            let _ = store_pool_meta(conn.as_mut(), &name, &meta).await;
                        }
                    } else {
                        match create_database_from_template(
                            &mut conn,
                            &name,
                            &template_name,
                        )
                        .await?
                        {
                            CreateDatabaseOutcome::Created => {
                                eprintln!("  Created new pool database: {name}");
                                let meta = PoolMeta {
                                    fingerprint: schema_fingerprint(),
                                    extensions: template_ext_versions.clone(),
                                    dirty: false,
                                    updated_at_rfc3339: Timestamp::now().format_rfc3339(),
                                    last_error: None,
                                };
                                let _ =
                                    store_pool_meta(conn.as_mut(), &name, &meta).await;
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
                    let url = url_with_db_name(&base_url, &name)
                        .map_err(|e| color_eyre::eyre::eyre!(e.to_string()))?;
                    reset::ensure_pool_db_invariants(&url)
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
                    .map_err(|e| {
                        SinexError::service(format!("Database creation task failed: {e}"))
                    })?
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
                    schema_verified: AtomicBool::new(false),
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

    /// Acquire a database from the pool
    async fn acquire(&self) -> TestResult<TestDatabase> {
        let start_time = std::time::Instant::now();
        let mut attempts = 0;

        const MAX_ACQUISITION_TIMEOUT: Duration = Duration::from_mins(2);
        const MAX_ATTEMPTS: usize = 100;

        let pid = std::process::id();
        let random_offset = rand::random::<u64>() as usize;
        let start_index = (pid as usize + random_offset) % self.slots.len();
        slog!(
            Level::Debug,
            "acquire_start",
            pid = pid,
            start_index = start_index,
            pool_size = self.slots.len()
        );

        loop {
            let elapsed = start_time.elapsed();
            if elapsed >= MAX_ACQUISITION_TIMEOUT {
                // Query pg_stat_activity to include lock holder context in the error.
                let lock_holders = query_advisory_lock_holders().await;
                return Err(eyre!(format_acquisition_timeout_message(
                    elapsed,
                    attempts,
                    &lock_holders
                )));
            }

            // Warn once when acquisition has been stalled for an unusually long time.
            if elapsed > Duration::from_secs(10) && attempts == 1 {
                slog!(
                    Level::Warn,
                    "acquire_stalled",
                    pid = pid,
                    elapsed_secs = elapsed.as_secs(),
                    message = "Slot acquisition stalled; pool may be exhausted or a test crashed while holding a lock"
                );
            }

            for i in 0..self.slots.len() {
                let slot_index = (start_index + i) % self.slots.len();
                let slot = &self.slots[slot_index];

                if slot.quarantined.load(Ordering::SeqCst) {
                    slog!(Level::Warn, "slot_quarantined", slot = slot.name);
                    continue;
                }

                let Some(pool) = try_connect_to_slot(slot, self.slot_max_connections).await else {
                    continue;
                };

                // Skip verify_slot_health — pool.acquire() in try_advisory_lock_slot
                // already proves liveness, and the before_acquire callback on every
                // pooled connection runs ensure_default_session_state.
                let Some(lock_conn) = try_advisory_lock_slot(&pool, slot).await else {
                    continue;
                };

                let lock_id = advisory_lock_key(&slot.name);

                if let Ok(db) = self
                    .finalize_slot_acquisition(slot, &pool, lock_conn, lock_id, pid, start_time)
                    .await
                {
                    return Ok(db);
                }
            }

            attempts += 1;
            if attempts >= MAX_ATTEMPTS {
                let total_time = start_time.elapsed();
                return Err(eyre!(
                    "Failed to acquire database after {attempts} attempts ({total_time:.1?}). \
                     Consider increasing pool size or reducing test parallelism."
                ));
            }

            if attempts % 10 == 0 {
                slog!(
                    Level::Warn,
                    "acquire_contention",
                    pid = pid,
                    attempt = attempts,
                    elapsed_ms = start_time.elapsed().as_millis()
                );
            }

            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Post-lock slot finalization: metadata check, cleaning, and return.
    /// Returns `Ok(TestDatabase)` on success, `Err(())` if the slot should be skipped.
    async fn finalize_slot_acquisition(
        &self,
        slot: &Arc<DatabaseSlot>,
        pool: &sinex_db::DbPool,
        mut lock_conn: sqlx::pool::PoolConnection<sqlx::Postgres>,
        lock_id: i64,
        pid: u32,
        start_time: std::time::Instant,
    ) -> std::result::Result<TestDatabase, ()> {
        slot.in_use.store(true, Ordering::SeqCst);
        {
            let mut pool_opt = slot.pool.lock();
            *pool_opt = Some(pool.clone());
        }

        let mut existing_meta = match tokio::time::timeout(
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

        if existing_meta.is_some() && !meta_matches {
            slog!(Level::Info, "slot_meta_mismatch", slot = slot.name);
            match reset::schema_mismatch_reason(pool).await {
                // Confirmed drift: keep existing slow path (recreate from template).
                Ok(Some(reason)) => {
                    slog!(
                        Level::Warn,
                        "slot_schema_drift",
                        slot = slot.name,
                        reason = reason
                    );
                    release_slot(slot, pool, &mut lock_conn, lock_id).await;
                    if let Err(recreate_err) = recreate_pool_database(&slot.name, &slot.url).await {
                        slog!(
                            Level::Error,
                            "slot_recreate_failed",
                            slot = slot.name,
                            error = recreate_err
                        );
                        slot.quarantined.store(true, Ordering::SeqCst);
                    }
                    return Err(());
                }
                // Metadata drift only: heal metadata and force one cleanup pass.
                Ok(None) => {
                    slog!(Level::Info, "slot_meta_heal", slot = slot.name);
                    if let Some(meta) = existing_meta.as_mut() {
                        meta.dirty = true;
                    }
                }
                // Conservative fallback: avoid expensive recreate on transient check errors.
                Err(err) => {
                    slog!(
                        Level::Warn,
                        "slot_meta_schema_check_failed",
                        slot = slot.name,
                        error = err
                    );
                    if let Some(meta) = existing_meta.as_mut() {
                        meta.dirty = true;
                    }
                }
            }
        }

        let was_clean = existing_meta.as_ref().is_some_and(|m| !m.dirty);

        // Mark dirty immediately after lock acquisition (crash-safe).
        let dirty_meta = PoolMeta {
            fingerprint: expected_fp.clone(),
            extensions: expected_ext.clone(),
            dirty: true,
            updated_at_rfc3339: Timestamp::now().format_rfc3339(),
            last_error: None,
        };
        if let Err(e) = store_pool_meta(lock_conn.as_mut(), &slot.name, &dirty_meta).await {
            slog!(
                Level::Warn,
                "meta_persist_failed",
                slot = slot.name,
                error = e
            );
        }

        if was_clean {
            // "clean metadata" can drift from actual schema (interrupted schema apply, old slots).
            // Verify once per slot per process before taking the fast path.
            if !slot.schema_verified.load(Ordering::Relaxed) {
                match reset::schema_mismatch_reason(pool).await {
                    Ok(Some(reason)) => {
                        slog!(
                            Level::Warn,
                            "slot_schema_drift",
                            slot = slot.name,
                            reason = reason
                        );
                        return self
                            .clean_and_acquire_slot(slot, pool, lock_conn, lock_id, pid, start_time)
                            .await;
                    }
                    Ok(None) => {
                        slot.schema_verified.store(true, Ordering::Relaxed);
                    }
                    Err(err) => {
                        slog!(
                            Level::Warn,
                            "slot_schema_check_failed",
                            slot = slot.name,
                            error = err
                        );
                        return self
                            .clean_and_acquire_slot(slot, pool, lock_conn, lock_id, pid, start_time)
                            .await;
                    }
                }
            }

            let acq_time = start_time.elapsed();
            POOL_METRICS.record_acquisition(acq_time);
            slog!(
                Level::Info,
                "slot_acquired",
                slot = slot.name,
                duration_ms = acq_time.as_millis(),
                pid = pid,
                clean = true
            );
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

        // Clean the slot before use
        self.clean_and_acquire_slot(slot, pool, lock_conn, lock_id, pid, start_time)
            .await
    }

    /// Clean a dirty slot and return a `TestDatabase`, or release on failure.
    async fn clean_and_acquire_slot(
        &self,
        slot: &Arc<DatabaseSlot>,
        pool: &sinex_db::DbPool,
        mut lock_conn: sqlx::pool::PoolConnection<sqlx::Postgres>,
        lock_id: i64,
        pid: u32,
        start_time: std::time::Instant,
    ) -> std::result::Result<TestDatabase, ()> {
        let clean_start = std::time::Instant::now();
        match reset::clean_database(slot, pool, &slot.name, &slot.url).await {
            Ok(()) => {
                let clean_time = clean_start.elapsed();
                let acq_time = start_time.elapsed();
                POOL_METRICS.record_acquisition(acq_time);
                slog!(
                    Level::Info,
                    "slot_acquired",
                    slot = slot.name,
                    duration_ms = acq_time.as_millis(),
                    clean_ms = clean_time.as_millis(),
                    pid = pid,
                    clean = false
                );
                Ok(TestDatabase {
                    name: slot.name.clone(),
                    pool: pool.clone(),
                    slot: slot.clone(),
                    lock_id,
                    lock_conn: Some(lock_conn),
                    acquired_at: Instant::now(),
                    acquisition_process_id: pid,
                })
            }
            Err(e) => {
                slog!(Level::Warn, "cleanup_failed", slot = slot.name, error = e);
                POOL_METRICS.record_cleanup_failure();

                let dirty_meta = PoolMeta {
                    fingerprint: self.expected_fingerprint.clone(),
                    extensions: self.expected_extensions.clone(),
                    dirty: true,
                    updated_at_rfc3339: Timestamp::now().format_rfc3339(),
                    last_error: Some(e.to_string()),
                };
                let _ = store_pool_meta(lock_conn.as_mut(), &slot.name, &dirty_meta).await;
                release_slot(slot, pool, &mut lock_conn, lock_id).await;
                Err(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::sinex_test;

    #[sinex_test]
    async fn test_format_acquisition_timeout_message_includes_hint_and_attempts() -> TestResult<()>
    {
        let msg = format_acquisition_timeout_message(Duration::from_mins(1), 120, "");
        assert!(msg.contains("permanently locked"), "got: {msg}");
        assert!(msg.contains("120 attempts"), "got: {msg}");
        Ok(())
    }

    #[sinex_test]
    async fn test_format_acquisition_timeout_message_includes_lock_holders() -> TestResult<()> {
        let lock_holders =
            "\n\nLock holders:\n  pid=1234 app=nextest query=SELECT pg_advisory_lock(42)";
        let msg = format_acquisition_timeout_message(Duration::from_secs(30), 5, lock_holders);
        assert!(msg.contains("Lock holders"), "got: {msg}");
        assert!(msg.contains("pg_advisory_lock"), "got: {msg}");
        Ok(())
    }
}
