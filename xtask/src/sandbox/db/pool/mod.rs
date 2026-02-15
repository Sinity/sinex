//! Database pool management for sandbox.

use crate::sandbox::prelude::*;
use parking_lot::Mutex;

use sinex_primitives::temporal::Timestamp;
use sinex_primitives::SinexError;

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
    acquire_admin_connection, check_pool_health, pool_slot_count, prime_pool, reset_pool,
    PoolHealthReport,
};
pub use meta::{PoolMeta, TemplateInfo, TemplateMeta};
pub use reset::{ensure_default_session_state, seed_test_fixtures};
pub use stats::{CleanupDiagnostics, DatabaseStats, PoolStats, SlotStats};
pub use template::{migrations_fingerprint, optional_extension_missing};
pub use test_database::TestDatabase;

use config::{is_nextest_run, PoolConfig};
use metrics::POOL_METRICS;
use provisioning::{
    advisory_lock_key, connect_admin_with_retry, create_database_from_template, database_exists,
    detect_connection_budget, drop_database_if_exists, ensure_pool_database_exists,
    grant_pool_database_permissions, is_missing_database_error,
    is_timescaledb_missing_library_error, is_timescaledb_missing_library_error_message,
    load_pool_meta, recreate_pool_database, store_pool_meta, wait_for_database_absence,
    CreateDatabaseOutcome,
};
use slot::DatabaseSlot;
use template::{ensure_template_database, template_db_name};

// ── Pool test guard ─────────────────────────────────────────────────────────

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

// ── Pool stats ──────────────────────────────────────────────────────────────

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
    }

    Vec::new()
}

// ── Test database cleanup re-export ─────────────────────────────────────────

#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use reset::force_event_material_cleanup_for_tests;

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
                        // Check extension versions against the template; drop/recreate if drifted
                        let db_url = base_url.replace("/sinex_dev", &format!("/{name}"));
                        let mut needs_recreate = false;

                        if let Ok(db_pool) = sqlx::postgres::PgPoolOptions::new()
                            .max_connections(slot_max_conns.max(1))
                            .acquire_timeout(Duration::from_secs(5))
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
                                        "SELECT to_regclass('core.events')::text",
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
                                        eprintln!(
                                            "  Failed to verify schema in {name}, recreating"
                                        );
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

                                    match (
                                        events_has_blobs,
                                        events_has_subnano,
                                        payload_has_updated_at,
                                    ) {
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
                                        (Err(err), _, _)
                                        | (_, Err(err), _)
                                        | (_, _, Err(err)) => {
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
                                        fingerprint: migrations_fingerprint(),
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
                            eprintln!("  Reusing existing pool database: {name}");
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
                                    fingerprint: migrations_fingerprint(),
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
                    let url = base_url.replace("/sinex_dev", &format!("/{name}"));
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
                    if let Err(err) = reset::ensure_default_session_state_conn_pub(conn).await {
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
                    Self::slot_pool_options(self.slot_max_connections, Duration::from_secs(5))
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
                match tokio::time::timeout(
                    Duration::from_secs(2),
                    reset::ensure_default_session_state(&pool),
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
                    drop(lock_conn);
                    pool.close().await;
                    continue;
                }

                // We got the lock! This database is ours for the duration of the test
                eprintln!(
                    "🔑 Process {} acquired database slot: {} with advisory lock {}",
                    pid, slot.name, lock_id
                );

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
                    updated_at_rfc3339: Timestamp::now().format_rfc3339(),
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

                // Clean it before use
                let clean_start = std::time::Instant::now();
                match reset::clean_database(slot, &pool, &slot.name, &slot.url).await {
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
                            updated_at_rfc3339: Timestamp::now().format_rfc3339(),
                            last_error: Some(e.to_string()),
                        };
                        let _ = store_pool_meta(lock_conn.as_mut(), &slot.name, &dirty_meta).await;

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

            if attempts % 10 == 0 {
                let elapsed = start_time.elapsed();
                eprintln!(
                    "⚠️  Process {pid} waiting for database slot (attempt {attempts}, {elapsed:.1?} elapsed)"
                );
            }

            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}
