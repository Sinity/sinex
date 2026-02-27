//! Template database management — creation, migration, fingerprinting.

use crate::sandbox::prelude::*;
use parking_lot::Mutex;
use sinex_db::DbPool;
use sqlx::postgres::PgConnection;
use sqlx::Connection;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::OnceLock;
use std::time::Duration;
use tracing::warn;

use sha2::{Digest, Sha256};

use super::config::replace_db_name;
use super::meta::{TemplateInfo, TemplateMeta};
use super::metrics::POOL_METRICS;
use super::provisioning::{
    advisory_lock_key, connect_admin_with_retry, default_extension_versions, load_template_meta,
    quote_ident, store_template_meta, url_with_db_name,
};

// ── Statics ─────────────────────────────────────────────────────────────────

static OPTIONAL_EXTENSION_MISSING: std::sync::LazyLock<Mutex<HashMap<String, String>>> =
    std::sync::LazyLock::new(|| Mutex::new(HashMap::new()));

/// Template database name cached for the current test process
static TEMPLATE_DB_NAME: OnceLock<String> = OnceLock::new();

pub(crate) fn template_db_name() -> Option<String> {
    TEMPLATE_DB_NAME.get().cloned()
}

/// Mutex to ensure only one thread creates the template database
static TEMPLATE_CREATION_LOCK: std::sync::LazyLock<tokio::sync::Mutex<()>> =
    std::sync::LazyLock::new(|| tokio::sync::Mutex::new(()));

// ── TemplateGuard ───────────────────────────────────────────────────────────

/// Holds a shared advisory lock for the template database on a live admin connection.
///
/// Nextest runs each test in its own process, so we need a cross-process coordination mechanism
/// that ensures the template database cannot be dropped/recreated while this process is cloning
/// pool databases from it.
pub(super) struct TemplateGuard {
    pub(super) info: TemplateInfo,
    pub(super) lock_key: i64,
    pub(super) admin_conn: PgConnection,
}

impl TemplateGuard {
    pub(super) async fn release(mut self) -> TestResult<()> {
        let _ = sqlx::query("SELECT pg_advisory_unlock_shared($1)")
            .bind(self.lock_key)
            .execute(&mut self.admin_conn)
            .await;
        self.admin_conn.close().await?;
        Ok(())
    }
}

// ── Fingerprinting ──────────────────────────────────────────────────────────

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

    let mut entries: Vec<PathBuf> = std::fs::read_dir(&migrations_dir)
        .ok()?
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .collect();
    entries.extend(
        std::fs::read_dir(&schema_src_dir)
            .ok()?
            .filter_map(|entry| entry.ok().map(|e| e.path())),
    );
    // Sort entries to ensure consistent ordering
    entries.sort();

    let mut hasher = Sha256::new();
    // Bump this version when template seed data changes (forces template rebuild).
    // This is separate from schema migrations — it tracks data that must exist in
    // every test template (e.g. well-known fixture IDs for FK constraints).
    hasher.update(b"seed-version:3\n");
    for path in entries {
        if path.is_file() {
            // Hash filename first
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                hasher.update(name.as_bytes());
                hasher.update(b":"); // Separator between name and content
            }
            // Then hash content
            if let Ok(bytes) = std::fs::read(&path) {
                hasher.update(bytes);
                hasher.update(b"|"); // Separator between files
            }
        }
    }

    Some(format!("{:x}", hasher.finalize()))
}

// ── Template lifecycle ──────────────────────────────────────────────────────

pub(super) async fn harden_template_database(
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

pub(super) async fn ensure_template_database(
    admin_url: &str,
    _base_url: &str,
    slot_max_connections: u32,
) -> TestResult<TemplateGuard> {
    let _lock = TEMPLATE_CREATION_LOCK.lock().await;

    let template_name = "sinex_test_template_shared";
    eprintln!("🔧 Checking template database {template_name} ...");
    let template_start = std::time::Instant::now();

    let desired_fingerprint = migrations_fingerprint();
    if desired_fingerprint.is_none() {
        eprintln!(
            "⚠️  Unable to compute migrations fingerprint; template caching disabled for this run"
        );
    }

    let mut admin_conn = connect_admin_with_retry(admin_url).await?;
    let lock_key = advisory_lock_key(template_name);

    let ensure_deadline = std::time::Instant::now() + Duration::from_secs(45);
    let mut backoff = Duration::from_millis(25);
    loop {
        // Take shared lock and check for reusable template
        take_shared_advisory_lock(&mut admin_conn, lock_key).await?;

        if let Some(extensions) =
            check_template_reuse(&mut admin_conn, template_name, &desired_fingerprint, true).await?
        {
            harden_template_database(&mut admin_conn, template_name).await?;
            cache_template_name(template_name);
            return Ok(build_template_guard(
                template_name,
                extensions,
                lock_key,
                admin_conn,
            ));
        }

        // Not reusable. Release shared lock, try to become the exclusive rebuilder.
        let _ = sqlx::query("SELECT pg_advisory_unlock_shared($1)")
            .bind(lock_key)
            .execute(&mut admin_conn)
            .await;

        if std::time::Instant::now() > ensure_deadline {
            return Err(eyre!(
                "Template database was not ready before deadline (another test process may be stuck recreating it)"
            ));
        }

        let got_exclusive: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
            .bind(lock_key)
            .fetch_one(&mut admin_conn)
            .await?;

        if !got_exclusive {
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(Duration::from_millis(250));
            continue;
        }

        // Under exclusive lock: re-check before destructive work (another process may have rebuilt)
        if let Some(extensions) =
            check_template_reuse(&mut admin_conn, template_name, &desired_fingerprint, false)
                .await?
        {
            downgrade_to_shared_lock(&mut admin_conn, lock_key).await?;
            harden_template_database(&mut admin_conn, template_name).await?;
            cache_template_name(template_name);
            return Ok(build_template_guard(
                template_name,
                extensions,
                lock_key,
                admin_conn,
            ));
        }

        // Rebuild the template from scratch
        let extensions = rebuild_template(
            &mut admin_conn,
            template_name,
            admin_url,
            &desired_fingerprint,
            slot_max_connections,
            template_start,
        )
        .await?;

        downgrade_to_shared_lock(&mut admin_conn, lock_key).await?;
        cache_template_name(template_name);
        return Ok(build_template_guard(
            template_name,
            extensions,
            lock_key,
            admin_conn,
        ));
    }
}

/// Take a shared advisory lock for template checking.
async fn take_shared_advisory_lock(admin_conn: &mut PgConnection, lock_key: i64) -> TestResult<()> {
    tokio::time::timeout(
        Duration::from_secs(15),
        sqlx::query("SELECT pg_advisory_lock_shared($1)")
            .bind(lock_key)
            .execute(&mut *admin_conn),
    )
    .await
    .map_err(|_| eyre!("Template shared-lock timeout"))?
    .map_err(|e| eyre!(format!("Template shared-lock failed: {e}")))?;
    Ok(())
}

/// Downgrade an exclusive advisory lock to shared.
async fn downgrade_to_shared_lock(admin_conn: &mut PgConnection, lock_key: i64) -> TestResult<()> {
    sqlx::query("SELECT pg_advisory_lock_shared($1)")
        .bind(lock_key)
        .execute(&mut *admin_conn)
        .await?;
    let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(lock_key)
        .execute(&mut *admin_conn)
        .await;
    Ok(())
}

/// Check if an existing template can be reused. Returns `Some(extensions)` if reusable.
/// When `check_drift` is true, also verifies extension versions haven't changed.
async fn check_template_reuse(
    admin_conn: &mut PgConnection,
    template_name: &str,
    desired_fingerprint: &Option<String>,
    check_drift: bool,
) -> TestResult<Option<HashMap<String, String>>> {
    let exists: bool = sqlx::query_scalar(&format!(
        "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = '{template_name}')"
    ))
    .fetch_one(&mut *admin_conn)
    .await?;

    if !exists {
        return Ok(None);
    }

    let meta = load_template_meta(admin_conn, template_name).await?;

    let extensions = match (&desired_fingerprint, meta) {
        (Some(fp), Some(m)) if m.fingerprint == *fp && !m.extensions.is_empty() => {
            eprintln!("✅ Template database {template_name} reused (migrations unchanged)");
            m.extensions
        }
        (Some(fp), Some(m)) if m.fingerprint == *fp => {
            eprintln!(
                "♻️  Template metadata missing extension versions ({template_name}); recreating template"
            );
            let _ = m;
            return Ok(None);
        }
        (Some(fp), Some(m)) => {
            eprintln!(
                "♻️  Migration fingerprint changed ({} -> {fp}); recreating template",
                m.fingerprint
            );
            return Ok(None);
        }
        (Some(_), None) => {
            eprintln!("ℹ️  No template metadata found; recreating template");
            return Ok(None);
        }
        (None, Some(m)) if !m.extensions.is_empty() => {
            eprintln!("✅ Template database {template_name} reused (no fingerprint)");
            m.extensions
        }
        (None, Some(_)) => {
            eprintln!(
                "♻️  Template metadata missing extension versions ({template_name}); recreating template"
            );
            return Ok(None);
        }
        (None, None) => {
            eprintln!("ℹ️  Template metadata unavailable and fingerprint missing; recreating");
            return Ok(None);
        }
    };

    // Check extension version drift (e.g. TimescaleDB upgrade changes shared object paths)
    if check_drift {
        let defaults = default_extension_versions(admin_conn).await?;
        for (ext, template_ver) in &extensions {
            if let Some(default_ver) = defaults.get(ext) {
                if default_ver != template_ver {
                    eprintln!(
                        "♻️  Extension {ext} default_version changed ({template_ver} -> {default_ver}); recreating template"
                    );
                    return Ok(None);
                }
            }
        }
    }

    Ok(Some(extensions))
}

/// Rebuild the template database from scratch: drop, create, migrate, seed, optimize.
async fn rebuild_template(
    admin_conn: &mut PgConnection,
    template_name: &str,
    admin_url: &str,
    desired_fingerprint: &Option<String>,
    slot_max_connections: u32,
    template_start: std::time::Instant,
) -> TestResult<HashMap<String, String>> {
    POOL_METRICS.record_template_recreation();
    eprintln!(
        "♻️  Template database '{template_name}' requires recreation; rebuilding from scratch"
    );

    // Terminate connections and drop
    let terminate_query = format!(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
         WHERE datname = '{template_name}' AND pid <> pg_backend_pid()"
    );
    let _ = sqlx::query(&terminate_query)
        .execute(&mut *admin_conn)
        .await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let drop_query = format!("DROP DATABASE IF EXISTS {template_name} WITH (FORCE)");
    if sqlx::query(&drop_query)
        .execute(&mut *admin_conn)
        .await
        .is_err()
    {
        let fallback = format!("DROP DATABASE IF EXISTS {template_name}");
        sqlx::query(&fallback).execute(&mut *admin_conn).await?;
    }

    // Create fresh database
    create_template_db(&mut *admin_conn, template_name).await?;

    // Run migrations and seed data
    let template_admin_url = replace_db_name(admin_url, template_name);
    let template_pool_max = slot_max_connections.max(1).saturating_mul(2).max(4);
    let extensions = run_template_migrations(template_name, &template_admin_url, template_pool_max)
        .await
        .map_err(|e| eyre!(format!("Template migration/setup failed: {e}")))?;

    let template_elapsed = template_start.elapsed();
    eprintln!("✅ Template database created in {template_elapsed:?}");

    // Persist metadata
    if let Some(fp) = desired_fingerprint {
        let meta = TemplateMeta {
            fingerprint: fp.clone(),
            extensions: extensions.clone(),
        };
        if let Err(err) = store_template_meta(admin_conn, template_name, &meta).await {
            eprintln!("⚠️  Failed to persist template metadata for {template_name}: {err}");
            warn!("Failed to persist template metadata: {err}");
        }
    }

    harden_template_database(admin_conn, template_name).await?;
    cache_template_name(template_name);

    Ok(extensions)
}

/// Create a template database, tolerating "already exists" races.
async fn create_template_db(admin_conn: &mut PgConnection, template_name: &str) -> TestResult<()> {
    let create_query = format!("CREATE DATABASE {template_name}");
    match tokio::time::timeout(
        Duration::from_secs(10),
        sqlx::query(&create_query).execute(&mut *admin_conn),
    )
    .await
    {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(err)) => {
            let err_str = err.to_string();
            if err_str.contains("already exists") || err_str.contains("duplicate key value") {
                eprintln!(
                    "  Template database {template_name} already exists; reusing existing instance"
                );
                Ok(())
            } else {
                Err(eyre!(format!("Create database failed: {err}")))
            }
        }
        Err(_) => Err(eyre!("Create database timeout")),
    }
}

/// Connect to the template database, run migrations, install extensions, seed data.
async fn run_template_migrations(
    template_name: &str,
    template_admin_url: &str,
    template_pool_max: u32,
) -> TestResult<HashMap<String, String>> {
    let template_migration_url = if let Ok(super_url) = std::env::var("DATABASE_URL_SUPERUSER") {
        url_with_db_name(&super_url, template_name)?
    } else {
        url_with_db_name(template_admin_url, template_name)?
    };

    let template_pool: DbPool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(template_pool_max)
        .min_connections(1)
        .max_lifetime(Duration::from_mins(5))
        .idle_timeout(Duration::from_secs(10))
        .acquire_timeout(Duration::from_secs(15))
        .connect(&template_migration_url)
        .await?;

    apply_test_session_optimizations(&template_pool).await?;

    eprintln!("  📋 Running migrations on template database...");
    check_required_extensions(&template_pool)
        .await
        .map_err(|e| {
            eprintln!("❌ Missing required PostgreSQL extensions: {e}");
            eprintln!("   Check NixOS PostgreSQL configuration and required extensions.");
            e
        })?;

    // Temporarily point DATABASE_URL at the template for the migration helper
    let prev_db_url = std::env::var("DATABASE_URL").ok();
    unsafe { std::env::set_var("DATABASE_URL", &template_migration_url) };

    let migrate_result = tokio::time::timeout(
        Duration::from_secs(30),
        sinex_db::run_migrations_for_url(&template_migration_url),
    )
    .await
    .map_err(|_| eyre!("Migration timeout - check if all required extensions are installed"))
    .and_then(|res| res.map_err(|e| eyre!(format!("Migration failed: {e}"))));

    if let Some(url) = prev_db_url {
        unsafe { std::env::set_var("DATABASE_URL", url) };
    }
    migrate_result?;

    grant_template_permissions(&template_pool).await;

    // Seed well-known test fixture data for FK constraints
    sqlx::query(
        "INSERT INTO raw.source_material_registry \
            (id, material_kind, source_identifier, status, timing_info_type) \
         VALUES ('01H00000000000000000000000'::ulid, 'annex', 'test-fixture-material', 'completed', 'realtime') \
         ON CONFLICT (id) DO NOTHING",
    )
    .execute(&template_pool)
    .await?;

    optimize_template_for_tests(&template_pool).await?;
    let extensions = collect_extension_versions(&template_pool).await?;
    template_pool.close().await;
    Ok(extensions)
}

/// Grant schema permissions to the non-superuser role in the template database.
async fn grant_template_permissions(template_pool: &DbPool) {
    let Ok(Some(granter)) = crate::sandbox::db::permissions::PermissionGranter::from_env() else {
        return;
    };
    let Some(username) = std::env::var("DATABASE_URL_APP").ok().and_then(|url| {
        url.split("://")
            .nth(1)
            .and_then(|s| s.split('@').next().map(std::string::ToString::to_string))
    }) else {
        return;
    };

    eprintln!("  🔑 Granting schema permissions to user '{username}' in template database");
    use sinex_schema::schema_registry;
    for schema in schema_registry::SINEX_SCHEMAS {
        if let Err(e) = granter
            .grant_schema_access(template_pool, schema.name)
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

fn cache_template_name(template_name: &str) {
    if TEMPLATE_DB_NAME.get().is_none() {
        let _ = TEMPLATE_DB_NAME.set(template_name.to_string());
    }
}

fn build_template_guard(
    template_name: &str,
    extensions: HashMap<String, String>,
    lock_key: i64,
    admin_conn: PgConnection,
) -> TemplateGuard {
    TemplateGuard {
        info: TemplateInfo {
            name: template_name.to_string(),
            extensions,
        },
        lock_key,
        admin_conn,
    }
}

// ── Extension management ────────────────────────────────────────────────────

/// Check if a named extension is available in `pg_available_extensions`.
async fn is_extension_available(pool: &DbPool, name: &str) -> TestResult<bool> {
    let found: Option<String> =
        sqlx::query_scalar("SELECT name FROM pg_available_extensions WHERE name = $1")
            .bind(name)
            .fetch_optional(pool)
            .await?;
    Ok(found.is_some())
}

/// Install optional extensions, warning (not failing) if unavailable.
async fn install_optional_extensions(pool: &DbPool) {
    let optional_extensions = [
        ("pg_jsonschema", "pg_jsonschema for JSON validation"),
        ("vector", "pgvector for vector similarity search"),
    ];

    let mut missing = Vec::new();
    for (ext_name, description) in optional_extensions {
        match is_extension_available(pool, ext_name).await {
            Ok(false) | Err(_) => {
                missing.push((ext_name.to_string(), description.to_string()));
                continue;
            }
            Ok(true) => {}
        }
        if let Err(err) = ensure_extension_installed(pool, ext_name).await {
            warn!("Failed to auto-install optional extension '{ext_name}': {err}");
            missing.push((ext_name.to_string(), description.to_string()));
        }
    }

    if !missing.is_empty() {
        let mut guard = OPTIONAL_EXTENSION_MISSING.lock();
        for (ext_name, description) in missing {
            if guard
                .insert(ext_name.clone(), description.clone())
                .is_none()
            {
                warn!(
                    "Optional PostgreSQL extension '{ext_name}' unavailable; \
                     related features/tests will be skipped ({description})"
                );
            }
        }
    }
}

/// Check if required `PostgreSQL` extensions are available
async fn check_required_extensions(pool: &DbPool) -> TestResult<()> {
    let required_extensions = [
        ("ulid", "ULID extension for primary keys"),
        ("timescaledb", "TimescaleDB for hypertable partitioning"),
    ];

    let mut missing_required = Vec::new();
    for (ext_name, description) in required_extensions {
        if !is_extension_available(pool, ext_name).await? {
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

    install_optional_extensions(pool).await;
    Ok(())
}

async fn collect_extension_versions(pool: &DbPool) -> TestResult<HashMap<String, String>> {
    let rows = sqlx::query(
        r#"SELECT extname, extversion FROM pg_extension WHERE extname IN ('timescaledb','ulid','pg_jsonschema','vector')"#
    )
    .fetch_all(pool)
    .await?;

    let mut map = HashMap::new();
    for row in rows {
        let extname: String = sqlx::Row::get(&row, "extname");
        let extversion: String = sqlx::Row::get(&row, "extversion");
        map.insert(extname, extversion);
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

// ── Optimizations ───────────────────────────────────────────────────────────

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
                sqlx::postgres::PgQueryResult::default()
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
                sqlx::postgres::PgQueryResult::default()
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
