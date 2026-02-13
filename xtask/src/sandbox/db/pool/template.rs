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
    let ensure_deadline = std::time::Instant::now() + Duration::from_secs(45);
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

        if std::time::Instant::now() > ensure_deadline {
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

            // Seed well-known test fixture data that must exist for FK constraints.
            // sinex_primitives::testing::event_fixture() uses material_id 01H00000000000000000000000
            // which must exist in raw.source_material_registry for the core.events FK to pass.
            sqlx::query(
                "INSERT INTO raw.source_material_registry \
                    (id, material_kind, source_identifier, status, timing_info_type) \
                 VALUES ('01H00000000000000000000000'::ulid, 'annex', 'test-fixture-material', 'completed', 'realtime') \
                 ON CONFLICT (id) DO NOTHING"
            )
            .execute(&template_pool)
            .await?;

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

// ── Extension management ────────────────────────────────────────────────────

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
