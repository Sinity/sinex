//! Template database management — creation, schema apply, fingerprinting.

use crate::sandbox::prelude::*;
use color_eyre::eyre::{WrapErr, eyre};
use parking_lot::Mutex;
use sinex_db::DbPool;
use sqlx::Connection;
use sqlx::postgres::PgConnection;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::OnceLock;
use std::time::Duration;

use sha2::{Digest, Sha256};
use tracing::warn;

use super::config::replace_db_name;
use super::meta::{TemplateInfo, TemplateMeta};
use super::metrics::POOL_METRICS;
use super::provisioning::{
    advisory_lock_key, connect_admin_with_retry, default_extension_versions,
    is_duplicate_database_error, load_template_meta, quote_ident, store_template_meta,
    url_with_db_name,
};
use super::reset;

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

const CREATE_TEMPLATE_DB_TIMEOUT: Duration = Duration::from_secs(10);
const APPLY_TEMPLATE_SCHEMA_TIMEOUT: Duration = Duration::from_secs(30);

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

// ── Template seed data ──────────────────────────────────────────────────────

/// Well-known test fixture data seeded into every template database.
///
/// Changing this SQL automatically invalidates the template fingerprint and
/// forces a rebuild — no manual `seed-version:N` bump needed.
const TEMPLATE_SEED_SQL: &str = "\
INSERT INTO raw.source_material_registry \
    (id, material_kind, source_identifier, status, timing_info_type) \
VALUES ('00000000-0000-7000-8000-000000000000'::uuid, 'annex', 'test-fixture-material', 'completed', 'realtime') \
ON CONFLICT (id) DO NOTHING";

// ── Fingerprinting ──────────────────────────────────────────────────────────

/// Compute a fingerprint of declarative schema source files.
///
/// Hashes both filename and content in sorted order, so any schema source
/// change produces a different fingerprint.
///
/// Used by:
/// - Sandbox: to determine if template database needs rebuilding
/// - Preflight: to detect pending schema apply work
fn schema_source_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../crate/lib/sinex-schema/src")
}

fn schema_fingerprint_sources() -> TestResult<Vec<PathBuf>> {
    schema_fingerprint_sources_in(&schema_source_root())
}

fn schema_fingerprint_sources_in(schema_src_dir: &std::path::Path) -> TestResult<Vec<PathBuf>> {
    let schema_tables_dir = schema_src_dir
        .join("schema")
        .canonicalize()
        .wrap_err_with(|| {
            format!(
                "failed to resolve schema source directory '{}'",
                schema_src_dir.join("schema").display()
            )
        })?;
    let apply_file = schema_src_dir.join("apply.rs");
    let converge_file = schema_src_dir.join("converge.rs");
    let registry_file = schema_src_dir.join("schema_registry.rs");

    let mut entries = Vec::new();
    for entry in std::fs::read_dir(&schema_tables_dir).wrap_err_with(|| {
        format!(
            "failed to enumerate schema sources in '{}'",
            schema_tables_dir.display()
        )
    })? {
        let entry = entry.wrap_err_with(|| {
            format!(
                "failed to read schema source entry from '{}'",
                schema_tables_dir.display()
            )
        })?;
        entries.push(entry.path());
    }
    entries.push(apply_file);
    entries.push(converge_file);
    entries.push(registry_file);
    entries.sort();
    Ok(entries)
}

pub fn schema_fingerprint() -> TestResult<String> {
    let entries = schema_fingerprint_sources()?;

    let mut hasher = Sha256::new();
    // Hash the seed SQL content directly — fingerprint invalidates automatically when
    // seed data changes, no manual version bump required.
    hasher.update(TEMPLATE_SEED_SQL.as_bytes());
    hasher.update(b"\n");
    for path in entries {
        if path.is_file() {
            // Hash filename first
            let name = path.file_name().and_then(|n| n.to_str()).ok_or_else(|| {
                eyre!(
                    "schema fingerprint source is not valid UTF-8: {}",
                    path.display()
                )
            })?;
            hasher.update(name.as_bytes());
            hasher.update(b":"); // Separator between name and content
            // Then hash content
            let bytes = std::fs::read(&path).wrap_err_with(|| {
                format!(
                    "failed to read schema fingerprint source '{}'",
                    path.display()
                )
            })?;
            hasher.update(bytes);
            hasher.update(b"|"); // Separator between files
        }
    }

    Ok(format!("{:x}", hasher.finalize()))
}

#[cfg(test)]
mod tests {
    // Small inline test is justified here because it verifies the private
    // fingerprint source list directly.
    use super::{
        check_template_reuse, connect_admin_with_retry, create_template_db,
        run_template_schema_apply, schema_fingerprint, schema_fingerprint_sources,
        schema_fingerprint_sources_in, store_template_meta,
    };
    use crate::sandbox::db::pool::config::replace_db_name;
    use crate::sandbox::db::pool::meta::TemplateMeta;
    use crate::sandbox::db::pool::{PoolConfig, acquire_pool_test_guard};
    use std::fs;
    use xtask::sandbox::sinex_test;

    #[sinex_test]
    async fn schema_fingerprint_includes_convergence_inputs() -> TestResult<()> {
        let sources = schema_fingerprint_sources()?;
        let file_names = sources
            .iter()
            .filter_map(|path| path.file_name().and_then(|name| name.to_str()))
            .map(str::to_owned)
            .collect::<Vec<String>>();

        assert!(file_names.iter().any(|name| name == "apply.rs"));
        assert!(file_names.iter().any(|name| name == "converge.rs"));
        assert!(file_names.iter().any(|name| name == "schema_registry.rs"));
        Ok(())
    }

    #[sinex_test]
    async fn schema_fingerprint_sources_report_unreadable_schema_root() -> TestResult<()> {
        let temp = tempfile::tempdir()?;
        let schema_root = temp.path().join("schema-root");
        fs::create_dir_all(&schema_root)?;
        fs::write(schema_root.join("schema"), "not-a-directory")?;

        let error = schema_fingerprint_sources_in(&schema_root)
            .expect_err("non-directory schema root should fail honestly");
        let message = format!("{error:#}");
        assert!(message.contains("failed to enumerate schema sources"));
        Ok(())
    }

    #[sinex_test]
    async fn schema_fingerprint_is_computable_for_workspace_sources() -> TestResult<()> {
        let fingerprint = schema_fingerprint()?;
        assert!(!fingerprint.is_empty());
        Ok(())
    }

    #[sinex_test]
    async fn template_reuse_rejects_actual_schema_drift(_ctx: TestContext) -> TestResult<()> {
        let _guard = acquire_pool_test_guard().await;
        let config = PoolConfig::default();
        let template_name = format!("sinex_test_template_drift_{}", std::process::id());
        let desired_fingerprint = Some(schema_fingerprint()?);

        let mut admin_conn = connect_admin_with_retry(&config.admin_url).await?;
        let drop_query = format!("DROP DATABASE IF EXISTS {template_name} WITH (FORCE)");
        sqlx::query(&drop_query).execute(&mut admin_conn).await?;

        create_template_db(&mut admin_conn, &template_name).await?;
        let template_admin_url = replace_db_name(&config.admin_url, &template_name);
        let template_pool_max = config.slot_max_connections.max(1).saturating_mul(2).max(4);
        let extensions =
            run_template_schema_apply(&template_name, &template_admin_url, template_pool_max)
                .await?;
        store_template_meta(
            &mut admin_conn,
            &template_name,
            &TemplateMeta {
                fingerprint: desired_fingerprint
                    .clone()
                    .expect("desired fingerprint must be present"),
                extensions,
            },
        )
        .await?;

        let template_pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(2)
            .connect(&template_admin_url)
            .await?;
        sqlx::query(
            r#"
            ALTER TABLE raw.source_material_registry
                DROP CONSTRAINT IF EXISTS source_material_registry_status_check,
                ADD CONSTRAINT source_material_registry_status_check
                CHECK (status IN ('sensing', 'completed', 'recovered_partial', 'failed'))
            "#,
        )
        .execute(&template_pool)
        .await?;
        template_pool.close().await;

        let reusable = check_template_reuse(
            &mut admin_conn,
            &config.admin_url,
            &template_name,
            &desired_fingerprint,
            true,
        )
        .await?;
        assert!(
            reusable.is_none(),
            "template with actual schema drift must be recreated instead of reused"
        );

        let drop_query = format!("DROP DATABASE IF EXISTS {template_name} WITH (FORCE)");
        sqlx::query(&drop_query).execute(&mut admin_conn).await?;
        Ok(())
    }

    #[sinex_test]
    async fn template_reuse_rejects_actual_schema_drift_on_shared_fast_path(
        _ctx: TestContext,
    ) -> TestResult<()> {
        let _guard = acquire_pool_test_guard().await;
        let config = PoolConfig::default();
        let template_name = format!("sinex_test_template_shared_drift_{}", std::process::id());
        let desired_fingerprint = Some(schema_fingerprint()?);

        let mut admin_conn = connect_admin_with_retry(&config.admin_url).await?;
        let drop_query = format!("DROP DATABASE IF EXISTS {template_name} WITH (FORCE)");
        sqlx::query(&drop_query).execute(&mut admin_conn).await?;

        create_template_db(&mut admin_conn, &template_name).await?;
        let template_admin_url = replace_db_name(&config.admin_url, &template_name);
        let template_pool_max = config.slot_max_connections.max(1).saturating_mul(2).max(4);
        let extensions =
            run_template_schema_apply(&template_name, &template_admin_url, template_pool_max)
                .await?;
        store_template_meta(
            &mut admin_conn,
            &template_name,
            &TemplateMeta {
                fingerprint: desired_fingerprint
                    .clone()
                    .expect("desired fingerprint must be present"),
                extensions,
            },
        )
        .await?;

        let template_pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(2)
            .connect(&template_admin_url)
            .await?;
        sqlx::query(
            r#"
            ALTER TABLE raw.source_material_registry
                DROP CONSTRAINT IF EXISTS source_material_registry_status_check,
                ADD CONSTRAINT source_material_registry_status_check
                CHECK (status IN ('sensing', 'completed', 'recovered_partial', 'failed'))
            "#,
        )
        .execute(&template_pool)
        .await?;
        template_pool.close().await;

        let reusable = check_template_reuse(
            &mut admin_conn,
            &config.admin_url,
            &template_name,
            &desired_fingerprint,
            false,
        )
        .await?;
        assert!(
            reusable.is_none(),
            "shared fast-path reuse must reject actual schema drift instead of trusting metadata"
        );

        let drop_query = format!("DROP DATABASE IF EXISTS {template_name} WITH (FORCE)");
        sqlx::query(&drop_query).execute(&mut admin_conn).await?;
        Ok(())
    }
}

// ── Template lifecycle ──────────────────────────────────────────────────────

pub(super) async fn harden_template_database(
    admin_conn: &mut PgConnection,
    template_name: &str,
) -> TestResult<()> {
    let quoted = quote_ident(template_name);
    let clone_lock_id = advisory_lock_key(&format!("{template_name}::clone"));
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(clone_lock_id)
        .execute(&mut *admin_conn)
        .await?;
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
    let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(clone_lock_id)
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

    let desired_fingerprint = Some(schema_fingerprint()?);

    let mut admin_conn = connect_admin_with_retry(admin_url).await?;
    let lock_key = advisory_lock_key(template_name);

    let ensure_deadline = std::time::Instant::now() + Duration::from_secs(45);
    let mut backoff = Duration::from_millis(25);
    loop {
        // Take shared lock and check for reusable template
        take_shared_advisory_lock(&mut admin_conn, lock_key).await?;

        if let Some(extensions) = check_template_reuse(
            &mut admin_conn,
            admin_url,
            template_name,
            &desired_fingerprint,
            false,
        )
        .await?
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
        if let Some(extensions) = check_template_reuse(
            &mut admin_conn,
            admin_url,
            template_name,
            &desired_fingerprint,
            true,
        )
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
/// Actual schema drift is always verified; `check_drift` additionally verifies extension defaults.
async fn check_template_reuse(
    admin_conn: &mut PgConnection,
    admin_url: &str,
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

    let meta = match load_template_meta(admin_conn, template_name).await {
        Ok(meta) => meta,
        Err(error) => {
            eprintln!(
                "♻️  Template metadata is unreadable for {template_name}; recreating template ({error:#})"
            );
            return Ok(None);
        }
    };

    let extensions = match (&desired_fingerprint, meta) {
        (Some(fp), Some(m)) if m.fingerprint == *fp && !m.extensions.is_empty() => {
            eprintln!("✅ Template database {template_name} reused (schema unchanged)");
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

    let schema_drift = probe_template_schema_drift(admin_conn, admin_url, template_name).await?;
    if let Some(reason) = schema_drift {
        eprintln!("♻️  Template schema drift detected ({reason}); recreating template");
        return Ok(None);
    }

    // Check extension version drift (e.g. TimescaleDB upgrade changes shared object paths)
    if check_drift {
        let defaults = default_extension_versions(admin_conn).await?;
        for (ext, template_ver) in &extensions {
            if let Some(default_ver) = defaults.get(ext)
                && default_ver != template_ver
            {
                eprintln!(
                    "♻️  Extension {ext} default_version changed ({template_ver} -> {default_ver}); recreating template"
                );
                return Ok(None);
            }
        }
    }

    Ok(Some(extensions))
}

async fn probe_template_schema_drift(
    admin_conn: &mut PgConnection,
    admin_url: &str,
    template_name: &str,
) -> TestResult<Option<String>> {
    let quoted = quote_ident(template_name);
    // Serialize probe/cloning against the template. The shared fast path can be entered by
    // multiple nextest processes at once, and toggling `ALLOW_CONNECTIONS` concurrently on the
    // same pg_database row can fail with `tuple concurrently updated`.
    let clone_lock_id = advisory_lock_key(&format!("{template_name}::clone"));
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(clone_lock_id)
        .execute(&mut *admin_conn)
        .await?;

    sqlx::query(&format!(
        "ALTER DATABASE {quoted} WITH ALLOW_CONNECTIONS true"
    ))
    .execute(&mut *admin_conn)
    .await?;

    let template_admin_url = replace_db_name(admin_url, template_name);
    let template_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .acquire_timeout(Duration::from_secs(5))
        .connect(&template_admin_url)
        .await?;
    let drift_result = reset::schema_mismatch_reason(&template_pool).await;
    template_pool.close().await;

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
    let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(clone_lock_id)
        .execute(&mut *admin_conn)
        .await;

    drift_result
}

/// Rebuild the template database from scratch: drop, create, apply schema, seed, optimize.
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
    sqlx::query(&drop_query).execute(&mut *admin_conn).await?;

    // Create fresh database
    create_template_db(&mut *admin_conn, template_name).await?;

    // Apply declarative schema and seed data
    let template_admin_url = replace_db_name(admin_url, template_name);
    let template_pool_max = slot_max_connections.max(1).saturating_mul(2).max(4);
    let extensions =
        run_template_schema_apply(template_name, &template_admin_url, template_pool_max)
            .await
            .map_err(|e| eyre!(format!("Template schema/setup failed: {e}")))?;

    let template_elapsed = template_start.elapsed();
    eprintln!(
        "✅ Template database created in {:.1}s",
        template_elapsed.as_secs_f64()
    );

    // Persist metadata
    if let Some(fp) = desired_fingerprint {
        let meta = TemplateMeta {
            fingerprint: fp.clone(),
            extensions: extensions.clone(),
        };
        if let Err(err) = store_template_meta(admin_conn, template_name, &meta).await {
            tracing::warn!("Failed to persist template metadata for {template_name}: {err:#}");
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
        CREATE_TEMPLATE_DB_TIMEOUT,
        sqlx::query(&create_query).execute(&mut *admin_conn),
    )
    .await
    {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(err)) => {
            if is_duplicate_database_error(&err) {
                eprintln!(
                    "  Template database {template_name} already exists; reusing existing instance"
                );
                Ok(())
            } else {
                Err(eyre!(format!("Create database failed: {err}")))
            }
        }
        Err(_) => Err(eyre!(format!(
            "Create database timed out after {:?} while creating template {template_name}",
            CREATE_TEMPLATE_DB_TIMEOUT
        ))),
    }
}

/// Connect to the template database, apply schema, install extensions, seed data.
async fn run_template_schema_apply(
    template_name: &str,
    template_admin_url: &str,
    template_pool_max: u32,
) -> TestResult<HashMap<String, String>> {
    let template_schema_url = if let Ok(super_url) = std::env::var("DATABASE_URL_SUPERUSER") {
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
        .connect(&template_schema_url)
        .await?;

    apply_test_session_optimizations(&template_pool).await?;

    eprintln!("  📋 Applying declarative schema on template database...");
    check_required_extensions(&template_pool)
        .await
        .map_err(|e| {
            eprintln!("❌ Missing required PostgreSQL extensions: {e}");
            eprintln!("   Check NixOS PostgreSQL configuration and required extensions.");
            e
        })?;

    let apply_result = tokio::time::timeout(
        APPLY_TEMPLATE_SCHEMA_TIMEOUT,
        sinex_db::apply_schema_for_url(&template_schema_url),
    )
    .await
    .map_err(|_| {
        eyre!(format!(
            "Schema apply timed out after {:?} for template database {template_name}. \
             Check for missing PostgreSQL extensions, exhausted Timescale background workers, \
             or a stuck declarative DDL statement.",
            APPLY_TEMPLATE_SCHEMA_TIMEOUT
        ))
    })
    .and_then(|res| res.map_err(|e| eyre!(format!("Schema apply failed: {e}"))));
    apply_result?;

    // Sanity-check: verify the schema was applied completely (≥ 8 tables in core.*).
    // If a previous build was killed mid-apply the fingerprint may have been stored
    // but the schema is still incomplete; this catches that case at rebuild time.
    let core_table_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM information_schema.tables \
         WHERE table_schema = 'core' AND table_type = 'BASE TABLE'",
    )
    .fetch_one(&template_pool)
    .await
    .unwrap_or(0);
    if core_table_count < 8 {
        return Err(eyre!(
            "Schema apply incomplete: only {core_table_count} tables in core schema \
             (expected >= 8). Template will be rebuilt on next attempt."
        ));
    }

    grant_template_permissions(&template_pool).await?;

    // Seed well-known test fixture data for FK constraints
    sqlx::query(TEMPLATE_SEED_SQL)
        .execute(&template_pool)
        .await?;

    optimize_template_for_tests(&template_pool).await?;
    let extensions = collect_extension_versions(&template_pool).await?;
    template_pool.close().await;
    Ok(extensions)
}

/// Grant schema permissions to the non-superuser role in the template database.
async fn grant_template_permissions(template_pool: &DbPool) -> TestResult<()> {
    let Ok(Some(granter)) = crate::sandbox::db::permissions::PermissionGranter::from_env() else {
        return Ok(());
    };
    let Some(username) = std::env::var("DATABASE_URL_APP").ok().and_then(|url| {
        url.split("://")
            .nth(1)
            .and_then(|s| s.split('@').next().map(std::string::ToString::to_string))
    }) else {
        return Ok(());
    };

    eprintln!("  🔑 Granting schema permissions to user '{username}' in template database");
    for schema in crate::sandbox::db::permissions::granted_schema_names() {
        granter
            .grant_schema_access(template_pool, schema)
            .await
            .wrap_err_with(|| {
                format!("failed to grant permissions on schema {schema} in template database")
            })?;
    }

    Ok(())
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
    let optional_extensions: [(&str, &str); 0] = [];

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
        ("pg_jsonschema", "pg_jsonschema for JSON validation"),
        ("vector", "pgvector for vector similarity search"),
        ("pg_trgm", "trigram indexes used by schema"),
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
        r"SELECT extname, extversion FROM pg_extension WHERE extname IN ('timescaledb','uuid','pg_jsonschema','vector')"
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
        if let Err(e) = sqlx::query("ALTER TABLE core.events SET (fillfactor = 100)")
            .execute(pool)
            .await
        {
            eprintln!("⚠️  Could not set fillfactor on core.events: {e:#}");
        }

        // Clean up any test data that might have snuck in
        // Set operation_id for RLS policies
        if let Err(e) =
            sqlx::query("SELECT set_config('sinex.operation_id', 'template-setup', false)")
                .execute(pool)
                .await
        {
            eprintln!("⚠️  Could not set operation_id: {e}");
        }

        if let Err(e) = sqlx::query("DELETE FROM core.events WHERE source LIKE 'test_%'")
            .execute(pool)
            .await
        {
            eprintln!("⚠️  Could not clean test data: {e:#}");
        }

        // Reset operation_id
        if let Err(error) = sqlx::query("RESET sinex.operation_id").execute(pool).await {
            warn!(error = %error, "Could not reset sinex.operation_id after template optimization");
        }

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
