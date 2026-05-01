//! Database provisioning — creation, cloning, permissions, admin connections.

use crate::sandbox::prelude::*;
use serde::de::DeserializeOwned;
use sinex_primitives::temporal::Timestamp;
use sqlx::Row;
use sqlx::pool::PoolConnection;
use sqlx::postgres::PgConnection;
use sqlx::{Connection, Postgres};
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::io::ErrorKind;
use std::time::Duration;
use url::Url;

use super::config::SLOT_MAX_CONNECTIONS;
use super::meta::{PoolMeta, TemplateMeta};
use super::template::schema_fingerprint;

// ── Outcome type ────────────────────────────────────────────────────────────

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum CreateDatabaseOutcome {
    Created,
    AlreadyExists,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum EnsurePoolDatabaseOutcome {
    Ensured,
    Deferred,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum PoolCleanVerification {
    TrustedTemplateClone,
    RequireSchemaVerification,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SlotLifecycleLockMode {
    Wait,
    SkipIfLocked,
}

// ── Existence checks ────────────────────────────────────────────────────────

pub(super) async fn database_exists(
    conn: &mut PoolConnection<Postgres>,
    name: &str,
) -> TestResult<bool> {
    let exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)")
            .bind(name)
            .fetch_one(conn.as_mut())
            .await?;
    Ok(exists)
}

pub(super) async fn database_exists_admin(conn: &mut PgConnection, name: &str) -> TestResult<bool> {
    let exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)")
            .bind(name)
            .fetch_one(&mut *conn)
            .await?;
    Ok(exists)
}

// ── Quoting / error detection ───────────────────────────────────────────────

const SQLSTATE_UNDEFINED_DATABASE: &str = "3D000";
const SQLSTATE_DUPLICATE_DATABASE: &str = "42P04";
const SQLSTATE_UNIQUE_VIOLATION: &str = "23505";
const SQLSTATE_TOO_MANY_CONNECTIONS: &str = "53300";
const SQLSTATE_UNDEFINED_FILE: &str = "58P01";

const RETRYABLE_CONNECTION_SQLSTATES: &[&str] = &[
    "08000", // connection_exception
    "08001", // sqlclient_unable_to_establish_sqlconnection
    "08003", // connection_does_not_exist
    "08004", // sqlserver_rejected_establishment_of_sqlconnection
    "08006", // connection_failure
    "57P01", // admin_shutdown
    "57P02", // crash_shutdown
    "57P03", // cannot_connect_now
];

fn is_missing_database_code(code: Option<&str>) -> bool {
    code.is_some_and(|value| value == SQLSTATE_UNDEFINED_DATABASE)
}

fn is_duplicate_database_code(code: Option<&str>) -> bool {
    code.is_some_and(|value| {
        value == SQLSTATE_DUPLICATE_DATABASE || value == SQLSTATE_UNIQUE_VIOLATION
    })
}

fn is_too_many_clients_code(code: Option<&str>) -> bool {
    code.is_some_and(|value| value == SQLSTATE_TOO_MANY_CONNECTIONS)
}

pub(super) fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

fn postgres_error_code(err: &sqlx::Error) -> Option<String> {
    match err {
        sqlx::Error::Database(db_err) => db_err.code().map(std::borrow::Cow::into_owned),
        _ => None,
    }
}

pub(super) fn is_missing_database_error(err: &sqlx::Error) -> bool {
    is_missing_database_code(postgres_error_code(err).as_deref())
}

pub(super) fn is_duplicate_database_error(err: &sqlx::Error) -> bool {
    is_duplicate_database_code(postgres_error_code(err).as_deref())
}

fn is_too_many_clients_error(err: &sqlx::Error) -> bool {
    is_too_many_clients_code(postgres_error_code(err).as_deref())
}

fn is_retryable_io_kind(kind: ErrorKind) -> bool {
    matches!(
        kind,
        ErrorKind::BrokenPipe
            | ErrorKind::ConnectionAborted
            | ErrorKind::ConnectionReset
            | ErrorKind::NotConnected
            | ErrorKind::TimedOut
            | ErrorKind::UnexpectedEof
            | ErrorKind::WouldBlock
    )
}

pub(super) fn is_retryable_connection_error(err: &sqlx::Error) -> bool {
    if err
        .to_string()
        .contains("A Tokio 1.x context was found, but it is being shutdown")
    {
        return true;
    }

    if err.as_database_error().is_some_and(|db_err| {
        db_err
            .message()
            .contains("is not currently accepting connections")
    }) {
        return true;
    }

    if let Some(code) = postgres_error_code(err) {
        return RETRYABLE_CONNECTION_SQLSTATES.contains(&code.as_str());
    }

    if err
        .to_string()
        .contains("is not currently accepting connections")
    {
        return true;
    }

    match err {
        sqlx::Error::Io(io_err) => is_retryable_io_kind(io_err.kind()),
        sqlx::Error::PoolTimedOut | sqlx::Error::PoolClosed => true,
        _ => false,
    }
}

pub(super) fn is_retryable_connection_report(report: &color_eyre::Report) -> bool {
    if report
        .to_string()
        .contains("A Tokio 1.x context was found, but it is being shutdown")
    {
        return true;
    }

    for cause in report.chain() {
        if let Some(sqlx_err) = cause.downcast_ref::<sqlx::Error>()
            && is_retryable_connection_error(sqlx_err)
        {
            return true;
        }
        if let Some(io_err) = cause.downcast_ref::<std::io::Error>()
            && is_retryable_io_kind(io_err.kind())
        {
            return true;
        }
    }
    false
}

pub(super) fn is_timescaledb_missing_library_report(report: &color_eyre::Report) -> bool {
    report.chain().any(|cause| {
        cause
            .downcast_ref::<sqlx::Error>()
            .is_some_and(is_timescaledb_missing_library_error)
    })
}

pub(super) fn is_timescaledb_missing_library_error(err: &sqlx::Error) -> bool {
    match err {
        sqlx::Error::Database(db_err) => db_err
            .code()
            .as_ref()
            .is_some_and(|code| code.as_ref() == SQLSTATE_UNDEFINED_FILE),
        _ => false,
    }
}

// ── Drop / wait helpers ─────────────────────────────────────────────────────

pub(super) async fn drop_database_if_exists(
    conn: &mut PoolConnection<Postgres>,
    name: &str,
) -> TestResult<()> {
    let quoted = quote_ident(name);
    sqlx::query(&format!("DROP DATABASE IF EXISTS {quoted} WITH (FORCE)"))
        .execute(conn.as_mut())
        .await
        .map_err(|err| eyre!("Failed to drop database {name} with FORCE: {err}"))?;

    Ok(())
}

pub(super) async fn drop_database_if_exists_admin(
    conn: &mut PgConnection,
    name: &str,
) -> TestResult<()> {
    let quoted = quote_ident(name);
    sqlx::query(&format!("DROP DATABASE IF EXISTS {quoted} WITH (FORCE)"))
        .execute(&mut *conn)
        .await
        .map_err(|err| eyre!("Failed to drop database {name} with FORCE: {err}"))?;

    Ok(())
}

pub(super) async fn wait_for_database_absence(
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

    Err(eyre!("Database {name} still present after drop attempts"))
}

pub(super) async fn wait_for_database_absence_admin(
    conn: &mut PgConnection,
    name: &str,
) -> TestResult<()> {
    const MAX_ATTEMPTS: usize = 20;
    for attempt in 0..MAX_ATTEMPTS {
        if !database_exists_admin(conn, name).await? {
            return Ok(());
        }

        let delay = Duration::from_millis(50 + (attempt as u64 * 10));
        tokio::time::sleep(delay).await;
    }

    Err(eyre!("Database {name} still present after drop attempts"))
}

// ── Permissions ─────────────────────────────────────────────────────────────

/// Grant schema permissions to app user on a newly created pool database.
///
/// This uses the centralized permissions module which automatically grants on ALL
/// schemas (including `public`), eliminating hardcoded schema lists.
pub(super) async fn grant_pool_database_permissions(db_name: &str) -> TestResult<()> {
    crate::sandbox::db::permissions::grant_pool_database_permissions(db_name).await
}

pub(super) async fn grant_pool_database_permissions_checked(db_name: &str) -> TestResult<()> {
    grant_pool_database_permissions(db_name)
        .await
        .wrap_err_with(|| format!("failed to grant pool database permissions for {db_name}"))
}

fn slot_lifecycle_lock_key(db_name: &str) -> i64 {
    advisory_lock_key(&format!("{db_name}::lifecycle"))
}

async fn acquire_slot_lifecycle_lock(
    admin_conn: &mut PgConnection,
    db_name: &str,
    mode: SlotLifecycleLockMode,
) -> TestResult<Option<i64>> {
    let lock_id = slot_lifecycle_lock_key(db_name);
    match mode {
        SlotLifecycleLockMode::SkipIfLocked => {
            let got_lock: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
                .bind(lock_id)
                .fetch_one(&mut *admin_conn)
                .await?;
            Ok(got_lock.then_some(lock_id))
        }
        SlotLifecycleLockMode::Wait => {
            let deadline = std::time::Instant::now() + Duration::from_mins(1);
            let mut backoff = Duration::from_millis(25);
            loop {
                let got_lock: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
                    .bind(lock_id)
                    .fetch_one(&mut *admin_conn)
                    .await?;
                if got_lock {
                    return Ok(Some(lock_id));
                }
                if std::time::Instant::now() >= deadline {
                    return Err(eyre!(
                        "Could not acquire slot lifecycle lock for {db_name} within 60s. \
                         Another process may be provisioning or recreating it."
                    ));
                }
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_millis(250));
            }
        }
    }
}

async fn release_slot_lifecycle_lock(
    admin_conn: &mut PgConnection,
    lock_id: i64,
) -> TestResult<()> {
    sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(lock_id)
        .execute(&mut *admin_conn)
        .await?;
    Ok(())
}

async fn recreate_pool_database_locked(
    admin_conn: &mut PgConnection,
    db_name: &str,
    base_url: &str,
    template_name: &str,
    template_extensions: &HashMap<String, String>,
) -> TestResult<()> {
    drop_database_if_exists_admin(admin_conn, db_name).await?;
    wait_for_database_absence_admin(admin_conn, db_name).await?;

    let db_url = url_with_db_name(base_url, db_name)?;
    let verification =
        match create_database_from_template_admin(admin_conn, db_name, template_name).await? {
            CreateDatabaseOutcome::Created => PoolCleanVerification::TrustedTemplateClone,
            CreateDatabaseOutcome::AlreadyExists => {
                grant_pool_database_permissions_checked(db_name).await?;
                converge_pool_database_schema(db_name, &db_url).await?;
                PoolCleanVerification::RequireSchemaVerification
            }
        };
    mark_pool_database_clean(
        admin_conn,
        db_name,
        &db_url,
        template_extensions,
        verification,
    )
    .await?;
    Ok(())
}

// ── Create from template ────────────────────────────────────────────────────

pub(super) async fn create_database_from_template(
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
            grant_pool_database_permissions_checked(name).await?;
            Ok(CreateDatabaseOutcome::Created)
        }
        Err(err) => {
            if is_duplicate_database_error(&err) {
                Ok(CreateDatabaseOutcome::AlreadyExists)
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

pub(super) async fn create_database_from_template_admin(
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
            grant_pool_database_permissions_checked(name).await?;
            Ok(CreateDatabaseOutcome::Created)
        }
        Err(err) => {
            if is_duplicate_database_error(&err) {
                Ok(CreateDatabaseOutcome::AlreadyExists)
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

// ── Lazy provisioning / recreation ──────────────────────────────────────────

async fn ensure_pool_database_exists_inner(
    db_name: &str,
    slot_url: &str,
    lock_mode: SlotLifecycleLockMode,
) -> TestResult<EnsurePoolDatabaseOutcome> {
    let admin_url = admin_url_from_slot(slot_url)?;
    let base_url = base_url_from_slot(slot_url)?;
    let db_url = url_with_db_name(&base_url, db_name)?;
    let start = std::time::Instant::now();

    // Canonical lock order: template first, then lifecycle.
    // Both recreate_pool_database and this function must acquire locks in the same
    // order to prevent deadlocks when concurrent callers race on the same slot.
    let template_guard = super::template::ensure_template_database_for_key(
        &admin_url,
        &base_url,
        SLOT_MAX_CONNECTIONS,
        db_name,
    )
    .await?;
    let template_name = template_guard.info.name.clone();
    let template_extensions = template_guard.info.extensions.clone();

    let mut lifecycle_admin_conn = connect_admin_with_retry(&admin_url).await?;
    let Some(lock_id) =
        acquire_slot_lifecycle_lock(&mut lifecycle_admin_conn, db_name, lock_mode).await?
    else {
        return Ok(EnsurePoolDatabaseOutcome::Deferred);
    };

    let provision_result: TestResult<EnsurePoolDatabaseOutcome> = async {
        let mut created_from_fresh_template_clone = false;
        if !database_exists_admin(&mut lifecycle_admin_conn, db_name).await? {
            match create_database_from_template_admin(
                &mut lifecycle_admin_conn,
                db_name,
                &template_name,
            )
            .await?
            {
                CreateDatabaseOutcome::Created => {
                    created_from_fresh_template_clone = true;
                    eprintln!(
                        "  Created missing pool database: {db_name} (clone: {:?})",
                        start.elapsed()
                    );
                }
                CreateDatabaseOutcome::AlreadyExists => {}
            }
        }
        let verification = if created_from_fresh_template_clone {
            // Even fresh template clones must pass schema verification: the template
            // trust stamp could be stale (e.g. after a toolchain upgrade that changed
            // DefaultHasher output). Verification is cheap compared to the cost of
            // silently serving a drifted schema to tests.
            PoolCleanVerification::RequireSchemaVerification
        } else {
            // Existing or raced slot databases may still need grants and convergence.
            grant_pool_database_permissions_checked(db_name).await?;
            converge_pool_database_schema(db_name, &db_url).await?;
            PoolCleanVerification::RequireSchemaVerification
        };
        mark_pool_database_clean(
            &mut lifecycle_admin_conn,
            db_name,
            &db_url,
            &template_extensions,
            verification,
        )
        .await?;
        Ok(EnsurePoolDatabaseOutcome::Ensured)
    }
    .await;

    let provision_result = match provision_result {
        Ok(outcome) => Ok(outcome),
        Err(provision_error) => {
            eprintln!("  Slot provisioning failed for {db_name}; recreating slot from template");
            recreate_pool_database_locked(
                &mut lifecycle_admin_conn,
                db_name,
                &base_url,
                &template_name,
                &template_extensions,
            )
            .await
            .map_err(|recreate_err| {
                eyre!(
                    "slot provisioning failed for {db_name}: {provision_error}; recreate failed: {recreate_err}"
                )
            })?;
            Ok(EnsurePoolDatabaseOutcome::Ensured)
        }
    };

    let unlock_result = release_slot_lifecycle_lock(&mut lifecycle_admin_conn, lock_id).await;
    let release_result = template_guard.release().await;
    match provision_result {
        Ok(outcome) => {
            unlock_result.wrap_err_with(|| {
                format!("failed to release slot lifecycle lock after provisioning {db_name}")
            })?;
            release_result.wrap_err_with(|| {
                format!("failed to release template guard after provisioning {db_name}")
            })?;
            Ok(outcome)
        }
        Err(provision_error) => {
            unlock_result.wrap_err_with(|| {
                format!(
                    "failed to release slot lifecycle lock after provisioning {db_name}: {provision_error}"
                )
            })?;
            release_result.wrap_err_with(|| {
                format!(
                    "failed to release template guard after provisioning {db_name}: {provision_error}"
                )
            })?;
            Err(provision_error)
        }
    }
}

pub(super) async fn try_ensure_pool_database_exists(
    db_name: &str,
    slot_url: &str,
) -> TestResult<EnsurePoolDatabaseOutcome> {
    ensure_pool_database_exists_inner(db_name, slot_url, SlotLifecycleLockMode::SkipIfLocked).await
}

pub(super) async fn ensure_pool_database_exists(db_name: &str, slot_url: &str) -> TestResult<()> {
    match ensure_pool_database_exists_inner(db_name, slot_url, SlotLifecycleLockMode::Wait).await? {
        EnsurePoolDatabaseOutcome::Ensured => Ok(()),
        EnsurePoolDatabaseOutcome::Deferred => Ok(()),
    }
}

pub(super) async fn recreate_pool_database(db_name: &str, slot_url: &str) -> TestResult<()> {
    let admin_url = admin_url_from_slot(slot_url)?;
    let base_url = base_url_from_slot(slot_url)?;
    let mut template_guard = super::template::ensure_template_database_for_key(
        &admin_url,
        &base_url,
        SLOT_MAX_CONNECTIONS,
        db_name,
    )
    .await?;
    let template_name = template_guard.info.name.clone();
    let template_extensions = template_guard.info.extensions.clone();
    let lock_id = acquire_slot_lifecycle_lock(
        &mut template_guard.admin_conn,
        db_name,
        SlotLifecycleLockMode::Wait,
    )
    .await?
    .expect("wait mode always returns a lifecycle lock");

    let recreate_result = recreate_pool_database_locked(
        &mut template_guard.admin_conn,
        db_name,
        &base_url,
        &template_name,
        &template_extensions,
    )
    .await;

    let unlock_result = release_slot_lifecycle_lock(&mut template_guard.admin_conn, lock_id).await;
    let release_result = template_guard.release().await;
    match recreate_result {
        Ok(()) => {
            unlock_result.wrap_err_with(|| {
                format!("failed to release slot lifecycle lock after recreating {db_name}")
            })?;
            release_result.wrap_err_with(|| {
                format!("failed to release template guard after recreating {db_name}")
            })?;
            Ok(())
        }
        Err(recreate_error) => {
            unlock_result.wrap_err_with(|| {
                format!(
                    "failed to release slot lifecycle lock after recreating {db_name}: {recreate_error}"
                )
            })?;
            release_result.wrap_err_with(|| {
                format!(
                    "failed to release template guard after recreating {db_name}: {recreate_error}"
                )
            })?;
            Err(recreate_error)
        }
    }
}

pub(super) async fn reconcile_existing_pool_database(
    admin_url: &str,
    db_name: &str,
    db_url: &str,
    extensions: &HashMap<String, String>,
) -> TestResult<()> {
    grant_pool_database_permissions_checked(db_name).await?;
    converge_pool_database_schema(db_name, db_url).await?;

    let mut admin_conn = connect_admin_with_retry(admin_url).await?;
    mark_pool_database_clean(
        &mut admin_conn,
        db_name,
        db_url,
        extensions,
        PoolCleanVerification::RequireSchemaVerification,
    )
    .await
}

// ── Meta load / store ───────────────────────────────────────────────────────

pub(super) async fn load_template_meta(
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

    parse_database_meta_comment("template", template_name, &comment).map(Some)
}

pub(super) async fn load_pool_meta(
    conn: &mut PgConnection,
    db_name: &str,
) -> TestResult<Option<PoolMeta>> {
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

    parse_database_meta_comment("pool", db_name, &comment).map(Some)
}

fn parse_database_meta_comment<T>(kind: &str, db_name: &str, comment: &str) -> TestResult<T>
where
    T: DeserializeOwned,
{
    serde_json::from_str(comment).map_err(|error| {
        eyre!("failed to parse {kind} database metadata comment for {db_name}: {error}")
    })
}

pub(super) async fn default_extension_versions(
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
         WHERE name IN ('timescaledb','pg_jsonschema','vector','pg_trgm')",
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

pub(super) async fn store_template_meta(
    conn: &mut PgConnection,
    template_name: &str,
    meta: &TemplateMeta,
) -> TestResult<()> {
    let payload =
        serde_json::to_string(meta).map_err(|e| eyre!("Failed to serialize template meta: {e}"))?;

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

pub(super) async fn store_pool_meta(
    conn: &mut PgConnection,
    db_name: &str,
    meta: &PoolMeta,
) -> TestResult<()> {
    let payload =
        serde_json::to_string(meta).map_err(|e| eyre!("Failed to serialize pool meta: {e}"))?;

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

pub(super) async fn store_pool_meta_checked(
    conn: &mut PgConnection,
    db_name: &str,
    meta: &PoolMeta,
) -> TestResult<()> {
    store_pool_meta(conn, db_name, meta)
        .await
        .wrap_err_with(|| format!("failed to persist pool metadata for {db_name}"))
}

pub(super) async fn converge_pool_database_schema(db_name: &str, db_url: &str) -> TestResult<()> {
    sinex_db::apply_schema_for_url(db_url)
        .await
        .map_err(|apply_err| eyre!("schema apply failed for {db_name}: {apply_err}"))
}

async fn verify_pool_database_schema_clean(db_name: &str, db_url: &str) -> TestResult<()> {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .acquire_timeout(Duration::from_secs(5))
        .connect(db_url)
        .await
        .map_err(|error| {
            eyre!("failed to connect to {db_name} for schema verification: {error}")
        })?;

    let drift = super::reset::schema_mismatch_reason(&pool).await;
    pool.close().await;

    match drift? {
        Some(reason) => Err(eyre!(
            "pool database {db_name} still has schema drift after convergence: {reason}"
        )),
        None => Ok(()),
    }
}

pub(super) async fn mark_pool_database_clean(
    conn: &mut PgConnection,
    db_name: &str,
    db_url: &str,
    extensions: &HashMap<String, String>,
    verification: PoolCleanVerification,
) -> TestResult<()> {
    super::reset::ensure_pool_db_invariants(db_url).await?;
    if verification == PoolCleanVerification::RequireSchemaVerification {
        verify_pool_database_schema_clean(db_name, db_url).await?;
    }
    let meta = PoolMeta {
        fingerprint: Some(schema_fingerprint()?),
        extensions: extensions.clone(),
        dirty: false,
        updated_at_rfc3339: Timestamp::now().format_rfc3339(),
        last_error: None,
    };
    store_pool_meta_checked(conn, db_name, &meta).await
}

// ── Admin connection helpers ────────────────────────────────────────────────

pub(super) async fn connect_admin_with_retry(admin_url: &str) -> TestResult<PgConnection> {
    let mut delay = Duration::from_millis(100);
    let mut last_error: Option<sqlx::Error> = None;
    const MAX_ATTEMPTS: usize = 10;

    for attempt in 0..MAX_ATTEMPTS {
        match tokio::time::timeout(Duration::from_secs(5), PgConnection::connect(admin_url)).await {
            Ok(Ok(conn)) => return Ok(conn),
            Ok(Err(err)) => {
                if !is_too_many_clients_error(&err) {
                    return Err(eyre!(
                        "Admin connection failed: {err}. Ensure the local PostgreSQL instance is running and accessible (try `just db-setup`, `pg_ctl start`, or set DATABASE_URL to a reachable server)."
                    ));
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

    Err(eyre!(
        "Admin connection failed after retries: {}. Ensure PostgreSQL is running and reachable for tests.",
        last_error.map_or_else(|| "unknown error".to_string(), |e| e.to_string())
    ))
}

fn effective_connection_budget(max_connections: i64, reserved: i64) -> TestResult<u32> {
    if max_connections <= 0 {
        return Err(eyre!(
            "PostgreSQL reported invalid max_connections value ({max_connections})"
        ));
    }
    if reserved < 0 {
        return Err(eyre!(
            "PostgreSQL reported invalid superuser_reserved_connections value ({reserved})"
        ));
    }

    // Leave headroom for template provisioning, cleanup tasks, and ad-hoc diagnostics.
    const SAFETY_MARGIN: i64 = 16;

    let effective = max_connections
        .saturating_sub(reserved)
        .saturating_sub(SAFETY_MARGIN);
    if effective <= 0 {
        return Err(eyre!(
            "PostgreSQL effective connection budget is non-positive after reserving headroom \
             (max_connections={max_connections}, superuser_reserved_connections={reserved}, \
             safety_margin={SAFETY_MARGIN})"
        ));
    }

    Ok(effective as u32)
}

async fn read_connection_setting(conn: &mut PgConnection, name: &str) -> TestResult<i64> {
    sqlx::query_scalar("SELECT current_setting($1)::bigint")
        .bind(name)
        .fetch_one(&mut *conn)
        .await
        .wrap_err_with(|| format!("failed to read PostgreSQL setting {name}"))
}

pub(super) async fn detect_connection_budget(admin_url: &str) -> TestResult<u32> {
    let mut conn = connect_admin_with_retry(admin_url)
        .await
        .wrap_err("failed to connect while detecting PostgreSQL connection budget")?;
    let max_connections = read_connection_setting(&mut conn, "max_connections").await?;
    let reserved = read_connection_setting(&mut conn, "superuser_reserved_connections").await?;
    effective_connection_budget(max_connections, reserved)
}

// ── URL manipulation ────────────────────────────────────────────────────────

pub(super) fn admin_url_from_slot(slot_url: &str) -> TestResult<String> {
    let mut url = Url::parse(slot_url).map_err(|e| eyre!("Invalid slot url: {e}"))?;
    url.set_path("/postgres");
    Ok(url.to_string())
}

pub(super) fn base_url_from_slot(slot_url: &str) -> TestResult<String> {
    let mut url = Url::parse(slot_url).map_err(|e| eyre!("Invalid slot url: {e}"))?;
    url.set_path("/sinex_dev");
    Ok(url.to_string())
}

pub(super) fn url_with_db_name(raw_url: &str, db_name: &str) -> TestResult<String> {
    let mut url = Url::parse(raw_url).map_err(|e| eyre!("Invalid database url: {e}"))?;
    url.set_path(&format!("/{db_name}"));
    Ok(url.to_string())
}

// ── Advisory lock key ───────────────────────────────────────────────────────

pub(super) fn advisory_lock_key(name: &str) -> i64 {
    let mut hasher = DefaultHasher::new();
    name.hash(&mut hasher);
    // mask to positive i64 to match PostgreSQL advisory lock expectations
    (hasher.finish() & 0x7FFF_FFFF_FFFF_FFFF) as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::db::pool::config::PoolConfig;
    use crate::sandbox::db::pool::reset;
    use crate::sandbox::sinex_test;

    #[sinex_test]
    async fn test_sqlstate_classifiers() -> TestResult<()> {
        assert!(is_missing_database_code(Some("3D000")));
        assert!(!is_missing_database_code(Some("08006")));
        assert!(is_duplicate_database_code(Some("42P04")));
        assert!(is_duplicate_database_code(Some("23505")));
        assert!(!is_duplicate_database_code(Some("3D000")));
        assert!(is_too_many_clients_code(Some("53300")));
        assert!(!is_too_many_clients_code(Some("08003")));
        Ok(())
    }

    #[sinex_test]
    async fn test_quote_ident_escapes_embedded_quotes() -> TestResult<()> {
        assert_eq!(quote_ident("sinex_test"), "\"sinex_test\"");
        assert_eq!(quote_ident("sinex\"test"), "\"sinex\"\"test\"");
        Ok(())
    }

    #[sinex_test]
    async fn test_retryable_sqlstate_set() -> TestResult<()> {
        assert!(RETRYABLE_CONNECTION_SQLSTATES.contains(&"08006"));
        assert!(RETRYABLE_CONNECTION_SQLSTATES.contains(&"57P01"));
        assert!(!RETRYABLE_CONNECTION_SQLSTATES.contains(&"23505"));
        Ok(())
    }

    #[sinex_test]
    async fn test_effective_connection_budget_reserves_headroom() -> TestResult<()> {
        assert_eq!(effective_connection_budget(100, 3)?, 81);
        Ok(())
    }

    #[sinex_test]
    async fn test_effective_connection_budget_rejects_non_positive_budget() -> TestResult<()> {
        let err = effective_connection_budget(16, 3).expect_err("budget should be rejected");
        assert!(
            err.to_string().contains("non-positive"),
            "unexpected error: {err:#}"
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_detect_connection_budget_surfaces_connect_failures() -> TestResult<()> {
        let err = detect_connection_budget("definitely-not-a-postgres-url")
            .await
            .expect_err("invalid admin url should fail");
        let rendered = format!("{err:#}");
        assert!(
            rendered.contains("failed to connect while detecting PostgreSQL connection budget"),
            "missing budget detection context: {rendered}"
        );
        assert!(
            rendered.contains("Admin connection failed"),
            "missing admin connection context: {rendered}"
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_parse_template_meta_comment_rejects_invalid_json() -> TestResult<()> {
        let err = parse_database_meta_comment::<TemplateMeta>(
            "template",
            "sinex_test_template",
            "{ definitely not valid json",
        )
        .expect_err("invalid template metadata must not be treated as missing");
        assert!(
            err.to_string()
                .contains("failed to parse template database metadata comment"),
            "unexpected error: {err:#}"
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_parse_pool_meta_comment_rejects_invalid_json() -> TestResult<()> {
        let err = parse_database_meta_comment::<PoolMeta>(
            "pool",
            "sinex_test_pool_0",
            "{ definitely not valid json",
        )
        .expect_err("invalid pool metadata must not be treated as missing");
        assert!(
            err.to_string()
                .contains("failed to parse pool database metadata comment"),
            "unexpected error: {err:#}"
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_try_ensure_pool_database_exists_defers_when_lifecycle_lock_held() -> TestResult<()>
    {
        let config = PoolConfig::default();
        let db_name = format!("sinex_test_pool_deferred_{}", std::process::id());
        let slot_url = url_with_db_name(&config.base_url, &db_name)?;
        let mut admin_conn = connect_admin_with_retry(&config.admin_url).await?;

        drop_database_if_exists_admin(&mut admin_conn, &db_name).await?;
        wait_for_database_absence_admin(&mut admin_conn, &db_name).await?;

        let lock_id = slot_lifecycle_lock_key(&db_name);
        sqlx::query("SELECT pg_advisory_lock($1)")
            .bind(lock_id)
            .execute(&mut admin_conn)
            .await?;

        let start = std::time::Instant::now();
        let outcome = try_ensure_pool_database_exists(&db_name, &slot_url).await?;
        let elapsed = start.elapsed();

        let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
            .bind(lock_id)
            .execute(&mut admin_conn)
            .await;

        assert_eq!(outcome, EnsurePoolDatabaseOutcome::Deferred);
        assert!(
            elapsed < Duration::from_secs(2),
            "skip-locked provisioning should defer quickly, took {elapsed:?}"
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_recreate_pool_database_converges_schema_before_marking_clean() -> TestResult<()> {
        let config = PoolConfig::default();
        let db_name = format!("sinex_test_pool_recreate_{}", std::process::id());
        let slot_url = url_with_db_name(&config.base_url, &db_name)?;
        let mut admin_conn = connect_admin_with_retry(&config.admin_url).await?;

        drop_database_if_exists_admin(&mut admin_conn, &db_name).await?;
        wait_for_database_absence_admin(&mut admin_conn, &db_name).await?;

        recreate_pool_database(&db_name, &slot_url).await?;

        let slot_pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(2)
            .connect(&slot_url)
            .await?;
        sqlx::query(
            r"
            ALTER TABLE raw.source_material_registry
                DROP CONSTRAINT IF EXISTS source_material_registry_status_check,
                ADD CONSTRAINT source_material_registry_status_check
                CHECK (status IN ('sensing', 'completed', 'recovered_partial', 'failed'))
            ",
        )
        .execute(&slot_pool)
        .await?;

        let drift_before = reset::schema_mismatch_reason(&slot_pool).await?;
        assert!(
            drift_before
                .as_deref()
                .is_some_and(|reason| reason.contains("source_material_registry_status_check")),
            "expected stale status constraint drift, got {drift_before:?}"
        );
        slot_pool.close().await;

        recreate_pool_database(&db_name, &slot_url).await?;

        let repaired_pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(2)
            .connect(&slot_url)
            .await?;
        let drift_after = reset::schema_mismatch_reason(&repaired_pool).await?;
        assert_eq!(
            drift_after, None,
            "recreated pool database should be converged before metadata is marked clean"
        );
        repaired_pool.close().await;

        let pool_meta = load_pool_meta(&mut admin_conn, &db_name).await?;
        assert!(
            pool_meta.is_some(),
            "recreated pool database should persist clean metadata"
        );
        let pool_meta = pool_meta.expect("checked above");
        assert_eq!(pool_meta.fingerprint, Some(schema_fingerprint()?));
        assert!(
            !pool_meta.dirty,
            "recreated pool database must be marked clean"
        );

        drop_database_if_exists_admin(&mut admin_conn, &db_name).await?;
        wait_for_database_absence_admin(&mut admin_conn, &db_name).await?;
        Ok(())
    }

    #[sinex_test]
    async fn test_reconcile_existing_pool_database_refreshes_stale_metadata() -> TestResult<()> {
        let config = PoolConfig::default();
        let db_name = format!("sinex_test_pool_reconcile_{}", std::process::id());
        let slot_url = url_with_db_name(&config.base_url, &db_name)?;
        let mut admin_conn = connect_admin_with_retry(&config.admin_url).await?;

        drop_database_if_exists_admin(&mut admin_conn, &db_name).await?;
        wait_for_database_absence_admin(&mut admin_conn, &db_name).await?;
        recreate_pool_database(&db_name, &slot_url).await?;

        let current_meta = load_pool_meta(&mut admin_conn, &db_name)
            .await?
            .expect("pool metadata should exist after recreation");
        let expected_extensions = current_meta.extensions.clone();

        store_pool_meta(
            &mut admin_conn,
            &db_name,
            &PoolMeta {
                fingerprint: Some("stale-fingerprint".to_string()),
                extensions: HashMap::new(),
                dirty: true,
                updated_at_rfc3339: Timestamp::now().format_rfc3339(),
                last_error: Some("stale metadata".to_string()),
            },
        )
        .await?;

        reconcile_existing_pool_database(
            &config.admin_url,
            &db_name,
            &slot_url,
            &expected_extensions,
        )
        .await?;

        let reconciled_meta = load_pool_meta(&mut admin_conn, &db_name)
            .await?
            .expect("reconciled pool metadata should exist");
        assert_eq!(reconciled_meta.fingerprint, Some(schema_fingerprint()?));
        assert_eq!(reconciled_meta.extensions, expected_extensions);
        assert!(
            !reconciled_meta.dirty,
            "reconciled pool metadata must be clean"
        );
        assert_eq!(reconciled_meta.last_error, None);

        let slot_pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(2)
            .connect(&slot_url)
            .await?;
        let drift = reset::schema_mismatch_reason(&slot_pool).await?;
        assert_eq!(
            drift, None,
            "reconciled pool database should be schema-clean"
        );
        slot_pool.close().await;

        drop_database_if_exists_admin(&mut admin_conn, &db_name).await?;
        wait_for_database_absence_admin(&mut admin_conn, &db_name).await?;
        Ok(())
    }

    #[sinex_test]
    async fn test_mark_pool_database_clean_rejects_residual_schema_drift() -> TestResult<()> {
        let config = PoolConfig::default();
        let db_name = format!("sinex_test_pool_mark_clean_drift_{}", std::process::id());
        let slot_url = url_with_db_name(&config.base_url, &db_name)?;
        let mut admin_conn = connect_admin_with_retry(&config.admin_url).await?;

        drop_database_if_exists_admin(&mut admin_conn, &db_name).await?;
        wait_for_database_absence_admin(&mut admin_conn, &db_name).await?;
        recreate_pool_database(&db_name, &slot_url).await?;

        let expected_extensions = load_pool_meta(&mut admin_conn, &db_name)
            .await?
            .expect("pool metadata should exist after recreation")
            .extensions;

        let slot_pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(2)
            .connect(&slot_url)
            .await?;
        sqlx::query(
            r"
            ALTER TABLE raw.source_material_registry
                DROP CONSTRAINT IF EXISTS source_material_registry_status_check,
                ADD CONSTRAINT source_material_registry_status_check
                CHECK (status IN ('sensing', 'completed', 'recovered_partial', 'failed'))
            ",
        )
        .execute(&slot_pool)
        .await?;
        let drift = reset::schema_mismatch_reason(&slot_pool).await?;
        assert!(
            drift
                .as_deref()
                .is_some_and(|reason| reason.contains("source_material_registry_status_check")),
            "expected stale status constraint drift, got {drift:?}"
        );
        slot_pool.close().await;

        let error = mark_pool_database_clean(
            &mut admin_conn,
            &db_name,
            &slot_url,
            &expected_extensions,
            PoolCleanVerification::RequireSchemaVerification,
        )
        .await
        .expect_err("residual schema drift must prevent clean pool metadata");
        let rendered = format!("{error:#}");
        assert!(
            rendered.contains("still has schema drift after convergence"),
            "unexpected error: {rendered}"
        );
        assert!(
            rendered.contains("source_material_registry_status_check"),
            "drift detail should be preserved: {rendered}"
        );

        drop_database_if_exists_admin(&mut admin_conn, &db_name).await?;
        wait_for_database_absence_admin(&mut admin_conn, &db_name).await?;
        Ok(())
    }

    #[sinex_test]
    async fn test_retryable_connection_report_treats_runtime_shutdown_as_transient()
    -> TestResult<()> {
        let report = eyre!(
            "error communicating with database: A Tokio 1.x context was found, but it is being shutdown."
        );

        assert!(
            is_retryable_connection_report(&report),
            "runtime shutdown communication errors should be retried via fresh cleanup pools"
        );

        Ok(())
    }
}
