use crate::advisory_lock::AdvisoryLock;
use crate::{DbPool, PoolConfig};
use sinex_primitives::error::{Result, SinexError};
use sinex_primitives::units::Seconds;
use tracing::info;

const SQLSTATE_UNDEFINED_FILE: &str = "58P01";
const ERROR_CLASS_TIMESCALEDB_MISSING_LIBRARY: &str = "timescaledb_missing_library";
const ERROR_CLASS_MISSING_REQUIRED_EXTENSIONS: &str = "missing_required_extensions";
const ERROR_CLASS_SCHEMA_APPLY_INTERNAL: &str = "schema_apply_internal";
const ERROR_CLASS_SCHEMA_APPLY_CONCURRENT: &str = "schema_apply_concurrent";

/// sinex-z1dk: distinct from `event_engine`'s `"event_engine.migrations"` key
/// (`crate::advisory_lock` is a shared PG advisory-lock namespace keyed by
/// string, hashed independently per key) -- this guards the DDL convergence
/// engine specifically, which had no lock at all despite the lighter
/// event_engine schema-sync correctly using one.
const SCHEMA_DDL_APPLY_LOCK_KEY: &str = "sinex_schema.ddl_apply";

fn map_apply_error(err: crate::schema::apply::ApplyError) -> SinexError {
    match err {
        crate::schema::apply::ApplyError::MissingExtensions(missing) => {
            SinexError::database("Schema apply failed: required PostgreSQL extensions missing")
                .with_context("error_class", ERROR_CLASS_MISSING_REQUIRED_EXTENSIONS)
                .with_context("missing_extensions", missing.join(","))
        }
        crate::schema::apply::ApplyError::Sqlx(sqlx_err) => {
            let mut mapped = SinexError::database("Schema apply failed").with_std_error(&sqlx_err);
            if let sqlx::Error::Database(db_err) = &sqlx_err {
                if let Some(code) = db_err.code() {
                    mapped = mapped.with_context("sqlstate", code.as_ref());
                }
                if db_err
                    .code()
                    .as_deref()
                    .is_some_and(|code| code == SQLSTATE_UNDEFINED_FILE)
                {
                    mapped =
                        mapped.with_context("error_class", ERROR_CLASS_TIMESCALEDB_MISSING_LIBRARY);
                }
            }
            mapped
        }
        crate::schema::apply::ApplyError::Internal(message) => {
            SinexError::database("Schema apply failed")
                .with_context("error_class", ERROR_CLASS_SCHEMA_APPLY_INTERNAL)
                .with_context("cause", message)
        }
    }
}

/// Apply declarative schema using the given pool.
///
/// sinex-z1dk: guarded by a `PostgreSQL` advisory lock (`SCHEMA_DDL_APPLY_LOCK_KEY`)
/// so two concurrent `apply_schema` callers against the same database -- a
/// stuck old process during a botched restart, an operator manually running
/// `sinexd serve` for debugging while the systemd unit is also up -- fail
/// fast with a clear error instead of racing the two-phase
/// `NOT VALID -> VALIDATE CONSTRAINT` convergence step.
pub async fn apply_schema(pool: &DbPool) -> Result<()> {
    let _lock = match AdvisoryLock::try_acquire(pool, SCHEMA_DDL_APPLY_LOCK_KEY).await {
        Ok(Some(guard)) => guard,
        Ok(None) => {
            return Err(SinexError::database(
                "Another process is already applying the database schema",
            )
            .with_operation("schema_apply.ddl_apply_lock")
            .with_context("error_class", ERROR_CLASS_SCHEMA_APPLY_CONCURRENT));
        }
        Err(err) => {
            return Err(SinexError::database("Failed to acquire schema-apply advisory lock")
                .with_operation("schema_apply.ddl_apply_lock")
                .with_source(err));
        }
    };

    info!("Applying declarative database schema...");
    crate::schema::apply::apply(pool)
        .await
        .map_err(map_apply_error)?;
    info!("Database schema apply completed");
    Ok(())
}

/// Apply declarative schema for a given database URL by creating a temporary connection.
pub async fn apply_schema_for_url(database_url: &str) -> Result<()> {
    use crate::pool::create_pool_with_config;

    let mut config = PoolConfig::from_env();
    // Schema apply performs DDL/index convergence. On production-sized
    // hypertables, valid idempotent checks can exceed the ordinary OLTP query
    // guard, so use an unbounded statement timeout for this temporary pool.
    config.statement_timeout_secs = Seconds::from_secs(0);
    let pool = create_pool_with_config(database_url, &config)
        .await
        .map_err(|e| {
            SinexError::database("Failed to create pool for schema apply").with_std_error(&e)
        })?;

    apply_schema(&pool).await?;
    pool.close().await;
    Ok(())
}
