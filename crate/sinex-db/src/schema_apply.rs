use crate::repositories::DbPoolExt;
use crate::{DbPool, PoolConfig};
use sinex_primitives::error::{Result, SinexError};
use sinex_primitives::privacy::builtin_policy_seed_rules;
use sinex_primitives::units::Seconds;
use tracing::{info, warn};

const SQLSTATE_UNDEFINED_FILE: &str = "58P01";
const ERROR_CLASS_TIMESCALEDB_MISSING_LIBRARY: &str = "timescaledb_missing_library";
const ERROR_CLASS_MISSING_REQUIRED_EXTENSIONS: &str = "missing_required_extensions";
const ERROR_CLASS_SCHEMA_APPLY_INTERNAL: &str = "schema_apply_internal";

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
/// Also converges the built-in privacy-catalog rules (sinex-t5sr / sinex-4kbm
/// point 1): the catalog is the one piece of declared desired-state that
/// historically opted out of this same convergence path, requiring a manual
/// `sinexctl privacy policy seed-builtin` call that nothing invoked
/// automatically. `seed_rules` is idempotent (`ON CONFLICT ... DO UPDATE ...
/// WHERE ... IS DISTINCT FROM`), matching the DDL convergence semantics above,
/// so calling it on every apply is safe and cheap when nothing changed.
/// `SINEX_PRIVACY_SEED_BUILTIN=0/false/no/off` opts out (e.g. for a deployment
/// that wants to curate `privacy.rules` by hand); default is seed-on.
pub async fn apply_schema(pool: &DbPool) -> Result<()> {
    info!("Applying declarative database schema...");
    crate::schema::apply::apply(pool)
        .await
        .map_err(map_apply_error)?;
    info!("Database schema apply completed");

    if privacy_seed_builtin_enabled() {
        let rules = builtin_policy_seed_rules(true);
        match pool.privacy_policy().seed_rules(&rules).await {
            Ok(summary) => {
                info!(
                    inserted = summary.inserted,
                    updated = summary.updated,
                    unchanged = summary.unchanged,
                    "Converged built-in privacy catalog"
                );
            }
            Err(err) => {
                // Non-fatal: schema DDL already converged successfully. A
                // failed catalog seed should not block startup, but must be
                // loud -- silent catalog emptiness is exactly sinex-t5sr.
                warn!(error = %err, "Failed to converge built-in privacy catalog; redaction rules may be stale or absent");
            }
        }
    } else {
        info!("SINEX_PRIVACY_SEED_BUILTIN disabled -- skipping built-in privacy catalog convergence");
    }

    Ok(())
}

fn privacy_seed_builtin_enabled() -> bool {
    match std::env::var("SINEX_PRIVACY_SEED_BUILTIN") {
        Ok(v) => !matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "0" | "false" | "no" | "off"
        ),
        Err(_) => true,
    }
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
