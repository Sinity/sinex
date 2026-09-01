use crate::advisory_lock::AdvisoryLock;
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
            return Err(
                SinexError::database("Failed to acquire schema-apply advisory lock")
                    .with_operation("schema_apply.ddl_apply_lock")
                    .with_source(err),
            );
        }
    };

    let destructive_contract =
        crate::schema::converge::destructive_schema_contract().map_err(map_apply_error)?;
    if destructive_contract
        .split('|')
        .nth(1)
        .is_some_and(|changes| !changes.is_empty())
    {
        warn!(
            compatibility_epoch = sinex_primitives::EXPECTED_BINARY_SCHEMA_VERSION,
            destructive_contract,
            "startup schema apply includes destructive column drops; rollback to an older binary is refused"
        );
    }
    info!("Applying declarative database schema...");
    crate::schema::apply::apply(pool)
        .await
        .map_err(map_apply_error)?;
    record_binary_schema_version(pool).await?;
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
            Err(err) => return Err(err),
        }
    } else {
        info!(
            "SINEX_PRIVACY_SEED_BUILTIN disabled -- skipping built-in privacy catalog convergence"
        );
    }

    Ok(())
}

/// Record the compatibility epoch only after all DDL convergence has
/// succeeded. This is deliberately part of the schema-apply transaction
/// boundary: an old binary sees the upgraded epoch on rollback and refuses to
/// start instead of re-adding destructive-drop columns as empty data.
async fn record_binary_schema_version(pool: &DbPool) -> Result<()> {
    use sinex_primitives::EXPECTED_BINARY_SCHEMA_VERSION;

    sqlx::query(
        r#"
        INSERT INTO sinex_schemas.binary_schema_version (id, version)
        VALUES (1, $1)
        ON CONFLICT (id) DO UPDATE SET version = EXCLUDED.version
        "#,
    )
    .bind(EXPECTED_BINARY_SCHEMA_VERSION)
    .execute(pool)
    .await
    .map_err(|error| {
        SinexError::database("Failed to record binary schema version after schema apply")
            .with_operation("schema_apply.record_binary_schema_version")
            .with_source(error)
    })?;

    info!(
        version = EXPECTED_BINARY_SCHEMA_VERSION,
        "Recorded binary schema version"
    );
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

    let apply_result = apply_schema(&pool).await;
    pool.close().await;
    apply_result
}
