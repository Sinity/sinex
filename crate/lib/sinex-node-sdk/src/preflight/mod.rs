#![doc = include_str!("../../docs/preflight.md")]

pub mod configuration;
pub mod database;
pub mod resources;
pub mod services;
pub mod verification;

// validate_toml_file is now private to the configuration module
use crate::{NodeResult, SinexError};
pub use services::verify_service_dependencies;
use sinex_primitives::DeploymentReadinessDescriptor;
use sinex_primitives::constants::timeouts;
use sinex_primitives::env as shared_env;
use sqlx::PgPool;
use sqlx::postgres::{PgConnection, PgPoolOptions};
use std::process::Output;
use std::time::Duration;
use tracing::warn;

pub(crate) const PREFLIGHT_DB_MAX_CONNECTIONS: u32 = 4;
pub(crate) const PREFLIGHT_DB_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(5);
pub(crate) const PREFLIGHT_STATEMENT_TIMEOUT: &str = "5s";
pub(crate) const PREFLIGHT_LOCK_TIMEOUT: &str = "1s";
pub(crate) const PREFLIGHT_IDLE_IN_TRANSACTION_TIMEOUT: &str = "5s";
pub(crate) const PREFLIGHT_MAX_PARALLEL_WORKERS_PER_GATHER: &str = "0";

/// Run an external command with a timeout to prevent indefinite hangs during preflight.
pub(crate) async fn run_command_with_timeout(program: &str, args: &[&str]) -> NodeResult<Output> {
    let fut = tokio::process::Command::new(program).args(args).output();

    match tokio::time::timeout(timeouts::PREFLIGHT_COMMAND_TIMEOUT, fut).await {
        Ok(Ok(output)) => Ok(output),
        Ok(Err(e)) => Err(SinexError::processing(format!(
            "Failed to execute '{program}': {e}"
        ))),
        Err(_) => Err(SinexError::processing(format!(
            "Command '{program} {}' timed out after {}s",
            args.join(" "),
            timeouts::PREFLIGHT_COMMAND_TIMEOUT.as_secs()
        ))),
    }
}

/// Connect to PostgreSQL for preflight checks with startup-safe session limits.
///
/// Preflight runs on runtime startup paths, so its database work must stay cheap
/// and cancellable. All DB phases share this pool to keep connection count,
/// statement runtime, lock waits, and PostgreSQL parallel workers bounded.
pub(crate) async fn connect_preflight_database_pool(database_url: &str) -> NodeResult<PgPool> {
    let connect = PgPoolOptions::new()
        .max_connections(PREFLIGHT_DB_MAX_CONNECTIONS)
        .acquire_timeout(PREFLIGHT_DB_ACQUIRE_TIMEOUT)
        .after_connect(|conn, _meta| {
            Box::pin(async move {
                configure_preflight_database_session(conn).await?;
                Ok(())
            })
        })
        .before_acquire(|conn, _meta| {
            Box::pin(async move {
                if let Err(error) = configure_preflight_database_session(conn).await {
                    warn!(
                        error = %error,
                        "Preflight database connection failed bounded session setup"
                    );
                    return Ok(false);
                }

                Ok(true)
            })
        })
        .connect(database_url);

    match tokio::time::timeout(timeouts::PREFLIGHT_DATABASE_TIMEOUT, connect).await {
        Ok(Ok(pool)) => Ok(pool),
        Ok(Err(error)) => Err(SinexError::from(error)),
        Err(_) => Err(SinexError::processing(format!(
            "Preflight database connection timed out after {}s",
            timeouts::PREFLIGHT_DATABASE_TIMEOUT.as_secs()
        ))),
    }
}

pub(crate) async fn configure_preflight_database_session(
    conn: &mut PgConnection,
) -> sqlx::Result<()> {
    for (name, value) in [
        ("statement_timeout", PREFLIGHT_STATEMENT_TIMEOUT),
        ("lock_timeout", PREFLIGHT_LOCK_TIMEOUT),
        (
            "idle_in_transaction_session_timeout",
            PREFLIGHT_IDLE_IN_TRANSACTION_TIMEOUT,
        ),
        (
            "max_parallel_workers_per_gather",
            PREFLIGHT_MAX_PARALLEL_WORKERS_PER_GATHER,
        ),
    ] {
        sqlx::query("SELECT pg_catalog.set_config($1, $2, false)")
            .bind(name)
            .bind(value)
            .execute(&mut *conn)
            .await?;
    }

    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum VerificationStatus {
    Pass,
    Warning,
    Fail,
}

pub(crate) fn deployment_descriptor_result() -> NodeResult<Option<DeploymentReadinessDescriptor>> {
    DeploymentReadinessDescriptor::load()
}

pub(crate) fn edge_mode_enabled() -> bool {
    std::env::var_os("SINEX_EDGE_MODE").is_some()
}

pub(crate) fn runtime_database_expected() -> NodeResult<bool> {
    if edge_mode_enabled() {
        return Ok(false);
    }

    Ok(deployment_descriptor_result()?
        .is_none_or(|descriptor| descriptor.expectations.schema_apply))
}

pub fn resolve_database_url() -> NodeResult<String> {
    let base_url = shared_env::strict_var("DATABASE_URL")?.ok_or_else(|| {
        SinexError::configuration("Database URL environment variable not set (DATABASE_URL)")
    })?;

    sinex_db::resolve_effective_database_url(&base_url).map_err(|err| {
        SinexError::configuration("Failed to validate DATABASE_URL").with_std_error(&err)
    })
}

pub(crate) fn env_string_with_fallback(names: &[&str]) -> NodeResult<Option<String>> {
    for name in names {
        match shared_env::strict_var(name) {
            Ok(Some(value)) => return Ok(Some(value)),
            Ok(None) => {}
            Err(e) => return Err(e),
        }
    }
    Ok(None)
}

pub fn resolve_nats_url() -> NodeResult<String> {
    env_string_with_fallback(&["SINEX_NATS_URL"])?.ok_or_else(|| {
        SinexError::configuration(
            "NATS URL environment variable not set (SINEX_NATS_URL)".to_string(),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::{resolve_database_url, resolve_nats_url};
    use std::ffi::OsString;
    use std::sync::LazyLock;
    use xtask::sandbox::sinex_test;

    static ENV_LOCK: LazyLock<tokio::sync::Mutex<()>> =
        LazyLock::new(|| tokio::sync::Mutex::new(()));

    fn restore_var(key: &str, value: Option<OsString>) {
        match value {
            Some(value) => unsafe { std::env::set_var(key, value) },
            None => unsafe { std::env::remove_var(key) },
        }
    }

    #[sinex_test]
    async fn resolve_nats_url_reports_missing_variable() -> xtask::sandbox::TestResult<()> {
        let _guard = ENV_LOCK.lock().await;
        let previous = std::env::var_os("SINEX_NATS_URL");
        unsafe { std::env::remove_var("SINEX_NATS_URL") };

        let error = resolve_nats_url().expect_err("missing NATS URL should surface");

        restore_var("SINEX_NATS_URL", previous);

        assert!(error.to_string().contains("SINEX_NATS_URL"));
        Ok(())
    }

    #[cfg(unix)]
    #[sinex_test]
    async fn resolve_nats_url_rejects_non_unicode_override() -> xtask::sandbox::TestResult<()> {
        use std::os::unix::ffi::OsStringExt;

        let _guard = ENV_LOCK.lock().await;
        let previous = std::env::var_os("SINEX_NATS_URL");
        unsafe { std::env::set_var("SINEX_NATS_URL", OsString::from_vec(vec![0x66, 0x6f, 0x80])) };

        let error = resolve_nats_url().expect_err("non-unicode NATS URL should surface");

        restore_var("SINEX_NATS_URL", previous);

        assert!(error.to_string().contains("not valid UTF-8"));
        Ok(())
    }

    #[sinex_test]
    async fn resolve_database_url_reports_missing_variable() -> xtask::sandbox::TestResult<()> {
        let _guard = ENV_LOCK.lock().await;
        let previous = std::env::var_os("DATABASE_URL");
        unsafe { std::env::remove_var("DATABASE_URL") };

        let error = resolve_database_url().expect_err("missing DATABASE_URL should surface");

        restore_var("DATABASE_URL", previous);

        assert!(error.to_string().contains("DATABASE_URL"));
        Ok(())
    }

    #[cfg(unix)]
    #[sinex_test]
    async fn resolve_database_url_rejects_non_unicode_override() -> xtask::sandbox::TestResult<()> {
        use std::os::unix::ffi::OsStringExt;

        let _guard = ENV_LOCK.lock().await;
        let previous = std::env::var_os("DATABASE_URL");
        unsafe { std::env::set_var("DATABASE_URL", OsString::from_vec(vec![0x70, 0x80])) };

        let error = resolve_database_url().expect_err("non-unicode DATABASE_URL should surface");

        restore_var("DATABASE_URL", previous);

        assert!(error.to_string().contains("DATABASE_URL"));
        assert!(error.to_string().contains("not valid UTF-8"));
        Ok(())
    }
}
