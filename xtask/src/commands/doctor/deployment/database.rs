use super::DeploymentReadinessItem;
use color_eyre::eyre::{Result, WrapErr, eyre};
use sinex_primitives::{
    DeploymentDatabaseRuntime, DeploymentReadinessDescriptor,
    environment::SinexEnvironment,
    utils::{InvalidUrlPolicy, redact_url_password_for_diagnostics},
};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::commands::doctor) struct DatabaseProbeTarget {
    pub(in crate::commands::doctor) database_url: String,
    pub(in crate::commands::doctor) password_file: Option<PathBuf>,
    pub(in crate::commands::doctor) password_required: bool,
    pub(in crate::commands::doctor) source: String,
}

fn descriptor_database_url(database: &DeploymentDatabaseRuntime) -> Result<Option<String>> {
    if !database.enabled {
        return Ok(None);
    }

    let Some(user) = database.user.as_deref() else {
        return Err(eyre!(
            "deployment descriptor database runtime is enabled but database.user is missing"
        ));
    };
    let Some(host) = database.host.as_deref() else {
        return Err(eyre!(
            "deployment descriptor database runtime is enabled but database.host is missing"
        ));
    };
    let Some(port) = database.port else {
        return Err(eyre!(
            "deployment descriptor database runtime is enabled but database.port is missing"
        ));
    };
    let Some(name) = database.name.as_deref() else {
        return Err(eyre!(
            "deployment descriptor database runtime is enabled but database.name is missing"
        ));
    };

    Ok(Some(format!("postgresql://{user}@{host}:{port}/{name}")))
}

pub(in crate::commands::doctor) fn resolve_database_probe_target(
    database_url: Option<&str>,
    descriptor: Option<&DeploymentReadinessDescriptor>,
) -> Result<Option<DatabaseProbeTarget>> {
    if let Some(url) = database_url {
        return Ok(Some(DatabaseProbeTarget {
            database_url: url.to_string(),
            password_file: None,
            password_required: false,
            source: "DATABASE_URL".to_string(),
        }));
    }

    let Some(descriptor) = descriptor else {
        return Ok(None);
    };
    let Some(url) = descriptor_database_url(&descriptor.database)? else {
        return Ok(None);
    };

    Ok(Some(DatabaseProbeTarget {
        database_url: url,
        password_file: descriptor.secrets.database_password_file.clone(),
        password_required: descriptor.database.password_required,
        source: descriptor
            .source
            .clone()
            .unwrap_or_else(|| "deployment descriptor".to_string()),
    }))
}

pub(crate) fn resolve_effective_database_probe_url(
    database_url: Option<&str>,
    descriptor: Option<&DeploymentReadinessDescriptor>,
    purpose: &str,
) -> Result<Option<(String, String)>> {
    let Some(probe_target) = resolve_database_probe_target(database_url, descriptor)? else {
        return Ok(None);
    };

    let mut effective_url = SinexEnvironment::current()
        .wrap_err_with(|| format!("failed to resolve SINEX_ENVIRONMENT for {purpose}"))
        .and_then(|env| {
            env.database_url(probe_target.database_url.as_str())
                .wrap_err_with(|| format!("failed to derive namespaced database URL for {purpose}"))
        })?;

    if let Some(password_file) = probe_target.password_file.as_deref() {
        let password = read_database_password(password_file)?;
        let mut parsed = url::Url::parse(&effective_url).wrap_err_with(|| {
            format!(
                "resolved {} for {purpose} but failed to parse it as a database URL",
                probe_target.source
            )
        })?;
        parsed
            .set_password(Some(&password))
            .map_err(|()| eyre!("failed to apply database password for {purpose}"))?;
        effective_url = parsed.to_string();
    } else if probe_target.password_required && !database_url_has_password(&effective_url) {
        return Err(eyre!(
            "{purpose} requires password authentication, but {} does not provide a password and deployment secret material is missing",
            probe_target.source
        ));
    }

    Ok(Some((effective_url, probe_target.source)))
}

fn database_url_has_password(database_url: &str) -> bool {
    url::Url::parse(database_url)
        .ok()
        .and_then(|value| value.password().map(str::to_string))
        .is_some()
}

/// Redact the password component of a database URL for safe display/logging.
///
/// Replaces the password with `***` if present.  Returns the original string
/// unchanged if it is not a valid URL or contains no password.
pub(in crate::commands::doctor) fn redact_database_url_password(database_url: &str) -> String {
    redact_url_password_for_diagnostics(database_url, InvalidUrlPolicy::PreserveInput)
}

fn read_database_password(password_file: &Path) -> Result<String> {
    let password = std::fs::read_to_string(password_file).map_err(|error| {
        eyre!(
            "failed to read database password file {}: {error}",
            password_file.display()
        )
    })?;
    Ok(password.trim_end_matches(['\n', '\r']).to_string())
}

pub(in crate::commands::doctor) async fn check_schema_apply(
    database_url: Option<&str>,
    descriptor: Option<&DeploymentReadinessDescriptor>,
) -> DeploymentReadinessItem {
    if descriptor.is_some_and(|value| !value.expectations.schema_apply) {
        return DeploymentReadinessItem::skip(
            "schema-apply",
            "Schema bootstrap is not expected in the deployment descriptor",
        );
    }

    let (effective_url, source) = match resolve_effective_database_probe_url(
        database_url,
        descriptor,
        "schema-apply probe",
    ) {
        Ok(Some(result)) => result,
        Ok(None) => {
            return DeploymentReadinessItem::fail(
                "schema-apply",
                "Schema bootstrap is expected but neither DATABASE_URL nor deployment descriptor database runtime is available",
            );
        }
        Err(error) => {
            return DeploymentReadinessItem::fail("schema-apply", error.to_string());
        }
    };

    use sqlx::Row;
    use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

    let connect_options: PgConnectOptions = match effective_url.parse() {
        Ok(options) => options,
        Err(error) => {
            return DeploymentReadinessItem::fail(
                "schema-apply",
                format!("Resolved {source} for schema-apply but failed to parse it: {error}",),
            );
        }
    };

    let pool = match PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(crate::preflight::SCHEMA_READINESS_PROBE_TIMEOUT)
        .connect_with(connect_options)
        .await
    {
        Ok(p) => p,
        Err(e) => {
            return DeploymentReadinessItem::fail(
                "schema-apply",
                format!("Cannot connect to database via {source}: {e}"),
            );
        }
    };

    match tokio::time::timeout(
        crate::preflight::SCHEMA_READINESS_PROBE_TIMEOUT,
        sqlx::query("SELECT count(*) FROM information_schema.schemata WHERE schema_name = 'core'")
            .fetch_one(&pool),
    )
    .await
    {
        Err(_) => DeploymentReadinessItem::fail(
            "schema-apply",
            format!(
                "Database query timed out after {:?}",
                crate::preflight::SCHEMA_READINESS_PROBE_TIMEOUT
            ),
        ),
        Ok(Err(e)) => {
            DeploymentReadinessItem::fail("schema-apply", format!("Database query failed: {e}"))
        }
        Ok(Ok(row)) => {
            let count: i64 = row.get(0);
            if count > 0 {
                DeploymentReadinessItem::pass(
                    "schema-apply",
                    "Database reachable and 'core' schema exists",
                )
            } else {
                DeploymentReadinessItem::fail(
                    "schema-apply",
                    "Database reachable but 'core' schema is missing — schema-apply may not have run",
                )
            }
        }
    }
}
