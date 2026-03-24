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
use sinex_primitives::environment::environment;
use std::process::Output;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum VerificationStatus {
    Pass,
    Warning,
    Fail,
}

pub(crate) fn deployment_descriptor_result(
    _log_context: &str,
) -> NodeResult<Option<DeploymentReadinessDescriptor>> {
    DeploymentReadinessDescriptor::load()
}

pub(crate) fn edge_mode_enabled() -> bool {
    std::env::var_os("SINEX_EDGE_MODE").is_some()
}

pub(crate) fn runtime_database_expected() -> NodeResult<bool> {
    if edge_mode_enabled() {
        return Ok(false);
    }

    Ok(
        deployment_descriptor_result("preflight runtime expectation")?
            .map(|descriptor| descriptor.expectations.schema_apply)
            .unwrap_or(true),
    )
}

pub fn resolve_database_url() -> NodeResult<String> {
    let base_url = std::env::var("DATABASE_URL").map_err(|_| {
        SinexError::configuration("Database URL environment variable not set (DATABASE_URL)")
    })?;

    sinex_db::resolve_effective_database_url(&base_url).map_err(|err| {
        SinexError::configuration(format!(
            "Failed to resolve effective database URL for Sinex environment '{}'",
            environment().name()
        ))
        .with_std_error(&err)
    })
}

fn env_string_with_fallback(names: &[&str]) -> Option<String> {
    for name in names {
        if let Ok(value) = std::env::var(name) {
            return Some(value);
        }
    }
    None
}

pub fn resolve_nats_url() -> NodeResult<String> {
    env_string_with_fallback(&["SINEX_NATS_URL"]).ok_or_else(|| {
        SinexError::configuration(
            "NATS URL environment variable not set (SINEX_NATS_URL)".to_string(),
        )
    })
}
