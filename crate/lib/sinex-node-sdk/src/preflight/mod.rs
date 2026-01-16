#![doc = include_str!("../../docs/preflight.md")]

pub mod configuration;
pub mod database;
pub mod resources;
pub mod services;
pub mod verification;

// validate_toml_file is now private to the configuration module
use color_eyre::eyre::{self, Result};
pub use services::verify_service_dependencies;
pub use verification::run_preflight_checks;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum VerificationStatus {
    Pass,
    Warning,
    Fail,
}
fn env_string_with_fallback(names: &[&str]) -> Option<String> {
    for name in names {
        if let Ok(value) = std::env::var(name) {
            return Some(value);
        }
    }
    None
}

pub fn resolve_database_url() -> Result<String> {
    env_string_with_fallback(&["SINEX_DATABASE_URL", "DATABASE_URL"]).ok_or_else(|| {
        eyre::eyre!("Database URL environment variable not set (SINEX_DATABASE_URL/DATABASE_URL)")
    })
}

pub fn resolve_nats_url() -> Result<String> {
    env_string_with_fallback(&["SINEX_NATS_URL"])
        .ok_or_else(|| eyre::eyre!("NATS URL environment variable not set (SINEX_NATS_URL)"))
}
