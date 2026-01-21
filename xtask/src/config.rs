//! Environment-based configuration for xtask.
//!
//! Reads configuration from environment variables (typically set by devenv.nix)
//! to ensure xtask and the development environment stay in sync.

use std::{env, path::PathBuf};

/// Configuration derived from environment variables.
#[derive(Debug, Clone)]
pub struct Config {
    /// Database connection URL
    pub database_url: Option<String>,
    /// NATS server URL
    pub nats_url: Option<String>,
    /// State directory for persistent data (history, jobs)
    pub state_dir: PathBuf,
    /// Cache directory for temporary data
    pub cache_dir: PathBuf,
    /// Test results directory
    pub test_results_dir: Option<PathBuf>,
    /// Hostname of the current machine
    pub hostname: String,
    /// Toolchain identifier (e.g., "fenix(x86_64-linux)")
    pub toolchain: Option<String>,
    /// Whether we're inside a devenv shell
    pub in_devenv: bool,
}

impl Config {
    /// Load configuration from environment variables.
    pub fn from_env() -> Self {
        let state_dir = env::var("SINEX_STATE_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| {
                dirs::state_dir()
                    .unwrap_or_else(|| PathBuf::from("/tmp"))
                    .join("sinex")
            });

        let cache_dir = env::var("SINEX_CACHE_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| {
                dirs::cache_dir()
                    .unwrap_or_else(|| PathBuf::from("/tmp"))
                    .join("sinex")
            });

        let hostname = gethostname::gethostname().to_string_lossy().into_owned();

        Self {
            database_url: env::var("DATABASE_URL").ok(),
            nats_url: env::var("SINEX_NATS_URL").ok(),
            state_dir,
            cache_dir,
            test_results_dir: env::var("SINEX_TEST_RESULTS_DIR").map(PathBuf::from).ok(),
            hostname,
            toolchain: env::var("SINEX_DEVENV_TOOLCHAIN").ok(),
            in_devenv: env::var("SINEX_DEVENV_SYSTEM").is_ok(),
        }
    }

    /// Path to the history database.
    pub fn history_db_path(&self) -> PathBuf {
        self.state_dir.join("xtask-history.db")
    }

    /// Directory for job output files.
    pub fn jobs_dir(&self) -> PathBuf {
        self.state_dir.join("jobs")
    }

    /// Ensure the state directory exists.
    pub fn ensure_state_dir(&self) -> std::io::Result<()> {
        std::fs::create_dir_all(&self.state_dir)
    }

    /// Ensure the jobs directory exists.
    pub fn ensure_jobs_dir(&self) -> std::io::Result<()> {
        std::fs::create_dir_all(self.jobs_dir())
    }
}

impl Default for Config {
    fn default() -> Self {
        Self::from_env()
    }
}

/// Global configuration singleton.
static CONFIG: once_cell::sync::Lazy<Config> = once_cell::sync::Lazy::new(Config::from_env);

/// Get the global configuration.
pub fn config() -> &'static Config {
    &CONFIG
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_from_env() {
        let config = Config::from_env();
        // Should at least have a hostname
        assert!(!config.hostname.is_empty());
    }

    #[test]
    fn test_history_db_path() {
        let config = Config::from_env();
        let path = config.history_db_path();
        assert!(path.ends_with("xtask-history.db"));
    }
}
