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
    pub(crate) fn from_env() -> Self {
        let state_dir = env::var("SINEX_STATE_DIR").map_or_else(
            |_| {
                dirs::state_dir()
                    .unwrap_or_else(|| PathBuf::from("/tmp"))
                    .join("sinex")
            },
            PathBuf::from,
        );

        let cache_dir = env::var("SINEX_CACHE_DIR").map_or_else(
            |_| {
                dirs::cache_dir()
                    .unwrap_or_else(|| PathBuf::from("/tmp"))
                    .join("sinex")
            },
            PathBuf::from,
        );

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
    pub(crate) fn history_db_path(&self) -> PathBuf {
        self.state_dir.join("xtask-history.db")
    }

    /// Directory for job output files.
    pub(crate) fn jobs_dir(&self) -> PathBuf {
        self.state_dir.join("jobs")
    }

    /// Ensure the state directory exists.
    pub(crate) fn ensure_state_dir(&self) -> std::io::Result<()> {
        std::fs::create_dir_all(&self.state_dir)
    }

    /// Ensure the jobs directory exists.
    pub(crate) fn ensure_jobs_dir(&self) -> std::io::Result<()> {
        std::fs::create_dir_all(self.jobs_dir())
    }
}

impl Default for Config {
    fn default() -> Self {
        Self::from_env()
    }
}

/// Global configuration singleton.
static CONFIG: std::sync::LazyLock<Config> = std::sync::LazyLock::new(Config::from_env);

/// Get the global configuration.
pub fn config() -> &'static Config {
    &CONFIG
}

/// Detect whether xtask is being invoked from inside a cargo-nextest test run.
///
/// When nextest runs tests, it holds an exclusive lock on the cargo target directory
/// for the duration of the entire test suite. Any child process that tries to invoke
/// `cargo` (check, clippy, build, run) will block indefinitely waiting for that lock.
///
/// This function lets callers detect the nextest context and bail early instead of
/// hanging. Use it as a guard at the top of any function that would invoke cargo:
///
/// ```rust
/// if crate::config::is_nextest_run() {
///     bail!("Cannot invoke cargo from inside nextest (target/ lock deadlock risk)");
/// }
/// ```
#[must_use]
pub fn is_nextest_run() -> bool {
    std::env::var_os("NEXTEST_RUN_ID").is_some() || std::env::var_os("NEXTEST").is_some()
}

/// Determine the workspace root directory.
///
/// Uses `CARGO_MANIFEST_DIR` (set when running via xtask) and navigates
/// to the parent directory (since xtask is a workspace member in `xtask/`).
/// Falls back to the current directory if the env var is not set.
pub fn workspace_root() -> PathBuf {
    env::var("CARGO_MANIFEST_DIR")
        .map(PathBuf::from)
        .map_or_else(
            |_| {
                // Fallback: use current directory
                env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
            },
            |p| {
                // CARGO_MANIFEST_DIR points to xtask/, go up one level for workspace root
                p.parent().map(std::path::Path::to_path_buf).unwrap_or(p)
            },
        )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::sinex_test;

    #[sinex_test]
    async fn test_config_from_env() -> TestResult<()> {
        let config = Config::from_env();
        // Should at least have a hostname
        assert!(!config.hostname.is_empty());
        Ok(())
    }

    #[sinex_test]
    async fn test_history_db_path() -> TestResult<()> {
        let config = Config::from_env();
        let path = config.history_db_path();
        assert!(path.ends_with("xtask-history.db"));
        Ok(())
    }
}
