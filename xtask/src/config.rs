//! Environment-based configuration for xtask.
//!
//! Reads configuration from environment variables (typically set by devenv.nix)
//! to ensure xtask and the development environment stay in sync.
//!
//! # User preferences
//!
//! Optional preferences are loaded from `~/.config/xtask/preferences.toml` (W1).
//! This file is user-managed. Precedence: CLI flag > env var > prefs file > default.
//!
//! ## Example preferences file
//!
//! ```toml
//! notify_on_completion = true
//!
//! [coordinator]
//! auto_sequence = ["check -> test"]
//! ```
//!
//! ## NixOS home-manager integration (W2)
//!
//! ```nix
//! xdg.configFile."xtask/preferences.toml".text = ''
//!   notify_on_completion = true
//!
//!   [coordinator]
//!   auto_sequence = ["check -> test"]
//! '';
//! ```

use std::{env, path::PathBuf};

/// User-managed preferences loaded from `~/.config/xtask/preferences.toml`.
///
/// Fields prefixed with `coordinator` are schema-only: deserialized from TOML
/// today so users can configure them, wired into runtime once the coordinator ships.
#[allow(dead_code)]
#[derive(Debug, Clone, Default, serde::Deserialize)]
pub struct UserPreferences {
    /// Send a desktop notification via `notify-send` when a background job completes (W3).
    #[serde(default)]
    pub notify_on_completion: bool,
    /// Coordinator-specific preferences.
    #[serde(default)]
    pub coordinator: CoordinatorPrefs,
}

/// Coordinator preferences.
///
/// Fields here are intentionally schema-only: deserialized from the preferences
/// TOML file so users can configure them today, wired into runtime logic once
/// the coordinator feature ships.
#[allow(dead_code)]
#[derive(Debug, Clone, Default, serde::Deserialize)]
pub struct CoordinatorPrefs {
    /// Auto-sequence pairs, e.g. `["check -> test"]`.
    ///
    /// When the first command of a pair completes successfully, the second is
    /// automatically queued as a background job.  Currently informational only;
    /// the coordinator uses this for display purposes.
    #[serde(default)]
    pub auto_sequence: Vec<String>,
}

/// Configuration derived from environment variables.
#[derive(Debug, Clone)]
pub struct Config {
    /// Database connection URL
    pub database_url: Option<String>,
    /// NATS server URL
    pub nats_url: Option<String>,
    /// Gateway base URL (without `/rpc`) for HTTP readiness checks
    pub gateway_url: Option<String>,
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
    /// User preferences from `~/.config/xtask/preferences.toml` (W1).
    pub prefs: UserPreferences,
}

impl Config {
    /// Load configuration from environment variables.
    pub(crate) fn from_env() -> Self {
        let repo_state_root = workspace_root().join(".sinex");
        let state_dir = env::var("SINEX_STATE_DIR")
            .map_or_else(|_| repo_state_root.join("state"), PathBuf::from);

        let cache_dir = env::var("SINEX_CACHE_DIR")
            .map_or_else(|_| repo_state_root.join("cache"), PathBuf::from);

        let hostname = gethostname::gethostname().to_string_lossy().into_owned();

        Self {
            database_url: env::var("DATABASE_URL").ok(),
            nats_url: env::var("SINEX_NATS_URL").ok(),
            gateway_url: env::var("SINEX_GATEWAY_URL")
                .ok()
                .or_else(|| env::var("SINEX_RPC_URL").ok())
                .or_else(|| {
                    env::var("SINEX_GATEWAY_TCP_LISTEN")
                        .ok()
                        .map(|listen| format!("https://{listen}"))
                }),
            state_dir,
            cache_dir,
            test_results_dir: env::var("SINEX_TEST_RESULTS_DIR").map(PathBuf::from).ok(),
            hostname,
            toolchain: env::var("SINEX_DEVENV_TOOLCHAIN").ok(),
            in_devenv: env::var("SINEX_DEVENV_SYSTEM").is_ok(),
            prefs: load_user_preferences(),
        }
    }

    /// Path to the history database.
    ///
    /// `XTASK_HISTORY_DB` overrides the default path, enabling per-session
    /// alternate databases (e.g. synthetic history for exercises).
    pub(crate) fn history_db_path(&self) -> PathBuf {
        if let Ok(path) = env::var("XTASK_HISTORY_DB") {
            return PathBuf::from(path);
        }
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

/// Load `UserPreferences` from `~/.config/xtask/preferences.toml`.
///
/// Silently returns defaults if the file is missing, unreadable, or malformed.
/// This matches the plan's "precedence" contract: the prefs file is the fallback
/// after CLI flags and env vars, and before hardcoded defaults.
pub(crate) fn load_user_preferences() -> UserPreferences {
    let config_dir = dirs::config_dir().unwrap_or_else(|| PathBuf::from("~/.config"));
    load_user_preferences_from(&config_dir)
}

/// Testable core of `load_user_preferences` — reads from an explicit config dir.
pub(crate) fn load_user_preferences_from(config_dir: &std::path::Path) -> UserPreferences {
    let path = config_dir.join("xtask/preferences.toml");
    match std::fs::read_to_string(&path) {
        Ok(contents) => toml::from_str(&contents).unwrap_or_default(),
        Err(_) => UserPreferences::default(),
    }
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

    #[sinex_test]
    async fn test_load_preferences_valid_toml() -> TestResult<()> {
        let dir = tempfile::tempdir()?;
        let prefs_dir = dir.path().join("xtask");
        std::fs::create_dir_all(&prefs_dir)?;
        std::fs::write(
            prefs_dir.join("preferences.toml"),
            "notify_on_completion = true\n",
        )?;

        let prefs = load_user_preferences_from(dir.path());
        assert!(
            prefs.notify_on_completion,
            "should read notify_on_completion = true"
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_load_preferences_missing_file() -> TestResult<()> {
        let dir = tempfile::tempdir()?;
        // No preferences.toml written — should return defaults without panic
        let prefs = load_user_preferences_from(dir.path());
        assert!(
            !prefs.notify_on_completion,
            "missing file should yield default false"
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_load_preferences_malformed_toml() -> TestResult<()> {
        let dir = tempfile::tempdir()?;
        let prefs_dir = dir.path().join("xtask");
        std::fs::create_dir_all(&prefs_dir)?;
        std::fs::write(prefs_dir.join("preferences.toml"), "[[[not valid")?;

        // Malformed TOML — should silently return defaults, not panic
        let prefs = load_user_preferences_from(dir.path());
        assert!(
            !prefs.notify_on_completion,
            "malformed TOML should yield defaults"
        );
        Ok(())
    }
}
