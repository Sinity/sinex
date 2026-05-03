//! Environment-based configuration for xtask.
//!
//! Reads configuration from environment variables exported by the sinex
//! development shell (or compatible manual setup) so xtask and the checkout
//! stay in sync.
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
//! ```
//!
//! ## NixOS home-manager integration (W2)
//!
//! ```nix
//! xdg.configFile."xtask/preferences.toml".text = ''
//!   notify_on_completion = true
//! '';
//! ```

use std::{
    env,
    path::{Path, PathBuf},
};

/// User-managed preferences loaded from `~/.config/xtask/preferences.toml`.
#[derive(Debug, Clone, Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UserPreferences {
    /// Send a desktop notification via `notify-send` when a background job completes (W3).
    #[serde(default)]
    pub notify_on_completion: bool,
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
    /// Toolchain identifier when exported by the development shell
    pub toolchain: Option<String>,
    /// Whether we're inside the sinex development shell
    pub in_dev_shell: bool,
    /// User preferences from `~/.config/xtask/preferences.toml` (W1).
    pub prefs: UserPreferences,
}

impl Config {
    /// Load configuration from environment variables.
    pub(crate) fn from_env() -> Self {
        let workspace_root = workspace_root();
        let repo_state_root = workspace_root.join(".sinex");
        let state_dir = env::var("SINEX_STATE_DIR")
            .map_or_else(|_| repo_state_root.join("state"), PathBuf::from);

        let cache_dir = env::var("SINEX_CACHE_DIR")
            .map_or_else(|_| workspace_cache_root_for(&workspace_root), PathBuf::from);

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
            toolchain: env::var("SINEX_DEV_TOOLCHAIN")
                .ok()
                .or_else(|| env::var("RUSTUP_TOOLCHAIN").ok()),
            in_dev_shell: env::var("SINEX_DEV_ROOT").is_ok() || env::var("IN_NIX_SHELL").is_ok(),
            prefs: load_user_preferences(),
        }
    }

    /// Path to the history database.
    ///
    /// `XTASK_HISTORY_DB` is a test/exercise escape hatch for synthetic
    /// ledgers. Normal developer and observability flows should use the
    /// checkout-scoped canonical DB at `SINEX_STATE_DIR/xtask-history.db`.
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

    /// Directory for preflight cache, hash, and lock state.
    pub(crate) fn preflight_state_dir(&self) -> PathBuf {
        self.state_dir.join("preflight")
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

/// Repo-local state root used for checkout-scoped artifacts and caches.
#[must_use]
pub fn workspace_state_root() -> PathBuf {
    workspace_root().join(".sinex")
}

/// Cargo target directory for this checkout.
#[must_use]
pub fn workspace_target_dir() -> PathBuf {
    workspace_target_dir_for(&workspace_root())
}

/// Cargo target directory for a specific checkout root.
#[must_use]
pub fn workspace_target_dir_for(workspace_root: &Path) -> PathBuf {
    if let Ok(dir) = std::env::var("CARGO_TARGET_DIR") {
        return PathBuf::from(dir);
    }

    workspace_cache_root_for(workspace_root).join("target")
}

/// Cache root for checkout-local build/runtime artifacts.
#[must_use]
pub fn workspace_cache_root_for(workspace_root: &Path) -> PathBuf {
    env::var("SINEX_DEV_CACHE_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| workspace_root.join(".sinex/cache"))
}

/// Global configuration singleton.
#[cfg(not(test))]
static CONFIG: std::sync::LazyLock<Config> = std::sync::LazyLock::new(Config::from_env);

/// Get the current xtask configuration.
///
/// Production xtask processes treat environment-derived configuration as
/// immutable for the process lifetime. Unit tests intentionally mutate
/// environment variables between cases, so they must resolve configuration from
/// the live environment instead of a shared singleton.
#[cfg(not(test))]
pub fn config() -> &'static Config {
    &CONFIG
}

#[cfg(test)]
pub fn config() -> Config {
    Config::from_env()
}

/// Load `UserPreferences` from `~/.config/xtask/preferences.toml`.
///
/// Missing files fall back to defaults. Read/parse failures are surfaced to
/// stderr and also fall back to defaults so xtask remains usable.
pub(crate) fn load_user_preferences() -> UserPreferences {
    let config_dir = dirs::config_dir().unwrap_or_else(|| {
        // dirs::config_dir() returns None only in unusual environments (no $HOME set).
        // Fall back to $HOME/.config rather than the literal "~/.config" path, which
        // the OS would not expand and would fail to locate the file.
        let home = std::env::var_os("HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("/"));
        home.join(".config")
    });
    let path = config_dir.join("xtask/preferences.toml");
    match load_user_preferences_from(&config_dir) {
        Ok(prefs) => prefs,
        Err(error) => {
            eprintln!(
                "[xtask] failed to load user preferences from {}: {error}",
                path.display()
            );
            UserPreferences::default()
        }
    }
}

/// Testable core of `load_user_preferences` — reads from an explicit config dir.
pub(crate) fn load_user_preferences_from(
    config_dir: &std::path::Path,
) -> color_eyre::Result<UserPreferences> {
    use color_eyre::eyre::Context;

    let path = config_dir.join("xtask/preferences.toml");
    match std::fs::read_to_string(&path) {
        Ok(contents) => {
            toml::from_str(&contents).with_context(|| format!("failed to parse {}", path.display()))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            Ok(UserPreferences::default())
        }
        Err(error) => Err(error).with_context(|| format!("failed to read {}", path.display())),
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
/// Prefer the Sinex checkout containing the current working directory. This
/// keeps a globally installed or previously built `xtask` binary bound to the
/// active worktree instead of the checkout where the binary was compiled.
///
/// If the current directory is outside a Sinex checkout, fall back to
/// `xtask`'s compile-time `CARGO_MANIFEST_DIR`. Avoid runtime
/// `CARGO_MANIFEST_DIR`: under nextest it points at the test crate's manifest
/// and can scatter `.sinex/` state into crate subdirectories.
pub fn workspace_root() -> PathBuf {
    if let Ok(cwd) = env::current_dir()
        && let Some(root) = workspace_root_from_current_dir(&cwd)
    {
        return root;
    }

    compiled_workspace_root()
}

fn compiled_workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .map(Path::to_path_buf)
        .expect("xtask crate must have a parent directory (the workspace root)")
}

fn workspace_root_from_current_dir(start: &Path) -> Option<PathBuf> {
    start.ancestors().find_map(|dir| {
        let manifest = dir.join("Cargo.toml");
        let xtask_manifest = dir.join("xtask/Cargo.toml");
        if manifest.is_file() && xtask_manifest.is_file() {
            Some(dir.to_path_buf())
        } else {
            None
        }
    })
}

/// Path to the repo-local ast-grep config root.
pub fn ast_grep_root() -> PathBuf {
    workspace_root().join(".config/ast-grep")
}

/// Path to the repo-local ast-grep config file.
pub fn ast_grep_config_path() -> PathBuf {
    ast_grep_root().join("sgconfig.yml")
}

/// Path to the repo-local ast-grep rules directory.
pub fn ast_grep_rules_dir() -> PathBuf {
    ast_grep_root().join("rules")
}

/// Path to the generated ast-grep rule catalog.
pub fn ast_grep_catalog_path() -> PathBuf {
    ast_grep_root().join("README.md")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::{EnvGuard, sinex_test};

    #[sinex_test]
    async fn test_config_from_env() -> TestResult<()> {
        let config = Config::from_env();
        // Should at least have a hostname
        assert!(!config.hostname.is_empty());
        Ok(())
    }

    #[sinex_test]
    async fn test_history_db_path() -> TestResult<()> {
        let dir = tempfile::tempdir()?;
        let mut env = EnvGuard::with_keys(&["SINEX_STATE_DIR", "XTASK_HISTORY_DB"]);
        env.set("SINEX_STATE_DIR", dir.path());
        env.clear("XTASK_HISTORY_DB");

        let config = Config::from_env();
        let path = config.history_db_path();
        assert_eq!(path, dir.path().join("xtask-history.db"));
        Ok(())
    }

    #[sinex_test]
    async fn test_history_db_path_respects_override() -> TestResult<()> {
        let dir = tempfile::tempdir()?;
        let override_path = dir.path().join("custom-history.db");
        let mut env = EnvGuard::with_keys(&["SINEX_STATE_DIR", "XTASK_HISTORY_DB"]);
        env.set("SINEX_STATE_DIR", dir.path());
        env.set("XTASK_HISTORY_DB", &override_path);

        let config = Config::from_env();
        let path = config.history_db_path();
        assert_eq!(path, override_path);
        Ok(())
    }

    #[sinex_test]
    async fn test_config_cache_dir_respects_sinex_cache_dir() -> TestResult<()> {
        let dir = tempfile::tempdir()?;
        let cache_dir = dir.path().join("explicit-cache");
        let mut env = EnvGuard::with_keys(&["SINEX_CACHE_DIR", "SINEX_DEV_CACHE_ROOT"]);
        env.set("SINEX_CACHE_DIR", &cache_dir);
        env.clear("SINEX_DEV_CACHE_ROOT");

        let config = Config::from_env();
        assert_eq!(config.cache_dir, cache_dir);
        Ok(())
    }

    #[sinex_test]
    async fn test_config_cache_dir_uses_dev_cache_root_without_cache_dir() -> TestResult<()> {
        let dir = tempfile::tempdir()?;
        let cache_root = dir.path().join("dev-cache");
        let mut env = EnvGuard::with_keys(&["SINEX_CACHE_DIR", "SINEX_DEV_CACHE_ROOT"]);
        env.clear("SINEX_CACHE_DIR");
        env.set("SINEX_DEV_CACHE_ROOT", &cache_root);

        let config = Config::from_env();
        assert_eq!(config.cache_dir, cache_root);
        Ok(())
    }

    #[sinex_test]
    async fn test_workspace_target_dir_respects_cargo_target_dir() -> TestResult<()> {
        let dir = tempfile::tempdir()?;
        let target_dir = dir.path().join("target-cache");
        let mut env = EnvGuard::with_keys(&["CARGO_TARGET_DIR"]);
        env.set("CARGO_TARGET_DIR", &target_dir);

        assert_eq!(workspace_target_dir(), target_dir);
        Ok(())
    }

    #[sinex_test]
    async fn test_workspace_cache_root_respects_sinex_dev_cache_root() -> TestResult<()> {
        let workspace = tempfile::tempdir()?;
        let cache_root = workspace.path().join("configured-cache");
        let mut env = EnvGuard::with_keys(&["SINEX_DEV_CACHE_ROOT", "CARGO_TARGET_DIR"]);
        env.set("SINEX_DEV_CACHE_ROOT", &cache_root);
        env.clear("CARGO_TARGET_DIR");

        assert_eq!(workspace_cache_root_for(workspace.path()), cache_root);
        Ok(())
    }

    #[sinex_test]
    async fn test_workspace_cache_root_falls_back_to_checkout_cache() -> TestResult<()> {
        let workspace = tempfile::tempdir()?;
        let mut env = EnvGuard::with_keys(&["SINEX_DEV_CACHE_ROOT"]);
        env.clear("SINEX_DEV_CACHE_ROOT");

        assert_eq!(
            workspace_cache_root_for(workspace.path()),
            workspace.path().join(".sinex/cache")
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_workspace_target_dir_respects_sinex_dev_cache_root() -> TestResult<()> {
        let workspace = tempfile::tempdir()?;
        let cache_root = workspace.path().join("configured-cache");
        let mut env = EnvGuard::with_keys(&["SINEX_DEV_CACHE_ROOT", "CARGO_TARGET_DIR"]);
        env.set("SINEX_DEV_CACHE_ROOT", &cache_root);
        env.clear("CARGO_TARGET_DIR");

        assert_eq!(
            workspace_target_dir_for(workspace.path()),
            cache_root.join("target")
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_workspace_root_discovery_prefers_enclosing_checkout() -> TestResult<()> {
        let checkout = tempfile::tempdir()?;
        std::fs::write(checkout.path().join("Cargo.toml"), "[workspace]\n")?;
        std::fs::create_dir_all(checkout.path().join("xtask/src"))?;
        std::fs::write(
            checkout.path().join("xtask/Cargo.toml"),
            "[package]\nname = \"xtask\"\nversion = \"0.0.0\"\nedition = \"2024\"\n",
        )?;
        std::fs::create_dir_all(checkout.path().join("crate/lib/sinex-primitives"))?;

        let nested = checkout.path().join("crate/lib/sinex-primitives");
        let root = workspace_root_from_current_dir(&nested)
            .expect("nested checkout path should resolve to workspace root");
        assert_eq!(root, checkout.path());
        Ok(())
    }

    #[sinex_test]
    async fn test_workspace_root_discovery_rejects_non_xtask_workspace() -> TestResult<()> {
        let other = tempfile::tempdir_in("/tmp")?;
        std::fs::write(other.path().join("Cargo.toml"), "[workspace]\n")?;

        assert!(
            workspace_root_from_current_dir(other.path()).is_none(),
            "a generic Cargo workspace without xtask/Cargo.toml is not the Sinex root"
        );
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

        let prefs = load_user_preferences_from(dir.path())?;
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
        let prefs = load_user_preferences_from(dir.path())?;
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

        let error = load_user_preferences_from(dir.path())
            .expect_err("malformed TOML should surface a parse error");
        assert!(
            error.to_string().contains("failed to parse"),
            "expected parse context, got: {error}"
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_load_preferences_rejects_unknown_coordinator_section() -> TestResult<()> {
        let dir = tempfile::tempdir()?;
        let prefs_dir = dir.path().join("xtask");
        std::fs::create_dir_all(&prefs_dir)?;
        std::fs::write(
            prefs_dir.join("preferences.toml"),
            "notify_on_completion = true\n\n[coordinator]\nauto_sequence = [\"check -> test\"]\n",
        )?;

        let error = load_user_preferences_from(dir.path())
            .expect_err("schema-only coordinator preferences should not be accepted");
        assert!(
            error.to_string().contains("failed to parse"),
            "expected parse context, got: {error}"
        );
        Ok(())
    }
}
