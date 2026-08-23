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

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

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
    /// Root for per-test temporary directories
    pub test_tmp_dir: Option<PathBuf>,
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
        let state_dir = workspace_state_dir_for(&workspace_root);

        let cache_dir = workspace_pinned_env_path("SINEX_CACHE_DIR", &workspace_root, || {
            workspace_cache_root_for(&workspace_root)
        });

        let hostname = gethostname::gethostname().to_string_lossy().into_owned();

        Self {
            database_url: env::var("DATABASE_URL").ok(),
            nats_url: env::var("SINEX_NATS_URL").ok(),
            gateway_url: env::var("SINEX_API_URL")
                .ok()
                .or_else(|| env::var("SINEX_API_URL").ok())
                .or_else(|| {
                    env::var("SINEX_API_TCP_LISTEN")
                        .ok()
                        .map(|listen| format!("https://{listen}"))
                }),
            state_dir,
            cache_dir,
            test_results_dir: workspace_pinned_env_path_opt(
                "SINEX_TEST_RESULTS_DIR",
                &workspace_root,
            ),
            test_tmp_dir: workspace_pinned_env_path_opt("SINEX_TEST_TMPDIR", &workspace_root),
            hostname,
            toolchain: env::var("SINEX_DEV_TOOLCHAIN")
                .ok()
                .or_else(|| env::var("RUSTUP_TOOLCHAIN").ok()),
            in_dev_shell: env::var("SINEX_DEV_ROOT").is_ok() || env::var("IN_NIX_SHELL").is_ok(),
            prefs: load_user_preferences(),
        }
    }

    /// Path to the canonical development-loop history database.
    ///
    /// This database is durable observability data, not cache. It records
    /// command timings, diagnostics, test outcomes, job output, and resource
    /// evidence used to optimize the development environment. Performance
    /// cleanup must not delete it; if it becomes a bottleneck, preserve the
    /// dataset and optimize access patterns, indexes, WAL/compaction behavior,
    /// or explicit archive flows.
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

/// Durable state directory for this checkout.
///
/// `SINEX_STATE_DIR` is honored for explicit test/sandbox overrides, but not
/// when it points at the sinnix dev-cache `dev-state/state` tree. That tree is
/// for disposable runtime scratch; accepting it for xtask history repeatedly
/// forked the durable history DB away from `.sinex/state`.
#[must_use]
pub fn workspace_state_dir_for(workspace_root: &Path) -> PathBuf {
    let candidate = workspace_pinned_env_path("SINEX_STATE_DIR", workspace_root, || {
        workspace_root.join(".sinex/state")
    });
    if is_sinnix_dev_cache_state_dir(&candidate) {
        workspace_root.join(".sinex/state")
    } else {
        candidate
    }
}

/// Cargo target directory for this checkout.
#[must_use]
pub fn workspace_target_dir() -> PathBuf {
    workspace_target_dir_for(&workspace_root())
}

/// Cargo target directory for a specific checkout root.
///
/// When the inherited `CARGO_TARGET_DIR` is foreign to `workspace_root` — either
/// because it lives inside a different sinex checkout or because it is a
/// `/var/cache/sinex/<user>/<hash>/...` path whose hash does not match the active
/// workspace — the value is overridden with the worktree-correct target dir and a
/// prominent warning is emitted to stderr. The warning fires at most once per xtask
/// process (guarded by [`FOREIGN_TARGET_DIR_WARNED`]) so it does not spam on
/// repeated cargo invocations.
///
/// An arbitrary user-set path (e.g. `/tmp/custom`) that is neither a
/// `/var/cache/sinex/<hash>` shape nor inside another checkout is respected verbatim.
#[must_use]
pub fn workspace_target_dir_for(workspace_root: &Path) -> PathBuf {
    if let Ok(dir) = std::env::var("CARGO_TARGET_DIR") {
        let candidate = PathBuf::from(&dir);
        if is_foreign_sinex_cache_path(&candidate, workspace_root)
            || path_belongs_to_other_checkout(&candidate, workspace_root)
        {
            let corrected = workspace_cache_root_for(workspace_root).join("target");
            FOREIGN_TARGET_DIR_WARNED.get_or_init(|| {
                eprintln!(
                    "[xtask] WARNING: CARGO_TARGET_DIR={dir} belongs to a different checkout \
                     (hash mismatch); using {} to avoid validating the wrong tree",
                    corrected.display()
                );
            });
            return corrected;
        }
        return candidate;
    }

    workspace_cache_root_for(workspace_root).join("target")
}

/// Fired at most once per xtask process when an inherited `CARGO_TARGET_DIR` is
/// detected as foreign and overridden. Prevents duplicate warnings on repeated
/// cargo invocations within a single xtask run.
static FOREIGN_TARGET_DIR_WARNED: std::sync::OnceLock<()> = std::sync::OnceLock::new();

/// Return `true` when `path` appears to be a sinnix-managed cache dir for a
/// **different** workspace — that is, a path of the form
/// `/var/cache/sinex/<user>/<hash>/...` where `<hash>` does not match the
/// SHA-256-derived [`workspace_hash`] of `workspace_root`.
///
/// The check is deliberately narrow: a path that does not contain the
/// `/var/cache/sinex/<user>/<hash>/` prefix is never considered foreign by this
/// predicate, even if it is unusual or user-defined.
fn is_foreign_sinex_cache_path(path: &Path, workspace_root: &Path) -> bool {
    let components: Vec<_> = path
        .components()
        .map(|c| c.as_os_str().to_owned())
        .collect();

    // Locate the /var/cache/sinex/ triple in the component sequence.
    let Some(var_idx) = components
        .windows(3)
        .position(|w| w[0] == "var" && w[1] == "cache" && w[2] == "sinex")
    else {
        return false; // Not a sinnix cache path — leave it alone.
    };

    // After /var/cache/sinex/ the shape is: <user> (var_idx+3) then <hash> (var_idx+4).
    let hash_idx = var_idx + 4;
    let Some(path_hash) = components.get(hash_idx) else {
        return false; // Truncated path with no hash segment — treat as non-matching.
    };

    let expected = workspace_hash(workspace_root);
    path_hash.to_string_lossy() != expected.as_str()
}

/// Cache root for checkout-local build/runtime artifacts.
///
/// Honors `SINEX_DEV_CACHE_ROOT` only when the configured path does not point
/// inside a different sinex checkout — see [`workspace_pinned_env_path`] for
/// the worktree-isolation rationale.
#[must_use]
pub fn workspace_cache_root_for(workspace_root: &Path) -> PathBuf {
    workspace_pinned_env_path("SINEX_DEV_CACHE_ROOT", workspace_root, || {
        workspace_root.join(".sinex/cache")
    })
}

/// Checkout-scoped tmpfs directory when a sticky, writable `/dev/shm` has enough headroom.
#[must_use]
pub fn workspace_tmpfs_dir(prefix: &str, min_free_mb: f64) -> Option<PathBuf> {
    let shm = Path::new("/dev/shm");
    if !usable_sticky_tmpfs(shm, min_free_mb) {
        return None;
    }
    let user = env::var("USER").unwrap_or_else(|_| "user".to_string());
    let hash = workspace_hash(&workspace_root());
    Some(shm.join(format!("{prefix}-{user}-{hash}")))
}

#[cfg(unix)]
fn usable_sticky_tmpfs(path: &Path, min_free_mb: f64) -> bool {
    let Ok(metadata) = std::fs::metadata(path) else {
        return false;
    };
    if !metadata.is_dir() {
        return false;
    }
    let mode = metadata.permissions().mode();
    if mode & 0o1000 == 0 || mode & 0o222 == 0 {
        return false;
    }
    crate::process::shm_usage_mb().is_some_and(|(_, free_mb)| free_mb >= min_free_mb)
}

#[cfg(not(unix))]
fn usable_sticky_tmpfs(_path: &Path, _min_free_mb: f64) -> bool {
    false
}

pub(crate) fn workspace_hash(workspace_root: &Path) -> String {
    use sha2::{Digest, Sha256};

    let mut hasher = Sha256::new();
    hasher.update(workspace_root.as_os_str().as_encoded_bytes());
    let digest = hasher.finalize();
    let mut out = String::with_capacity(12);
    for byte in &digest[..6] {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
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
pub fn config() -> &'static Config {
    // Tests mutate env between cases, so resolve fresh on each call rather than
    // caching a singleton. Leak to match the non-test `&'static` signature: this
    // keeps `config()` monomorphic across cfgs (callers never branch on owned-vs-
    // borrowed), and the leak is bounded — one small `Config` per call in a
    // short-lived test process.
    Box::leak(Box::new(Config::from_env()))
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
        let home = std::env::var_os("HOME").map_or_else(|| PathBuf::from("/"), PathBuf::from);
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
/// Resolution order:
/// 1. `git rev-parse --show-toplevel` — authoritative for worktrees. Catches
///    the case where the current directory is the worktree root but the
///    inherited environment from the parent shell was set up for a different
///    checkout.
/// 2. Walk upward from the current working directory looking for the sinex
///    workspace markers (`Cargo.toml` + `xtask/Cargo.toml`).
/// 3. `xtask`'s compile-time `CARGO_MANIFEST_DIR` parent. Avoid runtime
///    `CARGO_MANIFEST_DIR`: under nextest it points at the test crate's
///    manifest and can scatter `.sinex/` state into crate subdirectories.
pub fn workspace_root() -> PathBuf {
    if let Some(root) = git_worktree_workspace_root() {
        return root;
    }

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

/// Ask git for the active worktree's top-level directory.
///
/// Returns `None` if git is unavailable, the cwd is not inside a git
/// repository, or the resolved path lacks the sinex workspace markers
/// (`Cargo.toml` + `xtask/Cargo.toml`). The marker check guards against
/// returning the toplevel of an unrelated git repository when `xtask` is
/// invoked from outside a sinex checkout but happens to share an ancestor
/// directory.
fn git_worktree_workspace_root() -> Option<PathBuf> {
    let output = std::process::Command::new("git")
        .args(["rev-parse", "--show-toplevel"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let stdout = String::from_utf8(output.stdout).ok()?;
    let path = PathBuf::from(stdout.trim());
    if path.as_os_str().is_empty() {
        return None;
    }
    if path.join("Cargo.toml").is_file() && path.join("xtask/Cargo.toml").is_file() {
        Some(path)
    } else {
        None
    }
}

/// Return `true` if `path` resolves inside a sinex checkout that is **not**
/// the active `workspace_root`.
///
/// Used to detect environment variables inherited from a parent shell that
/// was set up for a different checkout. Such env vars must be ignored — they
/// would otherwise route state writes from a worktree back into the main
/// checkout's `.sinex/`, defeating worktree isolation.
fn path_belongs_to_other_checkout(path: &Path, workspace_root: &Path) -> bool {
    let Some(other_root) = workspace_root_from_current_dir(path) else {
        return false;
    };
    other_root != workspace_root
}

/// Read a path-valued env var, ignoring values that point inside a different
/// sinex checkout or that carry a sinnix cache-hash segment belonging to a
/// different workspace. Falls back to `fallback()` when the env var is unset or
/// is foreign to `workspace_root`.
pub(crate) fn workspace_pinned_env_path<F>(var: &str, workspace_root: &Path, fallback: F) -> PathBuf
where
    F: FnOnce() -> PathBuf,
{
    match env::var(var) {
        Ok(raw) => {
            let candidate = PathBuf::from(raw);
            if path_belongs_to_other_checkout(&candidate, workspace_root)
                || is_foreign_sinex_cache_path(&candidate, workspace_root)
            {
                fallback()
            } else {
                candidate
            }
        }
        Err(_) => fallback(),
    }
}

fn is_sinnix_dev_cache_state_dir(path: &Path) -> bool {
    let components: Vec<_> = path
        .components()
        .map(|component| component.as_os_str())
        .collect();

    components
        .windows(2)
        .any(|window| window[0] == "dev-state" && window[1] == "state")
        && components
            .windows(3)
            .any(|window| window[0] == "var" && window[1] == "cache" && window[2] == "sinex")
}

/// Optional variant of [`workspace_pinned_env_path`] — returns `None` when the
/// env var is unset, while still rejecting cross-checkout values.
fn workspace_pinned_env_path_opt(var: &str, workspace_root: &Path) -> Option<PathBuf> {
    let raw = env::var(var).ok()?;
    let candidate = PathBuf::from(raw);
    if path_belongs_to_other_checkout(&candidate, workspace_root) {
        None
    } else {
        Some(candidate)
    }
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
#[path = "config_test.rs"]
mod tests;
