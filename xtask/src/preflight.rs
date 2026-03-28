//! Preflight checks and automatic setup for xtask commands.
//!
//! This module provides infrastructure readiness checks and lazy-start capabilities.
//! Commands that need Postgres, NATS, TLS, or declarative schema apply can call `ensure_ready()`
//! to prompt the user and set up infrastructure automatically.
//!
//! ## Preflight Result Cache
//!
//! After a successful preflight, the result is cached in `{state_dir()}/preflight-cache.json`.
//! Subsequent invocations within the TTL window skip preflight entirely if nothing relevant
//! changed: schema files, contract payload files, or the git HEAD commit.
//!
//! Cache is invalidated by:
//! - TTL expiry (60 seconds, configurable via `SINEX_PREFLIGHT_TTL_SECS`)
//! - Declarative schema source changes (detected via blake3 hash)
//! - Contract payload file content changes (detected via blake3 hash)
//! - Git HEAD commit change (new commit or branch switch)
//! - `xtask infra reset` (deletes the entire `.sinex/preflight/` directory)
//! - `xtask reset --yes --db` (explicitly invalidates the cache)

use color_eyre::eyre::{Result, WrapErr, bail, eyre};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::tools::{ToolInfo, ToolManager};

/// Spawn a watchdog thread that prints a "still waiting..." message every `interval` seconds.
/// Returns a handle that stops the watchdog when dropped.
fn spawn_watchdog(label: &str, interval_secs: u64) -> WatchdogGuard {
    let done = Arc::new(AtomicBool::new(false));
    let done_clone = done.clone();
    let label = label.to_string();
    std::thread::spawn(move || {
        let mut elapsed = 0u64;
        loop {
            std::thread::sleep(std::time::Duration::from_secs(1));
            if done_clone.load(Ordering::Relaxed) {
                break;
            }
            elapsed += 1;
            if elapsed.is_multiple_of(interval_secs) {
                eprintln!("  ⏳ {label}... still waiting ({elapsed}s)");
            }
        }
    });
    WatchdogGuard(done)
}

struct WatchdogGuard(Arc<AtomicBool>);

impl Drop for WatchdogGuard {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

/// Check if Postgres is available.
#[must_use]
pub fn is_postgres_ready() -> bool {
    crate::infra::probe::probe_postgres().ready()
}

/// Check if NATS is available on the configured port.
#[must_use]
pub fn is_nats_ready() -> bool {
    crate::infra::probe::probe_nats().ready()
}

/// Check if TLS certificates exist in `.sinex/tls/`.
#[must_use]
pub fn tls_certs_exist() -> bool {
    let tls_dir = std::path::Path::new(".sinex/tls");
    tls_dir.join("ca.pem").exists()
        && tls_dir.join("server.pem").exists()
        && tls_dir.join("client.pem").exists()
}

/// Get the state directory for caching preflight state.
fn state_dir() -> std::path::PathBuf {
    let crate_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    crate_dir.join("../.sinex/preflight")
}

/// Path to the preflight result cache file.
fn cache_path() -> std::path::PathBuf {
    state_dir().join("preflight-cache.json")
}

/// Default TTL for the preflight cache in seconds.
const PREFLIGHT_CACHE_DEFAULT_TTL_SECS: u64 = 60;

fn compiled_contracts_hash() -> &'static str {
    match option_env!("SINEX_XTASK_BUILD_CONTRACTS_HASH") {
        Some(hash) => hash,
        None => "unknown",
    }
}

fn write_state_file_atomically(path: &std::path::Path, contents: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).wrap_err_with(|| {
            format!(
                "failed to create preflight state directory {}",
                parent.display()
            )
        })?;
    }

    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, contents)
        .wrap_err_with(|| format!("failed to write temp state file {}", tmp.display()))?;

    if let Err(error) = std::fs::rename(&tmp, path) {
        if let Err(cleanup_error) = std::fs::remove_file(&tmp)
            && cleanup_error.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!(
                path = %tmp.display(),
                error = %cleanup_error,
                "failed to clean up temp preflight state file after rename failure"
            );
        }
        return Err(error).wrap_err_with(|| {
            format!(
                "failed to atomically replace preflight state file {}",
                path.display()
            )
        });
    }

    Ok(())
}

/// Preflight result cache — persisted as JSON after a successful preflight run.
///
/// All four fields must match for the cache to be considered valid:
/// - `timestamp_secs`: unix timestamp of last successful preflight (for TTL check)
/// - `schema_hash`: blake3 hash of declarative schema source contents
/// - `contracts_hash`: blake3 hash of all event payload source file contents
/// - `git_head`: current git HEAD commit SHA (or symref target for detached HEAD)
#[derive(Debug, Clone, Serialize, Deserialize)]
struct PreflightCache {
    /// Unix timestamp (seconds) when this cache entry was written.
    timestamp_secs: u64,
    /// blake3 hash of declarative schema source contents.
    schema_hash: String,
    /// blake3 hash of contract payload source file contents.
    contracts_hash: String,
    /// Git HEAD commit SHA or symbolic ref target.
    git_head: String,
}

impl PreflightCache {
    /// Load the cache from disk. Returns `None` if absent.
    fn load() -> Result<Option<Self>> {
        load_preflight_cache_from(&cache_path())
    }

    /// Write this cache entry to disk atomically (R1 fix). Non-fatal on failure (best-effort).
    ///
    /// Uses a temp file + rename to avoid partial writes being read by concurrent xtask
    /// processes — `fs::write` is not atomic; rename within the same filesystem is.
    fn save(&self) {
        let path = cache_path();
        let json = match serde_json::to_string_pretty(self) {
            Ok(j) => j,
            Err(e) => {
                tracing::debug!("preflight cache: failed to serialize: {e}");
                return;
            }
        };
        if let Err(error) = write_state_file_atomically(&path, &json) {
            tracing::warn!(
                path = %path.display(),
                error = %error,
                "preflight cache: failed to persist cache entry"
            );
        }
    }

    /// Build a fresh cache entry using the current state.
    fn current() -> Result<Self> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Ok(Self {
            timestamp_secs: now,
            schema_hash: hash_schema_sources()?,
            contracts_hash: hash_contracts_dir()?,
            git_head: read_git_head()?,
        })
    }

    /// Check whether this cache entry is still valid.
    ///
    /// Returns `true` (cache hit) if all of the following hold:
    /// - Age is within `ttl_secs` seconds
    /// - Declarative schema hash matches current files
    /// - Contracts hash matches current files
    /// - Git HEAD hasn't changed
    fn is_valid(&self, ttl_secs: u64) -> Result<bool> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let age = now.saturating_sub(self.timestamp_secs);
        if age >= ttl_secs {
            tracing::debug!("preflight cache: expired (age={age}s, ttl={ttl_secs}s)");
            return Ok(false);
        }

        let schema_hash = hash_schema_sources()?;
        if self.schema_hash != schema_hash {
            tracing::debug!(
                "preflight cache: schema hash mismatch (cached={}, current={})",
                self.schema_hash,
                schema_hash
            );
            return Ok(false);
        }

        let contracts_hash = hash_contracts_dir()?;
        if self.contracts_hash != contracts_hash {
            tracing::debug!(
                "preflight cache: contracts hash mismatch (cached={}, current={})",
                self.contracts_hash,
                contracts_hash
            );
            return Ok(false);
        }

        let git_head = read_git_head()?;
        if self.git_head != git_head {
            tracing::debug!(
                "preflight cache: git HEAD changed (cached={}, current={})",
                self.git_head,
                git_head
            );
            return Ok(false);
        }

        tracing::debug!("preflight cache: hit (age={age}s)");
        Ok(true)
    }
}

fn load_preflight_cache_from(path: &Path) -> Result<Option<PreflightCache>> {
    let contents = match std::fs::read_to_string(path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error)
                .wrap_err_with(|| format!("failed to read preflight cache {}", path.display()));
        }
    };

    serde_json::from_str(&contents)
        .wrap_err_with(|| format!("failed to parse preflight cache {}", path.display()))
        .map(Some)
}

/// Read the current git HEAD commit SHA without spawning a subprocess.
///
/// Reads `.git/HEAD` directly. If it contains a symbolic ref (`ref: refs/heads/main`),
/// resolves it to the commit SHA by reading the corresponding packed or loose ref.
/// Falls back to the symref string itself if the commit file doesn't exist yet
/// (e.g., initial empty repo).
fn read_git_head() -> Result<String> {
    read_git_head_for_root(&crate::config::workspace_root())
}

fn read_git_head_for_root(workspace_root: &Path) -> Result<String> {
    let git_dir = workspace_root.join(".git");
    let head_path = git_dir.join("HEAD");

    let head_contents = std::fs::read_to_string(&head_path)
        .wrap_err_with(|| format!("failed to read git HEAD at {}", head_path.display()))?
        .trim()
        .to_string();

    // Symbolic ref: "ref: refs/heads/main"
    if let Some(refname) = head_contents.strip_prefix("ref: ") {
        let ref_path = git_dir.join(refname);
        // Try loose ref first
        if let Ok(sha) = std::fs::read_to_string(&ref_path) {
            return Ok(sha.trim().to_string());
        }
        // Try packed-refs
        let packed_refs = git_dir.join("packed-refs");
        match std::fs::read_to_string(&packed_refs) {
            Ok(contents) => {
                for line in contents.lines() {
                    if line.starts_with('#') {
                        continue;
                    }
                    let parts: Vec<&str> = line.splitn(2, ' ').collect();
                    if parts.len() == 2 && parts[1] == refname {
                        return Ok(parts[0].to_string());
                    }
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error).wrap_err_with(|| {
                    format!("failed to read packed refs at {}", packed_refs.display())
                });
            }
        }
        // Ref exists but no commit yet (empty branch)
        return Ok(head_contents);
    }

    // Detached HEAD: the content is the commit SHA directly
    Ok(head_contents)
}

/// Compute a blake3 hash of declarative schema sources.
///
/// Hashes file contents in `sinex-schema/src/schema/**` plus `apply.rs`,
/// sorted by filename for deterministic ordering.
/// Returns a hex string. Returns `"empty"` if no files were found.
fn hash_schema_sources() -> Result<String> {
    let crate_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let schema_dir = crate_dir.join("../crate/lib/sinex-schema/src/schema");
    let apply_file = crate_dir.join("../crate/lib/sinex-schema/src/apply.rs");
    let registry_file = crate_dir.join("../crate/lib/sinex-schema/src/schema_registry.rs");

    let mut file_contents = collect_rust_sources_from_dir(&schema_dir, "schema")
        .wrap_err("failed to collect declarative schema sources")?;

    for (name, path) in [
        ("apply.rs", apply_file.as_path()),
        ("schema_registry.rs", registry_file.as_path()),
    ] {
        file_contents.insert(
            name.to_string(),
            std::fs::read(path)
                .wrap_err_with(|| format!("failed to read schema source {}", path.display()))?,
        );
    }

    Ok(hash_named_sources(&file_contents))
}

/// Compute a blake3 hash of the event payload contracts directory contents.
///
/// Hashes file *contents* sorted by filename for deterministic ordering.
/// Returns a hex string. Returns `"empty"` if the directory doesn't exist or
/// contains no `.rs` files.
fn hash_contracts_dir() -> Result<String> {
    let crate_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let payloads_dir = crate_dir.join("../crate/lib/sinex-primitives/src/events/payloads");
    hash_contracts_dir_from(&payloads_dir)
}

fn hash_contracts_dir_from(payloads_dir: &Path) -> Result<String> {
    if !payloads_dir.exists() {
        return Ok("empty".to_string());
    }

    let file_contents = collect_rust_sources_from_dir(payloads_dir, "")
        .wrap_err("failed to collect event payload contracts")?;
    if file_contents.is_empty() {
        return Ok("empty".to_string());
    }

    Ok(hash_named_sources(&file_contents))
}

fn collect_rust_sources_from_dir(
    dir: &Path,
    prefix: &str,
) -> Result<std::collections::BTreeMap<String, Vec<u8>>> {
    let mut file_contents = std::collections::BTreeMap::new();
    let entries =
        std::fs::read_dir(dir).wrap_err_with(|| format!("failed to read {}", dir.display()))?;

    for entry in entries {
        let entry = entry.wrap_err_with(|| format!("failed to enumerate {}", dir.display()))?;
        let name = entry.file_name().to_string_lossy().to_string();
        if !name.ends_with(".rs") {
            continue;
        }

        let path = entry.path();
        let contents = std::fs::read(&path)
            .wrap_err_with(|| format!("failed to read source file {}", path.display()))?;
        let key = if prefix.is_empty() {
            name
        } else {
            format!("{prefix}/{name}")
        };
        file_contents.insert(key, contents);
    }

    Ok(file_contents)
}

fn hash_named_sources(file_contents: &std::collections::BTreeMap<String, Vec<u8>>) -> String {
    if file_contents.is_empty() {
        return "empty".to_string();
    }

    let mut hasher = blake3::Hasher::new();
    for (name, contents) in file_contents {
        hasher.update(name.as_bytes());
        hasher.update(b"\0");
        hasher.update(contents);
        hasher.update(b"\0");
    }
    hasher.finalize().to_hex()[..16].to_string()
}

/// Delete the preflight result cache file, forcing a full preflight on the next run.
///
/// Called by `xtask reset --yes --db` after dropping and recreating the database.
/// `xtask infra reset` achieves the same by deleting the entire `.sinex/preflight/` directory.
pub fn invalidate_cache() {
    let path = cache_path();
    if path.exists() {
        if let Err(e) = std::fs::remove_file(&path) {
            tracing::debug!("preflight cache: failed to remove {}: {e}", path.display());
        } else {
            tracing::debug!("preflight cache: invalidated");
        }
    }
}

/// Check whether declarative schema sources changed since last apply.
#[must_use]
pub fn schema_changed_since_last_apply() -> bool {
    schema_changed_since_last_apply_result().unwrap_or_else(|error| {
        tracing::warn!(error = %error, "failed to inspect schema apply state; treating schema as changed");
        true
    })
}

fn schema_changed_since_last_apply_result() -> Result<bool> {
    let state_dir = state_dir();
    let hash_file = state_dir.join("schema-apply-hash.txt");

    let current_hash = hash_schema_sources()?;
    let cached_hash = read_optional_state_file(&hash_file, "schema apply hash")?;

    Ok(cached_hash.as_deref() != Some(&current_hash))
}

/// Record that declarative schema apply completed for current source state.
///
/// Uses atomic rename (R2 fix) so concurrent readers never see a partial write.
pub fn record_schema_applied() {
    let state_dir = state_dir();
    let hash_file = state_dir.join("schema-apply-hash.txt");
    let current_hash = match hash_schema_sources() {
        Ok(hash) => hash,
        Err(error) => {
            tracing::warn!(error = %error, "failed to compute schema hash after apply");
            return;
        }
    };
    if let Err(error) = write_state_file_atomically(&hash_file, &current_hash) {
        tracing::warn!(
            path = %hash_file.display(),
            error = %error,
            "failed to record declarative schema apply state"
        );
    }
}

/// Check for pending declarative schema apply work.
///
/// Uses:
/// 1. Schema hash state (source changed since last apply)
/// 2. Lightweight DB probes for required core objects
pub fn has_pending_schema_apply() -> Result<bool> {
    // Only check if Postgres is already running
    if !is_postgres_ready() {
        return Ok(false);
    }

    // Get config for database connection
    let config: crate::infra::stack::StackConfig =
        match crate::infra::stack::StackConfig::for_current_checkout() {
            Ok(c) => c,
            Err(error) => {
                return Err(error).wrap_err("failed to load stack config for schema readiness");
            }
        };

    // Strategy 1: hash changed since last apply
    if schema_changed_since_last_apply_result()? {
        return Ok(true);
    }

    // Strategy 2: check core declarative objects exist
    let output = std::process::Command::new("psql")
        .env("PGHOST", config.run_dir())
        .env("PGPORT", config.postgres.port.to_string())
        .env("PGUSER", &config.postgres.user)
        .env("PGDATABASE", &config.postgres.database)
        .args([
            "-tAc",
            "SELECT CASE
                 WHEN to_regclass('core.events') IS NULL THEN 1
                 WHEN to_regclass('core.operations_log') IS NULL THEN 1
                 WHEN NOT EXISTS (
                     SELECT 1 FROM information_schema.columns
                     WHERE table_schema='core' AND table_name='events' AND column_name='ts_persisted'
                 ) THEN 1
                 ELSE 0
             END",
        ])
        .output();

    match output {
        Ok(out) if out.status.success() => parse_schema_apply_probe_output(&out),
        Ok(out) => {
            tracing::warn!(
                error = %summarize_command_output(&out),
                "schema readiness probe failed; treating schema as pending"
            );
            Ok(true)
        }
        Err(error) => {
            tracing::warn!(
                error = %error,
                "failed to run schema readiness probe; treating schema as pending"
            );
            Ok(true)
        }
    }
}

fn parse_schema_apply_probe_output(output: &std::process::Output) -> Result<bool> {
    let pending_flag = String::from_utf8_lossy(&output.stdout)
        .trim()
        .parse::<i32>()
        .wrap_err_with(|| {
            format!(
                "schema readiness probe returned invalid output: {}",
                summarize_command_output(output)
            )
        })?;
    Ok(pending_flag != 0)
}

/// Infrastructure status for preflight checks.
#[derive(Debug)]
pub struct InfraStatus {
    pub postgres: bool,
    pub nats: bool,
    pub tls: bool,
    pub schema_apply_pending: bool,
}

impl InfraStatus {
    /// Capture current infrastructure status.
    #[must_use]
    pub fn capture() -> Self {
        Self {
            postgres: is_postgres_ready(),
            nats: is_nats_ready(),
            tls: tls_certs_exist(),
            schema_apply_pending: match has_pending_schema_apply() {
                Ok(pending) => pending,
                Err(error) => {
                    tracing::warn!(
                        error = %error,
                        "failed to capture schema readiness; treating schema apply as pending"
                    );
                    true
                }
            },
        }
    }

    /// Check if all infrastructure is ready.
    #[must_use]
    pub fn all_ready(&self) -> bool {
        self.postgres && self.nats && !self.schema_apply_pending
    }

    /// Check if stack (Postgres + NATS) is running.
    #[must_use]
    pub fn stack_running(&self) -> bool {
        self.postgres && self.nats
    }
}

/// Auto-start stack if not running.
///
/// Returns Ok(true) if stack is now running, Ok(false) if start failed.
/// No prompts - just auto-starts. This is agent-friendly.
///
/// Kills the subprocess if it runs longer than `SINEX_INFRA_START_TIMEOUT`
/// seconds (default: 120s) to prevent indefinite hangs.
pub fn auto_start_stack(verbose: bool) -> Result<()> {
    let status = InfraStatus::capture();

    if status.stack_running() {
        return Ok(());
    }

    // Always report what we're starting (even in quiet mode)
    if !status.postgres && !status.nats {
        eprintln!("⚡ Auto-starting stack (Postgres + NATS)...");
    } else if !status.postgres {
        eprintln!("⚡ Auto-starting Postgres...");
    } else {
        eprintln!("⚡ Auto-starting NATS...");
    }

    let timeout_secs = std::env::var("SINEX_INFRA_START_TIMEOUT")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(120);

    let start = std::time::Instant::now();
    let _watchdog = spawn_watchdog("Starting stack", 5);

    let mut child = match std::process::Command::new("xtask")
        .args(["infra", "start"])
        .stdout(if verbose {
            std::process::Stdio::inherit()
        } else {
            std::process::Stdio::null()
        })
        .stderr(std::process::Stdio::inherit())
        .spawn()
    {
        Ok(c) => c,
        Err(e) => {
            eprintln!("✗ Failed to start stack: {e}");
            return Err(eyre!("failed to spawn infra start: {e}"));
        }
    };

    let pid = child.id();
    let timeout = std::time::Duration::from_secs(timeout_secs);

    // Spawn a watchdog that kills the child if it runs too long.
    // Uses a channel to signal early exit when the child finishes before timeout.
    let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
    std::thread::spawn(move || {
        if done_rx.recv_timeout(timeout).is_err() {
            // Timeout — kill the process group
            eprintln!("✗ Stack start timed out after {timeout_secs}s — killing subprocess");
            unsafe {
                libc::kill(-(pid as i32), libc::SIGTERM);
            }
            std::thread::sleep(std::time::Duration::from_secs(2));
            unsafe {
                libc::kill(-(pid as i32), libc::SIGKILL);
            }
        }
    });

    let exit_status = child.wait();
    let _ = done_tx.send(()); // Signal watchdog: we're done

    let elapsed = start.elapsed();
    match exit_status {
        Ok(exit) if exit.success() => {
            eprintln!("✓ Stack started ({:.1}s)", elapsed.as_secs_f64());
            Ok(())
        }
        Ok(status) => {
            eprintln!("✗ Failed to start stack ({:.1}s)", elapsed.as_secs_f64());
            Err(eyre!("infra start exited with {status}"))
        }
        Err(e) => {
            eprintln!("✗ Failed to start stack: {e}");
            Err(e).wrap_err("failed to wait for infra start")
        }
    }
}

/// Generate TLS certificates if they don't exist and set environment variables.
pub fn ensure_tls_certs(is_interactive: bool) -> Result<()> {
    let tls_dir = std::path::Path::new(".sinex/tls");

    if !tls_certs_exist() {
        if is_interactive {
            eprintln!("Generating development TLS certificates...");
        }

        // Call TLS generation directly instead of spawning subprocess
        let config = crate::tls::CertConfig {
            output_dir: tls_dir.to_path_buf(),
            san: vec!["localhost".to_string(), "127.0.0.1".to_string()],
            ca_name: "Sinex Dev CA".to_string(),
            validity_days: crate::tls::DEFAULT_DEV_CERT_VALIDITY_DAYS,
            force: false,
        };
        crate::tls::generate_dev_certs(&config)?;

        if is_interactive {
            eprintln!("✓ TLS certificates generated");
        }
    }

    // Auto-set TLS environment variables for gateway if not already set
    set_tls_env_if_missing(tls_dir);

    // Auto-set dev RPC token if not already set (for gateway auth)
    set_dev_token_if_missing();

    Ok(())
}

/// Set a development RPC token if not already set.
/// This allows `xtask run gateway` to work without manual token setup.
/// Only sets the token in non-production environments.
fn set_dev_token_if_missing() {
    // Don't auto-set in production
    if std::env::var("SINEX_ENVIRONMENT")
        .ok()
        .is_some_and(|e| e == "production")
    {
        return;
    }

    // Check if any token source is already set
    let has_token = std::env::var("SINEX_RPC_TOKEN").is_ok()
        || std::env::var("SINEX_RPC_TOKEN_FILE").is_ok()
        || std::env::var("SINEX_GATEWAY_ADMIN_TOKEN_FILE").is_ok();

    if !has_token {
        // Generate a deterministic dev token based on hostname (for consistency across runs)
        // but still unique enough that it's clearly a dev token
        let hostname = gethostname::gethostname().to_string_lossy().to_string();
        let dev_token = format!("dev-token-{hostname}");
        unsafe { std::env::set_var("SINEX_RPC_TOKEN", &dev_token) };
        eprintln!("⚡ Auto-set SINEX_RPC_TOKEN={dev_token} (dev mode)");
    }
}

/// Set TLS environment variables if they're not already set.
/// This allows `xtask run gateway` to work without manually setting TLS env vars.
fn set_tls_env_if_missing(tls_dir: &std::path::Path) {
    let cert_path = tls_dir.join("server.pem");
    let key_path = tls_dir.join("server-key.pem");
    if cert_path.exists() && key_path.exists() {
        if std::env::var("SINEX_GATEWAY_TLS_CERT").is_err() {
            unsafe { std::env::set_var("SINEX_GATEWAY_TLS_CERT", &cert_path) };
        }
        if std::env::var("SINEX_GATEWAY_TLS_KEY").is_err() {
            unsafe { std::env::set_var("SINEX_GATEWAY_TLS_KEY", &key_path) };
        }
    }
}

/// Auto-apply pending declarative schema.
///
/// Calls `sinex_db::apply_schema_for_url()` in-process via `block_in_place`.
///
/// On success, records the current schema source hash to prevent
/// unnecessary re-runs.
///
/// **Serialization:** acquires an exclusive flock on `{state_dir}/schema-apply.lock`
/// before running. If the lock is already held (another apply in progress),
/// skips with an informational message — the lock holder will complete the apply.
fn auto_apply_schema(verbose: bool) -> Result<bool> {
    // Serialize concurrent schema-apply runs. If another process is already applying,
    // skip — it will complete the work. Use LOCK_NB (non-blocking) so we never wait.
    let lock_path = state_dir().join("schema-apply.lock");
    let _ = std::fs::create_dir_all(state_dir());
    let lock_file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(&lock_path);

    let lock_file = match lock_file {
        Ok(f) => f,
        Err(e) => {
            eprintln!("⚠️  Could not open schema-apply lock ({e}), proceeding without lock");
            // Continue without lock rather than failing
            return run_schema_apply_inner(verbose);
        }
    };

    use std::os::fd::AsRawFd;
    // LOCK_EX | LOCK_NB — exclusive, non-blocking
    let lock_result = unsafe { libc::flock(lock_file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if lock_result != 0 {
        // Another process is applying schema — skip, it'll complete it
        eprintln!("ℹ️  Schema apply already in progress (lock held) — skipping");
        return Ok(true);
    }

    // Lock acquired — run schema apply, drop lock when done
    let result = run_schema_apply_inner(verbose);
    // Lock released automatically when lock_file is dropped
    drop(lock_file);
    result
}

/// Inner implementation of schema apply, separated for the flock wrapper.
///
/// Calls `sinex_db::apply_schema_for_url()` directly (in-process, no subprocess).
/// Uses `block_in_place` since we're inside a multi-threaded tokio runtime but
/// this function is sync (called from `ensure_ready`).
fn run_schema_apply_inner(verbose: bool) -> Result<bool> {
    let config = crate::infra::stack::StackConfig::for_current_checkout()?;

    eprintln!("⚡ Applying pending declarative schema...");

    let start = std::time::Instant::now();
    let _watchdog = spawn_watchdog("Applying declarative schema", 5);

    let result = crate::infra::stack::apply_schema_for_database_url(&config.database_url(), false);

    let elapsed = start.elapsed();
    match result {
        Ok(()) => {
            if verbose {
                eprintln!(
                    "✓ Declarative schema applied ({:.1}s)",
                    elapsed.as_secs_f64()
                );
            }
            record_schema_applied();
            Ok(true)
        }
        Err(e) => {
            eprintln!(
                "✗ Failed to apply declarative schema ({:.1}s): {e}",
                elapsed.as_secs_f64()
            );
            bail!("declarative schema apply failed: {e:#}");
        }
    }
}

/// Check if contracts directory has changed since last deploy.
fn contracts_changed_since_last_deploy() -> bool {
    contracts_changed_since_last_deploy_result().unwrap_or_else(|error| {
        tracing::warn!(error = %error, "failed to inspect contracts deployment state; treating contracts as changed");
        true
    })
}

fn contracts_changed_since_last_deploy_result() -> Result<bool> {
    let state_dir = state_dir();
    let hash_file = state_dir.join("contracts-hash.txt");

    let current_hash = hash_contracts_dir()?;
    let cached_hash = read_optional_state_file(&hash_file, "contracts hash")?;

    Ok(cached_hash.as_deref() != Some(&current_hash))
}

/// Record that contracts were deployed with current directory state.
fn record_contracts_deployed() {
    let state_dir = state_dir();
    let hash_file = state_dir.join("contracts-hash.txt");
    let current_hash = match hash_contracts_dir() {
        Ok(hash) => hash,
        Err(error) => {
            tracing::warn!(error = %error, "failed to compute contracts hash after deployment");
            return;
        }
    };
    if let Err(error) = write_state_file_atomically(&hash_file, &current_hash) {
        tracing::warn!(
            path = %hash_file.display(),
            error = %error,
            "failed to record deployed contracts state"
        );
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ContractsDeployOutcome {
    Unchanged,
    SkippedDatabaseNotReady,
    SkippedProbeFailed(String),
    SkippedStaleRegistry(String),
    Deployed,
    Failed(String),
}

impl ContractsDeployOutcome {
    fn stage_success(&self) -> bool {
        matches!(self, Self::Unchanged | Self::Deployed)
    }

    fn cache_converged(&self) -> bool {
        matches!(self, Self::Unchanged | Self::Deployed)
    }
}

fn ensure_running_binary_contracts_inventory_current(
    current_hash: &str,
) -> Result<()> {
    ensure_compiled_contracts_inventory_current(current_hash, compiled_contracts_hash())
}

fn ensure_compiled_contracts_inventory_current(
    current_hash: &str,
    compiled_hash: &str,
) -> Result<()> {
    if compiled_hash == "unknown" {
        bail!(
            "running xtask binary does not carry a compiled event payload inventory hash; rebuild xtask before deploying contracts"
        );
    }

    if compiled_hash != current_hash {
        bail!(
            "running xtask binary carries stale event payload inventory (compiled hash {compiled_hash}, current source hash {current_hash}); rerun xtask after it rebuilds"
        );
    }

    Ok(())
}

fn summarize_command_output(output: &std::process::Output) -> String {
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    if !stderr.is_empty() {
        return stderr;
    }

    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if !stdout.is_empty() {
        return stdout;
    }

    format!("exit status {}", output.status)
}

fn check_contract_tables_ready(output: std::io::Result<std::process::Output>) -> Result<bool> {
    let output = output.wrap_err("failed to run contracts readiness probe via psql")?;
    if !output.status.success() {
        bail!(
            "contracts readiness probe failed: {}",
            summarize_command_output(&output)
        );
    }
    Ok(!output.stdout.is_empty())
}

/// Auto-deploy contracts if schemas have changed.
///
/// Only deploys if:
/// 1. Database is ready (contracts check-ready passes)
/// 2. Schema files have changed since last deploy
fn auto_deploy_contracts(verbose: bool) -> ContractsDeployOutcome {
    // Skip if no changes detected
    if !contracts_changed_since_last_deploy() {
        return ContractsDeployOutcome::Unchanged;
    }

    let config = match crate::infra::stack::StackConfig::for_current_checkout() {
        Ok(config) => config,
        Err(error) => {
            let message =
                format!("failed to load stack config before contracts deployment: {error}");
            eprintln!("⚠️  {message}");
            return ContractsDeployOutcome::SkippedProbeFailed(message);
        }
    };

    // Check if database is ready for contracts (tables exist)
    let tables_exist = match check_contract_tables_ready(
        std::process::Command::new("psql")
            .env("PGHOST", config.run_dir())
            .env("PGPORT", config.postgres.port.to_string())
            .env("PGUSER", &config.postgres.user)
            .env("PGDATABASE", &config.postgres.database)
            .args([
                "-tAc",
                "SELECT 1 FROM pg_tables WHERE schemaname='sinex_schemas' AND tablename='event_payload_schemas'",
            ])
            .output(),
    ) {
        Ok(ready) => ready,
        Err(error) => {
            let message = format!("{error:#}");
            eprintln!("⚠️  {message}");
            return ContractsDeployOutcome::SkippedProbeFailed(message);
        }
    };

    if !tables_exist {
        // Database not ready for contracts yet
        return ContractsDeployOutcome::SkippedDatabaseNotReady;
    }

    let current_hash = match hash_contracts_dir() {
        Ok(hash) => hash,
        Err(error) => {
            let message = format!("failed to hash event payload contracts before deployment: {error:#}");
            eprintln!("⚠️  {message}");
            return ContractsDeployOutcome::SkippedProbeFailed(message);
        }
    };

    if let Err(error) = ensure_running_binary_contracts_inventory_current(&current_hash) {
        let message = format!("{error:#}");
        eprintln!("ℹ️  Contracts deployment deferred: {message}");
        return ContractsDeployOutcome::SkippedStaleRegistry(message);
    }

    eprintln!("⚡ Auto-deploying event payload contracts (schemas changed)...");

    let start = std::time::Instant::now();
    let _watchdog = spawn_watchdog("Deploying contracts", 5);

    let elapsed = start.elapsed();
    match crate::infra::stack::sync_event_payload_schemas_for_database_url(
        &config.database_url(),
        verbose,
    ) {
        Ok(sync_result) => {
            eprintln!("✓ Contracts deployed ({:.1}s)", elapsed.as_secs_f64());
            if !verbose {
                eprintln!(
                    "  discovered={}, created={}, updated={}, unchanged={}",
                    sync_result.discovered,
                    sync_result.created,
                    sync_result.updated,
                    sync_result.unchanged
                );
            }
            record_contracts_deployed();
            ContractsDeployOutcome::Deployed
        }
        Err(error) => {
            // Non-fatal: contracts deploy failure shouldn't block tests.
            // Don't record hash — will retry next invocation so transient failures self-heal.
            let message = format!(
                "Contracts deploy failed ({:.1}s, non-fatal, will retry next run): {error:#}",
                elapsed.as_secs_f64(),
            );
            eprintln!("⚠️  {message}");
            ContractsDeployOutcome::Failed(message)
        }
    }
}

fn pending_cache_blockers(schema_pending: bool, contracts_pending: bool) -> Vec<&'static str> {
    let mut blockers = Vec::new();
    if schema_pending {
        blockers.push("schema apply still pending");
    }
    if contracts_pending {
        blockers.push("contracts deployment still pending");
    }
    blockers
}

fn read_optional_state_file(path: &Path, label: &str) -> Result<Option<String>> {
    match std::fs::read_to_string(path) {
        Ok(contents) => Ok(Some(contents)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error)
            .wrap_err_with(|| format!("failed to read {label} file {}", path.display())),
    }
}

/// Check for required external tools.
///
/// Returns an error with a clear message if any required tools are missing.
/// This helps users understand they need to enter the devshell.
///
/// Only checks for tools that are essential for preflight operations:
/// - pg_isready: Check if Postgres is running
/// - psql: Run schema apply checks and queries
/// - createdb: Create databases
///
/// NATS-related tools are checked separately when NATS is needed.
fn check_required_tools() -> Result<()> {
    check_required_tools_with(&["pg_isready", "psql", "createdb"], ToolManager::check_tool)
}

fn check_required_tools_with<F>(tools: &[&str], check_tool: F) -> Result<()>
where
    F: Fn(&str) -> Result<ToolInfo>,
{
    let mut failures = Vec::new();
    for tool in tools {
        match check_tool(tool) {
            Ok(info) => {
                if let Some(issue) = info.probe_issue {
                    failures.push(format!("{tool}: {issue}"));
                }
            }
            Err(error) => failures.push(format!("{tool}: {error}")),
        }
    }

    if !failures.is_empty() {
        bail!(
            "Required preflight tools are unavailable or unhealthy: {}. Enter devshell with `nix develop` or ensure these are on PATH.",
            failures.join("; ")
        );
    }
    Ok(())
}

/// Ensure all infrastructure is ready for a command.
///
/// This is the main entry point for preflight checks. It will:
/// 1. Check required tools are available
/// 2. Check if stack is running, auto-start if not
/// 3. Generate TLS certs if missing (interactive only)
/// 4. Auto-apply pending declarative schema
/// 5. Auto-deploy contracts if payload schemas changed
///
/// **Caching**: After a successful preflight, a cache entry is written to
/// `.sinex/preflight/preflight-cache.json`. Subsequent calls within the TTL
/// window (default 60s, override via `SINEX_PREFLIGHT_TTL_SECS`) skip preflight
/// entirely if schema files, contract payload files, and git HEAD are unchanged.
///
/// **Nextest context**: when running inside `cargo nextest`, this function is a
/// no-op. The test sandbox (TestContext) already manages DB/NATS/schema apply.
///
/// **IMPORTANT — NOT a deadlock guard**: This no-op does not protect callers against
/// the cargo target/ lock deadlock. Commands that invoke cargo subprocesses (`build`,
/// `fix`, `run`) must add their own `NEXTEST_RUN_ID` check. Relying on `ensure_ready`
/// as a nextest gate is WRONG — it only skips infra setup, not subprocess prevention.
pub fn ensure_ready(ctx: &crate::command::CommandContext) -> Result<()> {
    // Skip preflight entirely when running inside nextest.
    // nextest holds the cargo target/ lock — any cargo subprocess would deadlock.
    // The test sandbox (TestContext) already handles DB/NATS/schema apply.
    if crate::config::is_nextest_run() {
        return Ok(());
    }

    // Check the preflight result cache before doing any real work.
    let ttl_secs = std::env::var("SINEX_PREFLIGHT_TTL_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(PREFLIGHT_CACHE_DEFAULT_TTL_SECS);

    match PreflightCache::load() {
        Ok(Some(cache)) => {
            if cache.is_valid(ttl_secs).unwrap_or_else(|error| {
                tracing::warn!(
                    error = %error,
                    "failed to validate preflight cache; continuing with full preflight"
                );
                false
            }) {
                tracing::debug!("preflight cache: skipping preflight (cache valid)");
                return Ok(());
            }
        }
        Ok(None) => {}
        Err(error) => {
            tracing::warn!(
                error = %error,
                "failed to load preflight cache; continuing with full preflight"
            );
        }
    }

    // 0. Check required tools are available
    check_required_tools()?;

    let is_interactive = ctx.is_human();
    let mut status = InfraStatus::capture();

    // 1. Auto-start stack if not running
    // Note: infra start also applies schema, so we only need to check schema apply
    // in the case where the stack was already running
    if !status.stack_running() {
        let stage = ctx.start_stage("stack-start");
        let started = auto_start_stack(is_interactive);
        ctx.finish_stage(stage, started.is_ok());
        started.wrap_err(
            "Failed to auto-start infrastructure. Check logs or start manually: xtask infra start",
        )?;
        status = InfraStatus::capture();
    }

    // 2. Auto-generate TLS certs if missing
    if !status.tls {
        let stage = ctx.start_stage("tls-certs");
        let result = ensure_tls_certs(is_interactive);
        ctx.finish_stage(stage, result.is_ok());
        result?;
    }

    // 3. Auto-apply declarative schema if pending (stack was already running)
    if status.schema_apply_pending {
        let stage = ctx.start_stage("schema-apply");
        let result = auto_apply_schema(is_interactive);
        ctx.finish_stage(stage, result.as_ref().is_ok_and(|&ok| ok));
        result?;
        status.schema_apply_pending = false;
    }

    // 4. Auto-deploy contracts if payload schemas changed
    let stage = ctx.start_stage("contracts-deploy");
    let contracts_outcome = auto_deploy_contracts(is_interactive);
    ctx.finish_stage(stage, contracts_outcome.stage_success());

    let blockers = pending_cache_blockers(
        status.schema_apply_pending || schema_changed_since_last_apply(),
        !contracts_outcome.cache_converged() && contracts_changed_since_last_deploy(),
    );
    if blockers.is_empty() {
        // Write the preflight result cache so the next invocation can skip this work.
        match PreflightCache::current() {
            Ok(cache) => cache.save(),
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    "failed to capture current preflight state; skipping cache save"
                );
                if is_interactive {
                    eprintln!(
                        "⚠️  Failed to capture current preflight state ({error:#}); skipping cache save"
                    );
                }
            }
        }
    } else {
        tracing::warn!(
            blockers = ?blockers,
            "preflight completed without converged state; skipping cache save so setup retries next run"
        );
        if is_interactive {
            eprintln!(
                "⚠️  Preflight is not converged yet ({}); skipping cache save so setup retries next run",
                blockers.join(", ")
            );
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::sinex_test;
    use std::os::unix::process::ExitStatusExt;
    use tempfile::tempdir;

    #[sinex_test]
    async fn test_infra_status_capture() -> TestResult<()> {
        // This test just verifies the capture doesn't panic
        let status = InfraStatus::capture();
        // The actual values depend on the environment
        let _ = status.all_ready();
        let _ = status.stack_running();
        Ok(())
    }

    #[sinex_test]
    async fn test_write_state_file_atomically_creates_parent_dirs() -> TestResult<()> {
        let dir = tempdir()?;
        let path = dir.path().join("nested").join("state.json");

        write_state_file_atomically(&path, "{\"ok\":true}")?;

        assert_eq!(std::fs::read_to_string(&path)?, "{\"ok\":true}");
        Ok(())
    }

    #[sinex_test]
    async fn test_write_state_file_atomically_replaces_existing_contents() -> TestResult<()> {
        let dir = tempdir()?;
        let path = dir.path().join("state.json");
        std::fs::write(&path, "old")?;

        write_state_file_atomically(&path, "new")?;

        assert_eq!(std::fs::read_to_string(&path)?, "new");
        Ok(())
    }

    #[sinex_test]
    async fn test_write_state_file_atomically_reports_parent_creation_failures() -> TestResult<()> {
        let dir = tempdir()?;
        let blocking_path = dir.path().join("not-a-dir");
        std::fs::write(&blocking_path, "blocker")?;
        let path = blocking_path.join("state.json");

        let error = write_state_file_atomically(&path, "value").unwrap_err();
        let message = format!("{error:#}");
        assert!(message.contains("failed to create preflight state directory"));
        Ok(())
    }

    #[sinex_test]
    async fn test_check_contract_tables_ready_reports_probe_failures() -> TestResult<()> {
        let error = check_contract_tables_ready(Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "psql missing",
        )))
        .unwrap_err();
        assert!(format!("{error:#}").contains("psql missing"));

        let error = check_contract_tables_ready(Ok(std::process::Output {
            status: std::process::ExitStatus::from_raw(1 << 8),
            stdout: Vec::new(),
            stderr: b"permission denied".to_vec(),
        }))
        .unwrap_err();
        assert!(format!("{error:#}").contains("permission denied"));
        Ok(())
    }

    #[sinex_test]
    async fn test_hash_contracts_dir_from_returns_empty_for_missing_directory() -> TestResult<()> {
        let dir = tempdir()?;
        let missing = dir.path().join("missing-payloads");
        assert_eq!(hash_contracts_dir_from(&missing)?, "empty");
        Ok(())
    }

    #[sinex_test]
    async fn test_hash_contracts_dir_from_hashes_rust_sources_only() -> TestResult<()> {
        let dir = tempdir()?;
        std::fs::write(dir.path().join("alpha.rs"), "pub struct Alpha;")?;
        std::fs::write(dir.path().join("beta.txt"), "ignored")?;

        let hash = hash_contracts_dir_from(dir.path())?;
        std::fs::write(dir.path().join("beta.txt"), "ignored differently")?;
        let hash_after_non_rust_change = hash_contracts_dir_from(dir.path())?;

        assert_ne!(hash, "empty");
        assert_eq!(hash, hash_after_non_rust_change);
        Ok(())
    }

    #[sinex_test]
    async fn test_ensure_compiled_contracts_inventory_current_accepts_matching_hash() -> TestResult<()> {
        ensure_compiled_contracts_inventory_current("deadbeefcafebabe", "deadbeefcafebabe")?;
        Ok(())
    }

    #[sinex_test]
    async fn test_ensure_compiled_contracts_inventory_current_rejects_stale_hash() -> TestResult<()> {
        let error =
            ensure_compiled_contracts_inventory_current("deadbeefcafebabe", "feedface00000000")
                .unwrap_err();
        assert!(format!("{error:#}").contains("stale event payload inventory"));
        Ok(())
    }

    #[sinex_test]
    async fn test_ensure_compiled_contracts_inventory_current_rejects_missing_hash() -> TestResult<()> {
        let error =
            ensure_compiled_contracts_inventory_current("deadbeefcafebabe", "unknown").unwrap_err();
        assert!(format!("{error:#}").contains("does not carry a compiled event payload inventory hash"));
        Ok(())
    }

    #[sinex_test]
    async fn test_pending_cache_blockers_reports_unconverged_setup() -> TestResult<()> {
        assert_eq!(pending_cache_blockers(false, false), Vec::<&'static str>::new());
        assert_eq!(
            pending_cache_blockers(true, false),
            vec!["schema apply still pending"]
        );
        assert_eq!(
            pending_cache_blockers(false, true),
            vec!["contracts deployment still pending"]
        );
        assert_eq!(
            pending_cache_blockers(true, true),
            vec![
                "schema apply still pending",
                "contracts deployment still pending"
            ]
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_load_preflight_cache_from_reports_malformed_json() -> TestResult<()> {
        let dir = tempdir()?;
        let path = dir.path().join("preflight-cache.json");
        std::fs::write(&path, "{not json")?;

        let error = load_preflight_cache_from(&path).unwrap_err();
        assert!(format!("{error:#}").contains("failed to parse preflight cache"));
        Ok(())
    }

    #[sinex_test]
    async fn test_parse_schema_apply_probe_output_reports_invalid_output() -> TestResult<()> {
        let error = parse_schema_apply_probe_output(&std::process::Output {
            status: std::process::ExitStatus::from_raw(0),
            stdout: b"wat".to_vec(),
            stderr: Vec::new(),
        })
        .unwrap_err();
        assert!(format!("{error:#}").contains("schema readiness probe returned invalid output"));
        Ok(())
    }

    #[sinex_test]
    async fn test_read_optional_state_file_reports_non_not_found_errors() -> TestResult<()> {
        let dir = tempdir()?;
        let error = read_optional_state_file(dir.path(), "state file").unwrap_err();
        assert!(format!("{error:#}").contains("failed to read state file file"));
        Ok(())
    }

    #[sinex_test]
    async fn test_check_required_tools_with_accepts_healthy_tools() -> TestResult<()> {
        check_required_tools_with(&["pg_isready", "psql"], |_tool| {
            Ok(ToolInfo {
                path: "/nix/store/fake-tool".into(),
                version: "1.0.0".to_string(),
                probe_issue: None,
            })
        })?;
        Ok(())
    }

    #[sinex_test]
    async fn test_check_required_tools_with_surfaces_missing_and_broken_tools() -> TestResult<()> {
        let error = check_required_tools_with(&["pg_isready", "psql", "createdb"], |tool| {
            match tool {
                "pg_isready" => Ok(ToolInfo {
                    path: "/nix/store/pg_isready".into(),
                    version: "pg_isready 16".to_string(),
                    probe_issue: None,
                }),
                "psql" => Err(eyre!("Tool 'psql' not found in PATH")),
                "createdb" => Ok(ToolInfo {
                    path: "/nix/store/createdb".into(),
                    version: "unknown".to_string(),
                    probe_issue: Some("Failed to run 'createdb --version'".to_string()),
                }),
                _ => unreachable!(),
            }
        })
        .unwrap_err();

        let message = format!("{error:#}");
        assert!(message.contains("psql"));
        assert!(message.contains("not found in PATH"));
        assert!(message.contains("createdb"));
        assert!(message.contains("createdb --version"));
        Ok(())
    }
}
