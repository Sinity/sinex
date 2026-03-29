//! Scoped job coordination for concurrent xtask processes.
//!
//! When multiple agents call `xtask {check,test,build} --bg` concurrently,
//! they all compete for the same `target/` directory lock, causing serialized
//! compilation and redundant work.
//!
//! The coordinator deduplicates work using a **coordination scope**: two requests
//! are "the same work" if they have the same command class, tree fingerprint
//! (git working tree state), and scope key (command-specific parameters).
//!
//! ## Decision Matrix
//!
//! 1. **Excluded** — Non-coordinatable modes (debug, fuzz, coverage, etc.) run directly.
//! 2. **Fresh** — (check/build only) Last completed job has same fingerprint+scope → return cached.
//! 3. **Attach** — Running job has same fingerprint+scope → return its job ID.
//! 4. **Supersede** — Running bg job has same scope but different fingerprint → cancel + restart.
//! 5. **Queue** — Running job has different scope → queue after it.
//! 6. **Start** — No running job → start new.

use color_eyre::eyre::{Result, WrapErr, bail};
use nix::fcntl::{FlockArg, flock};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs;
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};

use crate::command::{CommandContext, CommandResult};
use crate::config::config;
use crate::history::InvocationStatus;
use crate::output::OutputFormat;

/// Result of a coordination request.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum CoordinationResult {
    /// Started a new job (no previous, or cancelled stale one).
    Started { job_id: i64 },
    /// Cancelled stale bg job with same scope, started fresh.
    Superseded { old_job_id: i64, new_job_id: i64 },
    /// Running job has same scope + tree — wait for its results.
    Attached { job_id: i64 },
    /// Last completed job already validated this scope + tree (check/build only).
    Fresh {
        job_id: i64,
        status: String,
        duration_secs: f64,
    },
    /// Different-scope job running — queued after it.
    Queued { current_job_id: i64 },
}

/// Persisted coordination state for a command class.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoordinationState {
    pub job_id: i64,
    pub pid: u32,
    pub is_foreground: bool,
    pub tree_fingerprint: String,
    pub scope_key: String,
    pub started_at: String,
    pub args: Vec<String>,
    /// FIFO queue of pending follow-up jobs.
    ///
    /// Supports multiple concurrent requesters queuing behind a running job.
    /// Each completion pops the first item; remaining items stay queued.
    pub queue: Vec<QueuedWork>,
}

/// A queued job waiting for the current one to finish.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueuedWork {
    pub args: Vec<String>,
    pub is_foreground: bool,
    pub output_format: OutputFormat,
}

/// Scoped job coordinator.
///
/// Uses POSIX advisory locks (`flock`) for mutual exclusion and JSON state files
/// for cross-process state. Lock is held ~100ms during the coordination decision,
/// NOT for the duration of the cargo build.
pub struct JobCoordinator {
    locks_dir: PathBuf,
}

impl JobCoordinator {
    /// Create a new coordinator, ensuring the locks directory exists.
    pub fn new() -> Result<Self> {
        let cfg = config();
        let locks_dir = cfg.state_dir.join("coordinator");
        fs::create_dir_all(&locks_dir).with_context(|| {
            format!(
                "failed to create coordinator directory: {}",
                locks_dir.display()
            )
        })?;
        Ok(Self { locks_dir })
    }

    /// Should this command+mode be coordinated?
    ///
    /// Returns `false` for modes that should bypass coordination entirely:
    /// test --debug, --fuzz, --mutants, --coverage, --bench, --list, --dry-run.
    #[must_use]
    pub fn should_coordinate(command: &str, args: &[String]) -> bool {
        match command {
            "check" | "build" | "fix" => true,
            "test" => {
                // Exclude non-coordinatable test modes
                let excluded = [
                    "--debug",
                    "--fuzz",
                    "--mutants",
                    "--coverage",
                    "--bench",
                    "--list",
                    "--dry-run",
                    "-l",
                ];
                !args.iter().any(|a| excluded.contains(&a.as_str()))
            }
            _ => false,
        }
    }

    /// Core coordination: request a coordinated job.
    ///
    /// Acquires the command-class lock, reads state, applies the decision matrix,
    /// and returns the coordination result. Lock is held briefly (~100ms).
    pub fn request(
        &self,
        command: &str,
        args: &[String],
        is_foreground: bool,
    ) -> Result<CoordinationResult> {
        self.request_with_format(command, args, is_foreground, OutputFormat::Human)
    }

    /// Core coordination with explicit output format propagation.
    ///
    /// `output_format` is persisted for queued work so follow-up jobs preserve
    /// caller semantics (notably `--json`) when eventually spawned.
    pub fn request_with_format(
        &self,
        command: &str,
        args: &[String],
        is_foreground: bool,
        output_format: OutputFormat,
    ) -> Result<CoordinationResult> {
        let lock_path = self.locks_dir.join(format!("{command}.lock"));
        let state_path = self.locks_dir.join(format!("{command}.state.json"));

        // Open/create lock file and acquire exclusive lock
        let lock_file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)
            .with_context(|| format!("failed to open lock file: {}", lock_path.display()))?;

        lock_exclusive_retry(lock_file.as_raw_fd())
            .with_context(|| format!("failed to acquire lock: {}", lock_path.display()))?;

        // R1: Compute scoped fingerprint (per-package when -p is specified, whole-workspace otherwise)
        let tree_fingerprint = scoped_tree_fingerprint(command, args)?;
        let scope_key = scope_key(command, args);

        // Read current state (if any)
        let current_state = read_state(&state_path)?;

        let result = if let Some(state) = current_state {
            // There's an existing state — check if process is still alive
            if is_process_alive(state.pid) {
                self.handle_running_job(
                    command,
                    args,
                    is_foreground,
                    output_format,
                    &tree_fingerprint,
                    &scope_key,
                    &state,
                    &state_path,
                )?
            } else if state.pid == 0 && state_file_is_recent(&state_path) {
                // X4: Sentinel PID=0 state was written very recently (<5s ago).
                // Another process is in the reserve→spawn window (start_new_job wrote
                // sentinel values but hasn't called update_state yet, ~100ms gap).
                // Queue behind it to avoid double-spawn. Worst case: the reservation
                // was abandoned — the queue item runs after the 8h timeout cleans up.
                self.queue_behind(&state, args, is_foreground, output_format, &state_path)?;
                CoordinationResult::Queued {
                    current_job_id: state.job_id,
                }
            } else {
                // Process died (or reservation is stale) — clean up and start fresh
                remove_state_file(&state_path, "remove stale coordinator state before restart")?;
                self.start_new_job(
                    command,
                    args,
                    is_foreground,
                    &tree_fingerprint,
                    &scope_key,
                    &state_path,
                )?
            }
        } else {
            // No state — check for fresh result (check/build only), then start new
            if command != "test"
                && let Some(fresh) = self.check_fresh(command, &tree_fingerprint, &scope_key)
            {
                // R5: Log fresh decision with structured fields
                let job_id = match &fresh {
                    CoordinationResult::Fresh { job_id, .. } => *job_id,
                    _ => -1,
                };
                tracing::info!(
                    target: "xtask::coordinator",
                    command = command,
                    decision = "fresh",
                    scope_key = %scope_key,
                    tree_fingerprint = %tree_fingerprint,
                    job_id = job_id,
                    "coordinator: fresh — no recompilation needed"
                );
                return Ok(fresh);
            }
            self.start_new_job(
                command,
                args,
                is_foreground,
                &tree_fingerprint,
                &scope_key,
                &state_path,
            )?
        };

        // R5: Log every coordination decision with structured fields so all decisions
        // are observable regardless of whether coordination_to_result() is called.
        {
            let decision = match &result {
                CoordinationResult::Started { .. } => "started",
                CoordinationResult::Superseded { .. } => "superseded",
                CoordinationResult::Attached { .. } => "attached",
                CoordinationResult::Fresh { .. } => "fresh",
                CoordinationResult::Queued { .. } => "queued",
            };
            let job_id = match &result {
                CoordinationResult::Started { job_id }
                | CoordinationResult::Attached { job_id } => *job_id,
                CoordinationResult::Superseded { new_job_id, .. } => *new_job_id,
                CoordinationResult::Fresh { job_id, .. } => *job_id,
                CoordinationResult::Queued { current_job_id } => *current_job_id,
            };
            tracing::info!(
                target: "xtask::coordinator",
                command = command,
                decision = decision,
                scope_key = %scope_key,
                tree_fingerprint = %tree_fingerprint,
                job_id = job_id,
                "coordinator decision"
            );
        }

        // Lock released on drop of lock_file
        Ok(result)
    }

    /// Called when a coordinated job completes. Pops next queued work (FIFO).
    ///
    /// If more items remain in the queue, the state file is preserved with
    /// sentinel values (job_id=-1, pid=0) — the caller must update via
    /// `update_state()` after spawning the returned work.
    pub fn handle_completion(&self, command: &str) -> Result<Option<QueuedWork>> {
        let lock_path = self.locks_dir.join(format!("{command}.lock"));
        let state_path = self.locks_dir.join(format!("{command}.state.json"));

        let lock_file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)?;

        lock_exclusive_retry(lock_file.as_raw_fd())?;

        let state = read_state(&state_path)?;

        match state {
            Some(mut state) if !state.queue.is_empty() => {
                // Pop first queued item (FIFO)
                let next = state.queue.remove(0);

                if state.queue.is_empty() {
                    // No more items — delete state file
                    remove_state_file(
                        &state_path,
                        "remove coordinator state after draining the final queued job",
                    )?;
                } else {
                    // More items waiting — preserve state with sentinel values.
                    // Caller updates via update_state() after spawning.
                    state.job_id = -1;
                    state.pid = 0;
                    state.is_foreground = next.is_foreground;
                    state.args.clone_from(&next.args);
                    state.started_at =
                        sinex_primitives::temporal::Timestamp::now().format_rfc3339();
                    write_state(&state_path, &state)?;
                }

                Ok(Some(next))
            }
            _ => {
                // No queue or no state — clean up
                remove_state_file(
                    &state_path,
                    "remove coordinator state after completion with no queued work",
                )?;
                Ok(None)
            }
        }
    }

    /// Read current state for display.
    pub fn state(&self, command: &str) -> Result<Option<CoordinationState>> {
        let state_path = self.locks_dir.join(format!("{command}.state.json"));
        read_state(&state_path)
    }

    // --- Internal decision logic ---

    fn handle_running_job(
        &self,
        command: &str,
        args: &[String],
        is_foreground: bool,
        output_format: OutputFormat,
        tree_fingerprint: &str,
        scope_key: &str,
        state: &CoordinationState,
        state_path: &std::path::Path,
    ) -> Result<CoordinationResult> {
        if state.scope_key == scope_key && state.tree_fingerprint == tree_fingerprint {
            // Same scope + same tree → ATTACH
            Ok(CoordinationResult::Attached {
                job_id: state.job_id,
            })
        } else if state.scope_key == scope_key {
            // Same scope, different tree → SUPERSEDE (if bg), QUEUE (if fg)
            if state.is_foreground {
                // Don't cancel interactive foreground jobs — queue instead
                self.queue_behind(state, args, is_foreground, output_format, state_path)?;
                Ok(CoordinationResult::Queued {
                    current_job_id: state.job_id,
                })
            } else {
                // Cancel stale bg job and start fresh
                let old_job_id = state.job_id;
                cancel_process(state.pid);
                mark_cancelled(old_job_id);
                remove_state_file(
                    state_path,
                    "remove superseded coordinator state before starting replacement job",
                )?;

                let new_result = self.start_new_job(
                    command,
                    args,
                    is_foreground,
                    tree_fingerprint,
                    scope_key,
                    state_path,
                )?;

                match new_result {
                    CoordinationResult::Started { job_id } => Ok(CoordinationResult::Superseded {
                        old_job_id,
                        new_job_id: job_id,
                    }),
                    other => Ok(other),
                }
            }
        } else {
            // Different scope → QUEUE (don't cancel valid work)
            self.queue_behind(state, args, is_foreground, output_format, state_path)?;
            Ok(CoordinationResult::Queued {
                current_job_id: state.job_id,
            })
        }
    }

    fn check_fresh(
        &self,
        command: &str,
        tree_fingerprint: &str,
        scope_key: &str,
    ) -> Option<CoordinationResult> {
        let cfg = config();
        let history_db_path = cfg.history_db_path();
        let db = match crate::history::HistoryDb::open(&history_db_path) {
            Ok(db) => db,
            Err(error) => {
                tracing::warn!(
                    target: "xtask::coordinator",
                    path = %history_db_path.display(),
                    error = %error,
                    command,
                    "coordinator freshness check disabled because history DB could not be opened"
                );
                return None;
            }
        };

        match db.get_last_completed_with_fingerprint(command) {
            Ok(Some(last))
                if last.tree_fingerprint.as_deref() == Some(tree_fingerprint)
                    && last.scope_key.as_deref() == Some(scope_key)
                    && last.status == InvocationStatus::Success =>
            {
                return Some(CoordinationResult::Fresh {
                    job_id: last.id,
                    status: "success".to_string(),
                    duration_secs: last.duration_secs.unwrap_or(0.0),
                });
            }
            Ok(_) => {}
            Err(error) => {
                tracing::warn!(
                    target: "xtask::coordinator",
                    path = %history_db_path.display(),
                    error = %error,
                    command,
                    tree_fingerprint = %tree_fingerprint,
                    scope_key = %scope_key,
                    "coordinator freshness check disabled because history lookup failed"
                );
            }
        }

        None
    }

    /// Reserve a coordination slot for a new job.
    ///
    /// Two-phase protocol:
    /// 1. `start_new_job()` reserves the slot with sentinel values (job_id=-1, pid=0)
    /// 2. Caller spawns the actual process via `spawn_background()`
    /// 3. Caller calls `update_state()` with the real `job_id` and `pid`
    ///
    /// Between steps 1 and 3 there is a TOCTOU window (~100ms) where another
    /// process could see the sentinel state. This is acceptable because:
    /// - `is_process_alive(0)` returns false, so another process would treat it as stale
    /// - The worst case is redundant work (two spawns), not data loss
    fn start_new_job(
        &self,
        _command: &str,
        args: &[String],
        is_foreground: bool,
        tree_fingerprint: &str,
        scope_key: &str,
        state_path: &std::path::Path,
    ) -> Result<CoordinationResult> {
        let state = CoordinationState {
            job_id: -1, // Sentinel: "pending spawn" — updated by caller via update_state()
            pid: 0,     // Sentinel: "not yet spawned" — updated by caller via update_state()
            is_foreground,
            tree_fingerprint: tree_fingerprint.to_string(),
            scope_key: scope_key.to_string(),
            started_at: sinex_primitives::temporal::Timestamp::now().format_rfc3339(),
            args: args.to_vec(),
            queue: Vec::new(),
        };

        write_state(state_path, &state)?;

        Ok(CoordinationResult::Started { job_id: -1 })
    }

    fn queue_behind(
        &self,
        state: &CoordinationState,
        args: &[String],
        is_foreground: bool,
        output_format: OutputFormat,
        state_path: &std::path::Path,
    ) -> Result<()> {
        // Append to FIFO queue (supports multiple concurrent requesters)
        let mut updated = state.clone();
        updated.queue.push(QueuedWork {
            args: args.to_vec(),
            is_foreground,
            output_format,
        });
        write_state(state_path, &updated)?;
        Ok(())
    }

    /// Update the state file with the actual job ID and PID after spawning.
    ///
    /// Preserves the queue — this is critical for FIFO queue correctness when
    /// `handle_completion()` left remaining items in the state file.
    pub fn update_state(&self, command: &str, job_id: i64, pid: u32) -> Result<()> {
        let lock_path = self.locks_dir.join(format!("{command}.lock"));
        let state_path = self.locks_dir.join(format!("{command}.state.json"));

        let lock_file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)?;

        lock_exclusive_retry(lock_file.as_raw_fd())?;

        if let Some(mut state) = read_state(&state_path)? {
            state.job_id = job_id;
            state.pid = pid;
            write_state(&state_path, &state)?;
        } else {
            // Another process may have completed and cleaned up the state in the reserve→spawn
            // window. This is benign; the spawned job remains tracked by the jobs subsystem.
        }

        Ok(())
    }
}

// --- Utility functions ---

/// Acquire an exclusive flock with a retry loop (D5 fix).
///
/// `flock(LOCK_EX)` blocks indefinitely; in a multi-process environment
/// a stuck holder would cause all callers to hang forever. We use the
/// non-blocking variant and retry up to ~500 ms before returning an error.
fn lock_exclusive_retry(fd: std::os::unix::io::RawFd) -> Result<()> {
    const MAX_RETRIES: u32 = 10;
    const RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_millis(50);
    for i in 0..MAX_RETRIES {
        match flock(fd, FlockArg::LockExclusiveNonblock) {
            Ok(()) => return Ok(()),
            Err(nix::errno::Errno::EWOULDBLOCK) if i + 1 < MAX_RETRIES => {
                std::thread::sleep(RETRY_INTERVAL);
            }
            Err(nix::errno::Errno::EWOULDBLOCK) => {
                bail!("coordinator: could not acquire lock after {MAX_RETRIES} retries (500 ms)");
            }
            Err(e) => return Err(e).wrap_err("coordinator: flock failed"),
        }
    }
    unreachable!()
}

/// Compute tree fingerprint: sha256 of `git status --porcelain` output.
///
/// Properties: deterministic (same tree → same hash), fast (~50ms),
/// captures staged, unstaged, and untracked changes.
fn summarize_git_error(output: &std::process::Output) -> String {
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

fn git_output(cwd: &Path, args: &[&str], description: &str) -> Result<std::process::Output> {
    let output = std::process::Command::new("git")
        .args(args)
        .current_dir(cwd)
        .output()
        .with_context(|| format!("failed to run git {description}"))?;

    if !output.status.success() {
        bail!(
            "git {description} failed: {}",
            summarize_git_error(&output)
        );
    }

    Ok(output)
}

fn refresh_git_index(cwd: &Path) -> Result<()> {
    let output = std::process::Command::new("git")
        .args(["update-index", "-q", "--refresh"])
        .current_dir(cwd)
        .output()
        .with_context(|| "failed to run git update-index -q --refresh".to_string())?;

    if !output.status.success() {
        bail!(
            "git update-index -q --refresh failed: {}",
            summarize_git_error(&output)
        );
    }

    Ok(())
}

fn tree_fingerprint_in(cwd: &Path) -> Result<String> {
    // Refresh the git index so status reflects actual filesystem state.
    // Without this, rapid edits within the same second can go undetected
    // because git caches stat data (mtime, size) in the index.
    refresh_git_index(cwd)?;

    let output = git_output(cwd, &["status", "--porcelain"], "status --porcelain")?;

    let mut hasher = Sha256::new();
    hasher.update(&output.stdout);
    Ok(format!("{:x}", hasher.finalize()))
}

fn tree_fingerprint() -> Result<String> {
    tree_fingerprint_in(Path::new("."))
}

/// R1: Map a package name to its source directory path for git diff scoping.
///
/// Used by `scoped_tree_fingerprint` to limit `git diff` to relevant directories.
/// Over-inclusion (returning a broader path) is safe — it causes unnecessary cache
/// misses but never incorrect freshness. Under-inclusion would be incorrect.
fn package_to_path(pkg: &str) -> String {
    match pkg {
        "sinexctl" => "crate/cli/".to_string(),
        "xtask" => "xtask/".to_string(),
        "sinex-e2e-tests" => "tests/e2e/".to_string(),
        _ => {
            let name_underscore = pkg.replace('-', "_");
            for category in &["lib", "core", "nodes", "tools"] {
                // Try hyphenated form first (canonical package naming)
                let path_hyphen = format!("crate/{category}/{pkg}/");
                if std::path::Path::new(&path_hyphen).exists() {
                    return path_hyphen;
                }
                // Try underscored form (directory may use underscores)
                let path_under = format!("crate/{category}/{name_underscore}/");
                if std::path::Path::new(&path_under).exists() {
                    return path_under;
                }
            }
            // Unknown package — include crate/ broadly (over-includes, never misses)
            "crate/".to_string()
        }
    }
}

/// R1: Extract package names from -p/--package flags in command args.
fn extract_explicit_packages(command: &str, args: &[String]) -> Vec<String> {
    if !matches!(command, "check" | "build" | "test") {
        return vec![];
    }

    let mut packages = Vec::new();
    let mut take_next = false;

    for arg in args {
        if take_next {
            packages.push(arg.clone());
            take_next = false;
            continue;
        }
        if arg == "-p" || arg == "--package" || arg == "--packages" {
            take_next = true;
        } else if let Some(pkg) = arg.strip_prefix("--packages=") {
            packages.push(pkg.to_string());
        } else if let Some(pkg) = arg.strip_prefix("--package=") {
            packages.push(pkg.to_string());
        } else if let Some(pkg) = arg.strip_prefix("-p").filter(|s| !s.is_empty()) {
            packages.push(pkg.to_string());
        }
    }

    packages
}

/// R1: Compute a scoped tree fingerprint for the given command and args.
///
/// If the command targets explicit packages (via `-p`), hashes only the git diff
/// for those package directories rather than the entire workspace. This means
/// changing `nixos/README.md` no longer invalidates `check -p sinex-db`.
///
/// Falls back to the whole-workspace `tree_fingerprint()` when no explicit
/// packages are specified (affected-mode and workspace-wide invocations).
fn scoped_tree_fingerprint_in(cwd: &Path, command: &str, args: &[String]) -> Result<String> {
    let packages = extract_explicit_packages(command, args);

    if packages.is_empty() {
        // No -p flag: use whole-workspace fingerprint (safe, over-inclusive)
        return tree_fingerprint_in(cwd);
    }

    // Refresh git index (same as tree_fingerprint)
    refresh_git_index(cwd)?;

    let mut hasher = Sha256::new();
    for pkg in &packages {
        let prefix = package_to_path(pkg);
        // Include tracked changes (staged + unstaged)
        let diff_out = git_output(
            cwd,
            &["diff", "--name-only", "HEAD", "--", &prefix],
            "diff --name-only HEAD -- <prefix>",
        )?;
        hasher.update(&diff_out.stdout);
        // Include untracked files in this package's directory
        let untracked = git_output(
            cwd,
            &["ls-files", "--others", "--exclude-standard", "--", &prefix],
            "ls-files --others --exclude-standard -- <prefix>",
        )?;
        hasher.update(&untracked.stdout);
    }

    Ok(format!("{:x}", hasher.finalize()))
}

fn scoped_tree_fingerprint(command: &str, args: &[String]) -> Result<String> {
    scoped_tree_fingerprint_in(Path::new("."), command, args)
}

/// Compute scope key: hash of command-specific parameters that define
/// what work is being done.
///
/// Handles both `--flag=value` and `--flag value` (two separate args) forms.
/// For flags like `-p sinex-db`, captures both the flag AND the following value.
fn scope_key(command: &str, args: &[String]) -> String {
    let relevant = extract_scope_args(command, args);

    let mut sorted: Vec<&str> = relevant.iter().map(String::as_str).collect();
    sorted.sort_unstable(); // Deterministic order

    let mut hasher = Sha256::new();
    for arg in &sorted {
        hasher.update(arg.as_bytes());
    }
    format!("{:x}", hasher.finalize())
}

/// Extract scope-relevant arguments for a command.
///
/// Handles the tricky case where `-p sinex-db` is two separate args:
/// the flag `-p` and its value `sinex-db` are both captured.
fn extract_scope_args(command: &str, args: &[String]) -> Vec<String> {
    fn is_value_flag(command: &str, arg: &str) -> bool {
        // Flags that take a separate next-arg value
        match command {
            "build" => matches!(arg, "-p" | "--package"),
            "test" => matches!(arg, "-p" | "--package" | "-E"),
            // Check scope includes -p/--all so narrow checks (e.g., -p sinex-primitives)
            // don't satisfy broader scopes (e.g., --all or workspace default).
            // Lint flags (--lint, --fmt, --forbidden) are intentionally excluded from
            // scope — they don't change which packages are compiled.
            "check" => matches!(arg, "-p" | "--package"),
            _ => false,
        }
    }

    fn is_standalone_flag(command: &str, arg: &str) -> bool {
        // Flags that are scope-relevant on their own (no value)
        match command {
            "build" => arg == "--release" || arg.starts_with("--all"),
            "test" => arg == "--heavy" || arg == "--include-ignored" || arg == "--all",
            "check" => arg == "--all",
            _ => false,
        }
    }

    fn is_combined_flag(command: &str, arg: &str) -> bool {
        // Flags with value attached: --package=foo, -p=foo, -Etest(name)
        match command {
            "build" => (arg.starts_with("-p") && arg.len() > 2) || arg.starts_with("--package="),
            "test" => {
                (arg.starts_with("-p") && arg.len() > 2)
                    || arg.starts_with("--package=")
                    || (arg.starts_with("-E") && arg.len() > 2)
            }
            "check" => (arg.starts_with("-p") && arg.len() > 2) || arg.starts_with("--package="),
            _ => false,
        }
    }

    let mut relevant = Vec::new();
    let mut take_next = false;

    for arg in args {
        if take_next {
            relevant.push(arg.clone());
            take_next = false;
            continue;
        }
        if is_value_flag(command, arg) {
            relevant.push(arg.clone());
            take_next = true; // Next arg is the value
        } else if is_standalone_flag(command, arg) || is_combined_flag(command, arg) {
            relevant.push(arg.clone());
        }
    }

    relevant
}

/// X4: Returns true if a coordinator state file was modified within the last 5 seconds.
///
/// Used to distinguish a fresh sentinel reservation (pid=0, just written) from a
/// genuinely stale state (process died and PID hasn't been recycled yet).
fn state_file_is_recent(path: &std::path::Path) -> bool {
    fs::metadata(path)
        .and_then(|m| m.modified())
        .map(|t| t.elapsed().map(|e| e.as_secs() < 5).unwrap_or(false))
        .unwrap_or(false)
}

/// Check if a process is still alive via `kill(pid, 0)`.
///
/// Returns false for sentinel PID 0 (not yet spawned).
fn is_process_alive(pid: u32) -> bool {
    if pid == 0 {
        return false; // Sentinel: "not yet spawned"
    }
    unsafe { libc::kill(pid as i32, 0) == 0 }
}

/// Cancel a process and its children: SIGTERM → wait 5s → SIGKILL.
///
/// Sends signals to the process group (negative PID) so that child processes
/// (e.g., rustc, nextest) spawned by the background cargo process are also
/// terminated. If process group kill fails (ESRCH), falls back to single-PID kill.
fn cancel_process(pid: u32) {
    let pid = pid as i32;
    if pid == 0 {
        return; // Sentinel PID — nothing to cancel
    }

    // Try process group first (-pid), fall back to single process
    unsafe {
        if libc::kill(-pid, libc::SIGTERM) != 0 {
            libc::kill(pid, libc::SIGTERM);
        }
    }

    // Wait up to 5 seconds for graceful exit.
    // X6: Check the process GROUP (-pid) not just the leader so that a leader that
    // exits before its children doesn't prematurely abort the grace period.
    for _ in 0..50 {
        std::thread::sleep(std::time::Duration::from_millis(100));
        if unsafe { libc::kill(-pid, 0) } != 0 {
            return; // Entire process group exited
        }
    }

    // Force kill (process group first, then single)
    unsafe {
        if libc::kill(-pid, libc::SIGKILL) != 0 {
            libc::kill(pid, libc::SIGKILL);
        }
    }
}

/// Mark an invocation as cancelled in the history DB (best-effort).
fn mark_cancelled(job_id: i64) {
    let cfg = config();
    match crate::history::HistoryDb::open(&cfg.history_db_path()) {
        Ok(db) => {
            if let Err(e) = db.finish_invocation(job_id, InvocationStatus::Cancelled, None, 0.0) {
                tracing::debug!(target: "xtask::coordinator", job_id, error = %e, "failed to mark invocation cancelled");
            }
        }
        Err(e) => {
            tracing::debug!(target: "xtask::coordinator", job_id, error = %e, "could not open history DB to mark invocation cancelled");
        }
    }
}

fn read_state(path: &std::path::Path) -> Result<Option<CoordinationState>> {
    let content = match fs::read_to_string(path) {
        Ok(content) => content,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("failed to read coordinator state: {}", path.display()));
        }
    };
    match serde_json::from_str::<CoordinationState>(&content) {
        Ok(state) => Ok(Some(state)),
        Err(e) => {
            tracing::warn!(
                target: "xtask::coordinator",
                path = %path.display(),
                error = %e,
                "corrupt coordinator state — treating as empty"
            );
            Ok(None)
        }
    }
}

fn remove_state_file(path: &std::path::Path, reason: &str) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error)
            .with_context(|| format!("{reason}: {}", path.display())),
    }
}

fn write_state(path: &std::path::Path, state: &CoordinationState) -> Result<()> {
    let json = serde_json::to_string_pretty(state)?;
    fs::write(path, json).with_context(|| format!("failed to write state: {}", path.display()))?;
    Ok(())
}

/// Coordinate and spawn a background job, deduplicating work across concurrent invocations.
///
/// Encapsulates the two-phase coordination protocol used by check, build, and test:
/// 1. Ask the coordinator if this work is already running/cached
/// 2. If not, spawn a background job and update coordination state
///
/// Returns early with cached/attached results when possible, otherwise spawns.
pub fn coordinate_and_spawn(
    command: &str,
    args: &[String],
    ctx: &CommandContext,
) -> Result<CommandResult> {
    if JobCoordinator::should_coordinate(command, args)
        && let Ok(coordinator) = JobCoordinator::new()
    {
        match coordinator.request_with_format(command, args, false, ctx.writer().format()) {
            Ok(
                result @ (CoordinationResult::Attached { .. }
                | CoordinationResult::Fresh { .. }
                | CoordinationResult::Queued { .. }),
            ) => {
                return Ok(coordination_to_result(&result, ctx));
            }
            Ok(CoordinationResult::Started { .. } | CoordinationResult::Superseded { .. }) => {
                // Fall through to spawn — coordinator reserved the slot
            }
            Err(e) => {
                tracing::warn!(target: "xtask::coordinator", error = %e, "coordinator request failed, spawning directly");
            }
        }
    }

    let bg_result = ctx.spawn_background(command, args)?;
    update_coordinator_state(command, &bg_result);
    Ok(bg_result)
}

/// After spawning a background job, update coordinator state with the real `job_id` and `pid`.
///
/// This is the second phase of the two-phase coordination protocol:
/// 1. `coordinator.request()` reserves a slot with sentinel values
/// 2. `spawn_background()` creates the actual process
/// 3. This function updates the slot with real values
pub fn update_coordinator_state(command: &str, bg_result: &CommandResult) {
    let Some(data) = &bg_result.data else {
        tracing::warn!(
            target: "xtask::coordinator",
            command,
            "background spawn returned no data; coordinator state was not updated"
        );
        return;
    };

    let Some(job_id) = data["job_id"].as_i64() else {
        tracing::warn!(
            target: "xtask::coordinator",
            command,
            data = %data,
            "background spawn returned no job_id; coordinator state was not updated"
        );
        return;
    };

    let Some(pid) = data["pid"].as_u64() else {
        tracing::warn!(
            target: "xtask::coordinator",
            command,
            data = %data,
            "background spawn returned no pid; coordinator state was not updated"
        );
        return;
    };

    let coordinator = match JobCoordinator::new() {
        Ok(coordinator) => coordinator,
        Err(error) => {
            tracing::warn!(
                target: "xtask::coordinator",
                command,
                job_id,
                pid,
                error = %error,
                "failed to initialize coordinator while recording spawned job"
            );
            return;
        }
    };

    if let Err(error) = coordinator.update_state(command, job_id, pid as u32) {
        tracing::warn!(
            target: "xtask::coordinator",
            command,
            job_id,
            pid,
            error = %error,
            "failed to persist coordinator state for spawned job"
        );
    }
}

/// Convert a coordination result to a command result for the --bg path.
pub fn coordination_to_result(result: &CoordinationResult, ctx: &CommandContext) -> CommandResult {
    match result {
        CoordinationResult::Fresh {
            job_id,
            status,
            duration_secs,
        } => coordination_fresh_result(*job_id, status, *duration_secs, ctx, fresh_packages_probe(*job_id)),
        CoordinationResult::Attached { job_id } => {
            tracing::info!(
                target: "xtask::coordinator",
                job_id = job_id,
                action = "attached",
                "coordinator: attached — identical check already running"
            );
            if ctx.is_human() {
                println!("🔗 Attached: identical check already running (job {job_id})");
                println!("   Monitor: xtask jobs status {job_id}");
            }
            CommandResult::success()
                .with_message(format!("Attached to running job {job_id}"))
                .with_data(serde_json::json!({
                    "action": "attached",
                    "job_id": job_id,
                    "hint": format!("Monitor with: xtask jobs status {job_id}"),
                }))
        }
        CoordinationResult::Superseded {
            old_job_id,
            new_job_id,
        } => {
            tracing::info!(
                target: "xtask::coordinator",
                old_job_id = old_job_id,
                new_job_id = new_job_id,
                action = "superseded",
                "coordinator: superseded — cancelled stale job, starting fresh"
            );
            if ctx.is_human() {
                println!(
                    "♻ Superseded: cancelled stale job {old_job_id}, starting fresh job {new_job_id}"
                );
            }
            CommandResult::success()
                .with_message(format!("Superseded job {old_job_id} with {new_job_id}"))
                .with_data(serde_json::json!({
                    "action": "superseded",
                    "old_job_id": old_job_id,
                    "new_job_id": new_job_id,
                }))
        }
        CoordinationResult::Queued { current_job_id } => {
            tracing::info!(
                target: "xtask::coordinator",
                current_job_id = current_job_id,
                action = "queued",
                "coordinator: queued — waiting for running job to complete"
            );
            if ctx.is_human() {
                println!("⏳ Queued: waiting for job {current_job_id} to complete");
            }
            CommandResult::success()
                .with_message(format!("Queued behind job {current_job_id}"))
                .with_data(serde_json::json!({
                    "action": "queued",
                    "current_job_id": current_job_id,
                }))
        }
        CoordinationResult::Started { job_id } => {
            tracing::info!(
                target: "xtask::coordinator",
                job_id = job_id,
                action = "started",
                "coordinator: started — new job launched"
            );
            // This shouldn't normally be returned in the --bg path since
            // we proceed to spawn_background after, but handle it for completeness
            CommandResult::success()
                .with_message(format!("Started job {job_id}"))
                .with_data(serde_json::json!({
                    "action": "started",
                    "job_id": job_id,
                }))
        }
    }
}

struct FreshPackagesProbe {
    packages: Vec<String>,
    issue: Option<String>,
}

fn fresh_packages_probe(job_id: i64) -> FreshPackagesProbe {
    let cfg = config();
    let db_path = cfg.history_db_path();
    let result = crate::history::HistoryDb::open(&db_path).and_then(|db| {
        db.get_compiled_packages_for_invocation(job_id)
            .map_err(Into::into)
    });
    fresh_packages_probe_from_result(job_id, &db_path, result)
}

fn fresh_packages_probe_from_result(
    job_id: i64,
    db_path: &std::path::Path,
    result: color_eyre::eyre::Result<Vec<String>>,
) -> FreshPackagesProbe {
    match result {
        Ok(packages) => FreshPackagesProbe {
            packages,
            issue: None,
        },
        Err(error) => FreshPackagesProbe {
            packages: Vec::new(),
            issue: Some(format!(
                "failed to load compiled packages for fresh job {job_id} from {}: {error:#}",
                db_path.display()
            )),
        },
    }
}

fn coordination_fresh_result(
    job_id: i64,
    status: &str,
    duration_secs: f64,
    ctx: &CommandContext,
    packages_probe: FreshPackagesProbe,
) -> CommandResult {
    tracing::info!(
        target: "xtask::coordinator",
        job_id = job_id,
        action = "fresh",
        cached_status = status,
        cached_duration_secs = duration_secs,
        "coordinator: fresh — last check already validated this code state"
    );

    if ctx.is_human() {
        if packages_probe.packages.is_empty() {
            println!(
                "✅ Fresh: last check already validated this code state (job {job_id}, {status} in {duration_secs:.1}s)"
            );
        } else {
            let pkg_list = if packages_probe.packages.len() <= 4 {
                packages_probe.packages.join(", ")
            } else {
                format!(
                    "{}, …+{}",
                    packages_probe.packages[..3].join(", "),
                    packages_probe.packages.len() - 3
                )
            };
            println!(
                "✅ Fresh: last check already validated {pkg_list} (job {job_id}, {duration_secs:.1}s)"
            );
        }
        if let Some(issue) = &packages_probe.issue {
            println!("   Warning: {issue}");
        }
    }

    let mut result = CommandResult::success()
        .with_message(format!("Fresh result from job {job_id}"))
        .with_data(serde_json::json!({
            "action": "fresh",
            "job_id": job_id,
            "cached_status": status,
            "cached_duration_secs": duration_secs,
            "compiled_packages": packages_probe.packages,
            "compiled_packages_issue": packages_probe.issue,
        }));

    if let Some(issue) = packages_probe.issue {
        result = result.with_warning(issue);
    }

    result
}

/// Tree fingerprint exposed for callers that need it (e.g., recording in history DB).
pub fn current_tree_fingerprint() -> Result<String> {
    tree_fingerprint()
}

/// Scope key exposed for callers (e.g., recording in history DB).
#[must_use]
pub fn compute_scope_key(command: &str, args: &[String]) -> String {
    scope_key(command, args)
}

// --- R2: Workflow Dependency Graph ---

/// R2: Operation dependency edges: (command, prerequisite).
///
/// Declares that `command` should be preceded by `prerequisite` in a workflow sequence.
/// `xtask work <target>` uses this to compute the minimum execution sequence.
static WORKFLOW: &[(&str, &str)] = &[
    ("test", "check"), // test builds on a passing check
];

/// R2: Topological sequencer for the workflow dependency graph.
///
/// Given a target command, returns the ordered sequence of commands needed to
/// reach that state. Dependencies appear before the commands that depend on them.
pub struct WorkflowGraph;

impl WorkflowGraph {
    /// Returns the minimum ordered sequence of commands needed to reach `target`.
    ///
    /// # Examples
    ///
    /// ```
    /// // "test" depends on "check", so the sequence is ["check", "test"]
    /// let seq = WorkflowGraph::sequence_to("test");
    /// assert_eq!(seq, vec!["check", "test"]);
    /// ```
    pub fn sequence_to(target: &str) -> Vec<String> {
        let mut result = Vec::new();
        let mut visited = std::collections::HashSet::new();
        Self::topo_collect(target, &mut result, &mut visited);
        result
    }

    fn topo_collect(
        target: &str,
        result: &mut Vec<String>,
        visited: &mut std::collections::HashSet<String>,
    ) {
        if visited.contains(target) {
            return;
        }
        visited.insert(target.to_string());
        // Add prerequisites first (depth-first)
        for &(cmd, prereq) in WORKFLOW {
            if cmd == target {
                Self::topo_collect(prereq, result, visited);
            }
        }
        result.push(target.to_string());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::sinex_test;
    use std::ffi::OsString;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
    use std::path::Path;

    fn run_git(args: &[&str], cwd: &Path) -> ::xtask::sandbox::TestResult<()> {
        let output = std::process::Command::new("git")
            .args(args)
            .current_dir(cwd)
            .output()?;
        assert!(
            output.status.success(),
            "git {} failed: stdout={} stderr={}",
            args.join(" "),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(())
    }

    struct ScopedEnvGuard {
        key: &'static str,
        previous: Option<OsString>,
    }

    impl ScopedEnvGuard {
        fn set_path(key: &'static str, value: &std::path::Path) -> Self {
            let previous = std::env::var_os(key);
            unsafe { std::env::set_var(key, value) };
            Self { key, previous }
        }
    }

    impl Drop for ScopedEnvGuard {
        fn drop(&mut self) {
            if let Some(previous) = &self.previous {
                unsafe { std::env::set_var(self.key, previous) };
            } else {
                unsafe { std::env::remove_var(self.key) };
            }
        }
    }

    #[sinex_test]
    async fn test_should_coordinate() -> TestResult<()> {
        assert!(JobCoordinator::should_coordinate("check", &[]));
        assert!(JobCoordinator::should_coordinate("build", &[]));
        assert!(JobCoordinator::should_coordinate(
            "test",
            &["-p".into(), "sinex-db".into()]
        ));
        assert!(!JobCoordinator::should_coordinate(
            "test",
            &["--debug".into()]
        ));
        assert!(!JobCoordinator::should_coordinate(
            "test",
            &["--fuzz".into()]
        ));
        assert!(!JobCoordinator::should_coordinate(
            "test",
            &["--coverage".into()]
        ));
        assert!(!JobCoordinator::should_coordinate(
            "test",
            &["--mutants".into()]
        ));
        assert!(!JobCoordinator::should_coordinate(
            "test",
            &["--bench".into()]
        ));
        assert!(JobCoordinator::should_coordinate("fix", &[]));
        Ok(())
    }

    #[sinex_test]
    async fn test_scope_key_deterministic() -> TestResult<()> {
        let args1 = vec!["-p".into(), "sinex-db".into(), "--all".into()];
        let args2 = vec!["--all".into(), "-p".into(), "sinex-db".into()];
        assert_eq!(scope_key("test", &args1), scope_key("test", &args2));
        Ok(())
    }

    #[sinex_test]
    async fn test_scope_key_different() -> TestResult<()> {
        let args1 = vec!["-p".into(), "sinex-db".into()];
        let args2 = vec!["-p".into(), "sinex-gateway".into()];
        assert_ne!(scope_key("test", &args1), scope_key("test", &args2));
        Ok(())
    }

    #[sinex_test]
    async fn test_scope_key_ignores_irrelevant() -> TestResult<()> {
        // --fail-fast, --skip-preflight, --prime are NOT scope-relevant for tests
        let args1 = vec!["-p".into(), "sinex-db".into()];
        let args2 = vec![
            "-p".into(),
            "sinex-db".into(),
            "--fail-fast".into(),
            "--skip-preflight".into(),
        ];
        assert_eq!(scope_key("test", &args1), scope_key("test", &args2));
        Ok(())
    }

    #[sinex_test]
    async fn test_check_fresh_returns_none_when_history_db_is_unopenable() -> TestResult<()> {
        let tempdir = tempfile::tempdir()?;
        let _history_db_guard = ScopedEnvGuard::set_path("XTASK_HISTORY_DB", tempdir.path());
        let coordinator = JobCoordinator::new()?;

        assert!(
            coordinator.check_fresh("check", "tree-fingerprint", "scope-key").is_none(),
            "unopenable history DB should disable freshness checks instead of panicking"
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_check_scope_varies_with_packages() -> TestResult<()> {
        let args_p1 = vec!["-p".into(), "sinex-db".into()];
        let args_p2 = vec!["-p".into(), "sinex-gateway".into()];
        let args_all = vec!["--all".into()];
        let args_lint = vec!["--lint".into()];
        let args_empty: Vec<String> = vec![];

        // Different packages → different scope
        assert_ne!(scope_key("check", &args_p1), scope_key("check", &args_p2));

        // -p vs --all → different scope
        assert_ne!(scope_key("check", &args_p1), scope_key("check", &args_all));

        // Lint flags don't affect scope (same compilation target)
        assert_eq!(
            scope_key("check", &args_lint),
            scope_key("check", &args_empty)
        );
        assert_eq!(
            scope_key("check", &["-p".into(), "sinex-db".into(), "--lint".into()]),
            scope_key("check", &["-p".into(), "sinex-db".into()])
        );

        Ok(())
    }

    #[sinex_test]
    async fn test_tree_fingerprint_fails_outside_git_repo() -> TestResult<()> {
        let dir = tempfile::tempdir()?;
        let error = tree_fingerprint_in(dir.path()).expect_err("expected non-repo to fail");
        assert!(
            error
                .to_string()
                .contains("git update-index -q --refresh failed")
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_scoped_tree_fingerprint_fails_outside_git_repo() -> TestResult<()> {
        let dir = tempfile::tempdir()?;
        let args = vec!["-p".into(), "xtask".into()];
        let error = scoped_tree_fingerprint_in(dir.path(), "check", &args)
            .expect_err("expected non-repo to fail");
        assert!(
            error
                .to_string()
                .contains("git update-index -q --refresh failed")
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_scoped_tree_fingerprint_succeeds_in_initialized_repo() -> TestResult<()> {
        let dir = tempfile::tempdir()?;
        run_git(&["init", "-q"], dir.path())?;
        run_git(&["config", "user.name", "Sinex Test"], dir.path())?;
        run_git(&["config", "user.email", "sinex@example.test"], dir.path())?;
        std::fs::create_dir_all(dir.path().join("xtask/src"))?;
        std::fs::write(dir.path().join("xtask/src/lib.rs"), "fn main() {}\n")?;
        run_git(&["add", "xtask/src/lib.rs"], dir.path())?;
        run_git(&["commit", "-qm", "init"], dir.path())?;
        std::fs::write(dir.path().join("xtask/src/lib.rs"), "fn main() { println!(\"dirty\"); }\n")?;

        let args = vec!["-p".into(), "xtask".into()];
        let fingerprint = scoped_tree_fingerprint_in(dir.path(), "check", &args)?;

        assert!(!fingerprint.is_empty());
        Ok(())
    }

    #[sinex_test]
    async fn test_tree_fingerprint_succeeds_in_dirty_repo() -> TestResult<()> {
        let dir = tempfile::tempdir()?;
        run_git(&["init", "-q"], dir.path())?;
        run_git(&["config", "user.name", "Sinex Test"], dir.path())?;
        run_git(&["config", "user.email", "sinex@example.test"], dir.path())?;
        std::fs::write(dir.path().join("tracked.txt"), "clean\n")?;
        run_git(&["add", "tracked.txt"], dir.path())?;
        run_git(&["commit", "-qm", "init"], dir.path())?;
        std::fs::write(dir.path().join("tracked.txt"), "dirty\n")?;

        let fingerprint = tree_fingerprint_in(dir.path())?;

        assert!(!fingerprint.is_empty());
        Ok(())
    }

    #[sinex_test]
    async fn test_build_release_different_scope() -> TestResult<()> {
        let args1: Vec<String> = vec![];
        let args2: Vec<String> = vec!["--release".into()];
        assert_ne!(scope_key("build", &args1), scope_key("build", &args2));
        Ok(())
    }

    // --- Queue and state serialization tests ---

    #[sinex_test]
    async fn test_queue_serialization_roundtrip() -> TestResult<()> {
        let state = CoordinationState {
            job_id: 42,
            pid: 1234,
            is_foreground: false,
            tree_fingerprint: "abc123".into(),
            scope_key: "def456".into(),
            started_at: "2026-01-01T00:00:00Z".into(),
            args: vec!["-p".into(), "sinex-db".into()],
            queue: vec![
                QueuedWork {
                    args: vec!["-p".into(), "sinex-gateway".into()],
                    is_foreground: false,
                    output_format: OutputFormat::Human,
                },
                QueuedWork {
                    args: vec!["-p".into(), "sinex-primitives".into()],
                    is_foreground: true,
                    output_format: OutputFormat::Json,
                },
            ],
        };

        let json = serde_json::to_string(&state)?;
        let deserialized: CoordinationState = serde_json::from_str(&json)?;
        assert_eq!(deserialized.queue.len(), 2);
        assert_eq!(deserialized.queue[0].args, vec!["-p", "sinex-gateway"]);
        assert_eq!(deserialized.queue[1].args, vec!["-p", "sinex-primitives"]);
        assert!(deserialized.queue[1].is_foreground);
        assert_eq!(deserialized.queue[0].output_format.as_cli_str(), "human");
        assert_eq!(deserialized.queue[1].output_format.as_cli_str(), "json");
        Ok(())
    }

    #[sinex_test]
    async fn test_queue_field_is_required() -> TestResult<()> {
        let json = r#"{
            "job_id": 1,
            "pid": 100,
            "is_foreground": false,
            "tree_fingerprint": "abc",
            "scope_key": "def",
            "started_at": "2026-01-01T00:00:00Z",
            "args": []
        }"#;
        let err = serde_json::from_str::<CoordinationState>(json).unwrap_err();
        assert!(
            err.to_string().contains("queue"),
            "expected missing queue error, got: {err}"
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_queue_fifo_ordering_via_state_file() -> TestResult<()> {
        let dir = tempfile::tempdir()?;
        let state_path = dir.path().join("test.state.json");

        // Create initial state with empty queue
        let state = CoordinationState {
            job_id: 1,
            pid: 100,
            is_foreground: false,
            tree_fingerprint: "fp1".into(),
            scope_key: "sk1".into(),
            started_at: "2026-01-01T00:00:00Z".into(),
            args: vec![],
            queue: Vec::new(),
        };
        write_state(&state_path, &state)?;

        // Queue three items
        let mut s = read_state(&state_path)?.expect("state should exist");
        s.queue.push(QueuedWork {
            args: vec!["first".into()],
            is_foreground: false,
            output_format: OutputFormat::Human,
        });
        s.queue.push(QueuedWork {
            args: vec!["second".into()],
            is_foreground: false,
            output_format: OutputFormat::Json,
        });
        s.queue.push(QueuedWork {
            args: vec!["third".into()],
            is_foreground: true,
            output_format: OutputFormat::Compact,
        });
        write_state(&state_path, &s)?;

        // Read back and verify FIFO order
        let s = read_state(&state_path)?.expect("state should exist");
        assert_eq!(s.queue.len(), 3);
        assert_eq!(s.queue[0].args, vec!["first"]);
        assert_eq!(s.queue[1].args, vec!["second"]);
        assert_eq!(s.queue[2].args, vec!["third"]);

        // Pop first (simulating handle_completion)
        let mut s = s;
        let popped = s.queue.remove(0);
        assert_eq!(popped.args, vec!["first"]);
        assert_eq!(s.queue.len(), 2);
        assert_eq!(s.queue[0].args, vec!["second"]);
        Ok(())
    }

    #[sinex_test]
    async fn test_is_process_alive_sentinel() -> TestResult<()> {
        assert!(!is_process_alive(0)); // Sentinel PID should always return false
        Ok(())
    }

    #[sinex_test]
    async fn test_is_process_alive_self() -> TestResult<()> {
        // Our own process should be alive
        let pid = std::process::id();
        assert!(is_process_alive(pid));
        Ok(())
    }

    #[sinex_test]
    async fn test_is_process_alive_nonexistent() -> TestResult<()> {
        // PID 999999999 is almost certainly not alive
        assert!(!is_process_alive(999_999_999));
        Ok(())
    }

    #[sinex_test]
    async fn test_extract_scope_args_build_package() -> TestResult<()> {
        let args: Vec<String> = vec!["-p".into(), "sinex-db".into(), "--release".into()];
        let scope = extract_scope_args("build", &args);
        assert!(scope.contains(&"-p".to_string()));
        assert!(scope.contains(&"sinex-db".to_string()));
        assert!(scope.contains(&"--release".to_string()));
        Ok(())
    }

    #[sinex_test]
    async fn test_extract_scope_args_build_combined() -> TestResult<()> {
        let args: Vec<String> = vec!["--package=sinex-db".into()];
        let scope = extract_scope_args("build", &args);
        assert!(scope.contains(&"--package=sinex-db".to_string()));
        Ok(())
    }

    #[sinex_test]
    async fn test_extract_scope_args_test_filter() -> TestResult<()> {
        let args: Vec<String> = vec![
            "-E".into(),
            "test(my_test)".into(),
            "-p".into(),
            "xtask".into(),
        ];
        let scope = extract_scope_args("test", &args);
        assert!(scope.contains(&"-E".to_string()));
        assert!(scope.contains(&"test(my_test)".to_string()));
        assert!(scope.contains(&"-p".to_string()));
        assert!(scope.contains(&"xtask".to_string()));
        Ok(())
    }

    #[sinex_test]
    async fn test_extract_scope_args_ignores_non_scope() -> TestResult<()> {
        let args: Vec<String> = vec![
            "-p".into(),
            "sinex-db".into(),
            "--fail-fast".into(),
            "--skip-preflight".into(),
            "--prime".into(),
        ];
        let scope = extract_scope_args("test", &args);
        assert_eq!(scope.len(), 2); // Only -p and sinex-db
        assert!(!scope.contains(&"--fail-fast".to_string()));
        assert!(!scope.contains(&"--skip-preflight".to_string()));
        assert!(!scope.contains(&"--prime".to_string()));
        Ok(())
    }

    #[sinex_test]
    async fn test_extract_scope_args_check_package() -> TestResult<()> {
        // -p is scope-relevant for check
        let args: Vec<String> = vec![
            "-p".into(),
            "sinex-db".into(),
            "--lint".into(),
            "--fmt".into(),
            "--forbidden".into(),
        ];
        let scope = extract_scope_args("check", &args);
        assert!(scope.contains(&"-p".to_string()));
        assert!(scope.contains(&"sinex-db".to_string()));
        // Lint flags are not scope-relevant
        assert!(!scope.contains(&"--lint".to_string()));
        assert!(!scope.contains(&"--fmt".to_string()));
        assert!(!scope.contains(&"--forbidden".to_string()));
        Ok(())
    }

    #[sinex_test]
    async fn test_extract_scope_args_check_all_flag() -> TestResult<()> {
        let args: Vec<String> = vec!["--all".into(), "--lint".into()];
        let scope = extract_scope_args("check", &args);
        assert!(scope.contains(&"--all".to_string()));
        assert!(!scope.contains(&"--lint".to_string()));
        Ok(())
    }

    #[sinex_test]
    async fn test_extract_scope_args_check_lint_only_empty() -> TestResult<()> {
        // Lint-only flags produce empty scope (same compilation target as bare check)
        let args: Vec<String> = vec!["--fmt".into(), "--lint".into(), "--forbidden".into()];
        let scope = extract_scope_args("check", &args);
        assert!(scope.is_empty());
        Ok(())
    }

    #[sinex_test]
    async fn test_state_write_read_roundtrip() -> TestResult<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("state.json");

        let state = CoordinationState {
            job_id: 42,
            pid: 1234,
            is_foreground: true,
            tree_fingerprint: "abc".into(),
            scope_key: "def".into(),
            started_at: "2026-01-01T00:00:00Z".into(),
            args: vec!["-p".into(), "foo".into()],
            queue: vec![QueuedWork {
                args: vec!["bar".into()],
                is_foreground: false,
                output_format: OutputFormat::Human,
            }],
        };

        write_state(&path, &state)?;
        let loaded = read_state(&path)?.expect("state should exist");

        assert_eq!(loaded.job_id, 42);
        assert_eq!(loaded.pid, 1234);
        assert!(loaded.is_foreground);
        assert_eq!(loaded.queue.len(), 1);
        assert_eq!(loaded.queue[0].args, vec!["bar"]);
        Ok(())
    }

    #[sinex_test]
    async fn test_read_state_missing_file() -> TestResult<()> {
        let result = read_state(std::path::Path::new("/nonexistent/path/state.json"))?;
        assert!(result.is_none());
        Ok(())
    }

    #[sinex_test]
    async fn test_read_state_corrupt_json() -> TestResult<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("state.json");
        fs::write(&path, "not json at all {{{")?;
        let result = read_state(&path)?;
        assert!(result.is_none());
        Ok(())
    }

    #[cfg(unix)]
    #[sinex_test]
    async fn test_state_surfaces_unreadable_state_path() -> TestResult<()> {
        let tempdir = tempfile::tempdir()?;
        let _state_guard = ScopedEnvGuard::set_path("SINEX_STATE_DIR", tempdir.path());
        let coordinator = JobCoordinator::new()?;
        let state_path = tempdir.path().join("coordinator/check.state.json");
        fs::create_dir_all(&state_path)?;

        let error = coordinator
            .state("check")
            .expect_err("directory state path must surface as unreadable");
        let message = format!("{error:#}");
        assert!(message.contains("failed to read coordinator state"));
        assert!(message.contains(state_path.display().to_string().as_str()));
        Ok(())
    }

    #[cfg(unix)]
    #[sinex_test]
    async fn test_request_surfaces_stale_state_cleanup_failures() -> TestResult<()> {
        let tempdir = tempfile::tempdir()?;
        let _state_guard = ScopedEnvGuard::set_path("SINEX_STATE_DIR", tempdir.path());
        let coordinator = JobCoordinator::new()?;
        let coordinator_dir = tempdir.path().join("coordinator");
        let state_path = coordinator_dir.join("check.state.json");
        let lock_path = coordinator_dir.join("check.lock");

        fs::write(&lock_path, [])?;
        write_state(
            &state_path,
            &CoordinationState {
                job_id: 41,
                pid: 999_999_999,
                is_foreground: false,
                tree_fingerprint: "old".into(),
                scope_key: "old".into(),
                started_at: "2026-01-01T00:00:00Z".into(),
                args: vec![],
                queue: Vec::new(),
            },
        )?;

        let original_mode = fs::metadata(&coordinator_dir)?.permissions().mode();
        let mut read_only = fs::metadata(&coordinator_dir)?.permissions();
        read_only.set_mode(0o555);
        fs::set_permissions(&coordinator_dir, read_only)?;

        let result = coordinator.request_with_format("check", &[], false, OutputFormat::Human);

        let mut restore = fs::metadata(&coordinator_dir)?.permissions();
        restore.set_mode(original_mode);
        fs::set_permissions(&coordinator_dir, restore)?;

        let error = result.expect_err("stale state cleanup failure must surface");
        let message = format!("{error:#}");
        assert!(message.contains("remove stale coordinator state before restart"));
        assert!(message.contains(state_path.display().to_string().as_str()));
        Ok(())
    }

    #[sinex_test]
    async fn test_cancel_process_sentinel_noop() -> TestResult<()> {
        // cancel_process(0) should be a no-op (sentinel PID)
        cancel_process(0); // Should not panic
        Ok(())
    }

    // --- coordination_to_result mapping tests ---

    fn json_ctx() -> CommandContext {
        CommandContext::new(
            crate::output::OutputWriter::new(crate::output::OutputFormat::Json),
            false,
            None,
            "coordinator",
        )
    }

    #[sinex_test]
    async fn test_coordination_to_result_fresh() -> TestResult<()> {
        let ctx = json_ctx();
        let result = coordination_fresh_result(
            42,
            "success",
            3.5,
            &ctx,
            FreshPackagesProbe {
                packages: vec!["sinex-db".into(), "xtask".into()],
                issue: None,
            },
        );

        assert!(result.is_success());
        let data = result.data.as_ref().expect("should have data");
        assert_eq!(data["action"], "fresh");
        assert_eq!(data["job_id"], 42);
        assert_eq!(data["cached_status"], "success");
        assert_eq!(data["cached_duration_secs"], 3.5);
        assert_eq!(data["compiled_packages"], serde_json::json!(["sinex-db", "xtask"]));
        assert_eq!(data["compiled_packages_issue"], serde_json::Value::Null);
        Ok(())
    }

    #[sinex_test]
    async fn test_coordination_fresh_result_surfaces_compiled_package_probe_errors() -> TestResult<()> {
        let ctx = json_ctx();
        let result = coordination_fresh_result(
            42,
            "success",
            3.5,
            &ctx,
            FreshPackagesProbe {
                packages: Vec::new(),
                issue: Some("probe exploded".into()),
            },
        );

        assert!(result.is_success());
        assert_eq!(result.warnings, vec!["probe exploded".to_string()]);
        let data = result.data.as_ref().expect("should have data");
        assert_eq!(data["compiled_packages"], serde_json::json!([]));
        assert_eq!(data["compiled_packages_issue"], "probe exploded");
        Ok(())
    }

    #[sinex_test]
    async fn test_fresh_packages_probe_from_result_reports_errors() -> TestResult<()> {
        let db_path = std::path::Path::new("/tmp/test-history.db");
        let probe = fresh_packages_probe_from_result(
            7,
            db_path,
            Err(color_eyre::eyre::eyre!("history exploded")),
        );
        assert!(probe.packages.is_empty());
        let issue = probe.issue.expect("probe failure should surface");
        assert!(issue.contains("failed to load compiled packages for fresh job 7"));
        assert!(issue.contains("/tmp/test-history.db"));
        assert!(issue.contains("history exploded"));
        Ok(())
    }

    #[sinex_test]
    async fn test_coordination_to_result_attached() -> TestResult<()> {
        let ctx = json_ctx();
        let coord = CoordinationResult::Attached { job_id: 99 };
        let result = coordination_to_result(&coord, &ctx);

        assert!(result.is_success());
        let data = result.data.as_ref().expect("should have data");
        assert_eq!(data["action"], "attached");
        assert_eq!(data["job_id"], 99);
        assert!(data["hint"].as_str().unwrap().contains("99"));
        Ok(())
    }

    #[sinex_test]
    async fn test_coordination_to_result_superseded() -> TestResult<()> {
        let ctx = json_ctx();
        let coord = CoordinationResult::Superseded {
            old_job_id: 10,
            new_job_id: 20,
        };
        let result = coordination_to_result(&coord, &ctx);

        assert!(result.is_success());
        let data = result.data.as_ref().expect("should have data");
        assert_eq!(data["action"], "superseded");
        assert_eq!(data["old_job_id"], 10);
        assert_eq!(data["new_job_id"], 20);
        Ok(())
    }

    #[sinex_test]
    async fn test_coordination_to_result_queued() -> TestResult<()> {
        let ctx = json_ctx();
        let coord = CoordinationResult::Queued { current_job_id: 55 };
        let result = coordination_to_result(&coord, &ctx);

        assert!(result.is_success());
        let data = result.data.as_ref().expect("should have data");
        assert_eq!(data["action"], "queued");
        assert_eq!(data["current_job_id"], 55);
        Ok(())
    }

    #[sinex_test]
    async fn test_coordination_to_result_started() -> TestResult<()> {
        let ctx = json_ctx();
        let coord = CoordinationResult::Started { job_id: -1 };
        let result = coordination_to_result(&coord, &ctx);

        assert!(result.is_success());
        let data = result.data.as_ref().expect("should have data");
        assert_eq!(data["action"], "started");
        assert_eq!(data["job_id"], -1);
        Ok(())
    }

    // --- extract_scope_args edge cases ---

    #[sinex_test]
    async fn test_extract_scope_args_build_short_combined() -> TestResult<()> {
        // -psinex-db (no space) should be captured as a combined flag
        let args: Vec<String> = vec!["-psinex-db".into()];
        let scope = extract_scope_args("build", &args);
        assert_eq!(scope, vec!["-psinex-db"]);
        Ok(())
    }

    #[sinex_test]
    async fn test_extract_scope_args_test_combined_filter() -> TestResult<()> {
        // -Etest(my_test) (no space) should be captured
        let args: Vec<String> = vec!["-Etest(my_test)".into()];
        let scope = extract_scope_args("test", &args);
        assert_eq!(scope, vec!["-Etest(my_test)"]);
        Ok(())
    }

    #[sinex_test]
    async fn test_extract_scope_args_test_heavy_flag() -> TestResult<()> {
        let args: Vec<String> = vec!["--heavy".into(), "-p".into(), "sinex-db".into()];
        let scope = extract_scope_args("test", &args);
        assert!(scope.contains(&"--heavy".to_string()));
        assert!(scope.contains(&"-p".to_string()));
        assert!(scope.contains(&"sinex-db".to_string()));
        assert_eq!(scope.len(), 3);
        Ok(())
    }

    #[sinex_test]
    async fn test_extract_scope_args_unknown_command() -> TestResult<()> {
        // Unknown commands should return empty scope
        let args: Vec<String> = vec!["-p".into(), "sinex-db".into(), "--release".into()];
        let scope = extract_scope_args("status", &args);
        assert!(scope.is_empty());
        Ok(())
    }

    #[sinex_test]
    async fn test_extract_scope_args_build_all_flag() -> TestResult<()> {
        let args: Vec<String> = vec!["--all".into()];
        let scope = extract_scope_args("build", &args);
        assert!(scope.contains(&"--all".to_string()));
        Ok(())
    }

    #[sinex_test]
    async fn test_coordination_result_serde_roundtrip() -> TestResult<()> {
        let variants = vec![
            CoordinationResult::Started { job_id: 1 },
            CoordinationResult::Attached { job_id: 2 },
            CoordinationResult::Fresh {
                job_id: 3,
                status: "success".into(),
                duration_secs: 1.5,
            },
            CoordinationResult::Superseded {
                old_job_id: 4,
                new_job_id: 5,
            },
            CoordinationResult::Queued { current_job_id: 6 },
        ];

        for variant in &variants {
            let json = serde_json::to_string(variant)?;
            let deserialized: CoordinationResult = serde_json::from_str(&json)?;
            // Re-serialize and compare JSON strings for equality
            let json2 = serde_json::to_string(&deserialized)?;
            assert_eq!(json, json2, "Roundtrip failed for: {json}");
        }
        Ok(())
    }

    #[sinex_test]
    async fn test_should_coordinate_test_list_flag() -> TestResult<()> {
        // --list and -l should both exclude coordination
        assert!(!JobCoordinator::should_coordinate(
            "test",
            &["--list".into()]
        ));
        assert!(!JobCoordinator::should_coordinate("test", &["-l".into()]));
        Ok(())
    }

    #[sinex_test]
    async fn test_should_coordinate_test_dry_run() -> TestResult<()> {
        assert!(!JobCoordinator::should_coordinate(
            "test",
            &["--dry-run".into()]
        ));
        Ok(())
    }

    // --- R1: Per-package fingerprinting ---

    #[sinex_test]
    async fn test_extract_explicit_packages_p_flag() -> TestResult<()> {
        let args = vec!["-p".into(), "sinex-db".into()];
        let pkgs = extract_explicit_packages("check", &args);
        assert_eq!(pkgs, vec!["sinex-db"]);
        Ok(())
    }

    #[sinex_test]
    async fn test_extract_explicit_packages_long_flag() -> TestResult<()> {
        let args = vec!["--package".into(), "sinex-gateway".into()];
        let pkgs = extract_explicit_packages("check", &args);
        assert_eq!(pkgs, vec!["sinex-gateway"]);
        Ok(())
    }

    #[sinex_test]
    async fn test_extract_explicit_packages_equals_form() -> TestResult<()> {
        let args = vec!["--package=sinex-primitives".into()];
        let pkgs = extract_explicit_packages("check", &args);
        assert_eq!(pkgs, vec!["sinex-primitives"]);
        Ok(())
    }

    #[sinex_test]
    async fn test_extract_explicit_packages_multiple() -> TestResult<()> {
        let args = vec![
            "-p".into(),
            "sinex-db".into(),
            "-p".into(),
            "sinex-gateway".into(),
        ];
        let pkgs = extract_explicit_packages("check", &args);
        assert_eq!(pkgs.len(), 2);
        assert!(pkgs.contains(&"sinex-db".to_string()));
        assert!(pkgs.contains(&"sinex-gateway".to_string()));
        Ok(())
    }

    #[sinex_test]
    async fn test_extract_explicit_packages_none() -> TestResult<()> {
        // No -p flag: returns empty (will use workspace fingerprint)
        let args: Vec<String> = vec!["--lint".into(), "--all".into()];
        let pkgs = extract_explicit_packages("check", &args);
        assert!(pkgs.is_empty());
        Ok(())
    }

    #[sinex_test]
    async fn test_extract_explicit_packages_unknown_command() -> TestResult<()> {
        // Non-compilable commands return empty (fix, etc.)
        let args = vec!["-p".into(), "sinex-db".into()];
        let pkgs = extract_explicit_packages("fix", &args);
        assert!(pkgs.is_empty());
        Ok(())
    }

    #[sinex_test]
    async fn test_package_to_path_well_known() -> TestResult<()> {
        assert_eq!(package_to_path("sinexctl"), "crate/cli/");
        assert_eq!(package_to_path("xtask"), "xtask/");
        assert_eq!(package_to_path("sinex-e2e-tests"), "tests/e2e/");
        Ok(())
    }

    #[sinex_test]
    async fn test_package_to_path_known_crate() -> TestResult<()> {
        // sinex-primitives should resolve to crate/lib/sinex-primitives/
        let path = package_to_path("sinex-primitives");
        assert!(
            path.starts_with("crate/"),
            "expected crate/ prefix, got: {path}"
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_package_to_path_unknown_falls_back() -> TestResult<()> {
        // Unknown package should fall back to "crate/" (broad, safe)
        let path = package_to_path("nonexistent-package-xyz");
        assert_eq!(path, "crate/");
        Ok(())
    }

    // --- R2: Workflow dependency graph ---

    #[sinex_test]
    async fn test_workflow_sequence_test() -> TestResult<()> {
        // test depends on check → sequence should be ["check", "test"]
        let seq = WorkflowGraph::sequence_to("test");
        assert_eq!(seq, vec!["check", "test"]);
        Ok(())
    }

    #[sinex_test]
    async fn test_workflow_sequence_check() -> TestResult<()> {
        // check has no prerequisites
        let seq = WorkflowGraph::sequence_to("check");
        assert_eq!(seq, vec!["check"]);
        Ok(())
    }

    #[sinex_test]
    async fn test_workflow_sequence_build() -> TestResult<()> {
        // build has no declared prerequisites
        let seq = WorkflowGraph::sequence_to("build");
        assert_eq!(seq, vec!["build"]);
        Ok(())
    }

    #[sinex_test]
    async fn test_workflow_sequence_deduplicates() -> TestResult<()> {
        // No duplicates even with shared prereqs
        let seq = WorkflowGraph::sequence_to("test");
        let unique: std::collections::HashSet<_> = seq.iter().collect();
        assert_eq!(
            seq.len(),
            unique.len(),
            "sequence should not have duplicates"
        );
        Ok(())
    }

    // ────────────────────────────────────────────────────────────────────────
    // Property tests — scope_key invariants
    // ────────────────────────────────────────────────────────────────────────

    use crate::sandbox::sinex_proptest;
    use proptest::prelude::*;

    sinex_proptest! {
        /// scope_key is deterministic: identical inputs always produce the same hash.
        ///
        /// This is the foundational invariant — the coordinator's dedup logic
        /// relies on the same work always producing the same scope key so that
        /// concurrent agents attach to the same running job rather than spawning
        /// duplicates.
        fn prop_scope_key_is_deterministic(
            pkg in "[a-z][a-z0-9-]{2,15}"
        ) -> TestResult<()> {
            let args: Vec<String> = vec!["-p".to_string(), pkg];
            prop_assert_eq!(scope_key("check", &args), scope_key("check", &args));
            Ok(())
        }

        /// Non-scope flags do not change the scope key.
        ///
        /// Flags like --lint, --fmt, --forbidden, --bg, --json change *how* to run
        /// the command but not *what* is being compiled. Two agents targeting the
        /// same package must share one background job even if one passes --lint and
        /// the other doesn't.
        fn prop_scope_key_ignores_non_scope_flags(
            pkg in "[a-z][a-z0-9-]{2,15}",
            extra in prop_oneof![
                Just("--lint"),
                Just("--fmt"),
                Just("--forbidden"),
                Just("--full"),
                Just("--bg"),
                Just("--json"),
            ]
        ) -> TestResult<()> {
            let base: Vec<String> = vec!["-p".to_string(), pkg.clone()];
            let with_flag = {
                let mut v = base.clone();
                v.push(extra.to_string());
                v
            };
            prop_assert_eq!(
                scope_key("check", &base),
                scope_key("check", &with_flag),
                "non-scope flag '{}' must not change the scope key", extra
            );
            Ok(())
        }

        /// Distinct package names (non-overlapping lengths) produce distinct scope keys.
        ///
        /// Uses length-partitioned strategies — pkg_a is 3–9 chars, pkg_b is 10–15
        /// chars — so they can never be equal, avoiding prop_assume rejection while
        /// still exercising SHA256 collision resistance on distinct inputs.
        fn prop_scope_key_distinct_packages_differ(
            pkg_a in "[a-z][a-z0-9]{2,8}",
            pkg_b in "[a-z][a-z0-9]{9,14}"
        ) -> TestResult<()> {
            let ka = scope_key("check", &["-p".to_string(), pkg_a]);
            let kb = scope_key("check", &["-p".to_string(), pkg_b]);
            prop_assert_ne!(ka, kb, "distinct packages must produce distinct scope keys");
            Ok(())
        }

        /// --all scope key differs from any -p scoped key.
        ///
        /// A workspace-wide check (--all) and a package-scoped check (-p foo)
        /// are genuinely different work units. The coordinator must never attach
        /// an --all job to a -p job or vice versa.
        fn prop_scope_key_all_differs_from_scoped(
            pkg in "[a-z][a-z0-9]{2,8}"
        ) -> TestResult<()> {
            let scoped   = vec!["-p".to_string(), pkg];
            let all_args = vec!["--all".to_string()];
            prop_assert_ne!(
                scope_key("check", &scoped),
                scope_key("check", &all_args),
                "--all scope key must differ from package-scoped key"
            );
            Ok(())
        }
    }
}
