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
use std::path::PathBuf;

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

        // Compute fingerprint and scope
        let tree_fingerprint = tree_fingerprint()?;
        let scope_key = scope_key(command, args);

        // Read current state (if any)
        let current_state = read_state(&state_path);

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
                let _ = fs::remove_file(&state_path);
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

        let state = read_state(&state_path);

        match state {
            Some(mut state) if !state.queue.is_empty() => {
                // Pop first queued item (FIFO)
                let next = state.queue.remove(0);

                if state.queue.is_empty() {
                    // No more items — delete state file
                    let _ = fs::remove_file(&state_path);
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
                let _ = fs::remove_file(&state_path);
                Ok(None)
            }
        }
    }

    /// Read current state for display.
    pub fn state(&self, command: &str) -> Result<Option<CoordinationState>> {
        let state_path = self.locks_dir.join(format!("{command}.state.json"));
        Ok(read_state(&state_path))
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
                let _ = fs::remove_file(state_path);

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
        let db = crate::history::HistoryDb::open(&cfg.history_db_path()).ok();

        if let Some(db) = db
            && let Ok(Some(last)) = db.get_last_completed_with_fingerprint(command)
            && last.tree_fingerprint.as_deref() == Some(tree_fingerprint)
            && last.scope_key.as_deref() == Some(scope_key)
            && last.status == InvocationStatus::Success
        {
            return Some(CoordinationResult::Fresh {
                job_id: last.id,
                status: "success".to_string(),
                duration_secs: last.duration_secs.unwrap_or(0.0),
            });
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

        if let Some(mut state) = read_state(&state_path) {
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
fn tree_fingerprint() -> Result<String> {
    // Refresh the git index so status reflects actual filesystem state.
    // Without this, rapid edits within the same second can go undetected
    // because git caches stat data (mtime, size) in the index.
    let _ = std::process::Command::new("git")
        .args(["update-index", "--refresh"])
        .output();

    let output = std::process::Command::new("git")
        .args(["status", "--porcelain"])
        .output()
        .context("failed to run git status")?;

    let mut hasher = Sha256::new();
    hasher.update(&output.stdout);
    Ok(format!("{:x}", hasher.finalize()))
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

fn read_state(path: &std::path::Path) -> Option<CoordinationState> {
    let content = fs::read_to_string(path).ok()?;
    match serde_json::from_str::<CoordinationState>(&content) {
        Ok(state) => Some(state),
        Err(e) => {
            tracing::warn!(
                target: "xtask::coordinator",
                path = %path.display(),
                error = %e,
                "corrupt coordinator state — treating as empty"
            );
            None
        }
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
    if let Some(data) = &bg_result.data
        && let (Some(job_id), Some(pid)) = (data["job_id"].as_i64(), data["pid"].as_u64())
        && let Ok(coordinator) = JobCoordinator::new()
    {
        let _ = coordinator.update_state(command, job_id, pid as u32);
    }
}

/// Convert a coordination result to a command result for the --bg path.
pub fn coordination_to_result(result: &CoordinationResult, ctx: &CommandContext) -> CommandResult {
    match result {
        CoordinationResult::Fresh {
            job_id,
            status,
            duration_secs,
        } => {
            tracing::info!(
                target: "xtask::coordinator",
                job_id = job_id,
                action = "fresh",
                cached_status = status,
                cached_duration_secs = duration_secs,
                "coordinator: fresh — last check already validated this code state"
            );
            if ctx.is_human() {
                // H5: Include which packages were validated in the fresh message
                let packages = {
                    let cfg = config();
                    crate::history::HistoryDb::open(&cfg.history_db_path())
                        .ok()
                        .and_then(|db| db.get_compiled_packages_for_invocation(*job_id).ok())
                        .unwrap_or_default()
                };
                if packages.is_empty() {
                    println!(
                        "✅ Fresh: last check already validated this code state (job {job_id}, {status} in {duration_secs:.1}s)"
                    );
                } else {
                    let pkg_list = if packages.len() <= 4 {
                        packages.join(", ")
                    } else {
                        format!("{}, …+{}", packages[..3].join(", "), packages.len() - 3)
                    };
                    println!(
                        "✅ Fresh: last check already validated {pkg_list} (job {job_id}, {duration_secs:.1}s)"
                    );
                }
            }
            CommandResult::success()
                .with_message(format!("Fresh result from job {job_id}"))
                .with_data(serde_json::json!({
                    "action": "fresh",
                    "job_id": job_id,
                    "cached_status": status,
                    "cached_duration_secs": duration_secs,
                }))
        }
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

/// Tree fingerprint exposed for callers that need it (e.g., recording in history DB).
pub fn current_tree_fingerprint() -> Result<String> {
    tree_fingerprint()
}

/// Scope key exposed for callers (e.g., recording in history DB).
#[must_use]
pub fn compute_scope_key(command: &str, args: &[String]) -> String {
    scope_key(command, args)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::sinex_test;

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
        let mut s = read_state(&state_path).expect("state should exist");
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
        let s = read_state(&state_path).expect("state should exist");
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
        let loaded = read_state(&path).expect("state should exist");

        assert_eq!(loaded.job_id, 42);
        assert_eq!(loaded.pid, 1234);
        assert!(loaded.is_foreground);
        assert_eq!(loaded.queue.len(), 1);
        assert_eq!(loaded.queue[0].args, vec!["bar"]);
        Ok(())
    }

    #[sinex_test]
    async fn test_read_state_missing_file() -> TestResult<()> {
        let result = read_state(std::path::Path::new("/nonexistent/path/state.json"));
        assert!(result.is_none());
        Ok(())
    }

    #[sinex_test]
    async fn test_read_state_corrupt_json() -> TestResult<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("state.json");
        fs::write(&path, "not json at all {{{")?;
        let result = read_state(&path);
        assert!(result.is_none());
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
        )
    }

    #[sinex_test]
    async fn test_coordination_to_result_fresh() -> TestResult<()> {
        let ctx = json_ctx();
        let coord = CoordinationResult::Fresh {
            job_id: 42,
            status: "success".into(),
            duration_secs: 3.5,
        };
        let result = coordination_to_result(&coord, &ctx);

        assert!(result.is_success());
        let data = result.data.as_ref().expect("should have data");
        assert_eq!(data["action"], "fresh");
        assert_eq!(data["job_id"], 42);
        assert_eq!(data["cached_status"], "success");
        assert_eq!(data["cached_duration_secs"], 3.5);
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
}
