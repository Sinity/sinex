//! Scoped job coordination for concurrent xtask processes.
//!
//! When multiple agents call `cargo xtask {check,test,build} --bg` concurrently,
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

use anyhow::{Context, Result};
use nix::fcntl::{flock, FlockArg};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs;
use std::os::fd::AsRawFd;
use std::path::PathBuf;

use crate::command::{CommandContext, CommandResult};
use crate::config::config;
use crate::history::InvocationStatus;

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
    /// Args for a queued follow-up job (if any).
    pub queued: Option<QueuedWork>,
}

/// A queued job waiting for the current one to finish.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueuedWork {
    pub args: Vec<String>,
    pub is_foreground: bool,
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
    pub fn should_coordinate(command: &str, args: &[String]) -> bool {
        match command {
            "check" | "build" => true,
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
        let lock_path = self.locks_dir.join(format!("{command}.lock"));
        let state_path = self.locks_dir.join(format!("{command}.state.json"));

        // Open/create lock file and acquire exclusive lock
        let lock_file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)
            .with_context(|| format!("failed to open lock file: {}", lock_path.display()))?;

        flock(lock_file.as_raw_fd(), FlockArg::LockExclusive)
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
                    &tree_fingerprint,
                    &scope_key,
                    &state,
                    &state_path,
                )?
            } else {
                // Process died — clean up stale state and start fresh
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
            if command != "test" {
                if let Some(fresh) = self.check_fresh(command, &tree_fingerprint, &scope_key) {
                    return Ok(fresh);
                }
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

    /// Called when a coordinated job completes. Clears state, returns queued work.
    pub fn handle_completion(&self, command: &str) -> Result<Option<QueuedWork>> {
        let lock_path = self.locks_dir.join(format!("{command}.lock"));
        let state_path = self.locks_dir.join(format!("{command}.state.json"));

        let lock_file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)?;

        flock(lock_file.as_raw_fd(), FlockArg::LockExclusive)?;

        let state = read_state(&state_path);
        let queued = state.and_then(|s| s.queued);

        // Clear state
        let _ = fs::remove_file(&state_path);

        Ok(queued)
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
                self.queue_behind(state, args, is_foreground, state_path)?;
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
            self.queue_behind(state, args, is_foreground, state_path)?;
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

        if let Some(db) = db {
            if let Ok(Some(last)) = db.get_last_completed_with_fingerprint(command) {
                if last.tree_fingerprint.as_deref() == Some(tree_fingerprint)
                    && last.scope_key.as_deref() == Some(scope_key)
                    && last.status == InvocationStatus::Success
                {
                    return Some(CoordinationResult::Fresh {
                        job_id: last.id,
                        status: "success".to_string(),
                        duration_secs: last.duration_secs.unwrap_or(0.0),
                    });
                }
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
            queued: None,
        };

        write_state(state_path, &state)?;

        Ok(CoordinationResult::Started { job_id: -1 })
    }

    fn queue_behind(
        &self,
        state: &CoordinationState,
        args: &[String],
        is_foreground: bool,
        state_path: &std::path::Path,
    ) -> Result<()> {
        // Update state to include queued work
        let mut updated = state.clone();
        updated.queued = Some(QueuedWork {
            args: args.to_vec(),
            is_foreground,
        });
        write_state(state_path, &updated)?;
        Ok(())
    }

    /// Update the state file with the actual job ID and PID after spawning.
    pub fn update_state(&self, command: &str, job_id: i64, pid: u32) -> Result<()> {
        let lock_path = self.locks_dir.join(format!("{command}.lock"));
        let state_path = self.locks_dir.join(format!("{command}.state.json"));

        let lock_file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)?;

        flock(lock_file.as_raw_fd(), FlockArg::LockExclusive)?;

        if let Some(mut state) = read_state(&state_path) {
            state.job_id = job_id;
            state.pid = pid;
            write_state(&state_path, &state)?;
        }

        Ok(())
    }
}

// --- Utility functions ---

/// Compute tree fingerprint: sha256 of `git status --porcelain` output.
///
/// Properties: deterministic (same tree → same hash), fast (~50ms),
/// captures staged, unstaged, and untracked changes.
fn tree_fingerprint() -> Result<String> {
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
            _ => false,
        }
    }

    fn is_standalone_flag(command: &str, arg: &str) -> bool {
        // Flags that are scope-relevant on their own (no value)
        match command {
            "build" => arg == "--release" || arg.starts_with("--all"),
            "test" => arg == "--heavy" || arg == "--include-ignored" || arg == "--all",
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
            _ => false,
        }
    }

    if command == "check" {
        return vec![]; // All check runs are same scope
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

    // Wait up to 5 seconds for graceful exit
    for _ in 0..50 {
        std::thread::sleep(std::time::Duration::from_millis(100));
        if unsafe { libc::kill(pid, 0) } != 0 {
            return; // Process exited
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
    if let Ok(db) = crate::history::HistoryDb::open(&cfg.history_db_path()) {
        let _ = db.finish_invocation(job_id, InvocationStatus::Cancelled, None, 0.0);
    }
}

fn read_state(path: &std::path::Path) -> Option<CoordinationState> {
    fs::read_to_string(path)
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())
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
    if JobCoordinator::should_coordinate(command, args) {
        if let Ok(coordinator) = JobCoordinator::new() {
            match coordinator.request(command, args, false) {
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
                Err(_) => {} // Coordinator failed — spawn directly
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
    if let Some(data) = &bg_result.data {
        if let (Some(job_id), Some(pid)) = (data["job_id"].as_i64(), data["pid"].as_u64()) {
            if let Ok(coordinator) = JobCoordinator::new() {
                let _ = coordinator.update_state(command, job_id, pid as u32);
            }
        }
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
            if ctx.is_human() {
                println!("✅ Fresh: last check already validated this code state (job {job_id}, {status} in {duration_secs:.1}s)");
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
            if ctx.is_human() {
                println!("🔗 Attached: identical check already running (job {job_id})");
                println!("   Monitor: cargo xtask jobs status {job_id}");
            }
            CommandResult::success()
                .with_message(format!("Attached to running job {job_id}"))
                .with_data(serde_json::json!({
                    "action": "attached",
                    "job_id": job_id,
                    "hint": format!("Monitor with: cargo xtask jobs status {job_id}"),
                }))
        }
        CoordinationResult::Superseded {
            old_job_id,
            new_job_id,
        } => {
            if ctx.is_human() {
                println!("♻ Superseded: cancelled stale job {old_job_id}, starting fresh job {new_job_id}");
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
pub fn compute_scope_key(command: &str, args: &[String]) -> String {
    scope_key(command, args)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_should_coordinate() {
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
        assert!(!JobCoordinator::should_coordinate("fix", &[]));
    }

    #[test]
    fn test_scope_key_deterministic() {
        let args1 = vec!["-p".into(), "sinex-db".into(), "--all".into()];
        let args2 = vec!["--all".into(), "-p".into(), "sinex-db".into()];
        assert_eq!(scope_key("test", &args1), scope_key("test", &args2));
    }

    #[test]
    fn test_scope_key_different() {
        let args1 = vec!["-p".into(), "sinex-db".into()];
        let args2 = vec!["-p".into(), "sinex-gateway".into()];
        assert_ne!(scope_key("test", &args1), scope_key("test", &args2));
    }

    #[test]
    fn test_scope_key_ignores_irrelevant() {
        // --fail-fast, --skip-preflight, --prime are NOT scope-relevant for tests
        let args1 = vec!["-p".into(), "sinex-db".into()];
        let args2 = vec![
            "-p".into(),
            "sinex-db".into(),
            "--fail-fast".into(),
            "--skip-preflight".into(),
        ];
        assert_eq!(scope_key("test", &args1), scope_key("test", &args2));
    }

    #[test]
    fn test_check_scope_always_same() {
        let args1: Vec<String> = vec!["--lint=true".into()];
        let args2: Vec<String> = vec!["--skip-fmt".into()];
        assert_eq!(scope_key("check", &args1), scope_key("check", &args2));
    }

    #[test]
    fn test_build_release_different_scope() {
        let args1: Vec<String> = vec![];
        let args2: Vec<String> = vec!["--release".into()];
        assert_ne!(scope_key("build", &args1), scope_key("build", &args2));
    }
}
