//! Scoped job coordination for concurrent xtask processes.
//!
//! When multiple agents call `xtask {check,test,build,fix,vm} --bg` concurrently,
//! they all compete for the same cargo/Nix worker surface, causing redundant
//! recompilation and host pressure spikes.
//!
//! The coordinator deduplicates work using a **coordination scope** inside a shared
//! **coordination family**. Two requests are "the same work" if they have the same
//! command, tree fingerprint (git working tree state), and scope key
//! (command-specific parameters). Different heavy commands still share one family
//! lane, so they queue instead of stampeding the machine together.
//!
//! ## Decision Matrix
//!
//! 1. **Excluded** — Non-coordinatable modes (debug, fuzz, coverage, etc.) run directly.
//! 2. **Fresh** — (check/build only) Last completed job has same fingerprint+scope → return cached.
//! 3. **Attach** — Running job has same fingerprint+scope → return its job ID.
//! 4. **Supersede** — Running bg job has same scope but different fingerprint → cancel + restart.
//! 5. **Queue** — Running job has different scope → queue after it.
//! 6. **Start** — No running job → start new.

use color_eyre::eyre::{Result, WrapErr, bail, eyre};
use nix::fcntl::{Flock, FlockArg};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::command::{CommandContext, CommandResult};
use crate::config::config;
use crate::history::JobLifecycleStatus;
use crate::output::OutputFormat;

const SHARED_FINGERPRINT_INPUTS: &[&str] = &[
    "Cargo.toml",
    "Cargo.lock",
    "rust-toolchain",
    "rust-toolchain.toml",
    "flake.nix",
    "flake.lock",
    ".config/nextest.toml",
];

/// Human/machine-readable explanation of the current coordinator freshness key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FreshnessExplanation {
    pub command: String,
    pub args: Vec<String>,
    pub should_coordinate: bool,
    pub fresh_reuse_enabled: bool,
    pub proof_kind: String,
    pub scope_key: String,
    pub tree_fingerprint: String,
    pub scope: FreshnessScopeExplanation,
    pub shared_inputs: Vec<String>,
}

/// Scope inputs that feed a coordinator freshness fingerprint.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum FreshnessScopeExplanation {
    Workspace,
    Packages { packages: Vec<PackageScopeInput> },
}

/// Package-to-path mapping used by scoped fingerprints.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PackageScopeInput {
    pub package: String,
    pub path: String,
}

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
    /// Last completed invocation already validated this scope + tree.
    Fresh {
        invocation_id: i64,
        status: String,
        duration_secs: f64,
    },
    /// Different-scope job running — queued after it.
    Queued { current_job_id: i64 },
}

/// Persisted coordination state for a command class.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CoordinationState {
    #[serde(default)]
    pub command: String,
    pub job_id: i64,
    pub pid: u32,
    /// `/proc/{pid}/stat` start_ticks at spawn time. Used to detect PID reuse
    /// before sending signals — if the current process at this PID has different
    /// start_ticks, it is an unrelated process and must not be killed.
    #[serde(default)]
    pub process_start_ticks: u64,
    pub is_foreground: bool,
    #[serde(default)]
    pub tree_fingerprint: String,
    #[serde(default)]
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
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct QueuedWork {
    #[serde(default)]
    pub command: String,
    pub args: Vec<String>,
    pub is_foreground: bool,
    pub output_format: OutputFormat,
    #[serde(default)]
    pub tree_fingerprint: String,
    #[serde(default)]
    pub scope_key: String,
    /// Why this work is queued, e.g. "behind running check job 42" (#1163).
    #[serde(default)]
    pub reason: String,
}

/// Scoped job coordinator.
///
/// Uses POSIX advisory locks (`flock`) for mutual exclusion and JSON state files
/// for cross-process state. Lock is held ~100ms during the coordination decision,
/// NOT for the duration of the cargo build.
pub struct JobCoordinator {
    locks_dir: PathBuf,
}

/// Emit a structured tracing event for each coordination decision.
fn log_coordination_decision(
    command: &str,
    scope_key: &str,
    tree_fingerprint: &str,
    result: &CoordinationResult,
) {
    let (decision, job_id, invocation_id) = coordination_decision_fields(result);
    tracing::info!(
        target: "xtask::coordinator",
        command = command,
        decision = decision,
        scope_key = %scope_key,
        tree_fingerprint = %tree_fingerprint,
        job_id = job_id,
        invocation_id = invocation_id,
        "coordinator decision"
    );
}

fn coordination_decision_fields(
    result: &CoordinationResult,
) -> (&'static str, Option<i64>, Option<i64>) {
    match result {
        CoordinationResult::Fresh { invocation_id, .. } => ("fresh", None, Some(*invocation_id)),
        CoordinationResult::Started { job_id } => ("started", Some(*job_id), None),
        CoordinationResult::Attached { job_id } => ("attached", Some(*job_id), None),
        CoordinationResult::Superseded { new_job_id, .. } => {
            ("superseded", Some(*new_job_id), None)
        }
        CoordinationResult::Queued { current_job_id } => ("queued", Some(*current_job_id), None),
    }
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
            "check" | "fix" => true,
            "build" => !args.iter().any(|arg| arg == "--dry-run"),
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
            "vm" => !args.iter().any(|a| a == "--list"),
            _ => false,
        }
    }

    fn state_path_for(&self, command: &str) -> PathBuf {
        self.locks_dir
            .join(format!("{}.state.json", coordination_family(command)))
    }

    fn lock_path_for(&self, command: &str) -> PathBuf {
        self.locks_dir
            .join(format!("{}.lock", coordination_family(command)))
    }

    /// Core coordination: request a coordinated job.
    ///
    /// Acquires the command-class lock, reads state, applies the decision matrix,
    /// and returns the coordination result. Lock is held briefly (~100ms).
    pub fn request(
        &self,
        command: &str,
        spawn_args: &[String],
        scope_args: &[String],
        is_foreground: bool,
    ) -> Result<CoordinationResult> {
        self.request_with_format(
            command,
            spawn_args,
            scope_args,
            is_foreground,
            OutputFormat::Human,
        )
    }

    /// Core coordination with explicit output format propagation.
    ///
    /// `output_format` is persisted for queued work so follow-up jobs preserve
    /// caller semantics (notably `--json`) when eventually spawned.
    pub fn request_with_format(
        &self,
        command: &str,
        spawn_args: &[String],
        scope_args: &[String],
        is_foreground: bool,
        output_format: OutputFormat,
    ) -> Result<CoordinationResult> {
        self.request_with_format_policy(
            command,
            spawn_args,
            scope_args,
            is_foreground,
            output_format,
            true,
        )
    }

    fn request_with_format_policy(
        &self,
        command: &str,
        spawn_args: &[String],
        scope_args: &[String],
        is_foreground: bool,
        output_format: OutputFormat,
        allow_fresh_reuse: bool,
    ) -> Result<CoordinationResult> {
        let lock_path = self.lock_path_for(command);
        let state_path = self.state_path_for(command);

        // Open/create lock file and acquire exclusive lock
        let lock_file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)
            .with_context(|| format!("failed to open lock file: {}", lock_path.display()))?;

        let _lock_file = lock_exclusive_retry(lock_file)
            .with_context(|| format!("failed to acquire lock: {}", lock_path.display()))?;

        // R1: Compute scoped fingerprint (per-package when -p is specified, whole-workspace otherwise)
        let tree_fingerprint = scoped_tree_fingerprint(command, scope_args)?;
        let scope_key = scope_key(command, scope_args);

        // Read current state (if any)
        let current_state = read_state(&state_path)?;

        let result = if let Some(state) = current_state {
            // There's an existing state — check if process is still alive
            if is_process_alive(state.pid) {
                self.handle_running_job(
                    command,
                    spawn_args,
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
                self.queue_behind(
                    command,
                    &state,
                    spawn_args,
                    is_foreground,
                    output_format,
                    &tree_fingerprint,
                    &scope_key,
                    &state_path,
                )?;
                CoordinationResult::Queued {
                    current_job_id: state.job_id,
                }
            } else {
                // Process died (or reservation is stale) — clean up and start fresh
                remove_state_file(&state_path, "remove stale coordinator state before restart")?;
                self.start_new_job(
                    command,
                    spawn_args,
                    is_foreground,
                    &tree_fingerprint,
                    &scope_key,
                    &state_path,
                )?
            }
        } else {
            // No state — check for fresh result (check/build only), then start new
            if allow_fresh_reuse
                && supports_fresh_reuse_for(command, spawn_args)
                && let Some(fresh) =
                    self.check_fresh(command, spawn_args, &tree_fingerprint, &scope_key)
            {
                // R5: Log fresh decision with structured fields
                let invocation_id = match &fresh {
                    CoordinationResult::Fresh { invocation_id, .. } => *invocation_id,
                    _ => -1,
                };
                tracing::info!(
                    target: "xtask::coordinator",
                    command = command,
                    decision = "fresh",
                    scope_key = %scope_key,
                    tree_fingerprint = %tree_fingerprint,
                    invocation_id = invocation_id,
                    "coordinator: fresh — no recompilation needed"
                );
                return Ok(fresh);
            }
            self.start_new_job(
                command,
                spawn_args,
                is_foreground,
                &tree_fingerprint,
                &scope_key,
                &state_path,
            )?
        };

        // R5: Log every coordination decision with structured fields.
        log_coordination_decision(command, &scope_key, &tree_fingerprint, &result);

        // Lock released on drop of lock_file
        Ok(result)
    }

    /// Called when a coordinated job completes. Pops next queued work (FIFO).
    ///
    /// If more items remain in the queue, the state file is preserved with
    /// sentinel values (job_id=-1, pid=0) — the caller must update via
    /// `update_state()` after spawning the returned work.
    pub fn handle_completion(&self, command: &str) -> Result<Option<QueuedWork>> {
        let lock_path = self.lock_path_for(command);
        let state_path = self.state_path_for(command);

        let lock_file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)?;

        let _lock_file = lock_exclusive_retry(lock_file)?;

        let state = read_state(&state_path)?;

        match state {
            Some(mut state) if !state.queue.is_empty() => {
                // Pop first queued item (FIFO)
                let next = state.queue.remove(0);
                let next_command = if next.command.is_empty() {
                    state.command.clone()
                } else {
                    next.command.clone()
                };
                // Preserve a sentinel reservation for the promoted work even when it is the
                // final queued item. `update_state()` needs a state file to replace with the
                // real job id/pid after the spawn succeeds.
                state.command = next_command;
                state.job_id = -1;
                state.pid = 0;
                state.is_foreground = next.is_foreground;
                state.tree_fingerprint.clone_from(&next.tree_fingerprint);
                state.scope_key.clone_from(&next.scope_key);
                state.args.clone_from(&next.args);
                state.started_at = sinex_primitives::temporal::Timestamp::now().format_rfc3339();
                write_state(&state_path, &state)?;

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
        let state_path = self.state_path_for(command);
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
        let running_command = if state.command.is_empty() {
            command
        } else {
            state.command.as_str()
        };

        if running_command == command
            && state.scope_key == scope_key
            && state.tree_fingerprint == tree_fingerprint
        {
            // Same scope + same tree → ATTACH
            Ok(CoordinationResult::Attached {
                job_id: state.job_id,
            })
        } else if running_command == command && state.scope_key == scope_key {
            // Same scope, different tree → SUPERSEDE (if bg), QUEUE (if fg)
            if state.is_foreground {
                // Don't cancel interactive foreground jobs — queue instead
                self.queue_behind(
                    command,
                    state,
                    args,
                    is_foreground,
                    output_format,
                    tree_fingerprint,
                    scope_key,
                    state_path,
                )?;
                Ok(CoordinationResult::Queued {
                    current_job_id: state.job_id,
                })
            } else {
                // Cancel stale bg job and start fresh
                let old_job_id = state.job_id;
                cancel_process(state.pid, state.process_start_ticks);
                mark_cancelled(old_job_id)?;
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
            self.queue_behind(
                command,
                state,
                args,
                is_foreground,
                output_format,
                tree_fingerprint,
                scope_key,
                state_path,
            )?;
            Ok(CoordinationResult::Queued {
                current_job_id: state.job_id,
            })
        }
    }

    fn check_fresh(
        &self,
        command: &str,
        args: &[String],
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

        let proof_kind = proof_kind(command, args);

        if command == "test" {
            match db.get_successful_reusable_test_proof_unit(
                &proof_kind,
                tree_fingerprint,
                scope_key,
            ) {
                Ok(Some(unit)) => {
                    return Some(CoordinationResult::Fresh {
                        invocation_id: unit.invocation_id,
                        status: "success".to_string(),
                        duration_secs: unit.duration_secs.unwrap_or(0.0),
                    });
                }
                Ok(None) => {}
                Err(error) => {
                    tracing::warn!(
                        target: "xtask::coordinator",
                        path = %history_db_path.display(),
                        error = %error,
                        command,
                        proof_kind,
                        "coordinator freshness test proof query failed"
                    );
                    return None;
                }
            }
            return None;
        }

        match db.get_successful_proof_evidence(command, &proof_kind, tree_fingerprint, scope_key) {
            Ok(Some(last)) => {
                return Some(CoordinationResult::Fresh {
                    invocation_id: last.invocation_id,
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
        command: &str,
        args: &[String],
        is_foreground: bool,
        tree_fingerprint: &str,
        scope_key: &str,
        state_path: &std::path::Path,
    ) -> Result<CoordinationResult> {
        let state = CoordinationState {
            command: command.to_string(),
            job_id: -1, // Sentinel: "pending spawn" — updated by caller via update_state()
            pid: 0,     // Sentinel: "not yet spawned" — updated by caller via update_state()
            process_start_ticks: 0, // Sentinel: captured by update_coordinator_state after spawn
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
        command: &str,
        state: &CoordinationState,
        args: &[String],
        is_foreground: bool,
        output_format: OutputFormat,
        tree_fingerprint: &str,
        scope_key: &str,
        state_path: &std::path::Path,
    ) -> Result<()> {
        // Append to FIFO queue (supports multiple concurrent requesters)
        let mut updated = state.clone();
        updated.queue.push(QueuedWork {
            command: command.to_string(),
            args: args.to_vec(),
            is_foreground,
            output_format,
            tree_fingerprint: tree_fingerprint.to_string(),
            scope_key: scope_key.to_string(),
            reason: format!(
                "waiting for {} job {} to complete",
                state.command, state.job_id
            ),
        });
        write_state(state_path, &updated)?;
        Ok(())
    }

    /// Update the state file with the actual job ID and PID after spawning.
    ///
    /// Preserves the queue — this is critical for FIFO queue correctness when
    /// `handle_completion()` left remaining items in the state file.
    pub fn update_state(
        &self,
        command: &str,
        job_id: i64,
        pid: u32,
        start_ticks: u64,
    ) -> Result<()> {
        let lock_path = self.lock_path_for(command);
        let state_path = self.state_path_for(command);

        let lock_file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)?;

        let _lock_file = lock_exclusive_retry(lock_file)?;

        if let Some(mut state) = read_state(&state_path)? {
            state.job_id = job_id;
            state.pid = pid;
            state.process_start_ticks = start_ticks;
            write_state(&state_path, &state)?;
        } else {
            // Another process may have completed and cleaned up the state in the reserve→spawn
            // window. This is benign; the spawned job remains tracked by the jobs subsystem.
        }

        Ok(())
    }

    /// Remove a still-pending sentinel reservation if phase-two spawn recording failed.
    pub fn clear_pending_state(&self, command: &str) -> Result<bool> {
        let lock_path = self.lock_path_for(command);
        let state_path = self.state_path_for(command);

        let lock_file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)?;

        let _lock_file = lock_exclusive_retry(lock_file)?;

        let Some(state) = read_state(&state_path)? else {
            return Ok(false);
        };

        if state.pid == 0 && state.job_id == -1 {
            remove_state_file(
                &state_path,
                "remove pending coordinator state after failed spawn recording",
            )?;
            return Ok(true);
        }

        Ok(false)
    }
}

// --- Utility functions ---

/// Acquire an exclusive flock with a retry loop (D5 fix).
///
/// `flock(LOCK_EX)` blocks indefinitely; in a multi-process environment
/// a stuck holder would cause all callers to hang forever. We use the
/// non-blocking variant and retry for a bounded window before returning an
/// error. The window is intentionally seconds, not milliseconds: tree
/// fingerprinting and queue updates can legitimately hold the coordinator
/// during agent-driven parallel launches.
fn lock_exclusive_retry(mut file: fs::File) -> Result<Flock<fs::File>> {
    const MAX_RETRIES: u32 = 100;
    const RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_millis(50);
    const MAX_WAIT_MS: u32 = MAX_RETRIES * 50;
    for i in 0..MAX_RETRIES {
        match Flock::lock(file, FlockArg::LockExclusiveNonblock) {
            Ok(lock) => return Ok(lock),
            Err((unlocked_file, nix::errno::Errno::EWOULDBLOCK)) if i + 1 < MAX_RETRIES => {
                file = unlocked_file;
                std::thread::sleep(RETRY_INTERVAL);
            }
            Err((_unlocked_file, nix::errno::Errno::EWOULDBLOCK)) => {
                bail!(
                    "coordinator: could not acquire lock after {MAX_RETRIES} retries ({MAX_WAIT_MS} ms)"
                );
            }
            Err((_unlocked_file, e)) => return Err(e).wrap_err("coordinator: flock failed"),
        }
    }
    unreachable!()
}

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
        bail!("git {description} failed: {}", summarize_git_error(&output));
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

fn hash_labeled_bytes(hasher: &mut Sha256, label: &str, bytes: &[u8]) {
    hasher.update(label.as_bytes());
    hasher.update(b"\x00");
    hasher.update((bytes.len() as u64).to_le_bytes());
    hasher.update(b"\x00");
    hasher.update(bytes);
    hasher.update(b"\x00");
}

fn hash_git_output(
    cwd: &Path,
    hasher: &mut Sha256,
    label: &str,
    args: &[&str],
    description: &str,
) -> Result<()> {
    let output = git_output(cwd, args, description)?;
    hash_labeled_bytes(hasher, label, &output.stdout);
    Ok(())
}

fn hash_untracked_file_contents(cwd: &Path, hasher: &mut Sha256, pathspecs: &[&str]) -> Result<()> {
    let mut args = vec!["ls-files", "--others", "--exclude-standard", "-z", "--"];
    args.extend_from_slice(pathspecs);
    let output = git_output(
        cwd,
        &args,
        "ls-files --others --exclude-standard -z for fingerprint",
    )?;
    let mut paths = output
        .stdout
        .split(|byte| *byte == 0)
        .filter(|entry| !entry.is_empty())
        .collect::<Vec<_>>();
    paths.sort_unstable();

    for path_bytes in paths {
        hash_labeled_bytes(hasher, "untracked-path", path_bytes);
        let rel_path = String::from_utf8_lossy(path_bytes);
        let contents = fs::read(cwd.join(rel_path.as_ref())).with_context(|| {
            format!("failed to read untracked file for fingerprint: {rel_path}")
        })?;
        hash_labeled_bytes(hasher, "untracked-content", &contents);
    }

    Ok(())
}

fn hash_dirty_content(cwd: &Path, hasher: &mut Sha256, pathspecs: &[&str]) -> Result<()> {
    let mut cached_args = vec![
        "diff",
        "--binary",
        "--no-ext-diff",
        "--cached",
        "HEAD",
        "--",
    ];
    cached_args.extend_from_slice(pathspecs);
    hash_git_output(
        cwd,
        hasher,
        "staged-diff",
        &cached_args,
        "diff --binary --cached HEAD for fingerprint",
    )?;

    let mut unstaged_args = vec!["diff", "--binary", "--no-ext-diff", "--"];
    unstaged_args.extend_from_slice(pathspecs);
    hash_git_output(
        cwd,
        hasher,
        "unstaged-diff",
        &unstaged_args,
        "diff --binary for fingerprint",
    )?;

    hash_untracked_file_contents(cwd, hasher, pathspecs)
}

/// Compute tree fingerprint: sha256 of committed tree identity plus dirty content.
///
/// Properties: deterministic (same tree → same hash), conservative, and
/// content-sensitive for staged, unstaged, and untracked changes.
fn tree_fingerprint_in(cwd: &Path) -> Result<String> {
    // Refresh the git index so status reflects actual filesystem state.
    // Without this, rapid edits within the same second can go undetected
    // because git caches stat data (mtime, size) in the index.
    refresh_git_index(cwd)?;

    let mut hasher = Sha256::new();
    hasher.update(b"sinex-tree-fingerprint-v2\x00");
    hash_git_output(
        cwd,
        &mut hasher,
        "head",
        &["rev-parse", "HEAD"],
        "rev-parse HEAD for whole-tree fingerprint",
    )?;
    hash_dirty_content(cwd, &mut hasher, &[])?;
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
        "sinexctl" => "crate/sinexctl/".to_string(),
        "xtask" => "xtask/".to_string(),
        "sinex-e2e-tests" => "tests/e2e/".to_string(),
        "sinex-workspace-tests" => "tests/workspace/".to_string(),
        "sinex-vm-test-suite" => "tests/vm-suite/".to_string(),
        _ => {
            let name_underscore = pkg.replace('-', "_");
            let path_hyphen = format!("crate/{pkg}/");
            if std::path::Path::new(&path_hyphen).exists() {
                return path_hyphen;
            }
            let path_under = format!("crate/{name_underscore}/");
            if std::path::Path::new(&path_under).exists() {
                return path_under;
            }
            // Unknown package — include crate/ broadly (over-includes, never misses)
            "crate/".to_string()
        }
    }
}

/// R1: Extract package names from -p/--package flags in command args.
fn extract_explicit_packages(command: &str, args: &[String]) -> Vec<String> {
    if !matches!(command, "check" | "build" | "fix" | "test") {
        return vec![];
    }

    let mut packages = Vec::new();
    if let Some(marker) = args.iter().find(|arg| arg.starts_with("--scope=")) {
        let raw = marker.trim_start_matches("--scope=");
        if let Some(marker_packages) = raw.strip_prefix("packages:") {
            marker_packages
                .split(',')
                .filter(|entry| !entry.is_empty())
                .map(ToOwned::to_owned)
                .for_each(|package| packages.push(package));
        }
        // Unknown --scope= format: fall through and parse -p/--package flags
        // instead of silently dropping them.  A future scope variant will get
        // package resolution for free without a separate special-case here.
    }

    let mut take_next = false;

    for arg in args {
        if command == "test" && arg == "--" {
            break;
        }
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
        } else if let Some(runtime) = arg.strip_prefix("--runtime-binary=") {
            let package = runtime
                .split_once(':')
                .map_or(runtime, |(package, _)| package);
            if !package.is_empty() {
                packages.push(package.to_string());
            }
        }
    }

    packages.sort();
    packages.dedup();
    packages
}

fn extract_cargo_features(command: &str, args: &[String]) -> Vec<String> {
    if !matches!(command, "check" | "build" | "fix" | "test") {
        return vec![];
    }

    let mut features = Vec::new();
    let mut take_next = false;

    for arg in args {
        if command == "test" && arg == "--" {
            break;
        }
        if take_next {
            features.extend(split_cargo_features(arg));
            take_next = false;
            continue;
        }
        if arg == "--features" {
            take_next = true;
        } else if let Some(value) = arg.strip_prefix("--features=") {
            features.extend(split_cargo_features(value));
        }
    }

    features.sort();
    features.dedup();
    features
}

fn split_cargo_features(value: &str) -> impl Iterator<Item = String> + '_ {
    value
        .split(',')
        .map(str::trim)
        .filter(|feature| !feature.is_empty())
        .map(ToOwned::to_owned)
}

fn scoped_fingerprint_packages_in(
    cwd: &Path,
    packages: &[String],
    features: &[String],
) -> Result<Vec<String>> {
    if features.is_empty() {
        crate::affected::active_package_dependency_closure_in(cwd, packages, &[])
    } else {
        crate::affected::active_package_dependency_closure_in(cwd, packages, features)
    }
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
    let features = extract_cargo_features(command, args);

    if packages.is_empty() {
        // No -p flag: use whole-workspace fingerprint (safe, over-inclusive)
        return tree_fingerprint_in(cwd);
    }
    let fingerprint_packages = match scoped_fingerprint_packages_in(cwd, &packages, &features) {
        Ok(closure) => closure,
        Err(error) => {
            tracing::warn!(
                error = %error,
                "failed to resolve package dependency closure for scoped freshness; falling back to whole-tree fingerprint"
            );
            return tree_fingerprint_in(cwd);
        }
    };

    // Refresh git index (same as tree_fingerprint)
    refresh_git_index(cwd)?;

    let mut hasher = Sha256::new();

    // Seed the hasher so a clean working tree (no diff, no untracked) still
    // produces a fingerprint that's distinct per (HEAD, package-set). Before
    // this seeding, every clean per-package run hashed zero bytes and
    // collided on SHA256("") — 117 such collisions in 7d (#1212).
    //
    // Domain separator + version is intentional: changing the seeding format
    // later should bump the version to invalidate old cache entries.
    hasher.update(b"sinex-tree-fingerprint-v2\x00");
    hash_git_output(
        cwd,
        &mut hasher,
        "head",
        &["rev-parse", "HEAD"],
        "rev-parse HEAD for fingerprint seeding",
    )?;
    // fingerprint_packages includes the requested packages plus their transitive
    // workspace dependencies. Sort for deterministic fingerprint regardless of
    // -p order or metadata order.
    let mut sorted_packages: Vec<&String> = fingerprint_packages.iter().collect();
    sorted_packages.sort_unstable();
    for pkg in &sorted_packages {
        hasher.update(pkg.as_bytes());
        hasher.update(b"\x00");
    }

    for pkg in &sorted_packages {
        let prefix = package_to_path(pkg);
        hash_dirty_content(cwd, &mut hasher, &[&prefix])?;
    }
    hash_dirty_content(cwd, &mut hasher, SHARED_FINGERPRINT_INPUTS)?;

    Ok(format!("{:x}", hasher.finalize()))
}

fn scoped_tree_fingerprint(command: &str, args: &[String]) -> Result<String> {
    scoped_tree_fingerprint_in(Path::new("."), command, args)
}

/// Explain the current coordinator freshness key without mutating state.
///
/// This is the auditable counterpart to `scoped_tree_fingerprint`: consumers can
/// see the command/scope inputs before trusting a fresh-hit decision.
pub fn explain_freshness(command: &str, args: &[String]) -> Result<FreshnessExplanation> {
    let packages = extract_explicit_packages(command, args);
    let features = extract_cargo_features(command, args);
    let scope = if packages.is_empty() {
        FreshnessScopeExplanation::Workspace
    } else {
        let fingerprint_packages =
            scoped_fingerprint_packages_in(Path::new("."), &packages, &features)
                .unwrap_or(packages);
        let mut packages = fingerprint_packages
            .into_iter()
            .map(|package| PackageScopeInput {
                path: package_to_path(&package),
                package,
            })
            .collect::<Vec<_>>();
        packages.sort_unstable_by(|left, right| left.package.cmp(&right.package));
        FreshnessScopeExplanation::Packages { packages }
    };

    Ok(FreshnessExplanation {
        command: command.to_string(),
        args: args.to_vec(),
        should_coordinate: JobCoordinator::should_coordinate(command, args),
        fresh_reuse_enabled: supports_fresh_reuse_for(command, args),
        proof_kind: proof_kind(command, args),
        scope_key: scope_key(command, args),
        tree_fingerprint: scoped_tree_fingerprint(command, args)?,
        scope,
        shared_inputs: SHARED_FINGERPRINT_INPUTS
            .iter()
            .map(|input| (*input).to_string())
            .collect(),
    })
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

fn coordination_family(command: &str) -> &str {
    match command {
        "check" | "build" | "test" | "fix" | "vm" => "heavy-work",
        _ => command,
    }
}

fn supports_fresh_reuse(command: &str) -> bool {
    matches!(command, "check" | "build")
}

fn supports_fresh_reuse_for(command: &str, args: &[String]) -> bool {
    match command {
        "check" => supports_fresh_reuse(command) && !args.iter().any(|arg| arg == "--fix"),
        "build" => supports_fresh_reuse(command) && !args.iter().any(|arg| arg == "--dry-run"),
        "test" => test_scope_is_fresh_reusable(args),
        _ => false,
    }
}

fn test_scope_is_fresh_reusable(args: &[String]) -> bool {
    let has_runtime_or_mutating_flag = args.iter().any(|arg| {
        matches!(
            arg.as_str(),
            "--heavy"
                | "--include-ignored"
                | "--debug"
                | "--fuzz"
                | "--mutants"
                | "--coverage"
                | "--bench"
                | "--list"
                | "--dry-run"
                | "-l"
                | "--prime"
                | "--update-snapshots"
                | "--no-reuse"
        )
    });
    !has_runtime_or_mutating_flag
}

/// Human-readable proof unit class for a coordinated command.
#[must_use]
pub fn proof_kind(command: &str, args: &[String]) -> String {
    match command {
        "check" => {
            let mut modes = Vec::new();
            for flag in [
                "--all",
                "--fix",
                "--full",
                "--lint",
                "--fmt",
                "--forbidden",
                "--nix",
                "--skip-tests",
                "--changed-strict",
            ] {
                if args.iter().any(|arg| arg == flag) {
                    modes.push(flag.trim_start_matches('-').replace('-', "_"));
                }
            }
            if modes.is_empty() {
                "check.default".to_string()
            } else {
                modes.sort_unstable();
                format!("check.{}", modes.join("+"))
            }
        }
        "fix" => {
            if args.iter().any(|arg| arg == "--check") {
                "fix.check".to_string()
            } else {
                "fix.apply".to_string()
            }
        }
        "build" => {
            if args.iter().any(|arg| arg == "--dry-run") {
                "build.dry_run".to_string()
            } else {
                "build.default".to_string()
            }
        }
        "test" => {
            if test_scope_is_fresh_reusable(args) {
                "test.nextest.exact".to_string()
            } else {
                "test.nextest.plan".to_string()
            }
        }
        other => format!("{other}.default"),
    }
}

/// Extract scope-relevant arguments for a command.
///
/// Handles the tricky case where `-p sinex-db` is two separate args:
/// the flag `-p` and its value `sinex-db` are both captured.
fn extract_scope_args(command: &str, args: &[String]) -> Vec<String> {
    let marker = args
        .iter()
        .find(|arg| arg.starts_with("--scope="))
        .cloned()
        .or_else(|| canonical_package_scope_marker(command, args));

    fn is_package_value_flag(command: &str, arg: &str) -> bool {
        matches!(command, "build" | "check" | "fix" | "test")
            && matches!(arg, "-p" | "--package" | "--packages")
    }

    fn is_package_combined_flag(command: &str, arg: &str) -> bool {
        matches!(command, "build" | "check" | "fix" | "test")
            && ((arg.starts_with("-p") && arg.len() > 2)
                || arg.starts_with("--package=")
                || arg.starts_with("--packages="))
    }

    fn value_flag_prefix(command: &str, arg: &str) -> Option<&'static str> {
        // Flags that take a separate next-arg value
        match command {
            "check" => match arg {
                "--changed-strict" => Some("--changed-strict="),
                _ => None,
            },
            "test" => match arg {
                "-E" | "--filter" => Some("--filter="),
                "--test" => Some("--test="),
                "--exclude" => Some("--exclude="),
                "--features" => Some("--features="),
                "--runtime-binary" => Some("--runtime-binary="),
                "--threads" => Some("--threads="),
                "--retries" => Some("--retries="),
                "--timeout" => Some("--timeout="),
                _ => None,
            },
            _ => None,
        }
    }

    fn is_standalone_flag(command: &str, arg: &str) -> bool {
        // Flags that are scope-relevant on their own (no value)
        match command {
            "build" => arg == "--release" || arg.starts_with("--all") || arg == "--dry-run",
            "test" => matches!(
                arg,
                "--debug"
                    | "--heavy"
                    | "--include-ignored"
                    | "--all"
                    | "--lib"
                    | "--list"
                    | "--prime"
                    | "--update-snapshots"
                    | "--no-reuse"
            ),
            "check" | "fix" => {
                matches!(
                    arg,
                    "--all"
                        | "--fix"
                        | "--full"
                        | "--lint"
                        | "--fmt"
                        | "--forbidden"
                        | "--nix"
                        | "--heavy"
                        | "--skip-tests"
                ) || arg.starts_with("--changed-strict=")
            }
            _ => false,
        }
    }

    fn canonical_combined_flag(command: &str, arg: &str) -> Option<String> {
        // Flags with value attached: --package=foo, -p=foo, -Etest(name)
        match command {
            "test" => {
                if let Some(filter) = arg.strip_prefix("-E").filter(|value| !value.is_empty()) {
                    Some(format!("--filter={filter}"))
                } else if arg.starts_with("--filter=")
                    || arg.starts_with("--test=")
                    || arg.starts_with("--exclude=")
                    || arg.starts_with("--features=")
                    || arg.starts_with("--runtime-binary=")
                    || arg.starts_with("--threads=")
                    || arg.starts_with("--retries=")
                    || arg.starts_with("--timeout=")
                    || arg.starts_with("--db-pool-size-env=")
                    || arg.starts_with("--impact-mode=")
                    || arg.starts_with("--impact-planner-version=")
                    || arg.starts_with("--impact-coverage-schema=")
                {
                    Some(arg.to_string())
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    let mut relevant = Vec::new();
    if let Some(marker) = marker {
        relevant.push(marker);
    }
    let mut take_next: Option<&'static str> = None;
    let mut test_arg_index = 0usize;
    let mut iter = args.iter();

    while let Some(arg) = iter.next() {
        if command == "test" && arg == "--" {
            for test_arg in iter {
                relevant.push(format!("--test-arg[{test_arg_index:04}]={test_arg}"));
                test_arg_index += 1;
            }
            break;
        }
        if arg.starts_with("--scope=") {
            continue;
        }
        if is_package_value_flag(command, arg) {
            let _ = iter.next();
            continue;
        }
        if is_package_combined_flag(command, arg) {
            continue;
        }
        if let Some(prefix) = take_next.take() {
            relevant.push(format!("{prefix}{arg}"));
            continue;
        }
        if let Some(prefix) = value_flag_prefix(command, arg) {
            take_next = Some(prefix);
        } else if is_standalone_flag(command, arg) {
            relevant.push(arg.clone());
        } else if command == "test"
            && let Some(test_arg) = arg.strip_prefix("--test-arg=")
        {
            relevant.push(format!("--test-arg[{test_arg_index:04}]={test_arg}"));
            test_arg_index += 1;
        } else if let Some(canonical) = canonical_combined_flag(command, arg) {
            relevant.push(canonical);
        }
    }

    relevant
}

fn canonical_package_scope_marker(command: &str, args: &[String]) -> Option<String> {
    let mut packages = extract_explicit_packages(command, args);
    if packages.is_empty() {
        return None;
    }
    packages.sort();
    packages.dedup();
    Some(format!("--scope=packages:{}", packages.join(",")))
}

/// Describe the command's workload scope using only scope-relevant arguments.
///
/// Unlike `scope_key`, this preserves argument order for human-facing output.
#[must_use]
pub fn describe_scope(command: &str, args: &[String]) -> Option<String> {
    let relevant = extract_scope_args(command, args);
    (!relevant.is_empty()).then(|| relevant.join(" "))
}

/// X4: Returns true if a coordinator state file was modified within the last 5 seconds.
///
/// Used to distinguish a fresh sentinel reservation (pid=0, just written) from a
/// genuinely stale state (process died and PID hasn't been recycled yet).
fn state_file_is_recent(path: &std::path::Path) -> bool {
    fs::metadata(path)
        .and_then(|m| m.modified())
        .is_ok_and(|t| t.elapsed().is_ok_and(|e| e.as_secs() < 5))
}

/// Check if a process at `pid` is the same one we spawned, by comparing
/// `/proc/{pid}/stat` start_ticks. Returns true if:
/// - expected_start_ticks is 0 (sentinel: not captured — skip validation), or
/// - `/proc/{pid}/stat` reads successfully and start_ticks match.
///
/// Returns false if the process at this PID has different start_ticks
/// (PID was reused by an unrelated process since we captured the state).
fn process_identity_valid(pid: u32, expected_start_ticks: u64) -> bool {
    if expected_start_ticks == 0 {
        return true; // Sentinel: start_ticks not captured (pre-existing state)
    }
    match crate::process::read_proc_sample(pid) {
        Some(sample) => sample.start_ticks == expected_start_ticks,
        None => true, // Process is gone — nothing to kill
    }
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

/// Cancel a process and its children: validate PID identity → SIGTERM →
/// wait 5s → SIGKILL.
///
/// If `expected_start_ticks` is non-zero, the function validates that the
/// current process at `pid` has matching start_ticks before sending signals.
/// If the PID was reused by an unrelated process, the function skips the
/// kill and logs a warning — the stale job is still marked cancelled.
///
/// Sends signals to the process group (negative PID) so that child processes
/// (e.g., rustc, nextest) spawned by the background cargo process are also
/// terminated. If process group kill fails (ESRCH), falls back to single-PID kill.
fn cancel_process(pid: u32, expected_start_ticks: u64) {
    let pid = pid as i32;
    if pid == 0 {
        return; // Sentinel PID — nothing to cancel
    }

    // Validate that the process at this PID is still ours.
    if expected_start_ticks != 0 && !process_identity_valid(pid as u32, expected_start_ticks) {
        tracing::warn!(
            pid = pid,
            expected_start_ticks = expected_start_ticks,
            "Stale coordinator state: PID {pid} no longer belongs to the original process \
             (PID was reused). Skipping signal delivery; the stale job will be marked cancelled."
        );
        return;
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

/// Mark a superseded background job and its durable invocation as cancelled.
fn mark_cancelled(job_id: i64) -> Result<()> {
    let cfg = config();
    let db = crate::history::HistoryDb::open(&cfg.history_db_path()).with_context(|| {
        format!("failed to open history DB while cancelling superseded job {job_id}")
    })?;
    let job = db.get_background_job_by_id(job_id)?.ok_or_else(|| {
        eyre!("background job {job_id} missing while recording superseded cancellation")
    })?;
    if let Some(invocation_id) = job.invocation_id {
        db.finish_invocation_cancelled(
            invocation_id,
            None,
            0.0,
            "superseded",
            "coordinator",
        )
            .with_context(|| {
                format!(
                    "failed to finish invocation {invocation_id} while cancelling superseded job {job_id}"
                )
            })?;
    }
    db.finish_background_job(job_id, JobLifecycleStatus::Killed, None, 0.0, None, None)
        .with_context(|| format!("failed to finish superseded background job {job_id}"))?;
    Ok(())
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
        Err(error) => Err(error)
            .with_context(|| format!("failed to parse coordinator state: {}", path.display())),
    }
}

fn remove_state_file(path: &std::path::Path, reason: &str) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("{reason}: {}", path.display())),
    }
}

fn write_state(path: &std::path::Path, state: &CoordinationState) -> Result<()> {
    let json = serde_json::to_string_pretty(state)?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create state dir: {}", parent.display()))?;
    }

    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            eyre!(
                "coordinator state path is not valid UTF-8: {}",
                path.display()
            )
        })?;
    let unique_suffix = std::process::id();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    let tmp_path = path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(format!(".{file_name}.{unique_suffix}.{nanos}.tmp"));

    fs::write(&tmp_path, json)
        .with_context(|| format!("failed to write temp state: {}", tmp_path.display()))?;

    if let Err(error) = fs::rename(&tmp_path, path) {
        let cleanup_result = fs::remove_file(&tmp_path);
        if let Err(cleanup_error) = cleanup_result
            && cleanup_error.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!(
                path = %tmp_path.display(),
                error = %cleanup_error,
                "failed to clean up temp coordinator state file after rename failure"
            );
        }
        return Err(error)
            .with_context(|| format!("failed to atomically replace state: {}", path.display()));
    }

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
    coordinate_and_spawn_with_scope(command, args, args, ctx)
}

pub fn coordinate_and_spawn_with_scope(
    command: &str,
    spawn_args: &[String],
    coordination_args: &[String],
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let coordinator = if JobCoordinator::should_coordinate(command, spawn_args) {
        let coordinator = JobCoordinator::new()
            .with_context(|| format!("failed to initialize coordinator for `{command}`"))?;
        match coordinator.request_with_format_policy(
            command,
            spawn_args,
            coordination_args,
            false,
            ctx.writer().format(),
            !ctx.background_wait(),
        ) {
            Ok(
                result @ (CoordinationResult::Attached { .. }
                | CoordinationResult::Fresh { .. }
                | CoordinationResult::Queued { .. }),
            ) => {
                if ctx.background_wait() {
                    return wait_for_coordination_result(command, &result, ctx);
                }
                return Ok(coordination_to_result(&result, ctx));
            }
            Ok(CoordinationResult::Started { .. } | CoordinationResult::Superseded { .. }) => {
                // Fall through to spawn — coordinator reserved the slot
            }
            Err(e) => {
                return Err(e).with_context(|| {
                    format!("failed to coordinate background `{command}` invocation")
                });
            }
        }
        Some(coordinator)
    } else {
        None
    };

    let bg_result = match ctx.spawn_background(command, spawn_args) {
        Ok(bg_result) => bg_result,
        Err(error) => {
            if let Some(coordinator) = coordinator.as_ref() {
                match coordinator.clear_pending_state(command) {
                    Ok(cleared) => {
                        return Err(error).with_context(|| {
                            format!(
                                "failed to spawn background `{command}` invocation after reserving coordinator state; cleared_pending_state={cleared}"
                            )
                        });
                    }
                    Err(clear_error) => {
                        return Err(error).with_context(|| {
                            format!(
                                "failed to spawn background `{command}` invocation after reserving coordinator state; also failed to clear pending coordinator state: {clear_error}"
                            )
                        });
                    }
                }
            }
            return Err(error);
        }
    };
    update_coordinator_state(command, &bg_result).with_context(|| {
        format!("failed to record background `{command}` invocation in coordinator state")
    })?;
    if ctx.background_wait()
        && let Some(job_id) = bg_result
            .data
            .as_ref()
            .and_then(|data| data["job_id"].as_i64())
    {
        return wait_for_background_job(command, job_id, ctx);
    }
    Ok(bg_result)
}

fn wait_for_coordination_result(
    command: &str,
    result: &CoordinationResult,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    match result {
        CoordinationResult::Attached { job_id } => wait_for_background_job(command, *job_id, ctx),
        CoordinationResult::Started { job_id } => wait_for_background_job(command, *job_id, ctx),
        CoordinationResult::Superseded { new_job_id, .. } => {
            wait_for_background_job(command, *new_job_id, ctx)
        }
        CoordinationResult::Queued { current_job_id } => {
            let pending_job_assignment = *current_job_id < 0;
            let message = if pending_job_assignment {
                "proof wait cannot identify the queued job before it is assigned an ID; no proof was produced".to_string()
            } else {
                format!(
                    "proof wait cannot identify the future queued job behind {current_job_id}; no proof was produced"
                )
            };
            let suggestion = if pending_job_assignment {
                "wait for the active coordinated slot, then rerun this command with --bg --wait"
                    .to_string()
            } else {
                format!("wait for job {current_job_id}, then rerun this command with --bg --wait")
            };
            Ok(CommandResult::failure(
                crate::output::StructuredError::new("XTASK_BG_WAIT_QUEUED", message)
                    .with_suggestion(suggestion),
            )
            .with_data(serde_json::json!({
                "action": "queued",
                "current_job_id": (!pending_job_assignment).then_some(current_job_id),
                "current_job_pending_assignment": pending_job_assignment,
                "proof_status": "incomplete",
            })))
        }
        CoordinationResult::Fresh { .. } => {
            Ok(CommandResult::failure(crate::output::StructuredError::new(
                "XTASK_BG_WAIT_UNSEALED_FRESH",
                "proof wait refused an unsealed freshness hit; no proof was produced",
            ))
            .with_data(serde_json::json!({
                "action": "fresh",
                "proof_status": "incomplete",
            })))
        }
    }
}

fn wait_for_background_job(
    command: &str,
    job_id: i64,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let manager = crate::jobs::JobQueryManager::new(config().jobs_dir())?;
    let job = tokio::task::block_in_place(|| {
        tokio::runtime::Handle::current().block_on(manager.wait(job_id, None))
    })?;
    let exit_code = job.exit_code.unwrap_or(1);
    let proof_status = if exit_code == 0 {
        "success"
    } else if exit_code == 124 {
        "timed_out"
    } else {
        "failed"
    };
    let mut result = CommandResult::success()
        .with_message(format!(
            "Background job {job_id} finished with exit code {exit_code}"
        ))
        .with_data(serde_json::json!({
            "action": "wait",
            "job_id": job_id,
            "exit_code": exit_code,
            "job_status": job.job_status.as_str(),
            "proof_status": proof_status,
        }));
    if exit_code != 0 {
        result.status = if exit_code == 124 {
            crate::output::Status::Cancelled
        } else {
            crate::output::Status::Failed
        };
    }
    result.print(ctx.writer(), command);
    std::process::exit(exit_code);
}

/// After spawning a background job, update coordinator state with the real `job_id` and `pid`.
///
/// This is the second phase of the two-phase coordination protocol:
/// 1. `coordinator.request()` reserves a slot with sentinel values
/// 2. `spawn_background()` creates the actual process
/// 3. This function updates the slot with real values
pub fn update_coordinator_state(command: &str, bg_result: &CommandResult) -> Result<()> {
    let clear_pending = |coordinator: Option<&JobCoordinator>, reason: &str| -> Result<bool> {
        match coordinator {
            Some(coordinator) => coordinator.clear_pending_state(command),
            None => JobCoordinator::new()
                .with_context(|| {
                    format!(
                        "failed to initialize coordinator while clearing pending spawn state for `{command}`"
                    )
                })?
                .clear_pending_state(command),
        }
        .with_context(|| format!("{reason} for `{command}`"))
    };

    let Some(data) = &bg_result.data else {
        let cleared = clear_pending(None, "background spawn returned no data")?;
        bail!("background spawn returned no data for `{command}`; cleared_pending_state={cleared}");
    };

    let Some(job_id) = data["job_id"].as_i64() else {
        let cleared = clear_pending(None, "background spawn returned no job_id")?;
        bail!(
            "background spawn returned no job_id for `{command}`; cleared_pending_state={cleared}; data={data}"
        );
    };

    let Some(pid) = data["pid"].as_u64() else {
        let cleared = clear_pending(None, "background spawn returned no pid")?;
        bail!(
            "background spawn returned no pid for `{command}`; cleared_pending_state={cleared}; data={data}"
        );
    };

    let coordinator = JobCoordinator::new()
        .with_context(|| format!("failed to initialize coordinator while recording `{command}`"))?;

    // Capture process start_ticks for PID reuse detection. If the process has
    // already exited by the time we read /proc/{pid}/stat, store 0 (sentinel:
    // "unknown") — the coordinator will treat any non-zero process at this PID
    // as a mismatch and refuse to send signals.
    let start_ticks = crate::process::read_proc_sample(pid as u32).map_or(0, |s| s.start_ticks);

    if let Err(error) = coordinator.update_state(command, job_id, pid as u32, start_ticks) {
        let cleared = clear_pending(
            Some(&coordinator),
            "failed to clear pending coordinator state after spawn recording failure",
        )?;
        return Err(error).with_context(|| {
            format!(
                "failed to persist coordinator state for spawned `{command}` job {job_id} (pid {pid}); cleared_pending_state={cleared}"
            )
        });
    }

    Ok(())
}

/// Convert a coordination result to a command result for the --bg path.
fn coordination_attached_result(job_id: i64, ctx: &CommandContext) -> CommandResult {
    tracing::info!(
        target: "xtask::coordinator",
        job_id = job_id,
        action = "attached",
        "coordinator: attached — identical coordinated job already running"
    );
    if ctx.is_human() {
        println!("🔗 Attached: identical coordinated job already running (job {job_id})");
        println!("   Monitor: xtask jobs status {job_id}");
    }
    CommandResult::success()
        .with_message(format!("Attached to running job {job_id}"))
        .with_data(serde_json::json!({
            "action": "attached",
            "job_id": job_id,
            "hint": format!("Monitor with: xtask jobs status {job_id}"),
            "proof_status": "incomplete",
        }))
}

fn coordination_superseded_result(
    old_job_id: i64,
    new_job_id: i64,
    ctx: &CommandContext,
) -> CommandResult {
    tracing::info!(
        target: "xtask::coordinator",
        old_job_id = old_job_id,
        new_job_id = new_job_id,
        action = "superseded",
        "coordinator: superseded — cancelled stale job, starting fresh"
    );
    if ctx.is_human() {
        println!("♻ Superseded: cancelled stale job {old_job_id}, starting fresh job {new_job_id}");
    }
    CommandResult::success()
        .with_message(format!("Superseded job {old_job_id} with {new_job_id}"))
        .with_data(serde_json::json!({
            "action": "superseded",
            "old_job_id": old_job_id,
            "new_job_id": new_job_id,
            "proof_status": "incomplete",
        }))
}

fn coordination_queued_result(current_job_id: i64, ctx: &CommandContext) -> CommandResult {
    tracing::info!(
        target: "xtask::coordinator",
        current_job_id = current_job_id,
        action = "queued",
        "coordinator: queued — waiting for running job to complete"
    );
    let pending_job_assignment = current_job_id < 0;
    if ctx.is_human() {
        if pending_job_assignment {
            println!(
                "⏳ Queued: waiting for the active coordinated slot to finish assigning its next job id"
            );
            println!("   Monitor: xtask jobs active");
        } else {
            println!("⏳ Queued: waiting for job {current_job_id} to complete");
            println!("   Monitor: xtask jobs status {current_job_id}");
        }
    }
    CommandResult::success()
        .with_message(if pending_job_assignment {
            "Queued behind an active coordinated slot awaiting job assignment".to_string()
        } else {
            format!("Queued behind job {current_job_id}")
        })
        .with_data(serde_json::json!({
            "action": "queued",
            "current_job_id": (!pending_job_assignment).then_some(current_job_id),
            "current_job_pending_assignment": pending_job_assignment,
            "hint": if pending_job_assignment {
                "Monitor with: xtask jobs active".to_string()
            } else {
                format!("Monitor with: xtask jobs status {current_job_id}")
            },
            "proof_status": "incomplete",
        }))
}

pub fn coordination_to_result(result: &CoordinationResult, ctx: &CommandContext) -> CommandResult {
    match result {
        CoordinationResult::Fresh {
            invocation_id,
            status,
            duration_secs,
        } => coordination_fresh_result(
            *invocation_id,
            status,
            *duration_secs,
            ctx,
            fresh_packages_probe(*invocation_id),
        ),
        CoordinationResult::Attached { job_id } => coordination_attached_result(*job_id, ctx),
        CoordinationResult::Superseded {
            old_job_id,
            new_job_id,
        } => coordination_superseded_result(*old_job_id, *new_job_id, ctx),
        CoordinationResult::Queued { current_job_id } => {
            coordination_queued_result(*current_job_id, ctx)
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
                    "proof_status": "incomplete",
                }))
        }
    }
}

struct FreshPackagesProbe {
    packages: Vec<String>,
    issue: Option<String>,
}

fn fresh_packages_probe(invocation_id: i64) -> FreshPackagesProbe {
    let cfg = config();
    let db_path = cfg.history_db_path();
    let result = crate::history::HistoryDb::open(&db_path)
        .and_then(|db| db.get_compiled_packages_for_invocation(invocation_id));
    fresh_packages_probe_from_result(invocation_id, &db_path, result)
}

fn fresh_packages_probe_from_result(
    invocation_id: i64,
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
                "failed to load compiled packages for fresh invocation {invocation_id} from {}: {error:#}",
                db_path.display()
            )),
        },
    }
}

fn coordination_fresh_result(
    invocation_id: i64,
    status: &str,
    duration_secs: f64,
    ctx: &CommandContext,
    packages_probe: FreshPackagesProbe,
) -> CommandResult {
    tracing::info!(
        target: "xtask::coordinator",
        invocation_id = invocation_id,
        action = "fresh",
        cached_status = status,
        cached_duration_secs = duration_secs,
        "coordinator: fresh — last check already validated this code state"
    );

    if ctx.is_human() {
        if packages_probe.packages.is_empty() {
            println!(
                "✅ Fresh: last invocation already validated this code state (invocation {invocation_id}, {status} in {duration_secs:.1}s)"
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
                "✅ Fresh: last invocation already validated {pkg_list} (invocation {invocation_id}, {duration_secs:.1}s)"
            );
        }
        if let Some(issue) = &packages_probe.issue {
            println!("   Warning: {issue}");
        }
    }

    let mut result = CommandResult::success()
        .with_message(format!("Fresh result from invocation {invocation_id}"))
        .with_data(serde_json::json!({
            "action": "fresh",
            "invocation_id": invocation_id,
            "cached_status": status,
            "cached_duration_secs": duration_secs,
            "compiled_packages": packages_probe.packages,
            "compiled_packages_issue": packages_probe.issue,
            "proof_status": "incomplete",
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

/// Scoped tree fingerprint exposed for foreground command recording.
///
/// This mirrors the coordinator's background freshness key so foreground
/// invocations and `--bg` requests write comparable history rows.
pub fn current_scoped_tree_fingerprint(command: &str, args: &[String]) -> Result<String> {
    scoped_tree_fingerprint(command, args)
}

/// Scope key exposed for callers (e.g., recording in history DB).
#[must_use]
pub fn compute_scope_key(command: &str, args: &[String]) -> String {
    scope_key(command, args)
}

// --- R2: Workflow Dependency Graph ---

#[cfg(test)]
#[path = "coordinator_test.rs"]
mod tests;
