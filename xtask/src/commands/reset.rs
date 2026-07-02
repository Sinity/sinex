//! Reset command — wipe developer state for a fresh start.
//!
//! Each flag targets a specific category of state. With no category flags,
//! `--yes` alone resets operational developer state: db + nats + preflight + jobs + target.
//! The xtask history database is preserved unless `--history` is passed explicitly.
//! It is durable development observability evidence, not disposable cache.
//!
//! `--contracts` and `--schema` are surgical — they delete only the hash
//! files that gate preflight re-deployment. This forces re-run without
//! touching data.

use clap::Args;
use color_eyre::eyre::{Result, WrapErr, eyre};
use sinex_db::schema::apply::SHARED_ACCESS_ROLES;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use time::OffsetDateTime;

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::infra::services::postgres::PostgresManager;
use crate::infra::stack::{StackConfig, nats_stop, pg_stop};

/// Reset developer state for a fresh start.
#[derive(Args)]
pub struct ResetCommand {
    /// Required safety guard — must be passed to confirm destructive operation.
    #[arg(long, required = true)]
    yes: bool,

    /// Drop and recreate the database (stops Postgres, destroys all data).
    #[arg(long)]
    db: bool,

    /// Wipe NATS JetStream data (stops NATS, deletes stream data).
    #[arg(long)]
    nats: bool,

    /// Wipe the git-annex blobstore.
    #[arg(long)]
    blobs: bool,

    /// Wipe the configured preflight state directory (forces full preflight on next run).
    #[arg(long)]
    preflight: bool,

    /// Delete contracts hash file (forces contract redeploy on next run, no data loss).
    #[arg(long)]
    contracts: bool,

    /// Delete schema apply hash file (forces schema reapply on next run, no data loss).
    #[arg(long)]
    schema: bool,

    /// Archive the xtask history database and start a fresh one.
    ///
    /// The old DB is always preserved as a timestamped backup because history
    /// is a valuable timing/diagnostic/test dataset, not cache.
    #[arg(long)]
    history: bool,

    /// When used with --history: reseed the history database with synthetic data.
    #[arg(long, requires = "history")]
    seed: bool,

    /// Delete background job records and output files.
    #[arg(long)]
    jobs: bool,

    /// Delete stale per-test temporary directories.
    #[arg(long)]
    test_tmp: bool,

    /// Kill stale orphaned compiler/linker processes for this checkout's target dirs.
    #[arg(long)]
    stale_build_processes: bool,

    /// Wipe the cargo target/ directory (forces clean recompilation).
    #[arg(long)]
    target: bool,

    /// Regenerate TLS certificates.
    #[arg(long)]
    tls: bool,
}

impl XtaskCommand for ResetCommand {
    fn name(&self) -> &'static str {
        "reset"
    }

    async fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        // Determine whether this is "reset operational developer state" mode.
        let any_specific = self.db
            || self.nats
            || self.blobs
            || self.preflight
            || self.contracts
            || self.schema
            || self.history
            || self.jobs
            || self.test_tmp
            || self.stale_build_processes
            || self.target
            || self.tls;
        let all = !any_specific;

        let config = StackConfig::for_current_checkout()?;
        let cfg = crate::config::config();
        let verbose = ctx.is_human();
        let mut actions: Vec<&'static str> = Vec::new();

        ctx.heading("reset");

        // ── Database ──────────────────────────────────────────────────────────
        if all || self.db {
            reset_db(&config, verbose)?;
            actions.push("database dropped and recreated");
        }

        // ── NATS ─────────────────────────────────────────────────────────────
        if all || self.nats {
            reset_nats(&config, verbose)?;
            reset_hot_reload_checkpoints(verbose)?;
            reset_recovery_spools(verbose)?;
            actions.push("NATS JetStream data wiped");
            actions.push("hot-reload checkpoint files removed");
            actions.push("runtime recovery spools removed");
        }

        if all || self.db || self.nats {
            reset_event_engine_runtime_state(verbose)?;
            actions.push("event-engine runtime material state removed");
        }

        // ── Preflight cache ───────────────────────────────────────────────────
        if all || self.preflight {
            reset_preflight_dir(verbose)?;
            actions.push("preflight state removed");
        }

        // ── Blobs ────────────────────────────────────────────────────────────
        if self.blobs {
            reset_blobs(&config, verbose)?;
            actions.push("git-annex blobstore wiped");
        }

        // ── Contracts hash (surgical) ─────────────────────────────────────────
        if self.contracts {
            reset_contracts_hash(verbose)?;
            actions.push("contracts hash removed (forces redeploy)");
        }

        // ── Schema hash (surgical) ────────────────────────────────────────────
        if self.schema {
            reset_schema_hash(verbose)?;
            actions.push("schema hash removed (forces reapply)");
        }

        // ── History DB ───────────────────────────────────────────────────────
        // History is the user's accumulated dev-loop record across weeks or months —
        // never silently delete on reset; always rename to a timestamped backup.
        if self.history {
            let path = cfg.history_db_path();
            let msg = reset_history_db(&path, self.seed, verbose)?;
            actions.push(msg);
        }

        // ── Jobs ─────────────────────────────────────────────────────────────
        if all || self.jobs {
            let jobs_dir = cfg.jobs_dir();
            if jobs_dir.exists() {
                std::fs::remove_dir_all(&jobs_dir)
                    .with_context(|| format!("remove {}", jobs_dir.display()))?;
                if verbose {
                    println!("  removed {}", jobs_dir.display());
                }
            }
            actions.push("background job records deleted");
        }

        // ── Test temp dirs ───────────────────────────────────────────────────
        if all || self.test_tmp {
            let killed = reset_stale_test_postgres(verbose)?;
            let removed = reset_test_tmp(verbose)?;
            if killed == 1 {
                actions.push("stale test Postgres process killed");
            } else if killed > 1 {
                actions.push("stale test Postgres processes killed");
            }
            if removed {
                actions.push("stale test temp dirs removed");
            } else {
                actions.push("test temp dir already clean");
            }
        }

        // ── Stale build processes ───────────────────────────────────────────
        if all || self.stale_build_processes {
            let killed = reset_stale_build_processes(verbose)?;
            if killed == 0 {
                actions.push("no stale orphaned build processes found");
            } else if killed == 1 {
                actions.push("stale orphaned build process killed");
            } else {
                actions.push("stale orphaned build processes killed");
            }
        }

        // ── Target dir ────────────────────────────────────────────────────────
        if all || self.target {
            let removed = reset_target(verbose)?;
            if removed.is_empty() {
                actions.push("cargo target dirs already absent");
            } else if removed.len() == 1 {
                actions.push("cargo target dir removed");
            } else {
                actions.push("cargo target dirs removed");
            }
        }

        // ── TLS certificates ─────────────────────────────────────────────────
        if self.tls {
            reset_tls(verbose)?;
            actions.push("TLS certificates regenerated");
        }

        if actions.is_empty() {
            return Err(eyre!(
                "No reset actions performed. Pass --yes with specific flags or bare --yes to reset operational developer state."
            ));
        }

        let mut result = CommandResult::success().with_message(if all {
            "Full reset complete"
        } else {
            "Reset complete"
        });
        for action in &actions {
            result = result.with_detail(*action);
        }
        Ok(result)
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::build()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Reset implementations
// ─────────────────────────────────────────────────────────────────────────────

/// Backup the history DB to a timestamped `.reset.bak.<stamp>` file and
/// optionally reseed it. Returns a status message to push onto the actions list.
fn reset_history_db(path: &std::path::Path, seed: bool, verbose: bool) -> Result<&'static str> {
    if path.exists() {
        let stamp = OffsetDateTime::now_utc()
            .format(&time::format_description::well_known::Iso8601::DEFAULT)
            .unwrap_or_else(|_| "unknown".to_string())
            .replace([':', '-'], "");
        let backup_path = path.with_extension(format!("db.reset.bak.{stamp}"));
        std::fs::rename(path, &backup_path).with_context(|| {
            format!(
                "rename {} -> {} (preserve history before reset)",
                path.display(),
                backup_path.display()
            )
        })?;
        if verbose {
            println!(
                "  preserved history at {} (renamed from {})",
                backup_path.display(),
                path.display()
            );
        }
        // Move auxiliary SQLite/runtime artifacts out of the way.
        for ext in ["db-wal", "db-shm", "db.integrity.json", "cleanup.lock"] {
            let aux = path.with_extension(ext);
            if aux.exists() {
                let _ =
                    std::fs::rename(&aux, aux.with_extension(format!("{ext}.reset.bak.{stamp}")));
            }
        }
    }
    if seed {
        use crate::history::HistoryDb;
        use crate::history::seed::{SeedOptions, seed_history};
        let db = HistoryDb::open(path)?;
        seed_history(&db, &SeedOptions::default())?;
        if verbose {
            println!("  seeded history database with synthetic data (30 days, 100 invocations)");
            println!("  to clear: xtask reset --yes --history");
        }
        Ok("xtask history database renamed to .reset.bak.<ts>; fresh DB seeded")
    } else {
        Ok("xtask history database renamed to .reset.bak.<ts>")
    }
}

fn reset_db(config: &StackConfig, verbose: bool) -> Result<()> {
    // Stop postgres so we can drop the database cleanly
    if config.pg_pid_file().exists() {
        if verbose {
            println!("Stopping PostgreSQL...");
        }
        pg_stop(config, verbose)?;
    }

    use crate::infra::stack::{ensure_directories, pg_init, pg_start};
    ensure_directories(config)?;
    pg_init(config, verbose)?;
    pg_start(config, verbose)?;

    // Drop and recreate
    let mgr = PostgresManager::new(config.to_shared_pg());
    let superuser = &config.postgres.superuser;
    let db = &config.postgres.database;
    let owner = &config.postgres.user;

    if verbose {
        println!("Dropping database {db}...");
    }
    mgr.drop_db(db, superuser)?;

    if verbose {
        println!("Recreating database {db}...");
    }
    mgr.ensure_user(superuser, true, superuser)?;
    mgr.ensure_user(owner, true, superuser)?;
    for role in SHARED_ACCESS_ROLES {
        mgr.ensure_role(role, false, false, superuser)?;
    }
    mgr.ensure_db(db, owner, superuser)?;
    mgr.install_extensions(db, superuser)?;

    // Invalidate preflight cache so schema reapplies on next run
    crate::preflight::invalidate_cache();
    let state_dir = preflight_state_dir();
    let _ = remove_file_if_present(&state_dir.join("schema-apply-hash.txt"), verbose)?;

    if verbose {
        println!("Database reset complete");
    }
    Ok(())
}

fn reset_nats(config: &StackConfig, verbose: bool) -> Result<()> {
    // Stop NATS if running
    if config.nats_pid_file().exists() {
        if verbose {
            println!("Stopping NATS...");
        }
        nats_stop(config, verbose)?;
    }

    // Wipe JetStream data directory
    let nats_data = config.nats_data();
    if nats_data.exists() {
        std::fs::remove_dir_all(&nats_data)
            .with_context(|| format!("remove {}", nats_data.display()))?;
        if verbose {
            println!("  removed {}", nats_data.display());
        }
    }

    Ok(())
}

fn reset_hot_reload_checkpoints(verbose: bool) -> Result<()> {
    let Some(home) = std::env::var_os("HOME") else {
        return Ok(());
    };
    let checkpoint_dir = Path::new(&home).join(".cache/sinex");
    if !checkpoint_dir.exists() {
        return Ok(());
    }

    let mut removed_any = false;
    for entry in std::fs::read_dir(&checkpoint_dir)
        .with_context(|| format!("read {}", checkpoint_dir.display()))?
    {
        let entry =
            entry.with_context(|| format!("read entry under {}", checkpoint_dir.display()))?;
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if !name.ends_with(".checkpoint.json") {
            continue;
        }
        std::fs::remove_file(&path)
            .with_context(|| format!("remove hot-reload checkpoint file {}", path.display()))?;
        removed_any = true;
        if verbose {
            println!("  removed {}", path.display());
        }
    }

    if verbose && !removed_any {
        println!(
            "  no hot-reload checkpoint files under {}",
            checkpoint_dir.display()
        );
    }
    Ok(())
}

fn reset_recovery_spools(verbose: bool) -> Result<()> {
    let mut candidates = Vec::new();
    if let Some(work_dir) = std::env::var_os("SINEX_WORK_DIR") {
        candidates.push(Path::new(&work_dir).join("sinex_event_recovery_spool.jsonl"));
    }
    candidates.push(Path::new("/tmp/sinex/runtime-dev/sinex_event_recovery_spool.jsonl").into());

    let mut removed_any = false;
    for path in candidates {
        if !path.exists() {
            continue;
        }
        std::fs::remove_file(&path)
            .with_context(|| format!("remove runtime recovery spool {}", path.display()))?;
        removed_any = true;
        if verbose {
            println!("  removed {}", path.display());
        }
    }

    if verbose && !removed_any {
        println!("  no runtime recovery spools found");
    }
    Ok(())
}

fn reset_event_engine_runtime_state(verbose: bool) -> Result<()> {
    let mut dirs = Vec::new();
    push_env_path(&mut dirs, "SINEX_EVENT_ENGINE_WORK_DIR");
    push_env_path(&mut dirs, "SINEX_MATERIAL_ASSEMBLER_DIR");
    push_env_path(&mut dirs, "SINEX_CONTENT_STORE_PATH");
    if let Some(cache_dir) = dirs::cache_dir() {
        dirs.push(cache_dir.join("sinex").join("event_engine-dev"));
    }
    dirs.push(PathBuf::from("/tmp/sinex/event_engine-dev"));
    dirs.push(PathBuf::from("/tmp/sinex/event_engine"));

    dirs.sort();
    dirs.dedup();
    for dir in dirs {
        if dir.exists() {
            let trash = rename_runtime_state_for_background_delete(&dir)?;
            spawn_background_delete(&trash, verbose)?;
            if verbose {
                println!(
                    "  moved event-engine runtime state {} -> {} for background cleanup",
                    dir.display(),
                    trash.display()
                );
            }
        } else if verbose {
            println!("  no event-engine runtime state at {}", dir.display());
        }
    }

    reset_runtime_material_tmpfiles(verbose)
}

fn push_env_path(paths: &mut Vec<PathBuf>, var: &str) {
    if let Some(raw) = std::env::var_os(var) {
        paths.push(PathBuf::from(raw));
    }
}

fn rename_runtime_state_for_background_delete(path: &Path) -> Result<PathBuf> {
    let parent = path
        .parent()
        .ok_or_else(|| eyre!("runtime state path has no parent: {}", path.display()))?;
    let name = path
        .file_name()
        .ok_or_else(|| eyre!("runtime state path has no file name: {}", path.display()))?;
    let stamp = reset_timestamp();
    let trash = parent.join(format!(
        ".{}.reset-trash.{}.{}",
        name.to_string_lossy(),
        std::process::id(),
        stamp
    ));
    std::fs::rename(path, &trash)
        .with_context(|| format!("rename {} -> {}", path.display(), trash.display()))?;
    Ok(trash)
}

fn reset_timestamp() -> String {
    OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Iso8601::DEFAULT)
        .unwrap_or_else(|_| "unknown".to_string())
        .replace([':', '-'], "")
}

fn spawn_background_delete(path: &Path, verbose: bool) -> Result<()> {
    if let Ok(mut child) = Command::new("sinnix-scope")
        .arg("background")
        .arg("--")
        .arg("ionice")
        .arg("-c3")
        .arg("rm")
        .arg("-rf")
        .arg(path)
        .spawn()
    {
        if verbose {
            println!(
                "  background cleanup pid {} for {}",
                child.id(),
                path.display()
            );
        }
        std::thread::spawn(move || {
            let _ = child.wait();
        });
        return Ok(());
    }

    let mut child = Command::new("ionice")
        .arg("-c3")
        .arg("rm")
        .arg("-rf")
        .arg(path)
        .spawn()
        .with_context(|| format!("spawn background cleanup for {}", path.display()))?;
    if verbose {
        println!(
            "  background cleanup pid {} for {}",
            child.id(),
            path.display()
        );
    }
    std::thread::spawn(move || {
        let _ = child.wait();
    });
    Ok(())
}

fn reset_runtime_material_tmpfiles(verbose: bool) -> Result<()> {
    let mut runtime_dirs = Vec::new();
    if let Some(work_dir) = std::env::var_os("SINEX_WORK_DIR") {
        runtime_dirs.push(PathBuf::from(work_dir));
    }
    runtime_dirs.push(PathBuf::from("/tmp/sinex/runtime-dev"));

    runtime_dirs.sort();
    runtime_dirs.dedup();
    reset_runtime_material_tmpfiles_in_dirs(&runtime_dirs, verbose)
}

fn reset_runtime_material_tmpfiles_in_dirs(runtime_dirs: &[PathBuf], verbose: bool) -> Result<()> {
    for dir in runtime_dirs {
        if !dir.exists() {
            if verbose {
                println!("  no runtime material tmp dir at {}", dir.display());
            }
            continue;
        }
        for entry in std::fs::read_dir(&dir)
            .with_context(|| format!("read runtime material tmp dir {}", dir.display()))?
        {
            let entry = entry.with_context(|| format!("read entry in {}", dir.display()))?;
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if name.starts_with("sinex_material_") && name.ends_with(".tmp") {
                if verbose {
                    println!("  removed {}", path.display());
                }
                std::fs::remove_file(&path)
                    .with_context(|| format!("remove runtime material tmp {}", path.display()))?;
            }
        }
    }
    Ok(())
}

fn reset_blobs(config: &StackConfig, verbose: bool) -> Result<()> {
    let annex_data = config.annex_data();
    if annex_data.exists() {
        std::fs::remove_dir_all(&annex_data)
            .with_context(|| format!("remove {}", annex_data.display()))?;
        if verbose {
            println!("  removed {}", annex_data.display());
        }
    }
    Ok(())
}

fn reset_preflight_dir(verbose: bool) -> Result<()> {
    let preflight_dir = preflight_state_dir();
    if preflight_dir.exists() {
        std::fs::remove_dir_all(&preflight_dir)
            .with_context(|| format!("remove {}", preflight_dir.display()))?;
        if verbose {
            println!("  removed {}", preflight_dir.display());
        }
    }
    Ok(())
}

fn reset_contracts_hash(verbose: bool) -> Result<()> {
    let state_dir = preflight_state_dir();
    for name in &["contracts-hash.txt", "preflight-cache.json"] {
        let _ = remove_file_if_present(&state_dir.join(name), verbose)?;
    }
    Ok(())
}

fn reset_schema_hash(verbose: bool) -> Result<()> {
    let state_dir = preflight_state_dir();
    for name in &["schema-apply-hash.txt", "preflight-cache.json"] {
        let _ = remove_file_if_present(&state_dir.join(name), verbose)?;
    }
    Ok(())
}

fn reset_test_tmp(verbose: bool) -> Result<bool> {
    let test_tmp = crate::config::workspace_root().join(".sinex/test-tmp");
    if !test_tmp.exists() {
        return Ok(false);
    }
    normalize_tree_permissions(&test_tmp).with_context(|| {
        format!(
            "make stale test temp tree removable at {}",
            test_tmp.display()
        )
    })?;
    let mut removed_any = false;
    for entry in
        std::fs::read_dir(&test_tmp).with_context(|| format!("read {}", test_tmp.display()))?
    {
        let entry = entry.with_context(|| format!("read entry under {}", test_tmp.display()))?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .with_context(|| format!("read file type for {}", path.display()))?;
        if file_type.is_dir() {
            std::fs::remove_dir_all(&path)
                .with_context(|| format!("remove stale test temp dir {}", path.display()))?;
        } else {
            std::fs::remove_file(&path)
                .with_context(|| format!("remove stale test temp file {}", path.display()))?;
        }
        removed_any = true;
    }
    if verbose && removed_any {
        println!("  cleared {}", test_tmp.display());
    }
    Ok(removed_any)
}

fn normalize_tree_permissions(path: &Path) -> Result<()> {
    let metadata =
        std::fs::symlink_metadata(path).with_context(|| format!("stat {}", path.display()))?;
    if metadata.file_type().is_symlink() {
        return Ok(());
    }
    if metadata.is_dir() {
        for entry in std::fs::read_dir(path).with_context(|| format!("read {}", path.display()))? {
            let entry = entry.with_context(|| format!("read entry under {}", path.display()))?;
            normalize_tree_permissions(&entry.path())?;
        }
    }
    let permissions = metadata.permissions();
    let mode = permissions.mode();
    if mode & 0o200 == 0 {
        let mut permissions = permissions;
        permissions.set_mode(mode | 0o700);
        std::fs::set_permissions(path, permissions)
            .with_context(|| format!("chmod u+w {}", path.display()))?;
    }
    Ok(())
}

fn reset_target(verbose: bool) -> Result<Vec<std::path::PathBuf>> {
    let target_dirs = target_dirs_for_reset(
        &crate::config::workspace_target_dir(),
        &crate::config::workspace_root(),
    );
    let mut removed = Vec::new();
    for target_dir in target_dirs {
        if !target_dir.exists() {
            continue;
        }
        if verbose {
            println!(
                "Removing {} (this may take a moment)...",
                target_dir.display()
            );
        }
        std::fs::remove_dir_all(&target_dir)
            .with_context(|| format!("remove {}", target_dir.display()))?;
        if verbose {
            println!("  removed {}", target_dir.display());
        }
        removed.push(target_dir);
    }
    Ok(removed)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StaleBuildProcess {
    pid: u32,
    ppid: u32,
    age_secs: u64,
    command: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BuildProcessProbe {
    pid: u32,
    ppid: u32,
    age_secs: u64,
    command: String,
    parent_command: Option<String>,
}

const STALE_BUILD_PROCESS_MIN_AGE_SECS: u64 = 30 * 60;
const STALE_TEST_POSTGRES_MIN_AGE_SECS: u64 = 30 * 60;

#[derive(Debug, Clone, PartialEq, Eq)]
struct StaleTestPostgresProcess {
    pid: u32,
    ppid: u32,
    age_secs: u64,
    data_dir: std::path::PathBuf,
    command: String,
}

fn reset_stale_test_postgres(verbose: bool) -> Result<usize> {
    let candidates = stale_test_postgres_processes_for_reset(STALE_TEST_POSTGRES_MIN_AGE_SECS);

    let mut killed = 0_usize;
    for candidate in candidates {
        if verbose {
            println!(
                "  killing stale test Postgres pid={} ppid={} age={}s data_dir={}: {}",
                candidate.pid,
                candidate.ppid,
                candidate.age_secs,
                candidate.data_dir.display(),
                truncate_command_for_reset(&candidate.command, 160)
            );
        }
        kill_process_for_reset(candidate.pid)?;
        killed += 1;
        if let Some(root) = candidate.data_dir.parent() {
            let _ = std::fs::remove_dir_all(root);
        }
    }

    Ok(killed)
}

#[cfg(target_os = "linux")]
fn stale_test_postgres_processes_for_reset(min_age_secs: u64) -> Vec<StaleTestPostgresProcess> {
    let uptime_secs = linux_uptime_secs().unwrap_or(0);
    let clock_ticks = linux_clock_ticks_per_second();
    let mut processes = Vec::new();
    let Ok(entries) = std::fs::read_dir("/proc") else {
        return processes;
    };
    let self_pid = std::process::id();

    for entry in entries.flatten() {
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<u32>().ok())
        else {
            continue;
        };
        if pid == self_pid {
            continue;
        }

        let Some((ppid, start_ticks)) = read_linux_proc_stat_for_reset(pid) else {
            continue;
        };
        let Some(command) = read_linux_proc_cmdline_for_reset(pid) else {
            continue;
        };
        let parent_command = read_linux_proc_cmdline_for_reset(ppid);
        let start_secs = start_ticks / clock_ticks.max(1);
        let age_secs = uptime_secs.saturating_sub(start_secs);
        let probe = BuildProcessProbe {
            pid,
            ppid,
            age_secs,
            command,
            parent_command,
        };
        if let Some(process) = classify_stale_test_postgres_process(&probe, min_age_secs) {
            processes.push(process);
        }
    }

    processes
}

#[cfg(not(target_os = "linux"))]
fn stale_test_postgres_processes_for_reset(_min_age_secs: u64) -> Vec<StaleTestPostgresProcess> {
    Vec::new()
}

fn classify_stale_test_postgres_process(
    probe: &BuildProcessProbe,
    min_age_secs: u64,
) -> Option<StaleTestPostgresProcess> {
    if probe.age_secs < min_age_secs {
        return None;
    }
    if !orphaned_build_parent_for_reset(probe.ppid, probe.parent_command.as_deref()) {
        return None;
    }
    if !postgres_command_for_reset(&probe.command) {
        return None;
    }
    let data_dir = postgres_data_dir_from_command(&probe.command)?;
    if !is_sinex_test_postgres_data_dir(&data_dir) {
        return None;
    }

    Some(StaleTestPostgresProcess {
        pid: probe.pid,
        ppid: probe.ppid,
        age_secs: probe.age_secs,
        data_dir,
        command: probe.command.clone(),
    })
}

fn postgres_command_for_reset(command: &str) -> bool {
    let argv0 = command.split_whitespace().next().unwrap_or_default();
    let executable = std::path::Path::new(argv0)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(argv0)
        .to_ascii_lowercase();
    executable == "postgres" || executable == ".postgres-wrapped"
}

fn postgres_data_dir_from_command(command: &str) -> Option<std::path::PathBuf> {
    let mut parts = command.split_whitespace();
    while let Some(part) = parts.next() {
        if part == "-D" {
            return parts.next().map(std::path::PathBuf::from);
        }
    }
    None
}

fn is_sinex_test_postgres_data_dir(path: &Path) -> bool {
    let path = path.to_string_lossy();
    path.contains("/dev/shm/sinex-test-")
        && path.contains("/xtask-sqlx.")
        && path.ends_with("/pgdata")
}

fn reset_stale_build_processes(verbose: bool) -> Result<usize> {
    let target_dirs = target_dirs_for_reset(
        &crate::config::workspace_target_dir(),
        &crate::config::workspace_root(),
    );
    let candidates =
        stale_build_processes_for_reset(&target_dirs, STALE_BUILD_PROCESS_MIN_AGE_SECS);

    let mut killed = 0_usize;
    for candidate in candidates {
        if verbose {
            println!(
                "  killing stale build process pid={} ppid={} age={}s: {}",
                candidate.pid,
                candidate.ppid,
                candidate.age_secs,
                truncate_command_for_reset(&candidate.command, 160)
            );
        }
        kill_process_for_reset(candidate.pid)?;
        killed += 1;
    }

    Ok(killed)
}

#[cfg(target_os = "linux")]
fn stale_build_processes_for_reset(
    target_dirs: &[std::path::PathBuf],
    min_age_secs: u64,
) -> Vec<StaleBuildProcess> {
    let uptime_secs = linux_uptime_secs().unwrap_or(0);
    let clock_ticks = linux_clock_ticks_per_second();
    let mut processes = Vec::new();
    let Ok(entries) = std::fs::read_dir("/proc") else {
        return processes;
    };
    let self_pid = std::process::id();

    for entry in entries.flatten() {
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<u32>().ok())
        else {
            continue;
        };
        if pid == self_pid {
            continue;
        }

        let Some((ppid, start_ticks)) = read_linux_proc_stat_for_reset(pid) else {
            continue;
        };
        let Some(command) = read_linux_proc_cmdline_for_reset(pid) else {
            continue;
        };
        let parent_command = read_linux_proc_cmdline_for_reset(ppid);
        let start_secs = start_ticks / clock_ticks.max(1);
        let age_secs = uptime_secs.saturating_sub(start_secs);
        let probe = BuildProcessProbe {
            pid,
            ppid,
            age_secs,
            command,
            parent_command,
        };
        if let Some(process) = classify_stale_build_process(&probe, target_dirs, min_age_secs) {
            processes.push(process);
        }
    }

    processes
}

#[cfg(not(target_os = "linux"))]
fn stale_build_processes_for_reset(
    _target_dirs: &[std::path::PathBuf],
    _min_age_secs: u64,
) -> Vec<StaleBuildProcess> {
    Vec::new()
}

fn classify_stale_build_process(
    probe: &BuildProcessProbe,
    target_dirs: &[std::path::PathBuf],
    min_age_secs: u64,
) -> Option<StaleBuildProcess> {
    if probe.age_secs < min_age_secs {
        return None;
    }
    if !orphaned_build_parent_for_reset(probe.ppid, probe.parent_command.as_deref()) {
        return None;
    }
    if !build_tool_command_for_reset(&probe.command) {
        return None;
    }
    if !command_mentions_target_dir_for_reset(&probe.command, target_dirs) {
        return None;
    }

    Some(StaleBuildProcess {
        pid: probe.pid,
        ppid: probe.ppid,
        age_secs: probe.age_secs,
        command: probe.command.clone(),
    })
}

fn orphaned_build_parent_for_reset(ppid: u32, parent_command: Option<&str>) -> bool {
    if ppid <= 1 {
        return true;
    }
    parent_command.is_some_and(|command| {
        let command = command.to_ascii_lowercase();
        command.contains("systemd --user") || command.ends_with("/systemd")
    })
}

fn build_tool_command_for_reset(command: &str) -> bool {
    let argv0 = command.split_whitespace().next().unwrap_or_default();
    let executable = std::path::Path::new(argv0)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(argv0)
        .to_ascii_lowercase();
    matches!(
        executable.as_str(),
        "rustc"
            | "rustdoc"
            | "gcc"
            | "cc"
            | "clang"
            | "clang++"
            | "ld"
            | "ld.mold"
            | "mold"
            | "collect2"
            | "sccache"
    ) || executable.starts_with("mold")
}

fn command_mentions_target_dir_for_reset(
    command: &str,
    target_dirs: &[std::path::PathBuf],
) -> bool {
    target_dirs.iter().any(|target_dir| {
        let path = target_dir.to_string_lossy();
        !path.is_empty() && command.contains(path.as_ref())
    })
}

#[cfg(target_os = "linux")]
fn read_linux_proc_stat_for_reset(pid: u32) -> Option<(u32, u64)> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let close = stat.rfind(") ")?;
    let after = stat.get(close + 2..)?;
    let parts: Vec<&str> = after.split_whitespace().collect();
    if parts.len() <= 19 {
        return None;
    }
    let ppid = parts.get(1)?.parse().ok()?;
    let start_ticks = parts.get(19)?.parse().ok()?;
    Some((ppid, start_ticks))
}

#[cfg(target_os = "linux")]
fn read_linux_proc_cmdline_for_reset(pid: u32) -> Option<String> {
    let raw = std::fs::read(format!("/proc/{pid}/cmdline")).ok()?;
    if raw.is_empty() {
        return None;
    }
    Some(String::from_utf8_lossy(&raw).replace('\0', " "))
}

#[cfg(target_os = "linux")]
fn linux_uptime_secs() -> Option<u64> {
    let uptime = std::fs::read_to_string("/proc/uptime").ok()?;
    let first = uptime.split_whitespace().next()?;
    let secs = first.parse::<f64>().ok()?;
    Some(secs.max(0.0) as u64)
}

#[cfg(target_os = "linux")]
fn linux_clock_ticks_per_second() -> u64 {
    let ticks = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    if ticks > 0 { ticks as u64 } else { 100 }
}

#[cfg(target_os = "linux")]
fn kill_process_for_reset(pid: u32) -> Result<()> {
    let raw_pid = pid as libc::pid_t;
    let term_rc = unsafe { libc::kill(raw_pid, libc::SIGTERM) };
    if term_rc != 0 {
        let error = std::io::Error::last_os_error();
        if !matches!(error.raw_os_error(), Some(libc::ESRCH)) {
            return Err(error).wrap_err_with(|| format!("send SIGTERM to stale build pid {pid}"));
        }
        return Ok(());
    }

    std::thread::sleep(std::time::Duration::from_millis(500));
    let alive_rc = unsafe { libc::kill(raw_pid, 0) };
    if alive_rc == 0 {
        let kill_rc = unsafe { libc::kill(raw_pid, libc::SIGKILL) };
        if kill_rc != 0 {
            let error = std::io::Error::last_os_error();
            if !matches!(error.raw_os_error(), Some(libc::ESRCH)) {
                return Err(error).wrap_err_with(|| {
                    format!("send SIGKILL to stale build pid {pid} after SIGTERM")
                });
            }
        }
    }

    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn kill_process_for_reset(_pid: u32) -> Result<()> {
    Ok(())
}

fn truncate_command_for_reset(command: &str, max: usize) -> String {
    if command.len() <= max {
        command.to_string()
    } else {
        format!("{}...", &command[..max])
    }
}

fn target_dirs_for_reset(
    configured_target_dir: &Path,
    workspace_root: &Path,
) -> Vec<std::path::PathBuf> {
    let historical_target_dir = workspace_root.join(".sinex/target");
    let mut dirs = vec![configured_target_dir.to_path_buf()];
    if historical_target_dir != configured_target_dir {
        dirs.push(historical_target_dir);
    }
    dirs
}

fn reset_tls(verbose: bool) -> Result<()> {
    let workspace_root = crate::config::workspace_root();
    let tls_dir = workspace_root.join(".sinex/tls");
    if tls_dir.exists() {
        std::fs::remove_dir_all(&tls_dir)
            .with_context(|| format!("remove {}", tls_dir.display()))?;
    }
    // Regenerate using the library function
    let cert_config = crate::tls::CertConfig {
        output_dir: tls_dir,
        san: vec!["localhost".to_string(), "127.0.0.1".to_string()],
        ca_name: "sinex-dev-ca".to_string(),
        validity_days: crate::tls::DEFAULT_DEV_CERT_VALIDITY_DAYS,
        force: true,
    };
    crate::tls::generate_dev_certs(&cert_config)?;
    if verbose {
        println!("TLS certificates regenerated in .sinex/tls/");
    }
    Ok(())
}

/// Path to the configured preflight state directory.
fn preflight_state_dir() -> std::path::PathBuf {
    crate::config::config().preflight_state_dir()
}

fn remove_file_if_present(path: &Path, verbose: bool) -> Result<bool> {
    if !path.exists() {
        return Ok(false);
    }

    std::fs::remove_file(path).with_context(|| format!("remove {}", path.display()))?;
    if verbose {
        println!("  removed {}", path.display());
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::{EnvGuard, sinex_serial_test, sinex_test};

    #[sinex_test]
    async fn test_remove_file_if_present_reports_remove_failures() -> TestResult<()> {
        let temp = tempfile::tempdir()?;
        let error = remove_file_if_present(temp.path(), false).unwrap_err();
        assert!(format!("{error:#}").contains("remove "));
        Ok(())
    }

    #[sinex_test]
    async fn test_remove_file_if_present_returns_false_for_missing_path() -> TestResult<()> {
        let temp = tempfile::tempdir()?;
        let removed = remove_file_if_present(&temp.path().join("missing.txt"), false)?;
        assert!(!removed);
        Ok(())
    }

    #[sinex_test]
    async fn test_reset_hot_reload_checkpoints_removes_only_checkpoint_json() -> TestResult<()> {
        let temp = tempfile::tempdir()?;
        let cache = temp.path().join(".cache/sinex");
        std::fs::create_dir_all(&cache)?;
        std::fs::write(cache.join("raindrop-bookmarks.checkpoint.json"), "{}")?;
        std::fs::write(cache.join("keep.json"), "{}")?;
        std::fs::write(cache.join("notes.checkpoint.txt"), "")?;

        let mut env = EnvGuard::new();
        env.set("HOME", temp.path().to_string_lossy().as_ref());

        reset_hot_reload_checkpoints(false)?;

        assert!(!cache.join("raindrop-bookmarks.checkpoint.json").exists());
        assert!(cache.join("keep.json").exists());
        assert!(cache.join("notes.checkpoint.txt").exists());
        Ok(())
    }

    #[sinex_test]
    async fn test_reset_runtime_material_tmpfiles_removes_only_material_fragments() -> TestResult<()>
    {
        let temp = tempfile::tempdir()?;
        std::fs::write(temp.path().join("sinex_material_abc.tmp"), "fragment")?;
        std::fs::write(temp.path().join("sinex_material_abc.txt"), "keep")?;
        std::fs::write(temp.path().join("other.tmp"), "keep")?;

        reset_runtime_material_tmpfiles_in_dirs(&[temp.path().to_path_buf()], false)?;

        assert!(!temp.path().join("sinex_material_abc.tmp").exists());
        assert!(temp.path().join("sinex_material_abc.txt").exists());
        assert!(temp.path().join("other.tmp").exists());
        Ok(())
    }

    #[sinex_test]
    async fn test_target_dirs_for_reset_includes_historical_sinex_target() -> TestResult<()> {
        let workspace = tempfile::tempdir()?;
        let configured = workspace.path().join(".sinex/cache/target");

        let dirs = target_dirs_for_reset(&configured, workspace.path());

        assert_eq!(
            dirs,
            vec![configured, workspace.path().join(".sinex/target")]
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_target_dirs_for_reset_deduplicates_historical_target() -> TestResult<()> {
        let workspace = tempfile::tempdir()?;
        let configured = workspace.path().join(".sinex/target");

        let dirs = target_dirs_for_reset(&configured, workspace.path());

        assert_eq!(dirs, vec![configured]);
        Ok(())
    }

    #[sinex_test]
    async fn test_stale_build_classifier_requires_age_orphan_tool_and_target() -> TestResult<()> {
        let target = std::path::PathBuf::from("/tmp/sinex-target");
        let probe = BuildProcessProbe {
            pid: 42,
            ppid: 1,
            age_secs: STALE_BUILD_PROCESS_MIN_AGE_SECS,
            command: "/nix/store/bin/ld.mold /tmp/sinex-target/debug/deps/libfoo.rlib".to_string(),
            parent_command: Some("/sbin/init".to_string()),
        };

        let classified = classify_stale_build_process(
            &probe,
            std::slice::from_ref(&target),
            STALE_BUILD_PROCESS_MIN_AGE_SECS,
        );

        assert_eq!(
            classified,
            Some(StaleBuildProcess {
                pid: 42,
                ppid: 1,
                age_secs: STALE_BUILD_PROCESS_MIN_AGE_SECS,
                command: probe.command.clone(),
            })
        );

        let fresh = BuildProcessProbe {
            age_secs: STALE_BUILD_PROCESS_MIN_AGE_SECS - 1,
            ..probe.clone()
        };
        assert!(
            classify_stale_build_process(&fresh, &[target], STALE_BUILD_PROCESS_MIN_AGE_SECS)
                .is_none()
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_stale_build_classifier_rejects_live_parent() -> TestResult<()> {
        let target = std::path::PathBuf::from("/tmp/sinex-target");
        let probe = BuildProcessProbe {
            pid: 42,
            ppid: 99,
            age_secs: STALE_BUILD_PROCESS_MIN_AGE_SECS,
            command: "rustc --crate-name foo /tmp/sinex-target/debug/deps/foo.rs".to_string(),
            parent_command: Some("cargo check -p xtask".to_string()),
        };

        assert!(
            classify_stale_build_process(&probe, &[target], STALE_BUILD_PROCESS_MIN_AGE_SECS)
                .is_none()
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_stale_build_classifier_rejects_non_target_commands() -> TestResult<()> {
        let probe = BuildProcessProbe {
            pid: 42,
            ppid: 1,
            age_secs: STALE_BUILD_PROCESS_MIN_AGE_SECS,
            command: "gcc /tmp/other-target/debug/build/foo.o".to_string(),
            parent_command: Some("/sbin/init".to_string()),
        };

        assert!(
            classify_stale_build_process(
                &probe,
                &[std::path::PathBuf::from("/tmp/sinex-target")],
                STALE_BUILD_PROCESS_MIN_AGE_SECS,
            )
            .is_none()
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_orphaned_build_parent_accepts_user_systemd() -> TestResult<()> {
        assert!(orphaned_build_parent_for_reset(
            3492,
            Some("/nix/store/systemd/lib/systemd/systemd --user")
        ));
        assert!(!orphaned_build_parent_for_reset(
            3492,
            Some("cargo check -p xtask")
        ));
        Ok(())
    }

    #[sinex_test]
    async fn test_stale_test_postgres_classifier_accepts_orphaned_test_cluster() -> TestResult<()> {
        let probe = BuildProcessProbe {
            pid: 42,
            ppid: 99,
            age_secs: STALE_TEST_POSTGRES_MIN_AGE_SECS,
            command: "/nix/store/postgresql/bin/postgres -D /dev/shm/sinex-test-sinity-hash/xtask-sqlx.ABCD/pgdata".to_string(),
            parent_command: Some("/nix/store/systemd/lib/systemd/systemd --user".to_string()),
        };

        let classified =
            classify_stale_test_postgres_process(&probe, STALE_TEST_POSTGRES_MIN_AGE_SECS);

        assert_eq!(
            classified,
            Some(StaleTestPostgresProcess {
                pid: 42,
                ppid: 99,
                age_secs: STALE_TEST_POSTGRES_MIN_AGE_SECS,
                data_dir: std::path::PathBuf::from(
                    "/dev/shm/sinex-test-sinity-hash/xtask-sqlx.ABCD/pgdata"
                ),
                command: probe.command.clone(),
            })
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_stale_test_postgres_classifier_rejects_checkout_dev_postgres() -> TestResult<()> {
        let probe = BuildProcessProbe {
            pid: 42,
            ppid: 99,
            age_secs: STALE_TEST_POSTGRES_MIN_AGE_SECS,
            command: "/nix/store/postgresql/bin/postgres -D /var/cache/sinex/sinity/hash/dev-state/data/postgres".to_string(),
            parent_command: Some("/nix/store/systemd/lib/systemd/systemd --user".to_string()),
        };

        assert!(
            classify_stale_test_postgres_process(&probe, STALE_TEST_POSTGRES_MIN_AGE_SECS)
                .is_none()
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_stale_test_postgres_classifier_rejects_live_parent() -> TestResult<()> {
        let probe = BuildProcessProbe {
            pid: 42,
            ppid: 99,
            age_secs: STALE_TEST_POSTGRES_MIN_AGE_SECS,
            command: "/nix/store/postgresql/bin/postgres -D /dev/shm/sinex-test-sinity-hash/xtask-sqlx.ABCD/pgdata".to_string(),
            parent_command: Some("xtask test -p xtask".to_string()),
        };

        assert!(
            classify_stale_test_postgres_process(&probe, STALE_TEST_POSTGRES_MIN_AGE_SECS)
                .is_none()
        );
        Ok(())
    }

    #[sinex_serial_test]
    async fn test_reset_test_tmp_removes_readonly_stale_dirs() -> TestResult<()> {
        let workspace = tempfile::tempdir()?;
        std::fs::write(workspace.path().join("Cargo.toml"), "[workspace]\n")?;
        std::fs::create_dir_all(workspace.path().join("xtask"))?;
        std::fs::write(
            workspace.path().join("xtask/Cargo.toml"),
            "[package]\nname = \"xtask\"\nversion = \"0.0.0\"\nedition = \"2024\"\n",
        )?;
        let stale_dir = workspace
            .path()
            .join(".sinex/test-tmp/stale/.git/annex/objects");
        std::fs::create_dir_all(&stale_dir)?;
        let readonly_file = stale_dir.join("readonly.tmp");
        std::fs::write(&readonly_file, "stale")?;
        let mut permissions = std::fs::metadata(&readonly_file)?.permissions();
        permissions.set_readonly(true);
        std::fs::set_permissions(&readonly_file, permissions)?;

        let cwd = std::env::current_dir()?;
        std::env::set_current_dir(workspace.path())?;
        let result = reset_test_tmp(false);
        std::env::set_current_dir(cwd)?;

        assert!(result?);
        assert!(!workspace.path().join(".sinex/test-tmp/stale").exists());
        Ok(())
    }

    #[sinex_serial_test]
    async fn test_reset_target_removes_configured_and_historical_dirs() -> TestResult<()> {
        let workspace = tempfile::tempdir()?;
        std::fs::write(workspace.path().join("Cargo.toml"), "[workspace]\n")?;
        std::fs::create_dir_all(workspace.path().join("xtask"))?;
        std::fs::write(
            workspace.path().join("xtask/Cargo.toml"),
            "[package]\nname = \"xtask\"\nversion = \"0.0.0\"\nedition = \"2024\"\n",
        )?;
        let configured = workspace.path().join(".sinex/cache/target");
        let historical = workspace.path().join(".sinex/target");
        std::fs::create_dir_all(&configured)?;
        std::fs::create_dir_all(&historical)?;

        let mut env = crate::sandbox::EnvGuard::with_keys(&["CARGO_TARGET_DIR"]);
        env.set("CARGO_TARGET_DIR", &configured);
        let cwd = std::env::current_dir()?;
        std::env::set_current_dir(workspace.path())?;

        let result = reset_target(false);
        std::env::set_current_dir(cwd)?;
        let removed = result?;

        assert_eq!(removed, vec![configured.clone(), historical.clone()]);
        assert!(!configured.exists());
        assert!(!historical.exists());
        Ok(())
    }
}
