//! `xtask history unify` / `xtask history import` — fold stray ledgers in.
//!
//! Every workspace writes to one shared history database, but a worktree
//! running an xtask built before that rule keeps a private
//! `.sinex/state/xtask-history.db`. These commands absorb such a file into the
//! canonical ledger with its workspace provenance intact, so removing the
//! worktree no longer destroys its evidence.

use std::path::{Path, PathBuf};

use color_eyre::eyre::{Result, WrapErr, bail};
use console::style;

use crate::command::{CommandContext, CommandResult};
use crate::history::{
    HistoryDb, ImportReport, WorkspaceAttribution, attribution_for_workspace_db,
    backfill_workspace_attribution, recorded_workspace_roots,
};

/// Absorb one named history database.
pub fn execute_import(
    ctx: &CommandContext,
    source: &Path,
    workspace_root: Option<&str>,
    workspace_name: Option<&str>,
    remove_source: bool,
) -> Result<CommandResult> {
    let canonical = ctx.history_db_path().to_path_buf();
    if same_file(source, &canonical) {
        bail!(
            "{} is already the canonical ledger; nothing to import",
            source.display()
        );
    }
    let attribution = resolve_attribution(source, workspace_root, workspace_name)?;
    let db = HistoryDb::open(&canonical)?;
    let report = crate::history::import_history(&db, source, &attribution)?;
    print_report(&report);
    if remove_source {
        retire_source(source, &report)?;
    }
    Ok(CommandResult::success())
}

/// Absorb the private ledger of every linked worktree of this checkout.
pub fn execute_unify(ctx: &CommandContext, remove_sources: bool, dry_run: bool) -> Result<CommandResult> {
    let canonical = ctx.history_db_path().to_path_buf();
    let sources = stray_worktree_ledgers(&canonical)?;

    if dry_run {
        if sources.is_empty() {
            println!("No stray worktree history databases found.");
        } else {
            println!("Would import {} stray ledger(s):", sources.len());
            for source in &sources {
                println!("  {}", source.display());
            }
        }
        return Ok(CommandResult::success());
    }

    let db = HistoryDb::open(&canonical)?;
    let mut inserted = 0usize;
    for source in &sources {
        let attribution = resolve_attribution(source, None, None)?;
        let report = crate::history::import_history(&db, source, &attribution)?;
        print_report(&report);
        inserted += report.invocations_inserted;
        if remove_sources {
            retire_source(source, &report)?;
        }
    }
    if !sources.is_empty() {
        println!(
            "{} {inserted} invocation(s) absorbed from {} ledger(s)",
            style("unified:").green().bold(),
            sources.len()
        );
    }

    // Rows recorded before the ledger carried workspace columns can only be
    // placed by their working directory, and only against roots we know of.
    let backfill = backfill_workspace_attribution(&db, &known_workspace_roots(&db, &canonical)?)?;
    if backfill.unattributed_before > 0 {
        println!(
            "{} {} of {} legacy row(s) attributed",
            style("backfill:").green().bold(),
            backfill.attributed(),
            backfill.unattributed_before
        );
        for (workspace, count) in &backfill.attributed_by_workspace {
            println!("  {workspace}: {count}");
        }
        if backfill.unresolved > 0 {
            println!(
                "  {} row(s) left unattributed: their cwd matches no known workspace root",
                backfill.unresolved
            );
        }
    }
    Ok(CommandResult::success())
}

/// Workspace roots this machine can vouch for: the main checkout, every linked
/// worktree that still exists, and every root already named in the ledger.
fn known_workspace_roots(db: &HistoryDb, canonical: &Path) -> Result<Vec<String>> {
    let mut roots: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    if let Some(main) = canonical.parent().and_then(Path::parent).and_then(Path::parent) {
        roots.insert(main.display().to_string());
    }
    let output = std::process::Command::new("git")
        .args(["worktree", "list", "--porcelain"])
        .output()
        .context("failed to list git worktrees")?;
    if output.status.success() {
        let listing = String::from_utf8_lossy(&output.stdout).into_owned();
        for line in listing.lines() {
            if let Some(root) = line.strip_prefix("worktree ") {
                roots.insert(root.to_string());
            }
        }
    }
    roots.extend(recorded_workspace_roots(db)?);
    Ok(roots.into_iter().collect())
}

/// Private history databases held by linked worktrees of this checkout.
///
/// `git worktree list --porcelain` is the authority on which worktrees exist,
/// so no directory root has to be hardcoded or guessed.
fn stray_worktree_ledgers(canonical: &Path) -> Result<Vec<PathBuf>> {
    let output = std::process::Command::new("git")
        .args(["worktree", "list", "--porcelain"])
        .output()
        .context("failed to list git worktrees")?;
    if !output.status.success() {
        bail!("git worktree list failed; run from inside a sinex checkout");
    }
    let listing = String::from_utf8(output.stdout).context("git worktree list produced non-UTF-8")?;
    let mut found = Vec::new();
    for line in listing.lines() {
        let Some(root) = line.strip_prefix("worktree ") else {
            continue;
        };
        let candidate = Path::new(root).join(".sinex/state/xtask-history.db");
        if candidate.is_file() && !same_file(&candidate, canonical) {
            found.push(candidate);
        }
    }
    Ok(found)
}

fn resolve_attribution(
    source: &Path,
    workspace_root: Option<&str>,
    workspace_name: Option<&str>,
) -> Result<WorkspaceAttribution> {
    if let Some(root) = workspace_root {
        let name = workspace_name
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| crate::config::workspace_name_for(Path::new(root)));
        return Ok(WorkspaceAttribution {
            root: root.to_string(),
            name,
        });
    }
    let mut derived = attribution_for_workspace_db(source).ok_or_else(|| {
        color_eyre::eyre::eyre!(
            "cannot infer the workspace for {}; pass --workspace-root",
            source.display()
        )
    })?;
    if let Some(name) = workspace_name {
        derived.name = name.to_string();
    }
    Ok(derived)
}

/// Delete an absorbed ledger, refusing while anything is unaccounted for.
fn retire_source(source: &Path, report: &ImportReport) -> Result<()> {
    let accounted = report.invocations_inserted + report.invocations_duplicate;
    if accounted != report.invocations_seen {
        bail!(
            "refusing to remove {}: {} of {} invocations were neither imported nor matched",
            source.display(),
            report.invocations_seen - accounted,
            report.invocations_seen
        );
    }
    for suffix in ["", "-wal", "-shm"] {
        let path = PathBuf::from(format!("{}{suffix}", source.display()));
        if path.exists() {
            std::fs::remove_file(&path)
                .with_context(|| format!("failed to remove {}", path.display()))?;
        }
    }
    println!("  removed {}", source.display());
    Ok(())
}

fn print_report(report: &ImportReport) {
    println!(
        "{} {} → workspace {}",
        style("import").cyan().bold(),
        report.source.display(),
        report.attribution.name
    );
    println!(
        "  invocations: {} seen, {} inserted, {} already present",
        report.invocations_seen, report.invocations_inserted, report.invocations_duplicate
    );
    for (table, count) in &report.rows_by_table {
        println!("  {table}: {count}");
    }
}

fn same_file(left: &Path, right: &Path) -> bool {
    match (left.canonicalize(), right.canonicalize()) {
        (Ok(left), Ok(right)) => left == right,
        _ => left == right,
    }
}
