//! Reset command — wipe developer state for a fresh start.
//!
//! Each flag targets a specific category of state. With no category flags,
//! `--yes` alone resets everything: db + nats + preflight + jobs + target.
//!
//! `--contracts` and `--schema` are surgical — they delete only the hash
//! files that gate preflight re-deployment. This forces re-run without
//! touching data.

use clap::Args;
use color_eyre::eyre::{Result, WrapErr, eyre};
use sinex_schema::apply::SHARED_ACCESS_ROLES;
use std::path::Path;
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

    /// Wipe the entire .sinex/preflight/ directory (forces full preflight on next run).
    #[arg(long)]
    preflight: bool,

    /// Delete contracts hash file (forces contract redeploy on next run, no data loss).
    #[arg(long)]
    contracts: bool,

    /// Delete schema apply hash file (forces schema reapply on next run, no data loss).
    #[arg(long)]
    schema: bool,

    /// Delete the xtask history database.
    #[arg(long)]
    history: bool,

    /// When used with --history: reseed the history database with synthetic data.
    #[arg(long, requires = "history")]
    seed: bool,

    /// Delete background job records and output files.
    #[arg(long)]
    jobs: bool,

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
        // Determine whether this is "reset everything" mode
        let any_specific = self.db
            || self.nats
            || self.blobs
            || self.preflight
            || self.contracts
            || self.schema
            || self.history
            || self.jobs
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
            actions.push("NATS JetStream data wiped");
        }

        // ── Preflight cache ───────────────────────────────────────────────────
        if all || self.preflight {
            reset_preflight_dir(verbose)?;
            actions.push(".sinex/preflight/ removed");
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
        if all || self.history {
            let path = cfg.history_db_path();
            // History is the user's accumulated dev-loop record across weeks
            // or months — never silently delete on reset.  Rename to a
            // timestamped backup alongside the live file so the data
            // survives every `xtask reset --history` / `--all` invocation
            // and can be inspected/recovered if the wipe was unintended.
            // Also rename the WAL/SHM/integrity-stamp/cleanup-lock siblings
            // so the next open starts on a clean slate without inheriting
            // stale auxiliary files.
            if path.exists() {
                let stamp = OffsetDateTime::now_utc()
                    .format(&time::format_description::well_known::Iso8601::DEFAULT)
                    .unwrap_or_else(|_| "unknown".to_string())
                    .replace(':', "")
                    .replace('-', "");
                let backup_path = path
                    .with_extension(format!("db.reset.bak.{stamp}"));
                std::fs::rename(&path, &backup_path).with_context(|| {
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
                // Move auxiliary SQLite/runtime artifacts out of the way so
                // the recreated DB does not pick up the old ones.
                for ext in ["db-wal", "db-shm", "db.integrity.json", "cleanup.lock"] {
                    let aux = path.with_extension(ext);
                    if aux.exists() {
                        let _ = std::fs::rename(
                            &aux,
                            aux.with_extension(format!("{ext}.reset.bak.{stamp}")),
                        );
                    }
                }
            }
            if self.seed {
                // Reseed with synthetic data after wipe.
                // Exception: reset deletes then recreates the DB — ctx.with_history_db()
                // would re-open the old (now-deleted) path. Must open the fresh path directly.
                use crate::history::HistoryDb;
                use crate::history::seed::{SeedOptions, seed_history};
                let db = HistoryDb::open(&path)?;
                seed_history(&db, &SeedOptions::default())?;
                if verbose {
                    println!(
                        "  seeded history database with synthetic data (30 days, 100 invocations)"
                    );
                    println!("  to clear: xtask reset --yes --history");
                }
                actions.push("xtask history database renamed to .reset.bak.<ts>; fresh DB seeded");
            } else {
                actions.push("xtask history database renamed to .reset.bak.<ts>");
            }
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

        // ── Target dir ────────────────────────────────────────────────────────
        if all || self.target {
            reset_target(verbose)?;
            actions.push("cargo target/ removed");
        }

        // ── TLS certificates ─────────────────────────────────────────────────
        if self.tls {
            reset_tls(verbose)?;
            actions.push("TLS certificates regenerated");
        }

        if actions.is_empty() {
            return Err(eyre!(
                "No reset actions performed. Pass --yes with specific flags or bare --yes to reset everything."
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

fn reset_target(verbose: bool) -> Result<()> {
    let workspace_root = crate::config::workspace_root();
    let target_dir = workspace_root.join("target");
    if target_dir.exists() {
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
    }
    Ok(())
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

/// Path to the `.sinex/preflight/` directory (relative to workspace root).
fn preflight_state_dir() -> std::path::PathBuf {
    let crate_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    crate_dir.join("../.sinex/preflight")
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
    use crate::sandbox::sinex_test;

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
}
