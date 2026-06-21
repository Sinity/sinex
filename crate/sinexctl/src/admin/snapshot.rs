//! Snapshot implementation for the `sinexctl ops state` command surface.
//! surface.
//!
//! Captures Postgres (via `pg_dump`), NATS `JetStream` state, the CAS blob
//! repository, and remaining per-source host state files into a single
//! zstd-compressed tar archive.

use clap::Parser;
use color_eyre::eyre::{Context, Result, bail, eyre};
use serde::Serialize;
use sinex_primitives::source_contracts;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use walkdir::{DirEntry, WalkDir};

use crate::admin::exec;
use crate::admin::manifest::{
    CasExtras, ComponentExtras, ComponentRecord, NatsExtras, PostgresExtras, SnapshotManifest,
    StateExtras, Totals,
};
use crate::admin::staging::StagingDir;

/// Component selector — controls which subsystems are captured.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Component {
    Postgres,
    Nats,
    Cas,
    State,
}

impl Component {
    #[must_use]
    pub fn name(&self) -> &'static str {
        match self {
            Self::Postgres => "postgres",
            Self::Nats => "nats",
            Self::Cas => "cas",
            Self::State => "state",
        }
    }

    #[must_use]
    pub fn all() -> Vec<Self> {
        vec![Self::Postgres, Self::Nats, Self::Cas, Self::State]
    }

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "postgres" => Ok(Self::Postgres),
            "nats" => Ok(Self::Nats),
            "cas" => Ok(Self::Cas),
            "state" => Ok(Self::State),
            other => {
                bail!("unknown component `{other}`; valid components: postgres,nats,cas,state")
            }
        }
    }
}

// ── CLI definition ────────────────────────────────────────────────────────────

/// Create a snapshot of the complete sinex runtime state.
#[derive(Debug, Parser)]
#[command(after_help = "\
EXAMPLES:
    # Snapshot to /var/backup/sinex with defaults (zstd level 3, all components)
    sinexctl ops state snapshot --output /var/backup/sinex/2026-05-15.sinex.tar.zst

    # Higher compression for archival
    sinexctl ops state snapshot --output /var/backup/sinex/latest.sinex.tar.zst --compression 15

    # Estimate sizes without writing anything
    sinexctl ops state snapshot --output /var/backup/sinex/latest.sinex.tar.zst --dry-run

    # Capture without stopping services for urgent forensic preservation
    sinexctl ops state snapshot --output /var/backup/sinex/live.sinex.tar.zst --mode live

    # Automatically stop services and snapshot postgres + CAS only
    sinexctl ops state snapshot --output /var/backup/sinex/pg-cas.tar.zst \\
        --components postgres,cas --auto-stop

RESTORE:
    Inspect and run an isolated drill before any manual live restore:
        sinexctl ops state inspect --archive <archive>
        sinexctl ops state restore --archive <archive> --target-dir /tmp/restore-drill --dry-run
        sinexctl ops state restore --archive <archive> --target-dir /tmp/restore-drill \\
            --confirm-restore --allow-active-services
    See crate/sinexctl/docs/state_snapshot.md for the full restore runbook.
")]
pub struct AdminSnapshotCommand {
    /// Path to write the snapshot archive (e.g. /var/backup/sinex/2026-05.tar.zst).
    #[arg(long)]
    pub output: PathBuf,

    /// zstd compression level (1-19, default 3).
    #[arg(long, default_value = "3", value_parser = clap::value_parser!(u8).range(1..=19))]
    pub compression: u8,

    /// Number of zstd parallel workers (0 = use all cores).
    #[arg(long, default_value = "0")]
    pub workers: u32,

    /// Snapshot mode.
    ///
    /// `quiesce` requires sinex services to be stopped, or stops them with
    /// `--auto-stop`. `live` captures without stopping services and records
    /// that weaker consistency mode in the manifest.
    #[arg(long, default_value = "quiesce")]
    pub mode: String,

    /// Estimate sizes and print a summary; do not write any archive.
    #[arg(long)]
    pub dry_run: bool,

    /// `PostgreSQL` connection URL (defaults to `DATABASE_URL` env var).
    #[arg(long, env = "DATABASE_URL")]
    pub database_url: Option<String>,

    /// Sinex state directory root (defaults to `SINEX_STATE_DIR`, then /var/lib/sinex).
    #[arg(long, env = "SINEX_STATE_DIR")]
    pub state_dir: Option<PathBuf>,

    /// Stop sinex services automatically if they are running.
    #[arg(long)]
    pub auto_stop: bool,

    /// Comma-separated component list to capture (default: postgres,nats,cas,state).
    #[arg(
        long,
        default_value = "postgres,nats,cas,state",
        value_delimiter = ',',
        value_parser = parse_component_str
    )]
    pub components: Vec<Component>,
}

/// Inspect a snapshot archive without restoring it.
#[derive(Debug, Parser)]
#[command(after_help = "\
EXAMPLES:
    sinexctl ops state inspect --archive /var/backup/sinex/latest.sinex.tar.zst

NOTES:
    This reads manifest.json from the archive and checks that non-empty
    component paths named by the manifest are present in the tar member list.
")]
pub struct AdminSnapshotInspectCommand {
    /// Snapshot archive to inspect.
    #[arg(long)]
    pub archive: PathBuf,
}

/// Validate a snapshot archive restore plan without writing target state.
#[derive(Debug, Parser)]
#[command(after_help = "\
EXAMPLES:
    sinexctl ops state restore --archive /var/backup/sinex/latest.sinex.tar.zst \\
        --target-dir /tmp/sinex-restore-drill --dry-run

NOTES:
    Without --dry-run, this executes an isolated restore drill for state, CAS,
    NATS, and Postgres when --restore-database-url is supplied. Postgres restore
    writes only to that explicitly supplied drill database.
")]
pub struct AdminSnapshotRestoreCommand {
    /// Snapshot archive to validate for restore.
    #[arg(long)]
    pub archive: PathBuf,

    /// Empty target directory intended for the restore drill.
    #[arg(long)]
    pub target_dir: PathBuf,

    /// Plan and validate only; do not extract or write restored state.
    #[arg(long)]
    pub dry_run: bool,

    /// Permit planning against a non-empty target directory.
    #[arg(long)]
    pub allow_non_empty_target: bool,

    /// Confirm execution of an isolated restore drill into an empty target directory.
    #[arg(long)]
    pub confirm_restore: bool,

    /// Permit isolated restore drill execution while sinex services are active.
    #[arg(long)]
    pub allow_active_services: bool,

    /// Empty target `PostgreSQL` database URL for restoring postgres components.
    #[arg(long, env = "SINEX_RESTORE_DATABASE_URL")]
    pub restore_database_url: Option<String>,

    /// Internal test/ops override for the `pg_restore` binary.
    #[arg(long, hide = true, env = "SINEX_PG_RESTORE_BIN")]
    pub pg_restore_bin: Option<PathBuf>,

    /// Internal test/ops override for the `psql` binary used by restore checks.
    #[arg(long, hide = true, env = "SINEX_PSQL_BIN")]
    pub psql_bin: Option<PathBuf>,
}

fn parse_component_str(s: &str) -> std::result::Result<Component, String> {
    Component::from_str(s).map_err(|e| e.to_string())
}

// ── Result types ────────────────────────────────────────────────────────────

/// What gets printed / returned from the snapshot command.
#[derive(Debug, Serialize)]
pub struct SnapshotResult {
    pub mode: &'static str,
    pub snapshot_id: String,
    pub output_path: Option<String>,
    pub archive_bytes: Option<u64>,
    pub uncompressed_bytes: u64,
    pub source_ids: Vec<String>,
    pub components_captured: Vec<ComponentSummary>,
}

#[derive(Debug, Serialize)]
pub struct ComponentSummary {
    pub name: String,
    pub bytes: u64,
    pub blake3: String,
}

/// Operator-facing summary produced by `admin snapshot-inspect`.
#[derive(Debug, Serialize)]
pub struct SnapshotInspectResult {
    pub archive_path: String,
    pub snapshot_id: String,
    pub created_at: String,
    pub mode: String,
    pub sinex_version: String,
    pub git_sha: Option<String>,
    pub host: String,
    pub archive_entries: usize,
    pub source_count: usize,
    pub source_ids: Vec<String>,
    pub state_source_count: Option<usize>,
    pub state_private_mode_state_present: Option<bool>,
    pub component_count: usize,
    pub components: Vec<ComponentSummary>,
    pub missing_component_paths: Vec<String>,
    pub manifest: SnapshotManifest,
}

/// Operator-facing restore drill plan produced by `admin snapshot-restore`.
#[derive(Debug, Serialize)]
pub struct SnapshotRestorePlanResult {
    pub archive_path: String,
    pub snapshot_id: String,
    pub dry_run: bool,
    pub target_dir: String,
    pub target_empty: bool,
    pub active_services: Vec<String>,
    pub archive_sensitivity: String,
    pub key_policy: String,
    pub planned_steps: Vec<RestorePlanStep>,
    pub drill_checks: RestoreDrillChecks,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_checks: Option<RestoreObservedChecks>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct RestorePlanStep {
    pub component: String,
    pub action: String,
    pub archive_path: String,
    pub target_path: String,
}

#[derive(Debug, Serialize)]
pub struct RestoreDrillChecks {
    pub source_count: usize,
    pub postgres_table_count: usize,
    pub nats_member_count: Option<usize>,
    pub cas_blob_count: Option<u64>,
    pub private_mode_state_present: bool,
    pub missing_component_paths: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct RestoreObservedChecks {
    pub target_entry_count: usize,
    pub checks_passed: bool,
    pub failed_checks: Vec<String>,
    pub source_count: usize,
    pub source_ids_match: bool,
    pub component_blake3: BTreeMap<String, String>,
    pub component_blake3_matches: BTreeMap<String, bool>,
    pub postgres_row_counts: BTreeMap<String, i64>,
    pub postgres_row_counts_match: Option<bool>,
    pub nats_state_present: bool,
    pub nats_member_count: Option<usize>,
    pub nats_member_paths_match: Option<bool>,
    pub cas_blob_count: Option<u64>,
    pub cas_blob_count_matches: Option<bool>,
    pub private_mode_state_present: bool,
    pub private_mode_state_matches_manifest: bool,
}

// ── Entry point ─────────────────────────────────────────────────────────────

impl AdminSnapshotCommand {
    pub fn execute(&self) -> Result<SnapshotResult> {
        let mode = SnapshotMode::parse(&self.mode)?;

        let state_dir = self
            .state_dir
            .clone()
            .unwrap_or_else(|| PathBuf::from("/var/lib/sinex"));

        let captures_postgres = self.components.iter().any(|c| c == &Component::Postgres);
        let database_url = if captures_postgres {
            Some(self.database_url.clone().ok_or_else(|| {
                eyre!("DATABASE_URL must be set (or pass --database-url) for Postgres capture")
            })?)
        } else {
            self.database_url.clone()
        };

        // 1. Generate a snapshot ID (UUIDv7 formatted as a string).
        let snapshot_id = gen_snapshot_id();
        let created_at = current_rfc3339();

        // 2. Verify/stop services.
        if !self.dry_run && mode.requires_quiescence() {
            let active = exec::active_sinex_services();
            if !active.is_empty() {
                if self.auto_stop {
                    eprintln!("Stopping {} active sinex service(s)…", active.len());
                    exec::stop_sinex_services()
                        .context("auto-stop sinex services before snapshot")?;
                } else {
                    bail!(
                        "sinex services are running ({}). \
                         Stop them before snapshotting, or pass --auto-stop.\n\
                         Active units:\n  {}",
                        active.len(),
                        active.join("\n  ")
                    );
                }
            }
        } else if !self.dry_run && self.auto_stop && !mode.requires_quiescence() {
            eprintln!("Ignoring --auto-stop for live snapshot mode; services remain active.");
        }

        // 3. Probe disk free.
        let output_parent = self
            .output
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .to_path_buf();

        // Ensure the output parent directory exists.
        if !output_parent.exists() {
            std::fs::create_dir_all(&output_parent)
                .with_context(|| format!("create output directory {}", output_parent.display()))?;
        }

        let estimated_state_bytes = estimate_dir_bytes(&state_dir);
        let free_bytes = free_bytes_at(&output_parent);
        let required = (estimated_state_bytes as f64 * 1.5) as u64;

        if !self.dry_run && free_bytes < required {
            bail!(
                "insufficient disk space at {}: {} free, {} required (1.5× estimated state {})",
                output_parent.display(),
                format_bytes(free_bytes),
                format_bytes(required),
                format_bytes(estimated_state_bytes)
            );
        }

        // 4. Create staging directory (RAII-cleaned).
        let mut staging = StagingDir::create(&output_parent, &snapshot_id)?;

        // Run the capture, ensuring staging is cleaned up on any failure.
        let result = self.run_capture(
            mode,
            &snapshot_id,
            &created_at,
            &state_dir,
            database_url.as_deref(),
            &mut staging,
        );

        match result {
            Ok(res) => Ok(res),
            Err(e) => {
                // staging drop() will clean up
                Err(e)
            }
        }
    }

    fn run_capture(
        &self,
        mode: SnapshotMode,
        snapshot_id: &str,
        created_at: &str,
        state_dir: &Path,
        database_url: Option<&str>,
        staging: &mut StagingDir,
    ) -> Result<SnapshotResult> {
        let mut component_records: Vec<ComponentRecord> = Vec::new();

        let component_set: BTreeSet<&str> = self.components.iter().map(Component::name).collect();

        // 5–8. Capture each component.
        if component_set.contains("postgres") {
            let database_url = database_url.ok_or_else(|| {
                eyre!("DATABASE_URL must be set (or pass --database-url) for Postgres capture")
            })?;
            let record = self.capture_postgres(database_url, staging, self.dry_run)?;
            component_records.push(record);
        }

        if component_set.contains("nats") {
            let nats_src = state_dir.join("nats/jetstream");
            let mut record = self.capture_dir_component(
                "nats",
                "nats/jetstream/",
                &nats_src,
                staging,
                self.dry_run,
                mode.tolerates_vanished_files(),
            )?;
            record.extras = Some(ComponentExtras::Nats(NatsExtras {
                member_paths: component_member_paths(&nats_src),
            }));
            component_records.push(record);

            // Best-effort NATS stream listing (fails gracefully if NATS is down).
            if !self.dry_run {
                let _ = try_nats_stream_summary(staging.path());
            }
        }

        if component_set.contains("cas") {
            let cas_src = state_dir.join("blob-repository");
            let blob_count = if cas_src.exists() {
                count_files_recursive(&cas_src)
            } else {
                0
            };
            let mut record = self.capture_dir_component(
                "cas",
                "cas/blob-repository/",
                &cas_src,
                staging,
                self.dry_run,
                mode.tolerates_vanished_files(),
            )?;
            record.extras = Some(ComponentExtras::Cas(CasExtras { blob_count }));
            component_records.push(record);
        }

        if component_set.contains("state") {
            let source_ids = registered_source_ids();
            let private_mode_state_present = state_dir.join("private-mode/state.json").exists();
            let mut record = self.capture_state_component(
                state_dir,
                staging,
                self.dry_run,
                mode.tolerates_vanished_files(),
            )?;
            record.extras = Some(ComponentExtras::State(StateExtras {
                source_ids,
                private_mode_state_present,
            }));
            component_records.push(record);
        }

        // 9. Write manifest.
        let uncompressed_bytes: u64 = component_records.iter().map(|r| r.bytes).sum();
        let source_ids = registered_source_ids();

        let manifest = SnapshotManifest {
            snapshot_id: snapshot_id.to_string(),
            created_at: created_at.to_string(),
            sinex_version: env!("CARGO_PKG_VERSION").to_string(),
            git_sha: git_sha(),
            host: hostname(),
            mode: mode.as_str().to_string(),
            source_ids: source_ids.clone(),
            components: component_records.clone(),
            totals: Totals {
                uncompressed_bytes,
                archive_bytes: None,
            },
        };

        if !self.dry_run {
            let manifest_path = staging.path().join("manifest.json");
            let json =
                serde_json::to_string_pretty(&manifest).context("serialise manifest to JSON")?;
            std::fs::write(&manifest_path, json)
                .with_context(|| format!("write manifest to {}", manifest_path.display()))?;
        }

        if self.dry_run {
            // Dry run: print estimates, skip archive creation.
            let summaries: Vec<ComponentSummary> = component_records
                .iter()
                .map(|r| ComponentSummary {
                    name: r.name.clone(),
                    bytes: r.bytes,
                    blake3: r.blake3.clone(),
                })
                .collect();
            return Ok(SnapshotResult {
                mode: "dry-run",
                snapshot_id: snapshot_id.to_string(),
                output_path: None,
                archive_bytes: None,
                uncompressed_bytes,
                source_ids,
                components_captured: summaries,
            });
        }

        // 10. Create the archive.
        exec::tar_create_zstd(staging.path(), &self.output, self.compression, self.workers)
            .with_context(|| format!("create snapshot archive at {}", self.output.display()))?;

        // 11. Verify integrity.
        exec::tar_verify(&self.output)
            .with_context(|| format!("verify snapshot archive at {}", self.output.display()))?;

        let archive_bytes = self.output.metadata().map_or(0, |m| m.len());

        // 12. Remove staging.
        staging.cleanup().context("remove staging directory")?;

        let summaries: Vec<ComponentSummary> = component_records
            .iter()
            .map(|r| ComponentSummary {
                name: r.name.clone(),
                bytes: r.bytes,
                blake3: r.blake3.clone(),
            })
            .collect();

        Ok(SnapshotResult {
            mode: mode.as_str(),
            snapshot_id: snapshot_id.to_string(),
            output_path: Some(self.output.display().to_string()),
            archive_bytes: Some(archive_bytes),
            uncompressed_bytes,
            source_ids,
            components_captured: summaries,
        })
    }

    fn capture_postgres(
        &self,
        database_url: &str,
        staging: &StagingDir,
        dry_run: bool,
    ) -> Result<ComponentRecord> {
        let pg_dir = staging.component_dir("postgres")?;
        let dump_path = pg_dir.join("sinex_prod.dump");

        let (bytes, blake3) = if dry_run {
            // In dry-run, query row counts but don't write a dump file.
            (0u64, "dry-run".to_string())
        } else {
            exec::pg_dump(database_url, &dump_path).context("capture postgres component")?;
            let bytes = dump_path.metadata().map_or(0, |m| m.len());
            let blake3 = blake3_file(&dump_path).unwrap_or_else(|_| "error".to_string());
            (bytes, blake3)
        };

        let row_counts = exec::pg_row_counts(database_url).unwrap_or_default();

        Ok(ComponentRecord {
            name: "postgres".to_string(),
            path: "postgres/sinex_prod.dump".to_string(),
            bytes,
            blake3,
            extras: Some(ComponentExtras::Postgres(PostgresExtras { row_counts })),
        })
    }

    fn capture_dir_component(
        &self,
        name: &str,
        relative_path: &str,
        src: &Path,
        staging: &StagingDir,
        dry_run: bool,
        tolerate_vanished_files: bool,
    ) -> Result<ComponentRecord> {
        let (bytes, blake3) = if !src.exists() {
            // Component directory absent — capture nothing, record zeros.
            (0u64, "absent".to_string())
        } else if dry_run {
            let bytes = estimate_dir_bytes(src);
            (bytes, "dry-run".to_string())
        } else {
            // e.g. staging/nats/jetstream/ holds the copied JetStream content,
            // while staging/nats/ remains the component hash root.
            let component_root = staging.path().join(name);
            let dst_dir = staging.path().join(relative_path.trim_end_matches('/'));
            std::fs::create_dir_all(&dst_dir)
                .with_context(|| format!("create {name} component dir in staging"))?;
            if tolerate_vanished_files {
                exec::cp_tree_live(src, &dst_dir).with_context(|| {
                    format!("live-copy {name} component from {}", src.display())
                })?;
            } else {
                exec::cp_tree(src, &dst_dir)
                    .with_context(|| format!("copy {name} component from {}", src.display()))?;
            }
            let bytes = estimate_dir_bytes(&component_root);
            let blake3 = blake3_dir(&component_root).unwrap_or_else(|_| "error".to_string());
            (bytes, blake3)
        };

        Ok(ComponentRecord {
            name: name.to_string(),
            path: relative_path.to_string(),
            bytes,
            blake3,
            extras: None,
        })
    }

    fn capture_state_component(
        &self,
        state_dir: &Path,
        staging: &StagingDir,
        dry_run: bool,
        tolerate_vanished_files: bool,
    ) -> Result<ComponentRecord> {
        // Capture everything under state_dir that is NOT already handled by
        // dedicated components (to avoid double-counting and live-copying
        // mutable database storage as ordinary runtime state).
        let skip = ["postgresql", "nats", "blob-repository"];

        if !state_dir.exists() {
            return Ok(ComponentRecord {
                name: "state".to_string(),
                path: "state/".to_string(),
                bytes: 0,
                blake3: "absent".to_string(),
                extras: None,
            });
        }

        let (bytes, blake3) = if dry_run {
            let bytes = estimate_dir_bytes_skip(state_dir, &skip);
            (bytes, "dry-run".to_string())
        } else {
            let dst_dir = staging.component_dir("state")?;
            // Copy all top-level entries except skip list.
            let rd = std::fs::read_dir(state_dir)
                .with_context(|| format!("read state dir {}", state_dir.display()))?;
            for entry in rd {
                let entry = entry.context("read state dir entry")?;
                let fname = entry.file_name();
                let fname_str = fname.to_string_lossy();
                if skip.iter().any(|s| *s == fname_str.as_ref()) {
                    continue;
                }
                let src_entry = entry.path();
                if src_entry.is_dir() {
                    let dst_sub = dst_dir.join(&fname);
                    std::fs::create_dir_all(&dst_sub)
                        .with_context(|| format!("create state sub-dir {}", dst_sub.display()))?;
                    if tolerate_vanished_files {
                        exec::cp_tree_live(&src_entry, &dst_sub).with_context(|| {
                            format!(
                                "live-copy state entry {} -> {}",
                                src_entry.display(),
                                dst_sub.display()
                            )
                        })?;
                    } else {
                        exec::cp_tree(&src_entry, &dst_sub).with_context(|| {
                            format!(
                                "copy state entry {} -> {}",
                                src_entry.display(),
                                dst_sub.display()
                            )
                        })?;
                    }
                } else {
                    let dst_file = dst_dir.join(&fname);
                    std::fs::copy(&src_entry, &dst_file).with_context(|| {
                        format!(
                            "copy state file {} -> {}",
                            src_entry.display(),
                            dst_file.display()
                        )
                    })?;
                }
            }
            let bytes = estimate_dir_bytes(&dst_dir);
            let blake3 = blake3_dir(&dst_dir).unwrap_or_else(|_| "error".to_string());
            (bytes, blake3)
        };

        Ok(ComponentRecord {
            name: "state".to_string(),
            path: "state/".to_string(),
            bytes,
            blake3,
            extras: None,
        })
    }
}

impl AdminSnapshotInspectCommand {
    pub fn execute(&self) -> Result<SnapshotInspectResult> {
        inspect_snapshot_archive(&self.archive)
    }
}

impl AdminSnapshotRestoreCommand {
    pub fn execute(&self) -> Result<SnapshotRestorePlanResult> {
        let inspect = inspect_snapshot_archive(&self.archive)?;
        if !inspect.missing_component_paths.is_empty() {
            bail!(
                "snapshot archive is incomplete; missing manifest paths: {}",
                inspect.missing_component_paths.join(", ")
            );
        }

        let archive_entries = exec::tar_list_zstd(&self.archive)
            .with_context(|| format!("list snapshot archive {}", self.archive.display()))?;
        validate_archive_entries_safe(&archive_entries)?;
        let target_state = classify_restore_target(&self.target_dir)?;
        if !target_state.empty && !self.allow_non_empty_target {
            bail!(
                "restore target {} is not empty; choose an empty drill target or pass \
                 --allow-non-empty-target for planning only",
                self.target_dir.display()
            );
        }

        let active_services = exec::active_sinex_services();
        let mut warnings = Vec::new();
        if !active_services.is_empty() {
            warnings.push(format!(
                "{} active sinex service(s) detected; destructive restore must quiesce services \
                 before writing target state",
                active_services.len()
            ));
        }
        if !target_state.exists {
            warnings.push(format!(
                "target directory {} does not exist and would be created by a restore drill",
                self.target_dir.display()
            ));
        }
        if !target_state.empty {
            warnings.push(format!(
                "target directory {} is non-empty; dry-run did not write to it",
                self.target_dir.display()
            ));
        }

        let planned_steps = inspect
            .manifest
            .components
            .iter()
            .map(|component| restore_step_for_component(component, &self.target_dir))
            .collect::<Vec<_>>();
        let drill_checks = restore_drill_checks(&inspect.manifest, &archive_entries);
        let observed_checks = if self.dry_run {
            None
        } else {
            Some(self.execute_restore_drill(&inspect, &archive_entries, &target_state)?)
        };

        Ok(SnapshotRestorePlanResult {
            archive_path: self.archive.display().to_string(),
            snapshot_id: inspect.snapshot_id,
            dry_run: self.dry_run,
            target_dir: self.target_dir.display().to_string(),
            target_empty: target_state.empty,
            active_services,
            archive_sensitivity: classify_archive_sensitivity(&inspect.manifest).to_string(),
            key_policy: SNAPSHOT_KEY_POLICY.to_string(),
            planned_steps,
            drill_checks,
            observed_checks,
            warnings,
        })
    }

    fn execute_restore_drill(
        &self,
        inspect: &SnapshotInspectResult,
        archive_entries: &[String],
        target_state: &RestoreTargetState,
    ) -> Result<RestoreObservedChecks> {
        if !self.confirm_restore {
            bail!(
                "snapshot restore drill execution requires --confirm-restore; rerun with \
                 --dry-run to inspect the plan without writing target state"
            );
        }
        if !self.allow_active_services {
            let active_services = exec::active_sinex_services();
            if !active_services.is_empty() {
                bail!(
                    "sinex services are active; stop them before restore drill execution or pass \
                     --allow-active-services for an explicitly isolated target"
                );
            }
        }
        if !target_state.empty {
            bail!(
                "restore drill execution requires an empty target directory; {} is not empty",
                self.target_dir.display()
            );
        }

        let unsupported = inspect
            .manifest
            .components
            .iter()
            .filter(|component| component.bytes > 0)
            .filter(|component| {
                !matches!(
                    component.name.as_str(),
                    "state" | "cas" | "nats" | "postgres"
                )
            })
            .map(|component| component.name.as_str())
            .collect::<Vec<_>>();
        if !unsupported.is_empty() {
            bail!(
                "restore drill supports state, cas, nats, and postgres components; archive also \
                 contains unsupported component(s) {}; use --dry-run for plan validation",
                unsupported.join(", ")
            );
        }

        std::fs::create_dir_all(&self.target_dir)
            .with_context(|| format!("create restore target {}", self.target_dir.display()))?;
        exec::tar_extract_zstd(&self.archive, &self.target_dir).with_context(|| {
            format!(
                "extract snapshot archive into {}",
                self.target_dir.display()
            )
        })?;

        let postgres_row_counts = self.execute_postgres_restore_drill(&inspect.manifest)?;

        Ok(observe_restored_target(
            &inspect.manifest,
            archive_entries,
            &self.target_dir,
            postgres_row_counts.as_ref(),
        ))
    }

    fn execute_postgres_restore_drill(
        &self,
        manifest: &SnapshotManifest,
    ) -> Result<Option<BTreeMap<String, i64>>> {
        let Some(expected_row_counts) = expected_postgres_row_counts(manifest) else {
            return Ok(None);
        };
        let Some(component) = manifest
            .components
            .iter()
            .find(|component| component.name == "postgres")
        else {
            return Ok(None);
        };
        if component.bytes == 0 {
            return Ok(Some(BTreeMap::new()));
        }

        let restore_database_url = self.restore_database_url.as_deref().ok_or_else(|| {
            eyre!(
                "postgres restore drill execution requires --restore-database-url pointing at \
                 an empty drill database"
            )
        })?;
        let dump_path = self.target_dir.join(&component.path);
        exec::psql_execute(
            restore_database_url,
            "CREATE EXTENSION IF NOT EXISTS timescaledb; SELECT timescaledb_pre_restore();",
            self.psql_bin.as_deref(),
        )
        .context("prepare TimescaleDB restore mode")?;
        let restore_result = exec::pg_restore(
            restore_database_url,
            &dump_path,
            self.pg_restore_bin.as_deref(),
        )
        .with_context(|| format!("restore postgres dump {}", dump_path.display()));
        let post_restore_result = exec::psql_execute(
            restore_database_url,
            "SELECT timescaledb_post_restore();",
            self.psql_bin.as_deref(),
        )
        .context("leave TimescaleDB restore mode");
        restore_result?;
        post_restore_result?;
        let observed = exec::pg_exact_row_counts(
            restore_database_url,
            expected_row_counts.keys().cloned(),
            self.psql_bin.as_deref(),
        )
        .context("query restored postgres row counts")?;
        Ok(Some(observed))
    }
}

// ── Helpers ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SnapshotMode {
    Quiesce,
    Live,
}

impl SnapshotMode {
    fn parse(raw: &str) -> Result<Self> {
        match raw {
            "quiesce" => Ok(Self::Quiesce),
            "live" => Ok(Self::Live),
            other => bail!("unsupported snapshot mode `{other}`; expected `quiesce` or `live`"),
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Quiesce => "quiesce",
            Self::Live => "live",
        }
    }

    const fn requires_quiescence(self) -> bool {
        matches!(self, Self::Quiesce)
    }

    const fn tolerates_vanished_files(self) -> bool {
        matches!(self, Self::Live)
    }
}

fn gen_snapshot_id() -> String {
    sinex_primitives::Uuid::now_v7().to_string()
}

fn current_rfc3339() -> String {
    // time crate is available in the workspace.
    let now = time::OffsetDateTime::now_utc();
    now.format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_string())
}

fn hostname() -> String {
    std::fs::read_to_string("/etc/hostname")
        .map_or_else(|_| "unknown".to_string(), |s| s.trim().to_string())
}

fn git_sha() -> Option<String> {
    // Try `git rev-parse --short HEAD` in the working directory.
    let output = std::process::Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .output()
        .ok()?;
    if output.status.success() {
        Some(String::from_utf8_lossy(&output.stdout).trim().to_string())
    } else {
        None
    }
}

fn registered_source_ids() -> Vec<String> {
    let mut ids: Vec<String> = source_contracts::all_source_contracts()
        .map(|descriptor| descriptor.id.to_string())
        .collect();
    ids.sort();
    ids.dedup();
    ids
}

fn inspect_snapshot_archive(archive_path: &Path) -> Result<SnapshotInspectResult> {
    let manifest = read_snapshot_manifest_from_archive(archive_path)?;
    let entries = exec::tar_list_zstd(archive_path)
        .with_context(|| format!("list snapshot archive {}", archive_path.display()))?;
    let missing_component_paths = manifest
        .components
        .iter()
        .filter(|component| component.bytes > 0)
        .filter(|component| !archive_path_contains(&entries, &component.path))
        .map(|component| component.path.clone())
        .collect();
    let components = manifest
        .components
        .iter()
        .map(|component| ComponentSummary {
            name: component.name.clone(),
            bytes: component.bytes,
            blake3: component.blake3.clone(),
        })
        .collect();
    let state_extras = manifest
        .components
        .iter()
        .find_map(|component| match &component.extras {
            Some(ComponentExtras::State(extras)) => Some(extras),
            _ => None,
        });

    Ok(SnapshotInspectResult {
        archive_path: archive_path.display().to_string(),
        snapshot_id: manifest.snapshot_id.clone(),
        created_at: manifest.created_at.clone(),
        mode: manifest.mode.clone(),
        sinex_version: manifest.sinex_version.clone(),
        git_sha: manifest.git_sha.clone(),
        host: manifest.host.clone(),
        archive_entries: entries.len(),
        source_count: manifest.source_ids.len(),
        source_ids: manifest.source_ids.clone(),
        state_source_count: state_extras.map(|extras| extras.source_ids.len()),
        state_private_mode_state_present: state_extras
            .map(|extras| extras.private_mode_state_present),
        component_count: manifest.components.len(),
        components,
        missing_component_paths,
        manifest,
    })
}

fn read_snapshot_manifest_from_archive(archive_path: &Path) -> Result<SnapshotManifest> {
    let mut last_error = None;
    for member in ["manifest.json", "./manifest.json"] {
        match exec::tar_read_file_zstd(archive_path, member) {
            Ok(bytes) => {
                return serde_json::from_slice(&bytes)
                    .with_context(|| format!("parse {member} from {}", archive_path.display()));
            }
            Err(error) => last_error = Some(error),
        }
    }

    Err(last_error
        .unwrap_or_else(|| eyre!("manifest.json not found in {}", archive_path.display()))
        .wrap_err(format!(
            "read manifest.json from {}",
            archive_path.display()
        )))
}

fn validate_archive_entries_safe(entries: &[String]) -> Result<()> {
    for entry in entries {
        let path = Path::new(entry);
        if path.is_absolute()
            || path
                .components()
                .any(|c| matches!(c, std::path::Component::ParentDir))
        {
            bail!("snapshot archive contains unsafe member path: {entry}");
        }
    }
    Ok(())
}

const SNAPSHOT_KEY_POLICY: &str = "archives exclude TLS/client/private keys by policy; if an \
operator explicitly stores keys under the selected state directory, the archive inherits that \
secret classification and must stay on encrypted storage";

struct RestoreTargetState {
    exists: bool,
    empty: bool,
}

fn classify_restore_target(target_dir: &Path) -> Result<RestoreTargetState> {
    if !target_dir.exists() {
        let parent = target_dir
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        if !parent.exists() {
            bail!(
                "restore target parent directory does not exist: {}",
                parent.display()
            );
        }
        return Ok(RestoreTargetState {
            exists: false,
            empty: true,
        });
    }

    if !target_dir.is_dir() {
        bail!(
            "restore target exists but is not a directory: {}",
            target_dir.display()
        );
    }

    let empty = std::fs::read_dir(target_dir)
        .with_context(|| format!("read restore target {}", target_dir.display()))?
        .next()
        .is_none();
    Ok(RestoreTargetState {
        exists: true,
        empty,
    })
}

fn restore_step_for_component(component: &ComponentRecord, target_dir: &Path) -> RestorePlanStep {
    let (action, target_path) = match component.name.as_str() {
        "postgres" => (
            "pg_restore into empty target database after schema owner preparation".to_string(),
            "postgres://<target>/sinex_prod".to_string(),
        ),
        "nats" => (
            "extract JetStream state into target NATS state root while services are stopped"
                .to_string(),
            target_dir.join("nats").display().to_string(),
        ),
        "cas" => (
            "extract CAS blob repository and verify blob count/checksum manifest".to_string(),
            target_dir.join("blob-repository").display().to_string(),
        ),
        "state" => (
            "extract runtime state files and compare private-mode/source surfaces".to_string(),
            target_dir.display().to_string(),
        ),
        other => (
            format!("inspect unrecognized component `{other}` before restore"),
            target_dir.join(other).display().to_string(),
        ),
    };

    RestorePlanStep {
        component: component.name.clone(),
        action,
        archive_path: component.path.clone(),
        target_path,
    }
}

fn restore_drill_checks(
    manifest: &SnapshotManifest,
    archive_entries: &[String],
) -> RestoreDrillChecks {
    let postgres_table_count = manifest
        .components
        .iter()
        .find_map(|component| match &component.extras {
            Some(ComponentExtras::Postgres(extras)) => Some(extras.row_counts.len()),
            _ => None,
        })
        .unwrap_or(0);
    let cas_blob_count = manifest
        .components
        .iter()
        .find_map(|component| match &component.extras {
            Some(ComponentExtras::Cas(extras)) => Some(extras.blob_count),
            _ => None,
        });
    let nats_member_count =
        manifest
            .components
            .iter()
            .find_map(|component| match &component.extras {
                Some(ComponentExtras::Nats(extras)) => Some(extras.member_paths.len()),
                _ => None,
            });
    let missing_component_paths = manifest
        .components
        .iter()
        .filter(|component| component.bytes > 0)
        .filter(|component| !archive_path_contains(archive_entries, &component.path))
        .map(|component| component.path.clone())
        .collect();

    RestoreDrillChecks {
        source_count: manifest.source_ids.len(),
        postgres_table_count,
        nats_member_count,
        cas_blob_count,
        private_mode_state_present: expected_private_mode_state_present(manifest).unwrap_or_else(
            || archive_path_contains(archive_entries, "state/private-mode/state.json"),
        ),
        missing_component_paths,
    }
}

fn observe_restored_target(
    manifest: &SnapshotManifest,
    archive_entries: &[String],
    target_dir: &Path,
    postgres_row_counts: Option<&BTreeMap<String, i64>>,
) -> RestoreObservedChecks {
    let source_ids = registered_source_ids();
    let expected_postgres_row_counts = expected_postgres_row_counts(manifest);
    let expected_cas_blob_count = manifest
        .components
        .iter()
        .find_map(|component| match &component.extras {
            Some(ComponentExtras::Cas(extras)) => Some(extras.blob_count),
            _ => None,
        });
    let expected_nats_member_paths =
        manifest
            .components
            .iter()
            .find_map(|component| match &component.extras {
                Some(ComponentExtras::Nats(extras)) => Some(&extras.member_paths),
                _ => None,
            });
    let component_blake3 = observed_component_blake3(manifest, target_dir);
    let component_blake3_matches = expected_component_blake3_matches(manifest, &component_blake3);
    let nats_state_root = target_dir.join("nats/jetstream");
    let nats_state_present = nats_state_root.exists();
    let nats_member_paths = nats_state_present.then(|| component_member_paths(&nats_state_root));
    let nats_member_count = nats_member_paths.as_ref().map(Vec::len);
    let cas_blob_count = target_dir
        .join("cas/blob-repository")
        .exists()
        .then(|| count_files_recursive(&target_dir.join("cas/blob-repository")));
    let private_mode_state_present = target_dir.join("state/private-mode/state.json").exists();
    let manifest_private_mode_state_present = expected_private_mode_state_present(manifest)
        .unwrap_or_else(|| archive_path_contains(archive_entries, "state/private-mode/state.json"));
    let source_ids_match = source_ids == manifest.source_ids;
    let postgres_row_counts_match = expected_postgres_row_counts
        .map(|expected| postgres_row_counts.is_some_and(|observed| observed == &expected));
    let nats_member_paths_match = expected_nats_member_paths.map(|expected| {
        nats_member_paths
            .as_ref()
            .is_some_and(|observed| observed == expected)
    });
    let cas_blob_count_matches = expected_cas_blob_count
        .map(|expected| cas_blob_count.map_or(expected == 0, |observed| observed == expected));
    let private_mode_state_matches_manifest =
        private_mode_state_present == manifest_private_mode_state_present;
    let failed_checks = restore_failed_checks(&RestoreFailedCheckInput {
        source_ids_match,
        component_blake3_matches: &component_blake3_matches,
        postgres_row_counts_match,
        nats_member_paths_match,
        cas_blob_count_matches,
        private_mode_state_matches_manifest,
    });

    RestoreObservedChecks {
        target_entry_count: count_files_recursive(target_dir) as usize,
        checks_passed: failed_checks.is_empty(),
        failed_checks,
        source_count: source_ids.len(),
        source_ids_match,
        component_blake3,
        component_blake3_matches,
        postgres_row_counts: postgres_row_counts.cloned().unwrap_or_default(),
        postgres_row_counts_match,
        nats_state_present,
        nats_member_count,
        nats_member_paths_match,
        cas_blob_count,
        cas_blob_count_matches,
        private_mode_state_present,
        private_mode_state_matches_manifest,
    }
}

struct RestoreFailedCheckInput<'a> {
    source_ids_match: bool,
    component_blake3_matches: &'a BTreeMap<String, bool>,
    postgres_row_counts_match: Option<bool>,
    nats_member_paths_match: Option<bool>,
    cas_blob_count_matches: Option<bool>,
    private_mode_state_matches_manifest: bool,
}

fn restore_failed_checks(input: &RestoreFailedCheckInput<'_>) -> Vec<String> {
    let mut failed = Vec::new();
    if !input.source_ids_match {
        failed.push("source_ids_match".to_string());
    }
    for (component, matched) in input.component_blake3_matches {
        if !matched {
            failed.push(format!("component_blake3_matches.{component}"));
        }
    }
    if input.postgres_row_counts_match == Some(false) {
        failed.push("postgres_row_counts_match".to_string());
    }
    if input.nats_member_paths_match == Some(false) {
        failed.push("nats_member_paths_match".to_string());
    }
    if input.cas_blob_count_matches == Some(false) {
        failed.push("cas_blob_count_matches".to_string());
    }
    if !input.private_mode_state_matches_manifest {
        failed.push("private_mode_state_matches_manifest".to_string());
    }
    failed
}

fn observed_component_blake3(
    manifest: &SnapshotManifest,
    target_dir: &Path,
) -> BTreeMap<String, String> {
    manifest
        .components
        .iter()
        .filter(|component| component.bytes > 0)
        .filter_map(|component| {
            let observed = match component.name.as_str() {
                "postgres" => blake3_file(&target_dir.join(&component.path)).ok(),
                "nats" => {
                    let component_root = target_dir.join(&component.name);
                    component_root
                        .exists()
                        .then(|| {
                            blake3_dir_excluding(&component_root, &["streams.summary.json"]).ok()
                        })
                        .flatten()
                }
                "state" | "cas" => {
                    let component_root = target_dir.join(&component.name);
                    component_root
                        .exists()
                        .then(|| blake3_dir(&component_root).ok())
                        .flatten()
                }
                other => {
                    let component_root = target_dir.join(other);
                    component_root
                        .exists()
                        .then(|| blake3_dir(&component_root).ok())
                        .flatten()
                }
            }?;
            Some((component.name.clone(), observed))
        })
        .collect()
}

fn expected_component_blake3_matches(
    manifest: &SnapshotManifest,
    observed: &BTreeMap<String, String>,
) -> BTreeMap<String, bool> {
    manifest
        .components
        .iter()
        .filter(|component| component.bytes > 0)
        .filter(|component| !matches!(component.blake3.as_str(), "absent" | "dry-run" | "error"))
        .map(|component| {
            (
                component.name.clone(),
                observed
                    .get(&component.name)
                    .is_some_and(|actual| actual == &component.blake3),
            )
        })
        .collect()
}

fn expected_postgres_row_counts(manifest: &SnapshotManifest) -> Option<BTreeMap<String, i64>> {
    manifest
        .components
        .iter()
        .find_map(|component| match &component.extras {
            Some(ComponentExtras::Postgres(extras)) => Some(
                extras
                    .row_counts
                    .iter()
                    .filter(|(table, _)| durable_postgres_row_count_key(table))
                    .map(|(table, count)| (table.clone(), *count))
                    .collect(),
            ),
            _ => None,
        })
}

fn durable_postgres_row_count_key(table: &str) -> bool {
    let Some((schema, _relation)) = table.split_once('.') else {
        return false;
    };
    !schema.starts_with("pg_temp_") && !schema.starts_with("pg_toast_temp_")
}

fn component_member_paths(root: &Path) -> Vec<String> {
    if !root.exists() {
        return Vec::new();
    }

    let mut members: Vec<_> = collect_files_sorted(root, root)
        .into_iter()
        .map(|(relative_path, _)| relative_path)
        .collect();
    members.sort();
    members
}

fn expected_private_mode_state_present(manifest: &SnapshotManifest) -> Option<bool> {
    manifest
        .components
        .iter()
        .find_map(|component| match &component.extras {
            Some(ComponentExtras::State(extras)) => Some(extras.private_mode_state_present),
            _ => None,
        })
}

fn classify_archive_sensitivity(manifest: &SnapshotManifest) -> &'static str {
    let has_postgres = manifest
        .components
        .iter()
        .any(|component| component.name == "postgres" && component.bytes > 0);
    let has_cas = manifest
        .components
        .iter()
        .any(|component| component.name == "cas" && component.bytes > 0);
    let has_state = manifest
        .components
        .iter()
        .any(|component| component.name == "state" && component.bytes > 0);

    if has_postgres || has_cas || has_state {
        "secret: contains event payloads, raw material, runtime state, or operator privacy state"
    } else {
        "restricted: manifest-only or empty component archive"
    }
}

fn archive_path_contains(entries: &[String], wanted: &str) -> bool {
    let wanted = normalize_archive_path(wanted);
    entries.iter().any(|entry| {
        let entry = normalize_archive_path(entry);
        entry == wanted || entry.starts_with(&wanted)
    })
}

fn normalize_archive_path(path: &str) -> String {
    path.trim_start_matches("./")
        .trim_end_matches('/')
        .to_string()
        + "/"
}

/// Estimate total bytes under a directory tree (best-effort, ignores errors).
fn estimate_dir_bytes(dir: &Path) -> u64 {
    walk_files(dir, false)
        .map(|entry| entry.metadata().map_or(0, |metadata| metadata.len()))
        .sum()
}

fn estimate_dir_bytes_skip(dir: &Path, skip: &[&str]) -> u64 {
    WalkDir::new(dir)
        .follow_links(true)
        .into_iter()
        .filter_entry(|entry| !is_skipped_top_level(entry, skip))
        .filter_map(std::result::Result::ok)
        .filter(|entry| entry.file_type().is_file())
        .map(|entry| entry.metadata().map_or(0, |metadata| metadata.len()))
        .sum()
}

/// Count files recursively under a directory (for CAS blob count).
fn count_files_recursive(dir: &Path) -> u64 {
    walk_files(dir, true).count() as u64
}

fn walk_files(dir: &Path, follow_links: bool) -> impl Iterator<Item = DirEntry> {
    WalkDir::new(dir)
        .follow_links(follow_links)
        .into_iter()
        .filter_map(std::result::Result::ok)
        .filter(|entry| entry.file_type().is_file())
}

fn is_skipped_top_level(entry: &DirEntry, skip: &[&str]) -> bool {
    entry.depth() == 1
        && entry
            .file_name()
            .to_str()
            .is_some_and(|name| skip.contains(&name))
}

/// Get available disk space at a path (Linux-only via `statvfs`).
fn free_bytes_at(_path: &Path) -> u64 {
    // Use a generous fallback (1 TiB) when we can't determine free space.
    // The check is best-effort safety, not a hard gate in test environments.
    #[cfg(target_os = "linux")]
    {
        use std::ffi::CString;
        let Ok(path_cstr) = CString::new(_path.to_string_lossy().as_bytes()) else {
            return u64::MAX;
        };
        let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
        let rc = unsafe { libc::statvfs(path_cstr.as_ptr(), &raw mut stat) };
        if rc == 0 {
            (stat.f_bavail as u64) * (stat.f_bsize as u64)
        } else {
            u64::MAX
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        u64::MAX
    }
}

/// Compute a BLAKE3 digest of a single file (hex string).
fn blake3_file(path: &Path) -> Result<String> {
    let data =
        std::fs::read(path).with_context(|| format!("read file for BLAKE3: {}", path.display()))?;
    let hash = blake3::hash(&data);
    Ok(hash.to_hex().to_string())
}

/// Compute a deterministic BLAKE3 summary over a directory tree.
///
/// Strategy: sort all regular file paths lexicographically, hash each file's
/// contents, then hash the concatenation of (`relative_path` + `file_hash`) pairs.
/// This gives a stable content-addressed fingerprint of the tree.
fn blake3_dir(dir: &Path) -> Result<String> {
    blake3_dir_excluding(dir, &[])
}

fn blake3_dir_excluding(dir: &Path, excluded_relative_paths: &[&str]) -> Result<String> {
    let mut entries = collect_files_sorted(dir, dir);
    entries.retain(|(rel_path, _)| !excluded_relative_paths.contains(&rel_path.as_str()));
    entries.sort_by(|(a, _), (b, _)| a.cmp(b));

    let mut hasher = blake3::Hasher::new();
    for (rel_path, abs_path) in &entries {
        let file_data = std::fs::read(abs_path)
            .with_context(|| format!("read {} for BLAKE3", abs_path.display()))?;
        let file_hash = blake3::hash(&file_data);
        hasher.update(rel_path.as_bytes());
        hasher.update(file_hash.as_bytes());
    }
    Ok(hasher.finalize().to_hex().to_string())
}

fn collect_files_sorted(base: &Path, dir: &Path) -> Vec<(String, PathBuf)> {
    let mut out = Vec::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let p = entry.path();
            if p.is_symlink() {
                continue;
            }
            if p.is_file() {
                let rel = p
                    .strip_prefix(base)
                    .map(|r| r.to_string_lossy().to_string())
                    .unwrap_or_default();
                out.push((rel, p));
            } else if p.is_dir() {
                out.extend(collect_files_sorted(base, &p));
            }
        }
    }
    out
}

/// Attempt to write a NATS stream summary — ignores failure if NATS is down.
fn try_nats_stream_summary(staging_path: &Path) -> Option<()> {
    use std::process::Stdio;
    let output = std::process::Command::new("nats")
        .args(["stream", "ls", "--json"])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .output()
        .ok()?;

    if !output.status.success() {
        return None;
    }

    let summary_path = staging_path.join("nats").join("streams.summary.json");
    if let Some(parent) = summary_path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    std::fs::write(&summary_path, &output.stdout).ok()?;
    Some(())
}

/// Format bytes into a human-readable string (KiB / MiB / GiB).
fn format_bytes(bytes: u64) -> String {
    if bytes < 1024 {
        format!("{bytes} B")
    } else if bytes < 1024 * 1024 {
        format!("{:.1} KiB", bytes as f64 / 1024.0)
    } else if bytes < 1024 * 1024 * 1024 {
        format!("{:.1} MiB", bytes as f64 / (1024.0 * 1024.0))
    } else {
        format!("{:.2} GiB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    }
}

// ── Display ────────────────────────────────────────────────────────────────

/// Render the snapshot result as a human-readable table string.
#[must_use]
pub fn format_snapshot_result(result: &SnapshotResult) -> String {
    let mut out = String::new();
    out.push_str("Sinex Snapshot\n");
    out.push_str(&format!("  ID:   {}\n", result.snapshot_id));
    out.push_str(&format!("  Mode: {}\n", result.mode));
    if let Some(path) = &result.output_path {
        out.push_str(&format!("  Output: {path}\n"));
    }
    out.push_str(&format!(
        "  Uncompressed: {}\n",
        format_bytes(result.uncompressed_bytes)
    ));
    out.push_str(&format!(
        "  Source contracts: {}\n",
        result.source_ids.len()
    ));
    if let Some(archive_bytes) = result.archive_bytes {
        out.push_str(&format!("  Archive: {}\n", format_bytes(archive_bytes)));
        if result.uncompressed_bytes > 0 {
            let ratio = result.uncompressed_bytes as f64 / archive_bytes as f64;
            out.push_str(&format!("  Ratio: {ratio:.2}×\n"));
        }
    }
    out.push_str("\n  Components:\n");
    for c in &result.components_captured {
        out.push_str(&format!(
            "    {:8}  {:>12}  {}\n",
            c.name,
            format_bytes(c.bytes),
            &c.blake3[..c.blake3.len().min(16)]
        ));
    }
    out
}

/// Render snapshot inspection as a human-readable table string.
#[must_use]
pub fn format_snapshot_inspect_result(result: &SnapshotInspectResult) -> String {
    let mut out = String::new();
    out.push_str("Sinex Snapshot Inspect\n");
    out.push_str(&format!("  Archive: {}\n", result.archive_path));
    out.push_str(&format!("  ID:      {}\n", result.snapshot_id));
    out.push_str(&format!("  Created: {}\n", result.created_at));
    out.push_str(&format!("  Mode:    {}\n", result.mode));
    out.push_str(&format!("  Host:    {}\n", result.host));
    out.push_str(&format!("  Entries: {}\n", result.archive_entries));
    out.push_str(&format!("  Source contracts: {}\n", result.source_count));
    if let Some(state_source_count) = result.state_source_count {
        out.push_str(&format!("  State source contracts: {state_source_count}\n"));
    }
    if let Some(private_mode_state_present) = result.state_private_mode_state_present {
        out.push_str(&format!(
            "  Private-mode state: {}\n",
            if private_mode_state_present {
                "present"
            } else {
                "absent"
            }
        ));
    }
    out.push_str("\n  Components:\n");
    for component in &result.components {
        out.push_str(&format!(
            "    {:8}  {:>12}  {}\n",
            component.name,
            format_bytes(component.bytes),
            &component.blake3[..component.blake3.len().min(16)]
        ));
    }
    if result.missing_component_paths.is_empty() {
        out.push_str("\n  Manifest paths: ok\n");
    } else {
        out.push_str("\n  Missing manifest paths:\n");
        for path in &result.missing_component_paths {
            out.push_str(&format!("    {path}\n"));
        }
    }
    out
}

/// Render a snapshot restore plan or isolated drill result as a human-readable table string.
#[must_use]
pub fn format_snapshot_restore_plan_result(result: &SnapshotRestorePlanResult) -> String {
    let mut out = String::new();
    out.push_str("Sinex Snapshot Restore Plan\n");
    out.push_str(&format!("  Archive: {}\n", result.archive_path));
    out.push_str(&format!("  ID:      {}\n", result.snapshot_id));
    out.push_str(&format!("  Target:  {}\n", result.target_dir));
    out.push_str(&format!(
        "  Mode:    {}\n",
        if result.dry_run {
            "dry-run"
        } else {
            "isolated-drill"
        }
    ));
    out.push_str(&format!(
        "  Target empty: {}\n",
        if result.target_empty { "yes" } else { "no" }
    ));
    out.push_str(&format!("  Sensitivity: {}\n", result.archive_sensitivity));
    out.push_str(&format!("  Key policy: {}\n", result.key_policy));
    out.push_str(&format!(
        "  Active services: {}\n",
        result.active_services.len()
    ));

    out.push_str("\n  Planned steps:\n");
    for step in &result.planned_steps {
        out.push_str(&format!(
            "    {:8}  {} -> {}\n",
            step.component, step.archive_path, step.target_path
        ));
        out.push_str(&format!("              {}\n", step.action));
    }

    out.push_str("\n  Drill checks:\n");
    out.push_str(&format!(
        "    source contracts: {}\n",
        result.drill_checks.source_count
    ));
    out.push_str(&format!(
        "    postgres tables: {}\n",
        result.drill_checks.postgres_table_count
    ));
    if let Some(member_count) = result.drill_checks.nats_member_count {
        out.push_str(&format!("    NATS members: {member_count}\n"));
    }
    if let Some(blob_count) = result.drill_checks.cas_blob_count {
        out.push_str(&format!("    CAS blobs: {blob_count}\n"));
    }
    out.push_str(&format!(
        "    private mode state: {}\n",
        if result.drill_checks.private_mode_state_present {
            "present"
        } else {
            "absent"
        }
    ));

    if let Some(observed) = &result.observed_checks {
        out.push_str("\n  Observed after isolated drill:\n");
        out.push_str(&format!(
            "    checks: {}\n",
            if observed.checks_passed {
                "passed"
            } else {
                "failed"
            }
        ));
        if !observed.failed_checks.is_empty() {
            out.push_str(&format!(
                "    failed checks: {}\n",
                observed.failed_checks.join(", ")
            ));
        }
        out.push_str(&format!(
            "    target entries: {}\n",
            observed.target_entry_count
        ));
        out.push_str(&format!(
            "    source contracts: {} ({})\n",
            observed.source_count,
            if observed.source_ids_match {
                "match"
            } else {
                "mismatch"
            }
        ));
        if let Some(blob_count) = observed.cas_blob_count {
            out.push_str(&format!("    CAS blobs: {blob_count}\n"));
        }
        if let Some(member_count) = observed.nats_member_count {
            let match_text = observed
                .nats_member_paths_match
                .map_or(
                    "not declared",
                    |matches| if matches { "match" } else { "mismatch" },
                );
            out.push_str(&format!(
                "    NATS members: {member_count} ({match_text})\n"
            ));
        } else if observed.nats_state_present {
            out.push_str("    NATS state: present\n");
        }
        if !observed.component_blake3_matches.is_empty() {
            let matched = observed
                .component_blake3_matches
                .values()
                .filter(|matches| **matches)
                .count();
            out.push_str(&format!(
                "    component hashes: {matched}/{} match\n",
                observed.component_blake3_matches.len()
            ));
        }
        if let Some(matches) = observed.postgres_row_counts_match {
            out.push_str(&format!(
                "    postgres rows: {} ({})\n",
                observed.postgres_row_counts.len(),
                if matches { "match" } else { "mismatch" }
            ));
        }
        out.push_str(&format!(
            "    private mode state: {} ({})\n",
            if observed.private_mode_state_present {
                "present"
            } else {
                "absent"
            },
            if observed.private_mode_state_matches_manifest {
                "matches manifest"
            } else {
                "manifest mismatch"
            }
        ));
    }

    if !result.warnings.is_empty() {
        out.push_str("\n  Warnings:\n");
        for warning in &result.warnings {
            out.push_str(&format!("    {warning}\n"));
        }
    }

    out
}
