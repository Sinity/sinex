//! `sinexctl admin snapshot` — quiesce-mode backup of the complete sinex
//! runtime state surface.
//!
//! Captures Postgres (via `pg_dump`), NATS `JetStream` state, the CAS blob
//! repository, and remaining per-source-worker state files into a single
//! zstd-compressed tar archive.

use clap::Parser;
use color_eyre::eyre::{Context, Result, bail, eyre};
use serde::Serialize;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use crate::admin::exec;
use crate::admin::manifest::{
    CasExtras, ComponentExtras, ComponentRecord, PostgresExtras, SnapshotManifest, Totals,
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

/// Create a quiesce-mode snapshot of the complete sinex runtime state.
#[derive(Debug, Parser)]
#[command(after_help = "\
EXAMPLES:
    # Snapshot to /var/backup/sinex with defaults (zstd level 3, all components)
    sinexctl admin snapshot --output /var/backup/sinex/2026-05-15.sinex.tar.zst

    # Higher compression for archival
    sinexctl admin snapshot --output /var/backup/sinex/latest.sinex.tar.zst --compression 15

    # Estimate sizes without writing anything
    sinexctl admin snapshot --output /var/backup/sinex/latest.sinex.tar.zst --dry-run

    # Automatically stop services and snapshot postgres + CAS only
    sinexctl admin snapshot --output /var/backup/sinex/pg-cas.tar.zst \\
        --components postgres,cas --auto-stop

RESTORE:
    Restore is manual:
        tar -xf <archive> --use-compress-program=zstd -C /tmp/restore/
        pg_restore -d sinex_prod /tmp/restore/postgres/sinex_prod.dump
    See docs/operations/snapshot.md for the full restore runbook.
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

    /// Snapshot mode. Only `quiesce` is supported in this MVP.
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
    sinexctl admin snapshot-inspect --archive /var/backup/sinex/latest.sinex.tar.zst

NOTES:
    This reads manifest.json from the archive and checks that non-empty
    component paths named by the manifest are present in the tar member list.
")]
pub struct AdminSnapshotInspectCommand {
    /// Snapshot archive to inspect.
    #[arg(long)]
    pub archive: PathBuf,
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
    pub source_unit_ids: Vec<String>,
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
    pub source_unit_count: usize,
    pub source_unit_ids: Vec<String>,
    pub component_count: usize,
    pub components: Vec<ComponentSummary>,
    pub missing_component_paths: Vec<String>,
    pub manifest: SnapshotManifest,
}

// ── Entry point ─────────────────────────────────────────────────────────────

impl AdminSnapshotCommand {
    pub fn execute(&self) -> Result<SnapshotResult> {
        if self.mode != "quiesce" {
            bail!(
                "only mode=quiesce is supported in this MVP; got `{}`",
                self.mode
            );
        }

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
        if !self.dry_run {
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
            let record = self.capture_dir_component(
                "nats",
                "nats/jetstream/",
                &nats_src,
                staging,
                self.dry_run,
            )?;
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
            )?;
            record.extras = Some(ComponentExtras::Cas(CasExtras { blob_count }));
            component_records.push(record);
        }

        if component_set.contains("state") {
            let record = self.capture_state_component(state_dir, staging, self.dry_run)?;
            component_records.push(record);
        }

        // 9. Write manifest.
        let uncompressed_bytes: u64 = component_records.iter().map(|r| r.bytes).sum();
        let source_unit_ids = discover_source_unit_ids(state_dir);

        let manifest = SnapshotManifest {
            snapshot_id: snapshot_id.to_string(),
            created_at: created_at.to_string(),
            sinex_version: env!("CARGO_PKG_VERSION").to_string(),
            git_sha: git_sha(),
            host: hostname(),
            mode: "quiesce".to_string(),
            source_unit_ids: source_unit_ids.clone(),
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
                source_unit_ids,
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
            mode: "quiesce",
            snapshot_id: snapshot_id.to_string(),
            output_path: Some(self.output.display().to_string()),
            archive_bytes: Some(archive_bytes),
            uncompressed_bytes,
            source_unit_ids,
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
    ) -> Result<ComponentRecord> {
        let (bytes, blake3) = if !src.exists() {
            // Component directory absent — capture nothing, record zeros.
            (0u64, "absent".to_string())
        } else if dry_run {
            let bytes = estimate_dir_bytes(src);
            (bytes, "dry-run".to_string())
        } else {
            // e.g. staging/nats/jetstream/  ← will hold the content
            let dst_dir = staging.path().join(name);
            std::fs::create_dir_all(&dst_dir)
                .with_context(|| format!("create {name} component dir in staging"))?;
            exec::cp_tree(src, &dst_dir)
                .with_context(|| format!("copy {name} component from {}", src.display()))?;
            let bytes = estimate_dir_bytes(&dst_dir);
            let blake3 = blake3_dir(&dst_dir).unwrap_or_else(|_| "error".to_string());
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
    ) -> Result<ComponentRecord> {
        // Capture everything under state_dir that is NOT already handled by
        // nats/cas components (to avoid double-counting).
        let skip = ["nats", "blob-repository"];

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
                    exec::cp_tree(&src_entry, &dst_sub).with_context(|| {
                        format!(
                            "copy state entry {} -> {}",
                            src_entry.display(),
                            dst_sub.display()
                        )
                    })?;
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

// ── Helpers ────────────────────────────────────────────────────────────────

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

fn discover_source_unit_ids(state_dir: &Path) -> Vec<String> {
    let candidates = [
        state_dir.join("source-units.json"),
        PathBuf::from("docs/source-units.json"),
    ];

    for candidate in candidates {
        if let Ok(data) = std::fs::read_to_string(&candidate)
            && let Some(ids) = parse_source_unit_ids(&data)
            && !ids.is_empty()
        {
            return ids;
        }
    }

    Vec::new()
}

fn parse_source_unit_ids(data: &str) -> Option<Vec<String>> {
    let value: serde_json::Value = serde_json::from_str(data).ok()?;
    let mut ids: Vec<String> = value
        .get("source_units")?
        .as_array()?
        .iter()
        .filter_map(|unit| unit.get("id")?.as_str())
        .map(str::to_string)
        .collect();
    ids.sort();
    ids.dedup();
    Some(ids)
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

    Ok(SnapshotInspectResult {
        archive_path: archive_path.display().to_string(),
        snapshot_id: manifest.snapshot_id.clone(),
        created_at: manifest.created_at.clone(),
        mode: manifest.mode.clone(),
        sinex_version: manifest.sinex_version.clone(),
        git_sha: manifest.git_sha.clone(),
        host: manifest.host.clone(),
        archive_entries: entries.len(),
        source_unit_count: manifest.source_unit_ids.len(),
        source_unit_ids: manifest.source_unit_ids.clone(),
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
    let mut total = 0u64;
    if let Ok(mut entries) = std::fs::read_dir(dir) {
        while let Some(Ok(entry)) = entries.next() {
            let p = entry.path();
            if p.is_symlink() {
                continue;
            }
            if p.is_file() {
                total += p.metadata().map_or(0, |m| m.len());
            } else if p.is_dir() {
                total += estimate_dir_bytes(&p);
            }
        }
    }
    total
}

fn estimate_dir_bytes_skip(dir: &Path, skip: &[&str]) -> u64 {
    let mut total = 0u64;
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let fname = entry.file_name();
            let fname_str = fname.to_string_lossy();
            if skip.iter().any(|s| *s == fname_str.as_ref()) {
                continue;
            }
            let p = entry.path();
            if p.is_file() {
                total += p.metadata().map_or(0, |m| m.len());
            } else if p.is_dir() {
                total += estimate_dir_bytes(&p);
            }
        }
    }
    total
}

/// Count files recursively under a directory (for CAS blob count).
fn count_files_recursive(dir: &Path) -> u64 {
    let mut count = 0u64;
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let p = entry.path();
            if p.is_file() {
                count += 1;
            } else if p.is_dir() {
                count += count_files_recursive(&p);
            }
        }
    }
    count
}

/// Get available disk space at a path (Linux-only via `statvfs`).
fn free_bytes_at(_path: &Path) -> u64 {
    // Use a generous fallback (1 TiB) when we can't determine free space.
    // The check is best-effort safety, not a hard gate in test environments.
    #[cfg(target_os = "linux")]
    {
        use std::ffi::CString;
        let path_cstr = match CString::new(_path.to_string_lossy().as_bytes()) {
            Ok(s) => s,
            Err(_) => return u64::MAX,
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
    let mut entries = collect_files_sorted(dir, dir);
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
        "  Source units: {}\n",
        result.source_unit_ids.len()
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
    out.push_str(&format!("  Source units: {}\n", result.source_unit_count));
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
