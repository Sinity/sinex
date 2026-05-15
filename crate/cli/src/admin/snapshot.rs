//! `sinexctl admin snapshot` — quiesce-mode backup of the complete sinex
//! runtime state surface.
//!
//! Captures Postgres (via `pg_dump`), NATS JetStream state, the CAS blob
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
    fn name(&self) -> &'static str {
        match self {
            Self::Postgres => "postgres",
            Self::Nats => "nats",
            Self::Cas => "cas",
            Self::State => "state",
        }
    }

    fn all() -> Vec<Self> {
        vec![Self::Postgres, Self::Nats, Self::Cas, Self::State]
    }

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "postgres" => Ok(Self::Postgres),
            "nats" => Ok(Self::Nats),
            "cas" => Ok(Self::Cas),
            "state" => Ok(Self::State),
            other => bail!(
                "unknown component `{other}`; valid components: postgres,nats,cas,state"
            ),
        }
    }
}

fn parse_components(s: &str) -> Result<Vec<Component>> {
    let mut out = Vec::new();
    let mut seen = BTreeSet::new();
    for part in s.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let c = Component::from_str(part)?;
        if seen.insert(part.to_string()) {
            out.push(c);
        }
    }
    if out.is_empty() {
        bail!("--components must contain at least one component");
    }
    Ok(out)
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

    /// PostgreSQL connection URL (defaults to DATABASE_URL env var).
    #[arg(long, env = "DATABASE_URL")]
    pub database_url: Option<String>,

    /// Sinex state directory root (defaults to SINEX_STATE_DIR, then /var/lib/sinex).
    #[arg(long, env = "SINEX_STATE_DIR")]
    pub state_dir: Option<PathBuf>,

    /// Stop sinex services automatically if they are running.
    #[arg(long)]
    pub auto_stop: bool,

    /// Comma-separated component list to capture (default: postgres,nats,cas,state).
    #[arg(
        long,
        default_value = "postgres,nats,cas,state",
        value_parser = parse_components_str
    )]
    pub components: Vec<Component>,
}

fn parse_components_str(s: &str) -> std::result::Result<Vec<Component>, String> {
    parse_components(s).map_err(|e| e.to_string())
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
    pub components_captured: Vec<ComponentSummary>,
}

#[derive(Debug, Serialize)]
pub struct ComponentSummary {
    pub name: String,
    pub bytes: u64,
    pub blake3: String,
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

        let database_url = self.database_url.clone().ok_or_else(|| {
            eyre!(
                "DATABASE_URL must be set (or pass --database-url) for Postgres capture"
            )
        })?;

        // 1. Generate a snapshot ID (UUIDv7 formatted as a hex string).
        let snapshot_id = gen_snapshot_id();
        let created_at = current_rfc3339();

        // 2. Verify/stop services.
        if !self.dry_run {
            let active = exec::active_sinex_services();
            if !active.is_empty() {
                if self.auto_stop {
                    eprintln!(
                        "Stopping {} active sinex service(s)…",
                        active.len()
                    );
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
            &database_url,
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
        database_url: &str,
        staging: &mut StagingDir,
    ) -> Result<SnapshotResult> {
        let mut component_records: Vec<ComponentRecord> = Vec::new();

        let component_set: BTreeSet<&str> =
            self.components.iter().map(|c| c.name()).collect();

        // 5–8. Capture each component.
        if component_set.contains("postgres") {
            let record =
                self.capture_postgres(database_url, staging, self.dry_run)?;
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

        let manifest = SnapshotManifest {
            snapshot_id: snapshot_id.to_string(),
            created_at: created_at.to_string(),
            sinex_version: env!("CARGO_PKG_VERSION").to_string(),
            git_sha: git_sha(),
            host: hostname(),
            mode: "quiesce".to_string(),
            components: component_records.clone(),
            totals: Totals {
                uncompressed_bytes,
                archive_bytes: None,
            },
        };

        if !self.dry_run {
            let manifest_path = staging.path().join("manifest.json");
            let json = serde_json::to_string_pretty(&manifest)
                .context("serialise manifest to JSON")?;
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
                components_captured: summaries,
            });
        }

        // 10. Create the archive.
        exec::tar_create_zstd(
            staging.path(),
            &self.output,
            self.compression,
            self.workers,
        )
        .with_context(|| {
            format!("create snapshot archive at {}", self.output.display())
        })?;

        // 11. Verify integrity.
        exec::tar_verify(&self.output).with_context(|| {
            format!("verify snapshot archive at {}", self.output.display())
        })?;

        let archive_bytes = self
            .output
            .metadata()
            .map(|m| m.len())
            .unwrap_or(0);

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
            exec::pg_dump(database_url, &dump_path)
                .context("capture postgres component")?;
            let bytes = dump_path.metadata().map(|m| m.len()).unwrap_or(0);
            let blake3 = blake3_file(&dump_path).unwrap_or_else(|_| "error".to_string());
            (bytes, blake3)
        };

        let row_counts = exec::pg_row_counts(database_url)
            .unwrap_or_default();

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
                    std::fs::create_dir_all(&dst_sub).with_context(|| {
                        format!("create state sub-dir {}", dst_sub.display())
                    })?;
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

// ── Helpers ────────────────────────────────────────────────────────────────

fn gen_snapshot_id() -> String {
    // Use UUIDv4 here (sufficient for uniqueness; UUIDv7 would require extra dep).
    // The ID is stable within a snapshot and used for the staging dir name.
    use std::time::{SystemTime, UNIX_EPOCH};
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    // Simple but collision-resistant: timestamp-ms + 8 random hex bytes.
    let r: u64 = rand::random();
    format!("{ts:013x}-{r:016x}")
}

fn current_rfc3339() -> String {
    // time crate is available in the workspace.
    let now = time::OffsetDateTime::now_utc();
    now.format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_string())
}

fn hostname() -> String {
    std::fs::read_to_string("/etc/hostname")
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|_| "unknown".to_string())
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
                total += p.metadata().map(|m| m.len()).unwrap_or(0);
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
                total += p.metadata().map(|m| m.len()).unwrap_or(0);
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
        let rc = unsafe { libc::statvfs(path_cstr.as_ptr(), &mut stat) };
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
    let data = std::fs::read(path)
        .with_context(|| format!("read file for BLAKE3: {}", path.display()))?;
    let hash = blake3::hash(&data);
    Ok(hash.to_hex().to_string())
}

/// Compute a deterministic BLAKE3 summary over a directory tree.
///
/// Strategy: sort all regular file paths lexicographically, hash each file's
/// contents, then hash the concatenation of (relative_path + file_hash) pairs.
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
