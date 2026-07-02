//! Bounded /cache enforcement: reclaim stale target-dir artifacts.
//!
//! This module addresses the monotonic-growth problem in `$CARGO_TARGET_DIR`:
//! cargo never GCs `target/debug/incremental/<crate>-<hash>/` directories nor
//! stale `target/debug/deps/<crate>-<hash>.rlib` files, so a 350K-LOC
//! workspace accumulates dozens of hash variants per crate over weeks.
//!
//! Reclaim strategy (in priority order):
//! 1. **cargo-sweep** (if available in PATH): removes unreferenced dep
//!    artifacts. Cargo-aware, safe.
//! 2. **incremental/ keep-N-newest-per-crate** (always): for each `<crate>-`
//!    prefix in `incremental/`, keep the N most-recently-modified hash dirs,
//!    delete the rest. Mirrors what cargo-sweep does for incremental while
//!    avoiding the cold-rebuild churn caused by pruning the active set too
//!    aggressively.
//! 3. **Disk-usage report** before + after.
//!
//! See issue #1213 for the broader cancel-reason substrate, and issue (TBD)
//! for the bounded enforcement.

use color_eyre::eyre::{Context, Result, eyre};
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::SystemTime;

/// Default number of hash-variant dirs to keep per crate in `incremental/`.
///
/// Cargo writes a new dir on each fingerprint change (feature set, rustc
/// version, source modification). Keeping the 3 most recent gives warm-cache
/// hits for the active config + one recent config + one older config. A keep-1
/// default was tested and rejected because it reclaimed space by forcing the
/// next edit-loop run to rebuild the active incremental state.
const DEFAULT_KEEP_PER_CRATE: usize = 3;

/// Threshold above which doctor warns the user.
pub const WARN_PERCENT: f64 = 70.0;

/// Threshold above which preflight auto-reclaims before proceeding.
pub const AUTO_RECLAIM_PERCENT: f64 = 85.0;

/// Threshold above which any heavy command refuses to run.
pub const REFUSE_PERCENT: f64 = 90.0;

/// Absolute free-space floor for refusing work.
///
/// Percentage-only refusal is too blunt on large filesystems: a multi-terabyte
/// mount can be above `REFUSE_PERCENT` while still having hundreds of GiB free,
/// which is enough headroom for Sinex build and test artifacts.
pub const REFUSE_MIN_FREE_GB: f64 = 50.0;
/// Default total budget for one user's `/var/cache/sinex` cache roots.
///
/// The active checkout target can be tens of GiB on its own, so this is a
/// retention budget for stale sibling roots rather than a hard per-checkout
/// cap.
pub const DEFAULT_GLOBAL_CACHE_MAX_GB: f64 = 160.0;
/// Keep a small number of the newest inactive roots even when the global cache
/// is over budget. This preserves one or two recent branch pivots without
/// letting abandoned worktree roots accumulate indefinitely.
pub const DEFAULT_GLOBAL_CACHE_KEEP_INACTIVE: usize = 2;

#[derive(Debug, Clone)]
pub struct DiskUsage {
    pub mount: String,
    pub total_gb: f64,
    pub used_gb: f64,
    pub free_gb: f64,
    pub percent_used: f64,
}

impl DiskUsage {
    #[must_use]
    pub fn warn(&self) -> bool {
        self.percent_used >= WARN_PERCENT
    }
    #[must_use]
    pub fn should_auto_reclaim(&self) -> bool {
        self.percent_used >= AUTO_RECLAIM_PERCENT
    }
    #[must_use]
    pub fn refuse(&self) -> bool {
        self.percent_used >= REFUSE_PERCENT && self.free_gb < REFUSE_MIN_FREE_GB
    }
}

/// Read disk usage for the filesystem containing `path`.
pub fn disk_usage(path: &Path) -> Result<DiskUsage> {
    let output = Command::new("df")
        .arg("-B")
        .arg("1")
        .arg(path)
        .output()
        .with_context(|| format!("failed to run df for {}", path.display()))?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    // df -B 1 output: `Filesystem 1B-blocks Used Available Use% Mounted on`
    // Second line is the data row.
    let line = stdout
        .lines()
        .nth(1)
        .ok_or_else(|| eyre!("df produced no data row for {}", path.display()))?;
    let fields: Vec<&str> = line.split_whitespace().collect();
    if fields.len() < 6 {
        return Err(eyre!("unexpected df output: {line}"));
    }
    let total_bytes: u64 = fields[1].parse().context("parse total bytes")?;
    let used_bytes: u64 = fields[2].parse().context("parse used bytes")?;
    let free_bytes: u64 = fields[3].parse().context("parse free bytes")?;
    let mount = fields[5].to_string();
    let to_gb = |b: u64| (b as f64) / 1024.0 / 1024.0 / 1024.0;
    let total_gb = to_gb(total_bytes);
    let used_gb = to_gb(used_bytes);
    let free_gb = to_gb(free_bytes);
    let percent_used = if total_bytes > 0 {
        (used_bytes as f64) / (total_bytes as f64) * 100.0
    } else {
        0.0
    };
    Ok(DiskUsage {
        mount,
        total_gb,
        used_gb,
        free_gb,
        percent_used,
    })
}

#[derive(Debug, Default)]
pub struct ReclaimReport {
    pub cargo_sweep_ran: bool,
    pub cargo_sweep_reclaimed_bytes: u64,
    pub incremental_dirs_deleted: usize,
    pub incremental_bytes_reclaimed: u64,
    pub before: Option<DiskUsage>,
    pub after: Option<DiskUsage>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct GlobalCacheRootReport {
    pub path: PathBuf,
    pub bytes: u64,
    pub active: bool,
    pub referenced_by_running_process: bool,
    pub deleted: bool,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct GlobalCacheRetentionReport {
    pub user_cache_root: PathBuf,
    pub budget_bytes: u64,
    pub total_before_bytes: u64,
    pub total_after_bytes: u64,
    pub active_cache_root: Option<PathBuf>,
    pub roots: Vec<GlobalCacheRootReport>,
}

impl GlobalCacheRetentionReport {
    #[must_use]
    pub fn deleted_bytes(&self) -> u64 {
        self.total_before_bytes
            .saturating_sub(self.total_after_bytes)
    }

    #[must_use]
    pub fn deleted_roots(&self) -> usize {
        self.roots.iter().filter(|root| root.deleted).count()
    }
}

/// Reclaim space from `target_dir`.
///
/// Runs `cargo-sweep --time 30 --recursive` if available, then prunes
/// `incremental/` to the configured newest hash dirs per crate.
pub fn reclaim(target_dir: &Path) -> Result<ReclaimReport> {
    let before = disk_usage(target_dir).ok();
    let mut report = ReclaimReport {
        before: before.clone(),
        ..Default::default()
    };

    // Step 1: cargo-sweep
    if which::which("cargo-sweep").is_ok() {
        let output = Command::new("cargo-sweep")
            .arg("sweep")
            .arg("--time")
            .arg("30")
            .arg("--recursive")
            .arg(target_dir)
            .output()
            .with_context(|| "failed to run cargo-sweep")?;
        if output.status.success() {
            report.cargo_sweep_ran = true;
            // cargo-sweep prints "Cleaned X bytes" on stderr; parse it
            let stderr = String::from_utf8_lossy(&output.stderr);
            for line in stderr.lines() {
                if let Some(rest) = line.strip_prefix("Cleaned ")
                    && let Some(bytes_str) = rest.split_whitespace().next()
                    && let Ok(b) = bytes_str.parse::<u64>()
                {
                    report.cargo_sweep_reclaimed_bytes = b;
                    break;
                }
            }
        }
    }

    // Step 2: incremental keep-N-newest-per-crate
    let incremental_dir = target_dir.join("debug").join("incremental");
    if incremental_dir.exists() {
        let keep_per_crate = incremental_keep_per_crate();
        let (deleted, bytes) = prune_incremental(&incremental_dir, keep_per_crate)?;
        report.incremental_dirs_deleted = deleted;
        report.incremental_bytes_reclaimed = bytes;
    }

    report.after = disk_usage(target_dir).ok();
    Ok(report)
}

/// Reclaim stale sibling roots under `/var/cache/sinex/<user>`.
///
/// This covers the agent-worktree failure mode that per-target reclaim cannot:
/// each worktree gets its own cache root, the worktree is removed, and the
/// now-unreachable target tree remains outside the active checkout's
/// `$CARGO_TARGET_DIR`.
pub fn enforce_global_retention_for_target(
    target_dir: &Path,
) -> Result<Option<GlobalCacheRetentionReport>> {
    let Some((user_cache_root, active_root)) = sinex_cache_scope_for_target(target_dir) else {
        return Ok(None);
    };

    enforce_global_retention(&user_cache_root, Some(&active_root))
        .map(Some)
        .with_context(|| {
            format!(
                "enforce global sinex cache retention for {}",
                user_cache_root.display()
            )
        })
}

fn enforce_global_retention(
    user_cache_root: &Path,
    active_root: Option<&Path>,
) -> Result<GlobalCacheRetentionReport> {
    let budget_bytes = global_cache_budget_bytes();
    let keep_inactive = global_cache_keep_inactive();
    let referenced_roots = running_process_cache_roots(user_cache_root);
    let active_root = active_root.map(Path::to_path_buf);

    let mut roots = Vec::new();
    for entry in std::fs::read_dir(user_cache_root)
        .with_context(|| format!("read {}", user_cache_root.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let bytes = dir_size_bytes(&path).unwrap_or(0);
        let active = active_root.as_deref() == Some(path.as_path());
        let referenced_by_running_process = referenced_roots.contains(&path);
        roots.push(GlobalCacheRootReport {
            path,
            bytes,
            active,
            referenced_by_running_process,
            deleted: false,
        });
    }

    roots.sort_by_key(|root| {
        (
            root.active,
            root.referenced_by_running_process,
            std::cmp::Reverse(root_modified_time(&root.path)),
        )
    });

    let total_before = roots.iter().map(|root| root.bytes).sum::<u64>();
    let mut total_after = total_before;

    let mut inactive_kept = 0usize;
    for root in &mut roots {
        if total_after <= budget_bytes {
            break;
        }
        if root.active || root.referenced_by_running_process {
            continue;
        }
        if inactive_kept < keep_inactive {
            inactive_kept += 1;
            continue;
        }
        if std::fs::remove_dir_all(&root.path).is_ok() {
            total_after = total_after.saturating_sub(root.bytes);
            root.deleted = true;
        }
    }

    Ok(GlobalCacheRetentionReport {
        user_cache_root: user_cache_root.to_path_buf(),
        budget_bytes,
        total_before_bytes: total_before,
        total_after_bytes: total_after,
        active_cache_root: active_root,
        roots,
    })
}

fn global_cache_budget_bytes() -> u64 {
    let gb = std::env::var("SINEX_GLOBAL_CACHE_MAX_GB")
        .ok()
        .and_then(|value| value.parse::<f64>().ok())
        .filter(|value| *value > 0.0)
        .unwrap_or(DEFAULT_GLOBAL_CACHE_MAX_GB);
    (gb * 1024.0 * 1024.0 * 1024.0) as u64
}

fn global_cache_keep_inactive() -> usize {
    std::env::var("SINEX_GLOBAL_CACHE_KEEP_INACTIVE")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(DEFAULT_GLOBAL_CACHE_KEEP_INACTIVE)
}

fn sinex_user_cache_root_for_target(target_dir: &Path) -> Option<(PathBuf, PathBuf)> {
    let target_dir = target_dir
        .canonicalize()
        .unwrap_or_else(|_| target_dir.to_path_buf());
    let components: Vec<_> = target_dir.components().collect();
    let sinex_idx = components.windows(3).position(|window| {
        window[0].as_os_str() == OsStr::new("var")
            && window[1].as_os_str() == OsStr::new("cache")
            && window[2].as_os_str() == OsStr::new("sinex")
    })?;

    let user_idx = sinex_idx + 3;
    let root_idx = sinex_idx + 4;
    components.get(root_idx)?;

    let mut user_cache_root = PathBuf::new();
    for component in &components[..=user_idx] {
        user_cache_root.push(component.as_os_str());
    }

    let mut active_root = user_cache_root.clone();
    active_root.push(components[root_idx].as_os_str());
    Some((user_cache_root, active_root))
}

fn running_process_cache_roots(user_cache_root: &Path) -> BTreeSet<PathBuf> {
    let mut roots = BTreeSet::new();
    let Ok(proc_entries) = std::fs::read_dir("/proc") else {
        return roots;
    };
    let user_prefix = user_cache_root.to_string_lossy();

    for entry in proc_entries.flatten() {
        let file_name = entry.file_name();
        if file_name.to_string_lossy().parse::<u32>().is_err() {
            continue;
        }
        let cmdline_path = entry.path().join("cmdline");
        let Ok(raw) = std::fs::read(&cmdline_path) else {
            continue;
        };
        if raw.is_empty() {
            continue;
        }
        let normalized = raw
            .iter()
            .map(|byte| if *byte == 0 { b' ' } else { *byte })
            .collect::<Vec<_>>();
        let command = String::from_utf8_lossy(&normalized);
        let Some(offset) = command.find(user_prefix.as_ref()) else {
            continue;
        };
        let tail = &command[offset + user_prefix.len()..];
        let mut parts = tail.trim_start_matches('/').split('/');
        let Some(root_name) = parts.next().filter(|name| !name.is_empty()) else {
            continue;
        };
        roots.insert(user_cache_root.join(root_name));
    }

    roots
}

fn sinex_cache_scope_for_target(target_dir: &Path) -> Option<(PathBuf, PathBuf)> {
    sinex_user_cache_root_for_target(target_dir).or_else(sinnix_workspace_cache_scope)
}

fn sinnix_workspace_cache_scope() -> Option<(PathBuf, PathBuf)> {
    let user = std::env::var("USER").ok()?;
    let user_cache_root = PathBuf::from("/var/cache/sinex").join(user);
    if !user_cache_root.is_dir() {
        return None;
    }
    let active_root = user_cache_root.join(crate::config::workspace_hash(
        &crate::config::workspace_root(),
    ));
    Some((user_cache_root, active_root))
}

fn root_modified_time(path: &Path) -> SystemTime {
    std::fs::metadata(path)
        .and_then(|metadata| metadata.modified())
        .unwrap_or(SystemTime::UNIX_EPOCH)
}

fn incremental_keep_per_crate() -> usize {
    std::env::var("SINEX_INCREMENTAL_KEEP_PER_CRATE")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_KEEP_PER_CRATE)
}

#[must_use]
pub fn configured_incremental_keep_per_crate() -> usize {
    incremental_keep_per_crate()
}

/// For each `<crate>-` prefix in `incremental_dir`, keep the `keep_n` most
/// recently modified hash subdirs, delete the rest. Returns (dirs_deleted,
/// bytes_freed).
fn prune_incremental(incremental_dir: &Path, keep_n: usize) -> Result<(usize, u64)> {
    let entries = std::fs::read_dir(incremental_dir)
        .with_context(|| format!("read_dir {}", incremental_dir.display()))?;

    // Group by crate prefix (everything before the last `-<hash>` segment).
    let mut by_crate: BTreeMap<String, Vec<(SystemTime, PathBuf)>> = BTreeMap::new();
    for entry in entries.flatten() {
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if !path.is_dir() {
            continue;
        }
        // Strip the trailing `-<hash>` segment.
        let Some(idx) = name.rfind('-') else {
            continue;
        };
        let crate_prefix = name[..idx].to_string();
        let mtime = entry
            .metadata()
            .and_then(|m| m.modified())
            .unwrap_or(SystemTime::UNIX_EPOCH);
        by_crate
            .entry(crate_prefix)
            .or_default()
            .push((mtime, path));
    }

    let mut deleted = 0usize;
    let mut bytes_freed = 0u64;

    for (_crate, mut hashes) in by_crate {
        if hashes.len() <= keep_n {
            continue;
        }
        // Sort newest first
        hashes.sort_by_key(|a| std::cmp::Reverse(a.0));
        // Delete everything past the keep_n boundary
        for (_mtime, dir_path) in hashes.into_iter().skip(keep_n) {
            let dir_bytes = dir_size_bytes(&dir_path).unwrap_or(0);
            if std::fs::remove_dir_all(&dir_path).is_ok() {
                deleted += 1;
                bytes_freed += dir_bytes;
            }
        }
    }

    Ok((deleted, bytes_freed))
}

fn dir_size_bytes(dir: &Path) -> Result<u64> {
    let mut total = 0u64;
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let meta = entry.metadata()?;
        if meta.is_dir() {
            total += dir_size_bytes(&entry.path()).unwrap_or(0);
        } else {
            total += meta.len();
        }
    }
    Ok(total)
}

#[cfg(test)]
#[path = "cache_hygiene_test.rs"]
mod tests;
