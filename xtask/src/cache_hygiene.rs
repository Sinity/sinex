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
//!    prefix in `incremental/`, keep the newest hash dir by default and delete
//!    the rest. Mirrors what cargo-sweep does for incremental, but intentionally
//!    more aggressively because incremental compilation is an opt-in local
//!    edit-loop policy in this repo.
//! 3. **Disk-usage report** before + after.
//!
//! See issue #1213 for the broader cancel-reason substrate, and issue (TBD)
//! for the bounded enforcement.

use color_eyre::eyre::{Context, Result, eyre};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::SystemTime;

/// Default number of hash-variant dirs to keep per crate in `incremental/`.
///
/// Cargo writes a new dir on each fingerprint change (feature set, rustc
/// version, source modification). Keep only the newest variant by default:
/// incremental is not the normal sccache-backed build policy, so stale variants
/// should not accumulate across feature/config experiments.
const DEFAULT_KEEP_PER_CRATE: usize = 1;

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
mod tests {
    use super::*;
    use xtask::sandbox::prelude::sinex_test;

    #[sinex_test]
    async fn prune_keeps_newest_n_per_crate() -> xtask::sandbox::TestResult<()> {
        let temp = tempfile::tempdir()?;
        let inc = temp.path().join("incremental");
        std::fs::create_dir(&inc)?;
        // Create 5 hashes for crate "foo"
        for (i, hash) in ["aaaa", "bbbb", "cccc", "dddd", "eeee"].iter().enumerate() {
            let d = inc.join(format!("foo-{hash}"));
            std::fs::create_dir(&d)?;
            // Different mtimes via touch via sleep is fragile; use filetime crate if needed.
            // For this test rely on creation order.
            std::fs::write(d.join("dummy"), vec![0u8; 100])?;
            std::thread::sleep(std::time::Duration::from_millis(20));
            let _ = i;
        }
        // Also create one hash for "bar" that should survive.
        std::fs::create_dir(inc.join("bar-zzzz"))?;
        std::fs::write(inc.join("bar-zzzz/dummy"), vec![0u8; 100])?;

        let (deleted, _bytes) = prune_incremental(&inc, 3)?;
        assert_eq!(deleted, 2, "expected to delete 2 oldest foo-* dirs");

        let remaining: Vec<_> = std::fs::read_dir(&inc)?
            .flatten()
            .map(|e| e.file_name().to_string_lossy().to_string())
            .collect();
        assert_eq!(remaining.len(), 4, "3 foo + 1 bar = 4 remaining");
        assert!(remaining.iter().any(|n| n == "bar-zzzz"));
        Ok(())
    }

    #[sinex_test]
    async fn disk_usage_reads_valid_filesystem() -> xtask::sandbox::TestResult<()> {
        // /tmp should always exist
        let u = disk_usage(Path::new("/tmp"))?;
        assert!(u.total_gb > 0.0);
        assert!(u.percent_used >= 0.0 && u.percent_used <= 100.0);
        Ok(())
    }

    #[sinex_test]
    async fn disk_refusal_requires_percent_and_low_absolute_free_space()
    -> xtask::sandbox::TestResult<()> {
        let large_mount = DiskUsage {
            mount: "/realm".to_string(),
            total_gb: 4096.0,
            used_gb: 3738.0,
            free_gb: 358.0,
            percent_used: 91.3,
        };
        assert!(large_mount.should_auto_reclaim());
        assert!(
            !large_mount.refuse(),
            "large mounts with hundreds of GiB free should not require \
             SINEX_PREFLIGHT_SKIP_DISK_CHECK"
        );

        let nearly_full_mount = DiskUsage {
            mount: "/cache".to_string(),
            total_gb: 500.0,
            used_gb: 460.0,
            free_gb: 40.0,
            percent_used: 92.0,
        };
        assert!(nearly_full_mount.refuse());
        Ok(())
    }
}
