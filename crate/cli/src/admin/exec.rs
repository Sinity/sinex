//! Thin wrappers around external tools: `pg_dump`, `tar`, `psql`, `systemctl`.
//!
//! All commands are invoked via [`std::process::Command`] with explicit argv —
//! never through `sh -c`.  Failures surface as `color_eyre::Result` with
//! structured context so the caller can add further context.

use color_eyre::eyre::{Context, Result, bail, eyre};
use std::collections::BTreeMap;
use std::path::Path;
use std::process::{Command, Stdio};

/// Run `pg_dump -Fc -Z 9 -f <dump_path> <database_url>`.
///
/// Returns the raw stderr bytes captured during the dump (for manifest
/// provenance).
pub fn pg_dump(database_url: &str, dump_path: &Path) -> Result<Vec<u8>> {
    let output = Command::new("pg_dump")
        .args([
            "--format=custom",
            "--compress=9",
            "--file",
            dump_path
                .to_str()
                .ok_or_else(|| eyre!("dump path is not valid UTF-8"))?,
            database_url,
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .context("spawn pg_dump")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "pg_dump failed (exit {}): {}",
            output.status.code().unwrap_or(-1),
            stderr.trim()
        );
    }

    Ok(output.stderr)
}

/// Query PostgreSQL for live row-count estimates (from `pg_stat_user_tables`).
///
/// Uses `psql` with `-t` (tuples only) and `-A` (unaligned) to produce
/// `schema.table|count` lines.  Returns a map of `"schema.table" → count`.
pub fn pg_row_counts(database_url: &str) -> Result<BTreeMap<String, i64>> {
    let sql = "SELECT schemaname || '.' || relname, n_live_tup \
               FROM pg_stat_user_tables \
               ORDER BY 1;";

    let output = Command::new("psql")
        .args([
            "--tuples-only",
            "--no-align",
            "--field-separator=|",
            "--command",
            sql,
            database_url,
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .context("spawn psql for row count query")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "psql row-count query failed (exit {}): {}",
            output.status.code().unwrap_or(-1),
            stderr.trim()
        );
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut map = BTreeMap::new();
    for line in stdout.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if let Some((table, count_str)) = line.split_once('|') {
            if let Ok(count) = count_str.trim().parse::<i64>() {
                map.insert(table.trim().to_string(), count);
            }
        }
    }
    Ok(map)
}

/// Create a compressed tar archive at `output_path` from `staging_dir`.
///
/// Uses `tar -I "zstd -T<workers> -<compression>" -cf` to pipe through zstd.
/// Both `tar` and `zstd` must be on `PATH`.
pub fn tar_create_zstd(
    staging_dir: &Path,
    output_path: &Path,
    compression: u8,
    workers: u32,
) -> Result<()> {
    let zstd_arg = format!("zstd -T{workers} -{compression}");
    let output = Command::new("tar")
        .args([
            "-I",
            &zstd_arg,
            "-cf",
            output_path
                .to_str()
                .ok_or_else(|| eyre!("output path is not valid UTF-8"))?,
            // Archive everything inside staging_dir, using staging_dir as cwd
            // so paths inside the archive are relative.
            ".",
        ])
        .current_dir(staging_dir)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .context("spawn tar for archive creation")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "tar creation failed (exit {}): {}",
            output.status.code().unwrap_or(-1),
            stderr.trim()
        );
    }
    Ok(())
}

/// Verify a tar archive by listing its contents.
///
/// On success the number of entries is returned.
pub fn tar_verify(archive_path: &Path) -> Result<usize> {
    let output = Command::new("tar")
        .args([
            "-tf",
            archive_path
                .to_str()
                .ok_or_else(|| eyre!("archive path is not valid UTF-8"))?,
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .context("spawn tar for archive verification")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "tar verification failed (exit {}): {}",
            output.status.code().unwrap_or(-1),
            stderr.trim()
        );
    }

    let count = String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter(|l| !l.trim().is_empty())
        .count();
    Ok(count)
}

/// Check which sinex systemd services are currently active.
///
/// Returns the list of active unit names matching `sinex-*`.  If `systemctl`
/// is not available (dev environment) returns an empty list.
pub fn active_sinex_services() -> Vec<String> {
    let Ok(output) = Command::new("systemctl")
        .args([
            "list-units",
            "--state=active",
            "--plain",
            "--no-legend",
            "sinex-*",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .output()
    else {
        return Vec::new();
    };

    if !output.status.success() {
        return Vec::new();
    }

    String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| {
            let unit = line.split_whitespace().next()?;
            Some(unit.to_string())
        })
        .collect()
}

/// Stop all sinex services: `systemctl stop 'sinex-*'`.
pub fn stop_sinex_services() -> Result<()> {
    let output = Command::new("systemctl")
        .args(["stop", "sinex-*"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .context("spawn systemctl stop sinex-*")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "systemctl stop sinex-* failed (exit {}): {}",
            output.status.code().unwrap_or(-1),
            stderr.trim()
        );
    }
    Ok(())
}

/// Copy a directory tree recursively with `cp -a`.
pub fn cp_tree(src: &Path, dst_parent: &Path) -> Result<()> {
    let src_str = src
        .to_str()
        .ok_or_else(|| eyre!("source path is not valid UTF-8: {}", src.display()))?;

    // Append a trailing slash so `cp -a src/ dst/` copies the *contents* of
    // src into dst, not src itself as a sub-directory.
    let src_with_slash = if src_str.ends_with('/') {
        src_str.to_string()
    } else {
        format!("{src_str}/")
    };

    let dst_str = dst_parent
        .to_str()
        .ok_or_else(|| eyre!("destination path is not valid UTF-8: {}", dst_parent.display()))?;

    let output = Command::new("cp")
        .args(["-a", &src_with_slash, dst_str])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .context("spawn cp for directory copy")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "cp -a failed (exit {}): {}",
            output.status.code().unwrap_or(-1),
            stderr.trim()
        );
    }
    Ok(())
}
