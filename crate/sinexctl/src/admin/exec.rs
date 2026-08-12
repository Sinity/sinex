//! Thin wrappers around external tools: `pg_dump`, `tar`, `psql`, `systemctl`.
//!
//! All commands are invoked via [`std::process::Command`] with explicit argv —
//! never through `sh -c`.  Failures surface as `color_eyre::Result` with
//! structured context so the caller can add further context.

use color_eyre::eyre::{Context, Result, bail, eyre};
use serde_json::Value;
use std::collections::BTreeMap;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
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

/// Query `PostgreSQL` for exact durable row counts.
///
/// Uses `psql` with `-t` (tuples only) and `-A` (unaligned) to list durable
/// user tables, then counts each table exactly. Temporary schemas are excluded:
/// they can appear in `pg_stat_user_tables` while active sessions exist, but
/// they are not restore-stable archive content.
pub fn pg_row_counts(database_url: &str) -> Result<BTreeMap<String, i64>> {
    pg_row_counts_with(database_url, None)
}

pub fn pg_row_counts_with(
    database_url: &str,
    psql_bin: Option<&Path>,
) -> Result<BTreeMap<String, i64>> {
    let sql = "SELECT schemaname || '.' || relname \
               FROM pg_stat_user_tables \
               WHERE schemaname NOT LIKE '\\_%' ESCAPE '\\' \
                 AND schemaname NOT LIKE 'pg_temp_%' \
                 AND schemaname NOT LIKE 'pg_toast_temp_%' \
               ORDER BY 1;";

    let output = Command::new(psql_bin.unwrap_or_else(|| Path::new("psql")))
        .args([
            "--tuples-only",
            "--no-align",
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
    let mut tables = Vec::new();
    for line in stdout.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        tables.push(line.to_string());
    }
    pg_exact_row_counts(database_url, tables, psql_bin)
}

/// Execute a SQL command through `psql`.
pub fn psql_execute(database_url: &str, sql: &str, psql_bin: Option<&Path>) -> Result<()> {
    let output = Command::new(psql_bin.unwrap_or_else(|| Path::new("psql")))
        .args([
            "--no-align",
            "--tuples-only",
            "--command",
            sql,
            database_url,
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .context("spawn psql")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "psql command failed (exit {}): {}",
            output.status.code().unwrap_or(-1),
            stderr.trim()
        );
    }

    Ok(())
}

/// Return the number of user relations in a restore target.
///
/// A restore drill must start with a database that has no user-owned
/// relations. Checking relation existence, rather than approximate row
/// statistics, catches a target that would collide with the dump even when its
/// tables happen to be empty.
pub fn pg_user_relation_count(database_url: &str, psql_bin: Option<&Path>) -> Result<i64> {
    let sql = "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relkind IN ('r','p','v','m','f') AND n.nspname <> 'information_schema' AND n.nspname NOT LIKE 'pg_%';";
    let output = Command::new(psql_bin.unwrap_or_else(|| Path::new("psql")))
        .args([
            "--tuples-only",
            "--no-align",
            "--command",
            sql,
            database_url,
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .context("spawn psql to verify restore target emptiness")?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "psql restore-target emptiness query failed (exit {}): {}",
            output.status.code().unwrap_or(-1),
            stderr.trim()
        );
    }
    String::from_utf8_lossy(&output.stdout)
        .trim()
        .parse::<i64>()
        .context("parse restore-target user relation count")
}

/// The independently deployed surfaces that a backup/restore operation must
/// understand.  All callers use this discovery result instead of inventing a
/// state path, unit glob, or database-target assumption locally.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotTopology {
    pub state_dir: PathBuf,
    pub nats_store_dir: Option<PathBuf>,
    pub active_writer_units: Vec<String>,
}

impl SnapshotTopology {
    /// Discover the deployed topology needed by one snapshot/restore route.
    /// Explicit state paths are test/alternate-deployment overrides; the
    /// normal path reads the service environment and NATS unit configuration.
    pub fn discover(
        state_dir_override: Option<&Path>,
        require_nats: bool,
        inspect_active_units: bool,
    ) -> Result<Self> {
        let state_dir = match state_dir_override {
            Some(path) => path.to_path_buf(),
            None => sinex_service_path("SINEX_STATE_DIR")
                .context("resolve deployed SINEX_STATE_DIR")?,
        };
        let nats_store_dir = if require_nats {
            Some(if state_dir_override.is_some() {
                state_dir.join("nats/jetstream")
            } else {
                nats_jetstream_store_dir()
                    .context("discover deployed NATS JetStream store directory")?
            })
        } else {
            None
        };
        let active_writer_units = if inspect_active_units {
            active_sinex_services().context("inspect active snapshot-writer units")?
        } else {
            Vec::new()
        };
        Ok(Self {
            state_dir,
            nats_store_dir,
            active_writer_units,
        })
    }

    /// Verify the isolated PostgreSQL restore target through the same
    /// topology object that discovered the writer services and state roots.
    pub fn verify_restore_database_empty(
        &self,
        database_url: &str,
        psql_bin: Option<&Path>,
    ) -> Result<()> {
        let user_relation_count = pg_user_relation_count(database_url, psql_bin)
            .context("verify restore target database is empty")?;
        if user_relation_count != 0 {
            bail!(
                "restore target database is not empty: {user_relation_count} user relation(s) exist"
            );
        }
        Ok(())
    }
}

/// Restore a custom-format `pg_dump` archive into `database_url`.
pub fn pg_restore(
    database_url: &str,
    dump_path: &Path,
    pg_restore_bin: Option<&Path>,
) -> Result<()> {
    let output = Command::new(pg_restore_bin.unwrap_or_else(|| Path::new("pg_restore")))
        .args([
            "--dbname",
            database_url,
            "--no-owner",
            "--no-privileges",
            dump_path
                .to_str()
                .ok_or_else(|| eyre!("dump path is not valid UTF-8"))?,
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .context("spawn pg_restore")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "pg_restore failed (exit {}): {}",
            output.status.code().unwrap_or(-1),
            stderr.trim()
        );
    }

    Ok(())
}

/// Query exact row counts for a known set of `schema.table` names.
pub fn pg_exact_row_counts(
    database_url: &str,
    tables: impl IntoIterator<Item = String>,
    psql_bin: Option<&Path>,
) -> Result<BTreeMap<String, i64>> {
    let mut counts = BTreeMap::new();
    for table in tables {
        let Some((schema, relation)) = table.split_once('.') else {
            continue;
        };
        let sql = format!(
            "SELECT count(*) FROM \"{}\".\"{}\";",
            schema.replace('"', "\"\""),
            relation.replace('"', "\"\"")
        );
        let output = Command::new(psql_bin.unwrap_or_else(|| Path::new("psql")))
            .args([
                "--tuples-only",
                "--no-align",
                "--command",
                &sql,
                database_url,
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .with_context(|| format!("spawn psql for exact row count of {table}"))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            bail!(
                "psql exact row-count query failed for {table} (exit {}): {}",
                output.status.code().unwrap_or(-1),
                stderr.trim()
            );
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        let count = stdout
            .trim()
            .parse::<i64>()
            .with_context(|| format!("parse exact row count for {table}"))?;
        counts.insert(table, count);
    }
    Ok(counts)
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

/// List a zstd-compressed tar archive.
pub fn tar_list_zstd(archive_path: &Path) -> Result<Vec<String>> {
    let output = Command::new("tar")
        .args([
            "--use-compress-program=zstd",
            "-tf",
            archive_path
                .to_str()
                .ok_or_else(|| eyre!("archive path is not valid UTF-8"))?,
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .context("spawn tar for zstd archive listing")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "tar listing failed (exit {}): {}",
            output.status.code().unwrap_or(-1),
            stderr.trim()
        );
    }

    Ok(String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect())
}

/// Read one member from a zstd-compressed tar archive.
pub fn tar_read_file_zstd(archive_path: &Path, member: &str) -> Result<Vec<u8>> {
    let output = Command::new("tar")
        .args([
            "--use-compress-program=zstd",
            "-xOf",
            archive_path
                .to_str()
                .ok_or_else(|| eyre!("archive path is not valid UTF-8"))?,
            member,
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .with_context(|| format!("spawn tar to read {member} from archive"))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "tar read failed for {member} (exit {}): {}",
            output.status.code().unwrap_or(-1),
            stderr.trim()
        );
    }

    Ok(output.stdout)
}

/// Extract a zstd-compressed tar archive into `target_dir`.
pub fn tar_extract_zstd(archive_path: &Path, target_dir: &Path) -> Result<()> {
    let output = Command::new("tar")
        .args([
            "--use-compress-program=zstd",
            "-xf",
            archive_path
                .to_str()
                .ok_or_else(|| eyre!("archive path is not valid UTF-8"))?,
            "-C",
            target_dir
                .to_str()
                .ok_or_else(|| eyre!("target path is not valid UTF-8"))?,
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .with_context(|| format!("spawn tar to extract {}", archive_path.display()))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "tar extraction failed (exit {}): {}",
            output.status.code().unwrap_or(-1),
            stderr.trim()
        );
    }

    Ok(())
}

/// Check which deployed services and timers can mutate snapshot components.
///
/// The runtime target is intentionally excluded. Its `PartOf` relationship
/// also stops PostgreSQL, which must remain available for the dump. Discovery
/// is based on the active systemd inventory so newly generated source workers
/// are not silently missed.
pub fn active_sinex_services() -> Result<Vec<String>> {
    let output = Command::new("systemctl")
        .args(["list-units", "--state=active", "--plain", "--no-legend"])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .context("inspect active systemd units for snapshot quiesce")?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "systemctl active-unit inspection failed (exit {}): {}",
            output.status.code().unwrap_or(-1),
            stderr.trim()
        );
    }

    Ok(String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| line.split_whitespace().next())
        .filter(|unit| is_snapshot_writer_unit(unit))
        .map(str::to_string)
        .collect())
}

/// Stop the active writer services without stopping PostgreSQL via a target.
pub fn stop_sinex_services() -> Result<()> {
    let topology = SnapshotTopology::discover(None, false, true)?;
    stop_sinex_services_for(&topology.active_writer_units)
}

/// Stop the writer units already observed by [`SnapshotTopology`].
pub fn stop_sinex_services_for(active: &[String]) -> Result<()> {
    if active.is_empty() {
        return Ok(());
    }
    let output = Command::new("systemctl")
        .arg("stop")
        .args(active)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .context("spawn systemctl stop snapshot writer services")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "systemctl stop snapshot writer services failed (exit {}): {}",
            output.status.code().unwrap_or(-1),
            stderr.trim()
        );
    }
    let remaining = active_sinex_services()?;
    if !remaining.is_empty() {
        bail!(
            "snapshot writer units remain active after stop: {}",
            remaining.join(", ")
        );
    }
    Ok(())
}

fn is_snapshot_writer_unit(unit: &str) -> bool {
    if unit == "nats.service" || unit == "sinexd.service" {
        return true;
    }
    if !unit.starts_with("sinex-") {
        return false;
    }
    let is_service_or_timer = unit.ends_with(".service") || unit.ends_with(".timer");
    let is_target_access = unit.contains("-target-access.service");
    let is_desktop_setup = unit.starts_with("sinex-kitty-")
        || unit.starts_with("sinex-terminal-target-access")
        || unit.starts_with("sinex-browser-target-access")
        || unit.starts_with("sinex-desktop-target-access")
        || unit.starts_with("sinex-document-target-access");
    is_service_or_timer && !is_target_access && !is_desktop_setup
}

/// Read a path-valued environment variable from the deployed `sinexd.service`.
pub fn sinex_service_path(variable: &str) -> Result<PathBuf> {
    let output = Command::new("systemctl")
        .args([
            "show",
            "sinexd.service",
            "--property=Environment",
            "--value",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .context("inspect sinexd deployment environment")?;
    if !output.status.success() {
        bail!("systemctl could not inspect sinexd.service environment");
    }
    let prefix = format!("{variable}=");
    let environment = String::from_utf8_lossy(&output.stdout);
    let value = environment
        .split_whitespace()
        .find_map(|entry| entry.strip_prefix(&prefix))
        .filter(|value| !value.is_empty())
        .ok_or_else(|| eyre!("sinexd.service has no non-empty {variable}"))?;
    Ok(PathBuf::from(value))
}

/// Discover the JetStream store directory from the running NATS unit's
/// validated configuration. This avoids coupling snapshots to SINEX_STATE_DIR:
/// NATS is deployed as a separate service with its own store root.
pub fn nats_jetstream_store_dir() -> Result<PathBuf> {
    let output = Command::new("systemctl")
        .args(["show", "nats.service", "--property=ExecStart", "--value"])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .context("discover NATS systemd command line")?;
    if !output.status.success() {
        bail!("systemctl could not inspect nats.service");
    }
    let command = String::from_utf8_lossy(&output.stdout);
    let config_path = nats_config_path_from_exec_start(&command)?;
    let config = std::fs::read_to_string(&config_path)
        .with_context(|| format!("read NATS config {}", config_path.display()))?;
    nats_store_dir_from_config(&config)
        .with_context(|| format!("parse NATS config {} as JSON", config_path.display()))
}

fn nats_config_path_from_exec_start(exec_start: &str) -> Result<PathBuf> {
    let tokens = exec_start.split_whitespace().collect::<Vec<_>>();
    let config_path = tokens
        .windows(2)
        .find_map(|parts| (parts[0] == "-c").then_some(parts[1]))
        .ok_or_else(|| eyre!("nats.service ExecStart does not expose a -c config path"))?;
    Ok(PathBuf::from(config_path))
}

fn nats_store_dir_from_config(config: &str) -> Result<PathBuf> {
    let config: Value = serde_json::from_str(config)?;
    let store_dir = config
        .get("jetstream")
        .and_then(|jetstream| jetstream.get("store_dir"))
        .and_then(Value::as_str)
        .filter(|path| !path.is_empty())
        .ok_or_else(|| eyre!("NATS config has no non-empty jetstream.store_dir"))?;
    Ok(PathBuf::from(store_dir))
}

/// Copy a directory tree recursively with `cp -a`.
pub fn cp_tree(src: &Path, dst_parent: &Path) -> Result<()> {
    let src_str = src
        .to_str()
        .ok_or_else(|| eyre!("source path is not valid UTF-8: {}", src.display()))?;

    // Use `/.` so `cp -a src/. dst/` copies the contents of src into dst,
    // not src itself as a sub-directory.
    let src_contents = if src_str.ends_with('/') {
        format!("{src_str}.")
    } else {
        format!("{src_str}/.")
    };

    let dst_str = dst_parent.to_str().ok_or_else(|| {
        eyre!(
            "destination path is not valid UTF-8: {}",
            dst_parent.display()
        )
    })?;

    let output = Command::new("cp")
        .args(["-a", &src_contents, dst_str])
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

/// Copy a directory tree recursively while tolerating source files that vanish.
///
/// Live snapshots read active runtime directories, so queue/spool files can be
/// deleted between directory enumeration and file copy. Quiesce-mode snapshots
/// should keep using [`cp_tree`] so those races remain visible when services are
/// expected to be stopped.
pub fn cp_tree_live(src: &Path, dst_parent: &Path) -> Result<()> {
    std::fs::create_dir_all(dst_parent)
        .with_context(|| format!("create live-copy destination {}", dst_parent.display()))?;
    copy_dir_contents_live(src, dst_parent)
}

fn copy_dir_contents_live(src: &Path, dst: &Path) -> Result<()> {
    let entries = match std::fs::read_dir(src) {
        Ok(entries) => entries,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("read live-copy source dir {}", src.display()));
        }
    };

    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) if error.kind() == ErrorKind::NotFound => continue,
            Err(error) => return Err(error).context("read live-copy source dir entry"),
        };
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());
        cp_entry_live(&src_path, &dst_path)?;
    }
    Ok(())
}

/// Copy one live-snapshot entry while treating a source that vanishes between
/// enumeration and copying as an expected race.
pub fn cp_entry_live(src: &Path, dst: &Path) -> Result<()> {
    let metadata = match std::fs::symlink_metadata(src) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("read live-copy source metadata {}", src.display()));
        }
    };

    if metadata.file_type().is_symlink() {
        let target = match std::fs::read_link(src) {
            Ok(target) => target,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("read live-copy symlink {}", src.display()));
            }
        };
        #[cfg(unix)]
        {
            let _ = std::fs::remove_file(dst);
            std::os::unix::fs::symlink(target, dst)
                .with_context(|| format!("create live-copy symlink {}", dst.display()))?;
        }
        #[cfg(not(unix))]
        {
            let _ = target;
        }
        return Ok(());
    }

    if metadata.is_dir() {
        std::fs::create_dir_all(dst)
            .with_context(|| format!("create live-copy dir {}", dst.display()))?;
        let _ = std::fs::set_permissions(dst, metadata.permissions());
        return copy_dir_contents_live(src, dst);
    }

    if metadata.is_file() {
        if let Some(parent) = dst.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create live-copy parent {}", parent.display()))?;
        }
        match std::fs::copy(src, dst) {
            Ok(_) => {
                let _ = std::fs::set_permissions(dst, metadata.permissions());
                Ok(())
            }
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error).with_context(|| {
                format!(
                    "copy live source file {} -> {}",
                    src.display(),
                    dst.display()
                )
            }),
        }
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{nats_config_path_from_exec_start, nats_store_dir_from_config};

    #[test]
    fn parses_systemd_nats_execstart_and_json_store_dir() {
        let exec_start = "{ path=/nix/store/nats/bin/nats-server ; argv[]=/nix/store/nats/bin/nats-server -c /nix/store/config ; status=0/0 }";
        assert_eq!(
            nats_config_path_from_exec_start(exec_start)
                .expect("systemd ExecStart should expose -c config path")
                .to_string_lossy(),
            "/nix/store/config"
        );
        assert_eq!(
            nats_store_dir_from_config(
                r#"{"jetstream":{"store_dir":"/var/lib/nats/jetstream"}}"#
            )
            .expect("NATS JSON should expose jetstream.store_dir")
            .to_string_lossy(),
            "/var/lib/nats/jetstream"
        );
    }

    #[test]
    fn rejects_nats_config_without_store_dir() {
        let error = nats_store_dir_from_config(r#"{"jetstream":{}}"#)
            .expect_err("missing NATS store_dir must not silently select a fallback");
        assert!(error.to_string().contains("store_dir"));
    }
}
