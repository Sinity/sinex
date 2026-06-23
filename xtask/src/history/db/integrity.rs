use super::HISTORY_DB_SCHEMA_VERSION;
use color_eyre::eyre::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tempfile::NamedTempFile;
use time::OffsetDateTime;

const HISTORY_DB_INTEGRITY_CHECK_INTERVAL: Duration = Duration::from_hours(6);
const HISTORY_DB_INTEGRITY_STAMP_EXTENSION: &str = "db.integrity.json";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct HistoryIntegrityStamp {
    pub(super) schema_version: i32,
    pub(super) checked_at_unix: i64,
}

impl HistoryIntegrityStamp {
    fn new(now: OffsetDateTime) -> Self {
        Self {
            schema_version: HISTORY_DB_SCHEMA_VERSION,
            checked_at_unix: now.unix_timestamp(),
        }
    }

    fn is_fresh(&self, now: OffsetDateTime, interval: Duration) -> bool {
        if self.schema_version != HISTORY_DB_SCHEMA_VERSION {
            return false;
        }

        let age_secs = now.unix_timestamp().saturating_sub(self.checked_at_unix);
        age_secs <= interval.as_secs().min(i64::MAX as u64) as i64
    }
}

pub(super) fn history_integrity_stamp_path(path: &Path) -> PathBuf {
    path.with_extension(HISTORY_DB_INTEGRITY_STAMP_EXTENSION)
}

fn history_recreation_artifact_paths(path: &Path) -> [PathBuf; 4] {
    [
        path.to_path_buf(),
        path.with_extension("db-wal"),
        path.with_extension("db-shm"),
        history_integrity_stamp_path(path),
    ]
}

fn history_artifact_backup_dir(path: &Path, suffix: &str) -> Result<PathBuf> {
    let file_name = path.file_name().ok_or_else(|| {
        color_eyre::eyre::eyre!("history artifact path has no file name: {}", path.display())
    })?;
    let file_name = file_name.to_string_lossy();
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    for index in 0..1000 {
        let candidate_name = if index == 0 {
            format!("{file_name}.{suffix}.bak")
        } else {
            format!("{file_name}.{suffix}.{index}.bak")
        };
        let candidate = parent.join(candidate_name);
        match std::fs::create_dir(&candidate) {
            Ok(()) => return Ok(candidate),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "failed to create history artifact backup directory: {}",
                        candidate.display()
                    )
                });
            }
        }
    }

    color_eyre::eyre::bail!(
        "failed to allocate unique backup directory for history artifact: {}",
        path.display()
    );
}

pub(super) fn preserve_history_artifacts_for_recreation(
    path: &Path,
    reason: &str,
) -> Result<Vec<(PathBuf, PathBuf)>> {
    let suffix = format!(
        "{reason}-{}",
        OffsetDateTime::now_utc().unix_timestamp_nanos()
    );
    let artifacts = history_recreation_artifact_paths(path)
        .into_iter()
        .filter(|artifact| artifact.exists())
        .collect::<Vec<_>>();
    if artifacts.is_empty() {
        return Ok(Vec::new());
    }

    let backup_dir = history_artifact_backup_dir(path, &suffix)?;
    let mut preserved = Vec::new();
    for artifact in artifacts {
        let artifact_name = artifact.file_name().ok_or_else(|| {
            color_eyre::eyre::eyre!(
                "history artifact path has no file name: {}",
                artifact.display()
            )
        })?;
        let backup_path = backup_dir.join(artifact_name);
        match std::fs::rename(&artifact, &backup_path) {
            Ok(()) => preserved.push((artifact, backup_path)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "failed to preserve history artifact {} before recreation",
                        artifact.display()
                    )
                });
            }
        }
    }
    Ok(preserved)
}

pub(super) fn format_preserved_history_artifact_destinations(
    backups: &[(PathBuf, PathBuf)],
) -> String {
    backups
        .iter()
        .map(|(_, backup)| backup.display().to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

pub(super) fn history_integrity_check_interval() -> Duration {
    match std::env::var("XTASK_HISTORY_INTEGRITY_INTERVAL_SECS") {
        Ok(raw) => raw
            .trim()
            .parse::<u64>()
            .map_or(HISTORY_DB_INTEGRITY_CHECK_INTERVAL, Duration::from_secs),
        Err(_) => HISTORY_DB_INTEGRITY_CHECK_INTERVAL,
    }
}

pub(super) fn load_history_integrity_stamp(path: &Path) -> Option<HistoryIntegrityStamp> {
    let bytes = std::fs::read(path).ok()?;
    serde_json::from_slice(&bytes).ok()
}

pub(super) fn should_run_history_integrity_check(path: &Path, now: OffsetDateTime) -> bool {
    let interval = history_integrity_check_interval();
    if interval.is_zero() {
        return true;
    }

    let stamp_path = history_integrity_stamp_path(path);
    !load_history_integrity_stamp(&stamp_path).is_some_and(|stamp| stamp.is_fresh(now, interval))
}

pub(super) fn persist_history_integrity_stamp(path: &Path, now: OffsetDateTime) -> Result<()> {
    let stamp_path = history_integrity_stamp_path(path);
    let parent = stamp_path.parent().ok_or_else(|| {
        color_eyre::eyre::eyre!(
            "history integrity stamp path has no parent: {}",
            stamp_path.display()
        )
    })?;
    std::fs::create_dir_all(parent).with_context(|| {
        format!(
            "failed to create history stamp directory: {}",
            parent.display()
        )
    })?;
    let payload = serde_json::to_vec_pretty(&HistoryIntegrityStamp::new(now))
        .context("failed to serialize history integrity stamp")?;
    let mut temp_file = NamedTempFile::new_in(parent).with_context(|| {
        format!(
            "failed to create temporary history integrity stamp in {}",
            parent.display()
        )
    })?;
    use std::io::Write as _;
    temp_file
        .write_all(&payload)
        .with_context(|| "failed to write temporary history integrity stamp")?;
    temp_file
        .persist(&stamp_path)
        .map_err(|error| error.error)
        .with_context(|| {
            format!(
                "failed to persist history integrity stamp: {}",
                stamp_path.display()
            )
        })?;
    Ok(())
}

pub(super) fn refresh_history_integrity_stamp(path: &Path, now: OffsetDateTime) {
    if let Err(error) = persist_history_integrity_stamp(path, now) {
        eprintln!(
            "⚠️  Failed to refresh history DB integrity stamp at {}: {error:#}",
            history_integrity_stamp_path(path).display()
        );
    }
}
