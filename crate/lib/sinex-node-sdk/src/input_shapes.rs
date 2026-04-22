use crate::{
    BatchImporterState, DiscoveredFile, ScanError, scan_for_new_files,
    sqlite_source::SqliteSnapshotState,
};
use camino::Utf8Path;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Shared checkpoint state for multiple SQLite-backed acquisition sources.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct SqliteSourceCheckpointState {
    #[serde(default)]
    row_ids: BTreeMap<String, i64>,
}

impl SqliteSourceCheckpointState {
    #[must_use]
    pub fn cursor(&self, key: &str) -> i64 {
        self.row_ids.get(key).copied().unwrap_or_default()
    }

    pub fn set_cursor(&mut self, key: impl Into<String>, row_id: i64) {
        self.row_ids.insert(key.into(), row_id);
    }

    pub fn advance_cursor(&mut self, key: impl Into<String>, row_id: i64) {
        let key = key.into();
        let next = self.cursor(&key).max(row_id);
        self.row_ids.insert(key, next);
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.row_ids.is_empty()
    }
}

/// Shared snapshot-evidence state for multiple SQLite-backed acquisition sources.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct SqliteSnapshotCheckpointState {
    #[serde(default)]
    snapshots: BTreeMap<String, SqliteSnapshotState>,
}

impl SqliteSnapshotCheckpointState {
    pub fn state_mut(&mut self, key: impl Into<String>) -> &mut SqliteSnapshotState {
        self.snapshots.entry(key.into()).or_default()
    }

    #[must_use]
    pub fn state(&self, key: &str) -> Option<&SqliteSnapshotState> {
        self.snapshots.get(key)
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.snapshots.is_empty()
    }
}

/// Remember the import root before scanning it for changed files.
pub fn discover_importable_files_at_root(
    state: &mut BatchImporterState,
    scan_root: &Utf8Path,
    extensions: &[&str],
) -> Result<Vec<DiscoveredFile>, ScanError> {
    state.remember_scan_root(scan_root.to_owned());
    scan_for_new_files(state, scan_root, extensions)
}
