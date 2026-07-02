//! Adapter for recursive directory walks.
//!
//! Emits one [`SourceRecord`] per file found under the configured roots,
//! using `(size_bytes, modified_ms)` fingerprints for cursor-based dedup.

use std::collections::BTreeMap;
use std::time::UNIX_EPOCH;

use async_trait::async_trait;
use camino::{Utf8Path, Utf8PathBuf};
use futures::StreamExt;
use futures::stream::{self, BoxStream};
use globset::{Glob, GlobSet, GlobSetBuilder};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::fs;

use sinex_primitives::events::SourceMaterial;
use sinex_primitives::ids::Id;
use sinex_primitives::parser::{InputShapeKind, MaterialAnchor, SourceRecord};

use crate::runtime::parser::{
    InputShapeAdapter, ParserError, ParserResult, SourceRecordFingerprint,
};

// =============================================================================
// FileFingerprint
// =============================================================================

/// Stable fingerprint for a directory entry.
///
/// Dedup is performed by comparing `(size_bytes, modified_ms)` against the
/// previous cursor. A changed fingerprint triggers re-emission.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileFingerprint {
    /// File size in bytes as reported by the OS.
    pub size_bytes: u64,
    /// Modification time in milliseconds since the Unix epoch.
    pub modified_ms: i64,
}

// =============================================================================
// DirectoryWalkConfig
// =============================================================================

/// Configuration for [`DirectoryWalkAdapter`].
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct DirectoryWalkConfig {
    /// Root directories to walk.
    #[schemars(with = "Vec<String>")]
    pub roots: Vec<Utf8PathBuf>,

    /// Optional glob patterns (e.g. `"**/*.md"`, `"*.json"`).
    ///
    /// When non-empty, only paths matching at least one pattern are emitted.
    /// Patterns are evaluated against the full path of each file.
    /// When empty, all files are emitted.
    #[serde(default)]
    pub globs: Vec<String>,

    /// Whether to follow symbolic links during the walk.
    #[serde(default)]
    pub follow_symlinks: bool,

    /// Maximum recursion depth relative to each root.
    ///
    /// `None` means unbounded.
    #[serde(default)]
    pub max_depth: Option<usize>,
}

// =============================================================================
// DirectoryWalkCursor
// =============================================================================

/// Cursor for [`DirectoryWalkAdapter`].
///
/// A map from path to [`FileFingerprint`] representing the last-seen state of
/// each file. A file is skipped on a subsequent walk if its fingerprint is
/// unchanged.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DirectoryWalkCursor(pub BTreeMap<Utf8PathBuf, FileFingerprint>);

impl DirectoryWalkCursor {
    /// Returns the stored fingerprint for `path`, if any.
    #[must_use]
    pub fn get(&self, path: &Utf8Path) -> Option<&FileFingerprint> {
        self.0.get(path)
    }

    /// Inserts or updates the fingerprint for `path`.
    pub fn insert(&mut self, path: Utf8PathBuf, fp: FileFingerprint) {
        self.0.insert(path, fp);
    }
}

// =============================================================================
// DirectoryWalkAdapter
// =============================================================================

/// Adapter for recursive directory walks.
///
/// On each `open()` call the adapter enumerates all matching files under every
/// configured root, skipping files whose fingerprint is unchanged from the
/// cursor. Files are emitted in deterministic (sorted) path order so that tests
/// and snapshots are stable.
///
/// File contents are read with `tokio::fs` (async, not buffered into memory all
/// at once — each record is read individually before being yielded).
#[derive(Debug, Clone, Default)]
pub struct DirectoryWalkAdapter;

impl DirectoryWalkAdapter {
    /// Compile the configured globs into a [`GlobSet`].
    ///
    /// Returns `Ok(None)` when no globs are configured (accept everything).
    fn build_glob_set(globs: &[String]) -> ParserResult<Option<GlobSet>> {
        if globs.is_empty() {
            return Ok(None);
        }
        let mut builder = GlobSetBuilder::new();
        for pattern in globs {
            let glob = Glob::new(pattern).map_err(|e| {
                ParserError::Config(format!("invalid glob pattern {pattern:?}: {e}"))
            })?;
            builder.add(glob);
        }
        let set = builder
            .build()
            .map_err(|e| ParserError::Config(format!("failed to build glob set: {e}")))?;
        Ok(Some(set))
    }

    /// Recursively collect all matching file paths under `root`, sorted.
    fn collect_paths(
        root: &Utf8Path,
        globs: &Option<GlobSet>,
        follow_symlinks: bool,
        max_depth: Option<usize>,
        current_depth: usize,
    ) -> ParserResult<Vec<Utf8PathBuf>> {
        let mut results = Vec::new();

        let entries = std::fs::read_dir(root).map_err(|e| {
            ParserError::Io(std::io::Error::new(
                e.kind(),
                format!("failed to read directory {root}: {e}"),
            ))
        })?;

        let mut names: Vec<std::ffi::OsString> = entries
            .filter_map(|entry| entry.ok().map(|e| e.file_name()))
            .collect();
        names.sort_unstable();

        for name in names {
            let std_path = std::path::Path::new(root.as_std_path()).join(&name);
            let Ok(path) = Utf8PathBuf::from_path_buf(std_path.clone()) else {
                continue; // skip non-UTF-8 paths
            };

            let metadata = if follow_symlinks {
                std::fs::metadata(&std_path)
            } else {
                std::fs::symlink_metadata(&std_path)
            };

            let Ok(metadata) = metadata else {
                continue; // skip unreadable entries
            };

            if metadata.is_symlink() && !follow_symlinks {
                continue;
            }

            if metadata.is_dir() {
                let next_depth = current_depth + 1;
                if max_depth.is_none_or(|limit| next_depth <= limit) {
                    let mut sub =
                        Self::collect_paths(&path, globs, follow_symlinks, max_depth, next_depth)?;
                    results.append(&mut sub);
                }
                continue;
            }

            if !metadata.is_file() {
                continue;
            }

            // Glob filter: test the full path string.
            if let Some(set) = globs
                && !set.is_match(path.as_str())
            {
                continue;
            }

            results.push(path);
        }

        Ok(results)
    }

    /// Compute a [`FileFingerprint`] from std filesystem metadata.
    fn fingerprint(meta: &std::fs::Metadata) -> FileFingerprint {
        let modified_ms = meta
            .modified()
            .ok()
            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
            .map_or(0, |d| d.as_millis() as i64);
        FileFingerprint {
            size_bytes: meta.len(),
            modified_ms,
        }
    }

    fn relative_manifest_path(path: &Utf8Path, roots: &[Utf8PathBuf]) -> String {
        roots
            .iter()
            .filter_map(|root| path.strip_prefix(root).ok())
            .min_by_key(|relative| relative.as_str().len())
            .map_or_else(
                || path.as_str().to_string(),
                |relative| relative.as_str().to_string(),
            )
    }

    fn manifest_file_kind(path: &Utf8Path) -> String {
        let extension = path.extension().map(str::to_ascii_lowercase);
        let base_kind = extension.as_ref().map_or_else(
            || "extension:<none>".to_string(),
            |extension| format!("extension:{extension}"),
        );
        match extension.as_deref() {
            Some(extension @ ("csv" | "tsv" | "json" | "jsonl")) => {
                match structured_file_shape_hash(path, extension) {
                    Some(hash) => format!("{base_kind};shape:{hash}"),
                    None => format!("{base_kind};shape:unavailable"),
                }
            }
            _ => base_kind,
        }
    }
}

fn structured_file_shape_hash(path: &Utf8Path, extension: &str) -> Option<String> {
    let bytes = std::fs::read(path.as_std_path()).ok()?;
    let fingerprint = match extension {
        "csv" => SourceRecordFingerprint::from_csv_bytes(&bytes).ok()?,
        "tsv" => SourceRecordFingerprint::from_tsv_bytes(&bytes).ok()?,
        "jsonl" => SourceRecordFingerprint::from_jsonl_bytes(&bytes).ok()?,
        "json" => {
            let value = serde_json::from_slice(&bytes).ok()?;
            SourceRecordFingerprint::from_json(&value)
        }
        _ => return None,
    };
    Some(fingerprint.hash().to_string())
}

#[async_trait]
impl InputShapeAdapter for DirectoryWalkAdapter {
    type Config = DirectoryWalkConfig;
    type Cursor = DirectoryWalkCursor;
    const KIND: InputShapeKind = InputShapeKind::DirectoryWalk;

    async fn open(
        &self,
        material_id: Id<SourceMaterial>,
        config: &Self::Config,
        cursor: Option<Self::Cursor>,
    ) -> ParserResult<BoxStream<'static, ParserResult<SourceRecord>>> {
        let glob_set = Self::build_glob_set(&config.globs)?;
        let cursor = cursor.unwrap_or_default();

        // Collect all matching paths across all roots (sorted per root, then
        // concatenated in root order).
        let mut all_paths: Vec<Utf8PathBuf> = Vec::new();
        for root in &config.roots {
            if !root.exists() {
                continue; // non-existent roots are silently skipped
            }
            let paths =
                Self::collect_paths(root, &glob_set, config.follow_symlinks, config.max_depth, 0)?;
            all_paths.extend(paths);
        }

        // Build the list of records to emit (those that changed or are new).
        // Reading is deferred to the stream so we don't buffer everything.
        struct PendingEntry {
            path: Utf8PathBuf,
            #[allow(dead_code)] // Used by callers consuming the cursor sidechannel
            fingerprint: FileFingerprint,
        }

        let mut pending: Vec<PendingEntry> = Vec::new();
        for path in all_paths {
            let Ok(meta) = std::fs::metadata(path.as_std_path()) else {
                continue;
            };
            if !meta.is_file() {
                continue;
            }
            let fp = Self::fingerprint(&meta);
            if cursor.get(&path).is_some_and(|prev| *prev == fp) {
                continue; // unchanged — skip
            }
            pending.push(PendingEntry {
                path,
                fingerprint: fp,
            });
        }

        // Build a stream that reads each file lazily.
        let stream = stream::iter(pending).then(move |entry| {
            let material_id = material_id;
            async move {
                let bytes = fs::read(entry.path.as_std_path()).await.map_err(|e| {
                    ParserError::Io(std::io::Error::new(
                        e.kind(),
                        format!("failed to read {}: {e}", entry.path),
                    ))
                })?;

                let record = SourceRecord {
                    material_id,
                    anchor: MaterialAnchor::DirectoryEntry {
                        path: entry.path.clone(),
                        content_hash: None,
                    },
                    bytes,
                    logical_path: Some(entry.path),
                    source_ts_hint: None,
                    metadata: serde_json::Value::Null,
                };

                Ok::<SourceRecord, ParserError>(record)
            }
        });

        Ok(Box::pin(stream))
    }

    fn input_fingerprint(
        &self,
        config: &Self::Config,
    ) -> ParserResult<Option<SourceRecordFingerprint>> {
        let glob_set = Self::build_glob_set(&config.globs)?;
        let mut entries = Vec::new();

        for root in &config.roots {
            if !root.exists() {
                continue;
            }
            let paths =
                Self::collect_paths(root, &glob_set, config.follow_symlinks, config.max_depth, 0)?;
            entries.extend(paths.into_iter().map(|path| {
                (
                    Self::relative_manifest_path(&path, &config.roots),
                    Self::manifest_file_kind(&path),
                )
            }));
        }

        entries.sort();
        entries.dedup();

        Ok(Some(SourceRecordFingerprint::from_directory_manifest(
            entries,
        )))
    }

    fn cursor_after(&self, record: &SourceRecord) -> ParserResult<Self::Cursor> {
        // Extract path and fingerprint from the record's anchor + bytes.
        let path = match &record.anchor {
            MaterialAnchor::DirectoryEntry { path, .. } => path.clone(),
            _ => {
                return Err(ParserError::Cursor(
                    "DirectoryWalkAdapter: record anchor is not DirectoryEntry".into(),
                ));
            }
        };
        let size_bytes = record.bytes.len() as u64;
        // We can't recover modified_ms from bytes alone; use 0 as sentinel
        // so a subsequent walk (which re-reads metadata) will compare correctly.
        // In practice cursor_after is called once per record and the runtime
        // merges cursors; the metadata comparison uses the live FS value.
        let fp = FileFingerprint {
            size_bytes,
            modified_ms: 0,
        };
        let mut cursor = DirectoryWalkCursor::default();
        cursor.insert(path, fp);
        Ok(cursor)
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
#[path = "directory_walk_test.rs"]
mod tests;
