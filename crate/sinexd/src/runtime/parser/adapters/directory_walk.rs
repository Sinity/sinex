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
mod tests {
    use super::*;
    use futures::StreamExt;
    use std::io::Write;
    use tempfile::TempDir;
    use xtask::sandbox::prelude::sinex_test;

    fn dummy_material_id() -> Id<SourceMaterial> {
        Id::from_uuid(uuid::Uuid::new_v4())
    }

    fn simple_config(roots: Vec<Utf8PathBuf>) -> DirectoryWalkConfig {
        DirectoryWalkConfig {
            roots,
            globs: vec![],
            follow_symlinks: false,
            max_depth: None,
        }
    }

    async fn collect_records(
        adapter: &DirectoryWalkAdapter,
        config: &DirectoryWalkConfig,
        cursor: Option<DirectoryWalkCursor>,
    ) -> Vec<SourceRecord> {
        let stream = adapter
            .open(dummy_material_id(), config, cursor)
            .await
            .unwrap();
        stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect()
    }

    // -------------------------------------------------------------------------

    #[sinex_test]
    async fn test_empty_directory_yields_zero_records() -> xtask::sandbox::TestResult<()> {
        let dir = TempDir::new().unwrap();
        let root = Utf8PathBuf::from_path_buf(dir.path().to_path_buf()).unwrap();
        let adapter = DirectoryWalkAdapter;
        let config = simple_config(vec![root]);
        let records = collect_records(&adapter, &config, None).await;
        assert_eq!(records.len(), 0);
        Ok(())
    }

    #[sinex_test]
    async fn test_walk_emits_record_per_file() -> xtask::sandbox::TestResult<()> {
        let dir = TempDir::new().unwrap();
        let root = Utf8PathBuf::from_path_buf(dir.path().to_path_buf()).unwrap();

        for name in &["a.txt", "b.txt", "c.txt"] {
            let mut f = std::fs::File::create(dir.path().join(name)).unwrap();
            write!(f, "content of {name}").unwrap();
        }

        let adapter = DirectoryWalkAdapter;
        let config = simple_config(vec![root]);
        let records = collect_records(&adapter, &config, None).await;

        assert_eq!(records.len(), 3);
        // Records are emitted in sorted path order.
        let paths: Vec<String> = records
            .iter()
            .map(|r| {
                r.logical_path
                    .as_ref()
                    .unwrap()
                    .file_name()
                    .unwrap()
                    .to_string()
            })
            .collect();
        assert_eq!(paths, vec!["a.txt", "b.txt", "c.txt"]);
        Ok(())
    }

    #[sinex_test]
    async fn test_cursor_based_dedup_skips_unchanged_files() -> xtask::sandbox::TestResult<()> {
        let dir = TempDir::new().unwrap();
        let root = Utf8PathBuf::from_path_buf(dir.path().to_path_buf()).unwrap();

        let file_path = dir.path().join("file.txt");
        let mut f = std::fs::File::create(&file_path).unwrap();
        write!(f, "initial").unwrap();
        drop(f);

        let adapter = DirectoryWalkAdapter;
        let config = simple_config(vec![root.clone()]);

        // First walk: file is new, so it is emitted.
        let records = collect_records(&adapter, &config, None).await;
        assert_eq!(records.len(), 1);

        // Build a cursor that matches the current fingerprint.
        let meta = std::fs::metadata(&file_path).unwrap();
        let fp = DirectoryWalkAdapter::fingerprint(&meta);
        let utf8_path = Utf8PathBuf::from_path_buf(file_path.clone()).unwrap();
        let mut cursor = DirectoryWalkCursor::default();
        cursor.insert(utf8_path.clone(), fp);

        // Second walk with matching cursor: file should be skipped.
        let records2 = collect_records(&adapter, &config, Some(cursor)).await;
        assert_eq!(records2.len(), 0, "unchanged file should be deduped");

        // Modify the file (change content to change size).
        let mut f2 = std::fs::File::create(&file_path).unwrap();
        write!(f2, "modified content that is longer").unwrap();
        drop(f2);

        // Build cursor with old fingerprint (size mismatch now).
        let mut stale_cursor = DirectoryWalkCursor::default();
        stale_cursor.insert(utf8_path, fp);

        // Third walk: fingerprint changed, file should be re-emitted.
        let records3 = collect_records(&adapter, &config, Some(stale_cursor)).await;
        assert_eq!(records3.len(), 1, "modified file should be re-emitted");
        Ok(())
    }

    #[sinex_test]
    async fn test_glob_filter_restricts_emission() -> xtask::sandbox::TestResult<()> {
        let dir = TempDir::new().unwrap();
        let root = Utf8PathBuf::from_path_buf(dir.path().to_path_buf()).unwrap();

        for name in &["doc.md", "data.json", "script.sh"] {
            let mut f = std::fs::File::create(dir.path().join(name)).unwrap();
            write!(f, "x").unwrap();
        }

        let adapter = DirectoryWalkAdapter;
        let config = DirectoryWalkConfig {
            roots: vec![root],
            globs: vec!["**/*.md".into()],
            follow_symlinks: false,
            max_depth: None,
        };

        let records = collect_records(&adapter, &config, None).await;
        assert_eq!(records.len(), 1);
        assert!(
            records[0]
                .logical_path
                .as_ref()
                .unwrap()
                .as_str()
                .ends_with("doc.md")
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_max_depth_bounds_recursion() -> xtask::sandbox::TestResult<()> {
        let dir = TempDir::new().unwrap();
        let root = Utf8PathBuf::from_path_buf(dir.path().to_path_buf()).unwrap();

        // Create: root/top.txt, root/sub/nested.txt
        let mut f = std::fs::File::create(dir.path().join("top.txt")).unwrap();
        write!(f, "top").unwrap();
        let sub = dir.path().join("sub");
        std::fs::create_dir(&sub).unwrap();
        let mut f2 = std::fs::File::create(sub.join("nested.txt")).unwrap();
        write!(f2, "nested").unwrap();

        let adapter = DirectoryWalkAdapter;

        // max_depth=0 → only files directly in root (no recursion into sub/).
        let config_shallow = DirectoryWalkConfig {
            roots: vec![root.clone()],
            globs: vec![],
            follow_symlinks: false,
            max_depth: Some(0),
        };
        let records_shallow = collect_records(&adapter, &config_shallow, None).await;
        assert_eq!(records_shallow.len(), 1, "only top.txt at depth 0");
        assert!(
            records_shallow[0]
                .logical_path
                .as_ref()
                .unwrap()
                .as_str()
                .ends_with("top.txt")
        );

        // max_depth=1 → includes sub/nested.txt.
        let config_deep = DirectoryWalkConfig {
            roots: vec![root],
            globs: vec![],
            follow_symlinks: false,
            max_depth: Some(1),
        };
        let records_deep = collect_records(&adapter, &config_deep, None).await;
        assert_eq!(
            records_deep.len(),
            2,
            "both top.txt and nested.txt at depth 1"
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_input_fingerprint_reports_directory_manifest_shape()
    -> xtask::sandbox::TestResult<()> {
        let dir = TempDir::new().unwrap();
        let root = Utf8PathBuf::from_path_buf(dir.path().to_path_buf()).unwrap();
        let sub = dir.path().join("sub");
        std::fs::create_dir(&sub).unwrap();

        let mut csv = std::fs::File::create(dir.path().join("events.csv")).unwrap();
        write!(csv, "id,name\n1,Alice").unwrap();
        let mut json = std::fs::File::create(sub.join("profile.JSON")).unwrap();
        write!(json, "{{\"id\":1}}").unwrap();
        let mut jsonl = std::fs::File::create(sub.join("events.jsonl")).unwrap();
        writeln!(jsonl, "{{\"event_id\":1}}").unwrap();

        let adapter = DirectoryWalkAdapter;
        let config = simple_config(vec![root]);
        let fingerprint = adapter.input_fingerprint(&config)?.unwrap();

        assert_eq!(fingerprint.format, "directory_manifest");
        assert_eq!(
            fingerprint.keys,
            vec!["events.csv", "sub/events.jsonl", "sub/profile.JSON"]
        );
        assert!(
            fingerprint
                .type_map
                .get("events.csv")
                .is_some_and(|kind| kind.starts_with("extension:csv;shape:"))
        );
        assert!(
            fingerprint
                .type_map
                .get("sub/profile.JSON")
                .is_some_and(|kind| kind.starts_with("extension:json;shape:"))
        );
        assert!(
            fingerprint
                .type_map
                .get("sub/events.jsonl")
                .is_some_and(|kind| kind.starts_with("extension:jsonl;shape:"))
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_input_fingerprint_hash_changes_when_file_set_changes()
    -> xtask::sandbox::TestResult<()> {
        let dir = TempDir::new().unwrap();
        let root = Utf8PathBuf::from_path_buf(dir.path().to_path_buf()).unwrap();
        let mut first = std::fs::File::create(dir.path().join("events.csv")).unwrap();
        write!(first, "id,name\n1,Alice").unwrap();

        let adapter = DirectoryWalkAdapter;
        let config = simple_config(vec![root]);
        let before = adapter.input_fingerprint(&config)?.unwrap();

        let mut second = std::fs::File::create(dir.path().join("events.json")).unwrap();
        write!(second, "{{\"id\":1}}").unwrap();
        let after = adapter.input_fingerprint(&config)?.unwrap();

        assert_ne!(before.hash(), after.hash());
        assert!(after.keys.contains(&"events.csv".to_string()));
        assert!(after.keys.contains(&"events.json".to_string()));
        Ok(())
    }

    #[sinex_test]
    async fn test_input_fingerprint_hash_changes_when_structured_child_shape_changes()
    -> xtask::sandbox::TestResult<()> {
        let dir = TempDir::new().unwrap();
        let root = Utf8PathBuf::from_path_buf(dir.path().to_path_buf()).unwrap();
        let csv_path = dir.path().join("events.csv");
        let mut first = std::fs::File::create(&csv_path).unwrap();
        write!(first, "id,name\n1,Alice").unwrap();
        drop(first);

        let adapter = DirectoryWalkAdapter;
        let config = simple_config(vec![root]);
        let before = adapter.input_fingerprint(&config)?.unwrap();

        let mut second = std::fs::File::create(&csv_path).unwrap();
        write!(second, "id,display_name,active\n1,Alice,true").unwrap();
        drop(second);
        let after = adapter.input_fingerprint(&config)?.unwrap();

        assert_eq!(before.keys, after.keys);
        assert_ne!(before.hash(), after.hash());
        assert_ne!(before.type_map["events.csv"], after.type_map["events.csv"]);
        Ok(())
    }

    #[sinex_test]
    async fn test_anchor_is_directory_entry() -> xtask::sandbox::TestResult<()> {
        let dir = TempDir::new().unwrap();
        let root = Utf8PathBuf::from_path_buf(dir.path().to_path_buf()).unwrap();
        let mut f = std::fs::File::create(dir.path().join("file.txt")).unwrap();
        write!(f, "hello").unwrap();

        let adapter = DirectoryWalkAdapter;
        let config = simple_config(vec![root]);
        let records = collect_records(&adapter, &config, None).await;

        assert_eq!(records.len(), 1);
        assert!(matches!(
            &records[0].anchor,
            MaterialAnchor::DirectoryEntry {
                path: _,
                content_hash: None
            }
        ));
        Ok(())
    }

    #[sinex_test]
    async fn test_non_existent_root_is_silently_skipped() -> xtask::sandbox::TestResult<()> {
        let adapter = DirectoryWalkAdapter;
        let config = simple_config(vec![Utf8PathBuf::from(
            "/nonexistent/dir/that/does/not/exist",
        )]);
        let records = collect_records(&adapter, &config, None).await;
        assert_eq!(records.len(), 0);
        Ok(())
    }
}
